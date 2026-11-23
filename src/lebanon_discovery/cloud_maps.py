"""Compose entry point for one cloud host using the existing durable collector."""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import socket
import sqlite3
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from zoneinfo import ZoneInfo

from .browser_trial import WorkerLock
from .map_walk import camera
from .map_walk_pool import PoolReader
from .national_catalog_v1 import camera_key, canonical
from .national_runtime_v2 import manage
from .national_state_v1 import State, checkpoint, write_json


# The runtime stops granting actions with 15 seconds left. Also leave time for
# the subprocess, browser worker and its coordinator to start.
MIN_NEW_BATCH_SECONDS = 30
NETWORK_CHECK_SECONDS = 15
WAKE_GAP_SECONDS = 20
RECOVERABLE_INTERRUPTS = {"network_unavailable", "host_resumed"}


def connection_ready():
    """Check DNS, routing and TLS without fetching a Maps page."""
    try:
        with socket.create_connection(("www.google.com", 443), timeout=4) as transport:
            with ssl.create_default_context().wrap_socket(transport, server_hostname="www.google.com"):
                return True
    except OSError:
        return False


def source_available(config):
    return config.get("mode") != "live" or connection_ready()


class CloudReader(PoolReader):
    """Bundled headless Chromium, sandboxed on the Linux host."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, headed=False)
        self.channel = None
        self.chromium_sandbox = True


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def integer(env, key, default, minimum, maximum):
    value = int(env.get(key, str(default)))
    if not minimum <= value <= maximum:
        raise ValueError(f"{key} must be between {minimum} and {maximum}")
    return value


def settings(env):
    mode = env.get("FYP_MODE", "fixture")
    if mode not in {"fixture", "live"}:
        raise ValueError("FYP_MODE must be fixture or live")
    scope = env.get("FYP_SCOPE_NOTE", "").strip()
    if mode == "live" and (env.get("FYP_ENABLE_LIVE") != "1" or not scope):
        raise ValueError("Live collection needs FYP_ENABLE_LIVE=1 and FYP_SCOPE_NOTE")
    config = {
        "kind": "cloud-session-v1", "mode": mode, "scope_note": scope,
        "workers": integer(env, "FYP_WORKERS", 3, 1, 50),
        "batch_seconds": integer(env, "FYP_BATCH_SECONDS", 3600, 60, 3600),
        "session_seconds": integer(env, "FYP_SESSION_SECONDS", 86400, 60, 90 * 86400),
        "memory_mib": integer(env, "FYP_MEMORY_MIB", 8192, 8192, 262144),
        "batch_limit": 120, "hourly_limit": 120, "daily_limit": 2880,
        "map_spacing": 30 if mode == "live" else 0,
        "action_spacing": 5 if mode == "live" else 0,
        "minimum_free_bytes": 2 * 1024 ** 3,
        "maximum_session_bytes": 5 * 1024 ** 3,
    }
    if "FYP_CHECKPOINT_SECONDS" in env:
        config["checkpoint_seconds"] = integer(env, "FYP_CHECKPOINT_SECONDS", 300, 30, 300)
    if "FYP_HOURLY_LIMIT" in env:
        config["hourly_limit"] = integer(env, "FYP_HOURLY_LIMIT", 120, 1, 120)
    if env.get("FYP_CONTINUOUS") == "1":
        config["continuous"] = True
    return config


def route(config, inputs):
    if config["mode"] == "fixture":
        data = {"kind": "cloud-map-route-v1", "views": [
            {"id": f"fixture-{i}", "region": region, "latitude": 33.89 + i / 1000,
             "longitude": 35.5, "zoom": 18}
            for i, region in enumerate(("Beirut", "South", "Bekaa"))
        ]}
    else:
        data = read(Path(inputs) / "route.json")
    if data.get("kind") != "cloud-map-route-v1" or not isinstance(data.get("views"), list):
        raise ValueError("Expected a cloud-map-route-v1 manifest")
    if not 1 <= len(data["views"]) <= 200000:
        raise ValueError("Route must have 1 to 200000 views")
    seen_ids, seen_cameras, views = set(), set(), []
    for item in data["views"]:
        identity, region = item["id"], item["region"]
        if (not isinstance(identity, str) or not re.fullmatch(r"[\w.-]{1,100}", identity)
                or identity in seen_ids or not isinstance(region, str)
                or not 1 <= len(region) <= 100 or any(ord(c) < 32 for c in region)):
            raise ValueError("Invalid route ID or region")
        lat, lon, zoom = item["latitude"], item["longitude"], item.get("zoom", 18)
        if (type(lat) not in (int, float) or type(lon) not in (int, float)
                or not math.isfinite(lat) or not math.isfinite(lon) or type(zoom) is not int):
            raise ValueError("Invalid coordinates or zoom")
        cam = camera(lat, lon, zoom=zoom)
        signature = camera_key(cam)
        if signature in seen_cameras:
            raise ValueError("Repeated camera in route")
        seen_ids.add(identity)
        seen_cameras.add(signature)
        views.append({"id": identity, "region": region, "camera": cam})
    return views


def build_catalog(path, views, workers):
    with closing(sqlite3.connect(path)) as db:
        db.executescript("""
            CREATE TABLE cameras(id TEXT PRIMARY KEY,camera_key TEXT UNIQUE,region TEXT,
                ordinal INTEGER,camera TEXT,worker_id TEXT);
            CREATE TABLE memberships(id TEXT,region TEXT,source_id TEXT,ordinal INTEGER);
            CREATE TABLE workers(id TEXT PRIMARY KEY,region TEXT,views INTEGER,bbox TEXT);
            CREATE INDEX camera_order ON cameras(worker_id,ordinal);
        """)
        with db:
            for i, view in enumerate(views):
                worker = f"w{i % workers + 1:03d}"
                db.execute("INSERT INTO cameras VALUES (?,?,?,?,?,?)", (
                    view["id"], camera_key(view["camera"]), view["region"], i,
                    canonical(view["camera"]), worker))
                db.execute("INSERT INTO memberships VALUES (?,?,?,?)", (
                    view["id"], view["region"], view["id"], i))
            for i in range(workers):
                identity = f"w{i + 1:03d}"
                count = db.execute("SELECT count(*) FROM cameras WHERE worker_id=?", (identity,)).fetchone()[0]
                db.execute("INSERT INTO workers VALUES (?,?,?,?)", (identity, "shared", count, "[]"))


def prepare(data, inputs, config):
    """Atomically publish one bootstrap; restarts must retain the same scope."""
    data = Path(data)
    views = route(config, inputs)
    config = dict(config, workers=min(config["workers"], len(views)))
    route_hash = hashlib.sha256(canonical(views).encode()).hexdigest()
    identity = {**config, "route_sha256": route_hash}
    session = data / config["mode"]
    if session.exists():
        old = read(session / "config.json")
        if old.get("settings") != identity:
            raise ValueError("Saved session settings or route changed; restore its original inputs")
        if hashlib.sha256((session / "catalog.sqlite3").read_bytes()).hexdigest() != old["catalog_sha256"]:
            raise ValueError("Saved catalog hash changed")
        return session
    if config.get("continuous") and config["mode"] == "live":
        raise ValueError("Continuous live collection needs an explicitly adopted checkpoint")
    staging = data / (".bootstrap-" + uuid.uuid4().hex)
    staging.mkdir()
    catalog = staging / "catalog.sqlite3"
    build_catalog(catalog, views, config["workers"])
    now = time.time()
    stored = {**config, "settings": identity, "created_epoch": now,
              "expires_epoch": config.get("host_pace_expiry", None if config.get("continuous") else now + config["session_seconds"]),
              "session_id": "cloud-" + config["mode"],
              "catalog_sha256": hashlib.sha256(catalog.read_bytes()).hexdigest()}
    (staging / "runs").mkdir()
    with tempfile.TemporaryDirectory() as temporary:
        state = State.initialize(Path(temporary) / "bootstrap.sqlite3", catalog)
        try:
            checkpoint(state, staging)
        finally:
            state.close()
    write_json(staging / "config.json", stored)
    write_json(staging / "route.json", {"kind": "cloud-map-camera-snapshot-v1", "views": views})
    staging.rename(session)
    if os.name != "nt":
        descriptor = os.open(data, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return session


def emit_status(data, session, phase, **fields):
    value = {"kind": "cloud-collector-status-v1", "mode": session.name,
             "phase": phase, "heartbeat_epoch": time.time(), **fields}
    write_json(data / "status.json", value)
    return value


def supervise_batch(data, session, job, stopped):
    """Run the checker outside the coordinator and browser processes."""
    folder = session / "runs" / job
    folder.mkdir()
    config = read(session / "config.json")
    started = time.time()
    with tempfile.TemporaryDirectory(prefix="maps-work-") as work:
        with (folder / "coordinator.log").open("w", encoding="utf-8") as log:
            process = subprocess.Popen([
                sys.executable, "-m", "lebanon_discovery.cloud_maps", "manage", "--session", str(session),
                "--work", str(Path(work) / "active"), "--job", job,
            ], stdout=log, stderr=log, start_new_session=True)
            reason, stopping_at = None, None
            last_cycle = time.time()
            probe_due = last_cycle + NETWORK_CHECK_SECONDS
            while process.poll() is None:
                now = time.time()
                progress = read(folder / "progress.json") if (folder / "progress.json").exists() else None
                age = now - (progress["heartbeat_epoch"] if progress else started)
                if reason is None and stopping_at is None and not stopped.is_set():
                    if now - last_cycle > WAKE_GAP_SECONDS:
                        reason = "host_resumed"
                    elif age > (90 if progress else 180):
                        reason = "coordinator_heartbeat_stale"
                    elif now - started > config["batch_seconds"] + 120:
                        reason = "batch_deadline"
                    elif now >= probe_due:
                        if not source_available(config):
                            reason = "network_unavailable"
                        probe_due = time.time() + NETWORK_CHECK_SECONDS
                if stopped.is_set() or reason:
                    if stopping_at is None:
                        if process.poll() is None:
                            process.terminate()
                        stopping_at = time.monotonic()
                    if time.monotonic() - stopping_at > 90:
                        os.killpg(process.pid, signal.SIGKILL)
                write_json(folder / "watchdog.json", {"at": now, "reason": reason, "job": job})
                emit_status(data, session, "stopping" if stopping_at else "running", job=job,
                            progress=progress)
                last_cycle = time.time()
                if stopped.is_set():
                    time.sleep(.1)
                else:
                    stopped.wait(2)
            process.wait()
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
    final = read(folder / "final.json") if (folder / "final.json").exists() else None
    healthy = (process.returncode == 0 and final and final["phase"] == "completed"
               and (reason is None or reason in RECOVERABLE_INTERRUPTS and not final["stats"]["halted"]))
    write_json(folder / "watchdog-final.json", {"healthy": bool(healthy), "reason": reason,
                                              "exit_code": process.returncode})
    if not healthy:
        return None
    return {**final, "reason": reason} if reason else final


def terminal_phase(final):
    if final is None or final["phase"] != "completed" or final["stats"]["halted"]:
        return "needs_attention"
    if final["stats"]["remaining_views"] == 0:
        return "completed_with_gaps" if final["stats"]["states"].get("incomplete") else "completed"
    return None



def next_batch_epoch(session, config, now=None):
    """Wait only until a shared budget actually becomes available."""
    now = time.time() if now is None else now
    if config["mode"] == "fixture":
        return now
    pointer = read(Path(session) / "snapshots/snapshot.json")
    candidates = [Path(session) / "snapshots" / name for name in ("current.sqlite3", "previous.sqlite3")]
    snapshot = next((p for p in candidates if p.exists()
                     and hashlib.sha256(p.read_bytes()).hexdigest() == pointer["sha256"]), None)
    if snapshot is None:
        raise ValueError("No verified budget snapshot")
    local_day = datetime.fromtimestamp(now, ZoneInfo("Asia/Beirut"))
    midnight = datetime.combine(local_day.date() + timedelta(days=1), datetime.min.time(),
                                tzinfo=ZoneInfo("Asia/Beirut")).timestamp()
    ready = now
    with closing(sqlite3.connect(snapshot.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        for kind in ("search", "detail"):
            daily = db.execute("SELECT count(*) FROM events WHERE kind=? AND day=?",
                               (kind, local_day.date().isoformat())).fetchone()[0]
            if daily >= config["daily_limit"]:
                ready = max(ready, midnight + 1)
            rolling = db.execute(
                "SELECT due FROM events WHERE kind=? AND due>? ORDER BY due DESC LIMIT ?",
                (kind, now - 3600, config["hourly_limit"]),
            ).fetchall()
            if len(rolling) >= config["hourly_limit"]:
                ready = max(ready, rolling[-1][0] + 3600 + .1)
    return ready


def run(data, inputs, stopped):
    data = Path(data)
    data.mkdir(parents=True, exist_ok=True)
    config = settings(os.environ)
    if (lane := os.environ.get("FYP_PC5_LANE")):
        from .host_pace import HostPace, LANES, ROOT
        if lane not in LANES:
            raise ValueError("Unknown PC 5 assignment")
        gate = HostPace(ROOT, config["mode"])
        try:
            config.update(host_pace_id=gate.identity, host_pace_expiry=gate.expires_epoch, host_pace_lane=lane)
        finally:
            gate.close()
    with WorkerLock(data / ".collector.lock"):
        session = prepare(data, inputs, config)
        config = read(session / "config.json")
        controller = session / "controller.json"
        state = read(controller) if controller.exists() else {"phase": "ready", "batches": 0}
        if state.get("active_job"):
            # On a container restart the old process group is gone. Durable replay
            # quarantines grants without confirmed outcomes before claiming work.
            recoveries = state.get("container_recoveries", 0) + 1
            state.update(container_recoveries=recoveries, active_job=None)
            if recoveries > 2:
                state["phase"] = "needs_attention"
            write_json(controller, state)
        while not stopped.is_set():
            if config.get("host_pace_id") and state["phase"] not in {"completed", "completed_with_gaps", "needs_attention", "expired"}:
                gate = HostPace(ROOT, config["mode"])
                try:
                    if gate.identity != config["host_pace_id"]:
                        raise ValueError("Shared pacing ledger identity changed")
                    if gate.halted():
                        state["phase"] = "needs_attention"
                        state["reason"] = "shared_source_stop"
                        write_json(controller, state)
                finally:
                    gate.close()
            if state["phase"] in {"completed", "completed_with_gaps", "needs_attention", "expired"}:
                emit_status(data, session, state["phase"], controller=state)
                if os.environ.get("FYP_EXIT_WHEN_TERMINAL") == "1":
                    return
                stopped.wait(15)
                continue
            now = time.time()
            end = config["expires_epoch"]
            remaining = end - now if end is not None else None
            if remaining is not None and remaining <= MIN_NEW_BATCH_SECONDS:
                state["phase"] = "expired"
                if remaining > 0:
                    state["expiry_reason"] = "insufficient_time_for_another_batch"
                write_json(controller, state)
                continue
            if now < state.get("next_batch_epoch", 0):
                emit_status(data, session, "waiting", controller=state)
                stopped.wait(max(0, min(15, state["next_batch_epoch"] - time.time())))
                continue
            if not source_available(config):
                emit_status(data, session, "waiting_for_network", controller=state)
                stopped.wait(NETWORK_CHECK_SECONDS)
                continue
            if stopped.is_set():
                break
            if end is not None and end - time.time() <= MIN_NEW_BATCH_SECONDS:
                continue
            job = f"{time.time_ns()}-{uuid.uuid4().hex[:8]}"
            state.update(phase="running", active_job=job, batches=state["batches"] + 1)
            write_json(controller, state)
            final = supervise_batch(data, session, job, stopped)
            state.update(active_job=None, last_job=job, phase=terminal_phase(final) or "ready")
            if final:
                state["stats"] = final["stats"]
                # The durable allowance function enforces real rolling hour/day
                # totals. Backoff avoids submitting empty batches while capped.
                state["next_batch_epoch"] = next_batch_epoch(session, config)
                if final.get("reason") in RECOVERABLE_INTERRUPTS:
                    state["next_batch_epoch"] = max(state["next_batch_epoch"], time.time() + NETWORK_CHECK_SECONDS)
                state["container_recoveries"] = 0
            write_json(controller, state)
            print(json.dumps({"mode": session.name, "phase": state["phase"], "job": job,
                              "stats": state.get("stats")}), flush=True)
        emit_status(data, session, "paused", controller=state)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("init-volume", "run", "manage", "health", "status"))
    parser.add_argument("--data", type=Path, default=Path("/data"))
    parser.add_argument("--inputs", type=Path, default=Path("/inputs"))
    parser.add_argument("--session", type=Path)
    parser.add_argument("--work", type=Path)
    parser.add_argument("--job")
    args = parser.parse_args()
    if args.command == "init-volume":
        args.data.mkdir(parents=True, exist_ok=True)
        os.chown(args.data, 10001, 10001)
        return 0
    if args.command == "status":
        print(json.dumps(read(args.data / "status.json"), indent=2))
        return 0
    if args.command == "health":
        value = read(args.data / "status.json")
        return 0 if time.time() - value["heartbeat_epoch"] < 75 and value["phase"] != "needs_attention" else 1
    stopped = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda signum, frame: stopped.set())
    if args.command == "manage":
        if not args.session or not args.work or not re.fullmatch(r"\d+-[a-f0-9]{8}", args.job or ""):
            parser.error("manage needs --session, --work and a valid --job")
        Path(os.environ.get("HOME", tempfile.gettempdir())).mkdir(parents=True, exist_ok=True)
        return manage(args.session, args.work, args.session / "catalog.sqlite3",
                      job_id=args.job, reader_factory=CloudReader, stop_requested=stopped.is_set)
    run(args.data, args.inputs, stopped)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(json.dumps({"phase": "startup_error", "error_type": type(error).__name__}), flush=True)
        raise SystemExit(2)
