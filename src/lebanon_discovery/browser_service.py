"""Local two-worker supervisor; explicit finite scope, persistent source budgets.

Fixture is the default. --watch keeps the process available, not an endless
search generator. Existing frozen collectors and production DB are untouched.
"""
from __future__ import annotations

import hashlib
import json
import multiprocessing as mp
import sqlite3
import time
import uuid
from contextlib import closing, nullcontext
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from .browser_trial import KIND, WorkerLock, own_database, prepare, validate_config, write_json
from .browser_details import DetailsReader

BEIRUT = ZoneInfo("Asia/Beirut")
SERVICE = KIND + ":service-v1"


def day_at(value: float) -> str:
    return datetime.fromtimestamp(value, BEIRUT).date().isoformat()


def tomorrow(value: float) -> float:
    date = datetime.fromtimestamp(value, BEIRUT).date() + timedelta(days=1)
    return datetime.combine(date, datetime.min.time(), BEIRUT).timestamp()


def initialize(root: Path, folder: Path, mode: str = "fixture") -> dict:
    config = prepare(root, folder, mode, "Local service pilot; fixed two-task scope; no automatic geographic expansion.")
    config["allowed_targets"] = {t["cell_id"]: t["governorate"] for t in config["tasks"]}
    config["tasks"] = config["tasks"][:2]
    if mode == "live":
        from urllib.parse import urlencode
        for task, category in zip(config["tasks"], ("architects", "accounting firms")):
            locality = task["query"].split("businesses near ", 1)[1]
            task["query"] = category + " near " + locality
            task["profile"] = category.replace(" ", "-") + "-service-pilot-v1"
            task["task_key"] = task["cell_id"] + ":" + task["profile"]
            task["maps_url"] = "https://www.google.com/maps/search/?" + urlencode({"api": "1", "query": task["query"]})
    config.update(service_schema=1, workers=2, max_searches=9, max_details=27,
                  daily_searches=10, daily_details=30, max_task_wall_seconds=180)
    write_json(folder / "config.json", config)
    return config


def configuration(folder: Path) -> tuple[dict, str]:
    data = (folder / "config.json").read_bytes()
    config = json.loads(data)
    validate_config(config)
    if (config.get("service_schema") != 1 or config.get("workers") not in {1, 2}
            or not 1 <= config.get("max_details", 0) <= 27
            or not 1 <= config.get("daily_searches", 0) <= 10
            or not 1 <= config.get("daily_details", 0) <= 30
            or not 60 <= config.get("max_task_wall_seconds", 0) <= 180):
        raise ValueError("Unsupported bounded service settings")
    if len({t["query"].casefold().strip() for t in config["tasks"]}) != len(config["tasks"]):
        raise ValueError("Duplicate queries in service scope")
    if (config.get("reader_version", "v2") not in {"v2", "ready-v3"}
            or type(config.get("headed", False)) is not bool):
        raise ValueError("Unsupported browser reader configuration")
    return config, hashlib.sha256(data).hexdigest()


class Queue:
    def __init__(self, folder: Path, config: dict, campaign: str):
        self.folder, self.config, self.campaign = folder, config, campaign
        self.path = folder / "queue.sqlite3"

    def connect(self):
        return own_database(self.path, SERVICE + ":queue")

    def initialize(self):
        with closing(self.connect()) as db:
            old = db.execute("SELECT value FROM meta WHERE key='config_hash'").fetchone()
            if old and old[0] != self.campaign:
                raise ValueError("Service config changed after initialization")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY,ordinal INTEGER,payload TEXT,
                    status TEXT DEFAULT 'queued',owner TEXT,started REAL,available REAL DEFAULT 0,
                    actual REAL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS observations(job TEXT,rank INTEGER,payload TEXT,PRIMARY KEY(job,rank));
            """)
            db.execute("INSERT OR IGNORE INTO meta VALUES ('config_hash',?)", (self.campaign,))
            for index, task in enumerate(self.config["tasks"]):
                db.execute("INSERT OR IGNORE INTO jobs(id,ordinal,payload) VALUES (?,?,?)",
                           (task["task_key"], index, json.dumps(task)))
            db.commit()

    def claim(self, owner: str):
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM jobs WHERE status IN ('queued','deferred') AND available<=? ORDER BY ordinal LIMIT 1",
                             (time.time(),)).fetchone()
            if not row:
                db.rollback()
                return None
            db.execute("UPDATE jobs SET status='running',owner=?,started=? WHERE id=?", (owner, time.time(), row["id"]))
            db.commit()
            return dict(row)

    def enqueue(self, tasks: list[dict]) -> int:
        candidate = {**self.config, "tasks": tasks}
        validate_config(candidate)
        if any(self.config["allowed_targets"].get(t["cell_id"]) != t["governorate"] for t in tasks):
            raise ValueError("Task is outside this service's independent target list")
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT value FROM meta WHERE key='config_hash'").fetchone()[0] != self.campaign:
                raise ValueError("Service config changed after initialization")
            old = {r["id"]: json.loads(r["payload"]) for r in db.execute("SELECT id,payload FROM jobs")}
            added = [t for t in tasks if t["task_key"] not in old]
            if any(t["task_key"] in old and old[t["task_key"]] != t for t in tasks):
                raise ValueError("Cannot replace a historical task")
            if len(old) + len(added) > self.config["max_searches"]:
                raise ValueError("Campaign task ceiling exceeded")
            queries = [t["query"].casefold().strip() for t in list(old.values()) + added]
            if len(set(queries)) != len(queries):
                raise ValueError("Query already in queue")
            ordinal = db.execute("SELECT coalesce(max(ordinal),-1)+1 FROM jobs").fetchone()[0]
            for index, task in enumerate(added):
                db.execute("INSERT INTO jobs(id,ordinal,payload) VALUES (?,?,?)", (task["task_key"], ordinal + index, json.dumps(task)))
            db.commit()
            return len(added)

    def finish(self, job: str, owner: str, outcome: dict, actual: float):
        if len(outcome.get("records", [])) > self.config["cards_per_search"]:
            raise ValueError("Selected-card cap exceeded")
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            if not db.execute("SELECT 1 FROM jobs WHERE id=? AND owner=? AND status='running'", (job, owner)).fetchone():
                raise ValueError("Stale worker result")
            db.execute("UPDATE jobs SET status=?,actual=? WHERE id=?", (outcome["status"], actual, job))
            for rank, record in enumerate(outcome.get("records", []), 1):
                db.execute("INSERT INTO observations VALUES (?,?,?)", (job, rank, json.dumps(record)))
            db.commit()

    def defer(self, job: str, owner: str, due: float):
        with closing(self.connect()) as db:
            db.execute("UPDATE jobs SET status='deferred',available=?,owner=NULL WHERE id=? AND owner=?", (due, job, owner))
            db.commit()


class Source:
    def __init__(self, path: Path, folder: Path, config: dict, campaign: str):
        self.path, self.folder, self.config, self.campaign = path, folder, config, campaign

    def connect(self):
        return own_database(self.path, SERVICE + ":source")

    def initialize(self, root: Path | None = None):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self.connect()) as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY,campaign TEXT,kind TEXT,identity TEXT,due REAL,day TEXT);
                CREATE TABLE IF NOT EXISTS cache(identity TEXT PRIMARY KEY,website TEXT,observed REAL);
            """)
            from . import browser_registry
            browser_registry.initialize(db)
            if not db.execute("SELECT 1 FROM meta WHERE key='browser_cache_seed_v1'").fetchone():
                for row in db.execute("SELECT identity,website FROM cache").fetchall():
                    browser_registry.remember(db, json.loads(row["website"]), row["identity"], "preexisting_source_cache")
                db.execute("INSERT INTO meta VALUES ('browser_cache_seed_v1','true')")
            if root:
                browser_registry.seed_frozen_history(root, db)
            if root and not db.execute("SELECT 1 FROM meta WHERE key='legacy_seeded'").fetchone():
                # Preserve legacy files; conservatively charge known earlier reservations.
                old = root / "reports/local_browser_source_v1/source.sqlite3"
                if old.exists():
                    with closing(sqlite3.connect(old.resolve().as_uri() + "?mode=ro", uri=True)) as previous:
                        count, last = previous.execute("SELECT attempts,last_at FROM budget").fetchone()
                    for index in range(count):
                        db.execute("INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?)",
                                   (f"legacy-query-{index}", "legacy", "search", str(index), last, day_at(last)))
                old = root / "reports/local_browser_details_source_v1/source.sqlite3"
                if old.exists():
                    with closing(sqlite3.connect(old.resolve().as_uri() + "?mode=ro", uri=True)) as previous:
                        rows = previous.execute("SELECT identity,due_at FROM details").fetchall()
                    for identity, due in rows:
                        db.execute("INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?)",
                                   ("legacy-detail-" + identity, "legacy", "detail", identity, due, day_at(due)))
                db.execute("INSERT INTO meta VALUES ('legacy_seeded','true')")
            db.commit()

    def halt(self, reason: str):
        with closing(self.connect()) as db:
            db.execute("INSERT OR IGNORE INTO meta VALUES ('halt',?)", (reason,))
            db.commit()

    def stopped(self) -> bool:
        if (self.folder / "STOP").exists():
            return True
        with closing(self.connect()) as db:
            return bool(db.execute("SELECT 1 FROM meta WHERE key='halt'").fetchone())

    def reserve(self, identity: str, *, now: float, spacing: float = 5, kind: str = "detail", query: str | None = None) -> dict:
        if kind not in {"search", "detail"}:
            raise ValueError("Unknown source action kind")
        if self.stopped():
            return {"state": "stopped", "delay": 0}
        event_id = (self.campaign + ":" + identity) if kind == "search" else "detail:" + identity
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT 1 FROM meta WHERE key='halt'").fetchone():
                db.rollback()
                return {"state": "stopped", "delay": 0}
            from .browser_registry import query_key, remember_query
            if kind == "search" and query and self.config.get("global_query_dedup"):
                if db.execute("SELECT 1 FROM browser_queries WHERE query_key=?", (query_key(query),)).fetchone():
                    db.rollback()
                    return {"state": "already_queried", "delay": 0}
            if db.execute("SELECT 1 FROM events WHERE id=? OR (kind='detail' AND identity=? AND ?='detail')",
                          (event_id, identity, kind)).fetchone():
                db.rollback()
                return {"state": "already_reserved", "delay": 0}
            cap = self.config["max_searches" if kind == "search" else "max_details"]
            count = db.execute("SELECT count(*) FROM events WHERE campaign=? AND kind=?", (self.campaign, kind)).fetchone()[0]
            if count >= cap:
                db.rollback()
                return {"state": "budget_exhausted", "delay": 0}
            live = self.config["mode"] == "live"
            last = db.execute("SELECT coalesce(max(due),0) FROM events").fetchone()[0]
            last_search = db.execute("SELECT coalesce(max(due),0) FROM events WHERE kind='search'").fetchone()[0]
            due = max(now, last + 5, last_search + 30 if kind == "search" else 0) if live else now
            day = day_at(due)
            daily_cap = self.config["daily_searches" if kind == "search" else "daily_details"]
            daily = db.execute("SELECT count(*) FROM events WHERE kind=? AND day=?", (kind, day)).fetchone()[0]
            if daily >= daily_cap:
                db.rollback()
                return {"state": "daily_limit", "delay": 0, "next_at": tomorrow(due)}
            db.execute("INSERT INTO events VALUES (?,?,?,?,?,?)", (event_id, self.campaign, kind, identity, due, day))
            if kind == "search" and query:
                remember_query(db, query, self.campaign)
            db.commit()
        # Wait here so stop/halt is checked before allowing the reader to navigate.
        while time.time() < due:
            if self.stopped():
                return {"state": "stopped", "delay": 0, "consumed": True}
            time.sleep(min(.2, max(0, due - time.time())))
        return {"state": "stopped" if self.stopped() else "reserved", "delay": 0, "consumed": True}

    def apply_cache(self, records: list[dict]):
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            for record in records:
                identity = record["source_identity"]
                if record.get("website"):
                    db.execute("INSERT OR REPLACE INTO cache VALUES (?,?,?)", (identity, json.dumps(record["website"]), time.time()))
                elif record.get("detail_reservation") == "already_reserved":
                    old = db.execute("SELECT website,observed FROM cache WHERE identity=?", (identity,)).fetchone()
                    if old:
                        record["website"] = json.loads(old[0])
                        record.pop("website_issue", None)
                        record["website_discovery"] = "cached_same_source_identity"
                        record["cached_observation_unix"] = old[1]
                if record.get("website"):
                    from .browser_registry import remember
                    record.update(remember(db, record["website"], identity, self.campaign, record.get("label", "")))
            db.commit()


def worker(root_text: str, folder_text: str, source_text: str, watch: bool):
    root, folder = Path(root_text), Path(folder_text)
    config, campaign = configuration(folder)
    queue, source = Queue(folder, config, campaign), Source(Path(source_text), folder, config, campaign)
    owner = uuid.uuid4().hex
    reader = None
    try:
        while not source.stopped():
            job = queue.claim(owner)
            if not job:
                if not watch:
                    break
                if reader:
                    reader.__exit__(None, None, None)
                    reader = None
                time.sleep(.5)
                continue
            started = time.monotonic()
            if reader is None:
                reader_class = DetailsReader
                if config.get("reader_version") == "ready-v3":
                    from .browser_details_ready import ReadyDetailsReader
                    reader_class = ReadyDetailsReader
                candidate_reader = reader_class(config["mode"], channel="msedge", headed=config.get("headed", False),
                    diagnostic_path=folder / f"diagnostics-{owner}.json", detail_budget=source)
                candidate_reader.__enter__()
                reader = candidate_reader
            reservation = source.reserve(job["id"], now=time.time(), kind="search", query=json.loads(job["payload"])["query"])
            if reservation["state"] == "already_queried":
                queue.finish(job["id"], owner, {"status": "skipped_known_query"}, time.monotonic() - started)
                continue
            if reservation["state"] == "daily_limit":
                queue.defer(job["id"], owner, reservation["next_at"])
                if not watch:
                    break
                time.sleep(.5)
                continue
            if reservation["state"] == "stopped":
                if reservation.get("consumed"):
                    queue.finish(job["id"], owner, {"status": "skipped_reserved_before_navigation"}, time.monotonic() - started)
                else:
                    queue.defer(job["id"], owner, time.time())
                break
            if reservation["state"] != "reserved":
                source.halt("search_" + reservation["state"])
                queue.finish(job["id"], owner, {"status": "interrupted"}, time.monotonic() - started)
                break
            reader.diagnostic_path = folder / f"diagnostics-{job['ordinal']:02d}-{owner}.json"
            outcome = reader.read(json.loads(job["payload"]), job["ordinal"], config["cards_per_search"], config["task_timeout_seconds"])
            if outcome["status"] in {"blocked", "layout_changed", "time_limit", "interrupted"}:
                source.halt(outcome["status"])
            source.apply_cache(outcome.get("records", []))
            queue.finish(job["id"], owner, outcome, time.monotonic() - started)
    except BaseException:
        source.halt("worker_exception_uncertain_attempt")
        raise
    finally:
        if reader:
            reader.__exit__(None, None, None)


def snapshot(folder: Path, source_path: Path) -> dict:
    config, campaign = configuration(folder)
    queue = Queue(folder, config, campaign)
    if not queue.path.exists() or not source_path.exists():
        return {"mode": config["mode"], "state": "prepared", "tasks": len(config["tasks"])}
    with closing(queue.connect()) as db:
        if db.execute("SELECT value FROM meta WHERE key='config_hash'").fetchone()[0] != campaign:
            raise ValueError("Service config changed after initialization")
        jobs = [dict(r) for r in db.execute("SELECT id,status,started,actual FROM jobs ORDER BY ordinal")]
        rows = [json.loads(r[0]) for r in db.execute("SELECT payload FROM observations")]
    sites = {}
    for row in rows:
        website = row.get("website")
        if not website:
            continue
        key = website["site_key"]
        if key not in sites:
            sites[key] = {**website, "support": "pending_review", "http_availability": "not_checked",
                          "occurrences": 0, "source_identities": [],
                          "domain_absent_from_baseline": website["registrable_domain"] not in config["baseline"]["domains"]}
        sites[key]["occurrences"] += 1
        if row["source_identity"] not in sites[key]["source_identities"]:
            sites[key]["source_identities"].append(row["source_identity"])
    with closing(Source(source_path, folder, config, campaign).connect()) as db:
        counts = {kind: db.execute("SELECT count(*) FROM events WHERE campaign=? AND kind=?", (campaign, kind)).fetchone()[0]
                  for kind in ("search", "detail")}
        daily = {kind: db.execute("SELECT count(*) FROM events WHERE kind=? AND day=?", (kind, day_at(time.time()))).fetchone()[0]
                 for kind in ("search", "detail")}
        halt = db.execute("SELECT value FROM meta WHERE key='halt'").fetchone()
    pending = any(j["status"] in {"queued", "running", "deferred"} for j in jobs)
    return {"mode": config["mode"], "campaign": campaign, "jobs": jobs, "halt": halt[0] if halt else None,
            "state": "halted" if halt else "stopped" if (folder / "STOP").exists() else "pending" if pending else "batch_complete",
            "source_reservations": counts, "selected_cards": len(rows), "site_keys": len(sites),
            "daily_source_reservations": daily, "configured_workers": config["workers"],
            "domains": len({s["registrable_domain"] for s in sites.values()}),
            "new_candidate_domains": len({s["registrable_domain"] for s in sites.values() if s["domain_absent_from_baseline"]}),
            "supported_new_domains": 0, "review_seconds": 0, "review_state": "not_performed",
            "coverage": "sampled tasks only; no geographic completeness", "review_queue": list(sites.values())}


def supervise(root: Path, folder: Path, *, watch: bool, enable_live: bool = False) -> dict:
    config, campaign = configuration(folder)
    live = config["mode"] == "live"
    if live and not enable_live:
        raise ValueError("Live service requires --enable-live-browser")
    source_path = root / "reports/local_browser_service_v1/source.sqlite3" if live else folder / "source.sqlite3"
    lock = WorkerLock(root / "reports/local_browser_source_v1/source.lock") if live else nullcontext()
    with WorkerLock(folder / ".supervisor.lock"), lock:
        queue = Queue(folder, config, campaign)
        queue.initialize()
        source = Source(source_path, folder, config, campaign)
        source.initialize(root if live else None)
        with closing(queue.connect()) as db:
            if db.execute("SELECT 1 FROM jobs WHERE status='running'").fetchone():
                db.execute("UPDATE jobs SET status='interrupted' WHERE status='running'")
                db.commit()
                source.halt("restart_with_uncertain_job")
            pending = bool(db.execute("SELECT 1 FROM jobs WHERE status IN ('queued','running','deferred')").fetchone())
        if (not watch and not pending) or source.stopped():
            return publish(folder, snapshot(folder, source_path))
        context = mp.get_context("spawn")
        processes = []
        shutdown_deadline = None
        try:
            if not source.stopped():
                for _ in range(config["workers"]):
                    process = context.Process(target=worker, args=(str(root), str(folder), str(source_path), watch))
                    process.start()
                    processes.append(process)
            while any(p.is_alive() for p in processes):
                if any(p.exitcode not in {None, 0} for p in processes):
                    source.halt("worker_exit")
                with closing(queue.connect()) as db:
                    stale = db.execute("SELECT 1 FROM jobs WHERE status='running' AND started<?",
                                       (time.time() - config["max_task_wall_seconds"],)).fetchone()
                if stale:
                    source.halt("worker_wall_time_exceeded")
                    (folder / "STOP").touch()
                    shutdown_deadline = time.monotonic() + 5
                    break
                if source.stopped():
                    shutdown_deadline = shutdown_deadline or time.monotonic() + 65
                    if time.monotonic() >= shutdown_deadline:
                        break
                write_json(folder / "heartbeat.json", {"at_unix": time.time(), "workers_alive": sum(p.is_alive() for p in processes),
                                                        "watch": watch, "stopped": source.stopped()})
                publish(folder, snapshot(folder, source_path))
                time.sleep(.5)
        except KeyboardInterrupt:
            (folder / "STOP").touch()
        except Exception:
            source.halt("supervisor_exception")
            (folder / "STOP").touch()
            raise
        finally:
            shutdown_deadline = shutdown_deadline or time.monotonic() + 65
            for process in processes:
                process.join(timeout=max(0, shutdown_deadline - time.monotonic()))
                if process.is_alive():
                    source.halt("worker_required_termination")
                    process.terminate()
                    process.join(timeout=5)
            write_json(folder / "heartbeat.json", {"at_unix": time.time(), "workers_alive": 0,
                                                    "watch": watch, "stopped": source.stopped(), "supervisor_exited": True})
        result = snapshot(folder, source_path)
        return publish(folder, result)


def publish(folder: Path, result: dict) -> dict:
    write_json(folder / "progress.json", {k: v for k, v in result.items() if k != "review_queue"})
    write_json(folder / "review-queue.json", {"mode": result["mode"], "sites": result.get("review_queue", [])})
    return result
