"""Versioned map-view routes and durable, candidate-only governorate bookmarks.

No network/browser imports. One local writer; UI work is outside transactions.
"""
from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import time
from contextlib import closing
from pathlib import Path

from . import browser_registry
from .websites import normalize_website

APP_ID = 0x4C424D57
KIND = "lebanon-map-walk-v1"
DONE = {"inspected", "inspected_no_detected_targets"}
TARGET_DONE = {"captured", "no_website", "not_place", "cached"}
DEFAULT_LIMITS = {"max_searches": 9, "max_details": 27, "daily_searches": 10, "daily_details": 30}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def key(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def pixel(lon, lat, zoom):
    if not -180 <= lon <= 180 or not -85 <= lat <= 85:
        raise ValueError("Invalid map coordinates")
    size = 256 * 2 ** zoom
    return ((lon + 180) / 360 * size,
            (1 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2 * size)


def lonlat(x, y, zoom):
    size = 256 * 2 ** zoom
    return (x / size * 360 - 180, math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / size)))))


def camera(lat, lon, *, zoom=18, width=1280, height=800, inset_x=160, inset_y=140):
    if not all(math.isfinite(v) for v in (lat, lon)) or not 14 <= zoom <= 20:
        raise ValueError("Invalid camera")
    if width < 800 or height < 600 or not 0 < inset_x < width / 3 or not 0 < inset_y < height / 3:
        raise ValueError("Invalid fixed camera layout")
    lat, lon = round(lat, 8), round(lon, 8)
    x, y = pixel(lon, lat, zoom)
    roi = [inset_x, inset_y, width - 2 * inset_x, height - 2 * inset_y]
    west, north = lonlat(x - roi[2] / 2, y - roi[3] / 2, zoom)
    east, south = lonlat(x + roi[2] / 2, y + roi[3] / 2, zoom)
    return {"latitude": round(lat, 8), "longitude": round(lon, 8), "zoom": zoom,
            "width": width, "height": height, "roi": roi,
            "planned_roi_bbox": [west, south, east, north], "bearing": 0, "tilt": 0,
            "bounds_meaning": "Web Mercator approximation, not verified entity coverage"}


def validate_plan(plan):
    if plan.get("kind") != KIND or plan.get("mode") not in {"fixture", "live"} or plan.get("schema") != 1:
        raise ValueError("Unrecognized map-walk plan")
    if not plan.get("regions") or len(set(plan["regions"])) != len(plan["regions"]):
        raise ValueError("Need distinct governorates")
    if not plan.get("views") or len(plan["views"]) > 200000:
        raise ValueError("Invalid bounded route size")
    seen = set()
    for v in plan["views"]:
        if v["governorate"] not in plan["regions"] or v["view_id"] in seen:
            raise ValueError("Invalid viewport identity")
        seen.add(v["view_id"])
        c = v["camera"]
        expected = camera(c["latitude"], c["longitude"], zoom=c["zoom"], width=c["width"], height=c["height"],
                          inset_x=c["roi"][0], inset_y=c["roi"][1])
        if canonical(c) != canonical(expected):
            raise ValueError("Camera layout/bounds do not match the plan")
    if set(v["governorate"] for v in plan["views"]) != set(plan["regions"]):
        raise ValueError("Each governorate needs its own route")
    return key(plan)


def prepare_plan(root: Path, *, mode="fixture", extent="pilot", zoom=18, overlap=.2):
    """Compile independent geography only, not Google data.

    Pilot covers the nine frozen reference cells. National uses active leaf
    cell geometry, with boundary membership ambiguity explicitly retained.
    """
    from shapely.geometry import box, shape
    source = root / "reports/grid_pilot_v1_2026-10-15/grid-registry.json"
    raw = source.read_bytes()
    registry = json.loads(raw)
    pins = json.loads((root / "reviews/fyp_hamra_five_minute_checkpoint_2026-10-16.json").read_text(encoding="utf-8"))["artifacts"]
    pin = pins[source.relative_to(root).as_posix()]
    if hashlib.sha256(raw).hexdigest() != pin["sha256"]:
        raise ValueError("Independent grid hash changed")
    if extent not in {"pilot", "national"} or not .1 <= overlap <= .4:
        raise ValueError("Invalid extent/overlap")
    prototype = camera(33.89, 35.48, zoom=zoom)
    span_x, span_y = prototype["roi"][2:]
    step_x, step_y = span_x * (1 - overlap), span_y * (1 - overlap)
    parents = {c["parent_id"] for c in registry["cells"] if c["parent_id"]}
    pilot = {p["cell_id"] for p in registry["pilot_cells"]}
    cells = [c for c in registry["cells"] if c["cell_id"] in pilot] if extent == "pilot" else [c for c in registry["cells"] if c["cell_id"] not in parents]
    regions = registry["governorates"]
    tiles = {r: {} for r in regions}
    for cell in cells:
        geom = shape(cell["geometry"])
        west, south, east, north = geom.bounds
        xmin, ymin = pixel(west, north, zoom)
        xmax, ymax = pixel(east, south, zoom)
        for row in range(math.floor((ymin - span_y / 2) / step_y), math.ceil((ymax + span_y / 2) / step_y) + 1):
            for col in range(math.floor((xmin - span_x / 2) / step_x), math.ceil((xmax + span_x / 2) / step_x) + 1):
                lon, lat = lonlat(col * step_x, row * step_y, zoom)
                cam = camera(lat, lon, zoom=zoom)
                if not box(*cam["planned_roi_bbox"]).intersects(geom):
                    continue
                for region in cell["governorate_area_m2"]:
                    tiles[region][row, col] = cam
        if sum(map(len, tiles.values())) > 199000:
            raise ValueError("Route exceeds the explicit 200,000-view planning ceiling")
    views = []
    for region in regions:
        seed = next(p["locality"] for p in registry["pilot_cells"] if p["governorate"] == region)
        cameras = [camera(seed["latitude"], seed["longitude"], zoom=zoom)]
        # North-to-south rows, alternating direction. Seed is a first reference view.
        rows = sorted({r for r, c in tiles[region]})
        for number, row in enumerate(rows):
            cameras.extend(tiles[region][row, col] for col in sorted((c for r, c in tiles[region] if r == row), reverse=bool(number % 2)))
        seen = set()
        for cam in cameras:
            digest = key([registry["release_id"], extent, overlap, region, cam])[:24]
            if digest not in seen:
                views.append({"view_id": region + ":" + digest, "governorate": region,
                              "ordinal": len(seen), "camera": cam})
                seen.add(digest)
    plan = {"kind": KIND, "schema": 1, "mode": mode, "regions": regions, "views": views,
            "extent": extent, "overlap": overlap, "detector": "colored-marker-v1",
            "grid_release": registry["release_id"], "grid_sha256": hashlib.sha256(raw).hexdigest(),
            "boundary_status": registry["boundary_status"], "source_notices": registry["source_notices"],
            "scope": "Reference viewport pass. No exhaustive coverage or exact governorate membership claim."}
    validate_plan(plan)
    return plan


class Store:
    def __init__(self, path: Path):
        self.path = Path(path)
        with closing(sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
            if db.execute("PRAGMA application_id").fetchone()[0] != APP_ID:
                raise ValueError("Refusing unrelated database")
            if db.execute("SELECT value FROM meta WHERE key='kind'").fetchone()[0] != KIND:
                raise ValueError("Wrong map-walk ledger")

    def connect(self, *, write=False):
        uri = str(self.path) if write else self.path.resolve().as_uri() + "?mode=ro"
        db = sqlite3.connect(uri, uri=not write, timeout=10)
        db.row_factory = sqlite3.Row
        return db

    @classmethod
    def create(cls, path: Path, plan: dict, *, new_pass=False):
        digest = validate_plan(plan)
        path = Path(path)
        if path.exists():
            store = cls(path)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            with closing(sqlite3.connect(path)) as db:
                db.executescript(f"""
                    PRAGMA application_id={APP_ID};
                    CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                    CREATE TABLE plans(hash TEXT PRIMARY KEY,payload TEXT NOT NULL);
                    CREATE TABLE views(id TEXT PRIMARY KEY,plan TEXT,governorate TEXT,ordinal INTEGER,payload TEXT,
                        state TEXT DEFAULT 'pending',owner TEXT,camera TEXT,updated REAL);
                    CREATE INDEX route_order ON views(plan,governorate,ordinal);
                    CREATE TABLE targets(id TEXT PRIMARY KEY,view_id TEXT,ordinal INTEGER,payload TEXT,
                        state TEXT DEFAULT 'pending',result TEXT,attempts INTEGER DEFAULT 0);
                    CREATE TABLE place_cache(identity TEXT PRIMARY KEY,result TEXT NOT NULL);
                    CREATE TABLE outcomes(target_id TEXT,attempt INTEGER,result TEXT NOT NULL,at REAL,
                        PRIMARY KEY(target_id,attempt));
                    CREATE TABLE audit(id INTEGER PRIMARY KEY,at REAL,view_id TEXT,event TEXT,payload TEXT);
                """)
                db.execute("INSERT INTO meta VALUES ('kind',?)", (KIND,))
                db.execute("INSERT INTO meta VALUES ('mode',?)", (plan["mode"],))
                browser_registry.initialize(db)
                db.commit()
            store = cls(path)
        with closing(store.connect(write=True)) as db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT value FROM meta WHERE key='mode'").fetchone()[0] != plan["mode"]:
                raise ValueError("Cannot mix real and synthetic evidence")
            old = db.execute("SELECT value FROM meta WHERE key='active_plan'").fetchone()
            if old and old[0] != digest and not new_pass:
                raise ValueError("Plan changed; use an explicit new pass, preserving previous history")
            if old and old[0] != digest and db.execute("SELECT 1 FROM views WHERE owner IS NOT NULL").fetchone():
                raise ValueError("Cannot switch plan while a view is owned/uncertain")
            db.execute("INSERT OR IGNORE INTO plans VALUES (?,?)", (digest, canonical(plan)))
            for v in plan["views"]:
                # A new pass keeps the shared site index and old view rows.
                vid = digest[:16] + ":" + v["view_id"]
                db.execute("INSERT OR IGNORE INTO views(id,plan,governorate,ordinal,payload,updated) VALUES (?,?,?,?,?,?)",
                           (vid, digest, v["governorate"], v["ordinal"], canonical(v), time.time()))
            db.execute("INSERT OR REPLACE INTO meta VALUES ('active_plan',?)", (digest,))
            db.commit()
        return store

    def plan(self):
        with closing(self.connect()) as db:
            row = db.execute("SELECT hash,payload FROM plans WHERE hash=(SELECT value FROM meta WHERE key='active_plan')").fetchone()
            data = json.loads(row["payload"])
            if key(data) != row["hash"]:
                raise ValueError("Stored immutable route hash changed")
            return data

    def claim(self, region, owner):
        with closing(self.connect(write=True)) as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM views WHERE plan=(SELECT value FROM meta WHERE key='active_plan') AND governorate=? AND state NOT IN ('inspected','inspected_no_detected_targets') ORDER BY ordinal LIMIT 1", (region,)).fetchone()
            if row is None:
                if region not in self.plan()["regions"]:
                    raise ValueError("Unknown governorate")
                return None
            if row["owner"] is not None or row["state"] == "unresolved":
                raise ValueError("Uncertain view requires explicit recovery; never replay silently")
            payload = json.loads(row["payload"])
            expected = next((v for v in self.plan()["views"] if v["governorate"] == region and v["ordinal"] == row["ordinal"]), None)
            if payload != expected:
                raise ValueError("Stored viewport differs from immutable route")
            db.execute("UPDATE views SET state='working',owner=?,updated=? WHERE id=?", (owner, time.time(), row["id"]))
            db.commit()
            return {**dict(row), "owner": owner, "payload": json.loads(row["payload"]),
                    "camera": json.loads(row["camera"]) if row["camera"] else None}

    @staticmethod
    def guard(db, vid, owner):
        if not db.execute("SELECT 1 FROM views WHERE id=? AND owner=? AND state='working'", (vid, owner)).fetchone():
            raise ValueError("Stale worker or view state")

    def targets(self, vid):
        with closing(self.connect()) as db:
            return [{**dict(r), "payload": json.loads(r["payload"]), "result": json.loads(r["result"]) if r["result"] else None}
                    for r in db.execute("SELECT * FROM targets WHERE view_id=? ORDER BY ordinal", (vid,))]

    def save_view(self, vid, owner, observed, targets):
        with closing(self.connect(write=True)) as db:
            db.execute("BEGIN IMMEDIATE")
            self.guard(db, vid, owner)
            row = db.execute("SELECT camera FROM views WHERE id=?", (vid,)).fetchone()
            if row[0] is not None:
                raise ValueError("Do not replace the frozen unfinished target queue")
            if len(targets) > 100:
                raise ValueError("Marker detector exceeded bounded targets/view")
            db.execute("UPDATE views SET camera=?,updated=? WHERE id=?", (canonical(observed), time.time(), vid))
            for i, target in enumerate(targets):
                tid = key([vid, i, target])
                db.execute("INSERT INTO targets(id,view_id,ordinal,payload) VALUES (?,?,?,?)", (tid, vid, i, canonical(target)))
            db.commit()

    def begin_target(self, vid, owner, tid):
        with closing(self.connect(write=True)) as db:
            db.execute("BEGIN IMMEDIATE")
            self.guard(db, vid, owner)
            if db.execute("UPDATE targets SET state='started',attempts=attempts+1 WHERE id=? AND view_id=? AND state='pending'", (tid, vid)).rowcount != 1:
                raise ValueError("Target is not pending; uncertain visits require recovery")
            db.commit()

    def remember(self, vid, owner, tid, result):
        status = result.get("status")
        if status not in TARGET_DONE | {"unresolved"}:
            raise ValueError("Invalid place outcome")
        normalized = normalize_website(result["website"]) if result.get("website") else None
        if normalized and status not in {"captured", "cached"}:
            raise ValueError("Website outcome must be candidate/cached")
        if status in {"captured", "cached", "no_website"} and not result.get("identity"):
            raise ValueError("Entity outcome needs an observed identity")
        result = {**result, "observed_in_view": vid, "observed_unix": time.time()}
        with closing(self.connect(write=True)) as db:
            db.execute("BEGIN IMMEDIATE")
            self.guard(db, vid, owner)
            if not db.execute("SELECT 1 FROM targets WHERE id=? AND view_id=? AND state='started'", (tid, vid)).fetchone():
                raise ValueError("No started target for this result")
            if normalized:
                novelty = browser_registry.remember(db, {"canonical_url": normalized.canonical_url}, result["identity"], vid, result.get("label", ""))
                result = {**result, **novelty, "site_key": normalized.site_key, "domain": normalized.registrable_domain,
                          "review": "pending", "http_availability": "not_checked"}
            if result.get("identity") and status in {"captured", "no_website"}:
                db.execute("INSERT OR REPLACE INTO place_cache VALUES (?,?)", (result["identity"], canonical(result)))
            db.execute("UPDATE targets SET state=?,result=? WHERE id=?", (status, canonical(result), tid))
            attempt = db.execute("SELECT attempts FROM targets WHERE id=?", (tid,)).fetchone()[0]
            db.execute("INSERT INTO outcomes VALUES (?,?,?,?)", (tid, attempt, canonical(result), time.time()))
            db.commit()
        return result

    def cached(self, identity):
        with closing(self.connect()) as db:
            row = db.execute("SELECT result FROM place_cache WHERE identity=?", (identity,)).fetchone()
            result = json.loads(row[0]) if row else None
            active = db.execute("SELECT value FROM meta WHERE key='active_plan'").fetchone()[0]
            return result if result and result.get("observed_in_view", "").startswith(active[:16] + ":") else None

    def seed_known_sites(self, root: Path):
        """Explicit read-only import of pinned browser site identities, not verdicts."""
        if self.plan()["mode"] != "live":
            raise ValueError("Never mix real browser history into synthetic fixtures")
        name = "reports/hamra_five_minute_final_2026-10-16/known-websites-after.json"
        pins = json.loads((root / "reviews/fyp_hamra_five_minute_checkpoint_2026-10-16.json").read_text(encoding="utf-8"))["artifacts"]
        raw = (root / name).read_bytes()
        if hashlib.sha256(raw).hexdigest() != pins[name]["sha256"]:
            raise ValueError("Historical browser index hash differs")
        sites = json.loads(raw)["sites"]
        with closing(self.connect(write=True)) as db:
            db.execute("BEGIN IMMEDIATE")
            if not db.execute("SELECT 1 FROM meta WHERE key='known_sites_seeded'").fetchone():
                for site in sites:
                    browser_registry.remember(db, site, "pinned_site:" + site["site_key"], name)
                db.execute("INSERT INTO meta VALUES ('known_sites_seeded',?)", (hashlib.sha256(raw).hexdigest(),))
            db.commit()

    def source_limits(self):
        with closing(self.connect()) as db:
            row = db.execute("SELECT value FROM meta WHERE key='source_limits'").fetchone()
            return json.loads(row[0]) if row else dict(DEFAULT_LIMITS)

    def set_source_limits(self, limits, reason):
        """Explicit finite configuration, not a reset or source-rights grant."""
        if set(limits) != set(DEFAULT_LIMITS) or any(type(v) is not int or not 1 <= v <= 10000000 for v in limits.values()):
            raise ValueError("Every source limit must be an explicit positive finite integer")
        if len(reason.strip()) < 20:
            raise ValueError("Budget change needs a concrete scope reason")
        with closing(self.connect(write=True)) as db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT 1 FROM views WHERE owner IS NOT NULL").fetchone():
                raise ValueError("Cannot change limits while a view is owned/uncertain")
            old = db.execute("SELECT value FROM meta WHERE key='source_limits'").fetchone()
            before = json.loads(old[0]) if old else dict(DEFAULT_LIMITS)
            db.execute("INSERT OR REPLACE INTO meta VALUES ('source_limits',?)", (canonical(limits),))
            db.execute("INSERT INTO audit(at,view_id,event,payload) VALUES (?,NULL,'explicit_budget_change',?)",
                       (time.time(), canonical({"before": before, "after": limits, "reason": reason})))
            db.commit()

    def release(self, vid, owner, *, complete=False, reason="batch_limit"):
        with closing(self.connect(write=True)) as db:
            db.execute("BEGIN IMMEDIATE")
            self.guard(db, vid, owner)
            states = [r[0] for r in db.execute("SELECT state FROM targets WHERE view_id=?", (vid,))]
            if "started" in states:
                raise ValueError("Uncertain target cannot be silently paused/retried")
            if complete and (db.execute("SELECT camera FROM views WHERE id=?", (vid,)).fetchone()[0] is None or any(s not in TARGET_DONE for s in states)):
                raise ValueError("Unfinished/unresolved targets cannot mark a view inspected")
            state = ("inspected" if states else "inspected_no_detected_targets") if complete else "paused"
            if "unresolved" in states:
                state = "unresolved"
            db.execute("UPDATE views SET state=?,owner=NULL,updated=? WHERE id=?", (state, time.time(), vid))
            db.execute("INSERT INTO audit(at,view_id,event,payload) VALUES (?,?,?,?)", (time.time(), vid, state, canonical({"reason": reason})))
            db.commit()

    def recover(self, vid, reason):
        """Explicit operator acknowledgement: only uncertain targets become pending.

        Attempt counters and source reservations are not erased.
        """
        if len(reason.strip()) < 12:
            raise ValueError("Recovery needs a concrete inspection reason")
        with closing(self.connect(write=True)) as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT state FROM views WHERE id=?", (vid,)).fetchone()
            if not row or row[0] in DONE:
                raise ValueError("Cannot recover a completed/missing viewport")
            db.execute("UPDATE targets SET state='pending' WHERE view_id=? AND state IN ('started','unresolved')", (vid,))
            db.execute("UPDATE views SET state='paused',owner=NULL,updated=? WHERE id=?", (time.time(), vid))
            db.execute("INSERT INTO audit(at,view_id,event,payload) VALUES (?,?,?,?)", (time.time(), vid, "explicit_recovery", canonical({"reason": reason})))
            db.commit()

    def snapshot(self):
        with closing(self.connect()) as db:
            db.execute("BEGIN")
            active = db.execute("SELECT value FROM meta WHERE key='active_plan'").fetchone()[0]
            payload = json.loads(db.execute("SELECT payload FROM plans WHERE hash=?", (active,)).fetchone()[0])
            if key(payload) != active:
                raise ValueError("Stored immutable route hash changed")
            regions = {}
            for region in payload["regions"]:
                rows = db.execute("SELECT * FROM views WHERE plan=? AND governorate=? ORDER BY ordinal", (active, region)).fetchall()
                current = next((r for r in rows if r["state"] not in DONE), None)
                regions[region] = {"planned_views": len(rows), "inspected_views": sum(r["state"] in DONE for r in rows),
                                   "resume": None if current is None else {"view_id": current["id"], "state": current["state"],
                                       "camera": json.loads(current["payload"])["camera"],
                                       "observed": json.loads(current["camera"]) if current["camera"] else None,
                                       "targets": [{"id": t["id"], "state": t["state"], "payload": json.loads(t["payload"])} for t in db.execute("SELECT * FROM targets WHERE view_id=? ORDER BY ordinal", (current["id"],))]}}
            sites = browser_registry.export(db, {"domains": []})
            for site in sites["sites"]:
                site.pop("absent_from_raw_master", None)
            limits_row = db.execute("SELECT value FROM meta WHERE key='source_limits'").fetchone()
            limits = json.loads(limits_row[0]) if limits_row else dict(DEFAULT_LIMITS)
            return {"kind": KIND, "mode": payload["mode"], "plan_hash": active, "regions": regions,
                    "configured_source_limits": limits,
                    "index": sites, "coverage": "Only detected-marker viewport passes; exhaustive recall unmeasured",
                    "production_imports": 0}
