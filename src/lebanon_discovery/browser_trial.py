"""Single-worker, bounded browser experiment, isolated from the discovery DB.

The default fixture mode uses authored HTML, never Google's saved content.
No API, scrolling, automatic retry, website visit or evidence promotion occurs.
"""
from __future__ import annotations

import csv
import hashlib
import html
import json
import sqlite3
import time
from contextlib import AbstractContextManager, closing, nullcontext
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from .websites import normalize_website

APP_ID = 0x4C425457
KIND = "lebanon-bounded-browser-v1"
MANIFEST = "docs/google-maps-investigation/small-regional-trial-2026-10-15/trial-manifest.json"
BASELINE = "reports/lebanon_master_ror_2026-10-09.domains.csv"
TERMINAL_STOPS = {"blocked", "layout_changed", "interrupted", "time_limit"}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path: Path, value: object) -> None:
    data = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") == data:
        return
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(data, encoding="utf-8")
    temporary.replace(path)


class WorkerLock(AbstractContextManager):
    """OS lock: one writer, including after a process dies (not an NFS lock)."""

    def __init__(self, path: Path):
        self.path = path

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+b")
        if self.path.stat().st_size == 0:
            self.handle.write(b"0")
            self.handle.flush()
        self.handle.seek(0)
        try:
            if __import__("os").name == "nt":
                import msvcrt
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.handle.close()
            raise RuntimeError("Another worker holds this local lock") from None
        return self

    def __exit__(self, *args):
        self.handle.close()


def prepare(root: Path, run_dir: Path, mode: str = "fixture", scope_note: str = "") -> dict:
    """Create a new, explicit run configuration; never inherit historical status."""
    if mode not in {"fixture", "live"}:
        raise ValueError("mode must be fixture or live")
    if run_dir.exists() and any(run_dir.iterdir()):
        raise ValueError("Prepare requires a new or empty run directory")
    manifest_path = root / MANIFEST
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    keys = ("task_key", "cell_id", "governorate", "profile", "query", "maps_url")
    tasks = [{key: task[key] for key in keys} for task in manifest["tasks"]]
    baseline = {"domains": ["example.com", "blogspot.com"],
                "site_keys": ["chain.example.com", "tenant0.blogspot.com"], "kind": "synthetic"}
    if mode == "live":
        with (root / BASELINE).open(encoding="utf-8-sig", newline="") as stream:
            rows = list(csv.DictReader(stream))
        baseline = {"domains": sorted({r["normalized_domain"] for r in rows}),
                    "site_keys": sorted({key for r in rows for key in json.loads(r["site_keys"]) if key}),
                    "kind": "raw_candidates_not_verified", "path": BASELINE,
                    "sha256": digest(root / BASELINE)}
    config = {"kind": KIND, "schema": 1, "mode": mode, "scope_note": scope_note,
              "targets_sha256": digest(manifest_path), "grid_release": manifest["grid_release"],
              "max_searches": 9, "cards_per_search": 3, "task_timeout_seconds": 60 if mode == "live" else 10,
              "work_budget_seconds": 900, "minimum_spacing_seconds": 30 if mode == "live" else 0,
              "baseline": baseline, "tasks": tasks}
    validate_config(config)
    run_dir.mkdir(parents=True, exist_ok=True)
    write_json(run_dir / "config.json", config)
    return config


def validate_config(config: dict) -> None:
    if config.get("kind") != KIND or config.get("schema") != 1 or config.get("mode") not in {"fixture", "live"}:
        raise ValueError("Unrecognized browser experiment configuration")
    for key, ceiling in (("max_searches", 9), ("cards_per_search", 3),
                         ("task_timeout_seconds", 60), ("work_budget_seconds", 900)):
        value = config.get(key)
        if type(value) is not int or not 1 <= value <= ceiling:
            raise ValueError(f"Invalid bounded setting: {key}")
    spacing = config.get("minimum_spacing_seconds")
    if type(spacing) is not int or not 0 <= spacing <= 60 or (config["mode"] == "live" and spacing < 30):
        raise ValueError("Live spacing must be at least 30 seconds")
    tasks = config["tasks"]
    if not 1 <= len(tasks) <= 9 or len({t["task_key"] for t in tasks}) != len(tasks):
        raise ValueError("Need one to nine distinct fixed tasks")
    for task in tasks:
        url = urlsplit(task["maps_url"])
        if (url.scheme != "https" or url.netloc != "www.google.com" or url.path != "/maps/search/"
                or parse_qs(url.query) != {"api": ["1"], "query": [task["query"]]}):
            raise ValueError("Only the fixed public Maps search URL form is accepted")


def own_database(path: Path, kind: str) -> sqlite3.Connection:
    """Refuse unrelated databases before a writable connection is opened."""
    existed = path.exists()
    if existed:
        try:
            with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as check:
                if check.execute("PRAGMA application_id").fetchone()[0] != APP_ID:
                    raise ValueError("Refusing an unrelated database")
                if check.execute("SELECT value FROM meta WHERE key='kind'").fetchone()[0] != kind:
                    raise ValueError("Wrong browser ledger type")
        except sqlite3.Error:
            raise ValueError("Refusing an unrecognized database") from None
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    if not existed:
        connection.execute(f"PRAGMA application_id={APP_ID}")
        connection.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        connection.execute("INSERT INTO meta VALUES ('kind', ?)", (kind,))
        connection.commit()
    return connection


class SourceBudget:
    """One shared local workspace budget, held under a source-wide WorkerLock."""

    def __init__(self, path: Path):
        self.db = own_database(path, KIND + ":source")
        self.db.execute("CREATE TABLE IF NOT EXISTS budget (id INTEGER PRIMARY KEY, attempts INTEGER, last_at REAL)")
        self.db.execute("INSERT OR IGNORE INTO budget VALUES (1, 0, 0)")
        self.db.commit()

    def delay(self, now: float, spacing: float) -> float:
        attempts, last = self.db.execute("SELECT attempts,last_at FROM budget").fetchone()
        if attempts >= 9:
            raise RuntimeError("Shared nine-search budget exhausted; creating another run does not reset it")
        return max(0, last + spacing - now)

    def reserve(self, now: float, spacing: float) -> None:
        if self.delay(now, spacing) > 0:
            raise RuntimeError("Shared pacing reservation is too early")
        self.db.execute("UPDATE budget SET attempts=attempts+1,last_at=? WHERE id=1", (now,))
        self.db.commit()


class RunStore:
    def __init__(self, run_dir: Path):
        self.directory = run_dir
        config_path = run_dir / "config.json"
        self.config = json.loads(config_path.read_text(encoding="utf-8"))
        validate_config(self.config)
        self.db = own_database(run_dir / "session.sqlite3", KIND)
        fingerprint = digest(config_path)
        old = self.db.execute("SELECT value FROM meta WHERE key='config_sha256'").fetchone()
        if old and old[0] != fingerprint:
            self.db.close()
            raise ValueError("Configuration changed after initialization; use a distinct run")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                task_key TEXT PRIMARY KEY, ordinal INTEGER, payload TEXT, status TEXT DEFAULT 'queued',
                attempts INTEGER DEFAULT 0, charge REAL DEFAULT 0, actual REAL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS observations (
                task_key TEXT, rank INTEGER, payload TEXT, PRIMARY KEY(task_key,rank));
        """)
        self.db.execute("INSERT OR IGNORE INTO meta VALUES ('config_sha256', ?)", (fingerprint,))
        for index, task in enumerate(self.config["tasks"]):
            self.db.execute("INSERT OR IGNORE INTO jobs(task_key,ordinal,payload) VALUES (?,?,?)",
                            (task["task_key"], index, json.dumps(task)))
        # With the OS lock held, a reserved task can only belong to a dead worker.
        interrupted = self.db.execute("UPDATE jobs SET status='interrupted' WHERE status='reserved'").rowcount
        if interrupted:
            self.halt("interrupted")
        self.db.commit()

    def halt(self, reason: str) -> None:
        self.db.execute("INSERT OR REPLACE INTO meta VALUES ('halt', ?)", (reason,))
        self.db.commit()

    def pending(self):
        if self.db.execute("SELECT 1 FROM meta WHERE key='halt'").fetchone():
            return None
        return self.db.execute("SELECT * FROM jobs WHERE status='queued' ORDER BY ordinal LIMIT 1").fetchone()

    def reserve(self, task_key: str, allowance: float) -> bool:
        self.db.execute("BEGIN IMMEDIATE")
        totals = self.db.execute("SELECT sum(attempts),sum(charge) FROM jobs").fetchone()
        if totals[0] >= self.config["max_searches"] or totals[1] + allowance > self.config["work_budget_seconds"]:
            self.db.rollback()
            self.halt("budget_exhausted")
            return False
        changed = self.db.execute("UPDATE jobs SET status='reserved',attempts=1,charge=? "
                                  "WHERE task_key=? AND status='queued'", (allowance, task_key)).rowcount
        self.db.commit()
        return bool(changed)

    def finish(self, task_key: str, outcome: dict, actual: float) -> None:
        records = outcome.get("records", [])
        if len(records) > self.config["cards_per_search"]:
            raise ValueError("Reader exceeded selected-card ceiling")
        status = outcome["status"]
        if status not in TERMINAL_STOPS | {"sampled_capped", "sampled_partial", "empty"}:
            raise ValueError("Unknown task outcome")
        with self.db:
            # Keep the conservative reserved charge (including pacing) after success too.
            self.db.execute("UPDATE jobs SET status=?,actual=? WHERE task_key=? AND status='reserved'",
                            (status, actual, task_key))
            for rank, record in enumerate(records, 1):
                self.db.execute("INSERT INTO observations VALUES (?,?,?)", (task_key, rank, json.dumps(record)))
        if status in TERMINAL_STOPS:
            self.halt(status)


def listing_record(name: str, listing_href: str, website_href: str | None) -> dict:
    parts = urlsplit(listing_href)
    place_id = parse_qs(parts.query).get("query_place_id", [None])[0]
    if place_id and (parts.scheme != "https" or parts.hostname not in {"google.com", "www.google.com"}):
        place_id = None
    # Fingerprint is explicitly not a Place ID or proof of separate entities.
    identity = "place_id:" + place_id if place_id else "link_fingerprint:" + hashlib.sha256(
        (parts.netloc + parts.path).encode()).hexdigest()
    result = {"label": name[:200], "source_identity": identity, "place_id": place_id,
              "support": "pending_review", "http_availability": "not_checked", "website": None}
    if website_href:
        try:
            normalized = normalize_website(website_href[:2048])
            result["website"] = {key: getattr(normalized, key) for key in
                                 ("canonical_url", "registrable_domain", "site_key", "classification")}
        except (ValueError, UnicodeError):
            result["website_issue"] = "invalid_or_unsupported_url"
    else:
        result["website_issue"] = "no_visible_website_link"
    return result


def fixture_html(task: dict, index: int) -> str:
    """Entirely authored synthetic content; URLs are never followed."""
    urls = ["https://chain.example.com/branch/" + str(index),
            f"https://tenant{index}.blogspot.com/", None if index % 2 == 0 else f"https://alpha{index}.example.org/",
            "https://must-not-inspect.example.net/"]
    cards = []
    for rank, url in enumerate(urls):
        link = f'<a aria-label="Visit synthetic website" href="{url}">Website</a>' if url else ""
        cards.append(f'<div role="article"><a aria-label="Synthetic entity {index}-{rank}" '
                     f'href="https://www.google.com/maps/search/?query_place_id=fixture-{index}-{rank}">Entity</a>{link}</div>')
    return ('<!doctype html><html lang="en"><title>Synthetic worker fixture</title>'
            '<img src="https://must-not-load.example.com/pixel" alt="Network guard test">'
            f'<main role="feed" aria-label="Results for {html.escape(task["query"], quote=True)}">'
            + "".join(cards) + "</main></html>")


class BrowserReader(AbstractContextManager):
    def __init__(self, mode: str, channel: str | None = None, headed: bool = False):
        self.mode, self.channel, self.headed = mode, channel, headed
        self.blocked_network_requests = 0

    def __enter__(self):
        from playwright.sync_api import sync_playwright
        self.playwright = sync_playwright().start()
        try:
            self.browser = self.playwright.chromium.launch(channel=self.channel, headless=not self.headed,
                timeout=30000, chromium_sandbox=getattr(self, "chromium_sandbox", False),
                args=["--disable-background-networking", "--disable-component-update", "--no-first-run"])
            self.context = self.browser.new_context(locale="en-US", service_workers="block")
            if self.mode == "fixture":
                self.context.route("**/*", self._abort)
                self.context.route_web_socket("**/*", lambda socket: socket.close())
            self.page = self.context.new_page()
            return self
        except BaseException:
            if hasattr(self, "browser"):
                self.browser.close()
            self.playwright.stop()
            raise

    def _abort(self, route):
        self.blocked_network_requests += 1
        route.abort()

    def __exit__(self, *args):
        try:
            self.context.close()
            self.browser.close()
        finally:
            self.playwright.stop()

    def read(self, task: dict, index: int, limit: int, timeout: int) -> dict:
        from playwright.sync_api import TimeoutError as BrowserTimeout
        deadline = time.monotonic() + timeout

        def remaining():
            milliseconds = int((deadline - time.monotonic()) * 1000)
            if milliseconds <= 0:
                raise BrowserTimeout("Task deadline")
            return milliseconds

        try:
            if self.mode == "fixture":
                self.page.set_content(fixture_html(task, index), timeout=remaining(), wait_until="load")
            else:
                self.page.goto(task["maps_url"], timeout=remaining(), wait_until="domcontentloaded")
            text = self.page.locator("body").inner_text(timeout=remaining()).lower()
            if (any(term in text for term in ("unusual traffic", "verify you are human", "access denied",
                                             "before you continue to google", "complete the captcha"))
                    or self.page.locator('iframe[src*="recaptcha"], form[action*="/sorry/"]').count()):
                return {"status": "blocked"}
            feed = self.page.get_by_role("feed", name="Results for " + task["query"], exact=True)
            if not feed.count() and ("no results found" in text or "couldn't find" in text):
                return {"status": "empty"}
            feed.wait_for(state="visible", timeout=remaining())
            articles = feed.get_by_role("article")
            articles.first.wait_for(state="visible", timeout=remaining())
            count = articles.count()
            records = []
            for rank in range(min(count, limit)):
                card = articles.nth(rank)
                listing = card.locator('a[href*="/maps/"][aria-label]').first
                if not listing.count():
                    return {"status": "layout_changed", "records": records}
                website = card.locator('a[aria-label^="Visit "]').first
                records.append(listing_record(listing.get_attribute("aria-label", timeout=remaining()) or "",
                    listing.get_attribute("href", timeout=remaining()) or "",
                    website.get_attribute("href", timeout=remaining()) if website.count() else None))
            return {"status": "sampled_capped" if count >= limit else "sampled_partial", "records": records}
        except BrowserTimeout:
            return {"status": "layout_changed" if time.monotonic() < deadline else "time_limit"}


def report(run_dir: Path) -> dict:
    """Read-only status; no browser import, calls, queue claims or DB creation."""
    path = run_dir / "session.sqlite3"
    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    validate_config(config)
    if not path.exists():
        return {"mode": config["mode"], "state": "prepared", "attempts_reserved": 0}
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        if db.execute("PRAGMA application_id").fetchone()[0] != APP_ID:
            raise ValueError("Refusing unrelated status database")
        if db.execute("SELECT value FROM meta WHERE key='config_sha256'").fetchone()[0] != digest(run_dir / "config.json"):
            raise ValueError("Configuration changed after initialization")
        jobs = list(db.execute("SELECT task_key,status,attempts,charge,actual FROM jobs ORDER BY ordinal"))
        occurrences = [{**json.loads(row["payload"]), "task_key": row["task_key"], "rank": row["rank"]}
                       for row in db.execute("SELECT * FROM observations ORDER BY task_key,rank")]
        halt = db.execute("SELECT value FROM meta WHERE key='halt'").fetchone()
        network = db.execute("SELECT value FROM meta WHERE key='fixture_requests_blocked'").fetchone()
    baseline = config["baseline"]
    sites = {}
    for record in occurrences:
        site = record.get("website")
        if not site:
            continue
        key = site["site_key"]
        if key not in sites:
            sites[key] = {**site, "source_identities": [], "listing_occurrences": 0, "observations": [],
                          "domain_absent_from_raw_baseline": site["registrable_domain"] not in baseline["domains"],
                          "site_key_absent_from_raw_baseline": key not in baseline["site_keys"],
                          "support": "pending_review", "http_availability": "not_checked"}
        sites[key]["listing_occurrences"] += 1
        sites[key]["observations"].append({field: record[field] for field in ("label", "source_identity", "task_key", "rank")})
        if record["source_identity"] not in sites[key]["source_identities"]:
            sites[key]["source_identities"].append(record["source_identity"])
    finished = all(j["status"] != "queued" for j in jobs)
    state = ("recovery_required" if any(j["status"] == "reserved" for j in jobs) else
             "finished_with_gaps" if finished and any(j["status"] == "interrupted" for j in jobs) else
             "complete" if finished else "ready")
    return {"kind": KIND, "mode": config["mode"],
            "state": halt[0] if halt else state,
            "attempts_reserved": sum(j["attempts"] for j in jobs), "selected_cards": len(occurrences),
            "website_link_occurrences": sum(bool(r.get("website")) for r in occurrences),
            "unique_source_identities": len({r["source_identity"] for r in occurrences}),
            "unique_site_keys": len(sites), "unique_domains": len({s["registrable_domain"] for s in sites.values()}),
            "candidate_domains_absent_from_baseline": len({s["registrable_domain"] for s in sites.values()
                                                          if s["domain_absent_from_raw_baseline"]}),
            "supported_new_domains": 0, "review_effort_seconds": 0,
            "evidence_review": "not_performed", "work_budget_charged_seconds": sum(j["charge"] for j in jobs),
            "measured_task_seconds": round(sum(j["actual"] for j in jobs), 3),
            "fixture_page_requests_blocked": int(network[0]) if network else 0,
            "geographic_coverage": "not_measured; sampled named-locality results are not cell completion",
            "jobs": [dict(j) for j in jobs], "review_queue": list(sites.values())}


def run(root: Path, run_dir: Path, *, enable_live: bool = False, channel: str | None = None,
        headed: bool = False, reader_factory=BrowserReader, clock=time.time, sleep=time.sleep) -> dict:
    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    validate_config(config)
    if config["mode"] == "live" and (not enable_live or not config["scope_note"].strip()):
        raise ValueError("Live mode requires --enable-live-browser and a distinct bounded scope_note; no run started")
    source = None
    with WorkerLock(run_dir / ".worker.lock"):
        store = RunStore(run_dir)
        try:
            if store.pending() is None:
                return save_reports(run_dir, report(run_dir))
            source_path = root / "reports/local_browser_source_v1/source.sqlite3"
            lock = WorkerLock(source_path.with_suffix(".lock")) if config["mode"] == "live" else nullcontext()
            with lock:
                if config["mode"] == "live":
                    source = SourceBudget(source_path)
                with reader_factory(config["mode"], channel=channel, headed=headed) as reader:
                    while (job := store.pending()) is not None:
                        task = json.loads(job["payload"])
                        try:
                            delay = source.delay(clock(), config["minimum_spacing_seconds"]) if source else 0
                        except RuntimeError:
                            store.halt("source_budget_exhausted")
                            break
                        allowance = config["task_timeout_seconds"] + delay
                        if not store.reserve(task["task_key"], allowance):
                            break
                        started = time.monotonic()
                        try:
                            if delay:
                                sleep(delay)
                            if source:
                                source.reserve(clock(), config["minimum_spacing_seconds"])
                            outcome = reader.read(task, job["ordinal"], config["cards_per_search"], config["task_timeout_seconds"])
                            store.finish(task["task_key"], outcome, time.monotonic() - started)
                        except BaseException:
                            store.finish(task["task_key"], {"status": "interrupted"}, time.monotonic() - started)
                            raise
                    if config["mode"] == "fixture":
                        store.db.execute("INSERT OR REPLACE INTO meta VALUES ('fixture_requests_blocked', ?)",
                                         (str(getattr(reader, "blocked_network_requests", 0)),))
                        store.db.commit()
        finally:
            store.db.close()
            if source:
                source.db.close()
    return save_reports(run_dir, report(run_dir))


def save_reports(run_dir: Path, result: dict) -> dict:
    """Reconstruct missing/stale working snapshots without repeating acquisition."""
    write_json(run_dir / "progress.json", {key: value for key, value in result.items() if key != "review_queue"})
    write_json(run_dir / "review-queue.json", {"mode": result["mode"], "state": "unreviewed_candidates_only",
                                             "sites": result["review_queue"]})
    return result


def acknowledge_interruption(run_dir: Path) -> None:
    """Skip uncertain reserved searches permanently, then permit untouched jobs."""
    with WorkerLock(run_dir / ".worker.lock"):
        store = RunStore(run_dir)
        try:
            row = store.db.execute("SELECT value FROM meta WHERE key='halt'").fetchone()
            if not row or row[0] != "interrupted":
                raise ValueError("Only interrupted sessions can be acknowledged; blocked tasks are never retried")
            store.db.execute("DELETE FROM meta WHERE key='halt'")
            store.db.commit()
        finally:
            store.db.close()
