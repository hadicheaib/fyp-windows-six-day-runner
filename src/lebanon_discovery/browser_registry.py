"""Candidate-only cross-run browser index, stored in the shared source ledger."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from contextlib import closing
from pathlib import Path

from .websites import normalize_website


def query_key(query: str) -> str:
    return " ".join(query.casefold().split())


def initialize(db):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS browser_sites(
            site_key TEXT PRIMARY KEY,domain TEXT,website TEXT,first_indexed REAL,last_indexed REAL);
        CREATE INDEX IF NOT EXISTS browser_site_domains ON browser_sites(domain);
        CREATE TABLE IF NOT EXISTS browser_urls(url TEXT PRIMARY KEY,site_key TEXT);
        CREATE TABLE IF NOT EXISTS browser_associations(
            identity TEXT,site_key TEXT,origin TEXT,label TEXT,PRIMARY KEY(identity,site_key,origin));
        CREATE TABLE IF NOT EXISTS browser_queries(
            query_key TEXT PRIMARY KEY,query_text TEXT,campaign TEXT,indexed REAL);
        CREATE TABLE IF NOT EXISTS browser_seeded(path TEXT PRIMARY KEY,sha256 TEXT);
    """)


def remember(db, website: dict, identity: str, origin: str, label: str = "") -> dict:
    normalized = normalize_website(website["canonical_url"])
    site = {key: getattr(normalized, key) for key in
            ("canonical_url", "registrable_domain", "site_key", "classification")}
    existed = bool(db.execute("SELECT 1 FROM browser_sites WHERE site_key=?", (site["site_key"],)).fetchone())
    domain_existed = bool(db.execute("SELECT 1 FROM browser_sites WHERE domain=?", (site["registrable_domain"],)).fetchone())
    now = time.time()
    db.execute("INSERT OR IGNORE INTO browser_sites VALUES (?,?,?,?,?)",
               (site["site_key"], site["registrable_domain"], json.dumps(site), now, now))
    db.execute("UPDATE browser_sites SET last_indexed=? WHERE site_key=?", (now, site["site_key"]))
    db.execute("INSERT OR IGNORE INTO browser_urls VALUES (?,?)", (site["canonical_url"], site["site_key"]))
    db.execute("INSERT OR IGNORE INTO browser_associations VALUES (?,?,?,?)", (identity, site["site_key"], origin, label[:200]))
    return {"site_seen_in_browser_index": existed, "domain_seen_in_browser_index": domain_existed}


def remember_query(db, query: str, campaign: str):
    db.execute("INSERT OR IGNORE INTO browser_queries VALUES (?,?,?,?)", (query_key(query), query, campaign, time.time()))


def seed_frozen_history(root: Path, db):
    """Use hash-pinned live capture queues; never scan synthetic data as real."""
    if db.execute("SELECT 1 FROM meta WHERE key='browser_registry_history_v1'").fetchone():
        return
    checkpoint = root / "reviews/fyp_browser_ready_retry_checkpoint_2026-10-15.json"
    pins = json.loads(checkpoint.read_text(encoding="utf-8"))["artifacts"] if checkpoint.exists() else {}
    for name, pin in pins.items():
        if not name.endswith("/review-queue.json"):
            continue
        path = root / name
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("mode") != "live":
            continue
        if hashlib.sha256(path.read_bytes()).hexdigest() != pin["sha256"]:
            raise ValueError("Historical browser capture hash changed")
        for site in payload.get("sites", []):
            for identity in site.get("source_identities", []):
                remember(db, site, identity, name)
                # Exact identities can reuse these captured links without another detail visit.
                db.execute("INSERT OR IGNORE INTO cache VALUES (?,?,?)", (identity, json.dumps({k: site[k] for k in
                    ("canonical_url", "registrable_domain", "site_key", "classification")}), path.stat().st_mtime))
        db.execute("INSERT OR IGNORE INTO browser_seeded VALUES (?,?)", (name, pin["sha256"]))
        config = json.loads((path.parent / "config.json").read_text(encoding="utf-8"))
        for ledger in (path.parent / "queue.sqlite3", path.parent / "session.sqlite3"):
            if ledger.exists():
                with closing(sqlite3.connect(ledger.resolve().as_uri() + "?mode=ro", uri=True)) as previous:
                    for (task,) in previous.execute("SELECT payload FROM jobs WHERE status NOT IN ('queued','deferred')"):
                        remember_query(db, json.loads(task)["query"], name)
                break
    regional = "docs/google-maps-investigation/small-regional-trial-2026-10-15/observations.json"
    if regional in pins:
        path = root / regional
        assert hashlib.sha256(path.read_bytes()).hexdigest() == pins[regional]["sha256"]
        for region in json.loads(path.read_text(encoding="utf-8"))["regions"]:
            remember_query(db, region["query"], regional)
            for listing in region["listings"]:
                if listing.get("website_url"):
                    identity = "historical_card:" + hashlib.sha256((regional + region["query"] + str(listing["rank"])).encode()).hexdigest()
                    remember(db, {"canonical_url": listing["website_url"]}, identity, regional, listing["name"])
        db.execute("INSERT OR IGNORE INTO browser_seeded VALUES (?,?)", (regional, pins[regional]["sha256"]))
    db.execute("INSERT INTO meta VALUES ('browser_registry_history_v1','true')")


def export(db, baseline: dict) -> dict:
    sites = []
    for row in db.execute("SELECT * FROM browser_sites ORDER BY site_key"):
        sites.append({**json.loads(row["website"]), "first_indexed_unix": row["first_indexed"],
                      "last_indexed_unix": row["last_indexed"], "support": "candidate_index_not_a_review_ledger",
                      "http_availability": "not_checked",
                      "absent_from_raw_master": row["domain"] not in baseline["domains"]})
    return {"sites": sites, "site_keys": len(sites), "domains": len({s["registrable_domain"] for s in sites}),
            "queries": [dict(r) for r in db.execute("SELECT * FROM browser_queries ORDER BY query_key")],
            "associations": [dict(r) for r in db.execute("SELECT * FROM browser_associations ORDER BY site_key,origin,identity")],
            "scope": "browser candidate index; raw master checked separately; no support promotion"}
