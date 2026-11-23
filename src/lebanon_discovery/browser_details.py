"""Bounded v2 reader: unique results feed and selected-place website actions."""
from __future__ import annotations

import html
import re
import time
from pathlib import Path
from urllib.parse import urlsplit

from .browser_trial import BrowserReader, KIND, WorkerLock, listing_record, own_database, write_json
from .browser_trial_diagnostics import page_location, safe_error_code


class DetailBudget:
    """Three lifetime detail reservations for this separately bounded local pilot."""

    def __init__(self, path: Path):
        self.path = path

    def reserve(self, identity: str, *, now: float, spacing: float = 5) -> dict:
        with WorkerLock(self.path.with_suffix(".lock")):
            db = own_database(self.path, KIND + ":details-v1")
            try:
                db.execute("CREATE TABLE IF NOT EXISTS details(identity TEXT PRIMARY KEY,due_at REAL)")
                db.commit()
                db.execute("BEGIN IMMEDIATE")
                if db.execute("SELECT 1 FROM details WHERE identity=?", (identity,)).fetchone():
                    db.rollback()
                    return {"state": "already_reserved", "delay": 0}
                count, last = db.execute("SELECT count(*),coalesce(max(due_at),0) FROM details").fetchone()
                if count >= 3:
                    db.rollback()
                    return {"state": "budget_exhausted", "delay": 0}
                due = max(now, last + spacing)
                db.execute("INSERT INTO details VALUES (?,?)", (identity, due))
                db.commit()
                return {"state": "reserved", "delay": max(0, due - now)}
            finally:
                db.close()


def fixture_search(task: dict, index: int) -> str:
    slug = re.sub(r"[^a-z0-9]", "", task["governorate"].lower())
    cards = "".join(f'<div role="article"><a aria-label="Synthetic {rank}" '
                    f'href="https://www.google.com/maps/place/{slug}-fixture-{index}-{rank}">Entity</a></div>'
                    for rank in range(4))
    return '<!doctype html><main role="feed" aria-label="Results">' + cards + '</main>'


def fixture_detail(label: str, rank: int, task: dict) -> str:
    slug = re.sub(r"[^a-z0-9]", "", task["governorate"].lower())
    url = f"https://{slug}{rank}.blogspot.com/" if rank < 2 else "https://chain.example.com/"
    return (f'<h1>{html.escape(label)}</h1><a data-item-id="authority" aria-label="Website" href="{url}">Website</a>'
            '<a href="https://irrelevant.example.net/">Unrelated link</a>')


def valid_place_link(url: str) -> bool:
    try:
        parts = urlsplit(url)
        return (parts.scheme == "https" and parts.hostname in {"google.com", "www.google.com"}
                and parts.port in {None, 443} and not parts.username and not parts.password
                and parts.path.startswith("/maps/"))
    except ValueError:
        return False


class DetailsReader(BrowserReader):
    def __init__(self, *args, diagnostic_path: Path, detail_budget: DetailBudget | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.diagnostic_path = diagnostic_path
        self.detail_budget = detail_budget

    def read(self, task: dict, index: int, limit: int, timeout: int) -> dict:
        from playwright.sync_api import Error, TimeoutError as BrowserTimeout
        started = time.monotonic()
        deadline = started + timeout
        stage = "start"
        selected = []
        diagnostic = {"schema": 2, "mode": self.mode, "events": [], "details": [],
                      "headed": self.headed, "browser_version": self.browser.version,
                      "detail_navigation_reservations": 0, "detail_limit": min(3, limit)}

        def emit(name: str, **fields):
            nonlocal stage
            stage = name
            diagnostic.update(fields)
            diagnostic["stage"] = name
            diagnostic["events"].append({"stage": name, "elapsed_seconds": round(time.monotonic() - started, 3)})
            write_json(self.diagnostic_path, diagnostic)
            print("browser stage: " + name, flush=True)

        def remaining(cap: int = 8000):
            value = min(cap, int((deadline - time.monotonic()) * 1000))
            if value <= 0:
                raise BrowserTimeout("Bounded details deadline")
            return value

        def finish(status: str, **fields):
            diagnostic.update(fields)
            diagnostic.update(result=status, stage=stage, elapsed_seconds=round(time.monotonic() - started, 3),
                              selected_cards=len(selected), location=page_location(self.page.url))
            write_json(self.diagnostic_path, diagnostic)
            return {"status": status, "records": selected}

        def blocked():
            body = self.page.locator("body").inner_text(timeout=remaining()).lower()
            return (any(term in body for term in ("unusual traffic", "verify you are human", "access denied",
                                                  "before you continue to google", "complete the captcha"))
                    or self.page.locator('iframe[src*="recaptcha"],form[action*="/sorry/"]').count() > 0)

        try:
            emit("search_navigation")
            if self.mode == "fixture":
                self.page.set_content(fixture_search(task, index), timeout=remaining(), wait_until="load")
                response = None
            else:
                response = self.page.goto(task["maps_url"], timeout=remaining(15000), wait_until="commit")
            diagnostic["main_document_status"] = response.status if response else None
            if response and response.status in {401, 403, 429}:
                return finish("blocked", reason="main_document_access_status")
            self.page.wait_for_load_state("domcontentloaded", timeout=remaining(15000))
            if blocked():
                return finish("blocked", reason="search_challenge")
            emit("unique_results_feed")
            feeds = self.page.get_by_role("feed")
            feeds.first.wait_for(state="visible", timeout=remaining(15000))
            if feeds.count() != 1:
                return finish("layout_changed", reason="ambiguous_results_feed")
            articles = feeds.first.get_by_role("article")
            articles.first.wait_for(state="visible", timeout=remaining())
            count = articles.count()
            links = []
            emit("selected_cards", article_matches=count)
            for rank in range(min(limit, 3, count)):
                card = articles.nth(rank)
                listing = card.locator('a[href*="/maps/"][aria-label]').first
                if not listing.count():
                    return finish("layout_changed", reason="listing_link_missing")
                href = listing.get_attribute("href", timeout=remaining()) or ""
                if not valid_place_link(href):
                    return finish("layout_changed", reason="unexpected_listing_destination")
                name = listing.get_attribute("aria-label", timeout=remaining()) or ""
                website = card.locator('a[aria-label^="Visit "],a[data-item-id="authority"]').first
                url = website.get_attribute("href", timeout=remaining()) if website.count() else None
                selected.append(listing_record(name, href, url))
                links.append(href)
            attempted = set()
            for rank, (record, href) in enumerate(zip(selected, links)):
                if record["website"]:
                    record["website_discovery"] = "result_card"
                    continue
                identity = record["source_identity"]
                if identity in attempted:
                    record["detail_issue"] = "duplicate_source_identity"
                    continue
                attempted.add(identity)
                if self.mode == "live" and self.detail_budget is None:
                    return finish("layout_changed", reason="missing_live_detail_budget")
                reservation = (self.detail_budget.reserve(identity, now=time.time()) if self.detail_budget
                               else {"state": "reserved", "delay": 0})
                record["detail_reservation"] = reservation["state"]
                if reservation["state"] != "reserved":
                    record["detail_issue"] = reservation["state"]
                    continue
                diagnostic["detail_navigation_reservations"] += 1
                detail = {"rank": rank + 1, "state": "reserved"}
                diagnostic["details"].append(detail)
                emit(f"detail_{rank + 1}_reserved")
                if reservation["delay"]:
                    delay_ms = int(reservation["delay"] * 1000)
                    if delay_ms >= remaining(60000):
                        return finish("time_limit", reason="detail_pacing_exceeds_remaining_time")
                    self.page.wait_for_timeout(delay_ms)
                emit(f"detail_{rank + 1}_navigation")
                if self.mode == "fixture":
                    self.page.set_content(fixture_detail(record["label"], rank, task), timeout=remaining(), wait_until="load")
                    response = None
                else:
                    response = self.page.goto(href, timeout=remaining(), wait_until="commit")
                detail["document_status"] = response.status if response else None
                if response and response.status in {401, 403, 429}:
                    return finish("blocked", reason="detail_access_status")
                self.page.wait_for_load_state("domcontentloaded", timeout=remaining())
                if blocked():
                    return finish("blocked", reason="detail_challenge")
                emit(f"detail_{rank + 1}_identity")
                # Avoid assigning a previous/different place's website to this card.
                self.page.get_by_role("heading", name=record["label"], exact=True, level=1).first.wait_for(
                    state="visible", timeout=remaining())
                emit(f"detail_{rank + 1}_website")
                authority = self.page.locator('a[data-item-id="authority"]')
                fallback = self.page.get_by_role("link", name=re.compile(r"^(website\b|visit .*website)", re.I))
                if not authority.count() and not fallback.count():
                    try:
                        authority.first.wait_for(state="visible", timeout=remaining(3000))
                    except BrowserTimeout:
                        if time.monotonic() >= deadline:
                            raise
                        detail["website_action_wait_expired"] = True
                actions = authority if authority.count() else fallback
                urls = set()
                for action_index in range(min(actions.count(), 5)):
                    action = actions.nth(action_index)
                    if action.is_visible():
                        value = action.get_attribute("href", timeout=remaining())
                        if value:
                            urls.add(value)
                detail["website_action_matches"] = actions.count()
                if len(urls) == 1:
                    enriched = listing_record(record["label"], href, urls.pop())
                    record["website"] = enriched["website"]
                    if record["website"]:
                        record.pop("website_issue", None)
                        record["website_discovery"] = "selected_place_detail"
                        detail["state"] = "website_link_captured"
                    else:
                        record["website_issue"] = enriched.get("website_issue", "unsupported_website_action")
                        detail["state"] = "unsupported_website_action"
                else:
                    record["website_issue"] = "no_detail_website_action_matched" if not urls else "ambiguous_detail_website_actions"
                    detail["state"] = record["website_issue"]
                write_json(self.diagnostic_path, diagnostic)
            return finish("sampled_capped" if count >= limit else "sampled_partial")
        except BrowserTimeout:
            return finish("time_limit", reason="stage_timeout")
        except Error as error:
            return finish("layout_changed", reason="browser_error", error_code=safe_error_code(str(error)))
