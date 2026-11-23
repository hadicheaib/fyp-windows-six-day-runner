"""Versioned detail reader: visible UI readiness, bounded independent stages.

Retains the earlier reader as historical code. A pending DOMContentLoaded event
must not prevent inspection of an already rendered, exactly identified place.
"""
from __future__ import annotations

import re
import time
from pathlib import Path

from .browser_trial import BrowserReader, listing_record, write_json
from .browser_trial_diagnostics import page_location, safe_error_code
from .browser_details import DetailBudget, fixture_search, fixture_detail, valid_place_link


class ReadyDetailsReader(BrowserReader):
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
        diagnostic = {"schema": 3, "readiness": "attached_body_visible_feed_exact_heading", "mode": self.mode, "events": [], "details": [],
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
            emit("search_body_ready")
            self.page.locator("body").wait_for(state="attached", timeout=remaining(15000))
            emit("search_challenge_check")
            if blocked():
                return finish("blocked", reason="search_challenge")
            emit("unique_results_feed")
            feeds = self.page.get_by_role("feed")
            feeds.first.wait_for(state="visible", timeout=remaining(30000))
            if feeds.count() != 1:
                return finish("layout_changed", reason="ambiguous_results_feed")
            if blocked():
                return finish("blocked", reason="search_challenge_after_feed")
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
                    response = self.page.goto(href, timeout=remaining(15000), wait_until="commit")
                detail["document_status"] = response.status if response else None
                if response and response.status in {401, 403, 429}:
                    return finish("blocked", reason="detail_access_status")
                emit(f"detail_{rank + 1}_body_ready")
                self.page.locator("body").wait_for(state="attached", timeout=remaining(15000))
                emit(f"detail_{rank + 1}_challenge_check")
                if blocked():
                    return finish("blocked", reason="detail_challenge")
                emit(f"detail_{rank + 1}_identity")
                # Avoid assigning a previous/different place's website to this card.
                self.page.get_by_role("heading", name=record["label"], exact=True, level=1).first.wait_for(
                    state="visible", timeout=remaining(30000))
                if blocked():
                    return finish("blocked", reason="detail_challenge_after_identity")
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
