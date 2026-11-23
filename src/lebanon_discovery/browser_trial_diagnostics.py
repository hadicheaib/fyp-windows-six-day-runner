"""A diagnostic reader for separately authorized bounded attempts.

Keeps the original frozen worker unchanged. Saves stages/counts/error codes,
not raw pages, screenshots, cookies, headers, console text or response bodies.
"""
from __future__ import annotations

import re
import time
from pathlib import Path
from urllib.parse import urlsplit

from .browser_trial import BrowserReader, fixture_html, listing_record, write_json


def safe_error_code(message: str) -> str:
    match = re.search(r"\bnet::ERR_[A-Z0-9_]+\b", message)
    return match.group(0) if match else "browser_error_without_network_code"


def page_location(url: str) -> dict:
    parts = urlsplit(url)
    return {"scheme": parts.scheme, "host": parts.hostname,
            "path_kind": "maps" if parts.path.startswith("/maps") else
                         "challenge" if parts.path.startswith("/sorry") else
                         "other"}


class DiagnosticReader(BrowserReader):
    def __init__(self, *args, diagnostic_path: Path, **kwargs):
        super().__init__(*args, **kwargs)
        self.diagnostic_path = diagnostic_path

    def read(self, task: dict, index: int, limit: int, timeout: int) -> dict:
        from playwright.sync_api import Error, TimeoutError as BrowserTimeout
        started = time.monotonic()
        deadline = started + timeout
        stage = "start"
        record = {"schema": 1, "mode": self.mode, "headed": self.headed,
                  "browser_version": self.browser.version, "events": [],
                  "navigation_failures": [], "stage": stage}

        def emit(name: str, **fields):
            nonlocal stage
            stage = name
            record.update(fields)
            record["stage"] = name
            event = {"stage": name, "elapsed_seconds": round(time.monotonic() - started, 3)}
            record["events"].append(event)
            write_json(self.diagnostic_path, record)
            print("browser stage: " + name, flush=True)

        def remaining(cap_ms: int = 15000):
            milliseconds = min(cap_ms, int((deadline - time.monotonic()) * 1000))
            if milliseconds <= 0:
                raise BrowserTimeout("Bounded diagnostic deadline")
            return milliseconds

        def failed(request):
            if request.is_navigation_request() and request.frame == self.page.main_frame:
                record["navigation_failures"].append(safe_error_code(request.failure or ""))

        def finish(status: str, records=None, **fields):
            record.update(fields)
            record["result"] = status
            record["failed_stage"] = stage if status in {"time_limit", "layout_changed", "blocked"} else None
            record["location"] = page_location(self.page.url)
            record["elapsed_seconds"] = round(time.monotonic() - started, 3)
            write_json(self.diagnostic_path, record)
            return {"status": status, "records": records or []}

        self.page.on("requestfailed", failed)
        selected = []
        try:
            emit("navigation_commit")
            if self.mode == "fixture":
                self.page.set_content(fixture_html(task, index), timeout=remaining(), wait_until="load")
                response = None
            else:
                # 'commit' distinguishes receiving a response from finishing DOM loading.
                response = self.page.goto(task["maps_url"], timeout=remaining(), wait_until="commit")
            emit("dom_content_loaded", main_document_status=response.status if response else None,
                 location=page_location(self.page.url))
            if response and response.status in {401, 403, 429}:
                return finish("blocked", reason="main_document_access_status")
            self.page.wait_for_load_state("domcontentloaded", timeout=remaining())
            emit("body")
            body = self.page.locator("body").inner_text(timeout=remaining()).lower()
            markers = [term for term in ("unusual traffic", "verify you are human", "access denied",
                                        "before you continue to google", "complete the captcha") if term in body]
            if markers or self.page.locator('iframe[src*="recaptcha"],form[action*="/sorry/"]').count():
                return finish("blocked", reason="visible_challenge_or_consent")
            emit("results_feed", exact_feed_matches=self.page.get_by_role(
                "feed", name="Results for " + task["query"], exact=True).count(),
                generic_feed_matches=self.page.get_by_role("feed").count())
            feeds = self.page.get_by_role("feed")
            if not feeds.count() and ("no results found" in body or "couldn't find" in body):
                return finish("empty", reason="explicit_empty_text")
            feeds.first.wait_for(state="visible", timeout=remaining())
            if feeds.count() != 1:
                return finish("layout_changed", reason="ambiguous_results_feeds")
            # Match the unique results feed even if Google shortens its accessible name.
            feed = feeds.first
            emit("result_cards", exact_feed_matches=self.page.get_by_role(
                "feed", name="Results for " + task["query"], exact=True).count(),
                generic_feed_matches=feeds.count(), role_article_matches=feed.get_by_role("article").count())
            articles = feed.get_by_role("article")
            if not articles.count():
                # Structural diagnostics are restricted to the first three listing anchors.
                anchors = feed.locator('a[href*="/maps/place/"][aria-label]')
                record["listing_anchor_count"] = anchors.count()
                record["first_listing_ancestor_shapes"] = []
                for rank in range(min(limit, anchors.count())):
                    record["first_listing_ancestor_shapes"].append(anchors.nth(rank).evaluate(
                        "el => {const out=[]; for(let p=el.parentElement;p && out.length<4;p=p.parentElement)"
                        "out.push({tag:p.tagName,role:p.getAttribute('role'),classes:[...p.classList].slice(0,5)});return out;}"))
                write_json(self.diagnostic_path, record)
            articles.first.wait_for(state="visible", timeout=remaining())
            emit("selected_card_links", role_article_matches=articles.count())
            for rank in range(min(limit, articles.count())):
                card = articles.nth(rank)
                listing = card.locator('a[href*="/maps/"][aria-label]').first
                if not listing.count():
                    return finish("layout_changed", selected, reason="selected_card_listing_link_missing")
                website = card.locator('a[aria-label^="Visit "]').first
                selected.append(listing_record(listing.get_attribute("aria-label", timeout=remaining()) or "",
                    listing.get_attribute("href", timeout=remaining()) or "",
                    website.get_attribute("href", timeout=remaining()) if website.count() else None))
            return finish("sampled_capped" if articles.count() >= limit else "sampled_partial", selected)
        except BrowserTimeout:
            return finish("time_limit", selected, reason="stage_timeout")
        except Error as error:
            return finish("layout_changed", selected, reason="browser_error", error_code=safe_error_code(str(error)))
        finally:
            self.page.remove_listener("requestfailed", failed)
