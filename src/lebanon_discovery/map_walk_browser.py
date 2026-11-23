"""Map-position browser adapter with a visible colored-marker heuristic.

Reads screenshots and public selected-place controls. No text search, hidden
endpoint, CAPTCHA bypass, or claim that every rendered/entity label is found.
"""
from __future__ import annotations

import hashlib
import html
import json
import math
import re
import time
from pathlib import Path
from urllib.parse import urlencode, urlsplit, urlunsplit

from .browser_trial import BrowserReader
from .browser_trial import write_json
from .map_walk import canonical, key

DETECTOR = "colored-marker-v1"


def map_url(camera):
    return "https://www.google.com/maps/@?" + urlencode({"api": "1", "map_action": "map",
        "center": f'{camera["latitude"]},{camera["longitude"]}', "zoom": camera["zoom"], "basemap": "roadmap"})


def observed_camera(url):
    match = re.search(r"/maps/@(-?[\d.]+),(-?[\d.]+),(\d+(?:\.\d+)?)z", url)
    if not match:
        raise ValueError("Observed map URL has no verifiable camera")
    return {"latitude": float(match[1]), "longitude": float(match[2]), "zoom": float(match[3])}


def place_identity(url):
    parts = urlsplit(url)
    if parts.scheme != "https" or parts.hostname not in {"www.google.com", "google.com"} or not parts.path.startswith("/maps/place/"):
        return None
    feature = re.search(r"!1s(0x[0-9a-f]+:0x[0-9a-f]+)", parts.path, re.I)
    if feature:
        return "maps_feature_id:" + feature[1].lower()
    # Public URL fallback, explicitly not a verified Google Place ID.
    path = re.sub(r"/@[^/]+", "", parts.path)
    return "place_url_fingerprint:" + hashlib.sha256(path.encode()).hexdigest()


def detect_markers(png: bytes, roi, *, max_targets=60):
    """Find compact saturated icon components, not text or all map businesses."""
    import cv2
    import numpy as np
    image = cv2.imdecode(np.frombuffer(png, dtype=np.uint8), cv2.IMREAD_COLOR)
    if image is None:
        raise ValueError("Invalid screenshot")
    x, y, w, h = roi
    if x < 0 or y < 0 or w <= 0 or h <= 0 or x + w > image.shape[1] or y + h > image.shape[0]:
        raise ValueError("Screenshot ROI is outside the fixed viewport")
    hsv = cv2.cvtColor(image[y:y+h, x:x+w], cv2.COLOR_BGR2HSV)
    if float(image[y:y+h, x:x+w].std()) < 1.5:
        raise ValueError("Map image appears unpainted; refusing to call it inspected")
    mask = cv2.inRange(hsv, np.array([0, 95, 125]), np.array([179, 255, 255]))
    count, labels, stats, centers = cv2.connectedComponentsWithStats(mask, 8)
    found = []
    for i in range(1, count):
        left, top, width, height, area = map(int, stats[i])
        if (not 9 <= width <= 36 or not 9 <= height <= 36 or not 80 <= area <= 800
                or not .55 <= width / height <= 1.8 or area / (width * height) < .25):
            continue
        cx, cy = round(float(centers[i][0]) + x, 2), round(float(centers[i][1]) + y, 2)
        if any(math.hypot(cx - p["x"], cy - p["y"]) < 14 for p in found):
            continue
        found.append({"x": cx, "y": cy, "detector": DETECTOR, "component_area": area})
    found.sort(key=lambda p: (round(p["y"] / 8), p["x"]))
    if len(found) > max_targets:
        raise ValueError("Too many marker candidates; view stays unresolved")
    return found


def match_target(saved, current, *, tolerance=9):
    candidates = [p for p in current if math.hypot(p["x"] - saved["x"], p["y"] - saved["y"]) <= tolerance]
    if len(candidates) != 1:
        raise ValueError("Saved target changed or became ambiguous; refusing stale pixel click")
    return candidates[0]


def fixture_view(view):
    """Authored UI, synthetic domains; no Google page or saved Maps content."""
    region = view["payload"]["governorate"]
    specs = [
        ("shared-chain", "Synthetic shared chain", "https://www.chain.example.com/" + region.replace(" ", "-"), 420, 280, "#f88b42"),
        ("tenant-" + region, "Synthetic hosted tenant", "https://" + re.sub(r"[^a-z]", "", region.lower()) + ".blogspot.com/", 710, 380, "#b45ee8"),
        ("no-site-" + region, "Synthetic no website", "", 930, 520, "#ee6cbd"),
    ]
    buttons = []
    for identity, label, url, x, y, color in specs:
        record = {"identity": "fixture_place:" + identity, "label": label, "website": url or None}
        payload = html.escape(json.dumps(record), quote=True)
        buttons.append(f'<button class="marker" data-record="{payload}" style="left:{x-11}px;top:{y-11}px;background:{color}" onclick="pick(this)">+</button>')
    return f'''<!doctype html><html><head><style>
        body{{margin:0;background:#e4e5e3;font:16px Arial}} #map{{position:absolute;inset:0}}
        .marker{{position:absolute;border:2px solid white;border-radius:50%;color:white;width:24px;height:24px;padding:0;font-weight:bold;visibility:hidden;animation:paint 0s linear .15s forwards}}
        @keyframes paint{{to{{visibility:visible}}}}
        #detail{{position:absolute;left:0;top:0;background:white;padding:22px;max-width:340px}}
        </style></head><body><div id="map" role="application" aria-label="خريطة · Synthetic fixture" aria-hidden="true" tabindex="0">{''.join(buttons)}</div><aside id="detail" hidden></aside>
        <script>function pick(b){{let p=JSON.parse(b.dataset.record);let d=document.getElementById('detail');d.hidden=false;d.replaceChildren();
        let h=document.createElement('h1');h.textContent=p.label;d.append(h);d.dataset.identity=p.identity;
        if(p.website){{let a=document.createElement('a');a.dataset.itemId='authority';a.setAttribute('aria-label','Website');a.href=p.website;a.textContent='Website';d.append(a);}}}}</script></body></html>'''


class MapReader(BrowserReader):
    def __init__(self, mode, folder: Path, *, timeout=35, headed=True):
        super().__init__(mode, channel="msedge", headed=headed)
        self.folder, self.timeout = folder, timeout
        self.stage = "created"
        self.diagnostic = {"mode": mode, "events": []}

    def emit(self, stage, **fields):
        self.stage = stage
        self.diagnostic.update(stage=stage, **fields)
        self.diagnostic["events"].append({"stage": stage, "at": time.time()})
        write_json(self.folder / "diagnostics.json", self.diagnostic)

    def failure_diagnostics(self):
        flags = {"stage": self.stage, "map_application_matches": None, "dom_map_matches": None, "access_stop_observed": None}
        try:
            flags["map_application_matches"] = self.page.get_by_role("application", name=re.compile(r"^Map\b", re.I)).count()
            flags["dom_map_matches"] = self.page.locator('[role="application"][aria-label^="Map"]').count()
            flags["all_dom_applications"] = self.page.locator('[role="application"]').count()
            flags["canvas_elements"] = self.page.locator('canvas').count()
            flags["rendered_map_rectangles"] = self.page.locator('[role="application"],canvas').evaluate_all("els => els.slice(0,12).map(e=>{let r=e.getBoundingClientRect();return {tag:e.tagName,x:r.x,y:r.y,width:r.width,height:r.height};})")
            flags["url_has_camera"] = bool(re.search(r"/maps/@-?[\d.]+,-?[\d.]+,[\d.]+z", self.page.url))
            # Public UI failure phrases only, never persist the account/body text.
            body = self.page.locator("body").inner_text(timeout=3000).lower()
            flags["public_error_signals"] = [s for s in ("update your browser", "can't connect", "enable javascript", "something went wrong", "no internet connection", "try again") if s in body]
            if self.mode == "live" and not re.search(r"[\w.+-]+@[\w.-]+\.[a-z]{2,}", body, re.I):
                # Small central UI diagnostic only; excludes browser chrome, the
                # account controls and side rail. Never a full-page Maps archive.
                shot = self.page.screenshot(clip={"x": 160, "y": 140, "width": 960, "height": 520}, timeout=3000)
                (self.folder / "readiness-roi.png").write_bytes(shot)
                flags["diagnostic_roi"] = "readiness-roi.png"
            self.check_block()
            flags["access_stop_observed"] = False
        except PermissionError:
            flags["access_stop_observed"] = True
        except Exception:
            pass
        self.emit("stopped", failure=flags)
        return flags

    def map_rect(self):
        # A visible full-page map application or render canvas is structural
        # evidence; the label/language/accessibility exposure can differ.
        expression = """([w,h]) => Array.from(document.querySelectorAll('[role="application"],canvas')).some(e=>{
            let r=e.getBoundingClientRect(),s=getComputedStyle(e);return s.visibility!=='hidden'&&s.display!=='none'&&
            Math.abs(r.x)<2&&Math.abs(r.y)<2&&Math.abs(r.width-w)<2&&Math.abs(r.height-h)<2;})"""
        self.page.wait_for_function(expression, arg=[1280, 800], timeout=self.left())
        rects = self.page.locator('[role="application"],canvas').evaluate_all("els=>els.map(e=>{let r=e.getBoundingClientRect();return {x:r.x,y:r.y,width:r.width,height:r.height};})")
        matches = [r for r in rects if all(abs(r[k]-v)<2 for k,v in {"x":0,"y":0,"width":1280,"height":800}.items())]
        if not matches:
            raise ValueError("No verified full-viewport map surface")
        return matches[0]

    def left(self, cap=None):
        remaining = getattr(self, "deadline", time.monotonic() + self.timeout) - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Map batch deadline")
        return max(1, min(int(remaining * 1000), (cap or self.timeout) * 1000))

    def __enter__(self):
        super().__enter__()
        self.page.set_viewport_size({"width": 1280, "height": 800})
        return self

    def check_block(self):
        # Inspect transiently; never save account details/body text in diagnostics.
        body = self.page.locator("body").inner_text(timeout=self.left(5)).lower()
        parts = urlsplit(self.page.url)
        signals = ("unusual traffic", "verify you are human", "complete the captcha", "access denied", "before you continue to google",
                   "حركة مرور غير معتادة", "تحقق من أنك", "قبل المتابعة إلى google", "قبل الانتقال إلى google",
                   "trafic inhabituel", "avant de continuer vers google", "vérifiez que vous êtes")
        if (parts.hostname == "consent.google.com" or parts.path.startswith("/sorry/")
                or any(s in body for s in signals)
                or self.page.locator('iframe[src*="recaptcha"],form[action*="/sorry/"]').count()):
            raise PermissionError("Map challenge/consent/access stop")

    def restore(self, view, *, navigation_number):
        cam = view["payload"]["camera"]
        self.emit("camera_navigation", navigation_number=navigation_number)
        if self.mode == "fixture":
            self.page.set_content(fixture_view(view), wait_until="load", timeout=self.left())
            map_area = None
            current = {k: cam[k] for k in ("latitude", "longitude", "zoom")}
        else:
            response = self.page.goto(map_url(cam), wait_until="commit", timeout=self.left())
            self.emit("camera_document", main_document_status=response.status if response else None)
            if response and response.status in {401, 403, 429}:
                raise PermissionError("Map document access status")
            self.page.locator("body").wait_for(state="attached", timeout=self.left(10))
            self.check_block()
            self.emit("map_application_ready")
            # The visible canvas can be aria-hidden in a standalone browser.
            # CSS visibility + exact full-viewport geometry are checked below.
            self.map_rect()
            self.check_block()
            # Known transient map introduction, not an account/permission action.
            later = self.page.get_by_role("button", name="Maybe later", exact=True)
            if later.count() == 1 and later.is_visible():
                later.click(timeout=self.left(3))
            self.emit("camera_url_ready")
            self.page.wait_for_function("/\\/maps\\/@-?[\\d.]+,-?[\\d.]+,[\\d.]+z/.test(location.pathname)", timeout=self.left())
            current = observed_camera(self.page.url)
            if (abs(current["latitude"] - cam["latitude"]) > .00002 or abs(current["longitude"] - cam["longitude"]) > .00002 or current["zoom"] != cam["zoom"]):
                raise ValueError("Requested map camera did not restore")
        rect = self.map_rect()
        self.emit("map_geometry", map_rect=rect)
        if not rect or any(abs(rect[k] - v) > 2 for k, v in {"x": 0, "y": 0, "width": 1280, "height": 800}.items()):
            raise ValueError("Map layout changed; saved pixel targets would be unsafe")
        # First paint/labels can lag the camera URL and canvas layout. Require
        # consecutive stable candidates after a minimum settling window, with
        # a strict snapshot count/deadline instead of accepting an early blank.
        previous, stable, settled = None, 0, False
        settling_started = time.monotonic()
        minimum = 3 if self.mode == "live" else .35
        for attempt in range(12):
            png = self.page.screenshot(timeout=self.left())
            try:
                targets = detect_markers(png, cam["roi"])
            except ValueError as error:
                self.emit("marker_detection", capture=attempt + 1, detector_issue=str(error))
                previous, stable = None, 0
            else:
                same = previous is not None and len(previous) == len(targets) and all(any(math.hypot(p["x"]-q["x"], p["y"]-q["y"]) < 3 for q in targets) for p in previous)
                stable = stable + 1 if same else 0
                self.emit("marker_detection", capture=attempt + 1, candidates=len(targets), stable=stable)
                if stable >= 2 and time.monotonic() - settling_started >= minimum:
                    settled = True
                    break
                previous = targets
            self.page.wait_for_timeout(min(self.left(), 700 if self.mode == "live" else 50))
        if not settled:
            raise ValueError("Visible markers did not settle within the bounded capture")
        directory = self.folder / "screenshots"
        directory.mkdir(exist_ok=True)
        # Do not save Google screenshots (can include account UI). Fixture shots are safe.
        screenshot = None
        if self.mode == "fixture":
            screenshot = directory / f'{key(view["id"])[:16]}-{navigation_number}.png'
            screenshot.write_bytes(png)
        self.current_targets = targets
        self.emit("markers_saved", detected_targets=len(targets), screenshot_sha256=hashlib.sha256(png).hexdigest())
        return {"camera": current, "map_rect": rect, "roi": cam["roi"], "screenshot_sha256": hashlib.sha256(png).hexdigest(),
                "fixture_screenshot": str(screenshot) if screenshot else None,
                "detector": DETECTOR, "targets": len(targets), "coverage": "Colored icons detected in central ROI only"}, targets

    def inspect(self, saved, cache):
        self.emit("match_saved_marker")
        current = match_target(saved, self.current_targets)
        self.emit("marker_click")
        self.left()
        self.page.mouse.click(current["x"], current["y"])
        if self.mode == "fixture":
            heading = self.page.get_by_role("heading", level=1)
            heading.wait_for(state="visible", timeout=self.left())
            identity = self.page.locator("#detail").get_attribute("data-identity")
        else:
            self.check_block()
            try:
                self.page.wait_for_url(re.compile(r"https://(?:www\.)?google\.com/maps/place/"), timeout=self.left(7))
            except Exception:
                self.check_block()
                return {"status": "not_place", "reason": "detected_color_did_not_open_a_place"}
            identity = place_identity(self.page.url)
            if not identity:
                return {"status": "unresolved", "reason": "selected_place_identity_missing"}
            heading = self.page.get_by_role("heading", level=1)
            heading.first.wait_for(state="visible", timeout=self.left())
        if heading.count() != 1:
            return {"status": "unresolved", "reason": "ambiguous_selected_place_heading"}
        label = heading.inner_text().strip()[:200]
        self.emit("selected_place_heading")
        known = cache(identity)
        if known:
            return {"status": "cached", "identity": identity, "label": label, "website": known.get("website"),
                    "cache_note": "Same observed identity in this pass; opening the marker was still required"}
        actions = self.page.locator('a[data-item-id="authority"]')
        try:
            actions.first.wait_for(state="visible", timeout=min(self.left(2), 2000 if self.mode == "live" else 100))
        except Exception:
            pass
        self.check_block()
        urls = {actions.nth(i).get_attribute("href") for i in range(min(actions.count(), 5)) if actions.nth(i).is_visible()}
        urls.discard(None)
        if len(urls) > 1:
            return {"status": "unresolved", "identity": identity, "label": label, "reason": "ambiguous_website_actions"}
        return {"status": "captured" if urls else "no_website", "identity": identity, "label": label,
                "website": next(iter(urls), None), "identity_note": "Observed public feature ID/fingerprint, not asserted Place ID"}
