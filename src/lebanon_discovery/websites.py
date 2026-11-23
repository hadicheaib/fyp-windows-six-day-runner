from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

import tldextract


# A pinned bundled PSL snapshot avoids a network request during parsing.
EXTRACT = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None, include_psl_private_domains=False)
SOCIAL = {"facebook.com", "instagram.com", "linkedin.com", "x.com", "twitter.com", "tiktok.com", "t.me", "telegram.me"}
DIRECTORY = {"tripadvisor.com", "yelp.com", "yellowpages.com", "booking.com", "google.com"}
PATH_HOSTS = {"sites.google.com": 2, "notion.site": 1}


@dataclass(frozen=True)
class NormalizedWebsite:
    original_url: str
    canonical_url: str
    hostname: str
    registrable_domain: str
    site_key: str
    classification: str


def normalize_website(value: str) -> NormalizedWebsite:
    original = value.strip()
    if not original:
        raise ValueError("empty website URL")
    candidate = original if "://" in original else "https://" + original
    parts = urlsplit(candidate)
    if parts.scheme.lower() not in {"http", "https"} or not parts.hostname:
        raise ValueError("only public HTTP(S) URLs are supported")
    if parts.username or parts.password:
        raise ValueError("URL credentials are not allowed")
    hostname = parts.hostname.rstrip(".").encode("idna").decode("ascii").lower()
    if not hostname or len(hostname) > 253:
        raise ValueError("invalid hostname")
    try:
        port = parts.port
    except ValueError as exc:
        raise ValueError("invalid port") from exc
    extension = EXTRACT(hostname)
    if not extension.domain or not extension.suffix:
        raise ValueError("hostname has no public suffix")
    domain = f"{extension.domain}.{extension.suffix}"
    explicit_port = port if port and not ((parts.scheme.lower() == "http" and port == 80) or (parts.scheme.lower() == "https" and port == 443)) else None
    netloc = hostname + (f":{explicit_port}" if explicit_port else "")
    canonical = urlunsplit((parts.scheme.lower(), netloc, parts.path or "/", parts.query, ""))
    classification = "social" if domain in SOCIAL else "directory" if domain in DIRECTORY else "candidate"
    site_host = hostname.removeprefix("www.")
    site_key = site_host + (f":{explicit_port}" if explicit_port else "")
    if hostname in PATH_HOSTS:
        sections = [section for section in parts.path.split("/") if section]
        site_key += "/" + "/".join(sections[: PATH_HOSTS[hostname]])
    return NormalizedWebsite(original, canonical, hostname, domain, site_key, classification)
