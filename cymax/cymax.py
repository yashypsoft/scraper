#!/usr/bin/env python3
import argparse
import csv
import html
import re
import sys
from collections import deque
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import unquote, urlparse
import xml.etree.ElementTree as ET

import requests

try:
    import yaml
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: PyYAML. Install it with: pip install pyyaml"
    ) from exc


DEFAULT_FLARESOLVERR = "http://localhost:8191/v1"


def normalize_site(site: str) -> str:
    site = site.strip()
    if not site:
        return ""
    if not site.startswith(("http://", "https://")):
        site = f"https://{site}"
    return site.rstrip("/")


def get_localname(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def fetch_with_flaresolverr(
    flaresolverr_url: str,
    target_url: str,
    timeout_ms: int = 120000,
    session: str = "cymax-sitemap-session",
) -> str:
    payload = {
        "cmd": "request.get",
        "url": target_url,
        "maxTimeout": timeout_ms,
        "session": session,
    }
    resp = requests.post(flaresolverr_url, json=payload, timeout=180)
    resp.raise_for_status()
    body = resp.json()
    if body.get("status") != "ok":
        message = body.get("message", "unknown FlareSolverr error")
        raise RuntimeError(f"FlareSolverr error for {target_url}: {message}")
    solution = body.get("solution", {})
    return solution.get("response", "") or ""


def extract_sitemaps_from_robots(robots_text: str) -> List[str]:
    # Accept plain-text robots and HTML-wrapped robots responses.
    pattern = re.compile(r"Sitemap:\s*([^\r\n]+)", flags=re.IGNORECASE)
    urls: List[str] = []
    for match in pattern.finditer(robots_text):
        raw = html.unescape(match.group(1).strip())
        decoded = unquote(raw)

        # Keep only the first URL and cut any appended wrappers/garbage.
        found = re.search(r"https?://\S+", decoded, flags=re.IGNORECASE)
        if not found:
            continue

        clean = found.group(0).split("<", 1)[0].strip().rstrip(".,;")
        xml_match = re.search(r"^(.+?\.xml(?:\.gz)?)", clean, flags=re.IGNORECASE)
        if xml_match:
            clean = xml_match.group(1)

        if clean:
            urls.append(clean)
    return urls


def maybe_unwrap_html_wrapped_text(content: str) -> str:
    text = content.strip().lstrip("\ufeff")
    lower = text.lower()

    if "<html" in lower and "<pre" in lower:
        pre_match = re.search(r"<pre[^>]*>(.*?)</pre>", text, flags=re.IGNORECASE | re.DOTALL)
        if pre_match:
            unwrapped = html.unescape(pre_match.group(1)).strip().lstrip("\ufeff")
            if unwrapped:
                return unwrapped

    if "<html" in lower and "xml-viewer-style" in lower:
        # Chromium XML viewer wraps XML in HTML. Pull the real XML document back out.
        patterns = [
            r"(<\?xml[^>]*\?>\s*<sitemapindex[\s\S]*?</sitemapindex>)",
            r"(<\?xml[^>]*\?>\s*<urlset[\s\S]*?</urlset>)",
            r"(<sitemapindex[\s\S]*?</sitemapindex>)",
            r"(<urlset[\s\S]*?</urlset>)",
        ]
        for pat in patterns:
            m = re.search(pat, text, flags=re.IGNORECASE)
            if m:
                xml_candidate = html.unescape(m.group(1)).strip().lstrip("\ufeff")
                if xml_candidate:
                    return xml_candidate

    return text


def parse_sitemap_xml(xml_text: str) -> Tuple[str, List[str]]:
    xml_text = maybe_unwrap_html_wrapped_text(xml_text)
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return "invalid", []

    root_name = get_localname(root.tag).lower()
    urls: List[str] = []

    if root_name == "sitemapindex":
        for sitemap in root:
            if get_localname(sitemap.tag).lower() != "sitemap":
                continue
            for child in sitemap:
                if get_localname(child.tag).lower() == "loc" and child.text:
                    urls.append(child.text.strip())
        return "index", urls

    if root_name == "urlset":
        for url_node in root:
            if get_localname(url_node.tag).lower() != "url":
                continue
            for child in url_node:
                if get_localname(child.tag).lower() == "loc" and child.text:
                    urls.append(child.text.strip())
        return "urlset", urls

    return "unknown", []


def describe_xml_payload(xml_text: str) -> str:
    text = maybe_unwrap_html_wrapped_text(xml_text)
    snippet = re.sub(r"\s+", " ", text).strip()[:220]
    try:
        root = ET.fromstring(text)
        root_name = get_localname(root.tag).lower()
        return f"root_tag={root_name}, snippet={snippet}"
    except ET.ParseError as exc:
        return f"parse_error={exc}, snippet={snippet}"


def is_product_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    return path.endswith(".htm")


def to_int(value: object, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def discover_processing_sitemaps(
    flaresolverr_url: str,
    root_sitemap_urls: Iterable[str],
    sitemap_offset: int,
    max_sitemaps: int,
) -> List[str]:
    discovered: List[str] = []

    for root_sitemap_url in root_sitemap_urls:
        url = root_sitemap_url.strip()
        if not url:
            continue
        print(f"[INFO] Checking root sitemap URL: {url}")

        try:
            xml_text = fetch_with_flaresolverr(flaresolverr_url, url)
        except Exception as exc:
            print(f"[WARN] Failed root sitemap fetch: {url} ({exc})")
            continue

        sitemap_type, urls = parse_sitemap_xml(xml_text)
        if sitemap_type == "index":
            print(f"[INFO] Root sitemap is sitemapindex: {url} ({len(urls)} child sitemaps)")
            discovered.extend(urls)
        elif sitemap_type == "urlset":
            print(f"[INFO] Root sitemap is urlset: {url} ({len(urls)} urls)")
            discovered.append(url)
        else:
            details = describe_xml_payload(xml_text)
            print(f"[WARN] Unsupported root sitemap XML: {url} | {details}")

    unique_discovered = list(dict.fromkeys(discovered))
    if not unique_discovered:
        unique_discovered = [u.strip() for u in root_sitemap_urls if u.strip()]

    start = max(0, sitemap_offset)
    if max_sitemaps > 0:
        end: Optional[int] = start + max_sitemaps
    else:
        end = None

    selected = unique_discovered[start:end]
    for idx, s_url in enumerate(selected, start=start):
        print(f"[INFO] Selected sitemap[{idx}]: {s_url}")
    return selected


def discover_product_urls_from_sitemaps(
    flaresolverr_url: str,
    sitemap_urls: Iterable[str],
    max_urls_per_sitemap: int,
) -> Set[str]:
    queue = deque(sitemap_urls)
    seen_sitemaps: Set[str] = set()
    product_urls: Set[str] = set()

    while queue:
        sitemap_url = queue.popleft().strip()
        if not sitemap_url or sitemap_url in seen_sitemaps:
            continue
        seen_sitemaps.add(sitemap_url)
        print(f"[INFO] Processing sitemap: {sitemap_url}")

        try:
            xml_text = fetch_with_flaresolverr(flaresolverr_url, sitemap_url)
        except Exception as exc:
            print(f"[WARN] Failed sitemap fetch: {sitemap_url} ({exc})")
            continue

        sitemap_type, urls = parse_sitemap_xml(xml_text)
        if sitemap_type == "index":
            print(f"[INFO] Sitemap index found: {sitemap_url} ({len(urls)} child sitemaps)")
            for sub_sitemap in urls:
                if sub_sitemap not in seen_sitemaps:
                    queue.append(sub_sitemap)
        elif sitemap_type == "urlset":
            collected = 0
            for url in urls:
                if is_product_url(url):
                    product_urls.add(url)
                    collected += 1
                    if max_urls_per_sitemap > 0 and collected >= max_urls_per_sitemap:
                        print(
                            f"[INFO] URL limit reached in {sitemap_url}: "
                            f"{max_urls_per_sitemap}"
                        )
                        break
            print(
                f"[INFO] Sitemap urlset processed: {sitemap_url} "
                f"(total loc={len(urls)}, collected .htm={collected})"
            )
        else:
            details = describe_xml_payload(xml_text)
            print(f"[WARN] Unsupported or invalid sitemap XML: {sitemap_url} | {details}")

    return product_urls


def parse_sites(raw_sites: List) -> List[str]:
    sites: List[str] = []
    for item in raw_sites:
        if isinstance(item, str):
            site = normalize_site(item)
        elif isinstance(item, dict):
            candidate = item.get("url") or item.get("site") or item.get("domain") or item.get("name")
            site = normalize_site(str(candidate or ""))
        else:
            site = ""
        if site:
            sites.append(site)
    return sites


def load_config(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError("YAML config must be a mapping/object at the top level")
    return data


def write_csv(path: Path, rows: List[Tuple[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["site", "product_url"])
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Discover product .htm URLs from robots/sitemaps using FlareSolverr."
    )
    parser.add_argument(
        "-c",
        "--config",
        default="cymax/sitemap_config.yml",
        help="Path to YAML config file",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config not found: {config_path}")
        return 1

    config = load_config(config_path)
    flaresolverr_url = config.get("flaresolverr_url", DEFAULT_FLARESOLVERR)
    output_csv = Path(config.get("output_csv", "cymax_product_urls.csv"))
    sitemap_offset = to_int(config.get("sitemap_offset", 0), default=0)
    max_sitemaps = to_int(config.get("max_sitemaps", 0), default=0)
    max_urls_per_sitemap = to_int(config.get("max_urls_per_sitemap", 0), default=0)
    sites = parse_sites(config.get("sites", []))

    if not sites:
        print("No valid sites found in config 'sites'.")
        return 1

    output_rows: List[Tuple[str, str]] = []

    for site in sites:
        robots_url = f"{site}/robots.txt"
        print(f"[INFO] Fetching robots: {robots_url}")

        try:
            robots_text = fetch_with_flaresolverr(flaresolverr_url, robots_url)
        except Exception as exc:
            print(f"[WARN] Failed robots fetch for {site}: {exc}")
            continue

        sitemap_urls = extract_sitemaps_from_robots(robots_text)
        if not sitemap_urls:
            fallback = f"{site}/sitemap.xml"
            sitemap_urls = [fallback]
            print(f"[WARN] No Sitemap entries in robots.txt for {site}; using {fallback}")

        print(f"[INFO] {site}: found {len(sitemap_urls)} root sitemap(s)")
        processing_sitemaps = discover_processing_sitemaps(
            flaresolverr_url=flaresolverr_url,
            root_sitemap_urls=sitemap_urls,
            sitemap_offset=sitemap_offset,
            max_sitemaps=max_sitemaps,
        )
        print(
            f"[INFO] {site}: processing {len(processing_sitemaps)} sitemap(s) "
            f"(offset={sitemap_offset}, max={max_sitemaps or 'all'})"
        )
        product_urls = discover_product_urls_from_sitemaps(
            flaresolverr_url=flaresolverr_url,
            sitemap_urls=processing_sitemaps,
            max_urls_per_sitemap=max_urls_per_sitemap,
        )
        print(f"[INFO] {site}: collected {len(product_urls)} product URLs (.htm)")

        for url in sorted(product_urls):
            output_rows.append((site, url))

    write_csv(output_csv, output_rows)
    print(f"[INFO] Wrote {len(output_rows)} rows to {output_csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
