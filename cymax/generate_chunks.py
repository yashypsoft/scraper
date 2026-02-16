#!/usr/bin/env python3
import json
import os
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


CURR_URL = os.getenv("CURR_URL", "https://www.cymax.com").rstrip("/")
SITEMAP_INDEX = f"{CURR_URL}/sitemap.xml"
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "13"))
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "0"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "50000"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))

FLARESOLVERR_URL = os.getenv("FLARESOLVERR_URL", "http://localhost:8191/v1")
FLARESOLVERR_URLS_RAW = os.getenv("FLARESOLVERR_URLS", "").strip()
FLARESOLVERR_TIMEOUT = int(os.getenv("FLARESOLVERR_TIMEOUT", "120"))
FLARESOLVERR_URLS = [u.strip() for u in FLARESOLVERR_URLS_RAW.split(",") if u.strip()]
if not FLARESOLVERR_URLS:
    FLARESOLVERR_URLS = [FLARESOLVERR_URL]

URL_LIST_FILE = "cymax_chunk_urls.txt"


def log(msg: str, level: str = "INFO") -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sys.stderr.write(f"[{ts}] [{level}] {msg}\n")
    sys.stderr.flush()


def sanitize_url_text(text: str) -> str:
    clean = re.sub(r"<[^>]+>", " ", text or "")
    m = re.search(r"https?://[^\s\"'<>]+", clean)
    return m.group(0).strip() if m else ""


def extract_xml_payload(raw: str) -> str:
    text = (raw or "").strip()
    for root_tag in ("sitemapindex", "urlset"):
        s = text.find(f"<{root_tag}")
        e_tag = f"</{root_tag}>"
        e = text.rfind(e_tag)
        if s != -1 and e != -1 and e > s:
            return text[s:e + len(e_tag)]
    return text


_thread_local = threading.local()
_endpoint_assign_lock = threading.Lock()
_endpoint_assign_counter = 0


def get_thread_flaresolverr_url() -> str:
    global _endpoint_assign_counter
    if hasattr(_thread_local, "flaresolverr_url"):
        return _thread_local.flaresolverr_url
    with _endpoint_assign_lock:
        idx = _endpoint_assign_counter % len(FLARESOLVERR_URLS)
        _endpoint_assign_counter += 1
    _thread_local.flaresolverr_url = FLARESOLVERR_URLS[idx]
    return _thread_local.flaresolverr_url


def get_session() -> Tuple[requests.Session, Dict[str, str]]:
    if not hasattr(_thread_local, "session"):
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=MAX_WORKERS * 2,
            pool_maxsize=MAX_WORKERS * 2,
            max_retries=Retry(total=2, backoff_factor=0.5),
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _thread_local.session = session
        _thread_local.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Referer": CURR_URL + "/",
        }
    return _thread_local.session, _thread_local.headers


def get_browser_session_id(session: requests.Session, fs_url: str) -> Optional[str]:
    if hasattr(_thread_local, "flaresolverr_session_id"):
        return _thread_local.flaresolverr_session_id
    session_id = f"cymax-plan-{threading.get_ident()}-{int(time.time()*1000)}-{random.randint(1000, 9999)}"
    try:
        resp = session.post(fs_url, json={"cmd": "sessions.create", "session": session_id}, timeout=30)
        if resp.status_code == 200 and resp.json().get("status") == "ok":
            _thread_local.flaresolverr_session_id = session_id
            return session_id
    except Exception:
        return None
    return None


def fs_get(url: str, retries: int = 3) -> Optional[str]:
    session, headers = get_session()
    fs_url = get_thread_flaresolverr_url()
    session_id = get_browser_session_id(session, fs_url)
    for attempt in range(retries):
        try:
            payload = {"cmd": "request.get", "url": url, "maxTimeout": 120000, "headers": headers}
            if session_id:
                payload["session"] = session_id
            resp = session.post(fs_url, json=payload, timeout=FLARESOLVERR_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "ok":
                    sol = data.get("solution", {})
                    return sol.get("response", "")
        except Exception:
            pass
        if attempt < retries - 1:
            time.sleep((2 ** attempt) + random.uniform(0, 1))
    return None


def load_xml(url: str) -> Optional[ET.Element]:
    text = fs_get(url)
    if not text:
        return None
    try:
        return ET.fromstring(extract_xml_payload(text))
    except ET.ParseError:
        return None


def extract_locs(root: ET.Element) -> List[str]:
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    elems = root.findall(".//ns:loc", ns) or root.findall(".//loc")
    return [e.text.strip() for e in elems if e.text and e.text.strip()]


def get_sitemap_index_url() -> str:
    robots = fs_get(f"{CURR_URL}/robots.txt")
    if robots:
        for line in robots.splitlines():
            if line.lower().startswith("sitemap:"):
                candidate = sanitize_url_text(line.split(":", 1)[1].strip())
                if candidate.startswith("http"):
                    log(f"Found sitemap in robots.txt: {candidate}")
                    return candidate
    return SITEMAP_INDEX


def collect_product_urls_from_sitemap(sitemap_url: str, visited: set, depth: int = 0, max_depth: int = 10) -> List[str]:
    if sitemap_url in visited or depth > max_depth:
        return []
    visited.add(sitemap_url)
    root = load_xml(sitemap_url)
    if root is None:
        return []
    locs = extract_locs(root)
    if not locs:
        return []

    tag = root.tag.lower()
    nested = [u for u in locs if u.lower().endswith(".xml") or u.lower().endswith(".xml.gz")]
    if "sitemapindex" in tag or (nested and len(nested) == len(locs)):
        out: List[str] = []
        for n in nested:
            out.extend(collect_product_urls_from_sitemap(n, visited, depth + 1, max_depth))
        return out

    urls = [u for u in locs if ".htm" in u and not any(x in u for x in ["--C", "--PC", "sitemap", "robots"])]
    return urls


def main() -> None:
    if CHUNK_SIZE <= 0:
        raise ValueError("CHUNK_SIZE must be > 0")

    sitemap_index_url = get_sitemap_index_url()
    log(f"Loading sitemap index: {sitemap_index_url}")
    root = load_xml(sitemap_index_url)
    if root is None:
        raise RuntimeError(f"Failed loading sitemap index: {sitemap_index_url}")

    sitemap_locs = extract_locs(root)
    if not sitemap_locs:
        raise RuntimeError("No sitemaps found in index")
    if MAX_SITEMAPS > 0:
        sitemap_locs = sitemap_locs[:MAX_SITEMAPS]
    log(f"Top-level sitemaps selected: {len(sitemap_locs)}")

    all_urls: List[str] = []
    visited_lock = threading.Lock()
    global_visited = set()

    def process_one(sm_url: str) -> List[str]:
        with visited_lock:
            local_visited = set(global_visited)
        urls = collect_product_urls_from_sitemap(sm_url, local_visited, 0, 10)
        if MAX_URLS_PER_SITEMAP > 0 and len(urls) > MAX_URLS_PER_SITEMAP:
            urls = urls[:MAX_URLS_PER_SITEMAP]
        log(f"Sitemap done: {sm_url} -> {len(urls)} urls")
        return urls

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, max(1, len(sitemap_locs)))) as ex:
        futures = [ex.submit(process_one, s) for s in sitemap_locs]
        for f in as_completed(futures):
            all_urls.extend(f.result())

    # Unique preserving order
    unique_urls = list(dict.fromkeys(all_urls))
    total = len(unique_urls)
    log(f"Total unique product urls: {total}")
    if total == 0:
        raise RuntimeError("No product urls discovered")

    with open(URL_LIST_FILE, "w", encoding="utf-8") as f:
        for u in unique_urls:
            f.write(u + "\n")

    chunks = []
    for i, offset in enumerate(range(0, total, CHUNK_SIZE)):
        limit = min(CHUNK_SIZE, total - offset)
        chunks.append(
            {
                "chunk_id": i,
                "offset": offset,
                "limit": limit,
                "url_file": URL_LIST_FILE,
                "base_url": CURR_URL,
                "total_urls": total,
            }
        )
    log(f"Generated chunks: {len(chunks)}")

    matrix_json = json.dumps(chunks)
    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            f.write(f"matrix={matrix_json}\n")
            f.write(f"total_urls={total}\n")
            f.write(f"chunk_count={len(chunks)}\n")
    else:
        print(matrix_json)


if __name__ == "__main__":
    main()
