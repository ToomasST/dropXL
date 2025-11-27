#!/usr/bin/env python3
"""
Prenta API data fetcher

Fetches maximum product-related information from Prenta sandbox API and saves it locally:
- Product list (/products)
- Product detail (/products/{product_id})
- Product attributes (/products/{product_id}/attributes)
- Product attribute values (/products/{product_id}/attribute_values)
- Prices (/prices)
- Stock levels (/stock_levels)

Outputs to: ./data/ by default
- data/products_list.json
- data/products/details/{id}.json
- data/products/attributes/{id}.json
- data/products/attribute_values/{id}.json
- data/prices.json
- data/stock_levels.json
- data/counts.json

Authentication is via Basic Auth. Provide via environment variables or CLI flags:
- PRENTA_USERNAME / PRENTA_PASSWORD
or
- --username / --password

Example:
  python prenta_fetch.py --base-url https://sandbox.prenta.lt/api/v1 --per-page 100 --workers 6
  
Defaults:
- Writes aggregate files (no per-product files)
- Produces products_enriched.json and products_attributes_kv.json
- FlixMedia rendering is ENABLED by default; disable via env PRENTA_FLIX_RENDER=false

Quick start:
  python prenta_fetch.py --max-products 15

Useful flags:
- --flix-timeout-ms 45000 (default)
- --flix-wait-selector "#flix-minisite,#flix-inpage"

Notes:
- Respects documented parameter constraints (per_page <= 100)
- Retries with exponential backoff on transient errors
- Concurrency is used for per-product detail and attribute endpoints
- Avoids assumptions: uses only confirmed endpoints and allowed field selections
"""

 
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional
import re
from threading import Semaphore
import contextlib

import requests
from requests.adapters import HTTPAdapter
from urllib.parse import urlencode, urlparse
import base64
from dotenv import load_dotenv, find_dotenv
from datetime import datetime, timezone

try:
    from tqdm import tqdm  # type: ignore
except Exception:  # pragma: no cover - tqdm is optional
    def tqdm(iterable=None, **kwargs):
        return iterable if iterable is not None else []

# Optional HTML parser for Flix features extraction
try:  # pragma: no cover - optional dependency
    from bs4 import BeautifulSoup  # type: ignore
except Exception:
    BeautifulSoup = None  # type: ignore


DEFAULT_BASE_URL = "https://sandbox.prenta.lt/api/v1"


@dataclass
class ClientConfig:
    base_url: str = DEFAULT_BASE_URL
    username: str = ""
    password: str = ""
    timeout: int = 30
    max_retries: int = 5
    backoff_factor: float = 0.8
    per_page: int = 100  # API max
    workers: int = 6
    newer_than: Optional[str] = None  # ISO 8601, if you want delta-sync
    verify_ssl: bool = True
    single_file_only: bool = False
    flush_every: int = 100
    kv_only: bool = False
    # FlixMedia rendering (optional)
    flix_render: bool = True
    flix_timeout_ms: int = 45000
    flix_wait_selector: str = "#flix-minisite,#flix-inpage"
    flix_concurrency: int = 2
    flix_origin: str = "https://e.prenta.lt/"


class PrentaClient:
    def __init__(self, cfg: ClientConfig) -> None:
        self.cfg = cfg
        self.session = requests.Session()
        self.session.auth = (cfg.username, cfg.password)
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })
        # SSL verification can be toggled for sandbox issues
        self.session.verify = cfg.verify_ssl
        # A basic HTTPAdapter; we'll do manual backoff
        self.session.mount("https://", HTTPAdapter(pool_connections=16, pool_maxsize=16))
        self.session.mount("http://", HTTPAdapter(pool_connections=16, pool_maxsize=16))

    def _url(self, path: str) -> str:
        return f"{self.cfg.base_url.rstrip('/')}/{path.lstrip('/')}"

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = self._url(path)
        attempt = 0
        last_exc: Optional[Exception] = None
        while attempt < self.cfg.max_retries:
            attempt += 1
            try:
                resp = self.session.request(method, url, params=params, timeout=self.cfg.timeout)
                # Handle auth errors fast
                if resp.status_code == 401:
                    raise RuntimeError("401 Unauthorized: check PRENTA_USERNAME/PRENTA_PASSWORD or CLI flags")
                # Retry-worthy status codes
                if resp.status_code >= 500 or resp.status_code in (429,):
                    raise RuntimeError(f"Transient HTTP {resp.status_code}")
                resp.raise_for_status()
                # JSON array or object
                if not resp.content:
                    return None
                return resp.json()
            except Exception as e:  # retry on transient
                last_exc = e
                if attempt >= self.cfg.max_retries:
                    break
                # backoff
                sleep_for = self.cfg.backoff_factor * (2 ** (attempt - 1))
                time.sleep(sleep_for)
        # Exhausted retries
        raise RuntimeError(f"Request failed for {url} params={params}: {last_exc}")

    def _paged(self, path: str, base_params: Optional[Dict[str, Any]] = None) -> Iterable[Any]:
        params = dict(base_params or {})
        page = 1
        per_page = min(max(1, self.cfg.per_page), 100)
        while True:
            params.update({"page": page, "per_page": per_page})
            data = self._request("GET", path, params=params)
            if data is None:
                break
            if not isinstance(data, list):
                # Some endpoints might return object; normalize to list if possible
                if isinstance(data, dict):
                    items = data.get("items") or data.get("results") or []
                else:
                    items = []
            else:
                items = data
            if not items:
                break
            for item in items:
                yield item
            if len(items) < per_page:
                break
            page += 1

    # Endpoints
    def iter_products(self, list_limit: Optional[int] = None) -> Iterable[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        if self.cfg.newer_than:
            params["newer_than"] = self.cfg.newer_than
        if list_limit and list_limit > 0:
            params["limit"] = list_limit
        # Wrap paging generator to stop after list_limit items if provided
        count = 0
        for item in self._paged("products", params):
            yield item
            count += 1
            if list_limit and list_limit > 0 and count >= list_limit:
                break

    def get_product_detail(self, product_id: Any) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        return self._request("GET", f"products/{product_id}", params)

    def iter_product_attributes(self, product_id: Any) -> Iterable[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        if self.cfg.newer_than:
            params["newer_than"] = self.cfg.newer_than
        return self._paged(f"products/{product_id}/attributes", params)

    def iter_product_attribute_values(self, product_id: Any) -> Iterable[Dict[str, Any]]:
        # Do not limit fields to ensure value_* fields are included
        params: Dict[str, Any] = {}
        if self.cfg.newer_than:
            params["newer_than"] = self.cfg.newer_than
        return self._paged(f"products/{product_id}/attribute_values", params)

    def iter_prices(self, product_id: Optional[int] = None) -> Iterable[Dict[str, Any]]:
        params: Dict[str, Any] = {"order": "product_id"}
        if product_id is not None:
            params["product_id"] = product_id
        return self._paged("prices", params)

    def iter_stock_levels(self, product_id: Optional[int] = None) -> Iterable[Dict[str, Any]]:
        params: Dict[str, Any] = {"order": "product_id"}
        if product_id is not None:
            params["product_id"] = product_id
        return self._paged("stock_levels", params)

    def iter_categories(self) -> Iterable[Dict[str, Any]]:
        # Categories collection endpoint
        params: Dict[str, Any] = {}
        return self._paged("categories", params)

    def iter_manufacturers(self) -> Iterable[Dict[str, Any]]:
        # Manufacturers collection endpoint
        params: Dict[str, Any] = {}
        return self._paged("manufacturers", params)


def ensure_dirs(base_out: str) -> Dict[str, str]:
    paths = {"root": base_out}
    os.makedirs(paths["root"], exist_ok=True)
    return paths


def save_json(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def slugify_key(name: str) -> str:
    """Convert attribute key into a stable, lowercase slug with underscores."""
    if not isinstance(name, str):
        return "attr"
    s = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip().lower())
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "attr"


def render_flixmedia_html(snippet_html: str, wait_selector: str, timeout_ms: int, origin_url: str) -> Dict[str, Any]:
    """Render FlixMedia snippet to static HTML using Playwright (if available).
    Returns dict with keys: html (str|None), status (rendered|timeout|unavailable|error), error (str|None).
    """
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:  # Playwright not installed
        return {"html": None, "status": "unavailable", "error": f"{e}"}

    # Ensure script URLs have protocol
    snippet = snippet_html.replace('src="//', 'src="https://')
    html_wrapper = f"""
    <html>
      <head><meta charset='utf-8'></head>
      <body>
        {snippet}
      </body>
    </html>
    """
    selectors = [s.strip() for s in (wait_selector or "").split(",") if s.strip()]
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                java_script_enabled=True,
                extra_http_headers={"Referer": origin_url},
            )
            page = context.new_page()
            # Navigate to origin to set correct document.location
            try:
                page.goto(origin_url, wait_until="domcontentloaded")
            except Exception:
                pass
            # Inject our content preserving origin
            try:
                page.evaluate("html => { document.open(); document.write(html); document.close(); }", html_wrapper)
            except Exception:
                page.set_content(html_wrapper, wait_until="domcontentloaded")
            # Give network some time to settle
            try:
                page.wait_for_load_state("networkidle", timeout=timeout_ms)
            except Exception:
                pass
            # Poll for content
            end = time.time() + (timeout_ms / 1000.0)
            content_html = None
            outer_html = None
            while time.time() < end:
                for sel in selectors:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            inner = el.inner_html()
                            if inner and inner.strip():
                                content_html = inner
                                break
                            # Fallback: capture outerHTML (e.g., iframe tag)
                            outer = el.evaluate("el => el.outerHTML")
                            if outer and (not outer_html):
                                outer_html = outer
                    except Exception:
                        pass
                if content_html:
                    break
                page.wait_for_timeout(200)
            context.close()
            browser.close()
            # Try to extract iframe src from whatever we got
            iframe_src = None
            probe_source = content_html or outer_html or ""
            m = re.search(r"<iframe[^>]+src=\"([^\"]+)\"", probe_source or "", flags=re.I)
            if m:
                iframe_src = m.group(1)
            if content_html:
                return {"html": content_html, "status": "rendered", "error": None, "iframe_src": iframe_src}
            if outer_html:
                return {"html": outer_html, "status": "outer", "error": None, "iframe_src": iframe_src}
            return {"html": None, "status": "timeout", "error": None, "iframe_src": iframe_src}
    except Exception as e:
        return {"html": None, "status": "error", "error": f"{e}", "iframe_src": None}


# --- Flix JSON API fallback helpers ----------------------------------------

def _b64_no_pad(s: str) -> str:
    return base64.b64encode(s.encode("utf-8")).decode("utf-8").rstrip("=")


def _parse_snippet_attrs(snippet: Optional[str]) -> Dict[str, Any]:
    if not snippet or not isinstance(snippet, str):
        return {}
    def grab(attr: str) -> Optional[str]:
        m = re.search(fr"data-flix-{attr}=\"([^\"]*)\"", snippet, flags=re.I)
        return m.group(1) if m else None
    return {
        "distributor": grab("distributor"),
        "language": grab("language"),
        "fallback_language": grab("fallback-language"),
        "ean": grab("ean"),
        "brand": grab("brand"),
        "sku": grab("sku"),
    }


def _get_domain_from_origin(origin_url: str) -> str:
    try:
        p = urlparse(origin_url)
        if p.netloc:
            return p.netloc
        return origin_url.strip().lstrip("/")
    except Exception:
        return "e.prenta.lt"


def _build_tjson_url(ean: str, sku: Optional[str], dist_id: str, iso: str, fl_iso: str, origin_domain: str) -> str:
    slug = f"{(sku or '')}{ean}{_b64_no_pad(origin_domain)}"
    base = f"https://media.flixcar.com/modular/web-api/{iso}/{dist_id}/{slug}/t.json"
    return f"{base}?mpn={sku or ''}&ean={ean}&distId={dist_id}&iso={iso}&flIso={fl_iso}"


def _fetch_tjson(url: str) -> Dict[str, Any]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Accept": "application/json,text/plain;q=0.8,*/*;q=0.5",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://e.prenta.lt/",
    }
    try:
        r = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
        try:
            data = r.json()
        except Exception:
            data = None
        return {"status_code": r.status_code, "json": data}
    except Exception as e:
        return {"status_code": None, "error": str(e)}


def _build_features_html_from_key_features_html(key_features_html: str) -> Optional[str]:
    if not key_features_html or not isinstance(key_features_html, str):
        return None
    # Prefer BeautifulSoup if available; otherwise do a simple fallback
    if BeautifulSoup:
        try:
            soup = BeautifulSoup(key_features_html, "html.parser")
            rows = soup.select(".hotspot_table_row")
            parts: List[str] = []
            for row in rows:
                img = row.select_one(".hotspot_feature_image_left img, .hotspot_feature_image_right img, img")
                img_src = (img.get("src") if img else None) or ""
                if img_src.startswith("//"):
                    img_src = "https:" + img_src
                img_alt = img.get("alt") if img else ""
                desc_el = row.select_one(".hotspot_feature_desc") or row
                title = None
                strong = desc_el.find("strong") if desc_el else None
                if strong and strong.text.strip():
                    title = strong.text.strip()
                else:
                    for tag in ["h3", "h2", "h4"]:
                        h = desc_el.find(tag) if desc_el else None
                        if h and h.text.strip():
                            title = h.text.strip()
                            break
                body_ps: List[str] = []
                if desc_el:
                    for p in desc_el.find_all("p"):
                        txt = p.get_text(" ", strip=True)
                        if not txt:
                            continue
                        if strong and p.find("strong") and txt == strong.get_text(" ", strip=True):
                            continue
                        body_ps.append(txt)
                figure_html = f'<figure class="flixf-media"><img src="{img_src}" alt="{(title or img_alt or '').replace("\"","&quot;")}"></figure>' if img_src else ""
                title_html = f"<h3>{title}</h3>" if title else ""
                body_html = "".join(f"<p>{re.sub(r'<[^>]+>', '', t)}</p>" for t in body_ps)
                parts.append(f"<article class=\"flixf-item\">{figure_html}<div class=\"flixf-body\">{title_html}{body_html}</div></article>")
            if parts:
                return "<!DOCTYPE html><meta charset=\"utf-8\"><section class=\"flix-features\">" + "\n".join(parts) + "</section>"
        except Exception:
            pass
    # Fallback: very light cleanup; keep as-is
    try:
        # Ensure protocol on // URLs
        html = re.sub(r"src=\"//", "src=\"https://", key_features_html)
        return "<!DOCTYPE html><meta charset=\"utf-8\">" + html
    except Exception:
        return None

def fetch_all(cfg: ClientConfig, out_dir: str) -> None:
    client = PrentaClient(cfg)
    paths = ensure_dirs(out_dir)

    master = None
    master_path = os.path.join(paths["root"], "products_master.json")
    # Aggregated KV-only file (product_id -> attributes_kv)
    kv_master_path = os.path.join(paths["root"], "products_attributes_kv.json")
    kv_map: Dict[str, Any] = {}
    if cfg.single_file_only:
        master = {
            "meta": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "base_url": cfg.base_url,
            },
            # Products keyed by product_id (string)
            "products": {},
        }
        save_json(master_path, master)

    # 1) Products list
    print("Fetching product list (/products)...", flush=True)
    product_list: List[Dict[str, Any]] = []
    list_limit = args.max_products if (args.max_products and args.max_products > 0) else None
    for item in tqdm(client.iter_products(list_limit=list_limit), desc="products list"):
        product_list.append(item)
    if not cfg.single_file_only:
        if not cfg.kv_only:
            save_json(os.path.join(paths["root"], "products_list.json"), product_list)
    product_ids = [p.get("id") for p in product_list if p.get("id") is not None]
    # Optional explicit product IDs override via CLI
    explicit_ids: Optional[List[int]] = None
    if getattr(args, "product_ids", None):
        try:
            explicit_ids = [int(s.strip()) for s in str(args.product_ids).split(",") if s.strip()]
        except Exception:
            explicit_ids = None
    print(f"Products listed: {len(product_ids)}", flush=True)
    if cfg.single_file_only and master is not None:
        # Seed master with base data per product
        for p in product_list:
            pid = p.get("id")
            if pid is None:
                continue
            key = str(pid)
            rec = master["products"].get(key) or {"product_id": pid}
            rec["base"] = p
            master["products"][key] = rec
        master["meta"].update({"products_list_total": len(product_ids)})
        save_json(master_path, master)

    # 2) Prices (global or filtered by product_ids for tests)
    print("Fetching prices (/prices)...", flush=True)
    prices: List[Dict[str, Any]] = []
    if explicit_ids or list_limit:
        ids_to_use = explicit_ids or product_ids
        for pid in tqdm(ids_to_use, desc="prices (filtered)"):
            for item in client.iter_prices(product_id=pid):
                prices.append(item)
    else:
        for item in tqdm(client.iter_prices(), desc="prices"):
            prices.append(item)
    if not cfg.single_file_only and not cfg.kv_only:
        save_json(os.path.join(paths["root"], "prices.json"), prices)
    print(f"Prices fetched: {len(prices)}", flush=True)
    if cfg.single_file_only and master is not None:
        # Merge prices into products
        for pr in prices:
            pid = pr.get("product_id")
            if pid is None:
                continue
            key = str(pid)
            rec = master["products"].get(key) or {"product_id": pid}
            rec["price"] = pr
            master["products"][key] = rec
        master["meta"].update({"prices_total": len(prices)})
        save_json(master_path, master)

    # 3) Stock levels (global or filtered by product_ids for tests)
    print("Fetching stock levels (/stock_levels)...", flush=True)
    stocks: List[Dict[str, Any]] = []
    if explicit_ids or list_limit:
        ids_to_use = explicit_ids or product_ids
        for pid in tqdm(ids_to_use, desc="stock_levels (filtered)"):
            for item in client.iter_stock_levels(product_id=pid):
                stocks.append(item)
    else:
        for item in tqdm(client.iter_stock_levels(), desc="stock_levels"):
            stocks.append(item)
    if not cfg.single_file_only and not cfg.kv_only:
        save_json(os.path.join(paths["root"], "stock_levels.json"), stocks)
    print(f"Stock levels fetched: {len(stocks)}", flush=True)
    if cfg.single_file_only and master is not None:
        # Merge stocks into products
        for st in stocks:
            pid = st.get("product_id")
            if pid is None:
                continue
            key = str(pid)
            rec = master["products"].get(key) or {"product_id": pid}
            rec["stock"] = st
            master["products"][key] = rec
        master["meta"].update({"stock_levels_total": len(stocks)})
        save_json(master_path, master)

    # 4) Categories/Manufacturers (skip in kv-only mode for speed)
    categories_by_id: Dict[Any, Any] = {}
    manufacturers_by_id: Dict[Any, Any] = []  # type: ignore
    if not cfg.kv_only:
        print("Fetching categories (/categories)...", flush=True)
        categories: List[Dict[str, Any]] = []
        try:
            for item in tqdm(client.iter_categories(), desc="categories"):
                categories.append(item)
        except Exception as e:
            print(f"WARN: categories fetch failed: {e}", file=sys.stderr)
        categories_by_id = {c.get("id"): c for c in categories}
        if cfg.single_file_only and master is not None:
            master["meta"].update({"categories_total": len(categories)})
            save_json(master_path, master)

        print("Fetching manufacturers (/manufacturers)...", flush=True)
        manufacturers: List[Dict[str, Any]] = []
        try:
            for item in tqdm(client.iter_manufacturers(), desc="manufacturers"):
                manufacturers.append(item)
        except Exception as e:
            print(f"WARN: manufacturers fetch failed: {e}", file=sys.stderr)
        manufacturers_by_id = {m.get("id"): m for m in manufacturers}
        if cfg.single_file_only and master is not None:
            master["meta"].update({"manufacturers_total": len(manufacturers)})
            save_json(master_path, master)

    # 5) Per product detail + attributes + attribute_values
    # Allow limiting to test smaller batch in case of very large catalogs
    to_fetch = product_ids
    if explicit_ids:
        to_fetch = explicit_ids
        print(f"Limiting to explicit --product-ids: {len(to_fetch)}", flush=True)
    elif args.max_products and args.max_products > 0:
        to_fetch = product_ids[: args.max_products]
        print(f"Limiting to first {len(to_fetch)} products due to --max-products", flush=True)

    # Make quick lookup maps for price and stock
    price_by_pid = {p.get("product_id"): p for p in prices}
    stock_by_pid = {s.get("product_id"): s for s in stocks}

    def worker(pid: Any) -> Dict[str, Any]:
        detail = client.get_product_detail(pid)
        attrs = list(client.iter_product_attributes(pid))
        attr_vals = list(client.iter_product_attribute_values(pid))
        # Build lookups for resolution
        attr_by_id = {a.get("id"): a for a in attrs}
        attr_val_by_id = {v.get("id"): v for v in attr_vals}
        # Resolve attribute_line_ids -> human-readable
        resolved: List[Dict[str, Any]] = []
        kv: Dict[str, Any] = {}
        for line in (detail.get("attribute_line_ids") or []):
            aid = (line or {}).get("attribute_id")
            vid = (line or {}).get("value_id")
            a = attr_by_id.get(aid) if aid is not None else None
            v = attr_val_by_id.get(vid) if vid is not None else None
            name = (a or {}).get("name")
            uom_id = (a or {}).get("uom_id")
            atype = (v or {}).get("type") or (a or {}).get("type")
            value: Any = None
            if v is not None:
                if atype == "selection" or ("value_text" in v):
                    value = v.get("value_text")
                elif atype == "integer" or ("value_integer" in v):
                    value = v.get("value_integer")
                elif atype == "float" or ("value_float" in v):
                    value = v.get("value_float")
                elif atype == "boolean" or ("value_boolean" in v):
                    value = v.get("value_boolean")
            resolved.append({
                "attribute_id": aid,
                "attribute_name": name,
                "value_id": vid,
                "type": atype,
                "value": value,
                "uom_id": uom_id,
            })
            if name is not None:
                kv[name] = value
        # Slugified KV (stable keys for downstream systems)
        kv_slug = {slugify_key(k): v for k, v in kv.items()}
        # Images summary
        images = detail.get("images") or []
        primary_url = None
        if isinstance(images, list) and images:
            first = images[0]
            if isinstance(first, str):
                primary_url = first
            elif isinstance(first, dict):
                primary_url = first.get("url") or first.get("image") or first.get("src")
        images_count = len(images) if isinstance(images, list) else 0
        # Category path (if hierarchy is available)
        category_path_list: List[str] = []
        if isinstance(detail.get("category_id"), (int, str)) and isinstance(categories_by_id, dict) and categories_by_id:
            cat = categories_by_id.get(detail.get("category_id")) or {}
            seen: set = set()
            cur = cat
            while isinstance(cur, dict) and cur:
                name = cur.get("name")
                if name:
                    category_path_list.insert(0, name)
                parent_id = cur.get("parent_id")
                if not parent_id or parent_id in seen:
                    break
                seen.add(parent_id)
                cur = categories_by_id.get(parent_id)
        category_path = " > ".join(category_path_list) if category_path_list else ((categories_by_id.get(detail.get("category_id")) or {}).get("name") if isinstance(categories_by_id, dict) else None)
        # Optionally render FlixMedia block to static HTML
        flix_html = None
        flix_status = None
        flix_error = None
        desc_flix = detail.get("description_flixmedia")
        flix_description_html = None
        if cfg.flix_render and isinstance(desc_flix, str) and desc_flix.strip():
            # Inject identifiers if missing
            try:
                brand_name = (manufacturers_by_id.get(detail.get("manufacturer_id")) or {}).get("name")
            except Exception:
                brand_name = None
            sku_val = detail.get("internal_reference") or detail.get("barcode")
            if "data-flix-brand=\"\"" in desc_flix and brand_name:
                desc_flix = re.sub(r"data-flix-brand=\"\"", f"data-flix-brand=\"{brand_name}\"", desc_flix)
            if "data-flix-sku=\"\"" in desc_flix and sku_val:
                desc_flix = re.sub(r"data-flix-sku=\"\"", f"data-flix-sku=\"{sku_val}\"", desc_flix)
            if flix_sem is not None:
                flix_sem.acquire()
            try:
                r = render_flixmedia_html(desc_flix, cfg.flix_wait_selector, cfg.flix_timeout_ms, cfg.flix_origin)
                flix_html = r.get("html")
                flix_status = r.get("status")
                flix_error = r.get("error")
                flix_iframe_src = r.get("iframe_src")
            finally:
                if flix_sem is not None:
                    flix_sem.release()

            # JSON fallback to build minimal features HTML when not rendered or iframe-locked
            if not flix_html or (isinstance(flix_status, str) and flix_status.lower() != "rendered"):
                attrs = _parse_snippet_attrs(desc_flix)
                dist_id = (attrs.get("distributor") or "15151").strip()
                iso = (attrs.get("language") or "en").strip()
                fl_iso = (attrs.get("fallback_language") or "lt").strip()
                ean_code = detail.get("barcode")
                sku_code = detail.get("internal_reference") or detail.get("barcode")
                if ean_code:
                    origin_domain = _get_domain_from_origin(cfg.flix_origin)
                    tjson_url = _build_tjson_url(str(ean_code), sku_code, dist_id, iso, fl_iso, origin_domain)
                    tjson_resp = _fetch_tjson(tjson_url)
                    data = tjson_resp.get("json")
                    try:
                        root = data[0] if isinstance(data, list) and data else data
                        key_features_html = (
                            (((root or {}).get("modules") or {}).get("hotspot") or {}).get("key_features", {})
                        ).get("html")
                        if isinstance(key_features_html, str) and key_features_html.strip():
                            flix_description_html = _build_features_html_from_key_features_html(key_features_html)
                    except Exception:
                        pass
        # Per-product files disabled by design
        enriched = {
            "product_id": pid,
            "product": detail,
            # Only keep key-value representation of attributes
            "attributes_kv": kv,
            "attributes_kv_slug": kv_slug,
            "price": price_by_pid.get(pid),
            "stock": stock_by_pid.get(pid),
            "category": {
                "id": detail.get("category_id"),
                "name": (categories_by_id.get(detail.get("category_id")) or {}).get("name"),
            },
            "manufacturer": {
                "id": detail.get("manufacturer_id"),
                "name": (manufacturers_by_id.get(detail.get("manufacturer_id")) or {}).get("name"),
            },
            # Provide text-only variant of description for downstream use
            "description_text": re.sub(r"<[^>]*>", "", detail.get("description") or "").strip(),
            # Convenience
            "image_primary": primary_url,
            "images_count": images_count,
            "category_path": category_path,
            "category_hierarchy": category_path_list if category_path_list else None,
            "sku": detail.get("internal_reference"),
            "ean": detail.get("barcode"),
            # FlixMedia rendered content (optional)
            "flixmedia_html": flix_html,
            "flixmedia_status": flix_status,
            "flixmedia_error": flix_error,
            "flixmedia_iframe_src": locals().get("flix_iframe_src"),
            # Compact features HTML from Flix JSON fallback (if available)
            "flix_description_html": flix_description_html,
        }
        return {
            **enriched,
            "detail_bytes": len(json.dumps(detail, ensure_ascii=False)),
            "attrs_count": len(attrs),
            "attr_vals_count": len(attr_vals),
        }

    print("Fetching product details + attributes + attribute_values...", flush=True)
    # Limit FlixMedia parallel renders with a semaphore
    flix_sem: Optional[Semaphore] = None
    if cfg.flix_render:
        try:
            flix_sem = Semaphore(max(1, int(cfg.flix_concurrency)))
        except Exception:
            flix_sem = Semaphore(1)
    results: List[Dict[str, Any]] = []
    enriched_list: List[Dict[str, Any]] = []
    merged_since_flush = 0
    with ThreadPoolExecutor(max_workers=cfg.workers) as ex:
        futures = {ex.submit(worker, pid): pid for pid in to_fetch}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="per-product"):
            try:
                r = fut.result()
                results.append(r)
                # Collect enriched rows for non-single-file mode
                enriched_list.append({
                    "product_id": r["product_id"],
                    "product": r.get("product"),
                    "attributes_kv": r.get("attributes_kv"),
                    "attributes_kv_slug": r.get("attributes_kv_slug"),
                    "price": r.get("price"),
                    "stock": r.get("stock"),
                    "category": r.get("category"),
                    "manufacturer": r.get("manufacturer"),
                    "description_text": r.get("description_text"),
                    "image_primary": r.get("image_primary"),
                    "images_count": r.get("images_count"),
                    "category_path": r.get("category_path"),
                    "category_hierarchy": r.get("category_hierarchy"),
                    "sku": r.get("sku"),
                    "ean": r.get("ean"),
                    "flixmedia_html": r.get("flixmedia_html"),
                    "flixmedia_status": r.get("flixmedia_status"),
                    "flixmedia_error": r.get("flixmedia_error"),
                    "flixmedia_iframe_src": r.get("flixmedia_iframe_src"),
                    "flix_description_html": r.get("flix_description_html"),
                })
                # Incremental merge into master dict and flush periodically
                if cfg.single_file_only and master is not None:
                    pid = r["product_id"]
                    key = str(pid)
                    rec = master["products"].get(key) or {"product_id": pid}
                    rec.update({
                        "product": r.get("product"),
                        "attributes_kv": r.get("attributes_kv"),
                        "attributes_kv_slug": r.get("attributes_kv_slug"),
                        "price": r.get("price"),
                        "stock": r.get("stock"),
                        "category": r.get("category"),
                        "manufacturer": r.get("manufacturer"),
                        "description_text": r.get("description_text"),
                        "image_primary": r.get("image_primary"),
                        "images_count": r.get("images_count"),
                        "category_path": r.get("category_path"),
                        "category_hierarchy": r.get("category_hierarchy"),
                        "sku": r.get("sku"),
                        "ean": r.get("ean"),
                        "flixmedia_html": r.get("flixmedia_html"),
                        "flixmedia_status": r.get("flixmedia_status"),
                        "flixmedia_error": r.get("flixmedia_error"),
                        "flixmedia_iframe_src": r.get("flixmedia_iframe_src"),
                        "flix_description_html": r.get("flix_description_html"),
                    })
                    master["products"][key] = rec
                    merged_since_flush += 1
                    # Update KV-only map and flush
                    kv_map[key] = r.get("attributes_kv") or {}
                    if cfg.flush_every > 0 and (merged_since_flush % cfg.flush_every == 0):
                        save_json(master_path, master)
                        save_json(kv_master_path, kv_map)
                else:
                    # Even when not in single-file-only mode, maintain KV-only map
                    key = str(r["product_id"])
                    kv_map[key] = r.get("attributes_kv") or {}
                    if cfg.kv_only and cfg.flush_every > 0 and (merged_since_flush % cfg.flush_every == 0):
                        save_json(kv_master_path, kv_map)
            except Exception as e:
                pid = futures[fut]
                print(f"ERROR for product {pid}: {e}", file=sys.stderr)

    # 5) Summary counts
    attributes_total = sum(r.get("attrs_count", 0) for r in results)
    attr_values_total = sum(r.get("attr_vals_count", 0) for r in results)
    # Compile counts and flix summary
    flix_summary: Dict[str, int] = {"rendered": 0, "outer": 0, "timeout": 0, "unavailable": 0, "error": 0}
    for r in results:
        st = (r.get("flixmedia_status") or "").lower()
        if st in flix_summary:
            flix_summary[st] += 1
    counts = {
        "products_list_total": len(product_ids),
        "products_fetched": len(results),
        "prices_total": len(prices),
        "stock_levels_total": len(stocks),
        "attributes_total": attributes_total,
        "attribute_values_total": attr_values_total,
        "flixmedia_rendered": flix_summary.get("rendered", 0),
        "flixmedia_outer": flix_summary.get("outer", 0),
        "flixmedia_timeout": flix_summary.get("timeout", 0),
        "flixmedia_unavailable": flix_summary.get("unavailable", 0),
        "flixmedia_error": flix_summary.get("error", 0),
    }
    if cfg.single_file_only:
        # Update counts inside meta, preserve existing master structure
        if master is not None:
            meta_counts = master["meta"].get("counts", {})
            meta_counts.update(counts)
            master["meta"]["counts"] = meta_counts
            save_json(os.path.join(paths["root"], "products_master.json"), master)
    else:
        if not cfg.kv_only:
            save_json(os.path.join(paths["root"], "counts.json"), counts)
            # 6) Write single aggregated file with all enriched products
            save_json(os.path.join(paths["root"], "products_enriched.json"), enriched_list)

    # Always write the KV-only aggregated file for convenience
    save_json(kv_master_path, kv_map)

    print("Done.")
    print(json.dumps(counts, indent=2))


def parse_args() -> argparse.Namespace:
    def env_bool(name: str, default: bool) -> bool:
        val = os.getenv(name)
        if val is None:
            return default
        return str(val).strip().lower() in ("1", "true", "yes", "y")

    parser = argparse.ArgumentParser(description="Fetch maximum product info from Prenta API")
    parser.add_argument("--base-url", default=os.getenv("PRENTA_BASE_URL", DEFAULT_BASE_URL), help="Base API URL (default: sandbox v1)")
    parser.add_argument("--username", default=os.getenv("PRENTA_USERNAME", ""), help="Basic auth username (or set PRENTA_USERNAME)")
    parser.add_argument("--password", default=os.getenv("PRENTA_PASSWORD", ""), help="Basic auth password (or set PRENTA_PASSWORD)")
    parser.add_argument("--output", default=os.path.join(os.path.dirname(__file__), "data"), help="Output directory (default: ./data)")
    parser.add_argument("--per-page", type=int, default=int(os.getenv("PRENTA_PER_PAGE", "100")), help="Items per page for list endpoints (max 100)")
    parser.add_argument("--workers", type=int, default=int(os.getenv("PRENTA_WORKERS", "6")), help="Concurrent workers for per-product fetch")
    parser.add_argument("--timeout", type=int, default=int(os.getenv("PRENTA_TIMEOUT", "30")), help="HTTP timeout in seconds")
    parser.add_argument("--retries", type=int, default=int(os.getenv("PRENTA_MAX_RETRIES", "5")), help="Max retries for transient errors")
    parser.add_argument("--newer-than", default=os.getenv("PRENTA_NEWER_THAN"), help="Optional ISO-8601 timestamp for delta sync (applies to applicable endpoints)")
    parser.add_argument("--max-products", type=int, default=int(os.getenv("PRENTA_MAX_PRODUCTS", "0")), help="Optional cap on number of products to fetch details for (0 = no cap)")
    parser.add_argument("--product-ids", default=os.getenv("PRENTA_PRODUCT_IDS"), help="Comma-separated product IDs to fetch only these (overrides --max-products)")
    # SSL verification toggle (default True). You can set PRENTA_VERIFY_SSL=false to disable.
    insecure_default = not env_bool("PRENTA_VERIFY_SSL", True)
    parser.add_argument("--insecure", action="store_true", default=insecure_default, help="Disable TLS certificate verification (useful for sandbox cert issues)")
    # Per-product files are disabled (no toggle)
    # Single-file-only mode (writes only products_master.json with meta + products)
    parser.add_argument("--single-file-only", action="store_true", default=env_bool("PRENTA_SINGLE_FILE_ONLY", False), help="Write only one master JSON (products_master.json) and skip all other outputs")
    parser.add_argument("--flush-every", type=int, default=int(os.getenv("PRENTA_FLUSH_EVERY", "100")), help="When in single-file-only mode, write master file after every N products (default 100; set 1 to flush each product)")
    parser.add_argument("--kv-only", action="store_true", default=env_bool("PRENTA_KV_ONLY", False), help="Only write products_attributes_kv.json (product_id -> attributes_kv). Skips all other outputs and per-product files")
    # FlixMedia rendering options
    parser.add_argument("--flix-render", action="store_true", default=env_bool("PRENTA_FLIX_RENDER", True), help="Render description_flixmedia embeds to static HTML using Playwright and include as flixmedia_html")
    parser.add_argument("--flix-timeout-ms", type=int, default=int(os.getenv("PRENTA_FLIX_TIMEOUT_MS", "45000")), help="Max time to wait for FlixMedia content (ms)")
    parser.add_argument("--flix-wait-selector", default=os.getenv("PRENTA_FLIX_WAIT_SELECTOR", "#flix-minisite,#flix-inpage"), help="Comma-separated CSS selectors to extract Flix HTML from")
    parser.add_argument("--flix-concurrency", type=int, default=int(os.getenv("PRENTA_FLIX_CONCURRENCY", "2")), help="Max concurrent Flix renders")
    parser.add_argument("--flix-origin", default=os.getenv("PRENTA_FLIX_ORIGIN", "https://e.prenta.lt/"), help="Origin URL to emulate for Flix rendering (sets Referer and document location)")
    return parser.parse_args()


if __name__ == "__main__":
    # Load environment variables from a local .env file if present
    # This must run BEFORE parse_args so env defaults are populated
    try:
        load_dotenv(find_dotenv(), override=False)
    except Exception:
        pass

    args = parse_args()

    if not args.username or not args.password:
        print("ERROR: Provide Basic Auth via --username/--password or PRENTA_USERNAME/PRENTA_PASSWORD env vars", file=sys.stderr)
        sys.exit(1)

    cfg = ClientConfig(
        base_url=args.base_url,
        username=args.username,
        password=args.password,
        timeout=args.timeout,
        max_retries=args.retries,
        per_page=args.per_page,
        workers=args.workers,
        newer_than=args.newer_than,
        verify_ssl=not args.insecure,
        single_file_only=args.single_file_only,
        flush_every=args.flush_every,
        kv_only=args.kv_only,
        flix_render=args.flix_render,
        flix_timeout_ms=args.flix_timeout_ms,
        flix_wait_selector=args.flix_wait_selector,
        flix_concurrency=args.flix_concurrency,
        flix_origin=args.flix_origin,
    )

    try:
        fetch_all(cfg, args.output)
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(2)
