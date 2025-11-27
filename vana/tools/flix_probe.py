import argparse
import base64
import json
import os
import re
import sys
import time
from typing import Any, Dict, Optional, List
from urllib.parse import urlparse
from bs4 import BeautifulSoup

# --- Extraction helpers ----------------------------------------------------

def walk(obj, path=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from walk(v, f"{path}.{k}" if path else k)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from walk(v, f"{path}[{i}]")
    else:
        yield path, obj


def is_image_url(u: str) -> bool:
    return isinstance(u, str) and (u.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')))


def is_doc_url(u: str) -> bool:
    return isinstance(u, str) and (u.lower().endswith(('.pdf', '.doc', '.docx')))


def extract_from_tjson(data: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "title": None,
        "mpn": None,
        "ean": None,
        "sku": None,
        "images": [],
        "features": [],
        "specs": [],
        "documents": [],
    }
    if not isinstance(data, (dict, list)):
        return out

    # Product meta
    try:
        node = None
        root = data[0] if isinstance(data, list) and data else data
        node = root.get("product_meta") if isinstance(root, dict) else None
        if isinstance(node, dict):
            out["title"] = node.get("product_title") or node.get("product_heading") or node.get("product_description")
            out["mpn"] = node.get("product_mpn")
            out["ean"] = node.get("product_ean")
            out["sku"] = node.get("product_sku")
    except Exception:
        pass

    # Images
    images: List[str] = []
    for p, v in walk(data):
        if isinstance(v, str) and is_image_url(v):
            images.append(v)
        # common object shapes
        if isinstance(v, dict):
            for key in ("url", "image", "img", "src"):
                u = v.get(key)
                if is_image_url(u):
                    images.append(u)
    # de-dup preserve order
    seen = set()
    out["images"] = [x for x in images if not (x in seen or seen.add(x))]

    # Documents (pdf)
    docs: List[Dict[str, Any]] = []
    for p, v in walk(data):
        if isinstance(v, str) and is_doc_url(v):
            docs.append({"title": None, "url": v})
        if isinstance(v, dict):
            u = v.get("url") or v.get("href")
            if is_doc_url(u):
                docs.append({"title": v.get("title") or v.get("name"), "url": u})
    # de-dup by url
    seen = set()
    out["documents"] = [d for d in docs if not (d["url"] in seen or seen.add(d["url"]))]

    # Features: look for items with title + (description/text)
    feats: List[Dict[str, Any]] = []
    def add_feat(obj: Dict[str, Any]):
        t = obj.get("title") or obj.get("heading") or obj.get("name")
        desc = obj.get("description") or obj.get("text") or obj.get("copy")
        img = obj.get("image") or obj.get("img") or obj.get("url")
        if t and (desc or img):
            feats.append({"title": t, "description": desc, "image": img if is_image_url(str(img or "")) else None})
    for p, v in walk(data):
        if isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                    cand_keys = set(k.lower() for k in item.keys())
                    if ("title" in cand_keys or "heading" in cand_keys or "name" in cand_keys) and ("description" in cand_keys or "text" in cand_keys or "copy" in cand_keys):
                        add_feat(item)
        elif isinstance(v, dict):
            cand_keys = set(k.lower() for k in v.keys())
            if ("title" in cand_keys or "heading" in cand_keys or "name" in cand_keys) and ("description" in cand_keys or "text" in cand_keys or "copy" in cand_keys):
                add_feat(v)
    out["features"] = feats

    # Specs: find name/value pairs
    specs: List[Dict[str, Any]] = []
    for p, v in walk(data):
        if isinstance(v, dict):
            name = v.get("name") or v.get("label") or v.get("title")
            value = v.get("value") or v.get("text") or v.get("val")
            if name and (value is not None) and not is_image_url(str(value)):
                specs.append({"name": name, "value": value})
        elif isinstance(v, list):
            # lists of dicts with name/value
            if all(isinstance(it, dict) for it in v):
                for it in v:
                    name = it.get("name") or it.get("label") or it.get("title")
                    value = it.get("value") or it.get("text") or it.get("val")
                    if name and (value is not None) and not is_image_url(str(value)):
                        specs.append({"name": name, "value": value})
    # de-dup by name+value
    seen_pairs = set()
    uniq_specs = []
    for s in specs:
        key = (s["name"], json.dumps(s["value"], ensure_ascii=False) if not isinstance(s["value"], str) else s["value"])
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        uniq_specs.append(s)
    out["specs"] = uniq_specs

    return out
#!/usr/bin/env python3
"""
FlixMedia probe utility (API mode)

Purpose:
- Build Flix t.json URL (API) from EAN/SKU/domain and fetch JSON payload.
- Save summary (flix_probe.json), full payload (flix_probe_data.json) and extracted info (flix_probe_extracted.json).
"""

 

# Paths
ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, "data")
ENRICHED_PATH = os.path.join(DATA_DIR, "products_enriched.json")
PROBE_JSON = os.path.join(DATA_DIR, "flix_probe.json")
PROBE_DATA = os.path.join(DATA_DIR, "flix_probe_data.json")
PROBE_EXTRACT = os.path.join(DATA_DIR, "flix_probe_extracted.json")
PROBE_DESC_HTML = os.path.join(DATA_DIR, "flix_probe_desc.html")


def load_enriched() -> List[Dict[str, Any]]:
    if not os.path.exists(ENRICHED_PATH):
        return []
    with open(ENRICHED_PATH, "r", encoding="utf-8") as f:
        try:
            return json.load(f) or []
        except Exception:
            return []


def pick_product(products: List[Dict[str, Any]], product_id: Optional[int], ean: Optional[str]) -> Optional[Dict[str, Any]]:
    if product_id is not None:
        for p in products:
            if int(p.get("product_id")) == int(product_id):
                return p
    if ean:
        for p in products:
            if str(p.get("ean") or "").strip() == str(ean).strip():
                return p
    return products[0] if products else None


def b64_no_pad(s: str) -> str:
    return base64.b64encode(s.encode("utf-8")).decode("utf-8").rstrip("=")


def parse_snippet_attrs(snippet: Optional[str]) -> Dict[str, Any]:
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


def get_domain_from_origin(origin_url: str) -> str:
    try:
        p = urlparse(origin_url)
        if p.netloc:
            return p.netloc
        # if user passed just a domain without scheme
        return origin_url.strip().lstrip("/")
    except Exception:
        return "e.prenta.lt"


def build_tjson_url(ean: str, sku: Optional[str], dist_id: str, iso: str, fl_iso: str, origin_domain: str) -> str:
    slug = f"{(sku or '')}{ean}{b64_no_pad(origin_domain)}"
    base = f"https://media.flixcar.com/modular/web-api/{iso}/{dist_id}/{slug}/t.json"
    return f"{base}?mpn={sku or ''}&ean={ean}&distId={dist_id}&iso={iso}&flIso={fl_iso}"


def fetch_tjson(url: str) -> Dict[str, Any]:
    try:
        import requests
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            "Accept": "application/json,text/plain;q=0.8,*/*;q=0.5",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://e.prenta.lt/",
        }
        r = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
        try:
            data = r.json()
        except Exception:
            data = None
        return {"status_code": r.status_code, "final_url": r.url, "json": data, "text": (r.text if not data else None)}
    except Exception as e:
        return {"status_code": None, "error": str(e)}


# NOTE: Playwright path removed for this probe. We fetch Flix JSON API directly.


def main() -> int:
    ap = argparse.ArgumentParser(description="FlixMedia probe")
    ap.add_argument("--product-id", type=int, help="Product ID from products_enriched.json")
    ap.add_argument("--ean", help="EAN to probe")
    ap.add_argument("--brand", help="Brand name to inject if snippet is missing values")
    ap.add_argument("--sku", help="SKU to inject if snippet is missing values")
    ap.add_argument("--language", default="en")
    ap.add_argument("--timeout-ms", type=int, default=30000)
    ap.add_argument("--origin", default="https://e.prenta.lt/", help="Origin URL (used to compute domain for Flix slug)")
    ap.add_argument("--save-json", action="store_true", help="Save full Flix JSON to data/flix_probe_data.json")
    args = ap.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)

    # Load product
    products = load_enriched()
    prod = pick_product(products, args.product_id, args.ean)

    snippet = None
    brand = args.brand
    sku = args.sku
    ean = args.ean

    if prod:
        # Prefer the snippet we got from API
        product_obj = prod.get("product") or {}
        snippet = product_obj.get("description_flixmedia") or prod.get("product", {}).get("description_flixmedia")
        if not brand:
            manufacturer = prod.get("manufacturer") or {}
            brand = manufacturer.get("name")
        if not sku:
            sku = prod.get("sku") or product_obj.get("internal_reference")
        if not ean:
            ean = prod.get("ean") or product_obj.get("barcode")

    # Extract attributes from snippet if available
    attrs = parse_snippet_attrs(snippet)
    dist_id = (attrs.get("distributor") or "15151").strip()
    iso = (attrs.get("language") or args.language or "en").strip()
    fl_iso = (attrs.get("fallback_language") or "lt").strip()
    # Fill identifiers
    ean = ean or attrs.get("ean") or (prod.get("ean") if prod else None)
    if not ean:
        print("ERROR: EAN required. Provide --ean or ensure product has barcode.", file=sys.stderr)
        return 2
    if not sku:
        sku = attrs.get("sku") or (prod.get("sku") if prod else None) or (prod.get("product", {}).get("internal_reference") if prod else None)
    # Compute slug domain from origin
    origin_domain = get_domain_from_origin(args.origin)
    tjson_url = build_tjson_url(ean=str(ean), sku=sku, dist_id=str(dist_id), iso=str(iso), fl_iso=str(fl_iso), origin_domain=origin_domain)
    resp = fetch_tjson(tjson_url)

    # Save outputs
    # Prepare preview of JSON
    data = resp.get("json")
    json_preview = None
    if isinstance(data, (dict, list)):
        try:
            json_preview = json.dumps(data, ensure_ascii=False)[:2000]
        except Exception:
            json_preview = None
    extracted = extract_from_tjson(data) if isinstance(data, (dict, list)) else {}

    # Build a minimal HTML description from key features (image + text)
    features_html = None
    features_count = 0
    try:
        root = data[0] if isinstance(data, list) and data else data
        key_features_html = (
            (((root or {}).get("modules") or {}).get("hotspot") or {}).get("key_features", {})
        ).get("html")
        if isinstance(key_features_html, str) and key_features_html.strip():
            soup = BeautifulSoup(key_features_html, "html.parser")
            rows = soup.select(".hotspot_table_row")
            parts: List[str] = []
            for row in rows:
                # image (left or right)
                img = row.select_one(".hotspot_feature_image_left img, .hotspot_feature_image_right img, img")
                img_src = (img.get("src") if img else None) or ""
                if img_src.startswith("//"):
                    img_src = "https:" + img_src
                img_alt = img.get("alt") if img else ""
                # text
                desc_el = row.select_one(".hotspot_feature_desc") or row
                # title may be in strong or heading
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
                # body paragraphs (exclude the one that only wraps <strong>)
                body_ps: List[str] = []
                if desc_el:
                    for p in desc_el.find_all("p"):
                        txt = p.get_text(" ", strip=True)
                        if not txt:
                            continue
                        if strong and p.find("strong") and txt == strong.get_text(" ", strip=True):
                            continue
                        body_ps.append(txt)
                # build minimal block
                figure_html = f'<figure class="flixf-media"><img src="{img_src}" alt="{(title or img_alt or "").replace("\"","&quot;")}"></figure>' if img_src else ""
                title_html = f"<h3>{title}</h3>" if title else ""
                body_html = "".join(f"<p>{re.sub(r'<[^>]+>', '', t)}</p>" for t in body_ps)
                parts.append(f"<article class=\"flixf-item\">{figure_html}<div class=\"flixf-body\">{title_html}{body_html}</div></article>")
            if parts:
                features_count = len(parts)
                features_html = (
                    "<!DOCTYPE html><meta charset=\"utf-8\"><section class=\"flix-features\">"
                    + "\n".join(parts)
                    + "</section>"
                )
    except Exception:
        pass
    out = {
        "input": {
            "product_id": prod.get("product_id") if prod else None,
            "brand": brand,
            "sku": sku,
            "ean": ean,
            "timeout_ms": args.timeout_ms,
            "origin_domain": origin_domain,
        },
        "mode": "api",
        "tjson_url": tjson_url,
        "tjson_status": resp.get("status_code"),
        "tjson_final_url": resp.get("final_url"),
        "error": resp.get("error"),
        "json_preview": json_preview,
        "extracted_counts": {
            "images": len(extracted.get("images", [])) if extracted else 0,
            "features": len(extracted.get("features", [])) if extracted else 0,
            "specs": len(extracted.get("specs", [])) if extracted else 0,
            "documents": len(extracted.get("documents", [])) if extracted else 0,
        },
        "features_html_count": features_count,
        "features_html_path": PROBE_DESC_HTML if features_html else None,
    }
    with open(PROBE_JSON, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    if args.save_json and isinstance(data, (dict, list)):
        with open(PROBE_DATA, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    # Always write extracted for convenience
    if isinstance(extracted, dict):
        with open(PROBE_EXTRACT, "w", encoding="utf-8") as f:
            json.dump(extracted, f, ensure_ascii=False, indent=2)
    # Write minimal features HTML if available
    if isinstance(features_html, str) and features_html:
        with open(PROBE_DESC_HTML, "w", encoding="utf-8") as f:
            f.write(features_html)

    # Print short summary + preview
    summary = {k: out.get(k) for k in ["mode", "tjson_status"]}
    print(json.dumps(summary, indent=2))
    if json_preview:
        print("\n--- JSON preview (first 400 chars) ---\n")
        print(json_preview[:400])
        print("\n--- end preview ---\n")
    # Print extraction summary
    print(f"Extracted -> images: {out['extracted_counts']['images']}, features: {out['extracted_counts']['features']}, specs: {out['extracted_counts']['specs']}, documents: {out['extracted_counts']['documents']}")
    print(f"Saved: {PROBE_JSON}")
    if args.save_json and isinstance(data, (dict, list)):
        print(f"Saved JSON: {PROBE_DATA}")
    print(f"Saved extracted: {PROBE_EXTRACT}")
    if features_html:
        print(f"Saved features HTML: {PROBE_DESC_HTML} (items: {features_count})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
