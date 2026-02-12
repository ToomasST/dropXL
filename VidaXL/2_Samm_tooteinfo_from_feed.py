#!/usr/bin/env python3
"""2. samm (DropXL feed): mapib VidaXL CSV feedi Step2 skeemile.

- Loeb VidaXL main CSV feedi.
- Filtreerib out-of-stock tooted.
- Piirab esmase jooksu 100 tootega.
- Kraabib variatsioonide SKU-d Product-Variation endpointi abil.
"""

from __future__ import annotations

import csv
import html
import json
import random
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests
from category_change_runner import apply_maps_to_path, DEFAULT_MAPS

ROOT = Path(__file__).resolve().parent
FEED_PATH = ROOT / "data" / "feeds" / "vidaXL_ee_dropshipping" / "vidaXL_ee_dropshipping.csv"
OUTPUT_PATH = ROOT / "2_samm_tooteinfo.json"
CATEGORY_TRANSLATION_PATH = ROOT / "category_translation.json"
TRANSLATED_GROUPED_PATH = ROOT / "data" / "tõlgitud" / "products_translated_grouped.json"

MAX_PRODUCTS = 4000
REQUEST_DELAY_SECONDS = 1.05
VIDAXL_PATTERN = re.compile(r"\bvida\s*x[l]?\b", re.IGNORECASE)
EXCLUDED_CATEGORY_ROOTS = [
    "Mööbel > Diivand",
    "Mööbel > Diivanid",
    "Mööbel > Voodid ja lisad > Voodid ja voodiraamid",
    "Mööbel > Voodid ja lisad > Peatsid ja jalused",
    "Aed > Aiamööbel",
]


def _load_category_translations() -> Dict[str, str]:
    if not CATEGORY_TRANSLATION_PATH.exists():
        return {}
    try:
        with CATEGORY_TRANSLATION_PATH.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    out: Dict[str, str] = {}
    for k, v in data.items():
        key = str(k or "").strip()
        val = str(v or "").strip()
        if key and val:
            out[key] = val
    return out


def _load_existing_output() -> List[Dict[str, Any]]:
    if not OUTPUT_PATH.exists():
        return []
    try:
        with OUTPUT_PATH.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, dict)]


CATEGORY_TRANSLATIONS = _load_category_translations()


def _load_translated_skus() -> set[str]:
    if not TRANSLATED_GROUPED_PATH.exists():
        return set()
    try:
        with TRANSLATED_GROUPED_PATH.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return set()
    if not isinstance(data, dict):
        return set()
    out: set[str] = set()
    for items in data.values():
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            sku = _as_str(item.get("sku") or "").strip()
            if sku:
                out.add(sku)
    return out


def _as_str(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, bool):
        return "True" if val else "False"
    return str(val)


def _clean_vidaxl(text: str) -> str:
    cleaned = VIDAXL_PATTERN.sub("", text)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned.strip()


def _to_int(val: Any) -> int:
    try:
        return int(float(str(val).strip()))
    except Exception:
        return 0


def _to_float(val: Any) -> Optional[float]:
    try:
        return float(str(val).strip())
    except Exception:
        return None


def _build_category(row: Dict[str, str]) -> Dict[str, Any]:
    path_raw = _as_str(row.get("Category") or "").strip()
    path = path_raw if path_raw else ""
    translated = CATEGORY_TRANSLATIONS.get(path) or path
    translated = apply_maps_to_path(translated, DEFAULT_MAPS)
    leaf = ""
    if translated:
        parts = [p.strip() for p in translated.split(">") if p.strip()]
        if parts:
            leaf = parts[-1]
    return {
        "category": {
            "source_id": _as_str(row.get("Category_id") or "").strip() or None,
            "path": path,
            "translated_path": translated,
            "leaf_name": leaf,
        },
        "categories": ([{"name": leaf}] if leaf else []),
    }


def _is_path_under(path: str, root: str) -> bool:
    if not path or not root:
        return False
    return path == root or path.startswith(f"{root} >")


def _is_excluded_category(row: Dict[str, str]) -> bool:
    path_raw = _as_str(row.get("Category") or "").strip()
    translated = CATEGORY_TRANSLATIONS.get(path_raw) or path_raw
    translated = apply_maps_to_path(translated, DEFAULT_MAPS)
    for root in EXCLUDED_CATEGORY_ROOTS:
        if _is_path_under(path_raw, root) or _is_path_under(translated, root):
            return True
    return False


def _build_images(row: Dict[str, str]) -> List[Dict[str, Any]]:
    fields = [f"Image {i}" for i in range(1, 13)] + ["image 13", "Image 13", "Image 14"]
    seen: set[str] = set()
    images: List[Dict[str, Any]] = []
    alt = _clean_vidaxl(_as_str(row.get("Product_title") or row.get("Title") or "").strip())
    for key in fields:
        val = _as_str(row.get(key) or "").strip()
        if not val or val in seen:
            continue
        seen.add(val)
        images.append({"src": val, "alt": alt})
    return images


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text).strip()


def _add_attr(attr_map: Dict[str, List[str]], name: str, value: str) -> None:
    raw_name = name.strip()
    raw_value = value.strip()
    name_key = raw_name.lower().replace(" ", "_")
    name_map = {
        "size": "Suurus",
        "parcel_or_pallet": "Pakkimise tüüp",
    }
    if name_key == "parcel_or_pallet":
        val_key = raw_value.lower()
        if val_key == "parcel":
            raw_value = "pakk"
        elif val_key == "pallet":
            raw_value = "alus"
    name = _clean_vidaxl(name_map.get(name_key, raw_name))
    value = _clean_vidaxl(raw_value)
    if value.lower() == "yes":
        value = "Jah"
    elif value.lower() == "no":
        value = "Ei"
    if not name or not value:
        return
    bucket = attr_map.setdefault(name, [])
    if value not in bucket:
        bucket.append(value)


def _parse_properties(row: Dict[str, str]) -> List[Dict[str, Any]]:
    attr_map: Dict[str, List[str]] = {}
    props = _as_str(row.get("Properties") or "").strip()
    if props:
        for item in re.findall(r"<li>(.*?)</li>", props, flags=re.I | re.S):
            clean = _strip_html(item)
            if not clean:
                continue
            if ":" in clean:
                key, val = clean.split(":", 1)
                _add_attr(attr_map, key, val)
            else:
                _add_attr(attr_map, clean, "Yes")

    for field in [
        "Color",
        "Gender",
        "Diameter",
        "Size",
        "Parcel_or_pallet",
        "Number_of_packages",
        "Product_volume",
    ]:
        value = _as_str(row.get(field) or "").strip()
        if value:
            _add_attr(attr_map, field, value)

    attributes: List[Dict[str, Any]] = []
    for name, values in attr_map.items():
        if values:
            attributes.append({"name": name, "values": values})
    return attributes


def _extract_swatch_urls(page_html: str) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    for label, url in re.findall(r'aria-label="([^"]+)"[^>]*data-url="([^"]+)"', page_html):
        label = html.unescape(_as_str(label).strip())
        url = html.unescape(_as_str(url).strip())
        if not url or not label:
            continue
        if "Product-Variation" not in url:
            continue
        if " " in label:
            attr_name, attr_value = label.split(" ", 1)
        else:
            attr_name, attr_value = "Variant", label
        results.append({"attr_name": attr_name.strip(), "attr_value": attr_value.strip(), "url": url})
    return results


def _fetch_variation_sku(url: str, cache: Dict[str, str]) -> Optional[str]:
    if url in cache:
        return cache[url]
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return None
    sku = None
    if isinstance(data, dict):
        product = data.get("product")
        if isinstance(product, dict):
            sku = _as_str(product.get("SKU") or "").strip()
    if sku:
        cache[url] = sku
    return sku


def _build_variant_skus(link: str, cache: Dict[str, str]) -> Dict[str, Dict[str, List[str]]]:
    if not link:
        return {}
    try:
        resp = requests.get(link, timeout=30)
        resp.raise_for_status()
        html_text = resp.text
    except Exception:
        return {}

    variant_skus: Dict[str, Dict[str, List[str]]] = {}
    for item in _extract_swatch_urls(html_text):
        sku = _fetch_variation_sku(item["url"], cache)
        if not sku:
            continue
        attr_name = item["attr_name"]
        attr_value = item["attr_value"]
        bucket = variant_skus.setdefault(attr_name, {})
        skus = bucket.setdefault(attr_value, [])
        if sku not in skus:
            skus.append(sku)
        time.sleep(REQUEST_DELAY_SECONDS)
    return variant_skus


def _build_product(row: Dict[str, str], variant_cache: Dict[str, str]) -> Dict[str, Any]:
    sku = _as_str(row.get("SKU") or "").strip()
    name = _clean_vidaxl(_as_str(row.get("Product_title") or row.get("Title") or "").strip())
    link = _as_str(row.get("Link") or "").strip()
    stock = _to_int(row.get("Stock"))
    purchase_price = _to_float(row.get("B2B price"))
    rrp_price = _to_float(row.get("Webshop price"))
    description = _as_str(row.get("HTML_description") or row.get("Description") or "").strip()

    cat_info = _build_category(row)
    brand_name = _clean_vidaxl(_as_str(row.get("Brand") or "").strip())
    brands = [{"name": brand_name}] if brand_name else []

    variant_skus = _build_variant_skus(link, variant_cache)
    product: Dict[str, Any] = {
        "sku": sku,
        "global_unique_id": _as_str(row.get("EAN") or "").strip(),
        "source": {
            "source_system": "dropxl_feed",
            "source_product_id": sku,
            "source_category_ids": [row.get("Category_id") or None],
            "rrp_price": rrp_price,
            "purchase_price": purchase_price,
            "source_product_url": link,
        },
        "name": name,
        "short_description": "",
        "description": description,
        "type": "simple",
        "status": "publish",
        "featured": False,
        "catalog_visibility": "visible",
        "regular_price": _as_str(row.get("Webshop price") or "").strip(),
        "sale_price": "",
        "purchasable": True,
        "virtual": False,
        "tax_status": "taxable",
        "tax_class": "",
        "manage_stock": True,
        "stock_status": "instock" if stock > 0 else "outofstock",
        "stock_quantity": stock,
        "backorders": "no",
        "backorders_allowed": False,
        "backordered": False,
        "low_stock_amount": None,
        "sold_individually": False,
        "weight": _as_str(row.get("Weight") or "").strip(),
        "dimensions": {"length": "", "width": "", "height": ""},
        "shipping_required": True,
        "shipping_taxable": True,
        "shipping_class": "",
        "reviews_allowed": True,
        "average_rating": "0.00",
        "rating_count": 0,
        "upsell_ids": [],
        "cross_sell_ids": [],
        "category": cat_info["category"],
        "categories": cat_info["categories"],
        "brands": brands,
        "tags": [],
        "images": _build_images(row),
        "attributes": _parse_properties(row),
        "default_attributes": [],
        "variations": [],
        "grouped_products": [],
        "menu_order": 0,
        "related_ids": [],
        "meta_data": [
            {"key": "_bp_gtin13", "value": _as_str(row.get("EAN") or "").strip()},
            {"key": "_bp_supplier", "value": "DropXL"},
        ],
        "variant_skus": variant_skus,
    }

    if brand_name:
        product["meta_data"].append({"key": "_bp_brand", "value": brand_name})

    color_variants = variant_skus.get("Värv") if isinstance(variant_skus, dict) else None
    if isinstance(color_variants, dict) and color_variants:
        color_skus: List[str] = []
        for values in color_variants.values():
            if not isinstance(values, list):
                continue
            for sku_val in values:
                sku_str = _as_str(sku_val).strip()
                if sku_str and sku_str not in color_skus:
                    color_skus.append(sku_str)
        if color_skus:
            product["meta_data"].append({"key": "_bp_color_match_sku", "value": ",".join(color_skus)})

    return product


def main() -> int:
    if not FEED_PATH.exists():
        raise SystemExit(f"Feed not found: {FEED_PATH}")

    print("NOTICE: Ajutiselt ei lisata tooteid kategooriatest:")
    for root in EXCLUDED_CATEGORY_ROOTS:
        print(f"  - {root}")

    def _write_partial_output(items: List[Dict[str, Any]]) -> None:
        with OUTPUT_PATH.open("w", encoding="utf-8") as fh:
            json.dump(items, fh, ensure_ascii=False, indent=2)

    products: List[Dict[str, Any]] = _load_existing_output()
    variant_cache: Dict[str, str] = {}
    translated_skus = _load_translated_skus()
    existing_skus = { _as_str(p.get("sku") or "").strip() for p in products if isinstance(p, dict) }
    existing_skus.discard("")
    processed = 0
    eligible_total = 0
    with FEED_PATH.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if _is_excluded_category(row):
                continue
            sku = _as_str(row.get("SKU") or "").strip()
            if _to_int(row.get("Stock")) > 0 and sku not in translated_skus and sku not in existing_skus:
                eligible_total += 1

    target_total = min(MAX_PRODUCTS, eligible_total)
    if target_total == 0:
        with OUTPUT_PATH.open("w", encoding="utf-8") as fh:
            json.dump(products, fh, ensure_ascii=False, indent=2)
        summary = {
            "input_file": str(FEED_PATH),
            "output_file": str(OUTPUT_PATH),
            "counts": {"products_out": len(products)},
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    segments = min(3, target_total)
    base = target_total // segments
    remainder = target_total % segments
    segment_sizes = [base + (1 if idx < remainder else 0) for idx in range(segments)]

    chosen_indices: set[int] = set()
    for idx, seg_size in enumerate(segment_sizes):
        if seg_size <= 0:
            continue
        seg_start = (eligible_total * idx) // segments
        seg_end = (eligible_total * (idx + 1)) // segments - 1
        if seg_end < seg_start:
            seg_end = seg_start
        seg_range = list(range(seg_start, seg_end + 1))
        pick_count = min(seg_size, len(seg_range))
        chosen_indices.update(random.sample(seg_range, pick_count))

    remaining = target_total - len(chosen_indices)
    if remaining > 0:
        all_indices = [idx for idx in range(eligible_total) if idx not in chosen_indices]
        chosen_indices.update(random.sample(all_indices, min(remaining, len(all_indices))))

    selected_indices = sorted(chosen_indices)
    selected_set = set(selected_indices)

    eligible_seen = 0
    with FEED_PATH.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            processed += 1
            if _to_int(row.get("Stock")) <= 0:
                continue
            if _is_excluded_category(row):
                continue
            sku = _as_str(row.get("SKU") or "").strip()
            if sku in translated_skus or sku in existing_skus:
                continue
            if eligible_seen in selected_set:
                products.append(_build_product(row, variant_cache))
                _write_partial_output(products)
                print(f"✔ Töödeldud: {processed} rida | valimis: {len(products)}/{target_total}")
                if len(products) >= target_total:
                    break
            eligible_seen += 1

    _write_partial_output(products)

    summary = {
        "input_file": str(FEED_PATH),
        "output_file": str(OUTPUT_PATH),
        "counts": {"products_out": len(products)},
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
