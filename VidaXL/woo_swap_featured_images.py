#!/usr/bin/env python3
"""Swap featured image with second image for specific WooCommerce SKUs."""

from __future__ import annotations

import os
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import find_dotenv, load_dotenv


DRY_RUN = False
TARGET_SKUS = [
    "135797",
    "135799",
    "135802",
]


def _get_site_and_auth() -> Tuple[Optional[str], Optional[Tuple[str, str]]]:
    load_dotenv(find_dotenv())
    site = os.getenv("WC_SITE_URL") or os.getenv("WP_BASE_URL")
    ck = os.getenv("WC_CONSUMER_KEY")
    cs = os.getenv("WC_CONSUMER_SECRET")
    if ck and cs:
        return site, (ck, cs)
    user = os.getenv("WP_USERNAME")
    app_pass = os.getenv("WP_APP_PASSWORD")
    if user and app_pass:
        return site, (user, app_pass)
    return site, None


def _fetch_product_by_sku(site: str, auth: Tuple[str, str], sku: str) -> List[Dict]:
    url = f"{site}/wp-json/wc/v3/products"
    params = {
        "per_page": 1,
        "sku": sku,
        "_fields": "id,name,images,sku",
    }
    resp = requests.get(url, auth=auth, params=params, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Woo fetch failed (sku {sku}): HTTP {resp.status_code} {resp.text[:200]}")
    data = resp.json()
    return data if isinstance(data, list) else []


def _build_swapped_images(images: List[Dict]) -> List[Dict]:
    swapped = images[:]
    swapped[0], swapped[1] = swapped[1], swapped[0]
    payload_images: List[Dict] = []
    for idx, im in enumerate(swapped):
        img_id = im.get("id")
        img_src = im.get("src")
        if img_id:
            payload_images.append({"id": img_id, "position": idx})
        elif img_src:
            payload_images.append({"src": img_src, "position": idx})
    return payload_images


def _update_product_images(site: str, auth: Tuple[str, str], product_id: int, images: List[Dict]) -> None:
    url = f"{site}/wp-json/wc/v3/products/{product_id}"
    resp = requests.put(url, auth=auth, json={"images": images}, timeout=30)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Woo update failed (id {product_id}): HTTP {resp.status_code} {resp.text[:200]}")


def main() -> int:
    site, auth = _get_site_and_auth()
    if not site or not auth:
        print("❌ Missing WC_SITE_URL/WP_BASE_URL or credentials (WC_CONSUMER_KEY/SECRET or WP_USERNAME/APP_PASSWORD).")
        return 1

    matched = 0
    updated = 0
    skus = [str(s).strip() for s in TARGET_SKUS if str(s).strip()]
    for sku in skus:
        products = _fetch_product_by_sku(site, auth, sku)
        if not products:
            print(f"[SKIP] SKU {sku} — ei leitud")
            continue
        for prod in products:
            name = str(prod.get("name") or "")
            images = prod.get("images") or []
            if len(images) < 2:
                print(f"[SKIP] SKU {sku} ID {prod.get('id')} '{name}' — vähem kui 2 pilti")
                continue
            matched += 1
            payload_images = _build_swapped_images(images)
            if DRY_RUN:
                print(f"[DRY] SKU {sku} ID {prod.get('id')} '{name}' — vahetan 1. ja 2. pildi")
                continue
            _update_product_images(site, auth, int(prod.get("id")), payload_images)
            updated += 1
            print(f"[OK] SKU {sku} ID {prod.get('id')} '{name}' — peapilt vahetatud")

    print(f"Valmis. Sobivaid tooteid: {matched}, uuendatud: {updated}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
