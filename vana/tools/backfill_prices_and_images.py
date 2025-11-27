#!/usr/bin/env python3
"""
Backfill helper for Prenta pipeline.

Normalises description <img> src values from Innpro absolute URLs back to
local data paths and ensures regular_price uses RRP when available across:
  * data/processed/products_grouped.json
  * data/processed/products/*.json
  * data/tõlgitud/products_translated_grouped.json

Usage:
    C:/Python313/python.exe tools/backfill_prices_and_images.py

Run from project root after applying recent pipeline fixes.
"""

from __future__ import annotations

import json
import re
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_GROUPED = BASE_DIR / "data" / "processed" / "products_grouped.json"
PROCESSED_DIR = BASE_DIR / "data" / "processed" / "products"
TRANSLATED_GROUPED = BASE_DIR / "data" / "tõlgitud" / "products_translated_grouped.json"

INNPRO_IMG_PATTERN = re.compile(r"https://b2b\.innpro\.eu(?:data)?/processed/([^\"'\s>]+)")


def _pick_rrp(source: dict | None) -> str:
    if not isinstance(source, dict):
        return ""
    detail = source.get("detail")
    if isinstance(detail, dict):
        rrp = detail.get("price_rrp")
        if rrp:
            return str(rrp)
    price = source.get("price")
    if isinstance(price, dict):
        for key in ("rrp", "recommended_price"):
            val = price.get(key)
            if val:
                return str(val)
    return ""


def _rewrite_product(prod: dict) -> tuple[bool, bool]:
    """Return (desc_changed, price_changed)."""
    desc_changed = price_changed = False

    desc = prod.get("description")
    if isinstance(desc, str) and "b2b.innpro.eu" in desc:
        new_desc = INNPRO_IMG_PATTERN.sub(lambda m: f"data/processed/{m.group(1)}", desc)
        if new_desc != desc:
            prod["description"] = new_desc
            desc_changed = True

    source = prod.get("source")
    rrp = _pick_rrp(source)
    if rrp:
        if prod.get("regular_price") != rrp:
            prod["regular_price"] = rrp
            price_changed = True

    return desc_changed, price_changed


def _rewrite_grouped(path: Path) -> tuple[int, int]:
    if not path.exists():
        return 0, 0
    data = json.loads(path.read_text(encoding="utf-8"))

    desc_changes = price_changes = 0
    if isinstance(data, dict):
        for items in data.values():
            for prod in items or []:
                changed = _rewrite_product(prod)
                desc_changes += int(changed[0])
                price_changes += int(changed[1])

    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return desc_changes, price_changes


def _rewrite_individual(dir_path: Path) -> tuple[int, int]:
    if not dir_path.exists():
        return 0, 0
    desc_changes = price_changes = 0
    for fp in dir_path.glob("*.json"):
        try:
            prod = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            continue
        changed = _rewrite_product(prod)
        if any(changed):
            fp.write_text(json.dumps(prod, ensure_ascii=False, indent=2), encoding="utf-8")
            desc_changes += int(changed[0])
            price_changes += int(changed[1])
    return desc_changes, price_changes


def main() -> None:
    print("➡️  Backfilling description image sources and prices…")

    t_desc, t_price = _rewrite_grouped(TRANSLATED_GROUPED)
    print(f"   • Translated grouped: descriptions={t_desc}, prices={t_price}")

    p_desc, p_price = _rewrite_grouped(PROCESSED_GROUPED)
    print(f"   • Processed grouped: descriptions={p_desc}, prices={p_price}")

    i_desc, i_price = _rewrite_individual(PROCESSED_DIR)
    print(f"   • Individual processed products: descriptions={i_desc}, prices={i_price}")

    print("✅ Backfill complete. Rerun Stage 5 uploader to push updates to WooCommerce.")


if __name__ == "__main__":
    main()
