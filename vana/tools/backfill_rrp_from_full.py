#!/usr/bin/env python3
"""Backfill regular_price with RRP values from data/latest/full.json.

Usage (run from project root):
    C:/Python313/python.exe tools/backfill_rrp_from_full.py
"""
from __future__ import annotations

import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
FULL_PATH = BASE_DIR / "data" / "latest" / "full.json"
PROCESSED_GROUPED = BASE_DIR / "data" / "processed" / "products_grouped.json"
PROCESSED_DIR = BASE_DIR / "data" / "processed" / "products"
TRANSLATED_GROUPED = BASE_DIR / "data" / "tõlgitud" / "products_translated_grouped.json"


def load_rrp_map() -> dict[str, str]:
    full = json.loads(FULL_PATH.read_text(encoding="utf-8"))
    items = (
        full.get("offer", {})
        .get("products", {})
        .get("product", {})
        .get("item", [])
    )
    mapping: dict[str, str] = {}
    for entry in items:
        if not isinstance(entry, dict):
            continue
        sku = str(entry.get("@code_on_card") or "").strip()
        if not sku:
            continue
        detail = ((entry.get("source") or {}).get("detail") or {})
        selected: float | None = None
        if "price_rrp" in detail:
            try:
                selected = float(detail.get("price_rrp"))
            except Exception:
                selected = None
        if selected is None:
            price_node = entry.get("price") or {}
            try:
                gross = price_node.get("@gross")
                if gross is not None:
                    selected = float(gross)
            except Exception:
                selected = None

        if selected is None:
            continue
        existing = mapping.get(sku)
        try:
            existing_val = float(existing) if existing is not None else None
        except Exception:
            existing_val = None

        if existing_val is None or selected > existing_val:
            mapping[sku] = f"{selected:.2f}"
    return mapping


def update_grouped(path: Path, rrp_map: dict[str, str]) -> int:
    if not path.exists():
        return 0
    data = json.loads(path.read_text(encoding="utf-8"))
    changed = 0
    if isinstance(data, dict):
        for items in data.values():
            for prod in items or []:
                sku = str(prod.get("sku") or "").strip()
                rrp = rrp_map.get(sku)
                if rrp and prod.get("regular_price") != rrp:
                    prod["regular_price"] = rrp
                    changed += 1
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return changed


def update_individual(directory: Path, rrp_map: dict[str, str]) -> int:
    if not directory.exists():
        return 0
    changed = 0
    for fp in directory.glob("*.json"):
        prod = json.loads(fp.read_text(encoding="utf-8"))
        sku = str(prod.get("sku") or "").strip()
        rrp = rrp_map.get(sku)
        if rrp and prod.get("regular_price") != rrp:
            prod["regular_price"] = rrp
            fp.write_text(json.dumps(prod, ensure_ascii=False, indent=2), encoding="utf-8")
            changed += 1
    return changed


def main() -> None:
    print("Loading RRP map from data/latest/full.json …")
    rrp_map = load_rrp_map()
    print(f"   • Found RRP entries for {len(rrp_map)} SKUs")

    print("Updating grouped/translated files …")
    trans_changed = update_grouped(TRANSLATED_GROUPED, rrp_map)
    print(f"   • Translated grouped: prices updated for {trans_changed} products")

    proc_changed = update_grouped(PROCESSED_GROUPED, rrp_map)
    print(f"   • Processed grouped: prices updated for {proc_changed} products")

    indiv_changed = update_individual(PROCESSED_DIR, rrp_map)
    print(f"   • Individual processed files: prices updated for {indiv_changed} products")

    print("Done. Run Stage 5 uploader with --update-existing to push new RRPs to WooCommerce.")


if __name__ == "__main__":
    main()
