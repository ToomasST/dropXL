#!/usr/bin/env python3
"""Patch WooCommerce grouped products with Flix fallback HTML.

Reads the consolidated Stage 1 payload (1_samm_algandmed.json) and updates the
`description_flix` field inside data/processed/products_grouped.json (or a
user-specified target) wherever the universal builder produced HTML.

Usage examples
--------------
# dry-run summary only
python tools/patch_grouped_flix.py --dry-run

# write patched file next to the original (suffix "_flixpatched")
python tools/patch_grouped_flix.py

# overwrite the grouped file in-place (creates .bak backup by default)
python tools/patch_grouped_flix.py --in-place
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ALG_PATH = ROOT / "data" / "1_samm_algandmed.json"
DEFAULT_GROUPED_PATH = ROOT / "data" / "processed" / "products_grouped.json"
DEFAULT_OUTPUT_PATH = ROOT / "data" / "processed" / "products_grouped_flixpatched.json"


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def _collect_flix_html(products: Iterable[Dict[str, Any]]) -> Dict[int, str]:
    lookup: Dict[int, str] = {}
    for product in products:
        html = (product.get("flix_description_html") or "").strip()
        if not html:
            continue
        product_id = product.get("product_id")
        if product_id is None:
            continue
        try:
            product_id_int = int(product_id)
        except Exception:
            continue
        lookup[product_id_int] = html
    return lookup


def _iter_grouped_entries(grouped_payload: Dict[str, Any]):
    for category, products in grouped_payload.items():
        if not isinstance(products, list):
            continue
        for entry in products:
            yield category, entry


def patch_grouped(
    alg_path: Path,
    grouped_path: Path,
    output_path: Optional[Path],
    in_place: bool,
    backup: bool,
    dry_run: bool,
) -> None:
    if not alg_path.exists():
        raise FileNotFoundError(f"Stage 1 payload not found: {alg_path}")
    if not grouped_path.exists():
        raise FileNotFoundError(f"Grouped products file not found: {grouped_path}")

    stage1 = _load_json(alg_path)
    grouped = _load_json(grouped_path)

    products = stage1.get("products") if isinstance(stage1, dict) else None
    if not isinstance(products, list):
        raise ValueError("Unexpected Stage 1 payload structure: missing 'products' list")

    flix_lookup = _collect_flix_html(products)

    total_entries = 0
    updated_entries = 0
    missing_ids = 0
    empty_html = 0

    for _, entry in _iter_grouped_entries(grouped):
        total_entries += 1
        source = entry.get("source") if isinstance(entry, dict) else None
        product_id = None
        if isinstance(source, dict) and source.get("prenta_product_id") is not None:
            try:
                product_id = int(source.get("prenta_product_id"))
            except Exception:
                product_id = None
        # fallback to SKU match only if product_id missing
        html_value = None
        if product_id is not None:
            html_value = flix_lookup.get(product_id)
        if html_value is None and entry.get("sku"):
            # attempt lookup via SKU -> product data (rarely needed)
            sku = str(entry.get("sku")).strip()
            for prod in products:
                if str(prod.get("sku") or "").strip() == sku:
                    try:
                        product_id = int(prod.get("product_id"))
                    except Exception:
                        product_id = None
                    html_value = flix_lookup.get(product_id) if product_id is not None else None
                    break

        if html_value is None:
            missing_ids += 1
            continue
        if not html_value.strip():
            empty_html += 1
            continue

        current = (entry.get("description_flix") or "").strip()
        if current == html_value.strip():
            continue
        entry["description_flix"] = html_value
        updated_entries += 1

    summary = {
        "stage1_products": len(products),
        "flix_available": len(flix_lookup),
        "grouped_entries": total_entries,
        "updated_entries": updated_entries,
        "missing_matches": missing_ids,
        "empty_html_skipped": empty_html,
    }

    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if dry_run:
        return

    target_path = grouped_path if in_place else (output_path or DEFAULT_OUTPUT_PATH)

    if in_place and backup:
        backup_path = grouped_path.with_suffix(grouped_path.suffix + ".bak")
        shutil.copy2(grouped_path, backup_path)
        print(f"Backup written to {backup_path}")

    _save_json(target_path, grouped)
    print(f"Patched grouped file saved to {target_path}")


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Patch grouped products with Flix fallback HTML")
    parser.add_argument("--alg", default=str(DEFAULT_ALG_PATH), help="Path to 1_samm_algandmed.json")
    parser.add_argument("--grouped", default=str(DEFAULT_GROUPED_PATH), help="Path to products_grouped.json")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="Output path (ignored with --in-place)")
    parser.add_argument("--in-place", action="store_true", help="Overwrite the grouped file in-place")
    parser.add_argument("--no-backup", action="store_true", help="Skip creating .bak when using --in-place")
    parser.add_argument("--dry-run", action="store_true", help="Only print summary, do not write any files")
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    try:
        patch_grouped(
            alg_path=Path(args.alg),
            grouped_path=Path(args.grouped),
            output_path=Path(args.output) if args.output else None,
            in_place=bool(args.in_place),
            backup=not args.no_backup,
            dry_run=bool(args.dry_run),
        )
    except Exception as exc:  # pragma: no cover - defensive logging
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
