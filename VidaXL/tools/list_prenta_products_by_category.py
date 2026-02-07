#!/usr/bin/env python3
"""List Prenta products for a given category path using the Prenta API.

Usage examples (from the PRENTA project root or ___PRENTA TOODETE TÕLKIMINE kaustast):

    python tools/list_prenta_products_by_category.py \
        --path "All > Saleable > Home appliances > Free standing > Set"

This will print lines like:

    38599 | PRF0180299A | Product name

The script uses:
- data/category_catalog.json (new workflow) to map category path -> category_id
- vana/data/category_catalog.json as a fallback
- prenta_fetch.ClientConfig / PrentaClient for API access

API credentials are read from the existing .env variables:
- PRENTA_BASE_URL
- PRENTA_USERNAME
- PRENTA_PASSWORD
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, Optional

from dotenv import load_dotenv

# Ensure project root (where prenta_fetch.py lives) is on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from prenta_fetch import ClientConfig, PrentaClient


DATA_DIR = ROOT / "data"
VANA_DATA_DIR = ROOT.parent / "vana" / "data"


def load_category_catalog() -> Iterable[Dict[str, Any]]:
    """Load category_catalog.json from new or vana location.

    Returns an iterable of category dicts with at least keys: id, path, name.
    """

    candidates = [
        DATA_DIR / "category_catalog.json",
        VANA_DATA_DIR / "category_catalog.json",
    ]
    for fp in candidates:
        if fp.exists():
            try:
                with fp.open("r", encoding="utf-8") as fh:
                    data = json.load(fh)
                if isinstance(data, list):
                    return [d for d in data if isinstance(d, dict)]
            except Exception:
                continue
    return []


def find_category_id_by_path(path: str) -> Optional[int]:
    cats = load_category_catalog()
    for c in cats:
        try:
            if str(c.get("path") or "").strip() == path.strip():
                cid = c.get("id")
                if cid is not None:
                    return int(cid)
        except Exception:
            continue
    return None


def build_prenta_client_from_env() -> PrentaClient:
    base_url = os.getenv("PRENTA_BASE_URL") or ClientConfig.base_url
    username = os.getenv("PRENTA_USERNAME", "")
    password = os.getenv("PRENTA_PASSWORD", "")
    timeout = int(os.getenv("PRENTA_TIMEOUT", "30"))
    max_retries = int(os.getenv("PRENTA_MAX_RETRIES", "5"))
    per_page = int(os.getenv("PRENTA_PER_PAGE", "100"))
    verify_ssl_env = str(os.getenv("PRENTA_VERIFY_SSL", "false")).strip().lower()
    verify_ssl = verify_ssl_env in {"1", "true", "yes", "y"}

    cfg = ClientConfig(
        base_url=base_url,
        username=username,
        password=password,
        timeout=timeout,
        max_retries=max_retries,
        per_page=max(1, min(per_page, 100)),
        verify_ssl=verify_ssl,
    )
    return PrentaClient(cfg)


def iter_products_by_category(client: PrentaClient, category_id: int) -> Iterable[Dict[str, Any]]:
    """Iterate products for a given category_id via /products endpoint.

    Uses minimal fields for quick inspection.
    """

    params: Dict[str, Any] = {
        "category_id": int(category_id),
        "fields": "id,internal_reference,name,category_id",
        "order": "internal_reference",
    }
    # We reuse the internal _paged helper here; it already handles paging/retries
    for item in client._paged("products", params):  # type: ignore[attr-defined]
        if isinstance(item, dict):
            yield item


def main() -> int:
    parser = argparse.ArgumentParser(description="List Prenta products for a given category path")
    parser.add_argument(
        "--path",
        required=True,
        help="Category path string, e.g. 'All > Saleable > Home appliances > Free standing > Set'",
    )
    args = parser.parse_args()

    load_dotenv()

    cat_path = args.path.strip()
    if not cat_path:
        print("⚠️  Empty category path")
        return 1

    cid = find_category_id_by_path(cat_path)
    if cid is None:
        print(f"⚠️  Category not found in category_catalog.json for path: {cat_path}")
        return 1

    print(f"Category path: {cat_path}")
    print(f"Category ID:   {cid}")
    print("Fetching products from Prenta API...\n")

    client = build_prenta_client_from_env()

    count = 0
    for prod in iter_products_by_category(client, cid):
        pid = prod.get("id")
        sku = prod.get("internal_reference")
        name = prod.get("name")
        print(f"{pid} | {sku} | {name}")
        count += 1

    print(f"\nTotal products found: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
