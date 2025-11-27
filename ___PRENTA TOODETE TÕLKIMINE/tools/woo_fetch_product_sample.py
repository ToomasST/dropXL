#!/usr/bin/env python3

"""Abi-skript: tõmba WooCommerce'ist ühe toote täis JSON.

Kasutab sama keskkonda, mis 5. samm üleslaadimisel:
- WC_SITE_URL või WP_BASE_URL (baas-URL)
- WC_CONSUMER_KEY / WC_CONSUMER_SECRET (Woo REST võtmed)

Kasutus:
    python tools/woo_fetch_product_sample.py --sku FSE76738P --output woo_product_FSE76738P.json

Vaikimisi SKU: FSE76738P
Vaikimisi väljundfail: woo_product_<SKU>.json (salvestatakse uue töövoo juurkausta)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv, find_dotenv


ROOT = Path(__file__).resolve().parent.parent


def log(msg: str) -> None:
    print(msg)


def fetch_product_by_sku(base_url: str, auth: tuple[str, str], sku: str) -> List[Dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/wp-json/wc/v3/products"
    params = {"sku": sku, "per_page": 10}
    resp = requests.get(url, auth=auth, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected response type from Woo: {type(data)!r}")
    return data


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Tõmba WooCommerce'ist ühe SKU täis JSON")
    parser.add_argument("--sku", default="FSE76738P", help="Toote SKU, mille järgi Woo's otsida")
    parser.add_argument(
        "--output",
        default=None,
        help="Väljundfaili nimi (vaikimisi: woo_product_<SKU>.json uue töövoo juurkaustas)",
    )
    args = parser.parse_args(argv)

    load_dotenv(find_dotenv(), override=False)

    site_url = os.getenv("WP_BASE_URL") or os.getenv("WC_SITE_URL")
    if not site_url:
        log("❌ Puudub WP_BASE_URL või WC_SITE_URL .env failis")
        return 1

    ck = os.getenv("WC_CONSUMER_KEY")
    cs = os.getenv("WC_CONSUMER_SECRET")
    if not ck or not cs:
        log("❌ Puudub WC_CONSUMER_KEY / WC_CONSUMER_SECRET .env failis")
        return 1

    sku = str(args.sku).strip()
    if not sku:
        log("❌ SKU ei tohi olla tühi")
        return 1

    output_name = args.output or f"woo_product_{sku}.json"
    output_path = ROOT / output_name

    log(f"🔗 WooCommerce: {site_url}")
    log(f"🔎 Otsin toodet SKU järgi: {sku}")

    try:
        products = fetch_product_by_sku(site_url, (ck, cs), sku)
    except Exception as exc:
        log(f"❌ Viga Woo päringul: {exc}")
        return 1

    if not products:
        log("⚠️ Woo ei tagastanud ühtegi toodet selle SKU-ga")
    else:
        log(f"✔ Leitud {len(products)} toodet. Salvestan JSON faili: {output_path.name}")

    try:
        with output_path.open("w", encoding="utf-8") as fh:
            json.dump(products, fh, ensure_ascii=False, indent=2)
    except Exception as exc:
        log(f"❌ Ei suutnud kirjutada faili {output_path}: {exc}")
        return 1

    log("🎉 Valmis.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
