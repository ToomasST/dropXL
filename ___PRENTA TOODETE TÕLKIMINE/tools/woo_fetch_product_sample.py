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


def _meta_map(prod: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    try:
        for m in prod.get("meta_data") or []:
            if not isinstance(m, dict):
                continue
            k = str(m.get("key") or "").strip()
            if not k:
                continue
            out[k] = m.get("value")
    except Exception:
        return out
    return out


def _load_json_list(path: Path) -> List[Dict[str, Any]]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Failed to read JSON from {path}: {exc}")
    if isinstance(raw, list):
        return [it for it in raw if isinstance(it, dict)]
    if isinstance(raw, dict):
        return [raw]
    raise RuntimeError(f"Unexpected JSON structure in {path}: {type(raw)!r}")


def _diff_meta(old_prod: Dict[str, Any], new_prod: Dict[str, Any]) -> None:
    old_meta = _meta_map(old_prod)
    new_meta = _meta_map(new_prod)
    old_keys = set(old_meta.keys())
    new_keys = set(new_meta.keys())

    added = sorted(new_keys - old_keys)
    removed = sorted(old_keys - new_keys)
    changed = sorted(k for k in (old_keys & new_keys) if old_meta.get(k) != new_meta.get(k))

    if not added and not removed and not changed:
        log("ℹ️ meta_data: erinevusi ei leitud")
        return

    if added:
        log(f"➕ meta_data lisatud võtmed ({len(added)}):")
        for k in added:
            log(f"   + {k} = {new_meta.get(k)!r}")
    if removed:
        log(f"➖ meta_data eemaldatud võtmed ({len(removed)}):")
        for k in removed:
            log(f"   - {k} (vana väärtus={old_meta.get(k)!r})")
    if changed:
        log(f"✏️ meta_data muutunud võtmed ({len(changed)}):")
        for k in changed:
            log(f"   * {k}: {old_meta.get(k)!r} -> {new_meta.get(k)!r}")

    interesting = [
        k
        for k in (added + changed)
        if any(tok in k.lower() for tok in ("montonio", "parcel", "pickup", "ship"))
    ]
    if interesting:
        log("🔎 Võimalikud Montonio/parcel seotud võtmed:")
        for k in interesting:
            log(f"   ! {k} = {new_meta.get(k)!r}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Tõmba WooCommerce'ist ühe SKU täis JSON")
    parser.add_argument("--sku", default="EIS62453IZ", help="Toote SKU, mille järgi Woo's otsida")
    parser.add_argument(
        "--output",
        default=None,
        help="Väljundfaili nimi (vaikimisi: woo_product_<SKU>.json uue töövoo juurkaustas)",
    )
    parser.add_argument(
        "--diff-with",
        dest="diff_with",
        default=None,
        help="Võrdle meta_data erinevusi olemasoleva JSON-iga (nt enne/peale checkboxi muutmist)",
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

    diff_path = str(args.diff_with).strip() if args.diff_with else ""
    if diff_path:
        try:
            old_path = Path(diff_path)
            if old_path.exists():
                old_list = _load_json_list(old_path)
                if products and old_list:
                    log(f"🧾 Võrdlen meta_data: {old_path.name} -> (uus päring)")
                    _diff_meta(old_list[0], products[0])
                else:
                    log("⚠️ Diff: üks pool on tühi (vana või uus toode puudub)")
            else:
                log(f"⚠️ Diff: faili ei leitud: {old_path}")
        except Exception as exc:
            log(f"⚠️ Diff ebaõnnestus: {exc}")

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
