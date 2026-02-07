#!/usr/bin/env python3
"""DropXL stock/price update runner for WooCommerce.

Kasutus:
    python DropXL_stock_loop_runner.py               # loop iga 1800s järel
    python DropXL_stock_loop_runner.py --interval 900  # loop iga 900s järel
    python DropXL_stock_loop_runner.py --only-sku ABC123 --dry-run
    python DropXL_stock_loop_runner.py --no-update-prices  # ära uuenda hindasid

Reeglid:
- Võtab aluseks WooCommerce'i toodete nimekirja.
- Uuendab ainult Woo tooteid, mille meta `_bp_supplier` == "DropXL".
- Eeldab, et Woo SKU == DropXL CSV feedi SKU.
- Kui Woo DropXL SKU puudub feedis, pannakse laoseis 0 + outofstock (toode jääb alles).
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import requests
from dotenv import find_dotenv, load_dotenv

ROOT = Path(__file__).resolve().parent
OFFER_FEED_URL = (
    "https://feed.vidaxl.io/api/v1/feeds/download/"
    "e8eb166c-2dd3-4930-8c18-0ae5abb33245/EE/vidaXL_ee_dropshipping_offer.csv"
)
OFFER_FEED_PATH = ROOT / "data" / "feeds" / "vidaXL_ee_dropshipping_offer.csv"

PRICE_MARKUP_RATE = 0.10
PRICE_VAT_RATE = 0.24


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def download_offer_feed() -> bool:
    try:
        OFFER_FEED_PATH.parent.mkdir(parents=True, exist_ok=True)
        with requests.get(OFFER_FEED_URL, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            with OFFER_FEED_PATH.open("wb") as fh:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        fh.write(chunk)
        size_bytes = 0
        try:
            size_bytes = OFFER_FEED_PATH.stat().st_size
        except Exception:
            size_bytes = 0
        size_mb = size_bytes / (1024 * 1024) if size_bytes else 0
        log(f"✔ Offer feed downloaded: {OFFER_FEED_PATH} ({size_mb:.2f} MB)")
        return True
    except Exception as exc:
        log(f"⚠️ Offer feed download failed: {exc}")
        return False


def wc_site_and_auth() -> Tuple[Optional[str], Optional[Tuple[str, str]]]:
    try:
        site = os.getenv("WP_BASE_URL") or os.getenv("WC_SITE_URL")
        ck = os.getenv("WC_CONSUMER_KEY")
        cs = os.getenv("WC_CONSUMER_SECRET")
        if ck and cs:
            return site, (ck, cs)
        u = os.getenv("WP_USERNAME")
        p = os.getenv("WP_APP_PASSWORD")
        if u and p:
            return site, (u, p)
        return site, None
    except Exception:
        return None, None


def iter_woo_products() -> Iterable[Dict[str, Any]]:
    site, auth = wc_site_and_auth()
    if not site or not auth:
        log("⚠️ WooCommerce URL või auth puudub (.env)")
        return []
    url = f"{site.rstrip('/')}/wp-json/wc/v3/products"
    page = 1
    consecutive_rate_limits = 0
    while True:
        params = {
            "per_page": 100,
            "page": page,
            "_fields": "id,sku,name,meta_data,stock_quantity,stock_status,regular_price,sale_price,price",
        }
        try:
            resp = requests.get(url, auth=auth, params=params, timeout=30)
        except Exception as exc:
            log(f"⚠️ Woo toodete päringu viga (page {page}): {exc}")
            return
        if resp.status_code == 429:
            wait_s = min(30, 5 * (consecutive_rate_limits + 1))
            consecutive_rate_limits += 1
            log(f"⚠️ WooCommerce toodete päringut piiratakse (429). Ootan {wait_s}s ja proovin uuesti (page {page}).")
            try:
                time.sleep(wait_s)
            except Exception:
                pass
            continue
        consecutive_rate_limits = 0
        if resp.status_code != 200:
            log(f"⚠️ Woo toodete päring vastas koodiga {resp.status_code} (page {page}).")
            return
        try:
            data = resp.json()
        except Exception as exc:
            log(f"⚠️ Woo toodete JSON parse viga (page {page}): {exc}")
            return
        if not isinstance(data, list) or not data:
            break
        for item in data:
            if isinstance(item, dict):
                yield item
        if len(data) < 100:
            break
        page += 1


def has_dropxl_supplier(meta_data: Any) -> bool:
    try:
        for m in meta_data or []:
            if not isinstance(m, dict):
                continue
            key = str(m.get("key") or "").strip()
            if key != "_bp_supplier":
                continue
            val = str(m.get("value") or "").strip()
            if val.lower() == "dropxl":
                return True
    except Exception:
        return False
    return False


def _as_price_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        s = str(v).strip()
        s = s.replace("€", "").replace(" ", "")
        s = s.replace(",", ".")
        val = float(s)
        if val < 0:
            return ""
        return f"{val:.2f}"
    except Exception:
        return ""


def _round_up_to_99(v: Any) -> Optional[float]:
    try:
        val = float(str(v).strip())
    except Exception:
        return None
    if val <= 0:
        return val
    base = math.floor(val) + 0.99
    if val - base > 1e-9:
        base = math.ceil(val) + 0.99
    return base


def _effective_rrp_price(rrp_price: Optional[float], purchase_price: Optional[float]) -> Optional[float]:
    try:
        rrp = float(rrp_price) if rrp_price is not None else None
    except Exception:
        rrp = None
    try:
        pp = float(purchase_price) if purchase_price is not None else None
    except Exception:
        pp = None
    calc_price = None
    if pp is not None:
        try:
            calc_price = pp * (1.0 + PRICE_MARKUP_RATE) * (1.0 + PRICE_VAT_RATE)
        except Exception:
            calc_price = None
    if calc_price is not None and rrp is not None:
        effective = max(calc_price, rrp)
    elif calc_price is not None:
        effective = calc_price
    elif rrp is not None:
        effective = rrp
    else:
        effective = None
    if effective is not None:
        rounded = _round_up_to_99(effective)
        if rounded is not None:
            effective = rounded
    return effective


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


def extract_ean(meta_data: Any) -> str:
    try:
        for entry in meta_data or []:
            if not isinstance(entry, dict):
                continue
            key = str(entry.get("key") or "").strip()
            if key != "_bp_gtin13":
                continue
            value = str(entry.get("value") or "").strip()
            if value:
                return value
    except Exception:
        return ""
    return ""


def update_woo_product_fields(woo_product_id: int, payload: Dict[str, Any], dry_run: bool = False) -> bool:
    site, auth = wc_site_and_auth()
    if not site or not auth:
        log("⚠️ WooCommerce URL või auth puudub (.env)")
        return False
    url = f"{site.rstrip('/')}/wp-json/wc/v3/products/{woo_product_id}"
    if dry_run:
        log(f"[DRY-RUN] Uuendaks Woo product {woo_product_id} payloadiga: {payload}")
        return True
    try:
        resp = requests.put(url, auth=auth, json=payload, timeout=60)
    except Exception as exc:
        log(f"❌ Woo uuenduse viga (id={woo_product_id}): {exc}")
        return False
    if resp.status_code != 200:
        txt = resp.text[:200] if resp.text else ""
        log(f"❌ Woo uuendus ebaõnnestus (id={woo_product_id}): HTTP {resp.status_code} {txt}")
        return False
    return True


def load_feed_index() -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    if not OFFER_FEED_PATH.exists():
        log(f"⚠️ Offer feed not found: {OFFER_FEED_PATH}")
        return index
    try:
        with OFFER_FEED_PATH.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                sku = str((row.get("SKU") or "")).strip()
                if not sku:
                    continue
                stock = _to_int(row.get("Stock"))
                rrp_price = _to_float(row.get("Webshop price"))
                purchase_price = _to_float(row.get("B2B price"))
                ean = str((row.get("EAN") or "")).strip()
                index[sku] = {
                    "stock": stock,
                    "rrp_price": rrp_price,
                    "purchase_price": purchase_price,
                    "ean": ean,
                }
    except Exception as exc:
        log(f"⚠️ Feed lugemise viga: {exc}")
    log(f"Offer feed index valmis: {len(index)} SKU-d")
    return index


def run_once(
    only_skus: Optional[set[str]] = None,
    limit: int = 0,
    dry_run: bool = False,
    update_prices: bool = True,
) -> None:
    load_dotenv(find_dotenv(), override=False)
    download_offer_feed()
    feed_index = load_feed_index()

    updated = 0
    skipped_not_dropxl = 0
    skipped_no_sku = 0
    errors = 0
    processed = 0
    woo_total = 0
    feed_total = len(feed_index)
    feed_in_stock = 0
    feed_out_of_stock = 0
    missing_in_feed = 0
    ean_mismatch = 0

    for entry in feed_index.values():
        try:
            qty = int(entry.get("stock") or 0)
        except Exception:
            qty = 0
        if qty > 0:
            feed_in_stock += 1
        else:
            feed_out_of_stock += 1

    log("Alustan DropXL stock/hindade sync'i Woo toodete põhjal …")

    for woo_prod in iter_woo_products():
        woo_total += 1
        sku = str((woo_prod.get("sku") or "")).strip()
        if not sku:
            skipped_no_sku += 1
            continue
        if only_skus and sku not in only_skus:
            continue

        meta_data = woo_prod.get("meta_data") or []
        if not has_dropxl_supplier(meta_data):
            skipped_not_dropxl += 1
            continue

        processed += 1
        if limit and limit > 0 and processed > limit:
            break

        woo_id = woo_prod.get("id")
        old_qty = woo_prod.get("stock_quantity")
        old_stock_status = str(woo_prod.get("stock_status") or "")
        woo_name = str(woo_prod.get("name") or "").strip()
        if len(woo_name) > 90:
            woo_name = woo_name[:87].rstrip() + "..."

        header = (
            f"SKU={sku} | {woo_name} | Woo ID={woo_id}"
            if woo_name
            else f"SKU={sku} | Woo ID={woo_id}"
        )
        log("-" * len(header))
        log(header)

        feed_row = feed_index.get(sku)
        if feed_row is None:
            missing_in_feed += 1
            new_qty = 0
            new_stock_status = "outofstock"
            log(" Stock : (feed missing) -> 0 (outofstock)")
            payload: Dict[str, Any] = {
                "manage_stock": True,
                "stock_quantity": 0,
                "stock_status": "outofstock",
                "backorders": "no",
            }
            ok = update_woo_product_fields(int(woo_id), payload, dry_run=dry_run)
            if ok:
                updated += 1
                log(" Result: OK (stock updated: qty=0, outofstock)")
            else:
                errors += 1
                log(" Result: FAIL (stock update)")
            continue

        feed_ean = str(feed_row.get("ean") or "").strip()
        woo_ean = extract_ean(meta_data)
        if feed_ean and woo_ean and feed_ean != woo_ean:
            ean_mismatch += 1
            log(f" EAN mismatch: feed={feed_ean} woo={woo_ean} -> set status=draft")
            payload = {"status": "draft"}
            ok = update_woo_product_fields(int(woo_id), payload, dry_run=dry_run)
            if ok:
                updated += 1
                log(" Result: OK (set to draft due to EAN mismatch)")
            else:
                errors += 1
                log(" Result: FAIL (draft update)")
            continue

        new_qty = int(feed_row.get("stock") or 0)
        new_stock_status = "instock" if new_qty > 0 else "outofstock"
        log(f" Stock : {old_qty} ({old_stock_status}) -> {new_qty} ({new_stock_status})")

        payload = {
            "manage_stock": True,
            "stock_quantity": max(0, int(new_qty)),
            "stock_status": new_stock_status,
            "backorders": "no",
        }

        stock_changed = False
        try:
            stock_changed = int(old_qty or 0) != int(new_qty) or old_stock_status != new_stock_status
        except Exception:
            stock_changed = True

        price_changed = False
        if update_prices:
            rrp_price = feed_row.get("rrp_price")
            purchase_price = feed_row.get("purchase_price")
            effective_rrp = _effective_rrp_price(rrp_price, purchase_price)
            rp = _as_price_str(effective_rrp)

            old_reg = _as_price_str(woo_prod.get("regular_price") or woo_prod.get("price"))
            log(
                f" Price : regular {old_reg} -> {rp} | rrp={_as_price_str(rrp_price)} | purchase={_as_price_str(purchase_price)}"
            )

            if rp:
                payload["regular_price"] = rp

            try:
                price_changed = (old_reg != rp) if rp else False
            except Exception:
                price_changed = True

        if not stock_changed and not price_changed:
            log(" Result: SKIP (no changes)")
            continue

        ok = update_woo_product_fields(int(woo_id), payload, dry_run=dry_run)
        if ok:
            updated += 1
            if update_prices and "regular_price" in payload:
                log(f" Result: OK (stock+price updated: qty={new_qty}, regular={payload.get('regular_price')})")
            else:
                log(f" Result: OK (stock updated: qty={new_qty})")
        else:
            errors += 1
            log(" Result: FAIL (Woo update)")

    all_woo_in_feed = (missing_in_feed == 0)
    log("=" * 72)
    log("SUMMARY")
    log("=" * 72)
    log(f"Feed: total_skus={feed_total}, in_stock={feed_in_stock}, out_of_stock={feed_out_of_stock}")
    log(f"Woo: total_fetched={woo_total}, dropxl_processed={processed}")
    log(f"Missing in feed (Woo DropXL not in feed): {missing_in_feed}")
    log(f"EAN mismatches (set to draft): {ean_mismatch}")
    log(f"Updated={updated}, Skipped_not_dropxl={skipped_not_dropxl}, Skipped_no_sku={skipped_no_sku}, Errors={errors}")
    log(f"All Woo DropXL SKUs present in feed: {'YES' if all_woo_in_feed else 'NO'}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DropXL stock/price sync WooCommerce'i jaoks")
    parser.add_argument(
        "--only-sku",
        action="append",
        default=[],
        help="Töötle ainult neid SKUsid (võib korrata või anda komadega)",
    )
    parser.add_argument("--limit", type=int, default=0, help="Piira töödeldud toodete arvu")
    parser.add_argument("--dry-run", action="store_true", help="Ära uuenda, ainult logi")
    parser.add_argument(
        "--no-update-prices",
        dest="update_prices",
        action="store_false",
        help="Ära uuenda hindasid",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=1800,
        help="Kui >0, jookseb loop iga N sekundi järel (default: 1800)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    only_skus: Optional[set[str]] = None
    if args.only_sku:
        only_skus = set()
        for entry in args.only_sku:
            for part in str(entry).split(","):
                val = part.strip()
                if val:
                    only_skus.add(val)

    interval = int(args.interval or 0)
    if interval > 0:
        while True:
            run_once(
                only_skus=only_skus,
                limit=int(args.limit or 0),
                dry_run=bool(args.dry_run),
                update_prices=bool(args.update_prices),
            )
            log(f"⏳ Waiting {interval} seconds...")
            try:
                time.sleep(interval)
            except KeyboardInterrupt:
                log("Stopped by user.")
                break
    else:
        run_once(
            only_skus=only_skus,
            limit=int(args.limit or 0),
            dry_run=bool(args.dry_run),
            update_prices=bool(args.update_prices),
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
