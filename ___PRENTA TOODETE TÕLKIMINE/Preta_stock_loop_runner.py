#!/usr/bin/env python3
"""Prenta stock update runner for WooCommerce.

Kasutus:
    python Preta_stock_loop_runner.py               # üks jooks
    python Preta_stock_loop_runner.py --interval 900  # loop iga 900s järel
    python Preta_stock_loop_runner.py --only-sku ABC123 --dry-run

Reeglid:
- Võtab aluseks WooCommerce'i toodete nimekirja.
- Uuendab ainult Woo tooteid, mille meta `_bp_supplier` == "Prenta".
- Eeldab, et Woo SKU == Prenta `internal_reference`.
- Laoseis tuleb iga SKU jaoks otse Prenta API-st (`/stock_levels` summa).
"""

from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any, Dict, Iterable, Optional, Tuple

import requests
from dotenv import find_dotenv, load_dotenv

from prenta_fetch import ClientConfig, PrentaClient

def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


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
            "_fields": "id,sku,meta_data,stock_quantity,stock_status",
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


def has_prenta_supplier(meta_data: Any) -> bool:
    try:
        for m in meta_data or []:
            if not isinstance(m, dict):
                continue
            key = str(m.get("key") or "").strip()
            if key != "_bp_supplier":
                continue
            val = str(m.get("value") or "").strip()
            if val.lower() == "prenta":
                return True
    except Exception:
        return False
    return False


def sum_prenta_stock_for_product(client: PrentaClient, source_product_id: Any) -> int:
    """Summeeri /stock_levels qty antud Prenta product_id jaoks."""
    total = 0
    try:
        for rec in client.iter_stock_levels(product_id=source_product_id):
            if not isinstance(rec, dict):
                continue
            qty = rec.get("qty")
            if qty is None:
                continue
            try:
                total += max(0, int(qty))
            except Exception:
                continue
    except Exception as exc:
        log(f"⚠️ Prenta stock_levels viga (product_id={source_product_id}): {exc}")
        return 0
    return total


def update_woo_stock(woo_product_id: int, new_qty: int, dry_run: bool = False) -> bool:
    site, auth = wc_site_and_auth()
    if not site or not auth:
        log("⚠️ WooCommerce URL või auth puudub (.env)")
        return False
    url = f"{site.rstrip('/')}/wp-json/wc/v3/products/{woo_product_id}"
    payload: Dict[str, Any] = {
        "manage_stock": True,
        "stock_quantity": max(0, int(new_qty)),
        "stock_status": "instock" if new_qty > 0 else "outofstock",
        "backorders": "no",
    }
    if dry_run:
        log(f"[DRY-RUN] Uuendaks Woo product {woo_product_id} payloadiga: {payload}")
        return True
    try:
        resp = requests.put(url, auth=auth, json=payload, timeout=60)
    except Exception as exc:
        log(f"❌ Woo stock uuenduse viga (id={woo_product_id}): {exc}")
        return False
    if resp.status_code != 200:
        txt = resp.text[:200] if resp.text else ""
        log(f"❌ Woo stock uuendus ebaõnnestus (id={woo_product_id}): HTTP {resp.status_code} {txt}")
        return False
    return True


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


def build_prenta_sku_index(client: PrentaClient) -> Dict[str, Any]:
    """Ehita indeks Prenta internal_reference (SKU) -> product_id.

    Eeldus: Woo SKU on sama, mis Prenta internal_reference.
    """
    index: Dict[str, Any] = {}
    count = 0
    log("Laen Prenta toodete nimekirja SKU indeksi jaoks …")
    try:
        for prod in client.iter_products(list_limit=None):
            if not isinstance(prod, dict):
                continue
            sku = str(prod.get("internal_reference") or "").strip()
            pid = prod.get("id")
            if not sku or pid is None:
                continue
            if sku not in index:
                index[sku] = pid
                count += 1
    except Exception as exc:
        log(f"⚠️ Prenta toodete indeksi ehitamise viga: {exc}")
    log(f"Prenta SKU indeks valmis: {count} kirjet.")
    return index


def run_once(only_skus: Optional[set[str]] = None, limit: int = 0, dry_run: bool = False) -> None:
    load_dotenv(find_dotenv(), override=False)

    client = build_prenta_client_from_env()
    prenta_index = build_prenta_sku_index(client)

    updated = 0
    skipped_not_prenta = 0
    skipped_no_mapping = 0
    errors = 0
    processed = 0

    log("Alustan Prenta stock sync'i Woo toodete põhjal …")

    for woo_prod in iter_woo_products():
        sku = str((woo_prod.get("sku") or "")).strip()
        if not sku:
            continue
        if only_skus and sku not in only_skus:
            continue

        meta_data = woo_prod.get("meta_data") or []
        if not has_prenta_supplier(meta_data):
            continue

        processed += 1
        if limit and limit > 0 and processed > limit:
            break

        prenta_pid = prenta_index.get(sku)
        woo_id = woo_prod.get("id")
        old_qty = woo_prod.get("stock_quantity")
        log(f"[SKU={sku}] Woo ID={woo_id}, Prenta product_id={prenta_pid}, vana_laoseis={old_qty}")

        if prenta_pid is None:
            log("   ℹ️  Prenta product_id ei leitud selle SKU jaoks – märgin Woo toote out-of-stock.")
            ok = update_woo_stock(int(woo_id), 0, dry_run=dry_run)
            if ok:
                updated += 1
                log("   ✔ Stock uuendatud (Prentas puudub, Woo qty=0, outofstock).")
            else:
                errors += 1
            skipped_no_mapping += 1
            continue

        # 1) Värske laoseis Prenta API-st
        qty = sum_prenta_stock_for_product(client, prenta_pid)
        log(f"   Prenta stock qty={qty} (Woo vana={old_qty} → uus={qty})")

        # 2) Uuenda Woo stock
        ok = update_woo_stock(int(woo_id), qty, dry_run=dry_run)
        if ok:
            updated += 1
            log(f"   ✔ Stock uuendatud (Woo ID={woo_id}, qty={qty}).")
        else:
            errors += 1

    log(
        f"Kokkuvõte: updated={updated}, skipped_not_prenta={skipped_not_prenta}, "
        f"skipped_no_mapping={skipped_no_mapping}, errors={errors}."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prenta stock sync WooCommerce'i jaoks")
    parser.add_argument(
        "--only-sku",
        action="append",
        default=[],
        help="Töötle ainult neid SKUsid (võib korrata või anda komadega)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maksimaalselt töödeldavate toodete arv (0 = piiranguta)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=0,
        help="Kui >0, käita runnerit loopis antud sekundilise intervalliga.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Ära tee Woo poolel päris uuendusi, ainult logi.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    only_skus: set[str] = set()
    for token in args.only_sku or []:
        for part in str(token).split(","):
            part = part.strip()
            if part:
                only_skus.add(part)

    if args.interval and args.interval > 0:
        log(
            f"Käivitan Prenta stock runneri loopis, intervall {args.interval} s. "
            f"(dry_run={bool(args.dry_run)})"
        )
        while True:
            try:
                run_once(only_skus=only_skus or None, limit=int(args.limit or 0), dry_run=bool(args.dry_run))
            except KeyboardInterrupt:
                log("Saadud KeyboardInterrupt – lõpetan loopi.")
                break
            except Exception as exc:
                log(f"❌ Viga run_once sees: {exc}")
            # Väike paus enne järgmist tsüklit
            try:
                time.sleep(max(1, int(args.interval)))
            except KeyboardInterrupt:
                log("Saadud KeyboardInterrupt une ajal – lõpetan loopi.")
                break
        return 0

    # Üksik jooks
    run_once(only_skus=only_skus or None, limit=int(args.limit or 0), dry_run=bool(args.dry_run))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
