#!/usr/bin/env python3
"""Prenta stock update runner for WooCommerce.

Kasutus:
    python Preta_stock_loop_runner.py               # üks jooks
    python Preta_stock_loop_runner.py --interval 900  # loop iga 900s järel
    python Preta_stock_loop_runner.py --only-sku ABC123 --dry-run
    python Preta_stock_loop_runner.py --no-update-prices  # ära uuenda hindasid

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

PRICE_MARKUP_RATE = 0.10
PRICE_VAT_RATE = 0.24

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


def get_prenta_rrp_for_product_id(
    client: PrentaClient,
    product_id: Any,
    cache: Dict[Any, Any],
) -> Any:
    if product_id is None:
        return None
    if product_id in cache:
        return cache.get(product_id)
    try:
        detail = client.get_product_detail(product_id)
        if isinstance(detail, dict):
            cache[product_id] = detail.get("price_rrp")
        else:
            cache[product_id] = None
    except Exception:
        cache[product_id] = None
    return cache.get(product_id)


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


def update_woo_stock(woo_product_id: int, new_qty: int, dry_run: bool = False) -> bool:
    payload: Dict[str, Any] = {
        "manage_stock": True,
        "stock_quantity": max(0, int(new_qty)),
        "stock_status": "instock" if new_qty > 0 else "outofstock",
        "backorders": "no",
    }
    return update_woo_product_fields(woo_product_id, payload, dry_run=dry_run)


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
                index[sku] = {
                    "id": pid,
                    "price_rrp": prod.get("price_rrp"),
                }
                count += 1
    except Exception as exc:
        log(f"⚠️ Prenta toodete indeksi ehitamise viga: {exc}")
    log(f"Prenta SKU indeks valmis: {count} kirjet.")
    return index


def build_prenta_purchase_price_index(client: PrentaClient) -> Dict[Any, float]:
    """Ehita indeks product_id -> min purchase price (/prices price)."""
    out: Dict[Any, float] = {}
    log("Laen Prenta hinnad (/prices) purchase_price indeksi jaoks …")
    try:
        for rec in client.iter_prices(product_id=None):
            if not isinstance(rec, dict):
                continue
            pid = rec.get("product_id")
            v = rec.get("price")
            if pid is None or v is None:
                continue
            try:
                f = float(v)
            except Exception:
                continue
            prev = out.get(pid)
            if prev is None or f < prev:
                out[pid] = f
    except Exception as exc:
        log(f"⚠️ Prenta /prices indeksi ehitamise viga: {exc}")
    log(f"Prenta purchase_price indeks valmis: {len(out)} kirjet.")
    return out


def run_once(
    only_skus: Optional[set[str]] = None,
    limit: int = 0,
    dry_run: bool = False,
    update_prices: bool = True,
) -> None:
    load_dotenv(find_dotenv(), override=False)

    client = build_prenta_client_from_env()
    prenta_index = build_prenta_sku_index(client)
    purchase_by_pid = build_prenta_purchase_price_index(client) if update_prices else {}
    rrp_by_pid_cache: Dict[Any, Any] = {}

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
            skipped_not_prenta += 1
            continue

        processed += 1
        if limit and limit > 0 and processed > limit:
            break

        prenta_info = prenta_index.get(sku)
        prenta_pid = (prenta_info or {}).get("id") if isinstance(prenta_info, dict) else None
        woo_id = woo_prod.get("id")
        old_qty = woo_prod.get("stock_quantity")
        woo_name = str(woo_prod.get("name") or "").strip()
        if len(woo_name) > 90:
            woo_name = woo_name[:87].rstrip() + "..."
        header = (
            f"SKU={sku} | {woo_name} | Woo ID={woo_id} | Prenta product_id={prenta_pid}"
            if woo_name
            else f"SKU={sku} | Woo ID={woo_id} | Prenta product_id={prenta_pid}"
        )
        log("-" * len(header))
        log(header)

        if prenta_pid is None:
            log(" Stock : (mapping missing) -> 0 (outofstock)")
            ok = update_woo_stock(int(woo_id), 0, dry_run=dry_run)
            if ok:
                updated += 1
                log(" Result: OK (stock updated: qty=0, outofstock)")
            else:
                errors += 1
                log(" Result: FAIL (stock update)")
            skipped_no_mapping += 1
            continue

        # 1) Värske laoseis Prenta API-st
        qty = sum_prenta_stock_for_product(client, prenta_pid)
        new_stock_status = "instock" if qty > 0 else "outofstock"
        old_stock_status = str(woo_prod.get("stock_status") or "")
        log(f" Stock : {old_qty} ({old_stock_status}) -> {qty} ({new_stock_status})")

        # 2) Koosta Woo payload (stock + optional prices)
        payload: Dict[str, Any] = {
            "manage_stock": True,
            "stock_quantity": max(0, int(qty)),
            "stock_status": new_stock_status,
            "backorders": "no",
        }

        stock_changed = False
        try:
            stock_changed = int(old_qty or 0) != int(qty) or old_stock_status != new_stock_status
        except Exception:
            stock_changed = True

        if update_prices:
            meta_lookup: Dict[str, Any] = {}
            try:
                for m in meta_data or []:
                    if isinstance(m, dict) and m.get("key") is not None:
                        meta_lookup[str(m.get("key"))] = m.get("value")
            except Exception:
                meta_lookup = {}

            raw_regular_price = (
                meta_lookup.get("_bp_regular_price")
                or meta_lookup.get("_bp_price")
                or woo_prod.get("regular_price")
                or woo_prod.get("price")
            )
            raw_sale_price = (
                meta_lookup.get("_bp_sale_price")
                or woo_prod.get("sale_price")
            )

            purchase_price = purchase_by_pid.get(prenta_pid)
            rrp_price = None
            if isinstance(prenta_info, dict):
                rrp_price = prenta_info.get("price_rrp")

            # Kui /products list ei andnud price_rrp, küsi detailist (/products/{id})
            try:
                if rrp_price in (None, ""):
                    rrp_price = get_prenta_rrp_for_product_id(client, prenta_pid, rrp_by_pid_cache)
            except Exception:
                pass

            effective_regular_price: Any = rrp_price
            calc_price_debug: Any = None
            try:
                if purchase_price is not None:
                    pp = float(str(purchase_price))
                    calc_price_debug = pp * (1.0 + PRICE_MARKUP_RATE) * (1.0 + PRICE_VAT_RATE)
                    if rrp_price is not None and str(rrp_price).strip() != "":
                        rrp = float(str(rrp_price))
                        effective_regular_price = calc_price_debug if calc_price_debug > rrp else rrp
                    else:
                        # Kui RRP puudub, kasutame kalkuleeritud hinda
                        effective_regular_price = calc_price_debug
            except Exception:
                pass

            if effective_regular_price is None or str(effective_regular_price).strip() == "":
                effective_regular_price = raw_regular_price

            rp = _as_price_str(effective_regular_price)
            sp = _as_price_str(raw_sale_price)
            price_source = ""
            if rp:
                if rrp_price is not None and str(rrp_price).strip() != "":
                    price_source = "rrp_or_calc"
                elif calc_price_debug is not None:
                    price_source = "calc_only"
                else:
                    price_source = "woo_or_meta_fallback"

            old_reg = _as_price_str(woo_prod.get("regular_price") or woo_prod.get("price"))
            old_sale = _as_price_str(woo_prod.get("sale_price"))
            log(f" Price : regular {old_reg} -> {rp} | sale {old_sale} -> {sp} | src={price_source}")
            log(
                f"         inputs: purchase={_as_price_str(purchase_price) if purchase_price is not None else ''}, "
                f"rrp={_as_price_str(rrp_price) if rrp_price is not None else ''}, "
                f"calc={_as_price_str(calc_price_debug) if calc_price_debug is not None else ''}"
            )

            if rp:
                payload["regular_price"] = rp
            if sp:
                payload["sale_price"] = sp

            if not rp and not sp:
                log("         note: price skipped (no regular_price/sale_price value)")

            price_changed = False
            try:
                price_changed = (old_reg != rp) or (old_sale != sp)
            except Exception:
                price_changed = True
        else:
            price_changed = False

        if not stock_changed and not price_changed:
            log(" Result: SKIP (no changes)")
            continue

        ok = update_woo_product_fields(int(woo_id), payload, dry_run=dry_run)
        if ok:
            updated += 1
            if update_prices and ("regular_price" in payload or "sale_price" in payload):
                rp_out = payload.get("regular_price", "")
                sp_out = payload.get("sale_price", "")
                log(f" Result: OK (stock+price updated: qty={qty}, regular={rp_out}, sale={sp_out})")
            else:
                log(f" Result: OK (stock updated: qty={qty})")
        else:
            errors += 1
            log(" Result: FAIL (Woo update)")

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
        default=14400,
        help="Kui >0, käita runnerit loopis antud sekundilise intervalliga.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Ära tee Woo poolel päris uuendusi, ainult logi.",
    )
    prices_group = parser.add_mutually_exclusive_group()
    prices_group.add_argument(
        "--update-prices",
        dest="update_prices",
        action="store_true",
        help="Uuenda ka Woo hinnad (regular_price/sale_price) sama reegliga nagu 5. sammus. (default)",
    )
    prices_group.add_argument(
        "--no-update-prices",
        dest="update_prices",
        action="store_false",
        help="Ära uuenda hindasid (uuendab ainult laoseisu).",
    )
    parser.set_defaults(update_prices=True)
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
            f"(dry_run={bool(args.dry_run)}, update_prices={bool(args.update_prices)})"
        )
        while True:
            try:
                run_once(
                    only_skus=only_skus or None,
                    limit=int(args.limit or 0),
                    dry_run=bool(args.dry_run),
                    update_prices=bool(args.update_prices),
                )
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
    run_once(
        only_skus=only_skus or None,
        limit=int(args.limit or 0),
        dry_run=bool(args.dry_run),
        update_prices=bool(args.update_prices),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
