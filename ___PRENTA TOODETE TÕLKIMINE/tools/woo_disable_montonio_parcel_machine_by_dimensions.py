#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import find_dotenv, load_dotenv


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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


def request_with_retry(
    method: str,
    url: str,
    auth: Tuple[str, str],
    *,
    params: Optional[Dict[str, Any]] = None,
    json_payload: Optional[Dict[str, Any]] = None,
    timeout_s: int = 60,
    max_attempts: int = 6,
) -> requests.Response:
    consecutive_rate_limits = 0
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.request(method, url, auth=auth, params=params, json=json_payload, timeout=timeout_s)
        except Exception as exc:
            last_exc = exc
            wait_s = min(30.0, 0.6 * (2 ** (attempt - 1)))
            time.sleep(wait_s)
            continue

        if resp.status_code == 429:
            wait_s = min(30, 5 * (consecutive_rate_limits + 1))
            consecutive_rate_limits += 1
            time.sleep(wait_s)
            continue

        return resp

    raise RuntimeError(f"Request failed after {max_attempts} attempts: {method} {url} ({last_exc})")


def iter_woo_products(
    *,
    fields: str,
    per_page: int = 100,
) -> Iterable[Dict[str, Any]]:
    site, auth = wc_site_and_auth()
    if not site or not auth:
        log("⚠️ WooCommerce URL või auth puudub (.env)")
        return []

    url = f"{site.rstrip('/')}/wp-json/wc/v3/products"
    page = 1

    while True:
        params = {
            "per_page": max(1, min(int(per_page), 100)),
            "page": page,
            "_fields": fields,
            "orderby": "id",
            "order": "asc",
        }
        resp = request_with_retry("GET", url, auth, params=params, timeout_s=60)
        if resp.status_code != 200:
            txt = resp.text[:200] if resp.text else ""
            raise RuntimeError(f"Woo toodete päring ebaõnnestus: HTTP {resp.status_code} {txt}")
        data = resp.json()
        if not isinstance(data, list) or not data:
            break
        for item in data:
            if isinstance(item, dict):
                yield item
        if len(data) < params["per_page"]:
            break
        page += 1


def fetch_product_meta(product_id: int) -> Optional[Dict[str, Any]]:
    site, auth = wc_site_and_auth()
    if not site or not auth:
        return None
    url = f"{site.rstrip('/')}/wp-json/wc/v3/products/{int(product_id)}"
    params = {"_fields": "id,sku,meta_data"}
    resp = request_with_retry("GET", url, auth, params=params, timeout_s=60)
    if resp.status_code != 200:
        return None
    try:
        data = resp.json()
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def meta_to_map(meta_data: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    try:
        for m in meta_data or []:
            if not isinstance(m, dict):
                continue
            k = str(m.get("key") or "").strip()
            if not k:
                continue
            out[k] = m.get("value")
    except Exception:
        return out
    return out


def has_meta_equals(meta_data: Any, key: str, expected_value: str) -> bool:
    if not key:
        return False
    mm = meta_to_map(meta_data)
    if key not in mm:
        return False
    v = mm.get(key)
    if v is None:
        return False
    try:
        return str(v).strip().lower() == str(expected_value).strip().lower()
    except Exception:
        return False


def set_meta(meta_data: Any, key: str, value: Any) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    try:
        for m in meta_data or []:
            if isinstance(m, dict):
                items.append(dict(m))
    except Exception:
        items = []
    found = False
    for m in items:
        try:
            if str(m.get("key") or "").strip() == key:
                m["value"] = value
                found = True
                break
        except Exception:
            continue
    if not found:
        items.append({"key": key, "value": value})
    return items


def _parse_number(s: str) -> Optional[float]:
    raw = (s or "").strip().replace(",", ".")
    if not raw:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def _as_cm(val: float, unit: str) -> float:
    u = (unit or "").strip().lower()
    if u == "mm":
        return val / 10.0
    if u == "m":
        return val * 100.0
    return val


def _guess_unit(attr_name: str, attr_value: str) -> str:
    n = (attr_name or "").lower()
    v = (attr_value or "").lower()
    if "(mm" in n or " mm" in n or v.endswith("mm") or " mm" in v:
        return "mm"
    if "(m" in n or v.endswith("m"):
        return "m"
    return "cm"


def _iter_attribute_pairs(product: Dict[str, Any]) -> Iterable[Tuple[str, str]]:
    for a in product.get("attributes") or []:
        if not isinstance(a, dict):
            continue
        name = str(a.get("name") or "").strip()
        if not name:
            continue
        values: List[str] = []
        opts = a.get("options")
        if isinstance(opts, list):
            values.extend([str(x) for x in opts if str(x).strip()])
        vals = a.get("values")
        if not values and isinstance(vals, list):
            values.extend([str(x) for x in vals if str(x).strip()])
        single = a.get("value")
        if not values and single is not None:
            values.append(str(single))
        for v in values:
            vv = str(v).strip()
            if vv:
                yield name, vv


_DIM_TRIPLE_RE = re.compile(
    r"(?P<a>\d+(?:[\.,]\d+)?)\s*(?:x|×|\*)\s*(?P<b>\d+(?:[\.,]\d+)?)\s*(?:x|×|\*)\s*(?P<c>\d+(?:[\.,]\d+)?)(?:\s*(?P<unit>mm|cm|m))?",
    re.IGNORECASE,
)


def _parse_dim_triple(value: str) -> Optional[Tuple[float, float, float]]:
    m = _DIM_TRIPLE_RE.search(value or "")
    if not m:
        return None
    a = _parse_number(m.group("a") or "")
    b = _parse_number(m.group("b") or "")
    c = _parse_number(m.group("c") or "")
    if a is None or b is None or c is None:
        return None
    unit = (m.group("unit") or "cm").lower()
    return (_as_cm(a, unit), _as_cm(b, unit), _as_cm(c, unit))


def extract_packaging_dims_cm(product: Dict[str, Any]) -> Optional[Tuple[float, float, float]]:
    by_axis: Dict[str, float] = {}
    for name, value in _iter_attribute_pairs(product):
        n = name.lower()
        if "pakend" in n or "package" in n:
            triple = _parse_dim_triple(value)
            if triple:
                return triple

            unit = _guess_unit(name, value)
            num = _parse_number(re.sub(r"[^0-9\.,]", "", value))
            if num is None:
                continue
            cm = _as_cm(num, unit)
            if any(tok in n for tok in ("kõrgus", "height")):
                by_axis["height"] = cm
            elif any(tok in n for tok in ("laius", "width")):
                by_axis["width"] = cm
            elif any(tok in n for tok in ("pikkus", "length", "sügavus", "depth")):
                by_axis["length"] = cm

    if len(by_axis) == 3:
        return (by_axis["length"], by_axis["width"], by_axis["height"])
    return None


def extract_product_dims_cm(product: Dict[str, Any]) -> Optional[Tuple[float, float, float]]:
    dims = product.get("dimensions") or {}
    if not isinstance(dims, dict):
        return None
    l = _parse_number(str(dims.get("length") or ""))
    w = _parse_number(str(dims.get("width") or ""))
    h = _parse_number(str(dims.get("height") or ""))
    if l is None or w is None or h is None:
        return None
    if l <= 0 or w <= 0 or h <= 0:
        return None
    return (float(l), float(w), float(h))


def fits_parcel_machine(dims_cm: Tuple[float, float, float], max_dims_cm: Tuple[float, float, float]) -> bool:
    a = sorted([float(x) for x in dims_cm])
    b = sorted([float(x) for x in max_dims_cm])
    return a[0] <= b[0] and a[1] <= b[1] and a[2] <= b[2]


def _parse_max_dims(text: str) -> Tuple[float, float, float]:
    raw = (text or "").strip()
    parts = re.split(r"[x×,;\s]+", raw)
    nums = [p for p in (parts or []) if p]
    if len(nums) != 3:
        raise ValueError("--max-dims peab olema 3 numbrit, nt 39x38x64")
    a = _parse_number(nums[0])
    b = _parse_number(nums[1])
    c = _parse_number(nums[2])
    if a is None or b is None or c is None:
        raise ValueError("--max-dims ei suutnud numbreid parsida")
    return (float(a), float(b), float(c))


def scale_dims(dims: Tuple[float, float, float], factor: float) -> Tuple[float, float, float]:
    f = float(factor)
    return (dims[0] * f, dims[1] * f, dims[2] * f)


def chunked(items: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    n = max(1, int(size))
    for i in range(0, len(items), n):
        yield items[i : i + n]


def update_products_batch(updates: List[Dict[str, Any]], *, dry_run: bool) -> int:
    if not updates:
        return 0

    site, auth = wc_site_and_auth()
    if not site or not auth:
        raise RuntimeError("WooCommerce URL või auth puudub (.env)")

    if dry_run:
        for u in updates:
            log(f"[DRY-RUN] Uuendaks: id={u.get('id')} meta_data={u.get('meta_data')}")
        return len(updates)

    url = f"{site.rstrip('/')}/wp-json/wc/v3/products/batch"
    payload = {"update": updates}
    resp = request_with_retry("POST", url, auth, json_payload=payload, timeout_s=120)
    if resp.status_code not in {200, 201}:
        txt = resp.text[:200] if resp.text else ""
        raise RuntimeError(f"Woo batch update ebaõnnestus: HTTP {resp.status_code} {txt}")
    return len(updates)


def _iter_backup_records(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            s = (line or "").strip()
            if not s:
                continue
            try:
                rec = json.loads(s)
            except Exception:
                continue
            if isinstance(rec, dict):
                yield rec


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Keela Montonio pakiautomaadid toodetel, mis ei mahu max mõõtudesse (39x38x64 cm suvalise orientatsiooniga)."
    )
    parser.add_argument(
        "--meta-key",
        default="_montonio_no_parcel_machine",
        help="Montonio pakiautomaadi keelamise meta key (default: _montonio_no_parcel_machine)",
    )
    parser.add_argument("--meta-value", default="yes", help="Väärtus, mis tähendab 'disable parcel machine' (nt yes/1/true)")
    parser.add_argument(
        "--backup-out",
        default="",
        help="Kirjuta muudetavate toodete eelnev meta seisu backup JSONL faili (et saaks --restore-backup kasutada).",
    )
    parser.add_argument(
        "--restore-backup",
        default="",
        help="Taasta meta varasemaks backup JSONL faili alusel (ignoreerib mõõtude loogikat).",
    )
    parser.add_argument(
        "--restore-missing-value",
        default="",
        help="Kui backupis prev_exists=false, siis taastamisel pane meta väärtuseks see string (default: tühi string).",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Ära kirjuta backup faili (ohtlik).",
    )
    parser.add_argument(
        "--max-dims",
        default="39x38x64",
        help="Max mõõdud cm (nt 39x38x64). Kontroll on orientatsioonivaba (küljed sortitakse).",
    )
    parser.add_argument(
        "--fallback-multiplier",
        type=float,
        default=1.0,
        help="Kui pakendi mõõte ei leita, skaleeri Woo dimensions selle teguriga (nt 1.15 = +15%, 0.9 = -10%).",
    )
    parser.add_argument(
        "--disable-if-missing",
        action="store_true",
        help="Kui ei leita ei pakendi mõõte ega Woo dimensions, siis keela pakiautomaat.",
    )
    parser.add_argument(
        "--only-suppliers",
        action="append",
        default=["ACC,Prenta,Action"],
        help="Töötle ainult tooteid, mille meta_data `_bp_supplier` on selles loendis. Võib korrata või anda komadega. Tühjaks jätmisel ei filtreeri.",
    )
    parser.add_argument(
        "--supplier-meta-key",
        default="_bp_supplier",
        help="Meta key, mille alusel tarnija filtreerimine käib (default: _bp_supplier).",
    )
    parser.add_argument("--batch-size", type=int, default=50, help="Mitu toodet korraga Woo /products/batch update-sse")
    parser.add_argument("--only-sku", action="append", default=[], help="Töötle ainult neid SKUsid (võib korrata või anda komadega)")
    parser.add_argument("--limit", type=int, default=0, help="Maksimaalselt töödeldavate toodete arv (0=piiranguta)")
    parser.add_argument("--dry-run", action="store_true", help="Ära tee päris uuendusi, ainult logi")
    args = parser.parse_args(argv)

    load_dotenv(find_dotenv(), override=False)

    site, auth = wc_site_and_auth()
    if not site or not auth:
        log("❌ WooCommerce URL või auth puudub (.env)")
        return 1

    restore_backup_path = str(args.restore_backup or "").strip()
    if restore_backup_path:
        bp = Path(restore_backup_path)
        if not bp.exists():
            log(f"❌ Backup faili ei leitud: {bp}")
            return 1

        seen: set[int] = set()
        total = 0
        restored = 0
        missing_products = 0
        errors = 0

        log(f"RESTORE mode. Backup: {bp}")
        pending: List[Dict[str, Any]] = []
        for rec in _iter_backup_records(bp):
            pid = rec.get("id")
            try:
                pid_int = int(pid)
            except Exception:
                continue
            if pid_int in seen:
                continue
            seen.add(pid_int)

            meta_key = str(rec.get("meta_key") or str(args.meta_key) or "").strip()
            if not meta_key:
                continue

            prev_exists = bool(rec.get("prev_exists"))
            if prev_exists:
                prev_value = rec.get("prev_value")
            else:
                prev_value = str(args.restore_missing_value)

            current = fetch_product_meta(pid_int)
            if not current:
                missing_products += 1
                continue

            cur_meta = current.get("meta_data") or []
            new_meta = set_meta(cur_meta, meta_key, prev_value)
            pending.append({"id": pid_int, "meta_data": new_meta})
            total += 1

            if len(pending) >= max(1, int(args.batch_size)):
                try:
                    update_products_batch(pending, dry_run=bool(args.dry_run))
                    restored += len(pending)
                except Exception as exc:
                    errors += 1
                    log(f"❌ RESTORE batch viga: {exc}")
                pending = []

        if pending:
            try:
                update_products_batch(pending, dry_run=bool(args.dry_run))
                restored += len(pending)
            except Exception as exc:
                errors += 1
                log(f"❌ RESTORE batch viga: {exc}")

        log(
            "RESTORE kokkuvõte: "
            f"products_in_backup_unique={len(seen)}, processed={total}, restored_updates={restored}, "
            f"missing_products={missing_products}, errors={errors}, dry_run={bool(args.dry_run)}"
        )
        log("🎉 Valmis.")
        return 0

    try:
        max_dims = _parse_max_dims(str(args.max_dims))
    except Exception as exc:
        log(f"❌ Vigane --max-dims: {exc}")
        return 1

    only_skus: set[str] = set()
    for token in args.only_sku or []:
        for part in str(token).split(","):
            part = part.strip()
            if part:
                only_skus.add(part)

    supplier_allow: set[str] = set()
    for token in (args.only_suppliers or []):
        for part in str(token).split(","):
            part = part.strip()
            if part:
                supplier_allow.add(part.lower())
    supplier_meta_key = str(args.supplier_meta_key or "").strip() or "_bp_supplier"

    fields = "id,sku,meta_data,dimensions,attributes"
    processed = 0
    updates_total = 0
    disabled_new = 0
    disabled_already = 0
    kept_enabled = 0
    missing_dims = 0
    errors = 0
    pending_updates: List[Dict[str, Any]] = []

    backup_handle = None
    backup_path = None
    if not bool(args.no_backup) and (str(args.backup_out or "").strip() or not bool(args.dry_run)):
        try:
            if str(args.backup_out or "").strip():
                backup_path = Path(str(args.backup_out).strip())
            else:
                ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
                backup_path = Path.cwd() / f"montonio_parcel_machine_backup_{ts}.jsonl"
            backup_handle = backup_path.open("a", encoding="utf-8")
        except Exception as exc:
            log(f"❌ Ei suutnud avada backup faili: {exc}")
            return 1

    log(f"Woo: {site}")
    log(f"Meta key (disable parcel machine): {args.meta_key}={args.meta_value}")
    log(f"Max dims (cm): {max_dims}")
    log(f"Fallback multiplier: {float(args.fallback_multiplier)}")
    log(f"Disable if missing dims: {bool(args.disable_if_missing)}")
    if backup_path:
        log(f"Backup file: {backup_path}")
    if supplier_allow:
        log(f"Supplier filter: {supplier_meta_key} in {sorted(supplier_allow)}")
    if only_skus:
        log(f"Only SKU filter: {len(only_skus)} tk")

    def flush() -> None:
        nonlocal pending_updates, updates_total, errors
        if not pending_updates:
            return
        try:
            updates_total += update_products_batch(pending_updates, dry_run=bool(args.dry_run))
        except Exception as exc:
            errors += 1
            log(f"❌ Batch update viga: {exc}")
        pending_updates = []

    try:
        for prod in iter_woo_products(fields=fields, per_page=100):
            sku = str(prod.get("sku") or "").strip()
            if only_skus and sku not in only_skus:
                continue

            meta_data = prod.get("meta_data") or []
            if supplier_allow:
                supplier_val = meta_to_map(meta_data).get(supplier_meta_key)
                supplier_norm = str(supplier_val).strip().lower() if supplier_val is not None else ""
                if supplier_norm not in supplier_allow:
                    continue

            processed += 1
            if args.limit and processed > int(args.limit):
                break

            prod_id = prod.get("id")
            try:
                pid_int = int(prod_id)
            except Exception:
                continue

            packaging = extract_packaging_dims_cm(prod)
            dims_source = "packaging_attributes"
            effective_dims: Optional[Tuple[float, float, float]] = packaging

            if effective_dims is None:
                woo_dims = extract_product_dims_cm(prod)
                if woo_dims is not None:
                    effective_dims = scale_dims(woo_dims, float(args.fallback_multiplier))
                    dims_source = "woo_dimensions"
                else:
                    dims_source = "missing"

            should_disable = False
            if effective_dims is None:
                missing_dims += 1
                should_disable = bool(args.disable_if_missing)
            else:
                should_disable = not fits_parcel_machine(effective_dims, max_dims)

            if not should_disable:
                kept_enabled += 1
                continue

            current = meta_to_map(meta_data).get(str(args.meta_key))
            current_norm = str(current).strip().lower() if current is not None else ""
            target_norm = str(args.meta_value).strip().lower()
            if current_norm == target_norm:
                disabled_already += 1
                continue

            if backup_handle is not None:
                rec = {
                    "ts": datetime.now().isoformat(timespec="seconds"),
                    "id": pid_int,
                    "sku": sku,
                    "meta_key": str(args.meta_key),
                    "prev_exists": current is not None,
                    "prev_value": current,
                    "new_value": str(args.meta_value),
                    "dims_source": dims_source,
                    "dims_cm": effective_dims,
                }
                try:
                    backup_handle.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    backup_handle.flush()
                except Exception:
                    pass

            new_meta = set_meta(meta_data, str(args.meta_key), str(args.meta_value))
            pending_updates.append({"id": pid_int, "meta_data": new_meta})
            disabled_new += 1
            log(
                f"[DISABLE] sku={sku} id={pid_int} source={dims_source} dims_cm={effective_dims} -> set {args.meta_key}={args.meta_value}"
            )

            if len(pending_updates) >= max(1, int(args.batch_size)):
                flush()
    finally:
        try:
            if backup_handle is not None:
                backup_handle.close()
        except Exception:
            pass

    flush()

    log(
        "Kokkuvõte: "
        f"processed={processed}, disable_new={disabled_new}, already_disabled={disabled_already}, "
        f"kept_enabled={kept_enabled}, missing_dims={missing_dims}, updates_sent={updates_total}, errors={errors}."
    )
    log("🎉 Valmis.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
