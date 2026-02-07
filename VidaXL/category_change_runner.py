#!/usr/bin/env python3
"""Category change runner for VidaXL.

Kasutus:
  python category_change_runner.py  # kasutab DEFAULT_MAPS
  python category_change_runner.py --map "Sporditarbed=>Sport ja vaba aeg"
  python category_change_runner.py --map "A > B=>A > C" --skip-woo
  python category_change_runner.py --map "X=>Y" --dry-run

Mida teeb:
- Uuendab category_translation.json (võtmed + väärtused).
- Uuendab data/category_catalog.json (path + name + level).
- Uuendab data/tõlgitud/products_translated_grouped.json (group key + category path fields).
- Uuendab 2_samm_tooteinfo.json (category path fields), kui fail olemas.
- Valikuliselt uuendab WooCommerce'i kategooriaid (--update-woo).
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import find_dotenv, load_dotenv

ROOT = Path(__file__).resolve().parent
CATEGORY_TRANSLATION_PATH = ROOT / "category_translation.json"
CATEGORY_CATALOG_PATH = ROOT / "data" / "category_catalog.json"
PRODUCTS_TRANSLATED_GROUPED_PATH = ROOT / "data" / "tõlgitud" / "products_translated_grouped.json"
STEP2_OUTPUT_PATH = ROOT / "2_samm_tooteinfo.json"

DEFAULT_MAPS: List[Tuple[str, str]] = [
    ("Sporditarbed", "Sport ja vaba aeg"),
    ("Mängud ja mänguasjad", "Sport ja vaba aeg > Mängud ja mänguasjad"),
    ("Teised > Kunst ja meelelahutus", "Sport ja vaba aeg > Kunst ja meelelahutus"),
    ("Kodu ja aed", "Kodukaubad"),
    ("Rõivad ja aksessuaarid", "Rõivad"),
    ("Kodu ja aed > Aed ja muru", "Aed > Aed ja muru"),
    ("Kodu ja aed > Basseinid ja spaad", "Aed > Basseinid ja spaad"),
    ("Kodu ja aed > Kamina- ja ahjutarvikud", "Aed > Kamina- ja ahjutarvikud"),
    ("Kodu ja aed > Kaminad", "Aed > Kaminad"),
    ("Kodu ja aed > Õuevalgustus", "Aed > Õuevalgustus"),
    ("Mööbel > Õuemööbel", "Aed > Õuemööbel"),
    ("Mööbel > Õuemööbli tarvikud", "Aed > Õuemööbli tarvikud"),
    ("Aed > Õuemööbel", "Aed > Aiamööbel"),
    ("Aed > Õuevalgustus", "Aed > Aiavalgustus"),
    ("Koduloomade tarbed > Lemmikloomatarbed", "Kodukaubad > Lemmikloomatarbed"),
    ("Ehitustarbed > Aiad ja barjäärid", "Aed > Aiad ja barjäärid"),
    ("Tervis ja ilu", "Sport ja vaba aeg > Tervis ja ilu"),
    (
        "Elektroonika > Printimine, kopeerimine, skaneerimine ja faks",
        "Mööbel > Kontorimööbel > Printimine, kopeerimine, skaneerimine ja faks",
    ),
    (
        "Teised > Elektroonika > Printimine, kopeerimine, skaneerimine ja faks",
        "Mööbel > Kontorimööbel > Printimine, kopeerimine, skaneerimine ja faks",
    ),
    ("Kaamerad ja optika", "Sport ja vaba aeg > Kaamerad ja optika"),
    ("Teised > Kaamerad ja optika", "Sport ja vaba aeg > Kaamerad ja optika"),
    ("Sport ja vaba aeg > Stuudio valgustid", "Sport ja vaba aeg > Kaamerad ja optika > Stuudio valgustid"),
    ("Kontoritarbed > Ettekande tarvikud", "Mööbel > Kontorimööbel > Ettekande tarvikud"),
]


def log(msg: str) -> None:
    print(msg)


def parse_maps(values: List[str]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for raw in values:
        if "=>" not in raw:
            raise ValueError(f"Invalid map '{raw}', expected format 'OLD=>NEW'")
        left, right = raw.split("=>", 1)
        old = left.strip()
        new = right.strip()
        if not old or not new:
            raise ValueError(f"Invalid map '{raw}', empty side")
        if old == new:
            continue
        out.append((old, new))
    if not out:
        raise ValueError("No valid --map entries provided")
    return out


def _split_path(path: str) -> List[str]:
    return [p.strip() for p in str(path or "").split(">") if p.strip()]


def replace_prefix(path: str, old: str, new: str) -> str:
    if not path:
        return path
    if path == old:
        return new
    prefix = old + " > "
    if path.startswith(prefix):
        return new + path[len(old):]
    return path


def apply_maps_to_path(path: str, maps: List[Tuple[str, str]]) -> str:
    updated = path
    for old, new in maps:
        updated = replace_prefix(updated, old, new)
    return updated


def update_category_translation(maps: List[Tuple[str, str]], dry_run: bool) -> int:
    if not CATEGORY_TRANSLATION_PATH.exists():
        log(f"⚠️ {CATEGORY_TRANSLATION_PATH} not found")
        return 0
    with CATEGORY_TRANSLATION_PATH.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        log("⚠️ category_translation.json is not a dict")
        return 0
    changed = 0
    new_data: Dict[str, str] = {}
    for k, v in data.items():
        key = str(k)
        val = "" if v is None else str(v)
        new_key = apply_maps_to_path(key, maps)
        new_val = apply_maps_to_path(val, maps) if val else val
        if new_key != key or new_val != val:
            changed += 1
        new_data[new_key] = new_val
        if new_key != key:
            existing_old = new_data.get(key, "")
            if not existing_old:
                new_data[key] = new_key
                changed += 1
    for old, new in maps:
        if old == new:
            continue
        existing = new_data.get(old, "")
        if existing:
            continue
        new_data[old] = new
        changed += 1
    for old, new in maps:
        if old == new:
            continue
        new_prefix = f"{new} > "
        for key, val in list(new_data.items()):
            if key == new or key.startswith(new_prefix):
                suffix = key[len(new):]
                old_key = f"{old}{suffix}"
                if new_data.get(old_key):
                    continue
                new_data[old_key] = key
                changed += 1
    for key, val in list(new_data.items()):
        if val == key:
            new_data[key] = ""
            changed += 1
    if not dry_run and changed:
        ordered = dict(sorted(new_data.items(), key=lambda kv: kv[0].lower()))
        with CATEGORY_TRANSLATION_PATH.open("w", encoding="utf-8") as fh:
            json.dump(ordered, fh, ensure_ascii=False, indent=2)
    log(f"category_translation.json: changed={changed}")
    return changed


def update_category_catalog(maps: List[Tuple[str, str]], dry_run: bool) -> int:
    if not CATEGORY_CATALOG_PATH.exists():
        log(f"⚠️ {CATEGORY_CATALOG_PATH} not found")
        return 0
    with CATEGORY_CATALOG_PATH.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        log("⚠️ category_catalog.json is not a list")
        return 0
    changed = 0
    for item in data:
        if not isinstance(item, dict):
            continue
        path = str(item.get("path") or "")
        new_path = apply_maps_to_path(path, maps)
        if new_path != path:
            changed += 1
            item["path"] = new_path
            parts = _split_path(new_path)
            if parts:
                item["name"] = parts[-1]
                item["level"] = len(parts)
    if not dry_run and changed:
        with CATEGORY_CATALOG_PATH.open("w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
    log(f"category_catalog.json: changed={changed}")
    return changed


def _update_product_category_fields(cat_obj: Dict[str, Any], maps: List[Tuple[str, str]]) -> bool:
    changed = False
    path = str(cat_obj.get("path") or "")
    translated = str(cat_obj.get("translated_path") or "")
    new_path = apply_maps_to_path(path, maps)
    new_translated = apply_maps_to_path(translated, maps) if translated else translated
    if new_path != path:
        cat_obj["path"] = new_path
        changed = True
    if new_translated != translated:
        cat_obj["translated_path"] = new_translated
        changed = True
    target_path = new_translated or new_path
    parts = _split_path(target_path)
    if parts:
        leaf = parts[-1]
        if cat_obj.get("leaf_name") != leaf:
            cat_obj["leaf_name"] = leaf
            changed = True
    return changed


def update_products_grouped(maps: List[Tuple[str, str]], dry_run: bool) -> int:
    if not PRODUCTS_TRANSLATED_GROUPED_PATH.exists():
        log(f"⚠️ {PRODUCTS_TRANSLATED_GROUPED_PATH} not found")
        return 0
    with PRODUCTS_TRANSLATED_GROUPED_PATH.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        log("⚠️ products_translated_grouped.json is not a dict")
        return 0
    changed = 0
    new_groups: Dict[str, List[Dict[str, Any]]] = {}
    for group_key, items in data.items():
        new_group_key = apply_maps_to_path(str(group_key), maps)
        group_list = new_groups.setdefault(new_group_key, [])
        if new_group_key != group_key:
            changed += 1
        for item in items or []:
            if not isinstance(item, dict):
                continue
            cat_obj = item.get("category") or {}
            if isinstance(cat_obj, dict):
                if _update_product_category_fields(cat_obj, maps):
                    changed += 1
                item["category"] = cat_obj
            categories = item.get("categories")
            if isinstance(categories, list) and categories:
                leaf = (cat_obj.get("leaf_name") or "") if isinstance(cat_obj, dict) else ""
                if leaf:
                    try:
                        if isinstance(categories[0], dict) and categories[0].get("name") != leaf:
                            categories[0]["name"] = leaf
                            changed += 1
                    except Exception:
                        pass
            group_list.append(item)
    if not dry_run and changed:
        with PRODUCTS_TRANSLATED_GROUPED_PATH.open("w", encoding="utf-8") as fh:
            json.dump(new_groups, fh, ensure_ascii=False, indent=2)
    log(f"products_translated_grouped.json: changed={changed}")
    return changed


def update_step2_output(maps: List[Tuple[str, str]], dry_run: bool) -> int:
    if not STEP2_OUTPUT_PATH.exists():
        log(f"⚠️ {STEP2_OUTPUT_PATH} not found")
        return 0
    with STEP2_OUTPUT_PATH.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        log("⚠️ 2_samm_tooteinfo.json is not a list")
        return 0
    changed = 0
    for item in data:
        if not isinstance(item, dict):
            continue
        cat_obj = item.get("category") or {}
        if isinstance(cat_obj, dict):
            if _update_product_category_fields(cat_obj, maps):
                changed += 1
            item["category"] = cat_obj
        categories = item.get("categories")
        if isinstance(categories, list) and categories:
            leaf = (cat_obj.get("leaf_name") or "") if isinstance(cat_obj, dict) else ""
            if leaf:
                try:
                    if isinstance(categories[0], dict) and categories[0].get("name") != leaf:
                        categories[0]["name"] = leaf
                        changed += 1
                except Exception:
                    pass
    if not dry_run and changed:
        with STEP2_OUTPUT_PATH.open("w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
    log(f"2_samm_tooteinfo.json: changed={changed}")
    return changed


# WooCommerce helpers

def wc_site_and_auth() -> Tuple[Optional[str], Optional[Tuple[str, str]]]:
    site = os.getenv("WP_BASE_URL") or os.getenv("WC_SITE_URL")
    ck = os.getenv("WC_CONSUMER_KEY")
    cs = os.getenv("WC_CONSUMER_SECRET")
    if site and ck and cs:
        return site.rstrip("/"), (ck, cs)
    u = os.getenv("WP_USERNAME")
    p = os.getenv("WP_APP_PASSWORD")
    if site and u and p:
        return site.rstrip("/"), (u, p)
    return None, None


def fetch_all_categories() -> Dict[int, Dict[str, Any]]:
    site, auth = wc_site_and_auth()
    if not site or not auth:
        raise RuntimeError("WooCommerce auth missing")
    out: Dict[int, Dict[str, Any]] = {}
    page = 1
    while True:
        resp = requests.get(
            f"{site}/wp-json/wc/v3/products/categories",
            auth=auth,
            params={"per_page": 100, "page": page},
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list) or not data:
            break
        for item in data:
            if isinstance(item, dict) and item.get("id"):
                out[int(item["id"])] = item
        if len(data) < 100:
            break
        page += 1
    return out


def build_category_paths(categories: Dict[int, Dict[str, Any]]) -> Dict[int, str]:
    cache: Dict[int, str] = {}

    def path_for(cid: int) -> str:
        if cid in cache:
            return cache[cid]
        node = categories.get(cid) or {}
        name = str(node.get("name") or "").strip()
        parent = int(node.get("parent") or 0)
        if not parent:
            cache[cid] = name
            return name
        parent_path = path_for(parent)
        cache[cid] = f"{parent_path} > {name}" if parent_path else name
        return cache[cid]

    for cid in categories:
        path_for(cid)
    return cache


def fetch_products_by_category(site: str, auth: Tuple[str, str], category_id: int) -> List[Dict[str, Any]]:
    products: List[Dict[str, Any]] = []
    page = 1
    while True:
        resp = requests.get(
            f"{site}/wp-json/wc/v3/products",
            auth=auth,
            params={"per_page": 100, "page": page, "category": category_id},
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list) or not data:
            break
        products.extend([p for p in data if isinstance(p, dict)])
        if len(data) < 100:
            break
        page += 1
    return products


def update_product_categories(
    site: str,
    auth: Tuple[str, str],
    product_id: int,
    categories: List[Dict[str, Any]],
    dry_run: bool,
) -> None:
    if dry_run:
        log(f"[DRY-RUN] Update product id={product_id} categories={categories}")
        return
    resp = requests.put(
        f"{site}/wp-json/wc/v3/products/{product_id}",
        auth=auth,
        json={"categories": categories},
        timeout=60,
    )
    if resp.status_code != 200:
        log(f"❌ Update product categories failed: {resp.status_code} {resp.text[:200]}")


def update_category_parent(site: str, auth: Tuple[str, str], category_id: int, parent_id: int, dry_run: bool) -> None:
    if dry_run:
        log(f"[DRY-RUN] Update category id={category_id} parent={parent_id}")
        return
    resp = requests.put(
        f"{site}/wp-json/wc/v3/products/categories/{category_id}",
        auth=auth,
        json={"parent": parent_id},
        timeout=60,
    )
    if resp.status_code != 200:
        log(f"❌ Update category parent failed: {resp.status_code} {resp.text[:200]}")


def merge_woo_categories(
    site: str,
    auth: Tuple[str, str],
    old_id: int,
    new_id: int,
    dry_run: bool,
) -> None:
    categories = fetch_all_categories()
    child_index: Dict[Tuple[int, str], int] = {}
    parent_by_id: Dict[int, int] = {}
    for cat_id, cat in categories.items():
        if not isinstance(cat, dict):
            continue
        name = str(cat.get("name") or "").strip()
        parent_id = int(cat.get("parent") or 0)
        if name:
            child_index[(parent_id, name)] = int(cat_id)
        parent_by_id[int(cat_id)] = parent_id
    ancestor_ids: set[int] = set()
    cursor = parent_by_id.get(new_id, 0)
    while cursor:
        ancestor_ids.add(cursor)
        cursor = parent_by_id.get(cursor, 0)
    children = [c for c in categories.values() if int(c.get("parent") or 0) == old_id]
    for child in children:
        child_id = int(child.get("id") or 0)
        child_name = str(child.get("name") or "").strip()
        if not child_id or not child_name:
            continue
        existing_child = child_index.get((new_id, child_name))
        if existing_child and existing_child != child_id:
            merge_woo_categories(site, auth, child_id, existing_child, dry_run)
        else:
            update_category_parent(site, auth, child_id, new_id, dry_run)

    products = fetch_products_by_category(site, auth, old_id)
    for product in products:
        pid = int(product.get("id") or 0)
        if not pid:
            continue
        current = product.get("categories") or []
        if not isinstance(current, list):
            continue
        updated: List[Dict[str, Any]] = []
        seen_new = False
        for cat in current:
            if not isinstance(cat, dict):
                continue
            cid = int(cat.get("id") or 0)
            if cid in ancestor_ids:
                continue
            if cid == old_id:
                continue
            if cid == new_id:
                seen_new = True
            updated.append({"id": cid})
        if not seen_new:
            updated.append({"id": new_id})
        update_product_categories(site, auth, pid, updated, dry_run)
    if dry_run:
        log(f"[DRY-RUN] Delete Woo category id={old_id}")
        return
    resp = requests.delete(
        f"{site}/wp-json/wc/v3/products/categories/{old_id}",
        auth=auth,
        params={"force": True},
        timeout=60,
    )
    if resp.status_code != 200:
        log(f"❌ Delete Woo category failed: {resp.status_code} {resp.text[:200]}")
    else:
        log(f"✅ Merged Woo category id={old_id} into id={new_id}")


def create_category(site: str, auth: Tuple[str, str], name: str, parent_id: int, dry_run: bool) -> Optional[int]:
    if dry_run:
        log(f"[DRY-RUN] Create category: name={name}, parent={parent_id}")
        return None
    payload: Dict[str, Any] = {"name": name}
    if parent_id:
        payload["parent"] = parent_id
    resp = requests.post(f"{site}/wp-json/wc/v3/products/categories", auth=auth, json=payload, timeout=60)
    if resp.status_code not in (200, 201):
        log(f"❌ Create category failed: {resp.status_code} {resp.text[:200]}")
        return None
    data = resp.json()
    return int(data.get("id") or 0) or None


def ensure_category_path(site: str, auth: Tuple[str, str], path: str, cat_by_path: Dict[str, int], dry_run: bool) -> Optional[int]:
    parts = _split_path(path)
    if not parts:
        return None
    current_parent = 0
    current_path = ""
    for part in parts:
        current_path = part if not current_path else f"{current_path} > {part}"
        existing = cat_by_path.get(current_path)
        if existing:
            current_parent = existing
            continue
        new_id = create_category(site, auth, part, current_parent, dry_run)
        if not new_id:
            return None
        cat_by_path[current_path] = new_id
        current_parent = new_id
    return current_parent


def update_woo_categories(maps: List[Tuple[str, str]], dry_run: bool) -> None:
    site, auth = wc_site_and_auth()
    if not site or not auth:
        log("⚠️ Woo auth missing, skipping Woo updates")
        return
    categories = fetch_all_categories()
    paths = build_category_paths(categories)
    path_to_ids: Dict[str, List[int]] = {}
    for cid, path in paths.items():
        if not path:
            continue
        path_to_ids.setdefault(path, []).append(cid)

    merged_any = False
    for path, ids in path_to_ids.items():
        if len(ids) < 2:
            continue
        ids_sorted = sorted(ids)
        canonical = ids_sorted[0]
        for dup_id in ids_sorted[1:]:
            log(f"🔁 Merging duplicate Woo path: {path} (id={dup_id} -> id={canonical})")
            merge_woo_categories(site, auth, dup_id, canonical, dry_run)
            merged_any = True

    if merged_any:
        categories = fetch_all_categories()
        paths = build_category_paths(categories)
        path_to_ids = {}
        for cid, path in paths.items():
            if not path:
                continue
            path_to_ids.setdefault(path, []).append(cid)

    cat_by_path: Dict[str, int] = {p: min(ids) for p, ids in path_to_ids.items() if p}

    for old, new in maps:
        cid = cat_by_path.get(old)
        existing_new = cat_by_path.get(new)
        if cid and existing_new and cid != existing_new:
            log(f"🔁 Merging Woo categories: {old} -> {new}")
            merge_woo_categories(site, auth, cid, existing_new, dry_run)
            continue
        if not cid:
            if existing_new:
                log(f"✅ Woo category already updated: {old} -> {new}")
            else:
                log(f"⚠️ Woo category not found for path: {old}")
            continue
        new_parts = _split_path(new)
        if not new_parts:
            continue
        new_name = new_parts[-1]
        new_parent_path = " > ".join(new_parts[:-1])
        parent_id = 0
        if new_parent_path:
            parent_id = ensure_category_path(site, auth, new_parent_path, cat_by_path, dry_run) or 0

        payload: Dict[str, Any] = {"name": new_name}
        if parent_id:
            payload["parent"] = parent_id
        if dry_run:
            log(f"[DRY-RUN] Update Woo category id={cid} payload={payload}")
            continue
        resp = requests.put(
            f"{site}/wp-json/wc/v3/products/categories/{cid}",
            auth=auth,
            json=payload,
            timeout=60,
        )
        if resp.status_code != 200:
            log(f"❌ Update Woo category failed: {resp.status_code} {resp.text[:200]}")
        else:
            log(f"✅ Updated Woo category: {old} -> {new}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Category change runner")
    parser.add_argument("--map", action="append", default=[], help="Mapping OLD=>NEW (can repeat)")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes only")
    parser.add_argument("--skip-woo", action="store_true", help="Skip Woo category updates")
    parser.add_argument("--skip-translation", action="store_true", help="Skip category_translation.json")
    parser.add_argument("--skip-catalog", action="store_true", help="Skip category_catalog.json")
    parser.add_argument("--skip-products", action="store_true", help="Skip products_translated_grouped.json")
    parser.add_argument("--skip-step2", action="store_true", help="Skip 2_samm_tooteinfo.json")
    return parser.parse_args()


def main() -> int:
    load_dotenv(find_dotenv(), override=False)
    args = parse_args()
    maps = parse_maps(args.map) if args.map else DEFAULT_MAPS
    dry_run = bool(args.dry_run)

    if not args.skip_translation:
        update_category_translation(maps, dry_run)
    if not args.skip_catalog:
        update_category_catalog(maps, dry_run)
    if not args.skip_products:
        update_products_grouped(maps, dry_run)
    if not args.skip_step2:
        update_step2_output(maps, dry_run)
    if not args.skip_woo:
        update_woo_categories(maps, dry_run)

    log("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
