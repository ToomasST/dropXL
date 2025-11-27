#!/usr/bin/env python3
"""0. samm: kategooriate inventuur Prenta API-st.

Kogub kategooriate hierarhia, salvestab tooraine `data/category_catalog.json` failina
ja uuendab `category_translation.json` (lisab puuduolevad rajad tühja väärtusega).
Loob tühi `category_runlist.json`, kui seda veel pole.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import find_dotenv, load_dotenv

from prenta_fetch import ClientConfig, PrentaClient

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data"
CATALOG_PATH = DATA_DIR / "category_catalog.json"
TRANSLATION_PATH = ROOT / "category_translation.json"
RUNLIST_PATH = ROOT / "category_runlist.json"


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def build_category_paths(categories: List[Dict[str, Any]]) -> Dict[Any, str]:
    by_id: Dict[Any, Dict[str, Any]] = {c.get("id"): c for c in categories if c.get("id") is not None}
    cache: Dict[Any, str] = {}

    def path_for(cid: Any) -> str:
        if cid in cache:
            return cache[cid]
        node = by_id.get(cid)
        if not node:
            cache[cid] = ""
            return ""
        name = str(node.get("name") or "").strip()
        parent_id = node.get("parent_id")
        if parent_id is None or parent_id == cid:
            cache[cid] = name
            return cache[cid]
        parent_path = path_for(parent_id)
        full = name if not parent_path else f"{parent_path} > {name}"
        cache[cid] = full
        return full

    for cid in by_id:
        path_for(cid)
    return cache


def load_existing_translations() -> Dict[str, str]:
    if not TRANSLATION_PATH.exists():
        return {}
    try:
        with TRANSLATION_PATH.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
            if isinstance(data, dict):
                return {str(k): str(v) for k, v in data.items()}
    except Exception as exc:
        log(f"⚠️  Ei suutnud lugeda category_translation.json: {exc}")
    return {}


def write_translations(translations: Dict[str, str]) -> None:
    ensure_parent(TRANSLATION_PATH)
    ordered = dict(sorted(translations.items(), key=lambda kv: kv[0].lower()))
    with TRANSLATION_PATH.open("w", encoding="utf-8") as fh:
        json.dump(ordered, fh, ensure_ascii=False, indent=2)


def ensure_runlist() -> None:
    if RUNLIST_PATH.exists():
        return
    ensure_parent(RUNLIST_PATH)
    RUNLIST_PATH.write_text("[]\n", encoding="utf-8")
    log(f"✔ Loodud tühi category_runlist.json: {RUNLIST_PATH}")


def collect_categories(cfg: ClientConfig) -> List[Dict[str, Any]]:
    client = PrentaClient(cfg)
    cats: List[Dict[str, Any]] = []
    log("Laen kategooriaid (/categories)…")
    try:
        for item in client.iter_categories():
            cats.append(item)
    except Exception as exc:
        log(f"❌ Kategooriate kogumise viga: {exc}")
        raise
    log(f"✔ Kategooriaid kogutud: {len(cats)}")
    return cats


def build_catalog(categories: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    paths = build_category_paths(categories)
    children: Dict[Any, List[Any]] = defaultdict(list)
    for c in categories:
        pid = c.get("parent_id")
        cid = c.get("id")
        if cid is None:
            continue
        if pid is not None and pid != cid:
            children[pid].append(cid)

    catalog: List[Dict[str, Any]] = []
    for cat in categories:
        cid = cat.get("id")
        path = paths.get(cid, "")
        level = path.count(" > ") if path else 0
        catalog.append(
            {
                "id": cid,
                "name": cat.get("name"),
                "parent_id": cat.get("parent_id"),
                "date_edited": cat.get("date_edited"),
                "path": path,
                "level": level,
                "children": children.get(cid, []),
            }
        )
    catalog.sort(key=lambda item: (item["path"] or ""))
    return catalog


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kogu Prenta kategooriad ja uuenda tõlke/runlist faile")
    parser.add_argument("--base-url", default=os.getenv("PRENTA_BASE_URL", ClientConfig.base_url), help="API baas-URL")
    parser.add_argument("--username", default=os.getenv("PRENTA_USERNAME", ""), help="Basic Auth kasutajanimi")
    parser.add_argument("--password", default=os.getenv("PRENTA_PASSWORD", ""), help="Basic Auth parool")
    parser.add_argument("--per-page", type=int, default=int(os.getenv("PRENTA_PER_PAGE", "100")), help="Kirjete arv lehe kohta (max 100)")
    parser.add_argument("--timeout", type=int, default=int(os.getenv("PRENTA_TIMEOUT", "30")), help="HTTP timeout sekundites")
    parser.add_argument("--retries", type=int, default=int(os.getenv("PRENTA_MAX_RETRIES", "5")), help="Maksimaalsed päringu kordused")
    parser.add_argument("--verify-ssl", default=os.getenv("PRENTA_VERIFY_SSL", "false"), help="Kas valideerida TLS sert (true/false)")
    return parser.parse_args()


def main() -> int:
    load_dotenv(find_dotenv(), override=False)
    args = parse_args()

    if not args.username or not args.password:
        log("❌ Vajalikud PRENTA_USERNAME ja PRENTA_PASSWORD (või CLI lipud)")
        return 1

    verify_ssl = str(args.verify_ssl).strip().lower() in {"1", "true", "yes", "y"}
    if "sandbox.prenta.lt" in str(args.base_url):
        verify_ssl = False

    cfg = ClientConfig(
        base_url=args.base_url,
        username=args.username,
        password=args.password,
        timeout=args.timeout,
        max_retries=args.retries,
        per_page=max(1, min(args.per_page, 100)),
        verify_ssl=verify_ssl,
    )

    try:
        categories = collect_categories(cfg)
    except Exception:
        return 2

    catalog = build_catalog(categories)
    ensure_parent(CATALOG_PATH)
    with CATALOG_PATH.open("w", encoding="utf-8") as fh:
        json.dump(catalog, fh, ensure_ascii=False, indent=2)
    log(f"✔ Kataloog salvestatud: {CATALOG_PATH}")

    existing = load_existing_translations()
    updated = False
    for entry in catalog:
        path = entry.get("path") or entry.get("name") or ""
        if path and path not in existing:
            existing[path] = ""
            updated = True
    if updated or not TRANSLATION_PATH.exists():
        write_translations(existing)
        log(f"✔ Uuendatud category_translation.json: {TRANSLATION_PATH}")
    else:
        log("✔ category_translation.json oli juba ajakohane")

    ensure_runlist()

    log("Valmis.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
