#!/usr/bin/env python3
"""0. samm: kategooriate inventuur VidaXL feedist.

Eesmärk
-------
- Loeb täis-CSV feedi ja ehitab kategooriapuu (ID + nimi + parent + path).
- Uuendab category_translation.json ja category_runlist.json faile.
- Salvestab andmepuu faili data/category_catalog.json, et hiljem saaks
  olemasolevate toodete kategooriaid ümber map'ida (vana -> uus).

Reeglid
-------
- category_translation.json võtmed on kujul "All > ... > ...".
- Olemasolevad tõlked jäetakse puutumata; lisatakse ainult uued teed.
- category_runlist.json ei muudeta (ainult luuakse, kui puudub).
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parent
FEED_PATH = ROOT / "data" / "feeds" / "vidaXL_ee_dropshipping" / "vidaXL_ee_dropshipping.csv"
TRANSLATION_PATH = ROOT / "category_translation.json"
RUNLIST_PATH = ROOT / "category_runlist.json"
CATALOG_PATH = ROOT / "data" / "category_catalog.json"


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def build_category_paths(nodes: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    cache: Dict[str, str] = {}

    def path_for(cid: str) -> str:
        if cid in cache:
            return cache[cid]
        node = nodes.get(cid)
        if not node:
            cache[cid] = ""
            return ""
        name = str(node.get("name") or "").strip()
        parent_id = node.get("parent_id")
        if not parent_id or parent_id == cid:
            cache[cid] = name
            return cache[cid]
        parent_path = path_for(parent_id)
        full = name if not parent_path else f"{parent_path} > {name}"
        cache[cid] = full
        return full

    for cid in nodes:
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
    except Exception as exc:  # pragma: no cover - ainult logimiseks
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


def _split_path(raw: str) -> List[str]:
    parts = [p.strip() for p in raw.split(">") if p.strip()]
    return parts


def collect_categories_from_feed(feed_path: Path) -> tuple[Dict[str, Dict[str, Any]], List[str]]:
    if not feed_path.exists():
        raise FileNotFoundError(f"Feed not found: {feed_path}")

    nodes: Dict[str, Dict[str, Any]] = {}
    name_paths: List[str] = []

    with feed_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            name_path_raw = str(row.get("Category") or "").strip()
            if not name_path_raw:
                continue
            name_parts = _split_path(name_path_raw)
            if not name_parts:
                continue
            for idx in range(1, len(name_parts) + 1):
                name_path = " > ".join(name_parts[:idx])
                name_paths.append(name_path)

            id_path_raw = str(row.get("Category_id_path") or "").strip()
            id_parts = _split_path(id_path_raw) if id_path_raw else []
            if id_parts and len(id_parts) == len(name_parts):
                for idx, cid in enumerate(id_parts):
                    if not cid:
                        continue
                    parent_id = id_parts[idx - 1] if idx > 0 else ""
                    node = nodes.setdefault(cid, {"id": cid, "name": name_parts[idx], "parent_id": parent_id})
                    if not node.get("name"):
                        node["name"] = name_parts[idx]
                    if parent_id and not node.get("parent_id"):
                        node["parent_id"] = parent_id

    return nodes, sorted(set(name_paths))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ehita VidaXL feedist kategooriapuu ja uuenda tõlke/runlist faile.")
    parser.add_argument("--feed-path", default=str(FEED_PATH), help="CSV feedi failirada")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    feed_path = Path(args.feed_path).expanduser()

    try:
        nodes, name_paths = collect_categories_from_feed(feed_path)
    except Exception as exc:
        log(f"❌ Feedi lugemine ebaõnnestus: {exc}")
        return 1

    paths = build_category_paths(nodes)
    catalog: List[Dict[str, Any]] = []
    for cid, node in nodes.items():
        path = paths.get(cid, "")
        full_path = path
        level = len([p for p in path.split(" > ") if p]) if path else 0
        catalog.append(
            {
                "id": cid,
                "name": node.get("name") or "",
                "parent_id": node.get("parent_id") or "",
                "path": full_path,
                "level": level,
            }
        )

    ensure_parent(CATALOG_PATH)
    catalog_sorted = sorted(catalog, key=lambda item: item.get("path") or "")
    with CATALOG_PATH.open("w", encoding="utf-8") as fh:
        json.dump(catalog_sorted, fh, ensure_ascii=False, indent=2)
    log(f"✔ Kirjutatud category_catalog.json: {CATALOG_PATH}")

    existing = load_existing_translations()
    new_translations: Dict[str, str] = {}
    for path in name_paths:
        new_translations[path] = existing.get(path, "")

    if new_translations != existing or not TRANSLATION_PATH.exists():
        write_translations(new_translations)
        log(f"✔ Uuendatud category_translation.json: {TRANSLATION_PATH}")
    else:
        log("✔ category_translation.json oli juba ajakohane")

    ensure_runlist()
    log("Valmis.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

