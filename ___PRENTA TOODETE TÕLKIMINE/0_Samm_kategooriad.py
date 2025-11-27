#!/usr/bin/env python3
"""0. samm: kategooriate inventuur PRENTA projekti uues töövoos.

Eesmärk
-------
- Koguda allikasüsteemist (nt Prenta API-st) värske kategooria-hierarhia.
- Hoida kogu kategooriate tõlkimise ja valiku loogika kahes failis:
  - ``category_translation.json`` – master-tabel originaalradade ja tõlgitud radade jaoks.
  - ``category_runlist.json`` – projekti jaoks valitud kategooriate nimekiri.

Reeglid
-------
- ``category_translation.json`` struktuur:
  - Võtmed on alati kategooriarajad kujul: ``"All > Saleable > ... > ..."``.
  - Eraldaja kategooriate vahel on alati ``" > "`` (tühik, nooleke, tühik).
  - Väärtus on vastav tõlgitud rada (string) või tühi string, kui rada on veel tõlkimata.

- Igal skripti jooksutamisel:
  - Loetakse olemasolev ``category_translation.json`` sisse, kui see on olemas.
  - Kõik olemasolevad võtmed JA nende väärtused säilitatakse muutmata kujul.
    - Tõlgitud väärtusi ei kirjutata üle.
    - Tühje väärtusi (""), mida inimene pole veel täitnud, ei muudeta.
  - Uued kategooriarajad, mis on allikasüsteemis olemas, kuid failis puuduvad,
    lisatakse võtmetena kujul ``"rada": ""`` (tõlkimata placeholder).

- ``category_runlist.json`` reeglid:
  - Faili struktuur jääb samaks nagu olemasolevas töövoos (nt JSON massiiv).
  - Kui faili ei ole, luuakse see esmakordsel jooksutamisel vaikimisi sisuga ``[]``.
  - Olemasolevat sisu (valitud kategooriaid) skript ei muuda ega kustuta.

- Selles uues töövoos skript EI loo ega kasuta enam ``data/category_catalog.json`` faili.
  Kategooria-hierarhia inventuuri jaoks piisab alati ``category_translation.json`` võtmetest.

Sel failis kirjeldatud reeglid on Samm 0 alus – järgmised sammud (toodete kogumine,
integreerimine, tõlkimine, üleslaadimine) toetuvad sellele, et kategooriate tõlked
ja valikud on hallatud ainult nende kahe faili kaudu.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from dotenv import find_dotenv, load_dotenv

# Töövoo kausta lisamine sys.path-i, et samas kaustas olev prenta_fetch.py oleks leitav.
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from prenta_fetch import ClientConfig, PrentaClient
TRANSLATION_PATH = ROOT / "category_translation.json"
RUNLIST_PATH = ROOT / "category_runlist.json"


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def build_category_paths(categories: List[Dict[str, Any]]) -> Dict[Any, str]:
    """Ehita id -> path ("All > ... > ...") kaardistus API kategooriaandmete põhjal."""

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kogu Prenta kategooriad ja uuenda tõlke/runlist faile (uus töövoog)")
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

    paths = build_category_paths(categories)

    existing = load_existing_translations()
    updated = False

    for cat in categories:
        cid = cat.get("id")
        path = paths.get(cid, "")
        if not path:
            path = str(cat.get("name") or "").strip()
        if not path:
            continue
        if path not in existing:
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

