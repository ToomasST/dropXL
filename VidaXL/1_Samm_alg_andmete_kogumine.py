#!/usr/bin/env python3
"""1. samm: feedide laadimine ja uuendamine DropXL/VidaXL töövoos.

Eesmärk
-------
- Laeb alla VidaXL põhifeedi (CSV ZIP) ja laoseisu/hinna feedi.
- Salvestab feedid kausta ``data/feeds``, mida 2. samm kasutab.

Reeglid
-------
- Main feed salvestatakse ZIP-ina ja lahti pakituna.
- Offer feed salvestatakse CSV-na.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Optional

import requests
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parent
FEEDS_DIR = ROOT / "data" / "feeds"

MAIN_FEED_ZIP_URL = (
    "https://feed.vidaxl.io/api/v1/feeds/download/"
    "e8eb166c-2dd3-4930-8c18-0ae5abb33245/EE/vidaXL_ee_dropshipping.csv.zip"
)
OFFER_FEED_URL = (
    "https://feed.vidaxl.io/api/v1/feeds/download/"
    "e8eb166c-2dd3-4930-8c18-0ae5abb33245/EE/vidaXL_ee_dropshipping_offer.csv"
)

MAIN_ZIP_NAME = "vidaXL_ee_dropshipping.csv.zip"
MAIN_EXTRACT_DIR = "vidaXL_ee_dropshipping"
OFFER_NAME = "vidaXL_ee_dropshipping_offer.csv"


def _download(url: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        with target.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    fh.write(chunk)


def _download_main_feed(url: str) -> Path:
    zip_path = FEEDS_DIR / MAIN_ZIP_NAME
    _download(url, zip_path)
    extract_dir = FEEDS_DIR / MAIN_EXTRACT_DIR
    extract_dir.mkdir(parents=True, exist_ok=True)
    with ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
    return zip_path


def _download_offer_feed(url: str) -> Path:
    target = FEEDS_DIR / OFFER_NAME
    _download(url, target)
    return target


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lae VidaXL feedid alla (DropXL töövoog).")
    parser.add_argument("--main-url", default=MAIN_FEED_ZIP_URL, help="Main feed CSV ZIP URL")
    parser.add_argument("--offer-url", default=OFFER_FEED_URL, help="Offer feed CSV URL")
    parser.add_argument("--skip-main", action="store_true", help="Ära lae main feedi")
    parser.add_argument("--skip-offer", action="store_true", help="Ära lae offer feedi")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started = time.time()
    results = {}

    if not args.skip_main:
        main_zip = _download_main_feed(args.main_url)
        results["main_zip"] = str(main_zip)
        results["main_extract_dir"] = str(FEEDS_DIR / MAIN_EXTRACT_DIR)

    if not args.skip_offer:
        offer_path = _download_offer_feed(args.offer_url)
        results["offer_csv"] = str(offer_path)

    results["feeds_dir"] = str(FEEDS_DIR)
    results["elapsed_seconds"] = round(time.time() - started, 2)
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
