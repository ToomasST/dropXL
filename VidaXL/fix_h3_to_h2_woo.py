"""
Ühekordne skript: asenda kõikides WooCommerce toodetes <h3> → <h2> kirjeldustes.

Kasutus:
    python fix_h3_to_h2_woo.py --dry-run   # ainult loendab, ei muuda
    python fix_h3_to_h2_woo.py              # teeb muudatused päriselt
"""

import os
import re
import sys
import time
from pathlib import Path

import requests
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(), override=False)

# ── WooCommerce auth ──────────────────────────────────────────────

SITE = (os.getenv("WP_BASE_URL") or os.getenv("WC_SITE_URL") or "").rstrip("/")
CK = os.getenv("WC_CONSUMER_KEY")
CS = os.getenv("WC_CONSUMER_SECRET")

if not SITE or not CK or not CS:
    sys.exit("❌ WP_BASE_URL / WC_CONSUMER_KEY / WC_CONSUMER_SECRET puuduvad .env-ist")

AUTH = (CK, CS)
API = f"{SITE}/wp-json/wc/v3"

# ── Helpers ───────────────────────────────────────────────────────

H3_OPEN = re.compile(r"<h3([^>]*)>", re.IGNORECASE)
H3_CLOSE = re.compile(r"</h3>", re.IGNORECASE)


def has_h3(html: str) -> bool:
    return bool(H3_OPEN.search(html))


def replace_h3(html: str) -> str:
    out = H3_OPEN.sub(r"<h2\1>", html)
    out = H3_CLOSE.sub("</h2>", out)
    return out


def fetch_all_products():
    """Tõmbab kõik tooted WooCommerce'ist (ainult id + description)."""
    products = []
    page = 1
    while True:
        resp = requests.get(
            f"{API}/products",
            auth=AUTH,
            params={"per_page": 100, "page": page, "status": "any"},
            timeout=60,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        products.extend(batch)
        total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
        print(f"  Leht {page}/{total_pages} – {len(batch)} toodet")
        if page >= total_pages:
            break
        page += 1
    return products


def update_product_description(product_id: int, new_description: str):
    """Uuendab toote kirjeldust WooCommerce'is."""
    resp = requests.put(
        f"{API}/products/{product_id}",
        auth=AUTH,
        json={"description": new_description},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


# ── Main ──────────────────────────────────────────────────────────

def main():
    dry_run = "--dry-run" in sys.argv

    if dry_run:
        print("🔍 DRY-RUN režiim – muudatusi ei tehta\n")
    else:
        print("🔧 LIVE režiim – muudatused tehakse WooCommerce'is\n")

    print("📥 Tõmban kõik tooted...")
    products = fetch_all_products()
    print(f"   Kokku {len(products)} toodet\n")

    to_fix = []
    for p in products:
        desc = p.get("description") or ""
        if has_h3(desc):
            to_fix.append(p)

    print(f"🔎 Leitud {len(to_fix)} toodet, kus on <h3>\n")

    if not to_fix:
        print("✅ Midagi parandada pole!")
        return

    updated = 0
    errors = 0
    for i, p in enumerate(to_fix, 1):
        pid = p["id"]
        sku = p.get("sku", "?")
        name = p.get("name", "?")[:50]
        old_desc = p["description"]
        new_desc = replace_h3(old_desc)

        if dry_run:
            h3_count = len(H3_OPEN.findall(old_desc))
            print(f"  [{i}/{len(to_fix)}] id={pid} SKU={sku} – {h3_count}x <h3> → <h2>")
        else:
            try:
                update_product_description(pid, new_desc)
                updated += 1
                print(f"  ✅ [{i}/{len(to_fix)}] id={pid} SKU={sku} – {name}")
            except Exception as e:
                errors += 1
                print(f"  ❌ [{i}/{len(to_fix)}] id={pid} SKU={sku} – {e}")

            # Rate limit: ~2 req/s
            if i % 10 == 0:
                time.sleep(1)

    print()
    if dry_run:
        print(f"📊 Kokku {len(to_fix)} toodet vajavad parandamist")
    else:
        print(f"📊 Uuendatud: {updated}, vigaseid: {errors}")

    print("Valmis!")


if __name__ == "__main__":
    main()
