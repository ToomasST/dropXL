import os
import json
import pandas as pd
import csv
from openai import OpenAI
import re
import html
import requests
from pathlib import Path
from typing import Any, Dict, List, Optional
import argparse
import time
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load API key from .env (no hardcoded keys)
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass
client = OpenAI()

# JSON sisend / väljund
BASE = Path(__file__).parent
PROCESSED_DIR = BASE / "data" / "processed" / "products"
GROUPED_PROCESSED = BASE / "data" / "processed" / "products_grouped.json"
# Universaalse töövoo sisend: Step 2 väljund
STEP2_INPUT = BASE / "2_samm_tooteinfo.json"
OUT_DIR = BASE / "data" / "tõlgitud"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / "products_translated_grouped.json"
RUNLIST_FILE = BASE / "category_runlist.json"
LOG_DIR = BASE / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.now().strftime("%Y-%m-%d_%H%M%S")
LOG_FILE = LOG_DIR / f"run_{RUN_TS}.log"
EAN_CONFLICT_FILE = LOG_DIR / f"ean_conflicts_{RUN_TS}.csv"
EAN_LOG_LOCK = threading.Lock()
DEBUG_DIR = BASE / "data" / "debug_traces"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)
ATTR_CACHE_FILE = BASE / "data" / "attribute_translations.json"
REQUEST_TIMEOUT_SECONDS = 5400.0
OPENAI_SERVICE_TIER = "default"  # Kasuta "auto", "default", "flex" või "priority"
WORKERS = 1  # Paralleelselt töödeldavate toodete arv; 1 = ilma paralleelita
USE_STEP5_FINAL_REVIEW = False  # Lülita välja, kui lõppkontrolli pole vaja
USE_STEP7_ATTR_TRANSLATE = False  # Lülita välja, kui atribuudid on juba piisavad
USE_STEP8_ATTR_ENRICH = False  # Lülita välja, kui olemasolevad atribuudid piisavad
USE_RUNLIST_FILTER = False  # Lülita välja, et töödelda järjest kõiki sisendtooteid
GROUP_LOCK = threading.Lock()
WOO_SKU_CACHE: set[str] = set()
WOO_EAN_CACHE: set[str] = set()
# EAN-id, mille puhul leidsime vaste WooCommerce'i EAN-cache'ist (_bp_gtin13)
WOO_EAN_MATCHED_IN_WOO: set[str] = set()
WOO_SKU_CACHE_READY = False
WOO_SKU_CACHE_UNAVAILABLE = False

def log(msg: str) -> None:
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

def load_attr_cache() -> Dict[str, Any]:
    try:
        if ATTR_CACHE_FILE.exists():
            return json.loads(ATTR_CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return {}

def save_attr_cache(cache: Dict[str, Any]) -> None:
    try:
        ATTR_CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

def _html_text_length(value: str) -> int:
    if not value or not isinstance(value, str):
        return 0
    plain = html.unescape(re.sub(r"<[^>]+>", " ", value))
    plain = re.sub(r"\s+", " ", plain).strip()
    return len(plain)

# -----------------------------
# Attribute canonicalization
# -----------------------------
ATTR_CANON_MAP: Dict[str, str] = {
    # canonical_et: aliases (lowercased keys pointing to canonical)
}
# Build reverse map once
_attr_alias_pairs = [
    ("Mõõdud", ["mõõdud", "mõõtmed", "dimensioonid", "suurus", "mõõt" ]),
    # Canonicalize all weight-like names to 'Netokaal' to avoid duplicating Woo 'weight' (which includes packaging)
    ("Netokaal", [
        "netokaal", "neto kaal", "kaal (neto)", "kaal neto",
        "net weight", "net wt", "net mass",
        "kaal", "weight"
    ]),
    ("Maht", ["maht", "mahutavus", "volume" ]),
    ("Materjal", ["materjal", "materjalid", "material" ]),
    ("Värvus", ["värvus", "värv", "color" ]),
    ("Võimsus", ["võimsus", "power" ]),
    ("Toide", ["toide", "toiteallikas", "power supply", "input voltage" ]),
    ("Ühilduvus", ["ühilduvus", "compatible with", "compatibility" ]),
    ("Garantii", ["garantii", "garantiiaeg", "warranty" ]),
    ("EAN", ["ean", "gtin", "gtin13", "barcode" ]),
]
for canon, aliases in _attr_alias_pairs:
    for a in aliases:
        ATTR_CANON_MAP[a.lower()] = canon

def canonicalize_attr_name(name: str) -> str:
    key = (name or "").strip()
    if not key:
        return key
    canon = ATTR_CANON_MAP.get(key.lower())
    return canon or key

def _truncate_soft(s: str, limit: int = 60) -> str:
    s = (s or "").strip()
    if len(s) <= limit:
        return s
    cut = s.rfind(" ", 0, limit)
    return (s[:cut] if cut > limit * 0.5 else s[:limit]).rstrip()

def normalize_attr_value(attr_name: str, value: str) -> str:
    n = canonicalize_attr_name(attr_name)
    v = (value or "").strip()
    # unify spaces
    v = re.sub(r"\s+", " ", v)
    # common unit fixes
    # use multiplication sign × for dimensions like 60 x 40 x 20 cm
    if n == "Mõõdud":
        v = re.sub(r"(?i)\b(x|×)\b", "×", v)
        v = re.sub(r"\s*×\s*", " × ", v)
        # ensure space before units cm/mm
        v = re.sub(r"(?i)(\d)(cm|mm)", r"\1 \2", v)
        # collapse multiple spaces
        v = re.sub(r"\s+", " ", v).strip()
    elif n == "Maht":
        # normalize liters and milliliters spacing: 13 l, 50 ml
        v = re.sub(r"(?i)(\d)\s*(l|ml)", lambda m: f"{m.group(1)} {m.group(2).lower()}", v)
    elif n in ("Kaal", "Netokaal"):
        v = re.sub(r"(?i)(\d)\s*(kg|g)", lambda m: f"{m.group(1)} {m.group(2).lower()}", v)
    elif n == "Võimsus":
        v = re.sub(r"(?i)(\d)\s*w\b", lambda m: f"{m.group(1)} W", v)
    elif n == "Toide":
        # prefer pattern like "12 V DC" or "230 V AC"
        v = re.sub(r"(?i)(\d)\s*v\s*(dc|ac)?", lambda m: f"{m.group(1)} V {m.group(2).upper()}".strip(), v)
        v = v.replace("  ", " ").strip()
    return v

# Attributes that should not be shown to end customers
EXCLUDED_ATTR_KEYS = {
    "hs-kood",
    "hs kood",
    "hs code",
    "minimaalne kogus jaetellimuses",
    "minimaalne kogus hulgimüügitellimuses",
    "minimum order quantity",
    "min order quantity",
    "minimum retail order quantity",
    "minimum wholesale order quantity",
    "legal documents",
}

def is_excluded_attr(name: str) -> bool:
    return (name or "").strip().lower() in EXCLUDED_ATTR_KEYS

def retry_api_call(fn, attempts: int = 3, backoff: float = 2.0):
    """
    Execute fn() with retries and exponential backoff.
    backoff seconds grow as backoff * (2**(attempt-1)) between attempts.
    """
    last_err = None
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:
            last_err = e
            if i >= attempts:
                log(f"API call failed after {attempts} attempts: {e}")
                raise
            sleep_s = max(0.5, backoff * (2 ** (i - 1)))
            log(f"API call error (attempt {i}/{attempts}): {e} — retrying in {sleep_s:.1f}s ...")
            try:
                time.sleep(sleep_s)
            except Exception:
                pass

def create_with_retry(_step_key: str = None, _sku: str = None, **kwargs):
    try:
        payload = {
            "model": kwargs.get("model"),
            "instructions": kwargs.get("instructions"),
            "input": kwargs.get("input"),
            "tools": kwargs.get("tools"),
            "text": kwargs.get("text"),
            "reasoning": kwargs.get("reasoning"),
            "previous_response_id": kwargs.get("previous_response_id"),
            "service_tier": kwargs.get("service_tier") or OPENAI_SERVICE_TIER,
        }
        if _step_key and _sku:
            save_debug_json(_sku, f"{_step_key}_input", payload)
    except Exception:
        pass
    start_ts = time.time()
    log(f"API call start: {_step_key or 'unknown_step'} ({_sku or ''})")
    def _do():
        t = kwargs.pop("timeout", None)
        to = float(t) if t else REQUEST_TIMEOUT_SECONDS
        if OPENAI_SERVICE_TIER and not kwargs.get("service_tier"):
            kwargs["service_tier"] = OPENAI_SERVICE_TIER
        if to is None:
            return client.responses.create(**kwargs)
        return client.with_options(timeout=to).responses.create(**kwargs)
    # Heartbeat logger every 30s while waiting
    stop_evt = threading.Event()
    def _heartbeat():
        try:
            while not stop_evt.wait(30.0):
                elapsed = int(time.time() - start_ts)
                log(f"… ootan vastust: {_step_key or 'unknown_step'} ({_sku or ''}) — {elapsed}s")
        except Exception:
            pass
    hb = threading.Thread(target=_heartbeat, daemon=True)
    try:
        hb.start()
    except Exception:
        hb = None
    try:
        resp = retry_api_call(_do)
    finally:
        try:
            stop_evt.set()
            if hb:
                hb.join(timeout=1.0)
        except Exception:
            pass
    dur = time.time() - start_ts
    log(f"API call done: {_step_key or 'unknown_step'} ({_sku or ''}) in {dur:.1f}s")
    return resp

def normalize_prefix(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    val = raw.rstrip("/")
    val = val.replace(" > ", "/").replace(">", "/")
    if not val.endswith("/"):
        val += "/"
    while "//" in val:
        val = val.replace("//", "/")
    return val

def load_run_prefixes() -> List[str]:
    if not RUNLIST_FILE.exists():
        return []
    try:
        data = json.loads(RUNLIST_FILE.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [normalize_prefix(str(x)) for x in data if normalize_prefix(str(x))]
        return []
    except Exception:
        return []

def load_existing_grouped() -> Dict[str, List[Dict[str, Any]]]:
    if not OUT_FILE.exists():
        return {}
    try:
        return json.loads(OUT_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

def index_existing_skus(grouped: Dict[str, List[Dict[str, Any]]]) -> Dict[str, str]:
    idx: Dict[str, str] = {}
    for grp, items in grouped.items():
        for it in items or []:
            sku = str(it.get("sku") or "").strip()
            if sku:
                idx[sku] = grp
    return idx

def extract_ean(meta: Optional[List[Dict[str, Any]]] = None) -> str:
    for entry in meta or []:
        key = str((entry or {}).get("key") or "").strip()
        if key != "_bp_gtin13":
            continue
        value = str((entry or {}).get("value") or "").strip()
        if value:
            return value
    return ""

def index_existing_eans(grouped: Dict[str, List[Dict[str, Any]]]) -> Dict[str, str]:
    idx: Dict[str, str] = {}
    for grp, items in grouped.items():
        for it in items or []:
            ean = extract_ean(it.get("meta_data"))
            if ean and ean not in idx:
                idx[ean] = str(it.get("sku") or "").strip()
    return idx

def top_level_category(product: Dict[str, Any]) -> str:
    cats = product.get("categories") or []
    if not cats:
        return "Unmapped"
    nm = str((cats[0] or {}).get("name") or "").strip()
    return nm.split(" > ")[0] if nm else "Unmapped"

def _wc_site_and_auth():
    try:
        site = os.getenv('WP_BASE_URL') or os.getenv('WC_SITE_URL')
        ck = os.getenv('WC_CONSUMER_KEY')
        cs = os.getenv('WC_CONSUMER_SECRET')
        if ck and cs:
            return site, (ck, cs)
        u = os.getenv('WP_USERNAME')
        p = os.getenv('WP_APP_PASSWORD')
        if u and p:
            return site, (u, p)
        return site, None
    except Exception:
        return None, None

def _fetch_existing_woo_skus(max_pages: int = 0) -> Optional[set[str]]:
    """Lae WooCommerce'ist olemasolevad SKU-d ja EAN-id.

    - SKU-d kogutakse WOO_SKU_CACHE jaoks.
    - EAN-id kogutakse WOO_EAN_CACHE jaoks meta_data võtme _bp_gtin13 alusel.
    """
    site, auth = _wc_site_and_auth()
    if not site or not auth:
        return None
    url = f"{site}/wp-json/wc/v3/products"
    page = 1
    collected: set[str] = set()
    # tühjenda EAN cache enne täitmist
    WOO_EAN_CACHE.clear()
    consecutive_rate_limits = 0
    while True:
        params = {
            "per_page": 100,
            "page": page,
            "_fields": "id,sku,meta_data",
            "orderby": "id",
            "order": "asc",
        }
        try:
            resp = requests.get(url, auth=auth, params=params, timeout=30)
        except Exception as exc:
            log(f"⚠️ WooCommerce SKU päringu viga (page {page}): {exc}")
            return None
        if resp.status_code == 429:
            wait_s = min(30, 5 * (consecutive_rate_limits + 1))
            consecutive_rate_limits += 1
            log(f"⚠️ WooCommerce SKU päringut piiratakse (429). Ootan {wait_s}s ja proovin uuesti (page {page}).")
            try:
                time.sleep(wait_s)
            except Exception:
                pass
            continue
        consecutive_rate_limits = 0
        if resp.status_code != 200:
            log(f"⚠️ WooCommerce SKU päring vastas koodiga {resp.status_code} (page {page}).")
            return None
        try:
            data = resp.json()
        except Exception as exc:
            log(f"⚠️ WooCommerce SKU päringu JSON viga (page {page}): {exc}")
            return None
        if not isinstance(data, list):
            break
        if not data:
            break
        for item in data:
            try:
                sku = str((item or {}).get("sku") or "").strip()
            except Exception:
                sku = ""
            if sku:
                collected.add(sku)
            # korja ka EAN meta_data seast (_bp_gtin13)
            try:
                for m in (item or {}).get("meta_data") or []:
                    if not isinstance(m, dict):
                        continue
                    key = str(m.get("key") or "").strip()
                    if key != "_bp_gtin13":
                        continue
                    val = str(m.get("value") or "").strip()
                    if val:
                        WOO_EAN_CACHE.add(val)
            except Exception:
                pass
        if len(data) < 100:
            break
        page += 1
        if max_pages and page > max_pages:
            break
    return collected

def _ensure_woo_sku_cache() -> bool:
    global WOO_SKU_CACHE_READY, WOO_SKU_CACHE_UNAVAILABLE
    if WOO_SKU_CACHE_READY:
        return True
    if WOO_SKU_CACHE_UNAVAILABLE:
        return False
    log("Laadin WooCommerce SKU-de cache'i …")
    skus = _fetch_existing_woo_skus()
    if skus is None:
        WOO_SKU_CACHE_UNAVAILABLE = True
        log("⚠️ WooCommerce SKU-de eeltõmme ebaõnnestus; kasutan per-SKU päringuid.")
        return False
    WOO_SKU_CACHE.update(skus)
    WOO_SKU_CACHE_READY = True
    log(f"WooCommerce SKU-de cache valmis: {len(WOO_SKU_CACHE)} kirjet.")
    return True

def _wc_product_exists_remote(sku: str) -> bool:
    if not sku:
        return False
    site, auth = _wc_site_and_auth()
    if not site or not auth:
        return False
    try:
        url = f"{site}/wp-json/wc/v3/products"
        r = requests.get(url, auth=auth, params={"sku": sku, "per_page": 1}, timeout=20)
        if r.status_code != 200:
            return False
        data = r.json()
        return bool(data)
    except Exception:
        return False

def wc_product_exists(sku: str, ean: Optional[str] = None) -> bool:
    """Kontrolli, kas toode on WooCommerce'is olemas SKU või EAN järgi.

    - Eelistame cache'i (WOO_SKU_CACHE ja WOO_EAN_CACHE).
    - Kui cache'i ei saa laadida, tehakse varuvariant ainult SKU põhjal.
    - Kui vaste leitakse EAN-i järgi, logime selle EAN-i WOO_EAN_MATCHED_IN_WOO set'i,
      et jooksu lõpus saaksime teha kokkuvõtte.
    """
    if not sku and not ean:
        return False
    if _ensure_woo_sku_cache():
        if sku and sku in WOO_SKU_CACHE:
            return True
        if ean and ean in WOO_EAN_CACHE:
            WOO_EAN_MATCHED_IN_WOO.add(ean)
            return True
        return False
    # Varuvariant: kui cache'i ei saanud luua, kontrolli ainult SKU järgi
    if sku:
        return _wc_product_exists_remote(sku)
    return False

def ensure_meta(meta: List[Dict[str, Any]], key: str, value: str) -> List[Dict[str, Any]]:
    found = False
    for m in meta:
        if str(m.get("key")) == key:
            m["value"] = value
            found = True
            break
    if not found:
        meta.append({"key": key, "value": value})
    return meta

run_prefixes = load_run_prefixes()
grouped = load_existing_grouped()
existing_idx = index_existing_skus(grouped)
existing_eans = index_existing_eans(grouped)

def find_existing_translated_product(sku: str) -> Optional[Dict[str, Any]]:
    if not sku:
        return None
    for items in grouped.values():
        for it in items or []:
            try:
                if str(it.get("sku") or "").strip() == sku:
                    return it
            except Exception:
                continue
    return None

def log_ean_conflict_for_product(new_product: Dict[str, Any], ean_code: str) -> None:
    try:
        new_sku = str((new_product or {}).get("sku") or "").strip()
        if not (ean_code and new_sku):
            return
        existing_sku = existing_eans.get(ean_code, "")
        existing_product = find_existing_translated_product(existing_sku) if existing_sku else None
        new_name = str((new_product or {}).get("name") or new_product.get("original_name") or "").strip()
        new_category = str(((new_product or {}).get("source") or {}).get("prenta_category_path") or "").strip()
        existing_name = ""
        existing_category = ""
        if existing_product:
            existing_name = str(existing_product.get("name") or existing_product.get("original_name") or "").strip()
            existing_category = str(((existing_product.get("source") or {}).get("prenta_category_path")) or "").strip()
        if not existing_category:
            existing_category = existing_idx.get(existing_sku, "")
        row = [
            datetime.now().isoformat(timespec="seconds"),
            new_sku,
            ean_code,
            existing_sku,
            new_name,
            existing_name,
            new_category,
            existing_category,
        ]
        with EAN_LOG_LOCK:
            write_header = not EAN_CONFLICT_FILE.exists()
            with open(EAN_CONFLICT_FILE, "a", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                if write_header:
                    writer.writerow([
                        "timestamp",
                        "sku_new",
                        "ean",
                        "sku_existing",
                        "name_new",
                        "name_existing",
                        "category_new",
                        "category_existing",
                    ])
                writer.writerow(row)
    except Exception:
        pass

# CLI filters
parser = argparse.ArgumentParser(description="Tõlgi processed tooted ja salvesta koond JSONi")
parser.add_argument("--only-sku", action="append", default=[], help="Töötle ainult neid SKUsid (võib korrata või anda komadega)")
parser.add_argument("--limit", type=int, default=0, help="Töötle maksimaalselt N uut tõlget (0=piiranguta)")
args = parser.parse_args()

only_skus: set[str] = set()
for token in args.only_sku or []:
    for part in str(token).split(','):
        part = part.strip()
        if part:
            only_skus.add(part)

def clean_product_description(html):
    """
    Eemaldab HTML-st ilmselged tehnilised jäägid (inline CSS, <style>/<script>
    plokid, toor-CSS reeglid) ja normaliseerib tühikud.

    Ei lisa ega asenda teksti, ei muuda <img src> väärtusi ega tõlgi pealkirju.
    """
    if not html:
        return html
    # Eemalda kõik inline CSS atribuudid (näiteks style="...")
    cleaned_html = re.sub(r'\s*style="[^"]*"', '', html)
    # Eemalda <style> ja <script> plokid täielikult
    cleaned_html = re.sub(r'(?is)<style[^>]*>.*?</style>', '', cleaned_html)
    cleaned_html = re.sub(r'(?is)<script[^>]*>.*?</script>', '', cleaned_html)

    # Eemalda lehele sattunud toor-CSS reeglid (nt .table-wrapper{...}, .class,.class2{...})
    # Konservatiivne muster: klassi/id selektorid ja süsihargid kuni 120 märki enne esimest '{'
    cleaned_html = re.sub(r'(?m)(?:^|[\s>])(?:[.#][\w\-](?:[^{}]{0,120}?))\{[^}]*\}', ' ', cleaned_html)
    # Eemalda võimalikud jäänukid nagu "> .class{...}"
    cleaned_html = re.sub(r'(?m)(?:^|[\s>])(?:[\w#.*> ,\-]+)\{[^}]*\}', ' ', cleaned_html)

    # Normaliseeri liigsed tühikud
    cleaned_html = re.sub(r'\s+', ' ', cleaned_html).strip()

    return cleaned_html

def make_short_description_et(desc_html: str, limit: int = 280) -> str:
    # Strip HTML tags and entities, collapse whitespace, and truncate to limit without breaking mid-word
    if not desc_html:
        return ""
    txt = re.sub(r"<[^>]+>", " ", desc_html)
    txt = html.unescape(txt)
    txt = txt.replace("\xa0", " ").replace("&nbsp;", " ")
    txt = re.sub(r"\s+", " ", txt).strip()
    if len(txt) <= limit:
        return txt
    cut = txt.rfind(" ", 0, limit)
    if cut < max(120, int(limit * 0.5)):
        cut = limit
    return txt[:cut].rstrip()

# -----------------------------
# Content post-processing helpers
# -----------------------------
def clean_double_asterisks(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    cleaned = value.replace("**", "")
    cleaned = re.sub(r'\s{2,}', " ", cleaned)
    return cleaned.strip()

def save_debug_json(sku: str, step_key: str, data: Any) -> None:
    try:
        d = DEBUG_DIR / (sku or "unknown_sku")
        d.mkdir(parents=True, exist_ok=True)
        fp = d / f"{step_key}.json"
        fp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        # Also mirror a trimmed version into the run log for quick inspection
        try:
            log_step_output(sku, step_key, data)
        except Exception:
            pass
    except Exception:
        pass

def log_step_output(sku: str, step_key: str, data: Any, max_chars: int = 0) -> None:
    try:
        raw = json.dumps(data, ensure_ascii=False)
    except Exception:
        raw = str(data)
    total_len = len(raw or "")
    header = f"[DEBUG:{sku}] {step_key}: payload_len={total_len}, showing=all"
    try:
        # Write full payload into the log file and echo a one-liner to console via log()
        log(header)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"----- {step_key} BEGIN ({sku}) -----\n")
            f.write(raw + "\n")
            f.write(f"----- {step_key} END ({sku}) -----\n")
    except Exception:
        pass

products: List[Dict[str, Any]] = []

# Eelistatud sisend on Step 2 väljund (universaalne skeem)
if STEP2_INPUT.exists():
    try:
        data = json.loads(STEP2_INPUT.read_text(encoding="utf-8"))
        if isinstance(data, list):
            products = [it for it in data if isinstance(it, dict)]
    except Exception:
        products = []

# Tagavara: vana groupitud sisend või per-toode failid
if not products:
    if GROUPED_PROCESSED.exists():
        try:
            gp = json.loads(GROUPED_PROCESSED.read_text(encoding="utf-8"))
            if isinstance(gp, dict):
                for grp, items in gp.items():
                    for it in (items or []):
                        if isinstance(it, dict):
                            products.append(it)
        except Exception:
            products = []
    if not products:
        # Fallback: loe per-toode failid, kui need on alles
        for fp in sorted(PROCESSED_DIR.glob("*.json")):
            try:
                products.append(json.loads(fp.read_text(encoding="utf-8")))
            except Exception:
                continue

if not products:
    log("⚠️ Pole sisendkoondfaili data/processed/products_grouped.json ega per-toote faile.")

log(f"Leidsin {len(products)} sisendtoodet. Eesmärk: {args.limit or 'piiranguta'} uut tõlget.")
added = 0
skipped_existing = 0

def _atomic_write_grouped():
    try:
        tmp = OUT_FILE.with_suffix('.tmp')
        tmp.write_text(json.dumps(grouped, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(OUT_FILE)
    except Exception as e:
        log(f"⚠️ Kirjutamise viga: {e}")

def process_one_product(prod: Dict[str, Any], index: int) -> Dict[str, int]:
    local_added = 0
    local_skipped = 0
    sku = str(prod.get("sku") or "").strip()
    meta_data = prod.get("meta_data") or []
    ean_code = extract_ean(meta_data)
    
    # Token usage accumulator for this product
    token_usage: Dict[str, int] = {
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
        "cached_tokens": 0,
    }
    # Per-step usage map
    token_steps: Dict[str, Dict[str, int]] = {}

    def _get_usage_dict(resp: Any) -> Dict[str, int]:
        data: Dict[str, int] = {
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0,
            "cached_tokens": 0,
        }
        usage = getattr(resp, "usage", None)
        if not usage:
            usage = getattr(resp, "response", None)
        if not usage:
            return data
        def uget(k: str) -> int:
            try:
                if isinstance(usage, dict):
                    return int(usage.get(k) or 0)
                return int(getattr(usage, k, 0) or 0)
            except Exception:
                return 0
        data["input_tokens"] = uget("input_tokens")
        data["output_tokens"] = uget("output_tokens")
        data["total_tokens"] = uget("total_tokens")
        data["cache_creation_input_tokens"] = uget("cache_creation_input_tokens")
        data["cache_read_input_tokens"] = uget("cache_read_input_tokens")
        data["cached_tokens"] = uget("cached_tokens")
        return data

    def add_usage(resp: Any) -> None:
        try:
            u = _get_usage_dict(resp)
            token_usage["input_tokens"] += u.get("input_tokens", 0)
            token_usage["output_tokens"] += u.get("output_tokens", 0)
            token_usage["total_tokens"] += u.get("total_tokens", 0)
            token_usage["cache_creation_input_tokens"] += u.get("cache_creation_input_tokens", 0)
            token_usage["cache_read_input_tokens"] += u.get("cache_read_input_tokens", 0)
            token_usage["cached_tokens"] += u.get("cached_tokens", 0)
        except Exception:
            pass

    def record_usage(step_name: str, resp: Any) -> None:
        try:
            u = _get_usage_dict(resp)
            # keep only non-zero values
            token_steps[step_name] = {k: int(v) for k, v in u.items() if v}
        except Exception:
            pass
    if not sku:
        return {"added": 0, "skipped_existing": 0}
    if only_skus and sku not in only_skus:
        return {"added": 0, "skipped_existing": 0}
    # Runlist filter (source category prefix)
    if USE_RUNLIST_FILTER and run_prefixes:
        # Kasuta nii algset kategooriateed (path) kui ka tõlgitud teed (translated_path).
        raw_path = ""
        raw_translated = ""
        try:
            cat_obj = prod.get("category") or {}
            raw_path = str(cat_obj.get("path") or "")
            raw_translated = str(cat_obj.get("translated_path") or "")
        except Exception:
            raw_path = ""
            raw_translated = ""

        candidates: List[str] = []
        if raw_path:
            candidates.append(normalize_prefix(raw_path))
        if raw_translated:
            candidates.append(normalize_prefix(raw_translated))

        # Fallback vana skeemi peale (source.prenta_category_path), kui midagi ei leitud
        if not candidates:
            try:
                legacy = str(((prod.get("source") or {}).get("prenta_category_path")) or "")
            except Exception:
                legacy = ""
            if legacy:
                candidates.append(normalize_prefix(legacy))

        match_found = False
        for c in candidates:
            if c and any(c.startswith(pref) for pref in run_prefixes):
                match_found = True
                break
        if not match_found:
            log(f"Jätan vahele (runlist ei klapi): {sku}, kategooriateed={candidates}")
            return {"added": 0, "skipped_existing": 0}
    # Skip if already translated in grouped file
    with GROUP_LOCK:
        if sku in existing_idx:
            local_skipped += 1
            log(f"Jätan vahele (juba tõlgitud): {sku}")
            return {"added": 0, "skipped_existing": local_skipped}
        if ean_code and ean_code in existing_eans:
            log_ean_conflict_for_product(prod, ean_code)
            local_skipped += 1
            log(f"Jätan vahele (EAN juba esineb): {sku} / {ean_code}")
            return {"added": 0, "skipped_existing": local_skipped}

    # Skip if product already exists in WooCommerce (avoid re-translating existing shop items)
    try:
        if wc_product_exists(sku, ean_code):
            local_skipped += 1
            log(f"Jätan vahele (juba e-poes olemas SKU/EAN järgi): {sku} / {ean_code or '-'}")
            return {"added": 0, "skipped_existing": local_skipped}
    except Exception:
        # On connectivity error, proceed with translation rather than fail the whole run
        pass

    # Extract fields for prompts
    product_name = str(prod.get("name") or "")
    product_description = str(prod.get("description") or "").strip()
    images = prod.get("images") or []
    attributes = prod.get("attributes") or []
    first_image_url = ""
    if images:
        try:
            first_image_url = str((images[0] or {}).get("src") or "").strip()
        except Exception:
            first_image_url = ""
    context_response = None
    main_query = ""
    additional_queries: List[str] = []
    qa_pairs: List[Dict[str, Any]] = []

    # --------------------------------------------------------------
    # STEP 2+3: genereeri kõik
    # --------------------------------------------------------------
    log(f"STEP 2+3: genereeri kõik (SKU {sku})")
    input_content = [
        {
            "type": "input_text",
            "text": (
                "Genereeri tõlgitud andmete põhjal tootenimi, toote lühikirjeldus, SEO Title ja SEO Meta kirjeldus ning HTML-formaadis tootekirjeldus.\n\n"
                f"ALGNE_TOOTENIMI: {product_name}\n"
                f"ATRIBUUDID: {json.dumps(attributes, ensure_ascii=False)}\n"
                f"ORIGINAALNE_HTML_KIRJELDUS: {product_description}"
            )
        }
    ]
    if first_image_url:
        input_content.append({"type": "input_image", "image_url": first_image_url})

    try:
        combined_response = create_with_retry(
            _step_key="step2+3_all", _sku=sku,
            model="gpt-5.1",
            reasoning={"effort": "medium"},
            service_tier="default",
            previous_response_id=None,
            instructions=
            f"""
            Eesmärk:
            - Loo e-poe jaoks tootenimi, toote lühikirjeldus, SEO Title, SEO Meta kirjeldus ja detailne HTML-formaadis tootekirjeldus.

            1. Tootenimi ja lühikirjeldus:
            Tootenime koostamise reeglid:
            - Alusta tootenimetusega, mis on koos 1–3 võtmeomadusega. Esimesed sõnad peavad koheselt iseloomustama, mis tootega on tegemist ja mis on toote eesmärk/kasutuskoht.
            - Lisa detailseid tooteomadusi, mis pole veel nimetatud ja mis on vajalikult konkreetse toote eristamiseks (mõõt/maht/võimsus, materjal/värv, ühilduvus).
            - Tooteomadusi lisades püüa mõelda toote iseloomule, et kasutada kõige relevantsemat infot, mis on antud toote puhul tähtis ja vajalik teada.
            - Kui sisendis on mõõdud olemas, siis lisa mõõtmed ainult siis, kui need on toote eristamiseks olulised (eriti mööbel ja suuremad tooted). Kui toode on komplekt ja koosneb mitmest erinevast tootest, näiteks diivani komplekt, siis ära kuhja mitut mõõtu järjestikku.
            - Kui toode on komplekt (nt "tk", "komplekt", "set"), siis märgi see selgelt tootenimes (nt "20 tk", "komplekt").
            - Kasuta sisendnime kogu olulist infot: mõõdud, kogus, mudel/tüüp, eripärad. Ära jäta originaalnimest mainitud omadusi nimest välja.
            - Vormistus: max 200 tähemärki. Ühikud: 60 cm, 20 L, 250 ml, 65 W.
            - Väldi turundusklišeesid, jutumärke, semikoolonit ja punkti lõpus.

            Oluline sisendi kvaliteet:
            - Sisend on osaliselt tõlgitud ja võib sisaldada valesid tõlkeid. Ära kanna vigu edasi; paranda need loogika ja tooteinfo põhjal.
            - Näide: "terrassi vaheseinad" / "privaatsusseinad" ei ole "varikatus". Kui selline või sarnane viga ilmneb, paranda see. Kontrolli tootepilti, et kindlaks teha kas on tõlkimisel tehtud vigu.
            - Tõlkereegel: Teak/teakwood = "tiigipuu"/"tiigipuust" (mitte "tiikpuu" ega "tikkpuu").
            - Väldi väljendit "täis[puidu liik]puidust" (nt "täismännipuidust"). Kasuta "täispuidust" või "[puidu liik]puidust"; sobib ka "naturaalsest [puidu liik]puidust".
            - Väljund peab olema korrektne eesti keel; ära kasuta valesti käänatud/mitte-eestikeelseid sõnu; paranda vigased liitsõnad.
            - Väldi topelt "-ga" vormi samas fraasis; nt mitte "voodiraam liistudega põhiga", vaid "voodiraam liistudest põhjaga" (või "liistpõhjaga").
            - Kui toode on "aiasöögikomplekt", kasuta väljundis vormi "aiamööbli komplekt".
            - Eemalda ingliskeelsed jäägid; väljundis ei tohi olla ingliskeelset teksti (v.a koodid või pärisnimed).

            Head näited:
            - "Esikupink jalatsiriiuliga, hall, metallraamiga, 100 x 38,5 x 49 cm"
            - "Hall polsterdatud kahekohaline voodi peatsi ja puidust jalgadega, 160 x 200 cm"
            - "7-osaline aiamööbli komplekt recliner-funktsiooniga, must polürotang, akaatsiapuidust lauaplaadiga, laud 190 x 90 cm"
            - "Ühe inimese kontinentaalvoodi musta kangaga, polsterdatud peatsiga, 100 x 200 cm"
            - "Virnastatavad tiigipuust aiatoolid patjadega, 8 tk, roostevaba terasraam, pruun, 60 x 56 x 85 cm"

            Halvad näited:
            - "Parim nõudepesumasin ülisoodne super kvaliteetne!!!"
            - "Hamstri puur" (liiga üldine; mõõdud/eripärad puudu)

            Toote lühikirjelduse koostamise reeglid:
            - Kirjuta tootele lühikirjeldus eesti keeles, tuues esile toote olulisemad kasutegurid ja omadused.
            - Pikkus: 2–3 lauset (kokku umbes 250–300 tähemärki).
            - Hoia toon informatiivne ja neutraalne – väldi sisutühje hüüdlauseid või ülepaisutatud kiidusõnu.
            - Lühikirjeldus peaks andma kliendile kiire ja täpse ülevaate tootest: kus, kellele ja miks toodet kasutatakse, mis muret see lahendab ja mis on kliendi peamine kasu.
            - Võid kasutada sobivuse, kasutamise ja hoolduse rõhuasetusi, et lühikirjeldus vastaks tüüpilistele kliendiküsimustele, kuid ära korda küsimusi sõna-sõnalt.
            - Väldi klišeesid nagu "nagu pildil näha", "pildilt on nähtav" jne.
            - Ära kasuta kirjelduses semikoolonit ";". Lõpeta mõte punktiga ja alusta uue lausega.
            - Oluline: kasuta ainult seda infot, mis tuleneb algsetest tooteandmetest. Ära lisa tootenimesse ega lühikirjeldusse omadusi, mida sisendis ei olnud.

            2. SEO:
            - Loo olemasoleva info põhjal ka "SEO Title" ja "SEO Meta kirjeldus".
            - SEO Title: maksimaalselt 60 tähemärki (eesmärgiga 50–60), peab loomulikult sisaldama peamist otsingufraasi (toote tüüp + 1–2 võtmeomadust), olema selge ja täpne.
            - SEO Meta kirjeldus: maksimaalselt 160 tähemärki, kutsuv ja informatiivne, mitte liialt reklaamilik, kirjeldab lühidalt toote põhikasu ja omadusi.
            - Ära kasuta SEO väljundites tarnija nime ega diskreetset infot.
            - Ära kasuta semikoolonit ";" üheski väljundis (ei pealkirjades ega kirjeldustes).

            3. HTML-tootekirjeldus:
            - Kirjuta detailne tootekirjeldus eestikeeles HTML-formaadis.
            - Hoia sõnavara ühtlane ja kasuta loomulikku eesti keelt; väldi otsetõlget. Kasuta mõõtühikuid standardkujul.
            - Hoia toon neutraalne ja informatiivne ning väldi liigset reklaamikeelt.
            - Väldi katteta lubadusi ja ülepaisutatud väiteid.
            - Väldi sõnu nagu "kaaslane", "partner", "abiline".
            - Kontrolli sõnade käänete ja vormide õigsust.
            - Kasuta kirjelduse olulisimates märksõnades ja infotükkides boldi (<strong>); maksimaalselt 3 korda ühes lõigus (<p>) ja 1 kord ühes listi elemendis (<li>).
            - Ära lisa HTML kommentaare ega kopeeri juhenditeksti või kommentaaride sisu väljundisse.
            - Kui mõne ploki jaoks puudub usaldusväärne info, jäta see plokk (sh pealkiri) täielikult ära.
            - Ära kasuta tootekirjelduses tarnijale omaseid andmeid (tarnija nimi, URL-id, sisemised koodid/kaubandusandmed), sest see on diskreetne info.
            - Järgi eelnevalt kirjeldatud plokkide struktuuri ja järjekorda. Kui mõni tingimuslik plokk jääb ära, ära jäta tühja pealkirja, jätka ülejäänud plokkidega.
            - Väljund peab olema üks koherentne HTML-plokk. Kui algses kirjelduses olid <img>-elemendid, peavad kõik need elemendid väljundis alles olema (sama src); kui algses kirjelduses pilte ei olnud, ära lisa uusi <img>-elemente.
            - Ära lisa eraldi "Kiirvastused", "Kes/Milleks/Kuidas" ega muid küsimuspealkirju; Q&A sektsiooni käsitleb eraldi töövoo samm.
            - Ära lisa kirjeldusse lõpus toote põhiandmete/spec-tabelit – atribuudid hallatakse eraldi sammudes.
            - Ära maini, et tekst on tõlgitud või loodud AI poolt; tekst peab kõlama nagu ühtne, toimetatud eestikeelne tootekirjeldus.
            
            - Struktuur ja kohustuslikkuse reeglid:
                - Kohustuslikud plokid:
                    1. Ava plokk: <h3> pealkiri, mis seob toote kasuteguri lahendatava probleemiga (kasuta loomulikult olulisemaid otsingufraase) + järgnevalt <p>, mis kirjeldab väärtuspakkumist.
                    2. Peamised omadused: <h3>Peamised omadused</h3> ja sellele järgnev <ul> kuni 6–8 <li>-ga, mis seovad omaduse kliendi kasuga.
                - Tingimuslikud plokid (kasuta ainult siis, kui sisendmaterjal seda võimaldab):
                    • Algse kirjelduse ja pildiplokkide info: sinu käsutuses võib olla originaalne HTML-tootekirjeldus, mis võib sisaldada <img>-plokke. Kui originaalis on <img>-elemendid, kirjuta kirjeldus ümber loomulikuks eestikeelseks tekstiks ja SÄILITA KÕIK need <img>-elemendid (sama src). IGA lõplikus HTML-is olev <img>-element PEAB omama eestikeelset alt-attribuuti, mis lühidalt ja loomulikult kirjeldab pilti selle ümbruses oleva teksti kontekstis (ka juhul, kui algne alt oli muus keeles või puudus). Sa võid muuta, millise tekstiploki juurde konkreetne pilt paigutub, kuid ära jäta ühtegi algset <img>-elementi välja ning ära lisa uusi pilte, mida originaalis ei olnud. Kui algses kirjelduses pilte ei ole, ära lisa ise uusi <img>-elemente.
                    • Paigaldus ja kasutus: h3 + lõik või loetelu praktiliste sammudega (kasuta algkirjelduse infot, kui see on olemas).
                    • Komplektis sisalduv: h3 + loetelu või lõik, mis kirjeldab komplekti (nt mis tarvikud ja komponendid on kaasas).
                    • CTA plokk: h3 + lõik, mis võtab peamised kasutegurid kokku ja suunab ostule ilma agressiivse müügikeeleta. CTA pealkiri peab olema tegevusele suunav (nt "Miks valida [TOOTE NIMI]?", "Kas otsid [lahendust X]?", "Millal valida [TOOTE NIMI]?"). Ära kasuta meta-pealkirju nagu "Kokkuvõte", "Järeldus", "Lõppsõna" või muid sarnaseid kokkuvõttepealkirju.

            Väljund: Tagasta JSON, kus "translated_title" on tootenimi, "short_description" on toote lühikirjeldus, "seo_title" on SEO pealkiri, "seo_meta" on SEO meta kirjeldus ning "translated_description_html" on detailne tootekirjeldus HTML-formaadis.
        """,
        input=[
            {
                "role": "user",
                "content": input_content,
            }
        ],
        text={
            "verbosity": "medium",
            "format": {
                "type": "json_schema",
                "name": "translated_full_schema",
                "schema": {
                    "type": "object",
                    "properties": {
                        "translated_title": {"type": "string"},
                        "short_description": {"type": "string"},
                        "seo_title": {"type": "string"},
                        "seo_meta": {"type": "string"},
                        "translated_description_html": {"type": "string"}
                    },
                    "required": ["translated_title", "short_description", "seo_title", "seo_meta", "translated_description_html"],
                    "additionalProperties": False
                },
                "strict": True
            }
        }
    )
    except Exception as e:
        msg = str(e)
        if first_image_url and ("invalid_value" in msg or "Timeout while downloading" in msg):
            log(f"⚠️ Pildi URL ebaõnnestus; proovin ilma pildita (SKU {sku}): {first_image_url}")
            input_no_image = [input_content[0]]
            combined_response = create_with_retry(
                _step_key="step2+3_all_no_image", _sku=sku,
                model="gpt-5.1",
                reasoning={"effort": "medium"},
                service_tier="default",
                previous_response_id=None,
                instructions=
                f"""
            Eesmärk:
            - Loo e-poe jaoks tootenimi, toote lühikirjeldus, SEO Title, SEO Meta kirjeldus ja detailne HTML-formaadis tootekirjeldus.

            1. Tootenimi ja lühikirjeldus:
            Tootenime koostamise reeglid:
            - Alusta tootenimetusega, mis on koos 1–3 võtmeomadusega. Esimesed sõnad peavad koheselt iseloomustama, mis tootega on tegemist ja mis on toote eesmärk/kasutuskoht.
            - Lisa detailseid tooteomadusi, mis pole veel nimetatud ja mis on vajalikult konkreetse toote eristamiseks (mõõt/maht/võimsus, materjal/värv, ühilduvus).
            - Tooteomadusi lisades püüa mõelda toote iseloomule, et kasutada kõige relevantsemat infot, mis on antud toote puhul tähtis ja vajalik teada.
            - Kui sisendis on mõõdud olemas, siis lisa mõõtmed ainult siis, kui need on toote eristamiseks olulised (eriti mööbel ja suuremad tooted). Kui toode on komplekt ja koosneb mitmest erinevast tootest, näiteks diivani komplekt, siis ära kuhja mitut mõõtu järjestikku.
            - Kui toode on komplekt (nt "tk", "komplekt", "set"), siis märgi see selgelt tootenimes (nt "20 tk", "komplekt").
            - Kasuta sisendnime kogu olulist infot: mõõdud, kogus, mudel/tüüp, eripärad. Ära jäta originaalnimest mainitud omadusi nimest välja.
            - Vormistus: max 200 tähemärki. Ühikud: 60 cm, 20 L, 250 ml, 65 W.
            - Väldi turundusklišeesid, jutumärke, semikoolonit ja punkti lõpus.

            Oluline sisendi kvaliteet:
            - Sisend on osaliselt tõlgitud ja võib sisaldada valesid tõlkeid. Ära kanna vigu edasi; paranda need loogika ja tooteinfo põhjal.
            - Näide: "terrassi vaheseinad" / "privaatsusseinad" ei ole "varikatus". Kui selline või sarnane viga ilmneb, paranda see. Kontrolli tootepilti, et kindlaks teha kas on tõlkimisel tehtud vigu.
            - Tõlkereegel: Teak/teakwood = "tiigipuu"/"tiigipuust" (mitte "tiikpuu" ega "tikkpuu").
            - Väldi väljendit "täis[puidu liik]puidust" (nt "täismännipuidust"). Kasuta "täispuidust" või "[puidu liik]puidust"; sobib ka "naturaalsest [puidu liik]puidust".
            - Väljund peab olema korrektne eesti keel; ära kasuta valesti käänatud/mitte-eestikeelseid sõnu; paranda vigased liitsõnad.
            - Väldi topelt "-ga" vormi samas fraasis; nt mitte "voodiraam liistudega põhiga", vaid "voodiraam liistudest põhjaga" (või "liistpõhjaga").
            - Kui toode on "aiasöögikomplekt", kasuta väljundis vormi "aiamööbli komplekt".
            - Eemalda ingliskeelsed jäägid; väljundis ei tohi olla ingliskeelset teksti (v.a koodid või pärisnimed).

            Head näited:
            - "Esikupink jalatsiriiuliga, hall, metallraamiga, 100 x 38,5 x 49 cm"
            - "Hall polsterdatud kahekohaline voodi peatsi ja puidust jalgadega, 160 x 200 cm"
            - "7-osaline aiamööbli komplekt recliner-funktsiooniga, must polürotang, akaatsiapuidust lauaplaadiga, laud 190 x 90 cm"
            - "Ühe inimese kontinentaalvoodi musta kangaga, polsterdatud peatsiga, 100 x 200 cm"
            - "Virnastatavad tiigipuust aiatoolid patjadega, 8 tk, roostevaba terasraam, pruun, 60 x 56 x 85 cm"

            Halvad näited:
            - "Parim nõudepesumasin ülisoodne super kvaliteetne!!!"
            - "Hamstri puur" (liiga üldine; mõõdud/eripärad puudu)

            Toote lühikirjelduse koostamise reeglid:
            - Kirjuta tootele lühikirjeldus eesti keeles, tuues esile toote olulisemad kasutegurid ja omadused.
            - Pikkus: 2–3 lauset (kokku umbes 250–300 tähemärki).
            - Hoia toon informatiivne ja neutraalne – väldi sisutühje hüüdlauseid või ülepaisutatud kiidusõnu.
            - Lühikirjeldus peaks andma kliendile kiire ja täpse ülevaate tootest: kus, kellele ja miks toodet kasutatakse, mis muret see lahendab ja mis on kliendi peamine kasu.
            - Võid kasutada sobivuse, kasutamise ja hoolduse rõhuasetusi, et lühikirjeldus vastaks tüüpilistele kliendiküsimustele, kuid ära korda küsimusi sõna-sõnalt.
            - Väldi klišeesid nagu "nagu pildil näha", "pildilt on nähtav" jne.
            - Ära kasuta kirjelduses semikoolonit ";". Lõpeta mõte punktiga ja alusta uue lausega.
            - Oluline: kasuta ainult seda infot, mis tuleneb algsetest tooteandmetest. Ära lisa tootenimesse ega lühikirjeldusse omadusi, mida sisendis ei olnud.

            2. SEO:
            - Loo olemasoleva info põhjal ka "SEO Title" ja "SEO Meta kirjeldus".
            - SEO Title: maksimaalselt 60 tähemärki (eesmärgiga 50–60), peab loomulikult sisaldama peamist otsingufraasi (toote tüüp + 1–2 võtmeomadust), olema selge ja täpne.
            - SEO Meta kirjeldus: maksimaalselt 160 tähemärki, kutsuv ja informatiivne, mitte liialt reklaamilik, kirjeldab lühidalt toote põhikasu ja omadusi.
            - Ära kasuta SEO väljundites tarnija nime ega diskreetset infot.
            - Ära kasuta semikoolonit ";" üheski väljundis (ei pealkirjades ega kirjeldustes).

            3. HTML-tootekirjeldus:
            - Kirjuta detailne tootekirjeldus eestikeeles HTML-formaadis.
            - Hoia sõnavara ühtlane ja kasuta loomulikku eesti keelt; väldi otsetõlget. Kasuta mõõtühikuid standardkujul.
            - Hoia toon neutraalne ja informatiivne ning väldi liigset reklaamikeelt.
            - Väldi katteta lubadusi ja ülepaisutatud väiteid.
            - Väldi sõnu nagu "kaaslane", "partner", "abiline".
            - Kontrolli sõnade käänete ja vormide õigsust.
            - Kasuta kirjelduse olulisimates märksõnades ja infotükkides boldi (<strong>); maksimaalselt 3 korda ühes lõigus (<p>) ja 1 kord ühes listi elemendis (<li>).
            - Ära lisa HTML kommentaare ega kopeeri juhenditeksti või kommentaaride sisu väljundisse.
            - Kui mõne ploki jaoks puudub usaldusväärne info, jäta see plokk (sh pealkiri) täielikult ära.
            - Ära kasuta tootekirjelduses tarnijale omaseid andmeid (tarnija nimi, URL-id, sisemised koodid/kaubandusandmed), sest see on diskreetne info.
            - Järgi eelnevalt kirjeldatud plokkide struktuuri ja järjekorda. Kui mõni tingimuslik plokk jääb ära, ära jäta tühja pealkirja, jätka ülejäänud plokkidega.
            - Väljund peab olema üks koherentne HTML-plokk. Kui algses kirjelduses olid <img>-elemendid, peavad kõik need elemendid väljundis alles olema (sama src). Kui algses kirjelduses pilte ei olnud, ära lisa uusi <img>-elemente.
            - Ära lisa eraldi "Kiirvastused", "Kes/Milleks/Kuidas" ega muid küsimuspealkirju; Q&A sektsiooni käsitleb eraldi töövoo samm.
            - Ära lisa kirjeldusse lõpus toote põhiandmete/spec-tabelit – atribuudid hallatakse eraldi sammudes.
            - Ära maini, et tekst on tõlgitud või loodud AI poolt; tekst peab kõlama nagu ühtne, toimetatud eestikeelne tootekirjeldus.
            
            - Struktuur ja kohustuslikkuse reeglid:
                - Kohustuslikud plokid:
                    1. Ava plokk: <h3> pealkiri, mis seob toote kasuteguri lahendatava probleemiga (kasuta loomulikult olulisemaid otsingufraase) + järgnevalt <p>, mis kirjeldab väärtuspakkumist.
                    2. Peamised omadused: <h3>Peamised omadused</h3> ja sellele järgnev <ul> kuni 6–8 <li>-ga, mis seovad omaduse kliendi kasuga.
                - Tingimuslikud plokid (kasuta ainult siis, kui sisendmaterjal seda võimaldab):
                    • Algse kirjelduse ja pildiplokkide info: sinu käsutuses võib olla originaalne HTML-tootekirjeldus, mis võib sisaldada <img>-plokke. Kui originaalis on <img>-elemendid, kirjuta kirjeldus ümber loomulikuks eestikeelseks tekstiks ja SÄILITA KÕIK need <img>-elemendid (sama src). IGA lõplikus HTML-is olev <img>-element PEAB omama eestikeelset alt-attribuuti, mis lühidalt ja loomulikult kirjeldab pilti selle ümbruses oleva teksti kontekstis (ka juhul, kui algne alt oli muus keeles või puudus). Sa võid muuta, millise tekstiploki juurde konkreetne pilt paigutub, kuid ära jäta ühtegi algset <img>-elementi välja ning ära lisa uusi pilte, mida originaalis ei olnud. Kui algses kirjelduses pilte ei ole, ära lisa ise uusi <img>-elemente.
                    • Paigaldus ja kasutus: h3 + lõik või loetelu praktiliste sammudega (kasuta algkirjelduse infot, kui see on olemas).
                    • Komplektis sisalduv: h3 + loetelu või lõik, mis kirjeldab komplekti (nt mis tarvikud ja komponendid on kaasas).
                    • CTA plokk: h3 + lõik, mis võtab peamised kasutegurid kokku ja suunab ostule ilma agressiivse müügikeeleta. CTA pealkiri peab olema tegevusele suunav (nt "Miks valida [TOOTE NIMI]?", "Kas otsid [lahendust X]?", "Millal valida [TOOTE NIMI]?"). Ära kasuta meta-pealkirju nagu "Kokkuvõte", "Järeldus", "Lõppsõna" või muid sarnaseid kokkuvõttepealkirju.

            Väljund: Tagasta JSON, kus "translated_title" on tootenimi, "short_description" on toote lühikirjeldus, "seo_title" on SEO pealkiri, "seo_meta" on SEO meta kirjeldus ning "translated_description_html" on detailne tootekirjeldus HTML-formaadis.
                """,
                input=[
                    {
                        "role": "user",
                        "content": input_no_image,
                    }
                ],
                text={
                    "verbosity": "medium",
                    "format": {
                        "type": "json_schema",
                        "name": "translated_full_schema",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "translated_title": {"type": "string"},
                                "short_description": {"type": "string"},
                                "seo_title": {"type": "string"},
                                "seo_meta": {"type": "string"},
                                "translated_description_html": {"type": "string"}
                            },
                            "required": ["translated_title", "short_description", "seo_title", "seo_meta", "translated_description_html"],
                            "additionalProperties": False
                        },
                        "strict": True
                    }
                }
            )
        else:
            raise
    add_usage(combined_response)
    record_usage("STEP 2+3: genereeri kõik", combined_response)

    short_description = ""
    seo_title = ""
    seo_meta = ""
    try:
        desc_data = json.loads(combined_response.output_text)
        translated_title = clean_double_asterisks(desc_data.get("translated_title", "").strip())
        short_description = clean_double_asterisks(desc_data.get("short_description", "").strip())
        seo_title = clean_double_asterisks(desc_data.get("seo_title", "").strip())
        seo_meta = clean_double_asterisks(desc_data.get("seo_meta", "").strip())
        raw_html_desc = str(desc_data.get("translated_description_html", "")).strip()
        translated_description = clean_double_asterisks(raw_html_desc)
    except (json.JSONDecodeError, KeyError):
        translated_title = "ERROR: Could not parse translated description"
        short_description = ""
        seo_title = "ERROR: Could not parse SEO title"
        seo_meta = "ERROR: Could not parse SEO meta description"
        translated_description = "ERROR: Could not parse translated description"
    description_with_alt = translated_description
    save_debug_json(sku, "step2_title", {
        "translated_title": translated_title,
        "short_description": short_description,
        "seo_title": seo_title,
        "seo_meta": seo_meta
    })
    save_debug_json(sku, "step4_seo_meta", {
        "seo_title": seo_title,
        "seo_meta": seo_meta
    })
    save_debug_json(sku, "step3_description", {
        "translated_description_html": translated_description,
        "description_with_alt": description_with_alt
    })

    # --------------------------------------------------------------
    # STEP 5: Kontrolli ja paranda kõik genereeritud sisu
    # --------------------------------------------------------------
    log(f"STEP 5: lõppkontroll (SKU {sku})")
    final_response = None
    final_title = translated_title
    final_short_description = short_description
    final_description_with_alt_texts = clean_product_description(
        description_with_alt or translated_description or product_description
    )
    step5_debug_payload: Dict[str, Any] = {}

    if USE_STEP5_FINAL_REVIEW:
        log("   ℹ️  Lõppkontroll on lubatud.")
        final_response = create_with_retry(
            _step_key="step5_final_review", _sku=sku,
            model="gpt-5.1",
            reasoning={"effort": "medium"},
            previous_response_id=(
                combined_response.id if combined_response else (
                    context_response.id if context_response else None
                )
            ),
            instructions=
            """
                Oled professionaalne keeletoimetaja. Kontrolli lõplikult üle eelmistes sammudes loodud tootenimi, lühikirjeldus ja detailne tootekirjeldus ning tee vajadusel parandused.

                ✅ **Ülesande juhised:**
                - Kontrolli ja paranda grammatika, õigekeelsus ja lausete loomulikkus. Paranda käänete, sõna lõppude, pööramiste ja tehniliste lühendite kasutus.
                - Säilita faktitäpsus: ära lisa uusi omadusi ega fantaasiat, kontrolli et kõik väited tulenevad olemasolevast sisust.
                - Veendu, et lühikirjeldus oleks 2–3 lauset pikk, faktiline ning kooskõlas detailse kirjeldusega.
                - Kontrolli HTML-i korrektsust ja minimalistlikkust; säilita olemasolevad <img> elemendid koos src ja alt tekstidega ja nende järjekord. Kontrolli, et kõik sisendis olevad pildid oleks kasutatud.
                - Veendu, et tekstis poleks liialt kordusi, turunduslikku liialdamist ega ebaloomulikku tõlget.
                - Eemalda ingliskeelsed jäägid: ükski nähtav silt ega lõik ei tohi olla inglise keeles (v.a EAN ja teised pärisnimed/koodid).
                - Eemalda ebaloomulikud väljendid ja kohmakad otsetõlked (nt põhjustamatut tautoloogiat, liigseid kordusi, eba-idiomaatilisi sõnajärgi). 
                - Ära lisa eraldi "Kiirvastused"/"Kes?"/"Milleks?" plokke ega muid küsimuspealkirju; Q&A käsitletakse eraldi. Eemalda sellised plokid, kui need ilmuvad.
                - Ära lisa kirjeldusse lõpus "Põhiandmed", "Tehnilised andmed" või muid spec-loendeid; atribuudid hallatakse eraldi sammudes.
                - OLULINE! Tagastada tuleb kogu sisu 100% ja täielikult koos parandustega: pealkiri, lühikirjeldus, detailne kirjeldus. Paranduste käigus ei tohi mitte midagi kaduma minna!
            """,
            input=(
                "Kontrolli üle ja vajadusel paranda eelmistes sammudes loodud pealkiri, lühikirjeldus, detailne kirjeldus. Tagasta täielikult parandatud väärtused .\n\n"
                f"Praegune tootepealkiri: {translated_title}\n"
                f"Praegune lühikirjeldus: {short_description}\n"
                "Praegune detailne kirjeldus (HTML lubatud):\n"
                f"{description_with_alt}"
            ),
            text={
                "verbosity": "low",
                "format": {
                    "type": "json_schema",
                    "name": "final_output_schema",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "Final_Title": {"type": "string"},
                            "Final_Short_Description": {"type": "string"},
                            "Final_Description_with_alt_texts": {"type": "string"}
                        },
                        "required": [
                            "Final_Title",
                            "Final_Short_Description",
                            "Final_Description_with_alt_texts"
                        ],
                        "additionalProperties": False
                    },
                    "strict": True
                }
            }
        )
        add_usage(final_response)
        record_usage("STEP 5: lõppkontroll", final_response)

        try:
            final_data = json.loads(final_response.output_text)
            final_title = clean_double_asterisks(final_data.get("Final_Title", "").strip()) or translated_title
            final_short_description = clean_double_asterisks(final_data.get("Final_Short_Description", "").strip()) or short_description
            final_description_with_alt_texts = clean_double_asterisks(final_data.get("Final_Description_with_alt_texts", "").strip()) or (
                description_with_alt or translated_description or product_description
            )
        except (json.JSONDecodeError, KeyError):
            final_title = "ERROR: Could not parse final title"
            final_short_description = short_description
            final_description_with_alt_texts = "ERROR: Could not parse final description with alt texts"

        final_description_with_alt_texts = clean_product_description(final_description_with_alt_texts)
        step5_debug_payload = {
            "Final_Title": final_title,
            "Final_Short_Description": final_short_description,
            "Final_Description_with_alt_texts": final_description_with_alt_texts,
            "use_step5_final_review": True,
        }
    else:
        log("   ℹ️  Lõppkontroll on keelatud; kasutatakse eelmiste sammude väljundeid.")
        step5_debug_payload = {
            "Final_Title": final_title,
            "Final_Short_Description": final_short_description,
            "Final_Description_with_alt_texts": final_description_with_alt_texts,
            "use_step5_final_review": False,
            "skipped_reason": "USE_STEP5_FINAL_REVIEW is False",
        }

    if final_short_description:
        short_description = final_short_description
    else:
        final_short_description = short_description
    save_debug_json(sku, "step5_final_review", step5_debug_payload)

    # --------------------------------------------------------------
    # STEP 6: piltide ALT tekstid (ilma AI-ta)
    # --------------------------------------------------------------
    try:
        imgs = prod.get("images") or []
        if imgs:
            log(f"STEP 6: piltide ALT tekstid (SKU {sku})")
            title_base = (final_title or translated_title or product_name or "").strip()
            updated = 0
            for im in imgs:
                try:
                    src = str((im or {}).get("src") or "").strip()
                except Exception:
                    src = ""
                if not src:
                    continue
                if title_base:
                    im["alt"] = title_base
                    im["title"] = title_base
                    im["description"] = title_base
                    updated += 1
            save_debug_json(sku, "step6_images_meta", {
                "image_count": len(imgs),
                "title_base": title_base,
                "updated": updated,
            })
    except Exception as e:
        log(f"STEP 6 alt-tekstide viga: {e}")

    # Always drop excluded attributes (sh Legal Documents)
    attrs = prod.get("attributes") or []
    if attrs:
        filtered_attrs = []
        for a in attrs:
            if not isinstance(a, dict):
                continue
            raw_name = str((a or {}).get("name") or "")
            canon_name = canonicalize_attr_name(raw_name)
            if canon_name and canon_name != raw_name:
                a["name"] = canon_name
            if is_excluded_attr(canon_name):
                continue
            filtered_attrs.append(a)
        prod["attributes"] = filtered_attrs
        attrs = filtered_attrs

    # --------------------------------------------------------------
    # STEP 7: Tõlgi olemasolevad atribuudid (name ja values) cache'iga
    # --------------------------------------------------------------
    if USE_STEP7_ATTR_TRANSLATE:
        try:
            if attrs:
                log(f"STEP 7: atribuutide tõlkimine (SKU {sku})")
                cache = load_attr_cache()
                to_translate: List[Dict[str, Any]] = []
                for a in attrs:
                    try:
                        nm = str((a or {}).get("name") or "").strip()
                        if not nm:
                            continue
                        # Step 2 skeem: values-list; säilitame ka ühilduvuse options/value skeemiga
                        values = a.get("values") if isinstance(a.get("values"), list) else None
                        options = a.get("options") if isinstance(a.get("options"), list) else None
                        value = a.get("value") if isinstance(a.get("value"), str) else None

                        cache.setdefault(nm, {"name_et": None, "values": {}})
                        if not cache[nm].get("name_et"):
                            to_translate.append({"type": "name", "src": nm})

                        if values is not None:
                            for v in values:
                                s = str(v or "").strip()
                                if s and s not in cache[nm]["values"]:
                                    to_translate.append({"type": "value", "name": nm, "src": s})
                        elif options:
                            for opt in options:
                                s = str(opt or "").strip()
                                if s and s not in cache[nm]["values"]:
                                    to_translate.append({"type": "value", "name": nm, "src": s})
                        elif value:
                            s = str(value).strip()
                            if s and s not in cache[nm]["values"]:
                                to_translate.append({"type": "value", "name": nm, "src": s})
                    except Exception:
                        continue

                translations: List[Dict[str, Any]] = []
                if to_translate:
                    pairs: List[Dict[str, Any]] = []
                    for item in to_translate:
                        if item["type"] == "name":
                            pairs.append({"kind": "name", "source": item["src"]})
                        else:
                            pairs.append({"kind": "value", "attr_name": item["name"], "source": item["src"]})
                    try:
                        attr_translate_response = create_with_retry(
                            _step_key="step7_attr_translate", _sku=sku,
                            model="gpt-5.1",
                            reasoning={"effort": "medium"},
                            previous_response_id=(
                                final_response.id if final_response else (
                                    combined_response.id if combined_response else None
                                )
                            ),
                            instructions="""Tõlgi järgmised atribuudinimed ja -väärtused eesti keelde.""",
                            input=json.dumps({"pairs": pairs}, ensure_ascii=False),
                            text={
                                "verbosity": "low",
                                "format": {
                                    "type": "json_schema",
                                    "name": "attr_translation_schema",
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "translations": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "kind": {"type": "string"},
                                                        "source": {"type": "string"},
                                                        "attr_name": {"type": "string"},
                                                        "translation": {"type": "string"}
                                                    },
                                                    "required": ["kind", "source", "translation"],
                                                    "additionalProperties": False
                                                }
                                            }
                                        },
                                        "required": ["translations"],
                                        "additionalProperties": False
                                    },
                                    "strict": True
                                }
                            }
                        )
                        add_usage(attr_translate_response)
                        record_usage("STEP 7: attr translate", attr_translate_response)
                        try:
                            resp = json.loads(attr_translate_response.output_text)
                            translations = resp.get("translations", [])
                        except Exception:
                            translations = []
                    except Exception:
                        translations = []

                # Apply cache + translations
                updated_pairs = 0
                for a in attrs:
                    try:
                        nm = str((a or {}).get("name") or "").strip()
                        if not nm:
                            continue
                        cache.setdefault(nm, {"name_et": None, "values": {}})
                        for tr in translations:
                            kind = tr.get("kind")
                            src = str(tr.get("source") or "").strip()
                            val = str(tr.get("translation") or "").strip()
                            if not src or not val:
                                continue
                            if kind == "name" and src == nm:
                                cache[nm]["name_et"] = val
                            elif kind == "value":
                                attr_name = str(tr.get("attr_name") or "").strip()
                                if attr_name == nm:
                                    cache[nm]["values"][src] = val

                        # apply to attribute
                        if cache[nm].get("name_et"):
                            a["name"] = cache[nm]["name_et"]
                            updated_pairs += 1

                        values = a.get("values") if isinstance(a.get("values"), list) else None
                        options = a.get("options") if isinstance(a.get("options"), list) else None
                        value = a.get("value") if isinstance(a.get("value"), str) else None

                        if values is not None:
                            new_vals = []
                            for v in values:
                                s = str(v or "").strip()
                                new_vals.append(cache[nm]["values"].get(s, s))
                                updated_pairs += 1
                            a["values"] = new_vals
                        elif options:
                            new_opts = []
                            for opt in options:
                                s = str(opt or "").strip()
                                new_opts.append(cache[nm]["values"].get(s, s))
                                updated_pairs += 1
                            a["options"] = new_opts
                        elif value:
                            s = str(value).strip()
                            a["value"] = cache[nm]["values"].get(s, s)
                            updated_pairs += 1
                    except Exception:
                        continue
                prod["attributes"] = attrs
                save_debug_json(sku, "step7_attr_applied", {"updated": updated_pairs})
                save_attr_cache(cache)
        except Exception as e:
            log(f"STEP 7 atribuutide tõlke viga: {e}")

    # --------------------------------------------------------------
    # STEP 8: Rikasta attribuute
    # --------------------------------------------------------------
    if USE_STEP8_ATTR_ENRICH:
        try:
            log(f"STEP 8: atribuutide rikastamine (SKU {sku})")
            ctx_title = (final_title or translated_title or product_name)
            ctx_desc = (final_description_with_alt_texts or description_with_alt or translated_description or product_description)
            ctx_main = main_query
            ctx_add = additional_queries
            ctx_info_parts: List[str] = []
            if main_query:
                ctx_info_parts.append(f"Peamine päring: {main_query}")
            if additional_queries:
                ctx_info_parts.append("Lisa päringud: " + ", ".join(additional_queries))
            if qa_pairs:
                for idx, qa_item in enumerate(qa_pairs, start=1):
                    try:
                        q = str((qa_item or {}).get("question") or "").strip()
                        a = str((qa_item or {}).get("answer") or "").strip()
                    except Exception:
                        q = ""
                        a = ""
                    if q or a:
                        ctx_info_parts.append(f"Q{idx}: {q} | A{idx}: {a}")
            ctx_web = "\n".join([p for p in ctx_info_parts if p])

            existing_attrs = prod.get("attributes") or []
            existing_summary: List[Dict[str, Any]] = []
            for a in existing_attrs:
                try:
                    nm = str((a or {}).get("name") or "").strip()
                    if not nm:
                        continue
                    options = a.get("options") if isinstance(a.get("options"), list) else None
                    value = a.get("value") if isinstance(a.get("value"), str) else None
                    vals: List[str] = []
                    if options:
                        vals = [str(o or "").strip() for o in options if str(o or "").strip()]
                    elif value:
                        vals = [str(value).strip()]
                    existing_summary.append({"name": nm, "values": vals})
                except Exception:
                    continue

            attr_enrich_resp = create_with_retry(
                _step_key="step8_attr_enrich", _sku=sku,
                model="gpt-5.1",
                reasoning={"effort": "medium"},
                previous_response_id=(attr_translate_response.id if attr_translate_response else (final_response.id if final_response else (combined_response.id if combined_response else None))),
                instructions=
                """
                    Sul on eelnevast kontekstist kogu vajaduslik tooteinfo. Kasuta seda ja allolevat loendit olemasolevatest (juba tõlgitud) atribuutidest, et täiendada filtreerimiseks sobivaid atribuute.

                    Ära leiuta – kaasaa ainult faktid, mis on juba kinnitatud.
                    Normaliseeri mõõtühikud (cm, mm, L, W, ml, kg, g, V, A) ja kirjapilt; kasuta eesti keelt ja õigekirja.

                    Väldi tarnijale/allikale viitavaid atribuute:
                    - Ära lisa atribuute, mille väärtused on URL-id, mis viitavad tarnija või allika lehtedele (nt b2b.innpro.eu, files.innpro.pl, psr-assets.innpro.pl) või nendele ressurssidele.

                    Täpsustus kaalu kohta:
                    - ÄRA lisa atribuuti "Kaal". Kui kontekstis on neto-kaal, kasuta atribuudi nime "Netokaal" ja väljenda väärtus kujul "0,75 kg" või "750 g".
                    - Vältida duplikaate: ära loo atribuute, mille nimi või väärtused juba eksisteerivad loetelus "existing_attributes" – vajadusel täienda olemasolevaid.

                    Tagasta ainult JSON skeemiga { attributes: [ { name: string, values: string[] } ] }.
                    - name: lühike filtritunnus (nt „Materjal“, „Mõõdud“, „Netokaal“, „Värvus“ jm toote põhiomadused).
                    - values: üks või mitu väärtust; ära dubleeri; hoia kompaktsed ja masinloetavad (nt „60 × 58 × 71 cm", "13 l", "hall", "12 V DC").
                    - Väldi üldsõnalisi fraase; kasuta selgeid väärtusi ja ühikuid.
                """,
                input=json.dumps({
                    "existing_attributes": existing_summary
                }, ensure_ascii=False),
                text={
                    "verbosity": "low",
                    "format": {
                        "type": "json_schema",
                        "name": "attr_enrich_schema",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "attributes": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "name": {"type": "string"},
                                            "values": {
                                                "type": "array",
                                                "items": {"type": "string"},
                                                "minItems": 1
                                            }
                                        },
                                        "required": ["name", "values"],
                                        "additionalProperties": False
                                    }
                                }
                            },
                            "required": ["attributes"],
                            "additionalProperties": False
                        },
                        "strict": True
                    }
                }
            )
            add_usage(attr_enrich_resp)
            record_usage("STEP 8: attr enrich", attr_enrich_resp)

            enrich = {"attributes": []}
            try:
                enrich = json.loads(attr_enrich_resp.output_text)
            except Exception:
                enrich = {"attributes": []}

            attrs = prod.get("attributes") or []

            def _key(s: str) -> str:
                return (s or "").strip().lower()

            existing_by_name = {}
            filtered_attrs = []
            removed_excluded = 0
            for a in attrs:
                if not isinstance(a, dict):
                    continue
                raw_name = str((a or {}).get("name") or "")
                canon_name = canonicalize_attr_name(raw_name)
                if canon_name and canon_name != raw_name:
                    a["name"] = canon_name
                if is_excluded_attr(canon_name):
                    removed_excluded += 1
                    continue
                existing_by_name[_key(canon_name)] = a
                filtered_attrs.append(a)
            attrs = filtered_attrs

            added_cnt = 0
            merged_cnt = 0
            for it in (enrich.get("attributes") or []):
                try:
                    nm = str((it or {}).get("name") or "").strip()
                    nmc = canonicalize_attr_name(nm)
                    vals_raw = [str(v or "").strip() for v in ((it or {}).get("values") or []) if str(v or "").strip()]
                    vals = [normalize_attr_value(nmc, v) for v in vals_raw]
                    if not nm or not vals:
                        continue
                    key = _key(nmc)
                    if is_excluded_attr(nmc):
                        continue
                    if key in existing_by_name and isinstance(existing_by_name[key], dict):
                        a = existing_by_name[key]
                        opts = a.get("options") if isinstance(a.get("options"), list) else None
                        val = a.get("value") if isinstance(a.get("value"), str) else None
                        if opts is not None:
                            cur = [normalize_attr_value(nmc, str(x)) for x in opts]
                            for v in vals:
                                if v not in cur:
                                    cur.append(v)
                                    merged_cnt += 1
                            a["options"] = cur
                        elif val is not None:
                            cur = []
                            base = val.strip()
                            if base:
                                cur = [normalize_attr_value(nmc, base)]
                            for v in vals:
                                if v not in cur:
                                    cur.append(v)
                                    merged_cnt += 1
                            a.pop("value", None)
                            if cur:
                                a["options"] = cur
                        else:
                            a["options"] = vals
                            merged_cnt += len(vals)
                    else:
                        new_attr = {
                            "name": nmc,
                            "visible": True,
                            "variation": False,
                            "options": vals,
                        }
                        attrs.append(new_attr)
                        existing_by_name[key] = new_attr
                        added_cnt += 1
                except Exception:
                    continue

            prod["attributes"] = attrs
            save_debug_json(sku, "step8_attr_enrich", {
                "suggested": enrich.get("attributes") or [],
                "added": added_cnt,
                "merged_values": merged_cnt,
                "removed_excluded": removed_excluded,
                "existing_before": existing_summary
            })
        except Exception as e:
            log(f"STEP 8 atribuutide rikastamise viga: {e}")

    # --------------------------------------------------------------
    # Rakenda muudatused tooteobjektile ja salvesta ühte koond JSONi
    # --------------------------------------------------------------
    qa = qa_pairs
    prod["name"] = final_title or translated_title or product_name
    prod["description"] = clean_product_description(final_description_with_alt_texts or description_with_alt or translated_description or product_description)
    prod["short_description"] = short_description or make_short_description_et(final_description_with_alt_texts or description_with_alt or translated_description or product_description)
    if qa:
        prod["qa"] = qa
    # Save token usage (totals + per-step) for this product
    prod["token_usage"] = {
        "totals": {k: int(v) for k, v in token_usage.items() if v and isinstance(v, int)},
        "steps": token_steps,
    }
    meta = list(prod.get("meta_data") or [])
    try:
        _main_q = main_query if 'main_query' in locals() and main_query else ""
    except Exception:
        _main_q = ""
    try:
        _add_q = " | ".join(additional_queries) if 'additional_queries' in locals() and additional_queries else ""
    except Exception:
        _add_q = ""
    meta = ensure_meta(meta, "_bp_seo_title", seo_title)
    meta = ensure_meta(meta, "_bp_seo_description", seo_meta)
    meta = ensure_meta(meta, "_bp_search_main_query", _main_q)
    meta = ensure_meta(meta, "_bp_search_additional_queries", _add_q)
    prod["meta_data"] = meta

    grp = top_level_category(prod)
    with GROUP_LOCK:
        grouped.setdefault(grp, [])
        grouped[grp].append(prod)
        existing_idx[sku] = grp
        local_added += 1

    # Print for quick verification (optional)
    print(f"=== Product index: {index} ===")
    print("SKU:", sku)
    print("Original Title:", product_name)
    print("Title (ET):", prod.get("name"))
    print("SEO Title (ET):", seo_title)
    print("Description (ET):", prod.get("description")[:80] + "..." if len(prod.get("description") or "") > 80 else prod.get("description"))
    print("-" * 100)

    # Persist after each product to avoid data loss
    with GROUP_LOCK:
        _atomic_write_grouped()

    return {"added": local_added, "skipped_existing": local_skipped}

# Run sequentially or with workers
if WORKERS and WORKERS > 1:
    log(f"Paralleelne töö: {WORKERS} workerit")
    futures = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for index, prod in enumerate(products):
            futures.append(ex.submit(process_one_product, prod, index))
        for fut in as_completed(futures):
            try:
                res = fut.result() or {}
                with GROUP_LOCK:
                    added += int(res.get("added") or 0)
                    skipped_existing += int(res.get("skipped_existing") or 0)
            except Exception as e:
                log(f"Worker viga: {e}")
else:
    for index, prod in enumerate(products):
        res = process_one_product(prod, index)
        added += int(res.get("added") or 0)
        skipped_existing += int(res.get("skipped_existing") or 0)

log(f"Valmis. Kokku sisendeid: {len(products)}, lisatud uusi tõlkeid: {added}, juba olemas: {skipped_existing}")

# WooCommerce'iga kattunud EAN-id (_bp_gtin13 meta järgi), mida selles jooksus leidsime
if WOO_EAN_MATCHED_IN_WOO:
    ean_list = sorted(WOO_EAN_MATCHED_IN_WOO)
    log(f"WooCommerce'iga kattuvaid EAN-e: {len(ean_list)}")
    log("Kattuvad EAN-id: " + ", ".join(ean_list))
else:
    log("WooCommerce'iga kattuvaid EAN-e ei leitud.")
