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
USE_STEP1_WEB_SEARCH = False  # Lülita välja, kui pole vaja veebiotsinguid konteksti jaoks
USE_STEP5_FINAL_REVIEW = False  # Lülita välja, kui lõppkontrolli pole vaja
USE_STEP8_ATTR_ENRICH = False  # Lülita välja, kui olemasolevad atribuudid piisavad
GROUP_LOCK = threading.Lock()
WOO_SKU_CACHE: set[str] = set()
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
    ("Mudel", ["mudel", "model", "model code" ]),
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
}

def is_excluded_attr(name: str) -> bool:
    return (name or "").strip().lower() in EXCLUDED_ATTR_KEYS

# -----------------------------
# Query post-processing for STEP 3
# -----------------------------
def postprocess_queries(main_q: str, add_qs: List[str], ean_code: str, brand: str, product_name: str) -> Dict[str, Any]:
    def _clean(s: str) -> str:
        s = re.sub(r"\s+", " ", (s or "").strip())
        return _truncate_soft(s, 60)
    mq = _clean(main_q)
    seen = set()
    filtered: List[str] = []
    # allow max one transactional intent (osta/hind)
    transactional_seen = False
    for q in (add_qs or []):
        c = _clean(q)
        if not c:
            continue
        key = c.lower()
        if key in seen:
            continue
        is_trans = bool(re.search(r"\b(osta|hind)\b", key))
        if is_trans:
            if transactional_seen:
                continue
            transactional_seen = True
        seen.add(key)
        filtered.append(c)
        if len(filtered) >= 6:  # extra buffer before final trim
            break

    # Ensure at least one EAN query (without the word EAN)
    if ean_code and not any(ean_code in q for q in filtered):
        candidate = _clean(f"{ean_code} {brand or ''}")
        if candidate.strip() and candidate.lower() not in seen:
            filtered.append(candidate)

    # keep exactly 4
    filtered = filtered[:4]
    # if less than 4, try to pad with simple informative variants
    while len(filtered) < 4:
        head = (brand or product_name or "toode").split()
        base = " ".join(head[:3]).strip()
        pad = _clean(f"kuidas kasutada {base}")
        if pad.lower() in seen or not pad:
            pad = _clean(f"{base} võrdlus")
        if pad and pad.lower() not in seen:
            filtered.append(pad)
            seen.add(pad.lower())
        else:
            break

    return {"main_query": mq, "additional_queries": filtered[:4]}

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
    site, auth = _wc_site_and_auth()
    if not site or not auth:
        return None
    url = f"{site}/wp-json/wc/v3/products"
    page = 1
    collected: set[str] = set()
    consecutive_rate_limits = 0
    while True:
        params = {
            "per_page": 100,
            "page": page,
            "_fields": "id,sku",
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

def wc_product_exists(sku: str) -> bool:
    if not sku:
        return False
    if _ensure_woo_sku_cache():
        return sku in WOO_SKU_CACHE
    return _wc_product_exists_remote(sku)

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
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
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
    if run_prefixes:
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
        if wc_product_exists(sku):
            local_skipped += 1
            log(f"Jätan vahele (juba e-poes olemas): {sku}")
            return {"added": 0, "skipped_existing": local_skipped}
    except Exception:
        # On connectivity error, proceed with translation rather than fail the whole run
        pass

    # Extract fields for prompts
    product_name = str(prod.get("name") or "")
    product_description = str(prod.get("description") or "").strip()
    desc_len = _html_text_length(product_description)
    images = prod.get("images") or []
    attributes = prod.get("attributes") or []
        
    # --------------------------------------------------------------
    # STEP 1: Koosta kontekst ja veebiotsing
    # --------------------------------------------------------------
    # Kogume otsingu jaoks täpsed terminid meie kontekstist
    # EAN kood meta_data'st (_bp_gtin13), tootja (brand), tootja kood (Innpro code_on_card), tootenimi
    ean_code = ""
    for m in meta_data:
        try:
            if str(m.get("key")) == "_bp_gtin13" and str(m.get("value") or "").strip():
                ean_code = str(m.get("value")).strip()
                break
        except Exception:
            pass
    brand_name = ""
    try:
        # Step 2 standard: bränd tuleb prod["brands"][0]["name"] väljast
        brands = prod.get("brands") or []
        if brands:
            brand_name = str((brands[0] or {}).get("name") or "").strip()
    except Exception:
        brand_name = ""
    search_terms: List[str] = []
    if ean_code:
        search_terms.append(f"EAN:{ean_code}")
        search_terms.append(ean_code)
    if product_name:
        search_terms.append(product_name)
    search_query = " ".join(search_terms)
    use_web_search = USE_STEP1_WEB_SEARCH or (desc_len < 160)
    log(f"STEP 1: kontekst+veebiotsing ⇒ {search_query or '–'}")
    if not use_web_search:
        log("   ℹ️  Veebiotsing on keelatud; kasutatakse ainult olemasolevat tooteteavet.")
    elif not USE_STEP1_WEB_SEARCH:
        log("   ℹ️  Veebiotsing aktiveeriti, sest kirjeldused puudusid või jäid alla 160 tähemärgi.")

    try:
        if use_web_search:
            instructions_text = (
                """
                Sa tegutsed tooteanalüüsi spetsialisti ja uuriva copywriter'ina, kelle ülesanne on enne sisuloomet koguda tooteinfo, mis võimaldavad luua AI-ajastule vastava müügikirjelduse e-poodi. See on "STEP 1: Kontekst ja veebiotsing", mille tulemusi kasutavad kõik järgnevad sammud.

                Alusta koondades olemasoleva tooteinfo ja tee vajadusel sihitud veebiotsinguid (ametlikud allikad, manuaalid, usaldusväärsed partnerid), mis aitavad luua põhjaliku tooteinfo ja detailse kirjelduse ning positsioneerida toote AI-otsingutes.

                Veebiotsingut kasuta eelkõige selleks, et mõista:
                - milliseid probleeme see toode päriselt lahendab,
                - millistes olukordades ja kellele seda tüüpi toodet enim kasutatakse,
                - millised on tüüpilised kasutajate küsimused enne ostu (sobivus, paigaldus, hooldus, garantii, mis on kaasas).

                Pärast materjali läbitöötamist pead suutma vastata järgmistele „Kliendi põhiküsimustele, millele kirjeldus peab vastama“:
                1. Millist probleemi toode lahendab ja kuidas see ostja elu lihtsamaks teeb?
                2. Kas see sobib sihtkasutaja vajadustele (kellele, millistes olukordades)?
                3. Mis on toote põhiline funktsioon ja eesmärk (kuidas see töötab, millist tulemust annab)?
                4. Kuidas see erineb alternatiividest/mudelitest ning millised on unikaalsed eelised?
                6. Millised on peamised omadused ja tehnilised võtmenäitajad, mida peaks bullet'ites esile tooma?
                7. Kas see mahub kliendi ruumi/ökosüsteemi (mõõdud, maht, kontekstuaalne sobivus)?
                8. Millega see on ühilduv ja mis võib olla lisaks vajalik (platvormid, tarkvara, tarvikud, ökosüsteemid)?
                9. Kui lihtne on seda kasutada ja hooldada (kasutusloogika, hooldus-/uuendusvajadused, „plug & play“ aspektid)?
                10. Kui töökindel ja vastupidav see on ning milline on garantii või järelteenindus?
                11. Mis on tootega kaasas ja mida tuleks vajadusel eraldi soetada?
                12. Kas see on oma hinda väärt – milline on väärtuspakkumine ja pikaajaline kasu?

                Nende järelduste põhjal loo väljundid, mida kasutame otse e-poes, SEO-s ja Q&A plokis:

                - 5 otsingufraasi (1 peamine, 4 lisa), kõik loomulikus eestikeelses vormis. 
                  • main_query: fraas, mida inimene reaalselt kirjutaks otsingusse, kui ta otsib just seda konkreetset toodet (toote tüüp + 1–2 võtmeomadust + bränd/mudel). 
                  • additional_queries: 4 long-tail päringut, mis jagunevad nii:
                    1) probleemipõhine: kuidas lahendada tüüpilist probleemi selle tootega;
                    2) omaduspõhine: toote tüüp + võtmenäitaja (võimsus/mõõt/maht vms) + kasutusolukord;
                    3) võrdlev: mille poolest see erineb teistest sarnastest mudelitest;
                    4) kasutus/hooldus: kuidas paigaldada, hooldada või igapäevaselt kasutada.

                  Väljendid võivad sisaldada tehnilisi tähiseid ja mudeleid, kuid väldi ingliskeelseid täislauseid.

                - 3 Q&A küsimust ja vastust eesti keeles Q&A sektsiooni jaoks, mis aitavad kliendil ostuotsust teha. Kasuta järgmisi telgi:
                  1) sobivus ja valik (kas see sobib minu ruumi, vajadustele, olemasoleva süsteemiga);
                  2) kasutamine ja hooldus (kuidas seda kasutada, puhastada, paigaldada, hooldada);
                  3) töökindlus, garantii ja hinna-väärtuse suhe (kui vastupidav see on, milline on garantii, mis on peamine kasutegur hinna suhtes).

                Ära kopeeri teksti veebist sõna-sõnalt. Kui mõne fakti (nt konkreetne ühilduvus, garantiitingimus) kohta ei ole usaldusväärset infot, ära seda välja mõtle – jäta vastus neutraalseks või ütle, et seda infot sisendmaterjal ei täpsusta.

                Tagasta JSON skeemi alusel:
                - search_queries: objekt väljadega main_query (string) ja additional_queries (array 4 stringi)
                - qa: massiiv 3 elemendiga, igas objektis question ja answer väljad
                """
            )
            tools_spec = [{"type": "web_search_preview"}]
        else:
            instructions_text = (
                """
                Sa tegutsed tooteanalüüsi spetsialisti ja uuriva copywriter'ina. See on "STEP 1: Kontekst", mille tulemusi kasutavad kõik järgnevad sammud.

                Töötle olemasolevat toote alginfot (ilma veebiotsinguta) ja koonda sellest kontekst, mis aitab hilisemates sammudes luua AI-ajastule vastava müügikirjelduse.

                Läbi antud materjal ja veendu, et suudad vastata järgmistele küsimustele:
                1. Millist probleemi toode lahendab ja kuidas see ostja elu lihtsamaks teeb?
                2. Kas see sobib sihtkasutaja vajadustele (kellele, millistes olukordades)?
                3. Mis on toote põhiline funktsioon ja eesmärk (kuidas see töötab, millist tulemust annab)?
                4. Kuidas see erineb alternatiividest/mudelitest ning millised on unikaalsed eelised?
                6. Millised on peamised omadused ja tehnilised võtmenäitajad, mida peaks bullet'ites esile tooma?
                7. Kas see mahub kliendi ruumi/ökosüsteemi (mõõdud, maht, kontekstuaalne sobivus)?
                8. Millega see on ühilduv ja mis võib olla lisaks vajalik (platvormid, tarkvara, tarvikud, ökosüsteemid)?
                9. Kui lihtne on seda kasutada ja hooldada (kasutusloogika, hooldus-/uuendusvajadused, „plug & play“ aspektid)?
                10. Kui töökindel ja vastupidav see on ning milline on garantii või järelteenindus?
                11. Mis on tootega kaasas ja mida tuleks vajadusel eraldi soetada?
                12. Kas see on oma hinda väärt – milline on väärtuspakkumine ja pikaajaline kasu?

                Nende järelduste põhjal loo väljundid, mida kasutame otse e-poes, SEO-s ja Q&A plokis:

                - 5 otsingufraasi (1 peamine, 4 lisa), kõik loomulikus eestikeelses vormis ja tuginedes ainult olemasolevale infole.
                  • main_query: fraas, mida inimene reaalselt kirjutaks otsingusse, kui ta otsib just seda konkreetset toodet (toote tüüp + 1–2 võtmeomadust + bränd/mudel).
                  • additional_queries: 4 long-tail päringut, mis jagunevad nii:
                    1) probleemipõhine: kuidas lahendada tüüpilist probleemi selle tootega;
                    2) omaduspõhine: toote tüüp + võtmenäitaja (võimsus/mõõt/maht vms) + kasutusolukord;
                    3) võrdlev: mille poolest see erineb teistest sarnastest mudelitest;
                    4) kasutus/hooldus: kuidas paigaldada, hooldada või igapäevaselt kasutada.

                  Väljendid võivad sisaldada tehnilisi tähiseid ja mudeleid, kuid väldi ingliskeelseid täislauseid.

                - 3 Q&A küsimust ja vastust eesti keeles Q&A sektsiooni jaoks, mis aitavad kliendil ostuotsust teha. Kasuta järgmisi telgi:
                  1) sobivus ja valik (kas see sobib minu ruumi, vajadustele, olemasoleva süsteemiga);
                  2) kasutamine ja hooldus (kuidas seda kasutada, puhastada, paigaldada, hooldada);
                  3) töökindlus, garantii ja hinna-väärtuse suhe (kui vastupidav see on, milline on garantii, mis on peamine kasutegur hinna suhtes).

                Ära leiuta uusi fakte. Kui mõne info (nt konkreetne ühilduvus, garantiitingimus, täpne kasutusviis) kohta sisendmaterjal midagi ei ütle, ära seda välja mõtle – jäta vastus neutraalseks või ütle, et sisend seda ei täpsusta.

                Tagasta (kõik väärtused eesti keeles):
                - search_queries: objekt väljadega main_query (string) ja additional_queries (array 4 stringi)
                - qa: massiiv 3 elemendiga, igas objektis question ja answer väljad
                """
            )
            tools_spec = []
        context_response = create_with_retry(
            _step_key="step1_context",
            _sku=sku,
            model="gpt-5.1",
            reasoning={"effort": "medium"},
            instructions=instructions_text,
            input=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": (
                                "Toote alginfo (JSON): "
                                + json.dumps(prod, ensure_ascii=False)
                            ),
                        }
                    ],
                }
            ],
            tools=tools_spec,
            text={
                "verbosity": "medium",
                "format": {
                    "type": "json_schema",
                    "name": "context_and_research_schema",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "search_queries": {
                                "type": "object",
                                "properties": {
                                    "main_query": {"type": "string"},
                                    "additional_queries": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                        "minItems": 4,
                                        "maxItems": 4
                                    }
                                },
                                "required": ["main_query", "additional_queries"],
                                "additionalProperties": False
                            },
                            "qa": {
                                "type": "array",
                                "minItems": 3,
                                "maxItems": 3,
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "question": {"type": "string"},
                                        "answer": {"type": "string"}
                                    },
                                    "required": ["question", "answer"],
                                    "additionalProperties": False
                                }
                            },
                        },
                        "required": ["search_queries", "qa"],
                        "additionalProperties": False
                    },
                    "strict": True
                }
            }
        )
    except Exception:
        context_response = None
    else:
        add_usage(context_response)
        record_usage("STEP 1: kontekst", context_response)

    context_data: Dict[str, Any] = {}
    if context_response and getattr(context_response, "output_text", None):
        try:
            context_data = json.loads(context_response.output_text)
        except (json.JSONDecodeError, TypeError):
            context_data = {}

    search_block = context_data.get("search_queries") if isinstance(context_data.get("search_queries"), dict) else {}
    main_query = str(search_block.get("main_query", "")).strip()
    additional_queries = [str(q).strip() for q in (search_block.get("additional_queries") or []) if str(q).strip()]
    qa_pairs = context_data.get("qa") if isinstance(context_data.get("qa"), list) else []

    log(f"STEP 1: peamine päring='{main_query}' lisa={len(additional_queries)} Q&A={len(qa_pairs)}")
    save_debug_json(sku, "step1_context", {
        "search_query": search_query,
        "search_queries": {
            "main_query": main_query,
            "additional_queries": additional_queries
        },
        "qa": qa_pairs
    })

    # Säilita STEP 1 tulemused kohe tooteobjektil ja meta-andmetes,
    # et järgmised sammud ja üleslaadija saaksid neid kasutada.
    if qa_pairs:
        prod["qa"] = qa_pairs
    meta_step1 = list(meta_data)
    meta_step1 = ensure_meta(meta_step1, "_bp_search_main_query", main_query)
    meta_step1 = ensure_meta(
        meta_step1,
        "_bp_search_additional_queries",
        " | ".join(additional_queries) if additional_queries else ""
    )
    if qa_pairs:
        meta_step1 = ensure_meta(meta_step1, "_bp_qa_json", json.dumps(qa_pairs, ensure_ascii=False))
    prod["meta_data"] = meta_step1

    # --------------------------------------------------------------
    # STEP 2: Genereeri tootenimi
    # --------------------------------------------------------------
    log(f"STEP 2: tootenimi + lühikirjeldus (SKU {sku})")
    title_response = create_with_retry(
        _step_key="step2_title", _sku=sku,
        model="gpt-5.1",
        reasoning={"effort": "medium"},
        previous_response_id=(context_response.id if context_response else None),
        instructions=
        f"""
            Eesmärk:
            - Loo e-poe jaoks tootenimi ja toote lühikirjeldus.

            Tootenime koostamise reeglid:
            - Alusta tootenimetusega, mis on koos 1–3 võtmeomadusega. Esimesed sõnad peavad koheselt iseloomustama, mis tootega on tegemist ja mis on toote eesmärk/kasutuskoht.
            - Järgmiseks lisa TOOTJA nimi ja TÄPNE MUDEL.
            - Lisa detailseid tooteomadusi, mis pole veel nimetatud ja mis on vajalikult konkreetse toote eristamiseks (mõõt/maht/võimsus, materjal/värv, ühilduvus).
            - Tooteomadusi lisades püüa mõelda toote iseloomule, et kasutada kõige relevantsemat infot, mis on antud toote puhul tähtis ja vajalik teada.
            - Vormistus: max 200 tähemärki. Ühikud: 60 cm, 20 L, 250 ml, 65 W.
            - Väldi turundusklišeesid, jutumärke, liigset brändikordust, semikoolonit ja punkti lõpus.

            Head näited:
            - "Täissuuruses mänguklaviatuur RGB-valgustusega GENESIS Rhod 300 RGB NKG-1528, 104-klahviline, QWERTY, membraanlülitid, pritsmekindel, USB, must"
            - "MagSafe juhtmevabad kõrvaklapid Apple AirPods (3. põlvkond) MME73TY/A, IPX4, Bluetooth 5.0, kuni 30 tundi kestvust, valged, laadimiskarbiga"
            - "Kaasaskantav peokõlar LED-valgustuse ja Bluetooth 6.0-ga Kenwood AS-60BTB 5W must, 900 mAh aku, veekindel, USB-C laadimine, microSD tugi"
            - "Ventilaatoriga sülearvuti jahutusalus Trust GXT1126 Aura RGB 17″, 20 cm, reguleeritav kõrgus, RGB valgustus, 10 kg kandevõime, 421x312x26 mm"

            Halvad näited:
            - "Parim nõudepesumasin ülisoodne super kvaliteetne!!!"
            - "EcoPlus kassiliiv" (brändi täpsus/maht puudu)
            - "Hamstri puur" (liiga üldine; mõõdud/eripärad puudu)
            - "Telefonikaitse iPhone" (mudel ja materjal puudu)

            Toote lühikirjelduse koostamise reeglid:
            - Kirjuta tootele lühikirjeldus eesti keeles, tuues esile toote olulisemad kasutegurid ja omadused.
            - Pikkus: 2–3 lauset (kokku umbes 250–300 tähemärki).
            - Hoia toon informatiivne ja neutraalne – väldi sisutühje hüüdlauseid või ülepaisutatud kiidusõnu.
            - Lühikirjeldus peaks andma kliendile kiire ja täpse ülevaate tootest: kus, kellele ja miks toodet kasutatakse, mis muret see lahendab ja mis on kliendi peamine kasu.
            - Võid kasutada STEP 1 Q&A põhipunkte (sobivus, kasutamine, hooldus) selleks, et lühikirjeldus vastaks tüüpilistele kliendiküsimustele, kuid ära korda Q&A küsimusi sõna-sõnalt.
            - Väldi klišeesid nagu "nagu pildil näha", "pildilt on nähtav" jne.
            - Ära kasuta kirjelduses semikoolonit ";". Lõpeta mõte punktiga ja alusta uue lausega.

            🗣️ Terminoloogia eelistused (lemmikloomade veeseadmed):
            - Väldi sõnu "joogipurskkaev", "joogifontein" ja "fontään".
            - Kasuta "automaatne veedosaator", "automaatne joogikauss" või "joogivett filtreeriv jooginõu" – vali konteksti järgi loomulik.

            🗣️ Terminoloogia eelistused (integreeritavad köögiseadmed):
            - Ära kasuta sõna "nišš". Kasuta "köögimööbli avasse", "mööblisse" või konkreetset mõõtu (nt "60 cm kapp"), et kirjeldada paigalduskohta.
            - Asenda väljend "uksele-uksele hinged" variandiga "uks-uksele paigaldus".

            Oluline: kasuta ainult seda infot, mis tuleneb algsetest tooteandmetest ja STEP 1/veebiotsingu tulemustest. Ära lisa tootenimesse ega lühikirjeldusse omadusi, mida sisendis ega usaldusväärsetes allikates ei olnud.

            SEO jaoks:
            - Loo olemasoleva info põhjal ka "SEO Title" ja "SEO Meta kirjeldus".
            - SEO Title: maksimaalselt 60 tähemärki (eesmärgiga 50–60), peab loomulikult sisaldama peamist otsingufraasi (nt bränd + mudel + võtmeomadus), olema selge ja täpne.
            - SEO Meta kirjeldus: maksimaalselt 160 tähemärki, kutsuv ja informatiivne, mitte liialt reklaamilik, kirjeldab lühidalt toote põhikasu ja omadusi. 
            - Ära kasuta SEO väljundites tarnija nime ega diskreetset infot.
            - Ära kasuta semikoolonit ";" üheski väljundis (ei pealkirjades ega kirjeldustes).

            Väljund: Tagasta JSON, kus "translated_title" sisaldab tootenimetust, "short_description" sisaldab toote lühikirjeldust, "seo_title" sisaldab SEO pealkirja ja "seo_meta" sisaldab SEO meta kirjeldust.
        """,
        input="Genereeri tõlgitud andmete ja STEP 1 konteksti põhjal tootenimi ja toote lühikirjeldus.",
        text={
            "verbosity": "low",
            "format": {
                "type": "json_schema",
                "name": "translated_title_schema",
                "schema": {
                    "type": "object",
                    "properties": {
                        "translated_title": {"type": "string"},
                        "short_description": {"type": "string"},
                        "seo_title": {"type": "string"},
                        "seo_meta": {"type": "string"}
                    },
                    "required": ["translated_title", "short_description", "seo_title", "seo_meta"],
                    "additionalProperties": False
                },
                "strict": True
            }
        }
    )
    add_usage(title_response)
    record_usage("STEP 2: tootenimi + lühikirjeldus", title_response)

    short_description = ""
    seo_title = ""
    seo_meta = ""
    try:
        desc_data = json.loads(title_response.output_text)
        translated_title = clean_double_asterisks(desc_data.get("translated_title", "").strip())
        short_description = clean_double_asterisks(desc_data.get("short_description", "").strip())
        seo_title = clean_double_asterisks(desc_data.get("seo_title", "").strip())
        seo_meta = clean_double_asterisks(desc_data.get("seo_meta", "").strip())
    except (json.JSONDecodeError, KeyError):
        translated_title = "ERROR: Could not parse translated description"
        short_description = ""
        seo_title = "ERROR: Could not parse SEO title"
        seo_meta = "ERROR: Could not parse SEO meta description"
    save_debug_json(sku, "step2_title", {
        "translated_title": translated_title,
        "short_description": short_description,
        "seo_title": seo_title,
        "seo_meta": seo_meta
    })

    # Hoia eraldi debug-fail ka SEO väljundi jaoks, et sarnaneda varasema STEP 4 käitumisega.
    save_debug_json(sku, "step4_seo_meta", {
        "seo_title": seo_title,
        "seo_meta": seo_meta
    })

    # --------------------------------------------------------------
    # STEP 3: Tõlgi tootekirjeldus
    # --------------------------------------------------------------
    log(f"STEP 3: tootekirjelduse loomine (SKU {sku})")
    description_response = create_with_retry(
        _step_key="step3_description", _sku=sku,
        model="gpt-5.1",
        reasoning={"effort": "medium"},
        previous_response_id=(title_response.id if title_response else (context_response.id if context_response else None)),
        instructions=
        """
            Sa oled e-kaubanduse professionaalne toimetaja ja eesti keele ekspert. Kirjuta eesti keeles detailne tootekirjeldus HTML-formaadis.
            Kasuta algandmeteks eelmistes etappides kogutud tootekonteksti ja sisendandmeid ning veebiotsingu järeldusi, samuti algseid tooteandmeid ja loodud tootenime ja lühikirjeldust. Ära kasuta semikooloneid üheski väljundis.

            Rakenda järgmisi tootekirjelduse parimaid praktikaid:
                
                - Hoia sõnavara ühtlane ja kasuta loomulikku eesti keelt. Väldi otsetõlget. Ühikuid kasuta standardkujul
                - Hoia toon neutraalne ja informatiivne ning väldi liigset reklaamikeelt.
                - Väldi katteta lubadusi ja ülepaisutatud väiteid.
                - Väldi sõnu nagu "kaaslane", "partner", "abiline"
                - Asenda väljend "uksele-uksele hinged" variandiga "uks-uksele paigaldus".
                - Kontrolli sõnade käänete ja vormide õigsust
                - Kasuta läbi kirjelduse kõige olulisemates märksõnades ja infos boldi (<strong>). Max 3 korda ühe <p> ja max 1 kord <li> kohta.
                - Ära lisa HTML kommentaare ega kopeeri juhenditeksti või kommentaaride sisu väljundisse.
                - Kui mõne ploki jaoks puudub usaldusväärne info, jäta see plokk (sh pealkiri) täielikult ära.
                - Ära kasuta tootekirjelduses tarnijale omaseid andmeid (tarnija nimi, URL-id, sisemised koodid/kaubandusandmed), sest see on diskreetne info.

                Struktuur ja kohustuslikkuse reeglid:
                    - Kohustuslikud plokid:
                        1. Ava plokk: <h3> pealkiri, mis seob toote kasuteguri lahendatava probleemiga (kasuta loomulikult olulisemaid otsingufraase) + järgnevalt <p>, mis kirjeldab väärtuspakkumist.
                        2. Peamised omadused: <h3>Peamised omadused</h3> ja sellele järgnev <ul> kuni 6–8 <li>-ga, mis seovad omaduse kliendi kasuga.
                    - Tingimuslikud plokid (kasuta ainult siis, kui sisendmaterjal seda võimaldab):
                        • Algse kirjelduse ja pildiplokkide info: sinu käsutuses võib olla originaalne HTML-tootekirjeldus, mis võib sisaldada <img>-plokke. Kui originaalis on <img>-elemendid, kirjuta kirjeldus ümber loomulikuks eestikeelseks tekstiks ja SÄILITA KÕIK need <img>-elemendid (sama src). IGA lõplikus HTML-is olev <img>-element PEAB omama eestikeelset alt-attribuuti, mis lühidalt ja loomulikult kirjeldab pilti selle ümbruses oleva teksti kontekstis (ka juhul, kui algne alt oli muus keeles või puudus). Sa võid muuta, millise tekstiploki juurde konkreetne pilt paigutub, kuid ära jäta ühtegi algset <img>-elementi välja ning ära lisa uusi pilte, mida originaalis ei olnud. Kui algses kirjelduses pilte ei ole, ära lisa ise uusi <img>-elemente.
                        • Q&A teemade laiendus: loo h3 + 2–3 lõiku, mis vastavad STEP 1 Q&A põhipunktidele (ühilduvus, lisavarustus, hooldus jms) ja integreeri need ülejäänud tekstiga ühtseks kirjeldamiseks.
                        • Paigaldus ja kasutus: h3 + lõik või loetelu praktiliste sammudega (kasuta algkirjelduse infot, kui see on olemas).
                        • Komplektis sisalduv: h3 + loetelu või lõik, mis kirjeldab komplekti (nt mis tarvikud ja komponendid on kaasas).
                        • CTA plokk: h3 + lõik, mis võtab peamised kasutegurid kokku ja suunab ostule ilma agressiivse müügikeeleta. CTA pealkiri peab olema tegevusele suunav (nt "Miks valida [TOOTE NIMI]?", "Kas otsid [lahendust X]?", "Millal valida [TOOTE NIMI]?"). Ära kasuta meta-pealkirju nagu "Kokkuvõte", "Järeldus", "Lõppsõna" või muid sarnaseid kokkuvõttepealkirju.

                Järgi ülalkirjeldatud järjekorda. Kui mõni tingimuslik plokk jääb ära, jätka ülejäänud plokkidega ilma tühjade pealkirjadeta.
                Väljund peab olema üks koherentne HTML-plokk. Kui algses kirjelduses olid <img>-elemendid, peavad kõik need elemendid väljundis olemas olema (sama src); kui algses kirjelduses pilte ei olnud, ära lisa uusi <img>-elemente.

                    - Ära lisa eraldi "Kiirvastused", "Kes/Milleks/Kuidas" ega muid küsimuspealkirju; Q&A sektsiooni käsitleb eraldi töövoo samm.
                    - Ära lisa kirjeldusse lõpus toote põhiandmete/spec-tabelit. Atribuudid hallatakse eraldi sammudes.
                    - Ära maini, et tekst on tõlgitud, ümber kirjutatud või loodud AI poolt. Ära kasuta väljendeid nagu "originaalkirjelduse tõlge", "allolev kirjeldus" või muid meta-kommentaare – tekst peab kõlama nagu ühtne, toimetatud eestikeelne tootekirjeldus.

            🛡️ **AUTORIÕIGUSTE JA FAKTITÄPSUSE KAITSE:**
                - KEELATUD on kopeerida teksti otse veebilehtedelt või teistest allikatest.
                - Ümber sõnasta ALATI kõik info oma sõnadega.
                - Ära kasuta identset lausestust teistest allikatest.
                - Loo originaalne sisu, baseerudes faktidel, mitte teksti kopeerimisel.
                - Kasuta ainult neid fakte, mida kinnitavad algsed tooteandmed ja STEP 1/veebiotsingu tulemused. Ära lisa tehnilisi näitajaid, ühilduvusi ega garantiitingimusi, mida sisendmaterjal ei kinnita.
        """,
        input=(
            "Algne HTML-tootekirjeldus (sh kõik pildid) on all. "
            "Kirjuta see ja varasemates sammudes kogutud konteksti põhjal ümber loomulikuks eestikeelseks tootekirjelduseks, "
            "säilitades kõik algsed <img>-elemendid (sama src) ja järgides ülaltoodud struktuuri.\n\n"
            f"ORIGINAALNE_HTML_KIRJELDUS:\n{product_description}"
        ),
        text={
            "verbosity": "medium",
            "format": {
                "type": "json_schema",
                "name": "translated_description_schema",
                "schema": {
                    "type": "object",
                    "properties": {
                        "translated_description": {"type": "string"}
                    },
                    "required": ["translated_description"],
                    "additionalProperties": False
                },
                "strict": True
            }
        }
    )
    add_usage(description_response)
    record_usage("STEP 3: tootekirjeldus", description_response)

    try:
        desc_data = json.loads(description_response.output_text)
        translated_description = clean_double_asterisks(desc_data.get("translated_description", "").strip())
    except (json.JSONDecodeError, KeyError):
        translated_description = "ERROR: Could not parse translated description"
    # Kirjelduse HTML-i töötleme hiljem clean_product_description kaudu;
    # siin ei muudeta pilte ega <img> src/alt atribuute.
    description_with_alt = translated_description
    save_debug_json(sku, "step3_description", {
        "translated_description": translated_description,
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
                description_response.id if description_response else (
                    title_response.id if title_response else (
                        context_response.id if context_response else None
                    )
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
                - Eemalda ingliskeelsed jäägid: ükski nähtav silt ega lõik ei tohi olla inglise keeles (v.a brändi nimi, mudelikood, EAN ja teised pärisnimed/koodid).
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
    # STEP 6: piltide ALT tekstid
    # --------------------------------------------------------------
    images_alt_response = None
    try:
        imgs = prod.get("images") or []
        if imgs:
            log(f"STEP 6: piltide ALT tekstid (SKU {sku})")
            total = len(imgs)
            # Prepare base strings
            title_base = final_title or translated_title or product_name or ""
            seo_title_base = seo_title or title_base
            main_query_base = main_query or ""
            additional_cycle = additional_queries or []
            description_text = short_description or final_short_description or make_short_description_et(
                final_description_with_alt_texts or description_with_alt or translated_description or product_description
            )

            def _clean_text(text: str) -> str:
                return _truncate_soft(str(text or "").strip(), 120)

            updated = 0
            extras_used: List[str] = []
            for idx, im in enumerate(imgs):
                try:
                    src = str((im or {}).get("src") or "").strip()
                except Exception:
                    src = ""
                if not src:
                    continue

                if idx == 0:
                    alt_text = _clean_text(title_base)
                elif idx == 1:
                    alt_text = _clean_text(seo_title_base)
                elif idx == 2:
                    alt_text = _clean_text(main_query_base or seo_title_base)
                else:
                    if additional_cycle:
                        cycle_idx = (idx - 3) % len(additional_cycle)
                        alt_text = _clean_text(additional_cycle[cycle_idx])
                        extras_used.append(additional_cycle[cycle_idx])
                    else:
                        alt_text = _clean_text(seo_title_base)

                title_text = alt_text
                description_clean = _clean_text(description_text)

                if alt_text:
                    im["alt"] = alt_text
                    updated += 1
                if title_text:
                    im["title"] = title_text
                if description_clean:
                    im["description"] = description_clean

            save_debug_json(sku, "step6_images_meta", {
                "image_count": total,
                "seo_title": seo_title,
                "main_query": main_query,
                "additional_queries": additional_queries,
                "updated": updated,
                "cycle_used": extras_used,
                "description_applied": description_text,
            })
    except Exception as e:
        log(f"STEP 6 alt-tekstide viga: {e}")

    attr_translate_response = None

    # --------------------------------------------------------------
    # STEP 7: Tõlgi olemasolevad atribuudid (name ja values) cache'iga
    # --------------------------------------------------------------
    try:
        attrs = prod.get("attributes") or []
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
                                description_response.id if description_response else (
                                    title_response.id if title_response else (
                                        context_response.id if context_response else None
                                    )
                                )
                            )
                        ),
                        instructions="""
                            Tõlgi järgmised atribuudinimed ja -väärtused eesti keelde.

                            Oluline:
                            - Arvesta toote iseloomu ja kogu läbitöödeldud konteksti (pealkiri, kirjeldus, SEO päringud, KKK), et tõlge vastaks toote tegelikele omadustele.
                            - Säilita mõõtühikud õigel kujul (cm, mm, L, W, V jne) ja arvväärtused; kasuta korrektselt eestikeelseid käändeid ja idiomaatilist sõnajärge.
                            - Ära lisa uusi tähendusi ega väärtusi; tõlgi ainult antud nimesid ja väärtusi.
                            - Iga translation-objekt PEAB sisaldama ka "attr_name" välja. Kui kind == "name", siis kasuta attr_name = source.

                            Tagasta täpselt skeemiga { translations: [ { kind: 'name'|'value', source: string, translated: string, attr_name?: string } ] }.
                        """,
                        input=json.dumps({
                            "items": pairs
                        }, ensure_ascii=False),
                        text={
                            "verbosity": "low",
                            "format": {
                                "type": "json_schema",
                                "name": "attr_translations_schema",
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
                                                    "translated": {"type": "string"},
                                                    "attr_name": {"type": "string"}
                                                },
                                                "required": ["kind", "source", "translated", "attr_name"],
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
                        td = json.loads(attr_translate_response.output_text)
                        translations = td.get("translations", []) if isinstance(td, dict) else []
                    except Exception:
                        translations = []
                    for t in translations:
                        kind = t.get("kind")
                        src_val = str((t.get("source") or "")).strip()
                        translated_val = str((t.get("translated") or "")).strip()
                        if not src_val or not translated_val:
                            continue
                        if kind == "name":
                            cache.setdefault(src_val, {"name_et": None, "values": {}})
                            cache[src_val]["name_et"] = translated_val
                        elif kind == "value":
                            attr_name_src = str((t.get("attr_name") or "")).strip()
                            if attr_name_src:
                                cache.setdefault(attr_name_src, {"name_et": None, "values": {}})
                                cache[attr_name_src]["values"][src_val] = translated_val
                    save_attr_cache(cache)
                    save_debug_json(sku, "step7_attr_translate", {"translated_count": len(to_translate)})
                except Exception as e:
                    log(f"STEP 7 atribuutide tõlke API viga: {e}")
                    save_debug_json(sku, "step7_attr_translate", {"translated_count": len(to_translate), "error": str(e)})

            updated_pairs = 0
            for a in attrs:
                try:
                    nm = str((a or {}).get("name") or "").strip()
                    if not nm:
                        continue
                    ce = cache.get(nm) or {}
                    name_et = ce.get("name_et")
                    if name_et:
                        a["name"] = name_et

                    # Eelistame Step 2 skeemi (values-list).
                    if isinstance(a.get("values"), list):
                        vals = a.get("values") or []
                        new_vals = []
                        for v in vals:
                            s = str(v or "").strip()
                            new_vals.append(ce.get("values", {}).get(s, s))
                        a["values"] = new_vals
                        updated_pairs += len(new_vals)
                    else:
                        # Tagurpidi ühilduvus options/value skeemiga.
                        options = a.get("options") if isinstance(a.get("options"), list) else None
                        value = a.get("value") if isinstance(a.get("value"), str) else None
                        if options:
                            new_opts = []
                            for opt in options:
                                s = str(opt or "").strip()
                                new_opts.append(ce.get("values", {}).get(s, s))
                            a["options"] = new_opts
                            updated_pairs += len(new_opts)
                        elif value:
                            s = str(value).strip()
                            a["value"] = ce.get("values", {}).get(s, s)
                            updated_pairs += 1
                except Exception:
                    continue
            prod["attributes"] = attrs
            save_debug_json(sku, "step7_attr_applied", {"updated": updated_pairs})
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
                previous_response_id=(attr_translate_response.id if attr_translate_response else (final_response.id if final_response else seo_meta_response.id)),
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
                    - values: üks või mitu väärtust; ära dubleeri; hoia kompaktsed ja masinloetavad (nt „60 × 58 × 71 cm“, "13 l", "hall", "12 V DC").
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
