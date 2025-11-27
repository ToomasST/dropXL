import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Set

ROOT = Path(__file__).parent
GROUPED_DEFAULT = ROOT / "data" / "processed" / "products_grouped.json"
PRODUCTS_DIR_DEFAULT = ROOT / "data" / "processed" / "products"
RUNLIST_PATH = ROOT / "category_runlist.json"
TRANSLATION_PATH = ROOT / "category_translation.json"
STATUS_PATH = ROOT / "data" / "processed" / "CATEGORY_TRANSLATION_STATUS.json"
SUMMARY_PATH = ROOT / "data" / "processed" / "products_selection_summary.json"
def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def resolve_path(base: Path, value: str, default: Path) -> Path:
    if not value:
        return default
    path = Path(value)
    if not path.is_absolute():
        path = base / path
    return path


def relative_to_or_self(path: Path, base: Path) -> str:
    try:
        return str(path.relative_to(base))
    except ValueError:
        return str(path)


def load_grouped_products(path: Path) -> Dict[str, List[Dict[str, Any]]]:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError("products_grouped.json peab olema objekt kujul {kategooria: [..tooted..]}")
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for key, value in payload.items():
        if isinstance(value, list):
            grouped[key] = [item for item in value if isinstance(item, dict)]
    return grouped


def load_runlist(path: Path) -> List[str]:
    if not path.exists():
        return []
    try:
        data = json.load(path.open("r", encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    runlist: List[str] = []
    for raw in data:
        if not isinstance(raw, str):
            continue
        val = raw.strip()
        if not val:
            continue
        val = val.rstrip("/")
        val = val.replace(" > ", "/").replace(">", "/")
        if not val.endswith("/"):
            val += "/"
        while "//" in val:
            val = val.replace("//", "/")
        runlist.append(val)
    return runlist


def load_translations(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    try:
        data = json.load(path.open("r", encoding="utf-8"))
    except Exception:
        return {}
    return {str(k): str(v) for k, v in data.items() if isinstance(k, str)}


def category_path_to_prefix(path: str) -> str:
    normalized = path.replace(" > ", "/").strip()
    if normalized and not normalized.endswith("/"):
        normalized += "/"
    return normalized


def category_matches_runlist(source_path: str, runlist: Sequence[str]) -> bool:
    if not runlist:
        return True
    if not source_path:
        return False
    normalized = category_path_to_prefix(source_path)
    return any(normalized.startswith(prefix) for prefix in runlist)


def parse_only_sku(raw: str) -> Set[str]:
    if not raw:
        return set()
    return {part.strip() for part in raw.split(",") if part.strip()}


def category_matches_prefix(group_name: str, product: Dict[str, Any], prefix: str) -> bool:
    if not prefix:
        return True
    if group_name.startswith(prefix):
        return True
    for cat in product.get("categories") or []:
        name = str((cat or {}).get("name") or "").strip()
        if name.startswith(prefix):
            return True
    return False


def product_has_mapped_category(product: Dict[str, Any]) -> bool:
    cats = product.get("categories") or []
    for cat in cats:
        name = str((cat or {}).get("name") or "").strip()
        if name:
            return True
    return False


def stock_quantity(product: Dict[str, Any]) -> int:
    try:
        return int(float(str(product.get("stock_quantity") or 0)))
    except Exception:
        return 0


def safe_sku(product: Dict[str, Any]) -> str:
    sku = str(product.get("sku") or "").strip()
    if sku:
        return sku
    name = str(product.get("name") or "toode").strip() or "toode"
    slug = "".join(ch if ch.isalnum() else "-" for ch in name.lower()).strip("-")
    return slug or "toode"


def write_product_file(out_dir: Path, product: Dict[str, Any]) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    base_name = safe_sku(product)
    target = out_dir / f"{base_name}.json"
    if target.exists():
        idx = 2
        while True:
            alt = out_dir / f"{base_name}_{idx}.json"
            if not alt.exists():
                target = alt
                break
            idx += 1
    with target.open("w", encoding="utf-8") as fh:
        json.dump(product, fh, ensure_ascii=False, indent=2)
    return target


def update_translation_file(path: Path, translations: Dict[str, str], missing: Iterable[str]) -> None:
    changed = False
    for cat in missing:
        if cat and cat not in translations:
            translations[cat] = ""
            changed = True
    if not changed:
        return
    ordered = dict(sorted(translations.items(), key=lambda kv: kv[0].lower()))
    with path.open("w", encoding="utf-8") as fh:
        json.dump(ordered, fh, ensure_ascii=False, indent=2)


def write_translation_status(path: Path, translations: Dict[str, str], coverage: Set[str]) -> None:
    uniq = sorted({c for c in coverage if c})
    translated = sorted([c for c in uniq if translations.get(c)])
    untranslated = sorted([c for c in uniq if not translations.get(c)])
    status = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "total_unique_source_categories": len(uniq),
        "translated_count": len(translated),
        "untranslated_count": len(untranslated),
        "untranslated_list": untranslated,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")


def write_summary(path: Path, grouped_path: Path, out_dir: Path, counts: Dict[str, int], filters: Dict[str, Any], missing: Set[str], runlist: Sequence[str]) -> None:
    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "input": relative_to_or_self(grouped_path, ROOT),
        "output_dir": relative_to_or_self(out_dir, ROOT),
        "counts": counts,
        "filters": filters,
        "runlist_prefixes": list(runlist),
        "missing_translations": sorted(missing),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Vali ja laienda Prenta koondtabel WooCommerce toote failideks")
    parser.add_argument("--input", default="", help="Sisend fail (products_grouped.json). Vaikimisi data/processed/products_grouped.json")
    parser.add_argument("--output-dir", default="", help="Väljundi kaust üksikute toodete jaoks. Vaikimisi data/processed/products/")
    parser.add_argument("--only-target-prefix", default="", help="Filtreeri ainult kategooriad, mis algavad selle tõlkega (nt 'Lemmikloomatarbed')")
    parser.add_argument("--only-mapped", action="store_true", help="Ekspordi ainult tooted, mille tõlgitud kategooria on olemas (mitte tühi)")
    parser.add_argument("--skip-translation-check", action="store_true", help="Ära katkesta kui on tõlkimata kategooriaid")
    parser.add_argument("--only-sku", default="", help="Komaeraldatud SKU-de loend, mida eksportida")
    args = parser.parse_args()

    grouped_path = resolve_path(ROOT, args.input, GROUPED_DEFAULT)
    out_dir = resolve_path(ROOT, args.output_dir, PRODUCTS_DIR_DEFAULT)

    if not grouped_path.exists():
        log(f"❌ Sisend puudub: {grouped_path}")
        return 1

    try:
        grouped = load_grouped_products(grouped_path)
    except Exception as exc:
        log(f"❌ Ei suutnud lugeda koondfaili: {exc}")
        return 2

    run_prefixes = load_runlist(RUNLIST_PATH)
    translations = load_translations(TRANSLATION_PATH)
    only_sku_set = parse_only_sku(args.only_sku)

    counts = {
        "groups_total": len(grouped),
        "products_seen": 0,
        "written": 0,
        "skipped_runlist": 0,
        "skipped_target_prefix": 0,
        "skipped_unmapped": 0,
        "skipped_stock": 0,
        "skipped_sku": 0,
    }
    missing_translations: Set[str] = set()
    coverage_categories: Set[str] = set()
    filtered_grouped: Dict[str, List[Dict[str, Any]]] = {}

    log("Alustan products_grouped.json töötlemist")
    for group_name, items in grouped.items():
        for product in items:
            counts["products_seen"] += 1
            sku = safe_sku(product)
            if only_sku_set and sku not in only_sku_set:
                counts["skipped_sku"] += 1
                continue
            source_path = str((product.get("source") or {}).get("prenta_category_path") or "").strip()
            if run_prefixes and not category_matches_runlist(source_path, run_prefixes):
                counts["skipped_runlist"] += 1
                continue
            if args.only_target_prefix and not category_matches_prefix(group_name, product, args.only_target_prefix):
                counts["skipped_target_prefix"] += 1
                continue
            if args.only_mapped and not product_has_mapped_category(product):
                counts["skipped_unmapped"] += 1
                continue
            if stock_quantity(product) <= 1:
                counts["skipped_stock"] += 1
                continue

            if source_path:
                coverage_categories.add(source_path)
                if not translations.get(source_path):
                    missing_translations.add(source_path)

            write_product_file(out_dir, product)
            filtered_grouped.setdefault(group_name, []).append(product)
            counts["written"] += 1
            if counts["written"] % 100 == 0:
                log(f"… kirjutatud {counts['written']} toodet")

    if missing_translations:
        update_translation_file(TRANSLATION_PATH, translations, missing_translations)
        if not args.skip_translation_check:
            log("❌ Leidsin tõlkimata kategooriaid. Täienda category_translation.json ja käivita skript uuesti või kasuta --skip-translation-check.")
            for cat in sorted(missing_translations)[:20]:
                log(f" - {cat}")
            if len(missing_translations) > 20:
                log(f" … ja veel {len(missing_translations) - 20} kategooriat")
            return 8

    write_translation_status(STATUS_PATH, load_translations(TRANSLATION_PATH), coverage_categories)

    filtered_grouped_path: Path | None = None
    if filtered_grouped:
        filtered_grouped_path = grouped_path
        original_backup = grouped_path.with_suffix(".orig.json")
        try:
            if grouped_path.exists() and not original_backup.exists():
                original_backup.write_text(grouped_path.read_text(encoding="utf-8"), encoding="utf-8")
        except Exception:
            pass
        grouped_path.write_text(
            json.dumps(filtered_grouped, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    filters = {
        "only_target_prefix": args.only_target_prefix,
        "only_mapped": args.only_mapped,
        "only_sku": sorted(only_sku_set),
        "skip_translation_check": args.skip_translation_check,
    }
    write_summary(
        SUMMARY_PATH,
        filtered_grouped_path or grouped_path,
        out_dir,
        counts,
        filters,
        missing_translations,
        run_prefixes,
    )

    log(f"✔ Valmis. Kirjutatud {counts['written']} toodet (kokku vaadati {counts['products_seen']}).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
