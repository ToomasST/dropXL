import json
from pathlib import Path

BASE = Path(__file__).resolve().parent
INPUT_FILE = BASE / "2_samm_tooteinfo.json"
TRANSLATED_FILE = BASE / "data" / "tõlgitud" / "products_translated_grouped.json"


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing input file: {INPUT_FILE}")
    if not TRANSLATED_FILE.exists():
        raise FileNotFoundError(f"Missing translated file: {TRANSLATED_FILE}")

    with INPUT_FILE.open("r", encoding="utf-8") as f:
        products = json.load(f)

    skus = {str(p.get("sku")) for p in products if p.get("sku")}

    with TRANSLATED_FILE.open("r", encoding="utf-8") as f:
        grouped = json.load(f)

    cleaned = {}
    kept = 0
    for cat, items in grouped.items():
        if not isinstance(items, list):
            continue
        kept_items = [it for it in items if str(it.get("sku")) in skus]
        if kept_items:
            cleaned[cat] = kept_items
            kept += len(kept_items)

    with TRANSLATED_FILE.open("w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)

    print(f"Kept products: {kept}")
    print(f"Categories left: {len(cleaned)}")


if __name__ == "__main__":
    main()
