import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

ROOT = Path(__file__).resolve().parent.parent
PAYLOAD_PATH = ROOT / "data" / "1_samm_algandmed.json"

if not PAYLOAD_PATH.exists():
    raise SystemExit(f"Missing payload file: {PAYLOAD_PATH}")

with PAYLOAD_PATH.open("r", encoding="utf-8") as fh:
    payload = json.load(fh)

products = payload.get("products") or []

status_counter: Counter[str] = Counter()
html_counter: Counter[str] = Counter()
no_html_products: list[Dict[str, Any]] = []

ModuleLayoutKey = Tuple[Tuple[str, ...], bool, bool]
module_category_counter: Counter[str] = Counter()
module_type_counter: Counter[str] = Counter()
category_layouts: defaultdict[str, Counter[ModuleLayoutKey]] = defaultdict(Counter)
category_module_ids: defaultdict[str, Counter[str]] = defaultdict(Counter)
listkey_counter: Counter[Tuple[str, ...]] = Counter()
example_modules: Dict[str, Dict[str, Any]] = {}

LIST_KEYS = ("multiple_main", "multiple", "items", "blocks", "rows", "slides")
BACKGROUND_KEYS = ("background_image", "background_images", "main_background_image", "main_background_image1")


def iter_modules(node: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(node, dict):
        if "module_category" in node:
            yield node
        for value in node.values():
            yield from iter_modules(value)
    elif isinstance(node, list):
        for item in node:
            yield from iter_modules(item)


def first_dict_item(seq: Any) -> Dict[str, Any]:
    if isinstance(seq, list):
        for item in seq:
            if isinstance(item, dict):
                return item
    return {}


def describe_block_keys(module: Dict[str, Any]) -> Tuple[str, ...]:
    for key in LIST_KEYS:
        blocks = module.get(key)
        if isinstance(blocks, list):
            first = first_dict_item(blocks)
            if first:
                return tuple(sorted(first.keys()))
    return ()


for product in products:
    status = product.get("flixmedia_status") or ""
    status_counter[status] += 1
    has_html = bool((product.get("flix_description_html") or "").strip())
    html_counter["with_html" if has_html else "without_html"] += 1
    if not has_html:
        no_html_products.append(product)

    tjson = product.get("flix_tjson")
    if tjson is None:
        continue

    for module in iter_modules(tjson):
        category = module.get("module_category") or "(unknown)"
        module_category_counter[category] += 1
        module_type = module.get("type") or module.get("module_type") or ""
        module_type_counter[module_type] += 1
        module_id = module.get("module_id") or module.get("id")
        if module_id:
            category_module_ids[category][str(module_id)] += 1

        list_keys = tuple(sorted(k for k, v in module.items() if isinstance(v, list)))
        listkey_counter[list_keys] += 1

        has_inline_html = bool((module.get("html") or "").strip())
        has_background = any(module.get(key) for key in BACKGROUND_KEYS)
        layout_key: ModuleLayoutKey = (list_keys, has_inline_html, has_background)
        category_layouts[category][layout_key] += 1

        if category not in example_modules:
            example_modules[category] = module

print("=== Flix overview ===")
print(f"Products total: {len(products)}")
print("Flix status counts:")
for status, count in status_counter.most_common():
    print(f"  {status or '(empty)'}: {count}")
print("description_html availability:")
for key, count in html_counter.items():
    print(f"  {key}: {count}")
print(f"Products without flix_description_html: {len(no_html_products)}")
if no_html_products:
    sample_ids = [p.get("product_id") for p in no_html_products[:20]]
    print(f"  sample product_ids: {sample_ids}")

print("\n=== Module categories ===")
for category, count in module_category_counter.most_common(30):
    layout_count = sum(category_layouts[category].values())
    unique_layouts = len(category_layouts[category])
    unique_modules = len(category_module_ids[category])
    print(f"  {category}: modules={count}, layouts={unique_layouts}, module_ids={unique_modules}")

print("\n=== Layout signatures (top 20) ===")
for layout, count in listkey_counter.most_common(20):
    print(f"  list_keys={layout or '(none)'} -> {count}")

print("\n=== Category layout breakdown (top 10 categories) ===")
for category, _ in module_category_counter.most_common(10):
    print(f"  {category}:")
    for (list_keys, has_html, has_bg), count in category_layouts[category].most_common():
        lk = list_keys or ("(none)",)
        print(f"    list_keys={lk}, inline_html={'yes' if has_html else 'no'}, background={'yes' if has_bg else 'no'} -> {count}")

print("\n=== Example modules (first 5 categories) ===")
for category in list(module_category_counter.keys())[:5]:
    module = example_modules.get(category)
    if not isinstance(module, dict):
        continue
    keys = sorted(module.keys())
    print(f"  {category}: keys={keys}")
    for key in LIST_KEYS:
        blocks = module.get(key)
        if isinstance(blocks, list):
            block_keys = describe_block_keys(module)
            if block_keys:
                print(f"    first {key} block keys={block_keys}")
            break

print("\n=== Completed ===")
