import json
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).resolve().parent.parent
payload_path = ROOT / "data" / "1_samm_algandmed.json"
if not payload_path.exists():
    raise SystemExit(f"Missing payload file: {payload_path}")

with payload_path.open("r", encoding="utf-8") as fh:
    data = json.load(fh)

products = data.get("products") or []

TARGET_KEYS = {
    "rating": {"rating_summary", "review_data", "review_pages", "review_order_list"},
    "docs": {"docs"},
    "carousel": {"carousel_main"},
    "full_html": {"fullHtml", "full_html"},
}

results: Dict[str, Any] = {key: None for key in TARGET_KEYS}

for product in products:
    if (product.get("flix_description_html") or "").strip():
        continue
    root = product.get("flix_tjson")
    nodes: list[Dict[str, Any]] = []
    if isinstance(root, dict):
        nodes.append(root)
    elif isinstance(root, list):
        nodes.extend([n for n in root if isinstance(n, dict)])
    if not nodes:
        continue
    stack = nodes[:]
    while stack:
        node = stack.pop()
        if not isinstance(node, dict):
            continue
        keys = set(node.keys())
        for name, required in TARGET_KEYS.items():
            if results[name] is None and any(k in keys for k in required):
                results[name] = {
                    "product_id": product.get("product_id"),
                    "module_category": node.get("module_category"),
                    "module_id": node.get("module_id"),
                    "keys": sorted(node.keys()),
                    "module": node,
                }
        for value in node.values():
            if isinstance(value, dict):
                stack.append(value)
            elif isinstance(value, list):
                stack.extend([v for v in value if isinstance(v, dict)])
        if all(results.values()):
            break
    if all(results.values()):
        break

output_path = ROOT / "data" / "processed" / "flix_module_inspect.json"
with output_path.open("w", encoding="utf-8") as fh:
    json.dump(results, fh, ensure_ascii=False, indent=2)

print(f"Saved inspection samples to {output_path.relative_to(ROOT)}")
