import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
payload_path = ROOT / "data" / "1_samm_algandmed.json"
if not payload_path.exists():
    raise SystemExit(f"Missing payload file: {payload_path}")

with payload_path.open("r", encoding="utf-8") as fh:
    data = json.load(fh)

products = data.get("products") or []

samples = {}

for product in products:
    if (product.get("flix_description_html") or "").strip():
        continue
    flix = product.get("flix_tjson")
    if isinstance(flix, dict):
        nodes = [flix]
    elif isinstance(flix, list):
        nodes = [n for n in flix if isinstance(n, dict)]
    else:
        continue

    stack = list(nodes)
    while stack:
        node = stack.pop()
        module_category = node.get("module_category")
        module_id = node.get("module_id") or node.get("id")
        if module_category and (module_category not in samples):
            samples[module_category] = {
                "product_id": product.get("product_id"),
                "module_id": module_id,
                "module": node,
            }
        for value in node.values():
            if isinstance(value, dict):
                stack.append(value)
            elif isinstance(value, list):
                stack.extend([v for v in value if isinstance(v, dict)])
        if len(samples) >= 10:
            break
    if len(samples) >= 10:
        break

output_path = ROOT / "data" / "processed" / "flix_module_samples.json"
with output_path.open("w", encoding="utf-8") as fh:
    json.dump(samples, fh, ensure_ascii=False, indent=2)

print(f"Saved {len(samples)} samples to {output_path.relative_to(ROOT)}")
