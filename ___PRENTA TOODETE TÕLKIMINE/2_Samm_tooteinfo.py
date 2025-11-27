#!/usr/bin/env python3
"""2. samm: tooteinfo ettevalmistamine Woo/AI töövoo jaoks.

Loeb Samm 1 raw-väljundi (1_samm_raw_data.json) ja mapib iga toote
standardsele Step 2 skeemile (vt tools/tooteinfo-näidis.json).

Reeglid:
- Flix galerii pildid lisatakse images massiivi LÕPPU
  (originaal toote pildid ehk local_images jäävad ette).
- Flix feature_blocks HTML põimitakse description välja lõppu.
- Atribuudid lihtsustatakse kujule {"name": ..., "values": [...]},
  kus values on alati massiiv (üks või mitu väärtust).
- source plokis hoitakse Prenta päritoluinfot ja tooreid hindu.
- Q&A ja SEO meta väljad täidetakse hilisemates sammudes.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parent
RAW_INPUT_PATH = ROOT / "1_samm_raw_data.json"
OUTPUT_PATH = ROOT / "2_samm_tooteinfo.json"


def _load_raw_products() -> List[Dict[str, Any]]:
    if not RAW_INPUT_PATH.exists():
        raise FileNotFoundError(f"Sisendfaili ei leitud: {RAW_INPUT_PATH}")
    with RAW_INPUT_PATH.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        raise ValueError("1_samm_raw_data.json peab olema JSON massiiv.")
    return [item for item in data if isinstance(item, dict)]


def _save_products(products: List[Dict[str, Any]]) -> None:
    with OUTPUT_PATH.open("w", encoding="utf-8") as fh:
        json.dump(products, fh, ensure_ascii=False, indent=2)


def _as_str(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, bool):
        return "True" if val else "False"
    return str(val)


def _pick_purchase_price(raw: Dict[str, Any]) -> Any:
    prices = raw.get("prices") or []
    if not isinstance(prices, list) or not prices:
        return None
    candidates: List[float] = []
    for item in prices:
        if not isinstance(item, dict):
            continue
        v = item.get("price")
        if v is None:
            continue
        try:
            candidates.append(float(v))
        except Exception:
            continue
    if not candidates:
        return None
    return min(candidates)


def _sum_stock_qty(raw: Dict[str, Any]) -> int:
    total = 0
    for s in raw.get("stock_levels") or []:
        if not isinstance(s, dict):
            continue
        qty = s.get("qty")
        if qty is None:
            continue
        try:
            total += max(0, int(qty))
        except Exception:
            continue
    return total


def _build_category(raw: Dict[str, Any]) -> Dict[str, Any]:
    cid = raw.get("category_id")
    path = _as_str(raw.get("category_path") or "").strip()
    translated = _as_str(raw.get("category_path_translated") or "").strip()
    leaf = ""
    src = translated or path
    if src:
        parts = [p.strip() for p in src.split(">") if p.strip()]
        if parts:
            leaf = parts[-1]
    return {
        "category": {
            "source_id": cid,
            "path": path,
            "translated_path": translated,
            "leaf_name": leaf,
        },
        "categories": ([{"name": leaf}] if leaf else []),
    }


def _build_brands(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    name = _as_str(raw.get("manufacturer_name") or "").strip()
    if not name:
        return []
    return [{"name": name}]


def _build_images(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    images: List[Dict[str, Any]] = []
    alt_base = (
        _as_str(raw.get("compound_name") or "").strip()
        or _as_str(raw.get("name") or "").strip()
    )
    # Originaal toote pildid (local_images) esimesena
    for src in raw.get("local_images") or []:
        if not isinstance(src, str) or not src.strip():
            continue
        images.append({"src": src, "alt": alt_base})
    # Flix galerii pildid (gallery_images) LÕPPU
    for src in raw.get("gallery_images") or []:
        if not isinstance(src, str) or not src.strip():
            continue
        images.append({"src": src, "alt": alt_base})
    return images


def _extract_attr_value(raw: Dict[str, Any], attr_name: str) -> Any:
    """Leia esimene attribute_line_ids väärtus nime järgi."""
    lines = raw.get("attribute_line_ids") or []
    for line in lines:
        if not isinstance(line, dict):
            continue
        name = _as_str(line.get("attribute_name") or "").strip()
        if name == attr_name:
            return line.get("attribute_value")
    return None


def _build_dimensions(raw: Dict[str, Any]) -> Dict[str, str]:
    """Ehita Woo dimensions kasutades Height/Width/Depth atribuute (cm)."""

    def _to_dim_str(v: Any) -> str:
        if v is None:
            return ""
        s = _as_str(v).strip()
        if s.endswith(".0"):
            s = s[:-2]
        return s

    height = _extract_attr_value(raw, "Height")
    width = _extract_attr_value(raw, "Width")
    depth = _extract_attr_value(raw, "Depth")

    return {
        "length": _to_dim_str(depth),
        "width": _to_dim_str(width),
        "height": _to_dim_str(height),
    }


def _convert_value_unit(value: Any, unit: str) -> tuple[Any, str]:
    unit_norm = _as_str(unit).strip()
    if not unit_norm or value is None:
        return value, unit
    try:
        v = float(value)
    except Exception:
        return value, unit
    key = unit_norm.lower()
    if key == "lb(s)":
        v2 = round(v * 0.45359237, 2)
        return v2, "kg"
    if key == "oz(s)":
        v2 = round(v * 28.349523125, 2)
        return v2, "g"
    if key == "fl oz":
        v2 = round(v * 29.5735295625, 2)
        return v2, "ml"
    if key == "foot(ft)":
        v2 = round(v * 30.48, 2)
        return v2, "cm"
    if key == "inch(es)":
        v2 = round(v * 2.54, 2)
        return v2, "cm"
    if key == "gal(s)":
        v2 = round(v * 3.785411784, 2)
        return v2, "l"
    if key == "qt":
        v2 = round(v * 0.946352946, 2)
        return v2, "l"
    if key == "mile(s)":
        v2 = round(v * 1.609344, 2)
        return v2, "km"
    return value, unit


def _build_attributes(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    # values võivad olla stringid või numbrid (nt Standardized width/height puhul int)
    attr_map: Dict[str, List[Any]] = {}
    lines = raw.get("attribute_line_ids") or []
    if not isinstance(lines, list):
        return []
    for line in lines:
        if not isinstance(line, dict):
            continue
        base_name = _as_str(line.get("attribute_name") or "").strip()
        if not base_name:
            continue
        if base_name in {"Height", "Width", "Depth"}:
            continue
        unit_raw = _as_str(line.get("attribute_unit") or "").strip()
        value = line.get("attribute_value")
        if value is None:
            continue
        value, unit = _convert_value_unit(value, unit_raw)
        name = f"{base_name} ({unit})" if unit else base_name

        # Standardized width / height -> proovime teha int väärtuseks (kontrollime base_name'i)
        if base_name in {"Standardized width", "Standardized height"}:
            v_clean: Any
            try:
                if isinstance(value, (int, float)):
                    v_clean = int(value)
                else:
                    v_clean = int(str(value).strip())
            except Exception:
                # kui ei õnnestu, kukume tagasi stringi peale
                v_clean = _as_str(value).strip()
        else:
            if isinstance(value, bool):
                v_clean = "Yes" if value else "No"
            else:
                v_clean = _as_str(value).strip()

        if v_clean == "":
            continue
        bucket = attr_map.setdefault(name, [])
        if v_clean not in bucket:
            bucket.append(v_clean)
    # Lisa product_line eraldi atribuudina, kui olemas
    pl = _as_str(raw.get("product_line") or "").strip()
    if pl:
        bucket = attr_map.setdefault("Product line", [])
        if pl not in bucket:
            bucket.append(pl)

    # Koosta attributes list nii, et "Product line" tuleb esimesena
    attrs: List[Dict[str, Any]] = []
    pl_values = attr_map.pop("Product line", None)
    if pl_values:
        attrs.append({"name": "Product line", "values": pl_values})

    # Ülejäänud atribuudid tulevad täpselt Prenta attribute_name järgi, ilma UoM lisandit leiutamata
    for name, values in attr_map.items():
        if values:
            attrs.append({"name": name, "values": values})

    return attrs


def _build_description(raw: Dict[str, Any]) -> str:
    base = _as_str(raw.get("description") or "").strip()
    feature_html = _as_str(raw.get("feature_blocks") or "").strip()
    if feature_html:
        if base:
            return base.rstrip() + "\n\n" + feature_html
        return feature_html
    return base


def _build_meta(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    meta: List[Dict[str, Any]] = []
    barcode = _as_str(raw.get("barcode") or "").strip()
    brand = _as_str(raw.get("manufacturer_name") or "").strip()
    if barcode:
        meta.append({"key": "_bp_gtin13", "value": barcode})
    # Supplier on alati Prenta
    meta.append({"key": "_bp_supplier", "value": "Prenta"})
    if brand:
        meta.append({"key": "_bp_brand", "value": brand})
    # Ülejäänud _bp_* väljad täidetakse hiljem (SEO, Q&A jne)
    return meta


def build_product(raw: Dict[str, Any]) -> Dict[str, Any]:
    pid = raw.get("id")
    barcode = _as_str(raw.get("barcode") or "").strip()
    # SKU peab esmajärjekorras olema internal_reference
    sku = (
        _as_str(raw.get("internal_reference") or "").strip()
        or barcode
        or _as_str(pid or "")
    )
    # global_unique_id = ainult barcode (EAN/GTIN); kui seda pole, jätame tühjaks
    global_id = barcode

    purchase_price = _pick_purchase_price(raw)
    rrp_price = raw.get("price_rrp")

    stock_qty = _sum_stock_qty(raw)
    stock_status = "instock" if stock_qty > 0 else "outofstock"

    cat_info = _build_category(raw)
    brands = _build_brands(raw)
    images = _build_images(raw)
    attributes = _build_attributes(raw)
    description = _build_description(raw)
    meta_data = _build_meta(raw)

    name_val = _as_str(raw.get("compound_name") or "").strip() or _as_str(raw.get("name") or "").strip()

    category = cat_info["category"]
    categories = cat_info["categories"]

    source_category_ids: List[Any] = []
    if raw.get("category_id") is not None:
        source_category_ids.append(raw.get("category_id"))

    product: Dict[str, Any] = {
        "sku": sku,
        "global_unique_id": global_id,
        "source": {
            "source_system": "prenta",
            "source_product_id": pid,
            "source_category_ids": source_category_ids,
            "rrp_price": rrp_price,
            "purchase_price": purchase_price,
        },
        "name": name_val,
        "short_description": "",
        "description": description,
        "type": "simple",
        "status": "publish",
        "featured": False,
        "catalog_visibility": "visible",
        "regular_price": _as_str(rrp_price) if rrp_price is not None else "",
        "sale_price": "",
        "purchasable": True,
        "virtual": False,
        "tax_status": "taxable",
        "tax_class": "",
        "manage_stock": True,
        "stock_status": stock_status,
        "stock_quantity": stock_qty,
        "backorders": "no",
        "backorders_allowed": False,
        "backordered": False,
        "low_stock_amount": None,
        "sold_individually": False,
        "weight": _as_str(raw.get("weight") or ""),
        "dimensions": _build_dimensions(raw),
        "shipping_required": True,
        "shipping_taxable": True,
        "shipping_class": "prenta",
        "reviews_allowed": True,
        "average_rating": "0.00",
        "rating_count": 0,
        "upsell_ids": [],
        "cross_sell_ids": [],
        "category": category,
        "categories": categories,
        "brands": brands,
        "tags": [],
        "images": images,
        "attributes": attributes,
        "default_attributes": [],
        "variations": [],
        "grouped_products": [],
        "menu_order": 0,
        "related_ids": [],
        "meta_data": meta_data,
    }

    return product


def main() -> int:
    raw_products = _load_raw_products()
    out_products: List[Dict[str, Any]] = []
    for raw in raw_products:
        try:
            prod = build_product(raw)
        except Exception as exc:
            # Jätame vigase toote vahele, kuid ei katkesta kogu protsessi
            print(f"⚠️  Viga toote {raw.get('id')} töötlemisel: {exc}")
            continue
        out_products.append(prod)

    _save_products(out_products)
    summary = {
        "input_file": str(RAW_INPUT_PATH),
        "output_file": str(OUTPUT_PATH),
        "counts": {
            "products_in": len(raw_products),
            "products_out": len(out_products),
        },
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
