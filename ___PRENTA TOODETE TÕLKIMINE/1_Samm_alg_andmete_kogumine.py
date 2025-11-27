#!/usr/bin/env python3
"""1. samm: algandmete kogumine valitud kategooriate toodetele (uus töövoog).

Eesmärk
-------
- Lugeda kategooriate runlist ja tuvastada, milliste kategooriatega seotud tooted
  peame esimeses etapis alla laadima.
- Koguda Prenta API-st nende toodete detailandmed endpointi
  ``GET /products/{product_id}`` kaudu.
- Rikastada iga toote kirjet minimaalse lisainfoga (tootja, kategooria, laoseis,
  hinnad, atribuudid, pildid), et järgmistes sammudes oleks mugav standard
  skeemi peale ehitada.
- Salvestada kõik rikastatud tooteobjektid ühte faili ``1_samm_raw_data.json``.

Reeglid
-------
- Runlist (``category_runlist.json``) asub samas kaustas ja sisaldab kategooriaradu
  kujul ``"All > Saleable > ... > ..."``.
- Samm 1 kasutab runlisti esmase FILTRINA: otsustab, milliste kategooriate tooted
  üldse detailidena alla tõmmata.
- Teiseks filtriks on laoseis: kui antud toote kogusumma (``qty`` kõigis
  ``/stock_levels`` kirjetes) on ``<= 1``, toodet Samm 1 väljundisse ei lisata.
- Iga valitud ``product_id`` jaoks kogutakse:
  - /products/{id} detailandmed (toote põhiandmed);
  - /prices (filtreeritud product_id järgi) ja lisatakse ``prices`` väljale;
  - /stock_levels (filtreeritud product_id järgi) ja lisatakse ``stock_levels`` väljale;
  - /manufacturers listist leitakse vastav tootja ja lisatakse ``manufacturer_name``;
  - /categories abil arvutatakse ``category_path`` ja tõlkefailist
    ``category_path_translated``;
  - /products/{id}/attributes ja /attribute_values abil täiendatakse
    ``attribute_line_ids`` elemente väljadega ``attribute_name`` ja
    ``attribute_value``;
  - ``images`` URL-id alla laaditakse ja töödeldakse lokaalseks ruudukujuliseks
    WebP-pildiks (või .img fallback), mille suhteline rada lisatakse
    ``local_images`` väljale.
- Väljundfail ``1_samm_raw_data.json`` on JSON massiiv, kus iga element on üks
  rikastatud tooteobjekt (baasiks /products/{id} vastus + ülaltoodud lisaväljad).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from base64 import b64encode
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set
from urllib.parse import urlparse

import requests
from dotenv import find_dotenv, load_dotenv

try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None

try:  # PIL on valikuline, kasutame kui olemas
    from PIL import Image  # type: ignore
except Exception:  # pragma: no cover - valikuline sõltuvus
    Image = None  # type: ignore


ROOT = Path(__file__).resolve().parent
RUNLIST_PATH = ROOT / "category_runlist.json"
RAW_OUTPUT_PATH = ROOT / "1_samm_raw_data.json"
TRANSLATION_PATH = ROOT / "category_translation.json"
PRODUCT_IMAGES_DIR = ROOT / "product_images"
DEFAULT_IMAGE_TIMEOUT = 30
SKIP_EXISTING_RAW = True  # Kui True, taaskasutab olemasolevat 1_samm_raw_data.json faili ja laeb detailid ainult uutele toodetele


def log(msg: str) -> None:
    print(msg)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def load_runlist() -> List[str]:
    """Lae runlist sama kausta ``category_runlist.json`` failist.

    Runlist on lihtne massiiv stringidest, nt::

        [
          "All > Saleable > Home appliances > Built in > Fridge",
          "All > Saleable > Home appliances > Built in > Dishwasher"
        ]
    """

    if not RUNLIST_PATH.exists():
        return []
    try:
        with RUNLIST_PATH.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, list):
            items: List[str] = []
            for entry in data:
                if isinstance(entry, str) and entry.strip():
                    items.append(entry.strip())
            return items
    except Exception:
        return []
    return []


def save_raw_data(products: List[Dict[str, Any]]) -> None:
    ensure_parent(RAW_OUTPUT_PATH)
    with RAW_OUTPUT_PATH.open("w", encoding="utf-8") as fh:
        json.dump(products, fh, ensure_ascii=False, indent=2)


def _prepare_product_images_dir(product_id: Any) -> Optional[Path]:
    if product_id is None:
        return None
    target = PRODUCT_IMAGES_DIR / str(product_id)
    target.mkdir(parents=True, exist_ok=True)
    return target


def load_category_translations() -> Dict[str, str]:
    """Lae kategooriate tõlked samas kaustas olevast category_translation.json failist.

    Tagastab alati dicti (võib olla tühi), mitte kunagi None.
    """

    if not TRANSLATION_PATH.exists():
        return {}
    try:
        with TRANSLATION_PATH.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items()}
    except Exception:
        return {}
    return {}


def _square_product_image(url: str, dest_dir: Path, filename: str, timeout: int = DEFAULT_IMAGE_TIMEOUT) -> Optional[Path]:
    if not url:
        return None
    base_path = dest_dir / f"{filename}"
    # Reuse existing processed image if present (idempotent behavior)
    existing: list[Path] = []
    for ext in (".webp", ".img"):
        candidate = base_path.with_suffix(ext)
        if candidate.exists():
            existing.append(candidate)
        for p in dest_dir.glob(f"{filename}_*{ext}"):
            existing.append(p)
    if existing:
        return sorted(existing)[0]
    try:
        response = requests.get(url, timeout=max(1, timeout))
        response.raise_for_status()
    except Exception:
        return None

    data = response.content

    if Image is not None:
        try:
            img = Image.open(BytesIO(data))
            img.load()
            if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
                background = Image.new("RGBA", img.size, (255, 255, 255, 255))
                background.paste(img, mask=img.split()[-1])
                img = background.convert("RGB")
            else:
                img = img.convert("RGB")

            width, height = img.size
            base_size = max(width, height)
            margin_factor = 1.1
            square_size = int(max(base_size * margin_factor, base_size + 2))
            square_img = Image.new("RGB", (square_size, square_size), (255, 255, 255))
            offset_x = (square_size - width) // 2
            offset_y = (square_size - height) // 2
            square_img.paste(img, (offset_x, offset_y))
            final_img = square_img.resize((1280, 1280), Image.Resampling.LANCZOS)

            target_path = base_path.with_suffix(".webp")
            counter = 1
            while target_path.exists():
                target_path = dest_dir / f"{filename}_{counter}.webp"
                counter += 1
            final_img.save(target_path, "WEBP", quality=90, method=6)
            return target_path
        except Exception:
            pass

    target_path = base_path.with_suffix(".img")
    counter = 1
    while target_path.exists():
        target_path = dest_dir / f"{filename}_{counter}.img"
        counter += 1
    try:
        with target_path.open("wb") as fh:
            fh.write(data)
    except Exception:
        return None
    return target_path


def _b64_no_pad(s: str) -> str:
    return b64encode(s.encode("utf-8")).decode("utf-8").rstrip("=")


def _parse_snippet_attrs(snippet: Any) -> Dict[str, Any]:
    if not snippet or not isinstance(snippet, str):
        return {}

    def grab(attr: str) -> Any:
        m = re.search(rf'data-flix-{attr}="([^"]*)"', snippet, flags=re.I)
        if m:
            return m.group(1)
        return None

    return {
        "distributor": grab("distributor"),
        "language": grab("language"),
        "fallback_language": grab("fallback-language"),
        "ean": grab("ean"),
        "brand": grab("brand"),
        "sku": grab("sku"),
    }


def _get_domain_from_origin(origin_url: str) -> str:
    origin_url = (origin_url or "").strip()
    if not origin_url:
        return "e.prenta.lt"
    try:
        p = urlparse(origin_url)
        if p.netloc:
            return p.netloc
        return origin_url.lstrip("/")
    except Exception:
        return "e.prenta.lt"


def _build_tjson_url(ean: str, sku: Any, dist_id: str, iso: str, fl_iso: str, origin_domain: str) -> str:
    slug = f"{(sku or '')}{ean}{_b64_no_pad(origin_domain)}"
    base = f"https://media.flixcar.com/modular/web-api/{iso}/{dist_id}/{slug}/t.json"
    return f"{base}?mpn={sku or ''}&ean={ean}&distId={dist_id}&iso={iso}&flIso={fl_iso}"


def _fetch_tjson(url: str) -> Dict[str, Any]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        ),
        "Accept": "application/json,text/plain;q=0.8,*/*;q=0.5",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://e.prenta.lt/",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
        try:
            data = resp.json()
        except Exception:
            data = None
        return {"status_code": resp.status_code, "json": data}
    except Exception as exc:
        return {"status_code": None, "error": str(exc)}


def _normalize_flix_url(url: Any) -> Any:
    if not url or not isinstance(url, str):
        return None
    url = url.strip()
    if not url:
        return None
    if url.startswith("//"):
        return "https:" + url
    return url


def _get_modules_from_record(record_json: Any) -> Dict[str, Any] | None:
    if record_json is None:
        return None
    root: Any = record_json
    if isinstance(record_json, list) and record_json:
        root = record_json[0]
    if not isinstance(root, dict):
        return None
    modules = root.get("modules")
    if isinstance(modules, dict):
        return modules
    return None


def _extract_gallery_from_modules(modules: Dict[str, Any]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    hotspot = modules.get("hotspot")
    if isinstance(hotspot, dict):
        pg = hotspot.get("product_gallery")
        html = pg.get("html") if isinstance(pg, dict) else None
        if html and isinstance(html, str) and BeautifulSoup is not None:
            try:
                soup = BeautifulSoup(html, "html.parser")
            except Exception:
                soup = None
            if soup is not None:
                for img in soup.find_all("img"):
                    src = img.get("src") or img.get("data-src")
                    url = _normalize_flix_url(src)
                    if not url:
                        continue
                    alt = (img.get("alt") or "").strip()
                    results.append({"url": url, "alt": alt})
    if results:
        return results
    img_gallery = modules.get("image_gallery")
    if isinstance(img_gallery, dict):
        items = img_gallery.get("feature_images")
        if not isinstance(items, list) or not items:
            items = img_gallery.get("images")
        if isinstance(items, list):
            for item in items:
                if not isinstance(item, dict):
                    continue
                url = (
                    item.get("img_1000_url")
                    or item.get("img_800_url")
                    or item.get("img_600_url")
                    or item.get("img_400_url")
                    or item.get("img_200_url")
                    or item.get("image_url")
                )
                url = _normalize_flix_url(url)
                if not url:
                    continue
                alt = (item.get("image_text") or "").strip()
                results.append({"url": url, "alt": alt})
    return results


def _extract_features_from_modules(modules: Dict[str, Any]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    hotspot = modules.get("hotspot")
    if isinstance(hotspot, dict):
        kf = hotspot.get("key_features")
        html = kf.get("html") if isinstance(kf, dict) else None
        if html and isinstance(html, str) and BeautifulSoup is not None:
            try:
                soup = BeautifulSoup(html, "html.parser")
            except Exception:
                soup = None
            if soup is not None:
                rows = soup.select(".hotspot_table_row")
                for row in rows:
                    img_el = row.select_one(".hotspot_feature_image_left img, .hotspot_feature_image_right img, img")
                    image_url = _normalize_flix_url(img_el.get("src")) if img_el else None
                    image_alt = (img_el.get("alt") or "").strip() if img_el else ""
                    desc_container = row.select_one(".hotspot_feature_desc") or row
                    title = ""
                    body = ""
                    if desc_container is not None:
                        strong = desc_container.find("strong")
                        if strong is not None:
                            title = strong.get_text(" ", strip=True)
                        lines: List[str] = []
                        for p in desc_container.find_all("p"):
                            text = p.get_text(" ", strip=True)
                            if text:
                                lines.append(text)
                        if lines:
                            body = "\n\n".join(lines)
                    if title or body or image_url:
                        results.append(
                            {
                                "title": title,
                                "subtitle": "",
                                "body": body,
                                "image_url": image_url,
                                "image_alt": image_alt,
                                "kind": "feature",
                            }
                        )
    features_module = modules.get("features")
    if isinstance(features_module, list):
        for mod in features_module:
            if not isinstance(mod, dict):
                continue
            title = (mod.get("title") or "").strip()
            subtitle = (mod.get("sub_title") or "").strip()
            desc = (mod.get("description") or "").strip()
            multiple = mod.get("multiple_main")
            if isinstance(multiple, list) and multiple:
                for entry in multiple:
                    if not isinstance(entry, dict):
                        continue
                    entry_desc = (entry.get("description") or "").strip()
                    imgs = entry.get("background_image")
                    image_url = None
                    image_alt = ""
                    if isinstance(imgs, list) and imgs:
                        img0 = imgs[0]
                        if isinstance(img0, dict):
                            image_url = (
                                img0.get("image_url")
                                or img0.get("img_1000_url")
                                or img0.get("img_800_url")
                                or img0.get("img_600_url")
                                or img0.get("img_400_url")
                                or img0.get("img_200_url")
                            )
                            image_alt = (img0.get("image_text") or "").strip()
                    image_url = _normalize_flix_url(image_url)
                    if not entry_desc and not image_url:
                        continue
                    results.append(
                        {
                            "title": entry_desc,
                            "subtitle": "",
                            "body": "",
                            "image_url": image_url,
                            "image_alt": image_alt,
                            "kind": "icon",
                        }
                    )
            bg_imgs = mod.get("background_image")
            image_url = None
            image_alt = ""
            if isinstance(bg_imgs, list) and bg_imgs:
                img0 = bg_imgs[0]
                if isinstance(img0, dict):
                    image_url = (
                        img0.get("image_url")
                        or img0.get("img_1000_url")
                        or img0.get("img_800_url")
                        or img0.get("img_600_url")
                        or img0.get("img_400_url")
                        or img0.get("img_200_url")
                    )
                    image_alt = (img0.get("image_text") or "").strip()
            image_url = _normalize_flix_url(image_url)
            if title or subtitle or desc or image_url:
                results.append(
                    {
                        "title": title,
                        "subtitle": subtitle,
                        "body": desc,
                        "image_url": image_url,
                        "image_alt": image_alt,
                        "kind": "feature",
                    }
                )
    return results


def _is_background_pixel(rgb: Any, threshold: int = 245) -> bool:
    r, g, b = rgb
    return r >= threshold and g >= threshold and b >= threshold


def _crop_white_border(img: Any, threshold: int = 245, min_content_fraction: float = 0.01) -> Any:
    if img.mode not in ("RGB", "RGBA"):
        img = img.convert("RGB")
    if img.mode == "RGBA":
        background = Image.new("RGBA", img.size, (255, 255, 255, 255))
        background.paste(img, mask=img.split()[-1])
        img = background.convert("RGB")
    width, height = img.size
    pixels = img.load()

    def row_is_background(y: int) -> bool:
        non_bg = 0
        for x in range(width):
            if not _is_background_pixel(pixels[x, y], threshold=threshold):
                non_bg += 1
        return non_bg / max(1, width) < min_content_fraction

    def col_is_background(x: int) -> bool:
        non_bg = 0
        for y in range(height):
            if not _is_background_pixel(pixels[x, y], threshold=threshold):
                non_bg += 1
        return non_bg / max(1, height) < min_content_fraction

    top = 0
    while top < height and row_is_background(top):
        top += 1
    bottom = height - 1
    while bottom >= top and row_is_background(bottom):
        bottom -= 1
    left = 0
    while left < width and col_is_background(left):
        left += 1
    right = width - 1
    while right >= left and col_is_background(right):
        right -= 1
    if right <= left or bottom <= top:
        return img
    return img.crop((left, top, right + 1, bottom + 1))


def _download_and_crop_flix_image(url: str, dest_dir: Path, filename: str, timeout: int = DEFAULT_IMAGE_TIMEOUT) -> Optional[Path]:
    if not url:
        return None
    base_path = dest_dir / filename
    existing: list[Path] = []
    for ext in (".webp", ".img"):
        candidate = base_path.with_suffix(ext)
        if candidate.exists():
            existing.append(candidate)
        for p in dest_dir.glob(f"{filename}_*{ext}"):
            existing.append(p)
    if existing:
        return sorted(existing)[0]
    try:
        resp = requests.get(url, timeout=max(1, timeout))
        resp.raise_for_status()
    except Exception:
        return None
    data = resp.content
    if Image is not None:
        try:
            img = Image.open(BytesIO(data))
            img.load()
            img = _crop_white_border(img)
            target_path = base_path.with_suffix(".webp")
            counter = 1
            while target_path.exists():
                target_path = dest_dir / f"{filename}_{counter}.webp"
                counter += 1
            img.save(target_path, "WEBP", quality=90, method=6)
            return target_path
        except Exception:
            pass
    target_path = base_path.with_suffix(".img")
    counter = 1
    while target_path.exists():
        target_path = dest_dir / f"{filename}_{counter}.img"
        counter += 1
    try:
        with target_path.open("wb") as fh:
            fh.write(data)
    except Exception:
        return None
    return target_path


def _download_and_square_flix_gallery_image(url: str, dest_dir: Path, filename: str, timeout: int = DEFAULT_IMAGE_TIMEOUT) -> Optional[Path]:
    return _square_product_image(url, dest_dir, filename, timeout=timeout)


def _html_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _build_feature_html(blocks: List[Dict[str, Any]], product_id: Any) -> str:
    if not blocks:
        return ""
    product_dir = _prepare_product_images_dir(product_id)
    if product_dir is None:
        product_dir = PRODUCT_IMAGES_DIR / str(product_id)
        product_dir.mkdir(parents=True, exist_ok=True)
    parts: List[str] = []
    feature_index = 0
    for block in blocks:
        if not isinstance(block, dict):
            continue
        title = str(block.get("title") or "").strip()
        subtitle = str(block.get("subtitle") or "").strip()
        body = str(block.get("body") or "").strip()
        image_url = block.get("image_url")
        image_alt = str(block.get("image_alt") or "").strip()
        local_src = None
        if isinstance(image_url, str) and image_url.strip():
            feature_index += 1
            local_path = _download_and_crop_flix_image(image_url, product_dir, f"flix_feature_{feature_index}")
            if local_path is not None:
                try:
                    rel = local_path.relative_to(ROOT)
                    local_src = str(rel).replace("\\", "/")
                except Exception:
                    local_src = str(local_path)
        if local_src:
            parts.append(
                f"<img src=\"{_html_escape(local_src)}\" alt=\"{_html_escape(image_alt)}\">"
            )
        if title:
            parts.append(f"<h3>{_html_escape(title)}</h3>")
        if subtitle:
            parts.append(f"<h4>{_html_escape(subtitle)}</h4>")
        if body:
            normalized = " ".join(body.split())
            parts.append(f"<p>{_html_escape(normalized)}</p>")
    return "\n".join(parts)


def _enrich_product_with_flix(detail: Dict[str, Any], product_id: Any) -> None:
    snippet = detail.get("description_flixmedia")
    if not isinstance(snippet, str) or not snippet.strip():
        return
    attrs = _parse_snippet_attrs(snippet)
    ean = attrs.get("ean") or detail.get("barcode")
    sku = attrs.get("sku") or detail.get("internal_reference") or detail.get("barcode")
    dist_id = (attrs.get("distributor") or "15151").strip() or "15151"
    iso = (attrs.get("language") or "en").strip() or "en"
    fl_iso = (attrs.get("fallback_language") or "lt").strip() or "lt"
    if not ean:
        return
    origin = "https://e.prenta.lt/"
    origin_domain = _get_domain_from_origin(origin)
    url = _build_tjson_url(str(ean), sku, dist_id, iso, fl_iso, origin_domain)
    resp = _fetch_tjson(url)
    status_code = resp.get("status_code")
    rec_json = resp.get("json")
    if status_code != 200 or rec_json is None:
        return
    if isinstance(rec_json, dict):
        oem = rec_json.get("oem")
        comp = rec_json.get("comp")
        if isinstance(oem, dict) and oem.get("match_failed"):
            return
        if isinstance(comp, dict) and comp.get("match_failed"):
            return
    modules = _get_modules_from_record(rec_json)
    if modules is None:
        return
    gallery = _extract_gallery_from_modules(modules)
    features = _extract_features_from_modules(modules)
    gallery_paths: List[str] = []
    if gallery:
        product_dir = _prepare_product_images_dir(product_id)
        if product_dir is None:
            product_dir = PRODUCT_IMAGES_DIR / str(product_id)
            product_dir.mkdir(parents=True, exist_ok=True)
        seen: set[str] = set()
        for idx, item in enumerate(gallery, start=1):
            if not isinstance(item, dict):
                continue
            url_val = item.get("url")
            if not isinstance(url_val, str) or not url_val.strip():
                continue
            if url_val in seen:
                continue
            seen.add(url_val)
            local = _download_and_square_flix_gallery_image(url_val, product_dir, f"flix_gallery_{idx}")
            if local is not None:
                try:
                    rel = local.relative_to(ROOT)
                    gallery_paths.append(str(rel).replace("\\", "/"))
                except Exception:
                    gallery_paths.append(str(local))
    if gallery_paths:
        detail["gallery_images"] = gallery_paths
    feature_html = _build_feature_html(features, product_id)
    if feature_html:
        detail["feature_blocks"] = feature_html


def build_category_paths(categories: List[Dict[str, Any]]) -> Dict[Any, str]:
    """Ehita category_id -> "All > ... > ..." rada.

    Sama loogika mis Samm 0-s: kasutame parent_id ja name välju.
    """

    by_id: Dict[Any, Dict[str, Any]] = {c.get("id"): c for c in categories if c.get("id") is not None}
    cache: Dict[Any, str] = {}

    def path_for(cid: Any) -> str:
        if cid in cache:
            return cache[cid]
        node = by_id.get(cid)
        if not node:
            cache[cid] = ""
            return ""
        name = str(node.get("name") or "").strip()
        parent_id = node.get("parent_id")
        if parent_id is None or parent_id == cid:
            cache[cid] = name
            return cache[cid]
        parent_path = path_for(parent_id)
        full = name if not parent_path else f"{parent_path} > {name}"
        cache[cid] = full
        return full

    for cid in by_id:
        path_for(cid)
    return cache


def category_is_allowed(category_id: Any, category_paths: Dict[Any, str], runlist: List[str]) -> bool:
    """Kontrolli, kas antud category_id rada sobitub mõne runlisti prefiksiga."""

    if not runlist:
        return True
    path = category_paths.get(category_id) or ""
    if not path:
        return False
    for entry in runlist:
        if path == entry:
            return True
        if path.startswith(entry + " > "):
            return True
    return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Kogu Prenta toodete detailandmed valitud kategooriate jaoks (raw /products/{id} väljund)"
    )
    parser.add_argument("--base-url", default=os.getenv("PRENTA_BASE_URL"), help="API baas-URL")
    parser.add_argument("--username", default=os.getenv("PRENTA_USERNAME", ""), help="Basic Auth kasutajanimi")
    parser.add_argument("--password", default=os.getenv("PRENTA_PASSWORD", ""), help="Basic Auth parool")
    parser.add_argument(
        "--per-page",
        type=int,
        default=int(os.getenv("PRENTA_PER_PAGE", "100")),
        help="Kirjete arv lehe kohta (max 100)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=int(os.getenv("PRENTA_TIMEOUT", "30")),
        help="HTTP timeout sekundites",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=int(os.getenv("PRENTA_MAX_RETRIES", "5")),
        help="Maksimaalsed päringu kordused",
    )
    parser.add_argument(
        "--verify-ssl",
        default=os.getenv("PRENTA_VERIFY_SSL", "false"),
        help="Kas valideerida TLS sert (true/false)",
    )
    parser.add_argument(
        "--max-products",
        type=int,
        default=0,
        help="Maksimaalselt töödeldavate toodete arv (0 = piiranguta, kasutamiseks testides)",
    )
    return parser.parse_args()


def main() -> int:
    # .env jääb projekti juurkausta; kasutame find_dotenv, et see üles leida.
    load_dotenv(find_dotenv(), override=False)
    from prenta_fetch import ClientConfig, PrentaClient  # import siia, et failisiseselt ROOT oleks valmis

    args = parse_args()

    if not args.username or not args.password:
        log("❌ Vajalikud PRENTA_USERNAME ja PRENTA_PASSWORD (või CLI lipud)")
        return 1

    verify_ssl = str(args.verify_ssl).strip().lower() in {"1", "true", "yes", "y"}

    cfg = ClientConfig(
        base_url=args.base_url or ClientConfig.base_url,
        username=args.username,
        password=args.password,
        timeout=args.timeout,
        max_retries=args.retries,
        per_page=max(1, min(args.per_page, 100)),
        verify_ssl=verify_ssl,
    )

    runlist = load_runlist()
    if runlist:
        log(f"✔ Runlistist loetud {len(runlist)} kategooriarada.")
    else:
        log("⚠️  Runlist on tühi või puudub – toodete valikut ei piirata kategooria järgi.")

    client = PrentaClient(cfg)

    # 1) Kategooria-hierarhia ja pathid
    log("Laen kategooriad (/categories)…")
    categories: List[Dict[str, Any]] = []
    try:
        for item in client.iter_categories():
            categories.append(item)
    except Exception as exc:
        log(f"❌ Kategooriate kogumise viga: {exc}")
        return 2
    log(f"✔ Kategooriaid kogutud: {len(categories)}")

    category_paths = build_category_paths(categories)

    # Tõlked kategooriaradade jaoks (kui olemas)
    category_translations = load_category_translations()

    # 1b) Tootjad
    log("Laen tootjad (/manufacturers)…")
    manufacturers: List[Dict[str, Any]] = []
    try:
        for item in client.iter_manufacturers():
            manufacturers.append(item)
    except Exception as exc:
        log(f"⚠️  Tootjate kogumisel esines viga: {exc}")
        manufacturers = []
    manufacturer_by_id: Dict[Any, Dict[str, Any]] = {
        m.get("id"): m
        for m in manufacturers
        if isinstance(m, dict) and m.get("id") is not None
    }

    # 1c) Ühikute (Units of Measurement) nimekiri
    log("Laen ühikud (/uoms)…")
    uoms: List[Dict[str, Any]] = []
    try:
        for item in client.iter_uoms():
            uoms.append(item)
    except Exception as exc:
        log(f"⚠️  Ühikute kogumisel esines viga: {exc}")
        uoms = []
    uom_by_id: Dict[Any, Dict[str, Any]] = {
        u.get("id"): u
        for u in uoms
        if isinstance(u, dict) and u.get("id") is not None
    }
    # Salvesta toorest /uoms väljundit uoms.json faili samasse kausta, et saaks hiljem üle vaadata.
    try:
        uoms_path = ROOT / "uoms.json"
        ensure_parent(uoms_path)
        with uoms_path.open("w", encoding="utf-8") as fh:
            json.dump(uoms, fh, ensure_ascii=False, indent=2)
    except Exception:
        pass

    # 2) Toodete list ja filtreerimine kategooria järgi
    max_products = int(getattr(args, "max_products", 0) or 0)
    log("Laen toodete listi (/products)…")
    candidate_ids: List[Any] = []
    try:
        for product in client.iter_products(list_limit=max_products if max_products > 0 else None):
            cid = product.get("category_id")
            if category_is_allowed(cid, category_paths, runlist):
                pid = product.get("id")
                if pid is not None:
                    candidate_ids.append(pid)
    except Exception as exc:
        log(f"❌ Toodete listi kogumise viga: {exc}")
        return 3

    candidate_count = len(candidate_ids)
    log(f"✔ Valitud toodete arv (runlisti järgi): {candidate_count}")
    if not candidate_ids:
        save_raw_data([])
        log(f"✔ Kirjutatud tühi {RAW_OUTPUT_PATH.name} (sobivaid tooteid ei leitud)")
        return 0

    # 2b) Laoseisu filter enne detailandmete laadimist
    log("Laen laoseisu kõigile toodetele (/stock_levels)…")
    all_stocks: List[Dict[str, Any]] = []
    try:
        for item in client.iter_stock_levels():
            if isinstance(item, dict):
                all_stocks.append(item)
    except Exception as exc:
        log(f"⚠️  Laoseisu kogumisel esines viga: {exc}")
    log(f"✔ Laoseisu kirjeid kokku: {len(all_stocks)}")

    candidate_set = set(candidate_ids)
    stock_by_pid: Dict[Any, List[Dict[str, Any]]] = {}
    for s in all_stocks:
        pid = s.get("product_id")
        if pid in candidate_set:
            stock_by_pid.setdefault(pid, []).append(s)

    kept_ids: List[Any] = []
    for pid in candidate_ids:
        stocks = stock_by_pid.get(pid) or []

        total_qty = 0
        for s in stocks:
            if not isinstance(s, dict):
                continue
            qty = s.get("qty")
            if qty is None:
                continue
            try:
                total_qty += max(0, int(qty))
            except Exception:
                continue

        if total_qty > 1:
            kept_ids.append(pid)

    log(f"✔ Laoseisu filtri järel jäi alles {len(kept_ids)}/{candidate_count} toodet (qty > 1).")
    if not kept_ids:
        save_raw_data([])
        log(f"✔ Kõigi runlisti toodete laoseis <= 1 – väljundiks tühi {RAW_OUTPUT_PATH.name}")
        return 0

    # 3) Iga laoseisu filtri läbinud product_id detailandmed
    raw_products: List[Dict[str, Any]] = []
    errors: int = 0

    # Võimalus olemasolevat RAW-faili taaskasutada, et mitte samu tooteid uuesti tõmmata
    ids_for_details: List[Any] = list(kept_ids)
    if SKIP_EXISTING_RAW and RAW_OUTPUT_PATH.exists():
        existing_by_id: Dict[Any, Dict[str, Any]] = {}
        try:
            with RAW_OUTPUT_PATH.open("r", encoding="utf-8") as fh:
                prev_data = json.load(fh)
            if isinstance(prev_data, list):
                for item in prev_data:
                    if not isinstance(item, dict):
                        continue
                    pid_val = item.get("id")
                    if pid_val is None:
                        continue
                    existing_by_id[pid_val] = item
        except Exception as exc:
            log(f"⚠️  Olemasoleva {RAW_OUTPUT_PATH.name} lugemine ebaõnnestus: {exc} – laen kõik {len(kept_ids)} toodet uuesti.")
            existing_by_id = {}

        if existing_by_id:
            new_ids: List[Any] = []
            reused = 0
            for pid in kept_ids:
                if pid in existing_by_id:
                    raw_products.append(existing_by_id[pid])
                    reused += 1
                else:
                    new_ids.append(pid)
            ids_for_details = new_ids
            log(
                f"✔ Olemasolevast {RAW_OUTPUT_PATH.name} failist taaskasutati {reused}/{len(kept_ids)} toodet; uute toodete detailid laetakse uuesti ({len(ids_for_details)} tk)."
            )
        else:
            log(f"ℹ️ {RAW_OUTPUT_PATH.name} ei andnud taaskasutatavaid kirjeid – laen kõik {len(kept_ids)} toodet uuesti.")
    elif SKIP_EXISTING_RAW and not RAW_OUTPUT_PATH.exists():
        log(f"ℹ️ {RAW_OUTPUT_PATH.name} ei leitud – laen kõik {len(kept_ids)} toodet uuesti.")

    log("Laen toodete detailandmeid (/products/{id}, hinnad, laoseisud, attribuudid, pildid)…")
    total = len(ids_for_details)
    for idx, pid in enumerate(ids_for_details, start=1):
        try:
            # Laoseisu filter on juba rakendatud (kept_ids), siin laeme ainult detailid ja hinnad
            detail = client.get_product_detail(pid)
            if isinstance(detail, dict):
                # Kategooriarada ja selle tõlge
                cid = detail.get("category_id")
                cat_path = category_paths.get(cid) or ""
                if cat_path:
                    detail["category_path"] = cat_path
                    translated = category_translations.get(cat_path)
                    if translated:
                        detail["category_path_translated"] = translated

                # Tootjanimi
                mid = detail.get("manufacturer_id")
                if mid is not None and manufacturer_by_id:
                    m = manufacturer_by_id.get(mid)
                    if m is not None and isinstance(m, dict):
                        name_val = m.get("name")
                        if isinstance(name_val, str) and name_val.strip():
                            detail["manufacturer_name"] = name_val.strip()

                # Hinnad selle toote jaoks
                prices = list(client.iter_prices(product_id=pid))

                if prices:
                    detail["prices"] = prices

                # Kasuta eelnevalt laetud laoseisu
                stocks = stock_by_pid.get(pid) or []
                if stocks:
                    detail["stock_levels"] = stocks

                # Tootepildid (detail["images"]) -> lokaalsed ruudukujulised WEBP-d
                images = detail.get("images") or []
                local_images: List[str] = []
                if isinstance(images, list) and images:
                    product_dir = _prepare_product_images_dir(pid)
                    if product_dir is not None:
                        for img_idx, url in enumerate(images):
                            if not isinstance(url, str) or not url.strip():
                                continue
                            local = _square_product_image(url, product_dir, f"image_{img_idx + 1}")
                            if local is not None:
                                try:
                                    rel = local.relative_to(ROOT)
                                    local_images.append(str(rel).replace("\\", "/"))
                                except Exception:
                                    local_images.append(str(local))
                if local_images:
                    detail["local_images"] = local_images

                try:
                    _enrich_product_with_flix(detail, pid)
                except Exception:
                    pass

                attrs = list(client.iter_product_attributes(pid))
                # Salvesta kogu /products/{id}/attributes vastus raw andmetesse,
                # et ükski UoM või muu väli vahepeal kaduma ei läheks.
                detail["product_attributes_raw"] = attrs

                attr_vals = list(client.iter_product_attribute_values(pid))
                attr_by_id = {
                    a.get("id"): a
                    for a in attrs
                    if isinstance(a, dict) and a.get("id") is not None
                }
                val_by_id = {
                    v.get("id"): v
                    for v in attr_vals
                    if isinstance(v, dict) and v.get("id") is not None
                }

                lines = detail.get("attribute_line_ids") or []
                if isinstance(lines, list):
                    for line in lines:
                        if not isinstance(line, dict):
                            continue
                        aid = line.get("attribute_id")
                        vid = line.get("value_id")
                        a = attr_by_id.get(aid) if aid is not None else None
                        v = val_by_id.get(vid) if vid is not None else None
                        name = (a or {}).get("name")
                        atype = (v or {}).get("type") or (a or {}).get("type")
                        uom_id = (a or {}).get("uom_id")
                        uom = uom_by_id.get(uom_id) if uom_id is not None else None
                        unit_name = (uom or {}).get("name")
                        value: Any = None
                        if v is not None:
                            if atype == "selection" or ("value_text" in v):
                                value = v.get("value_text")
                            elif atype == "integer" or ("value_integer" in v):
                                value = v.get("value_integer")
                            elif atype == "float" or ("value_float" in v):
                                value = v.get("value_float")
                            elif atype == "boolean" or ("value_boolean" in v):
                                value = v.get("value_boolean")
                        if name is not None:
                            line["attribute_name"] = name
                        if value is not None:
                            line["attribute_value"] = value
                        if uom_id is not None:
                            line["attribute_uom_id"] = uom_id
                        if isinstance(unit_name, str) and unit_name.strip():
                            line["attribute_unit"] = unit_name.strip()
                # Tõsta attribute_line_ids võtmena objekti lõppu, et lugemine oleks loogilisem.
                if "attribute_line_ids" in detail:
                    reordered: Dict[str, Any] = {}
                    for key, val in detail.items():
                        if key != "attribute_line_ids":
                            reordered[key] = val
                    reordered["attribute_line_ids"] = detail.get("attribute_line_ids")
                    detail = reordered

                raw_products.append(detail)
                if idx % 20 == 0 or idx == total:
                    log(f"… kandidaate läbi käidud {idx}/{total}, väljundisse lisatud {len(raw_products)}")
            else:
                log(f"⚠️  Toote {pid} detailvastus ei olnud JSON-objekt, jäeti vahele")
        except Exception as exc:
            errors += 1
            log(f"❌ Viga toote {pid} detaili laadimisel: {exc}")

    save_raw_data(raw_products)
    log(f"✔ Kirjutatud {len(raw_products)} toote detailid faili: {RAW_OUTPUT_PATH}")
    if errors:
        log(f"⚠️  Detailide laadimisel esines vigu {errors} toote puhul.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
