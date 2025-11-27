
from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

import requests
from bs4 import BeautifulSoup
from dotenv import find_dotenv, load_dotenv

from prenta_fetch import (
    ClientConfig,
    PrentaClient,
    render_flixmedia_html,
    slugify_key,
    _build_features_html_from_key_features_html,
    _build_tjson_url,
    _fetch_tjson,
    _get_domain_from_origin,
    _parse_snippet_attrs,
    DEFAULT_BASE_URL,
)

try:
    from tools.flix_probe import build_features_section as probe_build_features
except Exception:
    probe_build_features = None

try:
    from PIL import Image  # type: ignore
except Exception:
    Image = None  # type: ignore

try:  # pragma: no cover - tqdm on mugavuse jaoks
    from tqdm import tqdm  # type: ignore
except Exception:  # pragma: no cover - valikuline sõltuvus
    pass

ROOT = Path(__file__).parent
RUNLIST_PATH = ROOT / "category_runlist.json"
TRANSLATION_PATH = ROOT / "category_translation.json"
PROCESSED_DIR = ROOT / "data" / "processed"
LATEST_DIR = ROOT / "data" / "latest"
FULL_FEED_PATH = LATEST_DIR / "full.json"
LIGHT_FEED_PATH = LATEST_DIR / "light.json"
UNKNOWN_FIELDS_PATH = PROCESSED_DIR / "INNPRO_EXPORT_UNKNOWN_FIELDS.json"
FLIX_MEDIA_DIR = PROCESSED_DIR / "flix_media"
PRODUCT_IMAGES_DIR = PROCESSED_DIR / "product_images"
FLIX_MISSING_REPORT_PATH = PROCESSED_DIR / "flix_modules_missing.json"
DEFAULT_IMAGE_TIMEOUT = 30

def load_runlist() -> List[str]:
    if not RUNLIST_PATH.exists():
        return []
    try:
        with RUNLIST_PATH.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
            if isinstance(data, list):
                items: List[str] = []
                for entry in data:
                    if isinstance(entry, str) and entry.strip():
                        val = entry.strip()
                        val = val.rstrip("/")
                        val = val.replace(" > ", "/").replace(">", "/")
                        if not val.endswith("/"):
                            val += "/"
                        while "//" in val:
                            val = val.replace("//", "/")
                        items.append(val)
                return items
    except Exception:
        return []
    return []

def category_matches_runlist(path: Optional[str], runlist: List[str]) -> bool:
    if not runlist:
        return True
    normalized = (path or "").replace(" > ", "/")
    if normalized and not normalized.endswith("/"):
        normalized += "/"
    for prefix in runlist:
        if normalized.startswith(prefix):
            return True
    return False


def build_category_paths(categories: List[Dict[str, Any]]) -> Dict[Any, str]:
    by_id: Dict[Any, Dict[str, Any]] = {
        c.get("id"): c for c in categories if c.get("id") is not None
    }
    cache: Dict[Any, str] = {}

    def resolve(cid: Any) -> str:
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
        parent_path = resolve(parent_id)
        full = name if not parent_path else f"{parent_path} > {name}"
        cache[cid] = full
        return full

    for cid in by_id:
        resolve(cid)
    return cache


def load_category_translations() -> Dict[str, str]:
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


def grouped_output_path() -> Path:
    return PROCESSED_DIR / "products_grouped.json"


def ensure_processed_dir() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    FLIX_MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    PRODUCT_IMAGES_DIR.mkdir(parents=True, exist_ok=True)


def ensure_latest_dir() -> None:
    LATEST_DIR.mkdir(parents=True, exist_ok=True)


def _prepare_flix_media_dir(product_id: Any) -> Path:
    target = FLIX_MEDIA_DIR / str(product_id)
    if target.exists():
        for child in list(target.iterdir()):
            if child.is_file():
                try:
                    child.unlink()
                except Exception:
                    pass
    target.mkdir(parents=True, exist_ok=True)
    return target


def _prepare_product_images_dir(product_id: Any, clear: bool = False) -> Optional[Path]:
    if product_id is None:
        return None
    target = PRODUCT_IMAGES_DIR / str(product_id)
    if clear and target.exists():
        for child in list(target.iterdir()):
            if child.is_file():
                try:
                    child.unlink()
                except Exception:
                    pass
    target.mkdir(parents=True, exist_ok=True)
    return target


def _square_product_image(url: str, dest_dir: Path, filename: str, timeout: int = DEFAULT_IMAGE_TIMEOUT) -> Optional[Path]:
    if not url:
        return None
    try:
        response = requests.get(url, timeout=max(1, timeout))
        response.raise_for_status()
    except Exception:
        return None

    data = response.content
    base_path = dest_dir / f"{filename}"

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


def _sanitize_filename(name: str, default: str = "image") -> str:
    name = unquote(name or "")
    name = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    name = name.strip("._")
    return name or default


def _download_flix_image(url: str, dest_dir: Path, referer: str, timeout: int) -> Optional[Path]:
    if not url:
        return None
    if url.startswith("//"):
        url = "https:" + url
    if not url.lower().startswith("http"):
        return None
    dest_dir.mkdir(parents=True, exist_ok=True)
    parsed = urlparse(url)
    default_referer = f"{parsed.scheme or 'https'}://{parsed.netloc}/" if parsed.netloc else "https://media.flixcar.com/"
    if "flixcar.com" not in (parsed.netloc or ""):
        default_referer = "https://media.flixcar.com/"
    referer_header = referer or default_referer
    headers = {
        "Referer": referer_header,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }
    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=max(1, timeout),
        )
        response.raise_for_status()
    except Exception:
        return None

    base_name = _sanitize_filename(os.path.basename(parsed.path))
    stem, ext = os.path.splitext(base_name)
    content_type = (response.headers.get("content-type") or "").lower()
    if not ext:
        if "png" in content_type:
            ext = ".png"
        elif "webp" in content_type:
            ext = ".webp"
        elif "gif" in content_type:
            ext = ".gif"
        else:
            ext = ".jpg"
    filename = f"{stem or 'image'}{ext}"
    target_path = dest_dir / filename
    counter = 1
    while target_path.exists():
        target_path = dest_dir / f"{stem or 'image'}_{counter}{ext}"
        counter += 1
    if Image is not None:
        try:
            img = Image.open(BytesIO(response.content))
            img.load()
            if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
                rgba = img.convert("RGBA")
                alpha = rgba.split()[-1]
                bbox = alpha.getbbox()
                if bbox:
                    rgba = rgba.crop(bbox)
                    alpha = rgba.split()[-1]
                background = Image.new("RGBA", rgba.size, (255, 255, 255, 255))
                background.paste(rgba, mask=alpha)
                processed = background.convert("RGB")
            else:
                processed = img.convert("RGB") if img.mode not in ("RGB", "L") else img.convert("RGB")

            webp_name = f"{stem or 'image'}.webp"
            target_path = dest_dir / webp_name
            counter = 1
            while target_path.exists():
                target_path = dest_dir / f"{stem or 'image'}_{counter}.webp"
                counter += 1
            processed.save(target_path, "WEBP", quality=90, method=6)
            return target_path
        except Exception:
            pass

    try:
        with target_path.open("wb") as fh:
            fh.write(response.content)
    except Exception:
        return None
    return target_path


def _process_flix_features_html(html: str, product_id: Any, referer: str, timeout: int) -> str:
    if not html or not isinstance(html, str):
        return html
    try:
        media_dir = _prepare_flix_media_dir(product_id)
        soup = BeautifulSoup(html, "html.parser")
        new_doc = BeautifulSoup("", "html.parser")
        section = new_doc.new_tag("section", **{"class": "flix-features"})

        for article in soup.find_all("article"):
            new_article = new_doc.new_tag("article", **{"class": "flixf-item"})
            img = article.find("img")
            if img and img.get("src"):
                local_path = _download_flix_image(img.get("src"), media_dir, referer, timeout)
                if local_path:
                    figure = new_doc.new_tag("figure", **{"class": "flixf-media"})
                    img_tag = new_doc.new_tag(
                        "img",
                        src=_relative_to_root(local_path).replace("\\", "/"),
                        alt=img.get("alt", ""),
                    )
                    figure.append(img_tag)
                    new_article.append(figure)

            body_div = new_doc.new_tag("div", **{"class": "flixf-body"})
            heading = None
            for tag_name in ["h1", "h2", "h3", "h4", "h5", "h6"]:
                candidate = article.find(tag_name)
                if candidate and candidate.get_text(strip=True):
                    heading = candidate.get_text(" ", strip=True)
                    break
            if heading:
                h3 = new_doc.new_tag("h3")
                h3.string = heading
                body_div.append(h3)

            for p in article.find_all("p"):
                text = p.get_text(" ", strip=True)
                if text:
                    p_tag = new_doc.new_tag("p")
                    p_tag.string = text
                    body_div.append(p_tag)

            if body_div.contents:
                new_article.append(body_div)

            if new_article.contents:
                section.append(new_article)

        if not section.contents:
            return ""

        return "<!DOCTYPE html><meta charset=\"utf-8\">" + section.decode()
    except Exception:
        return html


def _clean_flix_text(value: Any) -> str:
    if not value or not isinstance(value, str):
        return ""
    text = html.unescape(value).strip()
    if not text:
        return ""
    if "<" in text and ">" in text:
        try:
            soup = BeautifulSoup(text, "html.parser")
            cleaned = soup.get_text(" ", strip=True)
            if cleaned:
                return cleaned
        except Exception:
            pass
    return text


def _summarize_flix_module(module: Dict[str, Any]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "module_id": module.get("module_id") or module.get("id"),
        "module_category": module.get("module_category"),
        "module_type": module.get("type"),
    }
    title = module.get("main_title") or module.get("title") or module.get("module_title")
    cleaned_title = _clean_flix_text(title)
    if cleaned_title:
        summary["title"] = cleaned_title
    keys = [str(k) for k in module.keys()]
    if keys:
        summary["keys"] = sorted(set(keys))
    for list_key in ("multiple_main", "multiple", "items", "blocks", "rows", "slides"):
        value = module.get(list_key)
        if isinstance(value, list):
            dict_items = [item for item in value if isinstance(item, dict)]
            summary[f"{list_key}_len"] = len(dict_items)
            if dict_items:
                first_keys = [str(k) for k in dict_items[0].keys()]
                if first_keys:
                    summary[f"{list_key}_first_keys"] = sorted(set(first_keys))
    html_val = module.get("html")
    if isinstance(html_val, str) and html_val.strip():
        summary["has_inline_html"] = True
    description_val = module.get("description") or module.get("main_description")
    cleaned_desc = _clean_flix_text(description_val)
    if cleaned_desc:
        summary["has_description"] = True
    return {k: v for k, v in summary.items() if v not in (None, [], {}, "")}


def _extract_flix_image(node: Any) -> Tuple[Optional[str], Optional[str]]:
    def pick_from_dict(d: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        for key in ("img_1000_url", "img_800_url", "img_600_url", "img_400_url", "img_200_url", "image_url", "src", "url"):
            val = d.get(key)
            if isinstance(val, str) and val.strip():
                alt = _clean_flix_text(d.get("image_text") or d.get("alt") or "")
                return val.strip(), alt
        return None, None

    if isinstance(node, dict):
        # First pick from nested image containers
        for key in (
            "background_image",
            "background_images",
            "main_background_image",
            "main_background_image1",
            "feature_images",
            "images",
            "image",
            "media",
        ):
            child = node.get(key)
            src, alt = _extract_flix_image(child)
            if src:
                return src, alt
        return pick_from_dict(node)
    if isinstance(node, list):
        for item in node:
            src, alt = _extract_flix_image(item)
            if src:
                return src, alt
    return None, None


def _build_features_html_from_modules(modules: Any) -> Optional[str]:
    """Build universal fallback HTML from Flix modules."""

    if not modules:
        return None

    queue: List[Any]
    if isinstance(modules, list):
        queue = list(modules)
    elif isinstance(modules, dict):
        queue = [modules]
    else:
        return None

    seen_ids: set[Any] = set()
    feature_parts: List[str] = []

    def to_iterable(value: Any) -> List[Any]:
        if isinstance(value, list):
            return value
        if isinstance(value, tuple):
            return list(value)
        if isinstance(value, dict):
            return [value]
        return []

    def extract_texts(value: Any) -> List[str]:
        texts: List[str] = []
        if isinstance(value, str):
            cleaned = _clean_flix_text(value)
            if cleaned:
                texts.append(cleaned)
        elif isinstance(value, (int, float)):
            texts.append(str(value))
        elif isinstance(value, dict):
            preferred_keys = (
                "text",
                "value",
                "label",
                "title",
                "name",
                "description",
                "copy",
                "body",
                "content",
            )
            found = False
            for key in preferred_keys:
                if key in value:
                    texts.extend(extract_texts(value.get(key)))
                    found = True
            if not found:
                for item in value.values():
                    texts.extend(extract_texts(item))
        elif isinstance(value, (list, tuple)):
            for item in value:
                texts.extend(extract_texts(item))
        return [t for t in texts if t]

    def normalize_text_list(block: Dict[str, Any], keys: Tuple[str, ...]) -> List[str]:
        texts: List[str] = []
        for key in keys:
            if key in block:
                texts.extend(extract_texts(block.get(key)))
        return texts

    def build_table(rows: Any) -> str:
        if not isinstance(rows, list):
            return ""
        body_rows: List[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            cells = to_iterable(row.get("cells") or row.get("columns") or row.get("values"))
            cell_html: List[str] = []
            for cell in cells:
                if isinstance(cell, dict):
                    text = _clean_flix_text(cell.get("text") or cell.get("value") or cell.get("label"))
                else:
                    text = _clean_flix_text(cell)
                if not text:
                    text = ""
                cell_html.append(f"<td>{html.escape(text)}</td>")
            if cell_html:
                body_rows.append(f"<tr>{''.join(cell_html)}</tr>")
        if not body_rows:
            return ""
        return f"<table class=\"flixf-table\"><tbody>{''.join(body_rows)}</tbody></table>"

    def build_documents(docs: Any) -> str:
        items = to_iterable(docs)
        links: List[str] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            url = item.get("url") or item.get("href") or (item.get("document") or {}).get("url")
            label = _clean_flix_text(item.get("name") or item.get("title") or item.get("label"))
            if url and label:
                links.append(f"<li><a href=\"{html.escape(url)}\" target=\"_blank\" rel=\"noopener\">{html.escape(label)}</a></li>")
        if not links:
            return ""
        return f"<ul class=\"flixf-docs\">{''.join(links)}</ul>"

    def collect_media(block: Dict[str, Any]) -> List[Tuple[str, Optional[str]]]:
        media_list: List[Tuple[str, Optional[str]]] = []
        src, alt = _extract_flix_image(block)
        if src:
            media_list.append((src, alt))
        for key in ("slides", "feature_images", "images", "cards"):
            for entry in to_iterable(block.get(key)):
                if isinstance(entry, dict):
                    src, alt = _extract_flix_image(entry)
                    if src:
                        media_list.append((src, alt))
        return media_list

    def module_blocks(module: Dict[str, Any]) -> List[Dict[str, Any]]:
        collected: List[Dict[str, Any]] = []
        for key in (
            "multiple_main",
            "multiple",
            "items",
            "blocks",
            "rows",
            "slides",
            "cards",
            "modules",
            "features",
        ):
            for entry in to_iterable(module.get(key)):
                if isinstance(entry, dict):
                    collected.append(entry)
        if not collected:
            collected.append(module)
        return collected

    def block_to_article(block: Dict[str, Any], module: Dict[str, Any]) -> Optional[str]:
        title_candidates = (
            block.get("title"),
            block.get("feature_title"),
            block.get("main_title"),
            block.get("main_sub_title"),
            block.get("sub_title"),
            module.get("main_title"),
            module.get("title"),
            module.get("module_title"),
        )
        title = ""
        for candidate in title_candidates:
            title = _clean_flix_text(candidate)
            if title:
                break

        description_fields = (
            "description",
            "main_description",
            "sub_description",
            "body",
            "text",
            "copy",
            "summary",
            "caption",
        )
        descriptions = normalize_text_list(block, description_fields)
        if not descriptions:
            fallback_module_desc = _clean_flix_text(module.get("main_description") or module.get("description"))
            if fallback_module_desc:
                descriptions.append(fallback_module_desc)

        bullets = block.get("bullets") or block.get("list") or block.get("bullet_points")
        bullet_texts = extract_texts(bullets) if bullets else []

        table_html = ""
        for key in ("table", "rows", "table_rows"):
            table_html = build_table(block.get(key))
            if table_html:
                break

        docs_html = ""
        for key in ("documents", "document_list", "downloads"):
            docs_html = build_documents(block.get(key))
            if docs_html:
                break

        inline_raw = block.get("html") or block.get("body_html")
        if isinstance(inline_raw, str) and "<" in inline_raw and ">" in inline_raw:
            trimmed = inline_raw.strip()
            if trimmed:
                return trimmed

        media_entries = collect_media(block)

        if not any([title, descriptions, bullet_texts, table_html, docs_html, media_entries]):
            return None

        media_html = ""
        if media_entries:
            figures: List[str] = []
            for src, alt in media_entries:
                if not src:
                    continue
                figures.append(
                    f'<figure class="flixf-media"><img src="{html.escape(src)}" '
                    f'alt="{html.escape(alt or title or "")}" /></figure>'
                )
            media_html = "".join(figures)

        body_segments: List[str] = []
        if title:
            body_segments.append(f"<h3>{html.escape(title)}</h3>")
        for desc in descriptions:
            body_segments.append(f"<p>{html.escape(desc)}</p>")
        if bullet_texts:
            bullet_items = "".join(f"<li>{html.escape(text)}</li>" for text in bullet_texts)
            body_segments.append(f"<ul>{bullet_items}</ul>")
        if table_html:
            body_segments.append(table_html)
        if docs_html:
            body_segments.append(docs_html)

        body_html = ""
        if body_segments:
            body_html = f"<div class=\"flixf-body\">{''.join(body_segments)}</div>"

        if not any([media_html, body_html]):
            return None
        return f"<article class=\"flixf-item\">{media_html}{body_html}</article>"

    while queue:
        node = queue.pop(0)
        if not isinstance(node, dict):
            continue

        node_id = node.get("module_id") or node.get("id")
        if node_id and node_id in seen_ids:
            continue
        if node_id:
            seen_ids.add(node_id)

        if node.get("html") and not node.get("module_category"):
            # Inline HTML snippet – reuse it directly
            inline_html = str(node.get("html") or "").strip()
            if inline_html:
                feature_parts.append(inline_html)
                continue

        blocks = module_blocks(node)
        for block in blocks:
            if block is node:
                article = block_to_article(block, node)
                if article:
                    feature_parts.append(article)
            else:
                queue.append(block)

    if not feature_parts:
        return None

    body = "".join(feature_parts)
    if "<article" not in body:
        return "<!DOCTYPE html><meta charset=\"utf-8\">" + body
    return "<!DOCTYPE html><meta charset=\"utf-8\"><section class=\"flix-features\">" + body + "</section>"


def save_json(path: str, payload: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def _relative_to_root(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _category_path_to_source(path: str) -> str:
    return path.replace(" > ", "/") if path else ""


def _slugify(text: str) -> str:
    text = (text or "").lower()
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    text = re.sub(r"[\s_-]+", "-", text).strip("-")
    return text


def _format_price(value: Any) -> str:
    if value is None:
        return ""
    try:
        number = float(str(value).replace(",", "."))
        return f"{number:.2f}"
    except Exception:
        return str(value)


def _to_int(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(float(str(value).replace(",", ".")))
    except Exception:
        return 0


def _build_short_description(text: str, limit: int = 280) -> str:
    plain = re.sub(r"\s+", " ", (text or "")).strip()
    return plain[:limit]


def _convert_attributes(attributes: Dict[str, Any]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for name, raw_value in (attributes or {}).items():
        if not name:
            continue
        values: List[str] = []
        if isinstance(raw_value, list):
            for entry in raw_value:
                entry_str = str(entry).strip()
                if entry_str:
                    values.append(entry_str)
        elif isinstance(raw_value, bool):
            values.append("Yes" if raw_value else "No")
        elif raw_value is None:
            continue
        else:
            entry_str = str(raw_value).strip()
            if entry_str:
                values.append(entry_str)
        if not values:
            continue
        items.append({
            "name": name,
            "visible": True,
            "variation": False,
            "options": values,
        })
    return items


def _normalize_attribute_values(raw_value: Any) -> List[str]:
    values: List[str] = []
    if isinstance(raw_value, list):
        for entry in raw_value:
            entry_str = str(entry).strip()
            if entry_str:
                values.append(entry_str)
    elif isinstance(raw_value, bool):
        values.append("Yes" if raw_value else "No")
    elif raw_value is None:
        return []
    else:
        entry_str = str(raw_value).strip()
        if entry_str:
            values.append(entry_str)
    return values


def _attributes_to_parameters(attributes: Dict[str, Any]) -> List[Dict[str, Any]]:
    params: List[Dict[str, Any]] = []
    for name, raw_value in (attributes or {}).items():
        if not name:
            continue
        values = _normalize_attribute_values(raw_value)
        if not values:
            continue
        if len(values) == 1:
            value_node: Any = {"@name": values[0]}
        else:
            value_node = [{"@name": val} for val in values]
        params.append({"@name": name, "value": value_node})
    return params


def _build_light_item(item: Dict[str, Any]) -> Dict[str, Any]:
    size_node = item.get("sizes", {}).get("size") or {}
    light: Dict[str, Any] = {
        "@code_on_card": item.get("@code_on_card"),
    }
    if item.get("category"):
        light["category"] = item["category"]
    if item.get("producer"):
        light["producer"] = item["producer"]
    description = item.get("description")
    if description:
        light["description"] = description
    light_size: Dict[str, Any] = {}
    if size_node.get("@iaiext:code_external"):
        light_size["@iaiext:code_external"] = size_node.get("@iaiext:code_external")
    price_node = size_node.get("price")
    if price_node:
        light_size["price"] = price_node
    stock_node = size_node.get("stock")
    if stock_node:
        light_size["stock"] = stock_node
    if light_size:
        light["sizes"] = {"size": light_size}
    return light


def build_innpro_item(enriched: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, List[str]]]:
    detail = enriched.get("product") or {}
    product_list = enriched.get("product_list") or {}
    price_info = enriched.get("price") or {}
    stock_info = enriched.get("stock") or {}
    manufacturer = enriched.get("manufacturer") or {}
    attributes = enriched.get("attributes_kv") or {}

    used_detail_keys: set[str] = set()
    used_price_keys: set[str] = set()
    used_stock_keys: set[str] = set()

    def _detail_get(key: str) -> Any:
        if key in detail:
            used_detail_keys.add(key)
        return detail.get(key)

    def _price_get(key: str) -> Any:
        if key in price_info:
            used_price_keys.add(key)
        return price_info.get(key)

    def _stock_get(key: str) -> Any:
        if key in stock_info:
            used_stock_keys.add(key)
        return stock_info.get(key)

    internal_ref = _detail_get("internal_reference")
    barcode = _detail_get("barcode")
    name_val = _detail_get("name")
    description_original = str(_detail_get("description") or "")
    flix_original = str(_detail_get("description_flixmedia") or "")
    images = _detail_get("images")
    _detail_get("manufacturer_id")
    _detail_get("category_id")
    weight_val = _detail_get("weight")
    weight_bruto_val = _detail_get("weight_bruto")

    sku = str(internal_ref or enriched.get("sku") or product_list.get("internal_reference") or product_list.get("name") or "").strip()
    ean = str(barcode or enriched.get("ean") or "").strip()
    product_name = str(name_val or product_list.get("name") or sku).strip()

    flix_description_raw = enriched.get("flix_description_html")
    flix_description = str(flix_description_raw or "").strip()
    if "<script" in flix_description.lower():
        flix_description = ""
    description_html = description_original or flix_description or flix_original
    description_flix = flix_description or ""
    description_original_field = description_original or ("" if flix_description else flix_original)

    category_path = str(enriched.get("category_path") or detail.get("category_path") or "").strip()
    category_source = _category_path_to_source(category_path)
    brand_name = str((manufacturer or {}).get("name") or "").strip()

    price_gross = _format_price(_price_get("price") or _price_get("gross_price"))
    price_net = _format_price(_price_get("net_price") or _price_get("price_net"))
    price_rrp = _format_price(_detail_get("price_rrp")) if _detail_get("price_rrp") is not None else ""

    stock_qty_raw = _stock_get("qty")
    stock_qty = _to_int(stock_qty_raw)
    forecast_date = _stock_get("forecast_date")

    size_entry: Dict[str, Any] = {}
    if ean:
        size_entry["@iaiext:code_external"] = ean
    price_node: Dict[str, Any] = {}
    if price_gross:
        price_node["@gross"] = price_gross
    if price_net:
        price_node["@net"] = price_net
    if price_node:
        size_entry["price"] = price_node
    stock_node: Dict[str, Any] = {}
    if stock_qty_raw is not None:
        stock_node["@available_stock_quantity"] = str(stock_qty_raw)
    if forecast_date:
        stock_node["@forecast_date"] = str(forecast_date)
    if stock_node:
        size_entry["stock"] = stock_node

    images_node: Dict[str, Any] = {}
    image_nodes: List[Dict[str, Any]] = []
    if isinstance(images, list):
        for idx, item in enumerate(images):
            url: Optional[str] = None
            if isinstance(item, str):
                url = item
            elif isinstance(item, dict):
                url = item.get("url") or item.get("image") or item.get("src") or item.get("href")
            if url:
                image_nodes.append({"@url": url, "@iaiext:priority": str(idx + 1)})
    if image_nodes:
        images_node["large"] = {"image": image_nodes}

    parameter_nodes = _attributes_to_parameters(attributes)

    description_node: Dict[str, Any] = {}
    if product_name:
        description_node["name"] = {"@xml:lang": "eng", "#text": product_name}
    if description_html:
        description_node["long_desc"] = {"@xml:lang": "eng", "#text": description_html}

    item: Dict[str, Any] = {
        "@code_on_card": sku,
    }
    if description_node:
        item["description"] = description_node
    if category_source:
        item["category"] = {"@name": category_source}
    if brand_name:
        item["producer"] = {"@name": brand_name}
    size_container: Dict[str, Any] = {}
    if size_entry:
        size_container["size"] = size_entry
    if size_container:
        item["sizes"] = size_container
    price_container: Dict[str, Any] = {}
    if price_gross:
        price_container["@gross"] = price_gross
    if price_net:
        price_container["@net"] = price_net
    if not price_container and price_rrp:
        price_container["@gross"] = price_rrp
    if price_container:
        item["price"] = price_container
    if images_node:
        item["images"] = images_node
    if parameter_nodes:
        item["parameters"] = {"parameter": parameter_nodes}
    if price_rrp:
        item["rrp"] = price_rrp
    if weight_val is not None or weight_bruto_val is not None:
        weight_block: Dict[str, Any] = {}
        if weight_val is not None:
            weight_block["@net"] = _format_price(weight_val)
        if weight_bruto_val is not None:
            weight_block["@gross"] = _format_price(weight_bruto_val)
        item["weight"] = weight_block

    item["source"] = {
        "innpro_category": category_source,
        "innpro_brand": brand_name,
        "innpro_code_on_card": sku,
        "prenta_product_id": enriched.get("product_id"),
        "prenta_category_path": category_path,
    }

    item["prenta_extra"] = {
        "product_list": product_list,
        "detail": detail,
        "price": price_info,
        "stock": stock_info,
        "attributes_kv": attributes,
        "attributes_resolved": enriched.get("attributes_resolved"),
        "flix": {
            "html": enriched.get("flixmedia_html"),
            "status": enriched.get("flixmedia_status"),
            "error": enriched.get("flixmedia_error"),
            "iframe_src": enriched.get("flixmedia_iframe_src"),
            "description_html": enriched.get("flix_description_html"),
            "tjson_url": enriched.get("flix_tjson_url"),
            "tjson_status": enriched.get("flix_tjson_status"),
        },
    }

    unknown = {
        "detail": sorted(k for k in detail.keys() if k not in used_detail_keys),
        "price": sorted(k for k in price_info.keys() if k not in used_price_keys),
        "stock": sorted(k for k in stock_info.keys() if k not in used_stock_keys),
    }

    return item, unknown


def export_innpro_full_feed(enriched_products: List[Dict[str, Any]]) -> Dict[str, Any]:
    ensure_latest_dir()
    ensure_processed_dir()

    items: List[Dict[str, Any]] = []
    light_items: List[Dict[str, Any]] = []
    source_categories: set[str] = set()
    unknown_detail: Dict[str, int] = defaultdict(int)
    unknown_price: Dict[str, int] = defaultdict(int)
    unknown_stock: Dict[str, int] = defaultdict(int)

    for enriched in enriched_products:
        item, unknown = build_innpro_item(enriched)
        items.append(item)
        light_items.append(_build_light_item(item))
        cat = str(((item.get("category") or {}).get("@name") or "").strip())
        if cat:
            source_categories.add(cat)
        for key in unknown.get("detail", []):
            unknown_detail[key] += 1
        for key in unknown.get("price", []):
            unknown_price[key] += 1
        for key in unknown.get("stock", []):
            unknown_stock[key] += 1

    full_payload = {"offer": {"products": {"product": {"item": items}}}}
    light_payload = {"offer": {"products": {"product": {"item": light_items}}}}

    FULL_FEED_PATH.write_text(json.dumps(full_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    LIGHT_FEED_PATH.write_text(json.dumps(light_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    unknown_report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "detail_unknown_keys": dict(sorted(unknown_detail.items())),
        "price_unknown_keys": dict(sorted(unknown_price.items())),
        "stock_unknown_keys": dict(sorted(unknown_stock.items())),
        "source_category_paths": sorted(source_categories),
    }
    UNKNOWN_FIELDS_PATH.write_text(json.dumps(unknown_report, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "full_path": FULL_FEED_PATH,
        "light_path": LIGHT_FEED_PATH,
        "unknown_report": UNKNOWN_FIELDS_PATH,
        "items_total": len(items),
        "source_categories": sorted(source_categories),
    }


def _collect_images(detail: Dict[str, Any], product_dir: Optional[Path]) -> List[Dict[str, Any]]:
    images = detail.get("images")
    output: List[Dict[str, Any]] = []
    if not isinstance(images, list):
        return output
    name = str(detail.get("name") or "").strip()
    for idx, item in enumerate(images):
        url: Optional[str] = None
        if isinstance(item, str):
            url = item
        elif isinstance(item, dict):
            url = item.get("url") or item.get("image") or item.get("src") or item.get("href")
        if not url:
            continue
        local_src: Optional[Path] = None
        if product_dir is not None:
            filename = f"image_{idx + 1}"
            local_src = _square_product_image(url, product_dir, filename)
        final_url = _relative_to_root(local_src).replace("\\", "/") if local_src else url
        output.append({
            "src": final_url,
            "position": idx,
            "alt": name,
            "origin": "prenta",
            "source_url": url,
        })
    return output


def _collect_flix_gallery_images(
    enriched: Dict[str, Any],
    product_dir: Optional[Path],
    start_position: int,
) -> List[Dict[str, Any]]:
    gallery_images: List[Dict[str, Any]] = []
    data = enriched.get("flix_tjson")
    root: Optional[Dict[str, Any]] = None
    if isinstance(data, list) and data:
        root = data[0] if isinstance(data[0], dict) else None
    elif isinstance(data, dict):
        root = data
    if not isinstance(root, dict):
        return gallery_images

    modules = root.get("modules")
    candidates: List[Dict[str, Any]] = []
    if isinstance(modules, dict):
        for value in modules.values():
            if isinstance(value, dict) and value.get("module_mapper") == "image_gallery":
                candidates.append(value)
        maybe_direct = modules.get("image_gallery")
        if isinstance(maybe_direct, dict) and maybe_direct not in candidates:
            candidates.append(maybe_direct)
    elif isinstance(modules, list):
        for entry in modules:
            if isinstance(entry, dict) and entry.get("module_mapper") == "image_gallery":
                candidates.append(entry)

    seen_urls: set[str] = set()
    for gallery in candidates:
        feature_images = gallery.get("feature_images") if isinstance(gallery, dict) else None
        if not isinstance(feature_images, list):
            continue
        for feature in feature_images:
            if not isinstance(feature, dict):
                continue
            url = None
            for key in ("img_1000_url", "img_800_url", "img_600_url", "img_400_url", "img_200_url"):
                candidate = feature.get(key)
                if candidate:
                    url = candidate
                    break
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            local_src: Optional[Path] = None
            if product_dir is not None:
                referer_hint = str(enriched.get("flixmedia_iframe_src") or enriched.get("flix_tjson_url") or "")
                local_src = _download_flix_image(url, product_dir, referer_hint, DEFAULT_IMAGE_TIMEOUT)
                if local_src is None:
                    filename = f"flix_gallery_{len(seen_urls)}"
                    local_src = _square_product_image(url, product_dir, filename)
            final_url = _relative_to_root(local_src).replace("\\", "/") if local_src else url
            alt_text = feature.get("image_text") or ""
            gallery_images.append({
                "src": final_url,
                "position": start_position + len(gallery_images),
                "alt": alt_text,
                "origin": "flix_gallery",
                "source_url": url,
            })
    return gallery_images


def map_enriched_to_grouped(
    enriched: Dict[str, Any],
    translations: Dict[str, str],
) -> Tuple[str, Dict[str, Any], bool]:
    detail = enriched.get("product") or {}
    manufacturer = (enriched.get("manufacturer") or {})
    price_info = enriched.get("price") or {}
    stock_info = enriched.get("stock") or {}

    category_path = str(enriched.get("category_path") or detail.get("category_path") or "").strip()
    translated_category = str(translations.get(category_path, "")).strip()
    group_name = translated_category or category_path or "Määramata kategooria"
    translation_missing = not bool(translated_category)

    raw_flix_clean = enriched.get("flix_description_html")
    flix_description = str(raw_flix_clean or "").strip()
    description_original = str(detail.get("description") or "")
    description_html = description_original.strip()

    short_desc_source = enriched.get("description_text") or re.sub(r"<[^>]+>", " ", (description_original or flix_description or ""))
    short_description = _build_short_description(short_desc_source)

    product_id = enriched.get("product_id")
    product_dir = _prepare_product_images_dir(product_id, clear=True)

    sku = str(detail.get("internal_reference") or enriched.get("sku") or "").strip()
    ean = str(detail.get("barcode") or enriched.get("ean") or "").strip()

    def _price_to_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(str(value).replace(",", "."))
        except Exception:
            return None

    primary_purchase_net = _price_to_float(price_info.get("price"))
    purchase_net = primary_purchase_net
    if purchase_net is None:
        purchase_net = _price_to_float(
            price_info.get("purchase_price")
            or price_info.get("purchase_net")
        )

    gross_net = _price_to_float(
        price_info.get("gross_price")
        or price_info.get("sell_price")
        or price_info.get("selling_price")
    )

    rrp_float = _price_to_float(detail.get("price_rrp"))

    vat_multiplier = 1.24
    min_margin_multiplier = 1.10

    protected_price = None
    if purchase_net is not None and purchase_net > 0:
        protected_price = purchase_net * min_margin_multiplier * vat_multiplier

    gross_candidate = None
    if gross_net is not None and gross_net > 0:
        gross_candidate = gross_net * vat_multiplier

    best_price = None
    if protected_price is not None:
        best_price = protected_price

    if rrp_float is not None:
        if protected_price is not None:
            if rrp_float >= protected_price:
                best_price = rrp_float
        elif best_price is None:
            best_price = rrp_float

    if best_price is None:
        if protected_price is not None:
            best_price = protected_price
        elif gross_candidate is not None:
            best_price = gross_candidate
        elif purchase_net is not None:
            best_price = purchase_net * vat_multiplier

    regular_price = _format_price(best_price)
    stock_qty = _to_int(stock_info.get("qty"))

    weight_val = detail.get("weight_bruto") or detail.get("weight")
    weight = _format_price(weight_val) if weight_val is not None else ""

    raw_attributes = enriched.get("attributes_kv") or {}

    def _extract_numeric_from_value(value: Any) -> str:
        if isinstance(value, list):
            for item in value:
                candidate = _extract_numeric_from_value(item)
                if candidate:
                    return candidate
            return ""
        raw = str(value or "").strip()
        if not raw:
            return ""
        match = re.search(r"[-+]?\d+(?:[.,]\d+)?", raw)
        if not match:
            return ""
        token = match.group(0).replace(",", ".")
        try:
            num_val = float(token)
        except Exception:
            num_val = None
        lowered = raw.lower()
        if num_val is not None:
            if "mm" in lowered:
                num_val = num_val / 10.0
            formatted = f"{num_val:.2f}".rstrip("0").rstrip(".")
            return formatted or "0"
        cleaned = token.rstrip(".")
        return cleaned

    def _extract_dimension_value(primary_tokens: tuple[str, ...], fallback_tokens: tuple[str, ...]) -> str:
        for key, value in raw_attributes.items():
            norm = re.sub(r"[^a-z]", "", str(key or "").lower())
            if norm in primary_tokens:
                candidate = _extract_numeric_from_value(value)
                if candidate:
                    return candidate
        for key, value in raw_attributes.items():
            norm = re.sub(r"[^a-z]", "", str(key or "").lower())
            if not norm:
                continue
            if any(token in norm for token in fallback_tokens):
                if any(discard in norm for discard in ("package", "packaging", "boxed", "shipping")):
                    continue
                candidate = _extract_numeric_from_value(value)
                if candidate:
                    return candidate
        return ""

    dimensions = {"length": "", "width": "", "height": ""}
    height_val = _extract_dimension_value(("height", "productheight", "netheight"), ("height",))
    width_val = _extract_dimension_value(("width", "productwidth", "netwidth"), ("width",))
    length_val = _extract_dimension_value(("length", "depth", "productlength", "productdepth", "netdepth", "netlength"), ("length", "depth"))
    if length_val:
        dimensions["length"] = length_val
    if width_val:
        dimensions["width"] = width_val
    if height_val:
        dimensions["height"] = height_val

    brand_name = str(manufacturer.get("name") or "").strip()
    images_prenta = _collect_images(detail, product_dir)
    images_flix_gallery = _collect_flix_gallery_images(
        enriched,
        product_dir,
        len(images_prenta),
    )
    images = images_prenta + images_flix_gallery
    attributes = _convert_attributes(raw_attributes)

    meta_data: List[Dict[str, Any]] = []
    if ean:
        meta_data.append({"key": "_bp_gtin13", "value": ean})
    meta_data.append({"key": "_bp_supplier", "value": "Prenta"})
    if brand_name:
        meta_data.append({"key": "_bp_brand", "value": brand_name})

    product_payload: Dict[str, Any] = {
        "name": detail.get("name") or enriched.get("product_list", {}).get("name") or sku,
        "type": "simple",
        "status": "publish",
        "catalog_visibility": "visible",
        "description": description_html or "",
        "description_flix": flix_description,
        "short_description": short_description,
        "sku": sku,
        "regular_price": regular_price,
        "sale_price": "",
        "tax_status": "taxable",
        "tax_class": "",
        "manage_stock": True,
        "stock_quantity": stock_qty,
        "backorders": "no",
        "backorders_allowed": False,
        "backordered": False,
        "supplier_sku_prefix": "RP-",
        "weight": weight,
        "dimensions": dimensions,
        "shipping_required": True,
        "shipping_taxable": True,
        "shipping_class": "prenta",
        "reviews_allowed": True,
        "categories": ([
            {"name": group_name, "slug": _slugify(group_name)}
        ] if group_name else []),
        "brands": ([
            {"name": brand_name, "slug": _slugify(brand_name)}
        ] if brand_name else []),
        "tags": [],
        "images": images,
        "meta_data": meta_data,
        "attributes": attributes,
        "source": {
            "prenta_product_id": enriched.get("product_id"),
            "prenta_category_path": category_path,
            "prenta_category_id": detail.get("category_id"),
            "price": price_info,
            "detail": detail,
        },
    }

    return group_name, product_payload, translation_missing


def build_grouped_products(
    enriched_products: List[Dict[str, Any]],
    translations: Dict[str, str],
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, int]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    missing_translation_count = 0
    for enriched in enriched_products:
        group_name, payload, translation_missing = map_enriched_to_grouped(enriched, translations)
        grouped[group_name].append(payload)
        if translation_missing:
            missing_translation_count += 1
    counts = {
        "categories": len(grouped),
        "products": sum(len(items) for items in grouped.values()),
        "missing_translations": missing_translation_count,
    }
    return dict(grouped), counts


def _flatten_html(html: Optional[str]) -> str:
    if not html or not isinstance(html, str):
        return ""
    return re.sub(r"<[^>]+>", "", html).strip()


def _build_category_path_list(detail: Dict[str, Any], categories_by_id: Dict[Any, Any]) -> List[str]:
    category_path_list: List[str] = []
    if (
        isinstance(detail.get("category_id"), (int, str))
        and isinstance(categories_by_id, dict)
        and categories_by_id
    ):
        cat = categories_by_id.get(detail.get("category_id")) or {}
        seen: set = set()
        cur = cat
        while isinstance(cur, dict) and cur:
            name = cur.get("name")
            if name:
                category_path_list.insert(0, name)
            parent_id = cur.get("parent_id")
            if not parent_id or parent_id in seen:
                break
            seen.add(parent_id)
            cur = categories_by_id.get(parent_id)
    return category_path_list


def enrich_product(
    client: PrentaClient,
    pid: Any,
    cfg: ClientConfig,
    base_product: Optional[Dict[str, Any]],
    price_map: Dict[Any, Dict[str, Any]],
    stock_map: Dict[Any, Dict[str, Any]],
    categories_by_id: Dict[Any, Any],
    manufacturers_by_id: Dict[Any, Any],
) -> Dict[str, Any]:
    detail = client.get_product_detail(pid)
    attrs = list(client.iter_product_attributes(pid))
    attr_vals = list(client.iter_product_attribute_values(pid))

    attr_by_id = {a.get("id"): a for a in attrs}
    attr_val_by_id = {v.get("id"): v for v in attr_vals}
    resolved: List[Dict[str, Any]] = []

    kv: Dict[str, Any] = {}
    for line in detail.get("attribute_line_ids") or []:
        aid = (line or {}).get("attribute_id")
        vid = (line or {}).get("value_id")
        a = attr_by_id.get(aid) if aid is not None else None
        v = attr_val_by_id.get(vid) if vid is not None else None
        name = (a or {}).get("name")
        atype = (v or {}).get("type") or (a or {}).get("type")
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
            kv[name] = value
        resolved.append({
            "attribute_id": aid,
            "attribute_name": name,
            "value_id": vid,
            "type": atype,
            "value": value,
            "raw_attribute": a,
            "raw_value": v,
        })

    kv_slug = {slugify_key(k): v for k, v in kv.items()}

    images = detail.get("images") or []
    primary_url: Optional[str] = None
    if isinstance(images, list) and images:
        first = images[0]
        if isinstance(first, str):
            primary_url = first
        elif isinstance(first, dict):
            primary_url = first.get("url") or first.get("image") or first.get("src")
    images_count = len(images) if isinstance(images, list) else 0

    category_path_list = _build_category_path_list(detail, categories_by_id)
    category_path = " > ".join(category_path_list) if category_path_list else (
        (categories_by_id.get(detail.get("category_id")) or {}).get("name") if isinstance(categories_by_id, dict) else None
    )

    manufacturer_info = manufacturers_by_id.get(detail.get("manufacturer_id")) if isinstance(manufacturers_by_id, dict) else None

    flix_html = None
    flix_status = None
    flix_error = None
    flix_iframe_src = None
    flix_description_html = None
    flix_tjson_url = None
    flix_tjson_status = None
    flix_tjson_data = None
    desc_flix = detail.get("description_flixmedia")
    missing_modules: List[Dict[str, Any]] = []

    if cfg.flix_render and isinstance(desc_flix, str) and desc_flix.strip():
        brand_name = (manufacturer_info or {}).get("name") if isinstance(manufacturer_info, dict) else None
        sku_val = detail.get("internal_reference") or detail.get("barcode")
        if brand_name and "data-flix-brand=\"\"" in desc_flix:
            desc_flix = re.sub(r"data-flix-brand=\"\"", f"data-flix-brand=\"{brand_name}\"", desc_flix)
        if sku_val and "data-flix-sku=\"\"" in desc_flix:
            desc_flix = re.sub(r"data-flix-sku=\"\"", f"data-flix-sku=\"{sku_val}\"", desc_flix)
        render_resp = render_flixmedia_html(desc_flix, cfg.flix_wait_selector, cfg.flix_timeout_ms, cfg.flix_origin)
        flix_html = render_resp.get("html")
        flix_status = render_resp.get("status")
        flix_error = render_resp.get("error")
        flix_iframe_src = render_resp.get("iframe_src")
        if flix_html and isinstance(flix_status, str) and flix_status.lower() == "rendered":
            flix_description_html = _process_flix_features_html(
                flix_html,
                pid,
                cfg.flix_origin,
                cfg.timeout,
            )

        attrs = _parse_snippet_attrs(desc_flix)
        dist_id = (attrs.get("distributor") or "15151").strip()
        iso = (attrs.get("language") or "en").strip()
        fl_iso = (attrs.get("fallback_language") or "lt").strip()
        ean_code = detail.get("barcode")
        sku_code = detail.get("internal_reference") or detail.get("barcode")
        if ean_code:
            origin_domain = _get_domain_from_origin(cfg.flix_origin)
            tjson_url = _build_tjson_url(str(ean_code), sku_code, dist_id, iso, fl_iso, origin_domain)
            flix_tjson_url = tjson_url
            tjson_resp = _fetch_tjson(tjson_url)
            flix_tjson_status = tjson_resp.get("status_code")
            flix_tjson_data = tjson_resp.get("json")
            data = tjson_resp.get("json")
            try:
                root = data[0] if isinstance(data, list) and data else data
                features_html: Optional[str] = None
                modules_root = (root or {}).get("modules") or {}
                key_features_html = (
                    ((modules_root.get("hotspot") or {}).get("key_features") or {}).get("html")
                    if isinstance(modules_root, dict)
                    else None
                )
                if isinstance(key_features_html, str) and key_features_html.strip():
                    if probe_build_features is not None:
                        features_html = probe_build_features(key_features_html)
                    else:
                        features_html = _build_features_html_from_key_features_html(key_features_html)

                if not features_html:
                    modules_to_parse: List[Dict[str, Any]] = []
                    if isinstance(modules_root, dict):
                        raw_features = modules_root.get("features")
                        if isinstance(raw_features, list):
                            modules_to_parse.extend([m for m in raw_features if isinstance(m, dict)])
                        for value in modules_root.values():
                            if isinstance(value, dict):
                                modules_to_parse.append(value)
                            elif isinstance(value, list):
                                modules_to_parse.extend([m for m in value if isinstance(m, dict)])
                    elif isinstance(modules_root, list):
                        modules_to_parse.extend([m for m in modules_root if isinstance(m, dict)])

                    if modules_to_parse:
                        features_html = _build_features_html_from_modules(modules_to_parse)
                        if not features_html:
                            module_summaries = [_summarize_flix_module(m) for m in modules_to_parse if isinstance(m, dict)]
                            missing_ctx: Dict[str, Any] = {
                                "modules": module_summaries,
                            }
                            if pid is not None:
                                missing_ctx["product_id"] = pid
                            if flix_tjson_url:
                                missing_ctx["tjson_url"] = flix_tjson_url
                            missing_modules.append(missing_ctx)

                if features_html:
                    flix_description_html = _process_flix_features_html(
                        features_html,
                        pid,
                        cfg.flix_origin,
                        cfg.timeout,
                    )
            except Exception:
                pass

    if missing_modules:
        enriched_missing = missing_modules
    else:
        enriched_missing = None

    enriched = {
        "product_id": pid,
        "product_list": base_product,
        "product": detail,
        "attributes_kv": kv,
        "attributes_kv_slug": kv_slug,
        "attributes": attrs,
        "attribute_values": attr_vals,
        "attributes_resolved": resolved,
        "price": price_map.get(pid),
        "stock": stock_map.get(pid),
        "category": {
            "id": detail.get("category_id"),
            "name": (categories_by_id.get(detail.get("category_id")) or {}).get("name") if isinstance(categories_by_id, dict) else None,
        },
        "manufacturer": {
            "id": detail.get("manufacturer_id"),
            "name": (manufacturer_info or {}).get("name") if isinstance(manufacturer_info, dict) else None,
        },
        "description_text": _flatten_html(detail.get("description")),
        "image_primary": primary_url,
        "images_count": images_count,
        "category_path": category_path,
        "category_hierarchy": category_path_list if category_path_list else None,
        "sku": detail.get("internal_reference"),
        "ean": detail.get("barcode"),
        "flixmedia_html": flix_html,
        "flixmedia_status": flix_status,
        "flixmedia_error": flix_error,
        "flixmedia_iframe_src": flix_iframe_src,
        "flix_description_html": flix_description_html,
        "flix_tjson_url": flix_tjson_url,
        "flix_tjson_status": flix_tjson_status,
        "flix_tjson": flix_tjson_data,
        "detail_bytes": len(json.dumps(detail, ensure_ascii=False)),
        "attrs_count": len(attrs),
        "attr_vals_count": len(attr_vals),
    }
    if enriched_missing:
        enriched["_flix_missing_modules"] = enriched_missing
    return enriched


def collect_products(cfg: ClientConfig, args: argparse.Namespace) -> Dict[str, Any]:
    client = PrentaClient(cfg)

    runlist = load_runlist()
    runlist_enabled = bool(runlist)

    list_limit = args.max_products if (args.max_products and args.max_products > 0) else None
    product_list: List[Dict[str, Any]] = []
    for item in tqdm(client.iter_products(list_limit=list_limit), desc="products list"):
        product_list.append(item)

    product_ids = [p.get("id") for p in product_list if p.get("id") is not None]

    explicit_ids: Optional[List[int]] = None
    if args.product_ids:
        try:
            explicit_ids = [int(s.strip()) for s in str(args.product_ids).split(",") if s.strip()]
        except Exception:
            raise SystemExit("--product-ids peab olema komaga eraldatud täisarvude loend")

    to_fetch: List[int] = []
    if explicit_ids:
        to_fetch = explicit_ids
    else:
        to_fetch = product_ids[: args.max_products] if (args.max_products and args.max_products > 0) else product_ids

    base_map = {int(p.get("id")): p for p in product_list if p.get("id") is not None}

    categories: List[Dict[str, Any]] = []
    manufacturers: List[Dict[str, Any]] = []
    try:
        for item in tqdm(client.iter_categories(), desc="categories"):
            categories.append(item)
    except Exception:
        pass
    try:
        for item in tqdm(client.iter_manufacturers(), desc="manufacturers"):
            manufacturers.append(item)
    except Exception:
        pass

    categories_by_id = {c.get("id"): c for c in categories if c.get("id") is not None}
    category_paths = build_category_paths(categories)
    manufacturers_by_id = {m.get("id"): m for m in manufacturers if m.get("id") is not None}

    def is_allowed_product(pid: int) -> bool:
        if explicit_ids and pid in explicit_ids:
            return True
        if not runlist_enabled:
            return True
        detail = base_map.get(pid) or {}
        path = None
        prod_category = detail.get("category_id") or (detail.get("category") or {}).get("id")
        if prod_category is not None:
            path = category_paths.get(prod_category)
        if path is None:
            path = detail.get("category_path") or detail.get("category", {}).get("path")
        return category_matches_runlist(path, runlist)

    filtered_ids: List[int] = []
    for pid in to_fetch:
        if is_allowed_product(pid):
            filtered_ids.append(pid)
    skipped_ids = [pid for pid in to_fetch if pid not in filtered_ids]

    ids_for_details: Optional[List[int]] = None
    if explicit_ids:
        ids_for_details = explicit_ids
    elif runlist_enabled:
        ids_for_details = filtered_ids
    elif list_limit:
        ids_for_details = to_fetch

    prices: List[Dict[str, Any]] = []
    if ids_for_details is None:
        for item in tqdm(client.iter_prices(), desc="prices"):
            prices.append(item)
    else:
        for pid in tqdm(ids_for_details, desc="prices (filtered)"):
            for item in client.iter_prices(product_id=pid):
                prices.append(item)

    stocks: List[Dict[str, Any]] = []
    if ids_for_details is None:
        for item in tqdm(client.iter_stock_levels(), desc="stock_levels"):
            stocks.append(item)
    else:
        for pid in tqdm(ids_for_details, desc="stock_levels (filtered)"):
            for item in client.iter_stock_levels(product_id=pid):
                stocks.append(item)

    price_map = {p.get("product_id"): p for p in prices if p.get("product_id") is not None}
    stock_map = {s.get("product_id"): s for s in stocks if s.get("product_id") is not None}

    results: List[Dict[str, Any]] = []
    missing_modules_report: List[Dict[str, Any]] = []
    missing_modules_category_counts: Dict[str, int] = defaultdict(int)
    flix_summary: Dict[str, int] = {"rendered": 0, "outer": 0, "timeout": 0, "unavailable": 0, "error": 0}

    def stock_qty_for(pid: int) -> int:
        record = stock_map.get(pid) or {}
        try:
            return int(float(str(record.get("qty") or 0)))
        except Exception:
            return 0

    stock_filtered_ids: List[int] = []
    low_stock_ids: List[int] = []
    for pid in filtered_ids:
        if stock_qty_for(pid) > 1:
            stock_filtered_ids.append(pid)
        else:
            low_stock_ids.append(pid)

    progress_desc = "enrich (filtered)" if runlist_enabled else "enrich"

    for pid in tqdm(stock_filtered_ids, desc=progress_desc):
        enriched = enrich_product(
            client=client,
            pid=pid,
            cfg=cfg,
            base_product=base_map.get(pid),
            price_map=price_map,
            stock_map=stock_map,
            categories_by_id=categories_by_id,
            manufacturers_by_id=manufacturers_by_id,
        )
        results.append(enriched)
        status = (enriched.get("flixmedia_status") or "").lower()
        if status in flix_summary:
            flix_summary[status] += 1
        missing_items = enriched.get("_flix_missing_modules") or []
        for item in missing_items:
            entry: Dict[str, Any] = {**item}
            entry.setdefault("product_id", enriched.get("product_id"))
            entry.setdefault("flixmedia_status", enriched.get("flixmedia_status"))
            entry.setdefault("has_description_html", bool(enriched.get("flix_description_html")))
            missing_modules_report.append(entry)
            for module_summary in entry.get("modules") or []:
                if not isinstance(module_summary, dict):
                    continue
                module_category = module_summary.get("module_category")
                if isinstance(module_category, str) and module_category:
                    missing_modules_category_counts[module_category] += 1

    missing_report_relpath: Optional[str] = None
    if missing_modules_report:
        missing_report_payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "count": len(missing_modules_report),
            "category_counts": dict(sorted(missing_modules_category_counts.items())),
            "items": missing_modules_report,
        }
        save_json(str(FLIX_MISSING_REPORT_PATH), missing_report_payload)
        try:
            missing_report_relpath = _relative_to_root(FLIX_MISSING_REPORT_PATH)
        except Exception:
            missing_report_relpath = str(FLIX_MISSING_REPORT_PATH)
    else:
        try:
            if FLIX_MISSING_REPORT_PATH.exists():
                FLIX_MISSING_REPORT_PATH.unlink()
        except Exception:
            pass

    translations = load_category_translations()
    ensure_processed_dir()
    grouped_products, grouped_counts = build_grouped_products(results, translations)
    grouped_path = grouped_output_path()
    save_json(str(grouped_path), grouped_products)

    feed_info = export_innpro_full_feed(results)

    counts = {
        "products_list_total": len(product_list),
        "products_enriched": len(results),
        "prices_total": len(prices),
        "stock_levels_total": len(stocks),
        "attributes_total": sum(r.get("attrs_count", 0) for r in results),
        "attribute_values_total": sum(r.get("attr_vals_count", 0) for r in results),
        "flixmedia_rendered": flix_summary.get("rendered", 0),
        "flixmedia_outer": flix_summary.get("outer", 0),
        "flixmedia_timeout": flix_summary.get("timeout", 0),
        "flixmedia_unavailable": flix_summary.get("unavailable", 0),
        "flixmedia_error": flix_summary.get("error", 0),
        "flix_missing_modules": len(missing_modules_report),
        "grouped_categories": grouped_counts.get("categories", 0),
        "grouped_products": grouped_counts.get("products", 0),
        "grouped_missing_translations": grouped_counts.get("missing_translations", 0),
        "innpro_feed_products": feed_info.get("items_total", 0),
        "innpro_source_categories": len(feed_info.get("source_categories", [])),
        "products_low_stock_skipped": len(low_stock_ids),
    }

    payload = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "base_url": cfg.base_url,
            "input": {
                "max_products": args.max_products,
                "product_ids": explicit_ids,
                "newer_than": cfg.newer_than,
                "per_page": cfg.per_page,
                "flix_render": cfg.flix_render,
            },
            "counts": counts,
            "outputs": {
                "grouped_products": str(grouped_path.relative_to(ROOT)),
                "innpro_full_feed": _relative_to_root(feed_info.get("full_path", FULL_FEED_PATH)),
                "innpro_light_feed": _relative_to_root(feed_info.get("light_path", LIGHT_FEED_PATH)),
                "innpro_unknown_report": _relative_to_root(feed_info.get("unknown_report", UNKNOWN_FIELDS_PATH)),
            },
        },
        "products": results,
    }
    if missing_report_relpath:
        payload["meta"]["outputs"]["flix_missing_modules"] = missing_report_relpath
    if runlist_enabled:
        payload["meta"].setdefault("runlist", runlist)
        payload["meta"].setdefault("counts", {})
        payload["meta"]["counts"].update({
            "products_runlist_total": len(stock_filtered_ids),
            "products_runlist_skipped": len(skipped_ids),
            "products_low_stock_skipped": len(low_stock_ids),
        })
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kogu Prenta tooteandmed ühte JSON faili")
    parser.add_argument("--base-url", default=os.getenv("PRENTA_BASE_URL", DEFAULT_BASE_URL), help="API baas-URL")
    parser.add_argument("--username", default=os.getenv("PRENTA_USERNAME", ""), help="Basic Auth kasutajanimi")
    parser.add_argument("--password", default=os.getenv("PRENTA_PASSWORD", ""), help="Basic Auth parool")
    parser.add_argument("--output-file", default=os.path.join(os.path.dirname(__file__), "data", "1_samm_algandmed.json"), help="Väljundi JSON faili tee")
    parser.add_argument("--per-page", type=int, default=int(os.getenv("PRENTA_PER_PAGE", "100")), help="Kirjete arv lehe kohta (max 100)")
    parser.add_argument("--timeout", type=int, default=int(os.getenv("PRENTA_TIMEOUT", "30")), help="HTTP timeout sekundites")
    parser.add_argument("--retries", type=int, default=int(os.getenv("PRENTA_MAX_RETRIES", "5")), help="Maksimaalsed päringu kordused")
    parser.add_argument("--newer-than", default=os.getenv("PRENTA_NEWER_THAN"), help="Valikuline ISO-8601 ajatempli filter")
    parser.add_argument("--max-products", type=int, default=int(os.getenv("PRENTA_MAX_PRODUCTS", "0")), help="Piira töödeldavate toodete arvu (0 = kõik)")
    parser.add_argument("--product-ids", default=os.getenv("PRENTA_PRODUCT_IDS"), help="Konkreetsete toote ID-de loend (komaga eraldatud)")
    verify_env = str(os.getenv("PRENTA_VERIFY_SSL", "false")).strip().lower()
    default_insecure = verify_env not in ("1", "true", "yes", "y")
    parser.add_argument(
        "--insecure",
        action="store_true",
        default=default_insecure,
        help="Lülita TLS sertifikaadi kontroll välja (vaikimisi sisse lülitatud sandboxi sertifikaadi tõrgete vältimiseks)",
    )
    parser.add_argument("--flix-timeout-ms", type=int, default=int(os.getenv("PRENTA_FLIX_TIMEOUT_MS", "45000")), help="FlixMedia renderduse timeout millisekundites")
    parser.add_argument("--flix-wait-selector", default=os.getenv("PRENTA_FLIX_WAIT_SELECTOR", "#flix-minisite,#flix-inpage"), help="CSS selektorid Flix sisu jaoks")
    parser.add_argument("--flix-origin", default=os.getenv("PRENTA_FLIX_ORIGIN", "https://e.prenta.lt/"), help="Origin URL Flix renderdamisel")
    parser.add_argument("--skip-flix", action="store_true", help="Keela FlixMedia renderdamine")
    return parser.parse_args()


def main() -> int:
    load_dotenv(find_dotenv(), override=False)
    args = parse_args()

    if not args.username or not args.password:
        print("ERROR: vaja Basic Auth mandaate (--username/--password või PRENTA_USERNAME/PRENTA_PASSWORD)", file=sys.stderr)
        return 1

    verify_ssl = not args.insecure
    if "sandbox.prenta.lt" in args.base_url:
        verify_ssl = False

    cfg = ClientConfig(
        base_url=args.base_url,
        username=args.username,
        password=args.password,
        timeout=args.timeout,
        max_retries=args.retries,
        per_page=max(1, min(args.per_page, 100)),
        newer_than=args.newer_than,
        verify_ssl=verify_ssl,
        flix_render=not args.skip_flix,
        flix_timeout_ms=args.flix_timeout_ms,
        flix_wait_selector=args.flix_wait_selector,
        flix_origin=args.flix_origin,
    )

    payload = collect_products(cfg, args)
    save_json(args.output_file, payload)

    counts = payload.get("meta", {}).get("counts", {})
    print(json.dumps({"output_file": args.output_file, "counts": counts}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
