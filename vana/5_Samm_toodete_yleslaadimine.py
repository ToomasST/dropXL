#!/usr/bin/env python3
"""
Upload ACME products with Estonian content to WooCommerce
"""

import requests
import json
import os
import time
import argparse
import re
import mimetypes
from urllib.parse import urlparse
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# Load environment variables
load_dotenv()

UPDATE_EXISTING_PRODUCTS = False

EXCLUDED_ATTR_NAMES = {
    "hs code", "hs-kood", "hskood", "hs-kood (harmonized system)",
    "minimaalne kogus jaemüügis", "minimaalne kogus hulgimüügis",
    "minimum retail quantity", "minimum wholesale quantity",
    "quantity per carton", "case pack", "carton qty"
}

class WooCommerceUploader:
    def __init__(self):
        # Prefer WP_BASE_URL if present, else WC_SITE_URL
        self.site_url = os.getenv('WP_BASE_URL') or os.getenv('WC_SITE_URL')
        # Auth can be WooCommerce consumer keys or WP App Password
        ck = os.getenv('WC_CONSUMER_KEY')
        cs = os.getenv('WC_CONSUMER_SECRET')
        if ck and cs:
            self.auth = (ck, cs)
            self.auth_mode = "wc_keys"
        else:
            print("❌ No WooCommerce credentials found. Please set WC_CONSUMER_KEY/WC_CONSUMER_SECRET or WP_USERNAME/WP_APP_PASSWORD.")
            exit(1)

    def _debug_log(self, message: str):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{ts}] {message}")

        wp_user = os.getenv('WP_USERNAME')
        wp_pwd = os.getenv('WP_APP_PASSWORD')
        self.auth = (wp_user, wp_pwd) if wp_user and wp_pwd else None
        self.auth_mode = "wp_app_pwd" if self.auth else "none"

        print(f"🔗 WooCommerce Site: {self.site_url}")
        print(f"🔐 Auth mode: {self.auth_mode}")
        
    def _norm(self, s: str) -> str:
        return (s or "").strip()
    
    # Pricing is managed elsewhere (stock runner). No pricing logic here.
    
    # We will supply images by URL via Woo payload; no local uploads here.
    
    def _wp_auth(self):
        u = os.getenv('WP_USERNAME')
        p = os.getenv('WP_APP_PASSWORD')
        return (u, p) if u and p else None

    def _slug(self, name: str) -> str:
        s = (name or '').lower()
        s = s.replace(' ', '-').replace('ä', 'a').replace('ö', 'o').replace('ü', 'u').replace('õ', 'o')
        s = ''.join(c for c in s if c.isalnum() or c == '-')
        while '--' in s:
            s = s.replace('--', '-')
        return s[:60].strip('-') or 'media'

    def _as_price_str(self, v):
        if v is None:
            return ""
        try:
            s = str(v).strip()
            s = s.replace('€', '').replace(' ', '')
            s = s.replace(',', '.')
            val = float(s)
            if val < 0:
                return ""
            return f"{val:.2f}"
        except Exception:
            return ""

    def _as_int(self, v):
        try:
            s = str(v).strip()
            s = s.replace(',', '.')
            n = int(float(s))
            return max(0, n)
        except Exception:
            return None

    def upload_image_from_url_to_media(self, url, pid: str, filename_title: str, index: int, alt_text: str = "", meta: dict | None = None):
        auth = self._wp_auth()
        if not auth:
            print("   ⚠️  WP credentials missing; cannot sideload description image")
            return None

        filename_base = f"{self._slug(filename_title)}-{pid}-{index}"
        meta = meta or {}

        try:
            parsed = urlparse(str(url))
            if parsed.scheme in ("http", "https"):
                path_ext = os.path.splitext(parsed.path)[1] or '.jpg'
                filename_guess = f"{filename_base}{path_ext}"
                existing = self.find_existing_image_by_filename(filename_guess)
                if existing:
                    return existing

                origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else (self.site_url or "")
                ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
                r = None
                for attempt in range(1, 5):
                    try:
                        headers = {
                            "User-Agent": ua,
                            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                            "Referer": origin if origin else self.site_url,
                        }
                        if attempt >= 3:
                            headers["Accept-Language"] = "et-EE,et;q=0.9,en;q=0.8"
                        r = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
                        if r.status_code == 200 and r.content:
                            break
                    except Exception:
                        pass
                    time.sleep(min(4, 0.5 * (2 ** (attempt - 1))))
                if not r or r.status_code != 200:
                    sc = getattr(r, 'status_code', 'ERR')
                    print(f"   [ERROR] Failed to fetch image: HTTP {sc} {url}")
                    return None
                content = r.content
                ctype = r.headers.get('Content-Type', 'image/jpeg').split(';')[0].strip()
                ext = mimetypes.guess_extension(ctype) or os.path.splitext(parsed.path)[1] or '.jpg'
                if not ext or len(ext) > 5:
                    ext = '.jpg'
                filename = f"{filename_base}{ext}"
                if filename != filename_guess:
                    existing2 = self.find_existing_image_by_filename(filename)
                    if existing2:
                        return existing2
                file_payload = {'file': (filename, content, ctype)}
            else:
                local_path = Path(url)
                if not local_path.is_absolute():
                    local_path = (Path(__file__).parent / local_path).resolve()
                if not local_path.exists():
                    print(f"   ❌ Local image not found: {local_path}")
                    return None
                ext = local_path.suffix or '.webp'
                if not ext:
                    ext = '.webp'
                filename = f"{filename_base}{ext}"
                existing = self.find_existing_image_by_filename(filename)
                if existing:
                    return existing
                ctype = mimetypes.guess_type(local_path.name)[0] or 'image/webp'
                file_payload = {'file': (filename, local_path.read_bytes(), ctype)}

            title_val = self._norm(meta.get('title')) or os.path.splitext(filename)[0]
            description_val = self._norm(meta.get('description')) or self._norm(meta.get('caption')) or title_val
            data = {
                'title': title_val,
                'alt_text': alt_text or title_val,
                'caption': description_val,
                'description': description_val,
            }
            print(f"   [INFO] Uploading media: {filename}")
            resp = requests.post(
                f"{self.site_url}/wp-json/wp/v2/media",
                files=file_payload,
                data=data,
                auth=auth,
                timeout=60,
            )
            if resp.status_code == 201:
                media = resp.json()
                time.sleep(0.3)
                print(f"   [OK] Uploaded media: ID {media.get('id')} '{filename}' -> {media.get('source_url')}")
                return {"id": media.get('id'), "src": media.get('source_url')}
            else:
                print(f"   [ERROR] Media upload failed: HTTP {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            print(f"   [ERROR] Error uploading inline image: {e}")
        return None

    def rewrite_description_images(self, html: str, pid: str, filename_title: str) -> str:
        if not isinstance(html, str) or '<img' not in html:
            return html if isinstance(html, str) else ''

        soup = BeautifulSoup(html, "html.parser")
        changed = False
        for idx, img in enumerate(soup.find_all("img")):
            src = self._norm(img.get("src"))
            if not src:
                continue

            alt_text = self._norm(img.get("alt"))
            meta = {
                "title": self._norm(img.get("title")),
                "caption": self._norm(img.get("data-caption"))
            }

            uploaded = self.upload_image_from_url_to_media(src, pid, f"{filename_title}-desc", idx + 1000, alt_text, meta=meta)
            if uploaded and uploaded.get("src"):
                img["src"] = uploaded["src"]
                changed = True

        return str(soup) if changed else html
    
    def find_existing_image_by_filename(self, filename):
        """Find existing WordPress media by filename to avoid duplicates"""
        url = f"{self.site_url}/wp-json/wp/v2/media"
        
        try:
            base = os.path.splitext(filename)[0]
            print(f"      [INFO] Searching WordPress media for base: '{base}' (from '{filename}')")
            response = requests.get(
                url,
                auth=(os.getenv('WP_USERNAME'), os.getenv('WP_APP_PASSWORD')),
                params={"search": base, "per_page": 10},  # search by base without extension
                timeout=30
            )
            
            if response.status_code == 200:
                media = response.json()
                print(f"      [INFO] Found {len(media)} media items in search results")
                
                # Debug: show what was found
                for i, item in enumerate(media):
                    title = item.get("title", {}).get("rendered", "")
                    slug = item.get("slug", "")
                    print(f"      [{i+1}] Title: '{title}', Slug: '{slug}'")
                
                # Look for exact filename match (handle slug/title/source_url variants)
                fn = filename.lower()
                fn_stem = Path(fn).stem.lower()
                base_lower = base.lower()
                for item in media:
                    title = (item.get("title", {}) or {}).get("rendered", "")
                    slug = item.get("slug", "") or ""
                    source_url = str(item.get("source_url") or "")

                    candidates: set[str] = set()
                    if title:
                        candidates.add(title.strip().lower())
                    if slug:
                        candidates.add(slug.strip().lower())
                    if source_url:
                        try:
                            parsed = urlparse(source_url).path
                            if parsed:
                                path_obj = Path(parsed)
                                candidates.add(path_obj.name.lower())
                                candidates.add(path_obj.stem.lower())
                        except Exception:
                            pass

                    if fn in candidates or fn_stem in candidates or base_lower in candidates:
                        print(f"      [OK] Found match! ID: {item['id']}, Title: '{title}'")
                        return {
                            "id": item["id"],
                            "src": item["source_url"],
                            "name": title,
                            "alt": item.get("alt_text")
                        }

                print(f"      [INFO] No match found for base '{base}'")
                return None
            else:
                print(f"      [ERROR] Search failed with status: {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"      [ERROR] Search error: {e}")
            return None
    
    def upload_image_to_media_library(self, image_path, ai_metadata=None, image_number=1, pid=None, estonian_content=None):
        """Upload single image to WordPress media library using AI-generated metadata"""
        url = f"{self.site_url}/wp-json/wp/v2/media"
        
        try:
            # Get WordPress credentials from environment
            wp_username = os.getenv('WP_USERNAME')
            wp_password = os.getenv('WP_APP_PASSWORD')
            
            if not wp_username or not wp_password:
                print(f"   [WARN] WordPress credentials not found in environment")
                return None
            
            # Use AI-generated metadata if available, otherwise fallback
            if ai_metadata:
                title = f"{pid} - {ai_metadata.get('title', f'Product Image {image_number}')}"
                alt_text = ai_metadata.get('alt_text', "Product Image")
                description = f"{pid} - {ai_metadata.get('description', 'Product Image')}"
                print(f"   [INFO] Using AI metadata: {alt_text[:50]}...")
            else:
                product_name = estonian_content.get('title', 'Unknown Product') if estonian_content else 'Unknown Product'
                title = f"{pid} - {product_name} - Image {image_number}"
                alt_text = f"{pid} - {product_name} - Image {image_number}"
                description = f"{pid} - {product_name} - Image {image_number}"
            
            # Generate SEO-friendly filename: Estonian product name + PID + image number
            if estonian_content and estonian_content.get('title'):
                product_name = estonian_content['title']
                # Clean product name for filename
                clean_name = product_name.lower()
                clean_name = clean_name.replace(' ', '-').replace('ä', 'a').replace('ö', 'o').replace('ü', 'u').replace('õ', 'o')
                clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '-')
                # Remove multiple dashes and limit length
                while '--' in clean_name:
                    clean_name = clean_name.replace('--', '-')
                clean_name = clean_name[:50].strip('-')
                filename = f"{clean_name}-{pid}-{image_number}.webp"
            else:
                filename = f"{pid}-image-{image_number}.webp"
            
            with open(image_path, 'rb') as img_file:
                files = {
                    'file': (filename, img_file, 'image/webp')
                }
                
                # Use WordPress Application Password authentication
                auth = (wp_username, wp_password)
                
                data = {
                    'title': title,
                    'alt_text': alt_text,
                    'description': description,
                    'caption': title
                }
                
                response = requests.post(
                    url,
                    files=files,
                    data=data,
                    auth=auth,
                    timeout=60
                )
                
                if response.status_code == 201:
                    media_data = response.json()
                    return {
                        'id': media_data.get('id'),
                        'src': media_data.get('source_url'),
                        'name': title,
                        'alt': alt_text,
                        'title': title,
                        'description': description
                    }
                else:
                    file_size = image_path.stat().st_size / (1024 * 1024)  # Size in MB
                    print(f"   ⚠️  Failed to upload {image_path.name}: HTTP {response.status_code}")
                    print(f"   📊 File size: {file_size:.1f} MB")
                    if response.status_code == 401:
                        print(f"   💡 Check WP_USERNAME and WP_APP_PASSWORD in .env file")
                    elif response.status_code == 500:
                        print(f"   💡 Server error - possibly file too large or server timeout")
                        print(f"   🔍 Response: {response.text[:200]}...")
                    else:
                        print(f"   🔍 Response: {response.text[:200]}...")
                    return None
                    
        except Exception as e:
            print(f"   ⚠️  Error uploading {image_path.name}: {str(e)}")
            return None
    
    def find_existing_product_by_sku(self, sku):
        """Find existing WooCommerce product by PID"""
        url = f"{self.site_url}/wp-json/wc/v3/products"
        
        try:
            response = requests.get(
                url,
                auth=self.auth,
                params={"sku": sku, "per_page": 1},
                timeout=120,  # Increased to 2 minutes for large products
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                products = response.json()
                return products[0] if products else None
            else:
                print(f"   ⚠️  Error searching for PID {sku}: HTTP {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"   ⚠️  Error searching for PID {sku}: {str(e)}")
            return None
    
    def update_product(self, product_id, product_payload):
        """Update an existing WooCommerce product"""
        url = f"{self.site_url}/wp-json/wc/v3/products/{product_id}"
        
        try:
            response = requests.put(
                url,
                json=product_payload,
                auth=self.auth,
                timeout=120,  # Increased to 2 minutes for large products
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                updated_product = response.json()
                return {
                    'success': True,
                    'action': 'updated',
                    'product_id': updated_product.get('id'),
                    'name': updated_product.get('name'),
                    'sku': updated_product.get('sku'),
                    'permalink': updated_product.get('permalink')
                }
            else:
                return {
                    'success': False,
                    'action': 'update_failed',
                    'error': f"HTTP {response.status_code}: {response.text}",
                    'sku': product_payload.get('sku', 'Unknown')
                }
                
        except requests.exceptions.RequestException as e:
            return {
                'success': False,
                'action': 'update_failed',
                'error': f"Request error: {str(e)}",
                'sku': product_payload.get('sku', 'Unknown')
            }
    
    def create_product(self, product_payload):
        """Create a new WooCommerce product"""
        url = f"{self.site_url}/wp-json/wc/v3/products"
        
        try:
            response = requests.post(
                url,
                json=product_payload,
                auth=self.auth,
                timeout=120,  # Increased to 2 minutes for image uploads
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 201:
                created_product = response.json()
                return {
                    'success': True,
                    'action': 'created',
                    'product_id': created_product.get('id'),
                    'name': created_product.get('name'),
                    'sku': created_product.get('sku'),
                    'permalink': created_product.get('permalink')
                }
            else:
                return {
                    'success': False,
                    'action': 'create_failed',
                    'error': f"HTTP {response.status_code}: {response.text}",
                    'sku': product_payload.get('sku', 'Unknown')
                }
                
        except requests.exceptions.RequestException as e:
            return {
                'success': False,
                'action': 'create_failed',
                'error': f"Request error: {str(e)}",
                'sku': product_payload.get('sku', 'Unknown')
            }
    
    def upload_product(self, product_payload):
        """Upload or update a product - checks for existing first"""
        sku = product_payload.get('sku', '')  # This is now PID
        
        # Check if product already exists
        existing_product = self.find_existing_product_by_sku(sku)
        
        if existing_product:
            if not UPDATE_EXISTING_PRODUCTS:
                print(f"   ⏭️ Updates disabled; skipping existing product ID {existing_product['id']}")
                return {
                    'success': True,
                    'action': 'skipped',
                    'product_id': existing_product.get('id'),
                    'name': existing_product.get('name'),
                    'sku': existing_product.get('sku'),
                    'permalink': existing_product.get('permalink')
                }
            print(f"   🔄 Product exists (ID: {existing_product['id']}), updating...")
            return self.update_product(existing_product['id'], product_payload)
        else:
            print(f"   ➕ Creating new product...")
            return self.create_product(product_payload)
            
    def create_product_payload(self, product: dict, product_exists: bool = False, status: str = "publish", update_images: bool = False, sku_override: str = None):
        """Create WooCommerce product payload from translated Innpro product data"""
        name = self._norm(product.get('name')) or self._norm(product.get('original_name') or product.get('product_name') or 'Toode')
        sku = self._norm(sku_override) if sku_override is not None else self._norm(product.get('sku'))
        raw_description = product.get('description') or ''
        raw_short = product.get('short_description') or ''
        # Prefer SEO title for filenames; fallback to product name
        seo_title_val = name
        for md in (product.get('meta_data') or []):
            if isinstance(md, dict) and md.get('key') == '_bp_seo_title' and md.get('value'):
                seo_title_val = self._norm(str(md.get('value')))
                break
        description = self.rewrite_description_images(raw_description, sku, seo_title_val)
        short_description = self.rewrite_description_images(raw_short, sku, seo_title_val)
        tax_status = product.get('tax_status') or 'taxable'
        tax_class = product.get('tax_class') or ''
        shipping_class = product.get('shipping_class') or ''

        # Filter meta_data: drop freeform block that may contain untranslated leftovers
        in_meta = product.get('meta_data') or []
        filtered_meta = []
        for m in in_meta:
            try:
                if isinstance(m, dict) and str(m.get('key')) == '_bp_attributes_freeform':
                    continue
                filtered_meta.append(m)
            except Exception:
                continue

        qa_pairs = product.get('qa') if isinstance(product.get('qa'), list) else None
        if qa_pairs:
            try:
                qa_json = json.dumps(qa_pairs, ensure_ascii=False)
            except Exception:
                qa_json = ""
            if qa_json:
                existing_qa = False
                for meta_item in filtered_meta:
                    try:
                        if isinstance(meta_item, dict) and str(meta_item.get('key')) == '_bp_qa_json':
                            meta_item['value'] = qa_json
                            existing_qa = True
                            break
                    except Exception:
                        continue
                if not existing_qa:
                    filtered_meta.append({'key': '_bp_qa_json', 'value': qa_json})

        payload = {
            "name": name,
            "type": product.get('type') or "simple",
            "status": status,
            "sku": sku,
            "description": description,
            "short_description": short_description,
            "tax_status": tax_status,
            "tax_class": tax_class,
            "categories": [],
            "tags": [],
            "attributes": [],
            "meta_data": filtered_meta,
        }
        if shipping_class:
            payload["shipping_class"] = shipping_class

        # Price mapping: prefer explicit product fields, fallback to _bp_* if present
        meta_lookup = {}
        for m in in_meta:
            if isinstance(m, dict) and m.get('key'):
                meta_lookup[str(m.get('key'))] = m.get('value')

        regular_price = product.get('regular_price') or product.get('price') or meta_lookup.get('_bp_regular_price') or meta_lookup.get('_bp_price')
        sale_price = product.get('sale_price') or meta_lookup.get('_bp_sale_price')
        rp = self._as_price_str(regular_price)
        sp = self._as_price_str(sale_price)
        if rp:
            payload['regular_price'] = rp
        if sp:
            payload['sale_price'] = sp

        # Stock mapping: prefer explicit quantity, else office stock, else supplier stock
        qty = product.get('stock_quantity')
        if qty is None:
            qty = meta_lookup.get('_bp_office_stock')
        if qty is None:
            qty = meta_lookup.get('_bp_supplier_stock')
        qty_int = self._as_int(qty)
        if qty_int is not None:
            payload['manage_stock'] = True
            payload['stock_quantity'] = qty_int
            payload['stock_status'] = 'instock' if qty_int > 0 else 'outofstock'
            payload['backorders'] = 'no'
        elif product.get('stock_status'):
            payload['stock_status'] = str(product.get('stock_status'))

        # Weight and dimensions (prefer direct fields)
        weight = self._norm(product.get('weight'))
        dims = product.get('dimensions') or {"length": "", "width": "", "height": ""}
        if weight:
            payload['weight'] = weight
        if isinstance(dims, dict):
            payload['dimensions'] = {"length": self._norm(dims.get('length')), "width": self._norm(dims.get('width')), "height": self._norm(dims.get('height'))}

        # Attributes (custom attributes)
        attrs = product.get('attributes') or []
        for a in attrs:
            try:
                nm = self._norm(a.get('name'))
                if not nm:
                    continue
                if nm.lower() in EXCLUDED_ATTR_NAMES:
                    continue
                options = None
                if isinstance(a.get('options'), list):
                    options = [self._norm(x) for x in a.get('options') if self._norm(x)]
                val = self._norm(a.get('value'))
                if not options and val:
                    options = [val]
                if not options:
                    continue
                payload['attributes'].append({
                    "name": nm,
                    "options": options,
                    "visible": True,
                    "variation": False,
                })
            except Exception:
                continue

        # Categories: map by name (prefer the last segment after ' > ')
        cats = product.get('categories') or []
        for c in cats:
            nm = self._norm((c or {}).get('name'))
            if not nm:
                continue
            last = nm.split(' > ')[-1].strip()
            cat_id = self.get_category_by_name(last)
            if cat_id:
                payload['categories'].append({"id": cat_id})
                break
        # If none matched, skip categories (or could add by name)

        # Brands: native WC brands taxonomy if available
        brands = product.get('brands') or []
        if brands:
            # pick the first brand
            bname = self._norm((brands[0] or {}).get('name'))
            if bname:
                bid = self.get_or_create_brand(bname)
                if bid:
                    payload['brands'] = [{"id": bid}]

        # Images: for new products or if update_images flag set
        if (not product_exists) or update_images:
            imgs = []
            seen_ids = set()
            raw_imgs = product.get('images') or []
            # Trust upstream processing: raw_imgs is ordered and already deduped/sized
            for idx, im in enumerate(raw_imgs):
                try:
                    src = self._norm((im or {}).get('src'))
                    if not src:
                        continue
                    filename_base = f"{self._slug(seo_title_val)}-{sku}-{idx}"
                    existing_media = self.find_existing_image_by_filename(filename_base)
                    if existing_media and existing_media.get('id') not in seen_ids:
                        imgs.append({"id": existing_media.get('id'), "position": idx})
                        seen_ids.add(existing_media.get('id'))
                        continue
                    alt = self._norm((im or {}).get('alt')) or seo_title_val
                    uploaded_media = self.upload_image_from_url_to_media(src, sku, seo_title_val, idx, alt, meta=im)
                    if uploaded_media and uploaded_media.get('id') not in seen_ids:
                        imgs.append({"id": uploaded_media.get('id'), "position": idx})
                        seen_ids.add(uploaded_media.get('id'))
                        continue
                    it = {"src": src, "position": idx}
                    if alt:
                        it['alt'] = alt
                    imgs.append(it)
                except Exception:
                    continue
            if imgs:
                payload['images'] = imgs

        return payload
    
    def upload_products_from_file(self, input_file, only_skus=None, limit: int = 0, status: str = 'publish', update_images: bool = False, dry_run: bool = False, sku_suffix: str = ""):
        """Upload products from grouped translated JSON file"""
        print(f"[INFO] Loading products from: {input_file}")
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                grouped = json.load(f)
        except Exception as e:
            print(f"❌ Error loading file: {e}")
            return

        # Flatten grouped dict {grp: [products]}
        products = []
        if isinstance(grouped, dict):
            for grp, items in grouped.items():
                for it in (items or []):
                    if isinstance(it, dict):
                        products.append(it)
        elif isinstance(grouped, list):
            products = grouped

        if only_skus:
            only = set([str(x).strip() for x in only_skus])
            products = [p for p in products if str(p.get('sku') or '').strip() in only]

        # Töötle kõige uuemad tõlked esimesena
        if products:
            products.reverse()

        if limit and limit > 0:
            products = products[:limit]

        print(f"[INFO] Starting upload of {len(products)} products to WooCommerce...")

        results = {
            'successful': [],
            'failed': [],
            'created': 0,
            'updated': 0
        }
        
        for i, product in enumerate(products, 1):
            title_dbg = (product.get('name') or product.get('original_name') or 'Unknown')[:60]
            print(f"\n[INFO] [{i}/{len(products)}] Processing: {title_dbg}...")

            sku = str(product.get('sku') or '').strip()
            if not sku:
                print("   ⚠️  Skipping - missing SKU")
                results['failed'].append({'sku': 'Unknown', 'error': 'Missing SKU'})
                continue

            upload_sku = f"{sku}{sku_suffix}" if sku_suffix else sku

            if dry_run:
                payload = self.create_product_payload(product, product_exists=False, status=status, update_images=update_images, sku_override=upload_sku)
                print(f"   📝 Title: {payload['name']}")
                print(f"   🏷️  SKU: {payload['sku']}")
                print("   🧪 Dry-run: not sending to WooCommerce")
                results['successful'].append({'success': True, 'action': 'dry_run', 'sku': upload_sku})
                continue

            existing = self.find_existing_product_by_sku(upload_sku)
            if existing and (not UPDATE_EXISTING_PRODUCTS):
                print(f"   ⏭️ Updates disabled; skipping existing product ID: {existing.get('id')}")
                results['successful'].append({'success': True, 'action': 'skipped', 'product_id': existing.get('id'), 'sku': upload_sku, 'permalink': existing.get('permalink')})
                continue
            payload = self.create_product_payload(product, product_exists=bool(existing), status=status, update_images=update_images, sku_override=upload_sku)
            print(f"   📝 Title: {payload['name']}")
            print(f"   🏷️  SKU: {payload['sku']}")

            result = self.upload_product(payload)
            
            if result['success']:
                action = result.get('action', 'unknown')
                if action == 'created':
                    print(f"   ✅ Created! Product ID: {result['product_id']}")
                    results['created'] += 1
                elif action == 'updated':
                    print(f"   ✅ Updated! Product ID: {result['product_id']}")
                    results['updated'] += 1
                
                print(f"   🔗 URL: {result['permalink']}")
                results['successful'].append(result)
            else:
                print(f"   ❌ Failed: {result['error']}")
                results['failed'].append(result)
            
            # Rate limiting - be nice to the API
            if i < len(products):
                print("   ⏳ Waiting 2 seconds...")
                time.sleep(2)
        
        # Summary
        print(f"\n📊 Upload Summary:")
        print(f"   ✅ Successful: {len(results['successful'])}")
        print(f"   ➕ Created: {results['created']}")
        print(f"   🔄 Updated: {results['updated']}")
        print(f"   ❌ Failed: {len(results['failed'])}")
        
        # Save results
        results_file = f"woocommerce_upload_results_{int(time.time())}.json"
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"   💾 Results saved to: {results_file}")
        
        if results['failed']:
            print(f"\n❌ Failed uploads:")
            for failure in results['failed']:
                print(f"   - SKU {failure['sku']}: {failure['error']}")

    def ensure_attributes_exist(self, attribute_names):
        """Ensure that all given attribute names exist in WooCommerce."""
        print(f"🔍 Checking {len(attribute_names)} attributes in WooCommerce...")
        
        # Get existing attributes
        try:
            response = requests.get(
                f"{self.site_url}/wp-json/wc/v3/products/attributes",
                auth=self.auth,
                params={'per_page': 100}
            )
            if response.status_code == 200:
                existing_attrs = response.json()
                existing_names = {attr['name'] for attr in existing_attrs}
                print(f"✅ Found {len(existing_attrs)} existing attributes")
            else:
                print(f"⚠️  Could not fetch attributes: {response.status_code}")
                existing_names = set()
                existing_attrs = []
        except Exception as e:
            print(f"❌ Error fetching attributes: {e}")
            existing_names = set()
            existing_attrs = []
        
        # Create missing attributes
        created_count = 0
        for attr_name in attribute_names:
            if attr_name not in existing_names:
                try:
                    new_attr = {
                        "name": attr_name,
                        "slug": attr_name.lower().replace(' ', '-').replace('(', '').replace(')', ''),
                        "type": "select",
                        "has_archives": False
                    }
                    
                    response = requests.post(
                        f"{self.site_url}/wp-json/wc/v3/products/attributes",
                        auth=self.auth,
                        json=new_attr
                    )
                    
                    if response.status_code == 201:
                        created_attr = response.json()
                        existing_attrs.append(created_attr)
                        existing_names.add(attr_name)
                        created_count += 1
                        print(f"   ✅ Created attribute: {attr_name}")
                    else:
                        print(f"   ❌ Failed to create attribute '{attr_name}': {response.status_code}")
                        print(f"      Response: {response.text}")
                except Exception as e:
                    print(f"   ❌ Error creating attribute '{attr_name}': {e}")
        
        if created_count > 0:
            print(f"🆕 Created {created_count} new attributes")
        else:
            print("✅ All attributes already exist")
        
        return existing_attrs

    def get_existing_attributes(self):
        """Fetch existing WooCommerce product attributes."""
        print("🔍 Fetching existing WooCommerce product attributes...")
        
        try:
            response = requests.get(
                f"{self.site_url}/wp-json/wc/v3/products/attributes",
                auth=self.auth,
                timeout=30
            )
            
            if response.status_code == 200:
                attributes = response.json()
                print(f"   ✅ Found {len(attributes)} existing attributes")
                
                # Create a lookup dict by name and slug
                attr_lookup = {}
                for attr in attributes:
                    attr_lookup[attr['name']] = attr
                    attr_lookup[attr['slug']] = attr
                
                return attr_lookup
            else:
                print(f"   ❌ Error fetching attributes: {response.status_code}")
                return {}
                
        except Exception as e:
            print(f"   ❌ Error fetching attributes: {e}")
            return {}
    
    def get_category_by_name(self, category_name):
        """Find existing category by name and return category ID. Does not create new categories."""
        if not category_name or category_name.strip() == "":
            return None
            
        try:
            # Search for existing category
            response = requests.get(
                f"{self.site_url}/wp-json/wc/v3/products/categories",
                auth=self.auth,
                params={"search": category_name, "per_page": 10},
                timeout=30
            )
            
            if response.status_code == 200:
                categories = response.json()
                # Look for exact match
                for cat in categories:
                    if cat['name'].lower().strip() == category_name.lower().strip():
                        print(f"   📁 Found existing category: {cat['name']} (ID: {cat['id']})")
                        return cat['id']
                
                # No exact match found
                print(f"   ⚠️  Category '{category_name}' not found in WooCommerce")
                return None
                    
        except Exception as e:
            print(f"   ❌ Error searching for category {category_name}: {e}")
            
        return None

    def get_or_create_brand(self, brand_name):
        """Get or create a brand by name and return the brand ID."""
        try:
            # Search for existing brand
            response = requests.get(
                f"{self.site_url}/wp-json/wc/v3/products/brands",
                auth=self.auth,
                params={"search": brand_name, "per_page": 10},
                timeout=30
            )
            
            if response.status_code == 200:
                brands = response.json()
                # Look for exact match
                for brand in brands:
                    if brand['name'].lower().strip() == brand_name.lower().strip():
                        print(f"   📁 Found existing brand: {brand['name']} (ID: {brand['id']})")
                        return brand['id']
                
                # No exact match found - create new brand
                print(f"   ➕ Creating new brand: {brand_name}")
                payload = {
                    "name": brand_name
                }
                response = requests.post(
                    f"{self.site_url}/wp-json/wc/v3/products/brands",
                    json=payload,
                    auth=self.auth,
                    timeout=30
                )
                
                if response.status_code == 201:
                    brand_data = response.json()
                    print(f"   ✅ Created brand: {brand_name} (ID: {brand_data['id']})")
                    return brand_data['id']
                else:
                    print(f"   ❌ Error creating brand: {response.status_code}")
                    return None
                    
        except Exception as e:
            print(f"   ❌ Error searching for brand {brand_name}: {e}")
            
        return None

def main():
    """Main function"""
    print("=== WooCommerce Product Uploader ===")

    parser = argparse.ArgumentParser(description="Upload translated Innpro products to WooCommerce")
    default_input = Path(__file__).parent / "data" / "tõlgitud" / "products_translated_grouped.json"
    parser.add_argument("--input", default=str(default_input), help="Path to grouped translated products JSON")
    parser.add_argument("--only-sku", action="append", default=[], help="Process only these SKUs (can repeat or comma-separated)")
    parser.add_argument("--limit", type=int, default=0, help="Max number of products to upload")
    parser.add_argument("--status", default="publish", choices=["publish", "draft"], help="Product publish status")
    parser.add_argument("--update-images", action="store_true", help="Update images for existing products as well")
    parser.add_argument("--dry-run", action="store_true", help="Do not perform write calls; only print")
    parser.add_argument("--sku-suffix", nargs='?', const='-N', default="", help="Suffix to append to SKU for upload (e.g., -N). If provided without a value, defaults to '-N'.")
    parser.add_argument("--update-existing", action="store_true", help="Update products already existing in WooCommerce")

    args = parser.parse_args()

    global UPDATE_EXISTING_PRODUCTS
    UPDATE_EXISTING_PRODUCTS = args.update_existing

    # Normalize only-sku list
    only_skus = []
    for token in args.only_sku or []:
        for part in str(token).split(','):
            part = part.strip()
            if part:
                only_skus.append(part)

    uploader = WooCommerceUploader()
    uploader.upload_products_from_file(
        input_file=args.input,
        only_skus=only_skus,
        limit=args.limit,
        status=args.status,
        update_images=args.update_images,
        dry_run=args.dry_run,
        sku_suffix=args.sku_suffix,
    )

if __name__ == "__main__":
    main()
