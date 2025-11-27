import os
import sys
import json
import argparse
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

import requests


def log(msg: str) -> None:
    from datetime import datetime
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def join_url(base: str, path: str) -> str:
    if base.endswith('/'):
        base = base[:-1]
    if not path.startswith('/'):
        path = '/' + path
    return base + path


def slugify(name: str) -> str:
    """Generate a URL-friendly slug for WooCommerce category creation."""
    norm = unicodedata.normalize('NFKD', name or '').encode('ascii', 'ignore').decode('ascii')
    norm = norm.lower()
    norm = re.sub(r'[^a-z0-9]+', '-', norm)
    norm = norm.strip('-')
    return norm[:45] or 'category'


def fetch_wp_product_categories(base_url: str, username: str, app_password: str) -> List[Dict[str, Any]]:
    per_page = 100
    page = 1
    out: List[Dict[str, Any]] = []
    url = join_url(base_url, '/wp-json/wp/v2/product_cat')
    while True:
        r = requests.get(url, params={'per_page': per_page, 'page': page}, auth=(username, app_password), timeout=60)
        if r.status_code == 400 and 'rest_post_invalid_page_number' in r.text:
            break
        r.raise_for_status()
        items = r.json()
        if not isinstance(items, list) or not items:
            break
        out.extend(items)
        if len(items) < per_page:
            break
        page += 1
    return out


def build_full_paths(terms: List[Dict[str, Any]]) -> Tuple[Dict[int, Dict[str, Any]], List[Dict[str, Any]]]:
    by_id: Dict[int, Dict[str, Any]] = {}
    for t in terms:
        try:
            by_id[int(t.get('id'))] = t
        except Exception:
            continue

    def full_path(term: Dict[str, Any]) -> str:
        names: List[str] = []
        seen: set[int] = set()
        cur = term
        while True:
            try:
                tid = int(cur.get('id'))
            except Exception:
                break
            if tid in seen:
                break
            seen.add(tid)
            names.append(str(cur.get('name') or '').strip())
            pid = cur.get('parent')
            if not pid:
                break
            cur = by_id.get(int(pid)) or {}
            if not cur:
                break
        names.reverse()
        return ' > '.join([n for n in names if n])

    enriched: List[Dict[str, Any]] = []
    for t in terms:
        fp = full_path(t)
        t2 = dict(t)
        t2['full_path'] = fp
        enriched.append(t2)
    return by_id, enriched


def split_path(p: str) -> List[str]:
    return [s.strip() for s in p.split('>') if s.strip()]


def normalize_prefix(raw: str) -> str:
    raw = (raw or '').strip()
    if not raw:
        return ''
    if '>' in raw:
        # assume already in "A > B" format
        return ' > '.join([part.strip() for part in raw.split('>') if part.strip()])
    parts = [part.strip() for part in raw.replace('\\', '/').split('/') if part.strip()]
    return ' > '.join(parts)


def build_indexes_for_levels(enriched: List[Dict[str, Any]]) -> Tuple[Dict[str, int], Dict[str, List[str]]]:
    # full_path -> id
    by_full_path: Dict[str, int] = {}
    # leaf name -> list of full paths where this leaf occurs
    by_leaf_name: Dict[str, List[str]] = {}
    for t in enriched:
        fp = str(t.get('full_path') or '').strip()
        if not fp:
            continue
        try:
            by_full_path[fp] = int(t.get('id'))
        except Exception:
            continue
        parts = split_path(fp)
        if parts:
            leaf = parts[-1]
            by_leaf_name.setdefault(leaf, []).append(fp)
    return by_full_path, by_leaf_name


def create_category(base_url: str, username: str, app_password: str, name: str, parent_id: int) -> Tuple[bool, int]:
    payload: Dict[str, Any] = {"name": name, "slug": slugify(name)}
    if parent_id:
        payload["parent"] = parent_id
    url = join_url(base_url, '/wp-json/wc/v3/products/categories')
    try:
        resp = requests.post(url, auth=(username, app_password), json=payload, timeout=60)
    except Exception as exc:
        log(f"❌ Kategooria '{name}' loomine ebaõnnestus: {exc}")
        return False, 0
    if resp.status_code == 201:
        data = resp.json()
        cid = int(data.get('id') or 0)
        log(f"✅ Loodud kategooria: {name} (ID {cid})")
        return True, cid
    if resp.status_code == 400:
        try:
            data = resp.json()
            existing_id = int((data.get('data') or {}).get('resource_id') or 0)
            if existing_id:
                log(f"ℹ️ Kategooria '{name}' juba olemas (ID {existing_id})")
                return True, existing_id
        except Exception:
            pass
        log(f"❌ WooCommerce tagastas 400 kategooria '{name}' jaoks: {resp.text[:200]}")
        return False, 0
    log(f"❌ WooCommerce viga kategooria '{name}' loomisel: HTTP {resp.status_code} {resp.text[:200]}")
    return False, 0


def ensure_category_path(base_url: str, username: str, app_password: str, full_path: str, existing: Dict[str, int]) -> bool:
    parts = split_path(full_path)
    if not parts:
        return True
    current_path: List[str] = []
    parent_id = 0
    for segment in parts:
        current_path.append(segment)
        joined = ' > '.join(current_path)
        existing_id = existing.get(joined)
        if existing_id:
            parent_id = existing_id
            continue
        ok, new_id = create_category(base_url, username, app_password, segment, parent_id)
        if not ok or not new_id:
            return False
        existing[joined] = new_id
        parent_id = new_id
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description='Kontrolli kategooriate vastavust WP-le')
    parser.add_argument('--source-prefix', default='', help='Filtreeri category_translation.json kirjed lähteprefiksi järgi (nt "Pets/")')
    parser.add_argument('--print-ok', action='store_true', help='Trüki ka korras rajad üksikasjalikus režiimis')
    args = parser.parse_args()
    base = Path(__file__).parent
    map_path = base / 'category_translation.json'
    out_dir = base / 'data' / 'processed'
    out_dir.mkdir(parents=True, exist_ok=True)

    wp_base_url = os.getenv('WP_BASE_URL') or os.getenv('WP_URL') or ''
    wp_user = os.getenv('WP_USERNAME') or ''
    wp_app_pass = os.getenv('WP_APP_PASSWORD') or ''

    missing_env = []
    if not wp_base_url:
        missing_env.append('WP_BASE_URL')
    if not wp_user:
        missing_env.append('WP_USERNAME')
    if not wp_app_pass:
        missing_env.append('WP_APP_PASSWORD')
    if missing_env:
        log('❌ Puuduvad .env muutujad: ' + ', '.join(missing_env))
        log('Lisa need .env faili (nt WP_BASE_URL=https://ruumistruumi.ee) ja proovi uuesti.')
        return 2

    try:
        translations: Dict[str, str] = {}
        if map_path.exists():
            translations = json.load(map_path.open('r', encoding='utf-8'))
        else:
            translations = {}
    except Exception as e:
        log(f'❌ Ei suutnud lugeda category_translation.json: {e}')
        return 3

    log('Laen WP tootekategooriaid …')
    try:
        terms = fetch_wp_product_categories(wp_base_url, wp_user, wp_app_pass)
    except Exception as e:
        log(f'❌ WP päring ebaõnnestus: {e}')
        return 4

    _, enriched = build_full_paths(terms)
    all_paths = {t['full_path']: int(t['id']) for t in enriched if t.get('full_path')}
    with (out_dir / 'WP_CATEGORIES.json').open('w', encoding='utf-8') as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)

    # Build sorted reverse indexes (omit by_name to keep file minimal)
    by_slug = {k: v for k, v in sorted(
        ((str(t.get('slug') or ''), int(t['id'])) for t in enriched if t.get('slug')),
        key=lambda kv: kv[0].lower()
    )}
    by_full_path_sorted = {k: all_paths[k] for k in sorted(all_paths.keys(), key=lambda s: s.lower())}
    with (out_dir / 'WP_CATEGORIES_INDEX.json').open('w', encoding='utf-8') as f:
        json.dump({'by_full_path': by_full_path_sorted, 'by_slug': by_slug}, f, ensure_ascii=False, indent=2)

    # Load optional runlist file (source prefixes)
    runlist_path = base / 'category_runlist.json'
    run_prefixes: List[str] = []
    if runlist_path.exists():
        try:
            run_prefixes = json.load(runlist_path.open('r', encoding='utf-8'))
            if not isinstance(run_prefixes, list):
                run_prefixes = []
        except Exception:
            run_prefixes = []
    run_prefixes = [normalize_prefix(prefix) for prefix in run_prefixes if normalize_prefix(prefix)]

    # Detailed level-by-level validation for a subset (from runlist or CLI)
    cli_prefix = normalize_prefix(args.source_prefix)
    prefixes = run_prefixes if run_prefixes else ([cli_prefix] if cli_prefix else [])
    if prefixes:
        by_full_path, by_leaf_name = build_indexes_for_levels(enriched)
        global_ok_total = 0
        global_bad_total = 0
        for prefix in prefixes:
            subset = {src: val for src, val in translations.items() if src.startswith(prefix)}
            if not subset:
                log(f'⚠️ Lähteprefiksiga "{prefix}" ei leitud ühtegi kirjet category_translation.json failis.')
                continue
            log(f'Detailne kontroll prefiksi "{prefix}" kohta (level-by-level):')
            ok_total = 0
            bad_total = 0
            missing_paths: List[str] = []
            for src, translated in sorted(subset.items(), key=lambda kv: kv[0].lower()):
                translated = str(translated or '').strip()
                if not translated:
                    log(f' - {src} → (tõlge puudu)')
                    bad_total += 1
                    continue
                parts = split_path(translated)
                chain_ok = True
                cur_path = ''
                log(f'⟹ {src} → {translated}')
                for i, seg in enumerate(parts):
                    cur_path = seg if i == 0 else f'{cur_path} > {seg}'
                    exists = cur_path in by_full_path
                    if exists:
                        if args.print_ok:
                            log(f'   ✓ tase {i+1}: {cur_path}')
                    else:
                        chain_ok = False
                        # suggestions: where does this leaf exist (if anywhere)
                        alts = by_leaf_name.get(seg, [])
                        log(f'   ✗ tase {i+1} puudub: {cur_path}')
                        if alts:
                            sample = ', '.join(alts[:3]) + (' …' if len(alts) > 3 else '')
                            log(f'     vihje: leiti nimi "{seg}" mujal asukohas: {sample}')
                        break
                if chain_ok:
                    ok_total += 1
                else:
                    bad_total += 1
                    missing_paths.append(translated)
            log(f'Kokku: OK={ok_total}, puudulikke={bad_total}')

            global_ok_total += ok_total
            global_bad_total += bad_total

            if missing_paths:
                try:
                    answer = input("Kas soovid WooCommerce'is puuduvad kategooriad luua? (jah/ei) ").strip().lower()
                except KeyboardInterrupt:
                    log("Katkestatud kasutaja poolt.")
                    return 5
                if answer in ('jah', 'j', 'yes', 'y'):
                    success = True
                    for path in sorted(set(missing_paths)):
                        if ensure_category_path(wp_base_url, wp_user, wp_app_pass, path, all_paths):
                            log(f"✔ Kategooriarada tagatud: {path}")
                        else:
                            log(f"❌ Kategooria '{path}' loomine ebaõnnestus.")
                            success = False
                    if not success:
                        log('⚠️ Mõned kategooriad jäid loomata. Kontrolli logi ja proovi vajadusel uuesti.')
                        return 5
                    log('✅ Puuduvad kategooriad loodud. Käivita skript uuesti, kui soovid kontrolli kinnitada.')
                else:
                    log('ℹ️ Kategooriaid ei loodud. Paranda need käsitsi ja käivita vajadusel uuesti.')

        log(f'Üldkokkuvõte: OK={global_ok_total}, puudulikke={global_bad_total}')

        # Do not fail in runlist/filtered mode
        return 0

    # Validate that every translated target in mapping exists in WP (global check)
    missing: List[str] = []
    for src, translated in translations.items():
        translated = str(translated or '').strip()
        if not translated:
            continue
        if translated not in all_paths:
            missing.append(translated)

    if missing:
        rep = out_dir / 'CATEGORY_TRANSLATION_MISMATCH.txt'
        rep.write_text('\n'.join(sorted(set(missing))), encoding='utf-8')
        log('❌ Mõned tõlgitud kategooria-rajad ei leidu WP-s.')
        for p in missing[:20]:
            log(f' - {p}')
        if len(missing) > 20:
            log(f' … ja veel {len(missing)-20} kirjet')

        try:
            answer = input("Kas soovid WooCommerce'is puuduvad kategooriad luua? (jah/ei) ").strip().lower()
        except KeyboardInterrupt:
            log("Katkestatud kasutaja poolt.")
            return 5
        if answer not in ('jah', 'j', 'yes', 'y'):
            log('ℹ️ Kategooriaid ei loodud. Paranda need käsitsi ja käivita uuesti.')
            return 5

        success = True
        for path in sorted(set(missing)):
            if ensure_category_path(wp_base_url, wp_user, wp_app_pass, path, all_paths):
                log(f"✔ Kategooriarada tagatud: {path}")
            else:
                log(f"❌ Kategooria '{path}' loomine ebaõnnestus.")
                success = False
        if not success:
            log('⚠️ Mõned kategooriad jäid loomata. Kontrolli logi ja proovi vajadusel uuesti.')
            return 5
        log('✅ Puuduvad kategooriad loodud. Käivita skript uuesti, kui soovid kontrolli kinnitada.')
        return 0

    log('✔ Kõik tõlked vastavad WP kategooriatele.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
