# Workflow Guide

Guidelines for onboarding a new supplier and running the full enrichment pipeline.

---

## 1. Overview & Prerequisites

- **Python environment:** Python 3.11+, virtualenv recommended. Install project requirements (`pip install -r requirements.txt`).
- **External services:**
  - Supplier API credentials (REST or export access).
  - FlixMedia/HTML source access if descriptions are rendered externally.
  - OpenAI API key in `.env` for translation/LLM prompts.
  - WooCommerce API/App Password for upload.
- **Tooling:** Playwright (if Stage 1 fetches live HTML), imagemagick/webp tools for image conversion as required by supplier.
- **Directory layout:**
  - `data/` — raw and processed artifacts.
  - `data/tõlgitud/` — Stage 4 translated grouped JSON.
  - `product_images/` — per-product image folders created by Stage 1.
  - `tools/` — helper utilities (nt Flix debug).
  - Stage scripts `0_Samm_…` → `5_Samm_…` controlling the pipeline.
- **Category formats:**
  - `category_runlist.json` **must** store entries in `"Level 1 > Level 2"` form. Scripts normalize internally (@MEMORY: runlist format uses translated-style ` > ` separators).
  - `category_translation.json` keys follow the same format.

---

## 2. Supplier Onboarding Checklist

Before running Stage 1 for a new supplier, confirm:

1. **API/Auth:** Endpoints reachable, tokens stored in `.env` or config file.
2. **Category hierarchy:** Exported and imported via `0_Samm_kategooriate_inventuur.py` (creates or updates translation keys, ensures runlist exists).
3. **Product essentials:** SKU, prices (net/gross), stock quantities, weight, dimensions, tax class info.
4. **Brand & attribute mapping:** Identify manufacturer field, attribute names requiring canonicalisation.
5. **Media:** Download URLs for product imagery, confirm format conversions (.webp) and storage under `product_images/<product_id>/`.
6. **Descriptions:** Source of content (Flix TJSON, HTML, plain text). Kinnita, et algkirjelduse HTML (koos `<img>` elementidega) jõuab Samm 1 raw-andmetena Step 2 sisendisse.
7. **Step 2 meta andmed:** Loo plaan, millised väljad lähevad Step 2 `source` plokki (nt `source_system`, `source_product_id`, `source_category_ids`, `purchase_price`, `rrp_price`).
8. **Default behaviour:** Choose default SKU prefix, tax/shipping defaults, product type (simple/grouped/variable).
9. **Runlist & translation:** Populate `category_translation.json` for all targeted paths; add relevant entries to `category_runlist.json` in the `"A > B"` pattern.

---

## 3. Stage 1 — Data Collection (`1_Samm_alg_andmete_kogumine.py`)

- **Purpose:** Fetch supplier catalogue, apply runlist + stock filters, enrich with basic meta (brand, category, attributes) and download imagery.
- **Key inputs:** optional runlist filter (`category_runlist.json`), CLI flags (`--max-products`, `--product-ids`, Flix toggle options).
- **Outputs:**
  - `1_samm_raw_data.json` — consolidated raw/enriched products list (Step 2 sisend).
  - `product_images/<product_id>/…` — downloaded product media.
  - vajadusel Flix-related artifacts (kui Step 1 on laiemaks kasutuseks kohandatud).
- **Supplier-specific customisations:**
  - API client implementation (authentication, pagination, expansions).
  - Field mapping for weights/dimensions/attributes (ensuring units and canonical names).
- **Sanity checks:**
  - Confirm runlist filter behaves as expected (log selected product IDs/counts).
  - Confirm imagery saved in expected `product_images/<id>/` location and format.

---

## 4. Stage 2 — Product Info (Step 2 schema) (`2_Samm_tooteinfo.py`)

Stage 2 ehitab **universaalse Step 2 skeemi** faili `2_samm_tooteinfo.json`, mida kasutavad sammud 3–5.

### 4.1 Pipeline behaviour

- Loeb `1_samm_raw_data.json` Sammust 1.
- Loogika on tarnija-spetsiifiline: convertib toorandmed Step 2 väljadeks.
- Tagab, et iga toote objekt vastab Step 2 lepingule (vt README „Step 2 skeem“ ja `2_samm_tooteinfo.json` näidet).

### 4.2 Step 2 contract (kokkuvõte)

Olulisemad väljad, mida Sammud 3–5 eeldavad:

- Identiteet:
  - `sku` – poe SKU.
  - `global_unique_id` – nt EAN (kui olemas).
- Allika meta:
  - `source.source_system`, `source.source_product_id`, `source.source_category_ids`.
  - hinnainfo `source.purchase_price`, `source.rrp_price` (kasutatakse Sammus 5 hinnareeglis).
- Põhiandmed:
  - `name`, `short_description`, `description` (HTML koos `<img>` elementidega).
- Laoväljad ja logistika:
  - `manage_stock`, `stock_status`, `stock_quantity`, `weight`, `dimensions{length,width,height}`, `shipping_required`, `shipping_taxable`, `shipping_class` jne.
- Taksonoomiad ja bränd:
  - `category{source_id,path,translated_path,leaf_name}`.
  - `categories[]` minimaalsete kategoorianimedega Woo jaoks.
  - `brands[]` brändinimedega.
- Pildid ja atribuudid:
  - `images[]` – lokaalsed pildirajad + algne alt.
  - `attributes[]` massiivist `{name, values[]}` — see struktuur on kriitiline Samm 4/7 jaoks.

### 4.3 Validation checklist

- Käivita Samm 2 väikese `1_samm_raw_data.json` alamsetiga ja vaata `2_samm_tooteinfo.json` näidiseid.
- Kontrolli, et kõik vajalikud Step 2 väljad on olemas (eriti `sku`, kategooria, pildid, attributes, hinnainfo ja laoväljad).
- Veendu, et `category.translated_path` väärtused klapivad `category_translation.json` ja `category_runlist.json` võtmetega.

---

## 5. Stage 3 — Category Control & Sync (`3_Samm_kategooriate_kontroll_ja_sync.py`)

- **Purpose:** Compare supplier categories vs Woo taxonomy, create missing branches, ensure translations exist.
- **Inputs:** `category_translation.json`, runlist, Woo credentials.
- **Usage:**
  - `python 3_Samm_kategooriate_kontroll_ja_sync.py --dry-run`
  - Optionally `--source-prefix "All > …"` for targeted audit.
- **What to watch:**
  - Log warnings for missing translation entries—update JSON before proceeding.
  - If Woo returns 429, script automatically retries; monitor logs.
  - Keep runlist/tõlkeformaadid aligned (same normalisation as Stage 2 & 4).

---

## 6. Stage 4 — Translation (`4_Samm_toodete_tolkimine.py`)

- **Purpose:** Translate descriptions, short descriptions, attributes, ALT texts; optionally perform contextual enrichment.
- **Inputs:**
  - `2_samm_tooteinfo.json` (Step 2 skeem).
  - `category_runlist.json` (filtreerib lähtekategooriad).
- **Behaviour:**
  - Skips SKUs already present in `data/tõlgitud/products_translated_grouped.json`.
  - Skips SKUs already in WooCommerce (cached automatically).
  - Respects `--only-sku`, `--limit`, `--dry-run` flags.
  - `normalize_prefix` ensures runlist entries match `source.prenta_category_path`.
  - Duplicate EAN-id logitakse faili `data/logs/ean_conflicts_<timestamp>.csv`, et otsustada, kas olemasolevad tooted tuleb eemaldada või uuesti tõlkida.
- **Requirements:** `_bp_gtin13`, brand info, consistent meta—if missing, translations degrade (and logs warn).
- **Outputs:** Updated `data/tõlgitud/products_translated_grouped.json` and per-product debug traces under `data/debug_traces/`.

---

## 7. Stage 5 — WooCommerce Upload (`5_Samm_toodete_yleslaadimine.py`)

- **Purpose:** Create/update Woo products using translated content.
- **Inputs:** `data/tõlgitud/products_translated_grouped.json` (default auto-resolves using `Path(__file__).parent`).
- **CLI flags:** `--dry-run`, `--only-sku`, `--limit`, `--status`, `--sku-suffix`, `--update-images`.
- **Process:** Handles brand lookup/creation, image upload, meta & attribute syncing.
- **Recommendations:**
  - Perform `--dry-run` first to inspect payloads.
  - For suppliers sharing SKUs with existing catalogue, use `--sku-suffix` or filter SKUs explicitly.
  - Monitor for Woo 429 responses; script logs and retries.
  - Kirjeldustes olevad pildid (sh kohalikud ja SVG-failid) laaditakse uuesti meediasse ning `<img>` `src`-id uuendatakse automaatselt.
  - Stage 1 paneb juba `shipping_class="prenta"`; Stage 5 ei saadaks tühja klassi üles.
- **Pricing margin logic:** Calculates `regular_price` as the maximum of RRP (including VAT) or at least the purchase price × 1.10 × 1.24, ensuring a ≥10% margin before VAT.

---

## 8. Helper Tools (`tools/`)

| Script | When to use |
| --- | --- |
| `tools/flix_audit.py` | Analyse unknown Flix modules and coverage gaps. |
| `tools/flix_rerender_missing.py` | Rebuild fallback Flix HTML for products missing descriptions. |
| `tools/patch_grouped_flix.py` | Patch Stage 2 grouped data from Stage 1 enriched HTML without rerunning entire pipeline. |

Best practice: run audits when new supplier modules appear or after Stage 1 refactor. Patch script helps retroactively sync Stage 2 output after Stage 1 improvements.

---

## 9. Troubleshooting & Logs

- **Logs:**
  - Stage scripts append to `data/logs/run_<timestamp>.log`.
  - Detailed per-SKU payloads under `data/debug_traces/<sku>/`.
- **Common issues:**
  - **Runlist mismatch:** Ensure entries use `" > "` separators; script normalisation handles slashes but prefer clean input.
  - **Missing translations:** Stage 2 summary lists categories lacking translations—update `category_translation.json` before continuing.
  - **Flix gaps:** Use audit + rerender tool; if still empty, confirm supplier actually provides modules (some respond with `match_failed`).
  - **Woo rate limits (429):** Scripts back off automatically; rerun with `--limit` for incremental uploads.
- **Sanity check routine:**
  1. Inspect random product in grouped JSON for mandatory fields.
  2. Verify imagery exists on disk.
  3. Run Stage 3 dry-run for categories.
  4. Execute Stage 4 for single SKU (`--only-sku`) and inspect debug JSON.

---

## 10. CLI Reference

| Stage | Command example | Key flags |
| --- | --- | --- |
| Stage 0 | `python 0_Samm_kategooriate_inventuur.py --dry-run` | `--dry-run`, supplier-specific auth |
| Stage 1 | `python 1_Samm_alg_andmete_kogumine.py --max-products 50` | `--max-products`, `--product-ids`, Flix toggles |
| Stage 2 | `python 2_Samm_tooteinfo.py` | tarnija-spetsiifiline Step 2 maping |
| Stage 3 | `python 3_Samm_kategooriate_kontroll_ja_sync.py --dry-run` | `--dry-run`, `--source-prefix` |
| Stage 4 | `python 4_Samm_toodete_tolkimine.py --only-sku ABC123 --dry-run` | `--only-sku`, `--limit`, `--dry-run` |
| Stage 5 | `python 5_Samm_toodete_yleslaadimine.py --dry-run --limit 10` | `--dry-run`, `--status`, `--sku-suffix`, `--update-images` |

Consider maintaining a supplier-specific config file (JSON/YAML) to feed defaults into Stage 2 (SKU prefix, tax settings, etc.) to avoid hardcoding.

---

## 11. Future Extensions

- Parameterise Stage 2 defaults through `supplier_config.json` (pending implementation).
- Add automated validation script that lints `products_grouped.json` against the contract table.
- Document sample pipelines for multiple suppliers and multi-run rollbacks.

---

_Last updated: 2025-11-15._
