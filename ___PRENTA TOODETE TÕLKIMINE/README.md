# Prenta andmekogumise ETL

See repo sisaldab skripte, mis võtavad tarnija (praegu Prenta) tootekataloogi ja valmistavad selle ette WooCommerce + AI töövoo jaoks. Põhisammud:

- `0_Samm_kategooriad.py` – kogub ja hoiab kategooriahierarhiat, tõlkeid ja runlisti.
- `1_Samm_alg_andmete_kogumine.py` – tõmbab valitud kategooriate tooted allikasüsteemist ja salvestab rikastatud raw-andmed faili `1_samm_raw_data.json` + pildid `product_images/` alla.
- `2_Samm_tooteinfo.py` – mapib raw-andmed _universaalsele Step 2 skeemile_ (`2_samm_tooteinfo.json`), mida kasutavad sammud 3–5.
- `3_Samm_kategooriate_kontroll_ja_sync.py` – kontrollib, et Step 2 kategooriate tõlked eksisteeriks WooCommerce’is ja loob/värskendab puuduvad kategooriad.
- `4_Samm_toodete_tolkimine.py` – loob AI abil eesti keelse nime, lühikokkuvõtte, pikad kirjeldused, SEO meta ja tõlgitud atribuudid Step 2 skeemi põhjal.
- `5_Samm_toodete_yleslaadimine.py` – laeb Step 4 väljundi WooCommerce’i (toodete loomine/uuendamine, pildid, hinnareegel).
- `tools/` – abiskriptid, sh FlixMedia t.json poole sisu proovimiseks (`flix_probe.py`).
- `prenta_fetch.py` – madalama taseme fetcher, mida saab kasutada arenduses ja debugimisel, kui on vaja otse Prenta API vahefaile.

## Keskkond ja sõltuvused

- Python 3.10+
- Vajalikud paketid: `requests`, `python-dotenv`, `tqdm`, `beautifulsoup4`, `playwright` (valikuline, Flix renderdus).
- API autentimine: HTTP Basic Auth.

`.env`-faili näide:

```
PRENTA_BASE_URL="https://sandbox.prenta.lt/api/v1"
PRENTA_USERNAME="user@example.com"
PRENTA_PASSWORD="secret"
PRENTA_PER_PAGE="100"
PRENTA_VERIFY_SSL=false
PRENTA_FLIX_RENDER=true
```

## Skriptid

### 0_Samm_kategooriate_inventuur.py

Eelkontroll, mis käivitatakse enne põhi-ETL-i. Skriptis on väikesed abifunktsioonid, mis kõik teenindavad üht eesmärki – valmistada ette kategooriate metaandmed järgmisteks sammudeks.

#### Funktsioonid ja rollid

- `log(msg)` – vormistab ajatempliga logirea, et CLI väljund oleks jälgitav.
- `ensure_parent(path)` – loob sihtfaili vanemkausta, et järgnevad salvestused ei ebaõnnestuks.
- `build_category_paths(categories)` – koostab rekursiivselt iga kategooria täistee (nt `Home > Fridge`), kasutades vahemälu, et vältida topeltarvutusi.
- `load_existing_translations()` / `write_translations(translations)` – hoiavad `category_translation.json` failis võtmeks kategooriate rajad ja väärtuseks inimese loodud tõlked; uued rajad lisatakse tühja stringiga.
- `ensure_runlist()` – garanteerib, et olemas on `category_runlist.json` (vähemalt tühi massiiv). Järgmised sammud kasutavad seda, et piirata töö vaid valitud harudega.
- `collect_categories(cfg)` – loob `PrentaClient`i ja iteratsiooniga küsib `/categories` endpointilt kõik kirjed.
- `build_catalog(categories)` – rikastab kategooriaobjektid arvutatud tee, hierarhiataseme ja laste loendiga, sorteerides tulemuse tee järgi.
- `parse_args()` – loeb CLI lipud ja keskkonnamuutujad (API URL, kasutaja, parool, timeout, jne).
- `main()` – orkestreerib eeltoodud sammu: laeb `.env`, ehitab `ClientConfig`i, kogub kategooriad, salvestab kataloogi, uuendab tõlke- ja runlist-faile ning lõpetab eduka koodiga.

#### Andmevoog

1. Laetakse CLI argumendid ning `.env`-st mandaadid.
2. `collect_categories` tõmbab kogu kategooria­puu ning annab selle `build_catalog`ile.
3. Kataloog salvestatakse `data/category_catalog.json` alla.
4. Tõlkefaili täiendatakse uute rajatega (vajadusel luuakse fail esmakordselt).
5. Runlisti fail luuakse või jäetakse puutumata, kui see juba eksisteerib.
6. Skript logib tehtud sammud ja lõpetab – nüüd on kategooria metaandmed valmis järgmisteks sammudeks.

### prenta_fetch.py

All-in-one fetcher, mis toodab mitmeid vahefaile:

- `/products` (list) → `data/products_list.json`
- `/prices` → `data/prices.json`
- `/stock_levels` → `data/stock_levels.json`
- `/products/{id}` detailid + atribuudid / attribute_values → koondatud `data/products_enriched.json`
- FlixMedia renderdus (Playwright või fallback -> t.json)

Toetab järgmisi lippe:

```
python prenta_fetch.py --base-url <url> --username <user> --password <pwd> \
    --max-products 100 --newer-than "2025-01-01T00:00:00"
```

Mõned olulisemad sätted:
- `--single-file-only` – kirjutab vaid `products_master.json`
- `--kv-only` – ainult `products_attributes_kv.json`
- `--flix-render=false` või `PRENTA_FLIX_RENDER=false` – keelab Flix scriptilise renderduse

### 1_Samm_alg_andmete_kogumine.py

Esimene tarnija-spetsiifiline ETL-samm. Eesmärk on tõmmata valitud kategooriate tooted allikasüsteemist (nt Prenta API-st), rikastada need minimaalse lisainfoga ja salvestada **raw** kujul ühte faili:

- `1_samm_raw_data.json` – JSON massiiv, kus iga element on üks rikastatud tooteobjekt.
- `product_images/` – alamkaustad toodete piltide jaoks.

Olulisemad osad:

- **Runlist ja kategooriad**  
  - `load_runlist()` loeb `category_runlist.json` ja otsustab, milliste kategooriate tooted üldse alla laadida.  
  - `load_category_translations()` annab juurdepääsu `category_translation.json` failile (tõlgitud rajad Step 2 ja 3 jaoks).

- **Piltide allalaadimine**  
  - `_prepare_product_images_dir()` loob kausta iga toote piltide jaoks.  
  - `_square_product_image()` laadib pildi alla, eemaldab läbipaistvuse ja salvestab selle ruudukujulise WebP (või `.img`) failina.

- **Toote rikastamine**  
  Skript kogub iga valitud `product_id` jaoks:
  - detailandmed `/products/{id}` endpointist;  
  - hinnad `/prices` vastava `product_id` kohta;  
  - laotasemed `/stock_levels` (kokku summeeritakse `qty`, et rakendada laofilter `qty > 1`);  
  - tootja nime `/manufacturers` listist;  
  - kategooriaraja `/categories` põhjal (`category_path` + `category_path_translated`);  
  - atribuudid (`attribute_line_ids` koos `attribute_name`, `attribute_value`, `attribute_unit` väljadega);  
  - piltide URL-id, mis laaditakse alla lokaalseks `product_images/<product_id>/…` struktuuriks.

- **Väljund**  
  Kõik rikastatud tooted salvestatakse ühe JSON-massiivina faili `1_samm_raw_data.json`, mida kasutab edasi Samm 2.

### tools/flix_probe.py

Kasulik FlixMedia fallback testimiseks. Võimaldab t.json payload’e käsurealt fetchida ning salvestada `data/flix_probe_*` failidesse.

Kasutus:
```
python tools/flix_probe.py --product-id 9872 --save-json
```

## TLS sandboxi eripärad

`sandbox.prenta.lt`-i sertifikaadil on hostname mismatch; mõlemad skriptid toetavad TLS kontrolli väljalülitamist:
- `.env` – `PRENTA_VERIFY_SSL=false`
- `1_Samm_alg_andmete_kogumine.py` tuvastab sandboxi ja lülitab kontrolli automaatselt välja.

## Andmete kasutusvoog

1. **Käivita `0_Samm_kategooriad.py`** – värskendab kategooriate tõlkefaili (`category_translation.json`) ja runlisti (`category_runlist.json`) vastavalt allikasüsteemi kategooriapuule.
2. **Käivita `1_Samm_alg_andmete_kogumine.py`** – loeb runlisti, tõmbab ainult valitud kategooriate tooted (ja nende laoseisu/hinnad), rikastab need ja salvestab `1_samm_raw_data.json` faili + pildid `product_images/` alla.
3. **Käivita `2_Samm_tooteinfo.py`** – loeb `1_samm_raw_data.json` ja mapib iga toote _universaalsele Step 2 skeemile_ (`2_samm_tooteinfo.json`), mida kasutavad kõik järgnevate sammude skriptid.
4. **Käivita `3_Samm_kategooriate_kontroll_ja_sync.py`** – kontrollib, et Step 2 `category.translated_path` väärtustele vastavad WooCommerce’i kategooriad oleksid olemas; vajadusel loob puuduolevad.
5. **Käivita `4_Samm_toodete_tolkimine.py`** – kasutab Step 2 skeemi, et genereerida eesti keelsed nimed, lühikokkuvõtted, pikad kirjeldused, SEO meta ja tõlgitud atribuudid; väljund salvestatakse faili `data/tõlgitud/products_translated_grouped.json`.
6. **Käivita `5_Samm_toodete_yleslaadimine.py`** – loeb Step 4 väljundi ja loob/uuendab vastavad tooted WooCommerce’is (sh pildid, kategooriad, atribuudid, hinnad vastavalt skriptis defineeritud reeglile).

Valikuline: **`prenta_fetch.py` ja `tools/flix_probe.py`** aitavad arenduse ja debugimise ajal API vastuseid uurida ning FlixMedia HTML-i eraldi kontrollida.

## Step 2 skeem (2_samm_tooteinfo.json)

Step 2 väljund on _universaalne tooteinfo leping_, mida sammud 3–5 eeldavad. Iga elemendi (toote) minimaalne struktuur:

- `sku` – poe SKU.
- `global_unique_id` – nt EAN või muu globaalne ID (kui olemas).
- `source` – metaandmed tarnija kohta:
  - `source_system` (nt `"prenta"`, `"supplier_x"`).
  - `source_product_id`, `source_category_ids`.
  - hinnainfo: `purchase_price`, `rrp_price` (kasutatakse hinnareeglis Sammus 5).
- põhiandmed: `name`, `short_description`, `description` (HTML, originaalkeel, võib sisaldada `<img>` ja Flix plokke).
- laoväljad: `manage_stock`, `stock_status`, `stock_quantity`, `backorders` jne.
- logistika: `weight`, `dimensions` (`{length, width, height}`), `shipping_required`, `shipping_taxable`, `shipping_class` jne.
- kategooriad ja bränd:
  - `category`: objekt väljadega `source_id`, `path`, `translated_path`, `leaf_name`.
  - `categories`: minimaalne nimekiri kategooriatest WooCommerce’i jaoks (nt `[{"name": leaf_name}]`).
  - `brands`: nimekiri brändidest (nt `[{"name": "ELICA"}]`).
- pildid:
  - `images`: massiiv objektidest `{"src": "product_images/…", "alt": "…"}`.
- atribuudid:
  - `attributes`: massiiv objektidest `{ "name": "…", "values": ["…", "…"] }`.  
    See **lihtsustatud struktuur on kriitiline**, sest Samm 4/7 tõlgib ja rikastab just neid välju.

Kõik ülejäänud tarnija-spetsiifilised väljad saab soovi korral hoida eraldi haru all (nt `source.extra`), kuid sammud 3–5 neid ei eelda.

### Step 2 näidis (üks toode)

```json
{
  "sku": "PRF0201324",
  "global_unique_id": "8020283060517",
  "source": {
    "source_system": "prenta",
    "source_product_id": 33898,
    "source_category_ids": [25],
    "rrp_price": 99.0,
    "purchase_price": 65.46
  },
  "name": "Hood ELICA CT17 GR/A/52",
  "short_description": "",
  "description": "<p><span>Charcoal filter: CFC0141571</span></p>",
  "type": "simple",
  "status": "publish",
  "regular_price": "99.0",
  "manage_stock": true,
  "stock_status": "instock",
  "stock_quantity": 5,
  "weight": "4.3",
  "dimensions": {
    "length": "29.96",
    "width": "53.5",
    "height": ""
  },
  "shipping_class": "prenta",
  "category": {
    "source_id": 25,
    "path": "All > Saleable > Home appliances > Built in > Hood",
    "translated_path": "Kodutehnika > Integreeritav kodutehnika > Integreeritavad õhupuhastid/kubud",
    "leaf_name": "Integreeritavad õhupuhastid/kubud"
  },
  "categories": [
    { "name": "Integreeritavad õhupuhastid/kubud" }
  ],
  "brands": [
    { "name": "ELICA" }
  ],
  "tags": [],
  "images": [
    {
      "src": "product_images/33898/image_1.webp",
      "alt": "Hood ELICA CT17 GR/A/52"
    }
  ],
  "attributes": [
    {
      "name": "Product line",
      "values": ["Elica Exclusive"]
    }
  ]
}
```

## Uue tarnija liidestamine

Uue tarnija lisamisel on põhimõte lihtne:

- **Sammud 3–5 on universaalsed** ja eeldavad, et sisend on _täpselt_ Step 2 vorming (`2_samm_tooteinfo.json`). Neid ei ole soovitatav muuta, v.a. hinnareegel Sammus 5.
- **Tarnija-spetsiifiline loogika koondub Sammudesse 1–2**: kuidas andmed API-st/CSV-st kätte saadakse ja kuidas need Step 2 skeemile mapitakse.

Soovituslik tegevusplaan:

1. **Kaardista tarnija toorandmed**  
   Kirjelda, millised väljad tarnijalt tulevad (SKU, nimed, kirjeldused, pildid, kategooria-ID-d, hinnad, laoseis, atribuudid jne).

2. **Kohanda Samm 1 uue tarnija jaoks**  
   Kirjuta või muuda Samm 1 nii, et see:
   - loeks uue tarnija andmed (REST, CSV, XML vms);  
   - rakendaks runlisti ja laovaru filtereid (nt `qty > 1`);  
   - salvestaks kõik vajalikud toorväljad faili `1_samm_raw_data.json` + pildid `product_images/` alla.

3. **Rakenda Samm 2 maping Step 2 skeemile**  
   Muuda `2_Samm_tooteinfo.py` loogikat nii, et:
   - `images` massiivis on esmalt põhipildid, seejärel vajadusel galerii/Flix pildid;  
   - atribuudid normaliseeritakse `attributes[{name, values[]}]` vormi (vajadusel ühikute teisendused);  
   - `category`/`categories` ja `brands` väljad täituvad nii, et Samm 3 ja 5 saavad neid otse kasutada;  
   - hinnad/laoseis ja muu oluline info talletatakse Step 2 väljadele (`regular_price`, `stock_quantity`, `source.purchase_price`, `source.rrp_price`, jne).

   Kui tarnija vahetub, muudad reeglina ainult Sammude 1–2 mapingut – sammud 3–5 jäävad samaks.

4. **Kasuta olemasolevaid universaalseid samme**  
   Kui uus tarnija toodab korrektse `2_samm_tooteinfo.json` faili, saad:
   - jooksutada muutmata kujul `3_Samm_kategooriate_kontroll_ja_sync.py` (kategooriate tõlge + loomine Woo-s);  
   - kasutada sama `4_Samm_toodete_tolkimine.py` (AI tõlge, piltide alt tekstid, SEO ja atribuudid);  
   - kasutada `5_Samm_toodete_yleslaadimine.py`, vajadusel ainult **hinnareegleid** kohandades.

5. **Dokumenteeri tarnija eripärad**  
   Lisa README-sse või eraldi faili lühike ülevaade:
   - millised toorväljad tulid tarnijalt;  
   - kuidas need mapiti Step 2 väljadele;  
   - milline hinnastamise reegel Step 5-s kehtib.

Nii saad uue tarnija puhul keskenduda ainult Sammudele 1–2, samal ajal kui **Sammud 3–5 jäävad stabiilseks universaalseks "AI + Woo" toruks**, mida saab korduvkasutada eri projektides.
