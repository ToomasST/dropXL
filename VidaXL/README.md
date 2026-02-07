# DropXL/VidaXL andmekogumise ETL

See repo sisaldab skripte, mis võtavad VidaXL feedid ja valmistavad need ette WooCommerce + AI töövoo jaoks. Põhisammud:

- `1_Samm_alg_andmete_kogumine.py` – laeb alla/uuendab VidaXL feedid `data/feeds/` alla.
- `2_Samm_tooteinfo_from_feed.py` – mapib feedi _universaalsele Step 2 skeemile_ (`2_samm_tooteinfo.json`), mida kasutavad sammud 3–5.
- `3_Samm_kategooriate_kontroll_ja_sync.py` – kontrollib, et Step 2 kategooriate tõlked eksisteeriks WooCommerce’is ja loob/värskendab puuduvad kategooriad.
- `4_Samm_toodete_tolkimine.py` – loob AI abil eesti keelse nime, lühikokkuvõtte, pikad kirjeldused, SEO meta ja tõlgitud atribuudid Step 2 skeemi põhjal.
- `5_Samm_toodete_yleslaadimine.py` – laeb Step 4 väljundi WooCommerce’i (toodete loomine/uuendamine, pildid, hinnareegel).
- `tools/` – abiskriptid.

## Keskkond ja sõltuvused

- Python 3.10+
- Vajalikud paketid: `requests` (ja olemasolevad sõltuvused, kui kasutad teisi skripte).

`.env` ei ole feedide tõmbamiseks vajalik (DropXL API jääb tulevikuks).

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

### 1_Samm_alg_andmete_kogumine.py

Feedide allalaadija. Eesmärk on uuendada `data/feeds/` kausta:

- `vidaXL_ee_dropshipping.csv.zip` (ja lahtipakitud CSV kaustas `vidaXL_ee_dropshipping/`)
- `vidaXL_ee_dropshipping_offer.csv`

Kasutus:

```
python 1_Samm_alg_andmete_kogumine.py
```

Valikud:
- `--skip-main` – jäta main feed vahele
- `--skip-offer` – jäta offer feed vahele

### 2_Samm_tooteinfo_from_feed.py

Loeb main CSV feedi, rakendab laofiltri ja limit (100), kraabib variatsioonide SKU-d tootelehelt ning koostab `2_samm_tooteinfo.json`.

### tools/flix_probe.py

Kasulik FlixMedia fallback testimiseks. Võimaldab t.json payload’e käsurealt fetchida ning salvestada `data/flix_probe_*` failidesse.

Kasutus:
```
python tools/flix_probe.py --product-id 9872 --save-json
```

## Andmete kasutusvoog

1. **Käivita `1_Samm_alg_andmete_kogumine.py`** – uuendab feedid kausta `data/feeds/`.
2. **Käivita `2_Samm_tooteinfo_from_feed.py`** – loeb main CSV feedi ja koostab _universaalse Step 2 skeemi_ (`2_samm_tooteinfo.json`).
3. **Käivita `3_Samm_kategooriate_kontroll_ja_sync.py`** – kontrollib, et Step 2 `category.translated_path` väärtustele vastavad WooCommerce’i kategooriad oleksid olemas; vajadusel loob puuduolevad.
4. **Käivita `4_Samm_toodete_tolkimine.py`** – kasutab Step 2 skeemi, et genereerida eesti keelsed nimed, lühikokkuvõtted, pikad kirjeldused, SEO meta ja tõlgitud atribuudid; väljund salvestatakse faili `data/tõlgitud/products_translated_grouped.json`.
5. **Käivita `5_Samm_toodete_yleslaadimine.py`** – loeb Step 4 väljundi ja loob/uuendab vastavad tooted WooCommerce’is (sh pildid, kategooriad, atribuudid, hinnad vastavalt skriptis defineeritud reeglile).

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
    "source_system": "dropxl_feed",
    "source_product_id": "PRF0201324",
    "source_category_ids": ["25"],
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
  "shipping_class": "",
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
