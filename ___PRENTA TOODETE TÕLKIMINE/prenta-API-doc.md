# Prenta API – Integratsioonid ja ETL (WC import)

Viimati uuendatud: 2025-10-07 11:52 (+03:00)

## Ülevaade
- **Baas-URL**: `https://sandbox.prenta.lt/api/{version}`
  - **Version**: `v1` → `https://sandbox.prenta.lt/api/v1`
- **Autentimine**: HTTP Basic Authentication
- **Meedia**: `Accept: application/json`, `Content-Type: application/json`
- **Veakoodid**: `401 Unauthorized` (vale/puuduv Authorization)

## Levinud päringu parameetrid
- **page**: lehe number (vaikimisi 1)
- **per_page**: kirjete arv lehel
- **offset**: nihe (alternatiiv pagineerimisele)
- **limit**: globaalne limiit tagastatavatele kirjetele (näide 300)
- **newer_than**: tagasta kirjed, mille `date_edited` > etteantud aeg (ISO 8601, ajavööndi offset lubatud; kirjelduses märgitud, et Leedu on +02:00)
- **order**: sortimine; vaikimisi kasvav, prefiks `-` kahanev (nt `-product_id`)
- **fields**: komaga eraldatud väljade loend; regex `^(\w+)(,\w+)*$` (näiteks „all fields” või konkreetne valik jõudluse jaoks)


## Vajalikud ressursid WooCommerce sünkroniseerimiseks

Allpool on jaotus „Kinnitatud” vs „Kinnitust vajav”. Kinnitatud plokk tugineb otseselt API mudelist leitud `apiContract:EndPoint` ja skeemi viidetele. Kinnitust vajav plokk on tuletatud API mudeli skeemidest (Product/ProductList, ProductStockList, jne), kuid endpointi täpne path ei olnud mudelis (loetud osades) üheselt nähtav. Palun kinnita täpsed path’id – väldin eeldamist.

### Kinnitatud endpointid (mudelis selgetena nähtavad)

- GET `/deliveries`
  - **Kirjeldus**: „Get a list of deliveries” (kollektsioon)
  - **Query** (näiteid mudelist): `page`, `per_page`, `offset`, `limit`, `order`, `fields`
  - **200**: JSON
  - **401**: Unauthorized

- GET `/orders/{order_id}/deliveries`
  - **Path-param**: `order_id` (string)
  - **Kirjeldus**: tellimuse tarneinfo päring (deliveries by order)
  - **200**: JSON
  - **401**: Unauthorized

- GET `/products`
  - **Kirjeldus**: Get a list of products
  - **Autentimine**: Basic Authentication
  - **Traits**: `orderable`, `pageable`, `searchableDate` (parameeter `newer_than`), `filterable`
  - **Query parameetrid**:
    - **id** (Integer)
    - **name** (String)
    - **internal_reference** (String)
    - **category_id** (Integer)
    - **date_edited** (DateTime)
    - **order** (String) — vaikimisi kasvav; `-` eesliide = kahanev. Saadaolevad väljad: “all fields”. Näide: `-internal_reference`
    - **page** (Integer) — vaikimisi: 1; Näide: 3
    - **per_page** (Integer) — vaikimisi: 100; Min: 1; Max: 100; Näide: 32
    - **offset** (Integer) — vaikimisi: 0; Näide: 1000
    - **limit** (Integer) — Näide: 300
    - **newer_than** (DateTime) — filtreerib `date_edited` järgi (ISO 8601; vajadusel ajavööndi offset; Leedu +02:00)
    - **fields** (String) — komaga eraldatud väljad; võimalik väärtus “all fields”; mustri regex: `^(\w+)(,\w+)*$`. Näide: `id,internal_reference`
  - **Vastus**: `application/json` — `ProductList` (Array)
    - Kirje väljad (näide): `id` (Integer, ≥0, Required), `name` (String, Required), `internal_reference` (String), `category_id` (Integer, ≥0, Required), `date_edited` (DateTime, Required)

- GET `/prices`
  - **Kirjeldus**: Get a list of prices
  - **Autentimine**: Basic Authentication
  - **Traits**: `orderable`, `pageable`
  - **Query parameetrid**:
    - **product_id** (Integer) — Näide: 2
    - **order** (String) — “Available fields: [product_id]”; Näide: `-product_id`
    - **page** (Integer) — vaikimisi: 1; Näide: 3
    - **per_page** (Integer) — vaikimisi: 100; Min: 1; Max: 100; Näide: 32
    - **offset** (Integer) — vaikimisi: 0; Näide: 1000
    - **limit** (Integer) — Näide: 300
  - **Vastus**: `application/json` — `ProductPriceList` (Array)
    - Kirje väljad: `product_id` (Integer, ≥0, Required), `price` (Number, ≥0), `gross_price` (Number, ≥0)

- GET `/stock_levels`
  - **Kirjeldus**: Get a list of stock_levels
  - **Autentimine**: Basic Authentication
  - **Traits**: `orderable`, `pageable`
  - **Query parameetrid**:
    - **product_id** (Integer) — Näide: 2
    - **order** (String) — “Available fields: [product_id]”; Näide: `product_id`
    - **page** (Integer) — vaikimisi: 1; Näide: 3
    - **per_page** (Integer) — vaikimisi: 100; Min: 1; Max: 100; Näide: 32
    - **offset** (Integer) — vaikimisi: 0; Näide: 1000
    - **limit** (Integer) — Näide: 300
  - **Vastus**: `application/json` — `ProductStockList` (Array)
    - Kirje väljad: `product_id` (Integer, ≥0, Required), `qty` (Integer, ≥0), `forecast_date` (Any; Next Replenishment)


- GET `/products/{product_id}`
  - **Kirjeldus**: Extended information on specific product
  - **Autentimine**: Basic Authentication
  - **Traits**: `filterable`
  - **Path-param**:
    - **product_id** (String, Required)
  - **Query parameetrid**:
    - **fields** (String) — komaga eraldatud väljad; võimalik väärtus “all fields”; mustri regex: `^(\w+)(,\w+)*$`. Näide: `id,name,description`
  - **Vastus**: `application/json` — `Product` (Object)
    - Võtmeväljad: `id` (Integer, ≥0, Required), `name` (String, Required), `description` (String), `description_flixmedia` (String, HTML), `images` (Array[String]), `internal_reference` (String), `category_id` (Integer, ≥0, Required), `manufacturer_id` (Any), `country_of_origin_id` (Any), `price_rrp` (Number, ≥0), `weight` (Number, ≥0), `volume` (Number, ≥0), `delivery_lead_time` (Integer), `exclusive` (Boolean), `date_edited` (DateTime, Required), `product_line` (String), `barcode` (String), `compound_name` (String), `attribute_line_ids[]` (Array of { `attribute_id` (Integer), `value_id` (Integer) })


- GET `/products/{product_id}/attributes`
  - **Kirjeldus**: Get a list of attributes (for a specific product)
  - **Autentimine**: Basic Authentication
  - **Traits**: `orderable`, `pageable`, `searchableDate` (parameeter `newer_than`), `filterable`
  - **Path-param**:
    - **product_id** (String, Required)
  - **Query parameetrid**:
    - **id** (Integer) — Attribute identifier
    - **name** (String) — Attribute name
    - **uom_id** (Integer) — Unit of Measurement identifier
    - **type** (String Enum) — `selection | boolean | float | integer | hidden`
    - **order** (String) — vaikimisi kasvav; `-` eesliide = kahanev. Saadaolevad väljad: “all fields”. Näide: `-uom_id`
    - **page** (Integer) — vaikimisi: 1; Näide: 3
    - **per_page** (Integer) — vaikimisi: 100; Min: 1; Max: 100; Näide: 32
    - **offset** (Integer) — vaikimisi: 0; Näide: 1000
    - **limit** (Integer) — Näide: 300
    - **date_edited** (DateTime)
    - **newer_than** (DateTime) — filtreerib `date_edited` järgi (ISO 8601; ajavööndi offset lubatud; Leedu +02:00)
    - **fields** (String) — komaga eraldatud väljad; võimalik väärtus “all fields”; mustri regex: `^(\w+)(,\w+)*$`. Näide: `id,uom_id`
  - **Vastus**: `application/json` — `AttributeList` (Array)
    - Kirje väljad: `id` (Integer, ≥0, Required), `name` (String, Required), `type` (Enum: `float|integer|boolean|selection|hidden`), `date_edited` (DateTime, Required), `uom_id` (Any)


- GET `/products/{product_id}/attribute_values`
  - **Kirjeldus**: Get a list of attribute_values (for a specific product)
  - **Autentimine**: Basic Authentication
  - **Traits**: `orderable`, `pageable`, `searchableDate` (parameeter `newer_than`), `filterable`
  - **Path-param**:
    - **product_id** (String, Required)
  - **Query parameetrid**:
    - **id** (Integer) — Attribute Value Identifier
    - **attribute_id** (Integer) — Attribute Identifier
    - **type** (String Enum) — `selection | boolean | float | integer | hidden`
    - **order** (String) — Saadaolevad väljad: `[id, attribute_id, type, date_edited]`; Näide: `-attribute_id`
    - **page** (Integer) — vaikimisi: 1; Näide: 3
    - **per_page** (Integer) — vaikimisi: 100; Min: 1; Max: 100; Näide: 32
    - **offset** (Integer) — vaikimisi: 0; Näide: 1000
    - **limit** (Integer) — Näide: 300
    - **date_edited** (DateTime)
    - **newer_than** (DateTime) — filtreerib `date_edited` järgi (ISO 8601; ajavööndi offset lubatud; Leedu +02:00)
    - **fields** (String) — lubatud: `[id, attribute_id, date_edited]`; regex: `^(\w+)(,\w+)*$`. Näide: `id,attribute_id`
  - **Vastus**: `application/json` — `AttributeValueList` (Array)
    - Kirje väljad: `id` (Integer, ≥0, Required), `attribute_id` (Integer, ≥0), `type` (Enum: `hidden|selection|boolean|float|integer`), `date_edited` (DateTime, Required), `value_text` (String), `value_integer` (Integer, ≥0), `value_float` (Number), `value_boolean` (Boolean)


### Kinnitust vajavad (palun täpsusta endpointide path’id)

Järgnevad ressursid ja skeemid esinevad API mudelis (JSON-skeemid ja NodeShape’id), kuid loetud mudeliosades ei olnud nende täpsed `apiContract:path` väärtused nähtaval. Palun kinnita alltoodud endpointide path’id.

- Categories (kategooriad)
  - **Skeem**: (kollektsiooni skeem kategooriatele; väljad tüüpiliselt `id`, `name`, `date_edited`, hierarhia väljad nagu `parent_id`, `parent_left`, `parent_right` esinevad mudelis)
  - **Path**: TBC (palun kinnita)

- Manufacturers (tootjad/brandid)
  - **Skeem**: kollektsioon `id`, `name`, `date_edited` (ja teised kirjeldavad väljad)
  - **Path**: TBC (palun kinnita)

- Attributes (atribuutide tüübid)
  - **Skeem**: Atribuudid koos `id`, `name`, `type` jne
  - **Path**: TBC (palun kinnita)

- Attribute Values (atribuutide väärtused)
  - **Skeem**: `attribute-value-list.json` – väljad sh `attribute_id`, `id`, `date_edited`, `type`, `value_*`
  - **Path**: TBC (palun kinnita)

- UoM (ühikud)
  - **Skeem**: `uom-list.json` – väljad: `id`, `name`, `date_edited`
  - **Path**: TBC (palun kinnita)

- Countries (riigid)
  - **Skeem**: `CountryList` (mudelis esineb), väljad: `id`, `code`, `name`, `date_edited`
  - **Path**: TBC (palun kinnita)


## cURL näited

NB! Asenda `{username}:{password}` oma sandboxi Basic Auth mandaatidega. Väärtusta ka `version`.

```bash
# Näide: lehekülje kaupa kollektsiooni päring (tooted)
curl -u "{username}:{password}" \
  -H "Accept: application/json" \
  "https://sandbox.prenta.lt/api/v1/products?page=1&per_page=100&order=-internal_reference&fields=id,internal_reference,name,category_id,date_edited"
```

```bash
# Näide: detailkirje (nt /products/{id})
curl -u "{username}:{password}" \
  -H "Accept: application/json" \
  "https://sandbox.prenta.lt/api/v1/products/12345?fields=id,name,description"
```

```bash
# Näide: varud (/stock_levels)
curl -u "{username}:{password}" \
  -H "Accept: application/json" \
  "https://sandbox.prenta.lt/api/v1/stock_levels?fields=product_id,qty,forecast_date"
```

```bash
# Näide: hinnad (/prices)
curl -u "{username}:{password}" \
  -H "Accept: application/json" \
  "https://sandbox.prenta.lt/api/v1/prices?page=1&per_page=100&order=-product_id"
```

```bash
# Näide: toote atribuudid (/products/{product_id}/attributes)
curl -u "{username}:{password}" \
  -H "Accept: application/json" \
  "https://sandbox.prenta.lt/api/v1/products/12345/attributes?page=1&per_page=100&fields=id,uom_id"
```


## WC kaardistuse soovitused (kõrgtasemel)
- **name** → Woo product title/slug
- **internal_reference** → SKU
- **description / description_flixmedia** → description/short description
- **price_rrp** → regular_price
- **images[]** → product gallery
- **category_id / manufacturer_id** → WC kategooriad / bränd (taksonoomiate sidumine id→slug/name)
- **attribute_line_ids** → WC attributes/variations
- **ProductStockList.qty** → stock_quantity; **forecast_date/delivery_lead_time** → kohaletoimetamise info


## Tundmatud / vajavad kinnitust
Palun kinnita järgmised punktid, et saaksin dokumendi ja skriptid lõpuni täita ilma eeldusteta:

1. **Täpne Products kollektsiooni path** (GET list) ja **detail path** (GET by id)
2. **Product stock** (varude) endpointi path
3. **Taksonoomiate** path’id: Categories, Manufacturers, Attributes, Attribute Values, UoM, Countries
4. Kas on **rate limit** / throttling reegleid, mida peaks arvestama?
5. Kas soovid, et kasutaksime **fields** parameetri vaikimisi „all fields” või koostan täpse minimaalse väljaloendi WC importi jaoks?
6. Kas saaksin **sandbox Basic Auth** mandaadid (või dummy), et ehitada ja testida ETL-i?


## Muud märkmed
- Kõik päringud ja vastused on JSON-is; `date_edited` on ISO 8601 formaadis.
- Inkrementaalne laadimine: kasuta `newer_than` koos pagineerimisega.

---

Kui kinnitad ülaltoodud path’id, lisan konkreetsed cURL/SDK näited ja panen kokku ETL skripti (tõmbab kollektsioonid, ühendab varudega ning valmistab JSON/CSV või WooCommerce API payload’i).
