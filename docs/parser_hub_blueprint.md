# Parser Hub Blueprint (Production-Grade CRE Ingestion)

This document translates the requirements into an implementable architecture for a production-grade ingestion, enrichment, deduplication, scoring, and CRM publishing pipeline for commercial real estate in Russia.

## 1. Architecture Diagram (Text)

```
                        +----------------------+
                        |  Seed Source Registry|
                        +----------+-----------+
                                   |
                                   v
                          +--------+--------+
                          | Source Discovery |
                          +--------+--------+
                                   |
                                   v
          +------------------- Source Catalog -------------------+
          |  known sources | candidates | unknowns | quarantine   |
          +-------------------+-------------------+--------------+
                              |
                              v
   +---------------------+   +-------------------+   +------------------+
   | source_scan_queue   |-> | listing_fetch_q   |-> | listing_parse_q  |
   +---------------------+   +-------------------+   +------------------+
                                                             |
                                                             v
   +---------------------+   +-------------------+   +------------------+
   | enrichment_queue    |-> | dedup_queue       |-> | scoring_queue    |
   +---------------------+   +-------------------+   +------------------+
                                                             |
                                                             v
                                                +--------------------------+
                                                | crm_publish_queue        |
                                                +--------------------------+
                                                             |
                                                             v
                                              +---------------------------+
                                              | CRM / Lead Intelligence   |
                                              +---------------------------+
```

## 2. Domain Model (Conceptual)

```
Source -> Listing -> Object
Listing -> Contact
Contact -> Organization
Listing -> Location
Listing -> PriceHistory
Listing -> EnrichmentFact
Listing -> LeadScore
Listing -> LeadEvent

One Object may map many Listings.
One Contact may map many Listings.
```

## 3. Queue / Event Design

```
source_scan_queue:
  input: source_id
  output: discovered URLs/domains

listing_fetch_queue:
  input: source_id + page_url
  output: raw page content

listing_parse_queue:
  input: raw content + parser config
  output: parsed Listing objects (raw)

enrichment_queue:
  input: Listing + Location
  output: EnrichmentFacts

dedup_queue:
  input: Listing + Contact + Object candidates
  output: duplicate_cluster_id + match_confidence

scoring_queue:
  input: Listing + EnrichmentFacts + dedup result
  output: LeadScore

crm_publish_queue:
  input: Listing + LeadScore + Contact + Object
  output: CRM publish event
```

## 4. Parser Config Schema (JSON)

```json
{
  "version": "1.0",
  "source_type": "classifieds",
  "start_urls": ["https://example.com/listings"],
  "list": {
    "item_selector": ".listing-card",
    "link_selector": "a.card-link",
    "link_attr": "href",
    "pagination": {
      "type": "next_link",
      "next_selector": "a.next",
      "max_pages": 20
    }
  },
  "detail": {
    "fields": {
      "title": {"selector": "h1"},
      "description": {"selector": ".description"},
      "price": {"selector": ".price", "regex": "(\\d[\\d\\s]+)"},
      "area_sqm": {"selector": ".area", "regex": "(\\d+(?:[.,]\\d+)?)"},
      "address_raw": {"selector": ".address"},
      "contact_phone": {"selector": "a[href^='tel:']", "attr": "href", "regex": "(\\+7[\\d\\s\\-()]+)"}
    }
  },
  "anti_bot": {
    "rate_limit_sec": 1.2,
    "retry_count": 3,
    "proxy_pool": "default"
  },
  "crawl_priority": 50,
  "update_frequency_min": 1440
}
```

## 5. Sample SQL Schema (Core Entities)

```sql
create table sources (
  id bigserial primary key,
  name text not null,
  source_type text not null,
  base_url text not null,
  config jsonb,
  is_active boolean default true,
  created_at timestamptz default now()
);

create table listings (
  id bigserial primary key,
  source_id bigint references sources(id),
  external_id text,
  title text,
  description text,
  price_rub numeric,
  area_sqm numeric,
  deal_type text,
  property_type text,
  address_raw text,
  city text,
  region text,
  lat numeric,
  lng numeric,
  contact_phone text,
  contact_email text,
  scraped_at timestamptz default now(),
  payload jsonb
);

create table objects (
  id bigserial primary key,
  canonical_address text,
  lat numeric,
  lng numeric,
  object_type text
);

create table contacts (
  id bigserial primary key,
  name text,
  phone text,
  email text
);

create table lead_scores (
  id bigserial primary key,
  listing_id bigint references listings(id),
  score numeric,
  breakdown jsonb,
  created_at timestamptz default now()
);
```

## 6. API Contract for CRM Publishing

```http
POST /api/v1/crm/leads
Content-Type: application/json

{
  "listing_id": "123",
  "object_id": "45",
  "contact_id": "67",
  "score": 82.4,
  "score_breakdown": {...},
  "tier": "premium",
  "title": "...",
  "price_rub": 5000000,
  "area_sqm": 250,
  "address": "...",
  "phone": "+7 (900) 000-00-00",
  "source": "Avito"
}
```

## 7. Pseudo-Code (Core Steps)

### Source Discovery
```
seed_domains -> crawl outbound links -> classify -> score relevance
if score >= threshold:
  create source config
else:
  send to review queue
```

### Parsing & Normalization
```
for url in start_urls:
  fetch page
  extract listing links
  for each link:
    fetch detail page
    parse fields via config selectors
    normalize phone / area / price / address
```

### Deduplication
```
fingerprint = hash(phone + address + area + price)
match by phone, address similarity, geo distance, title similarity
if high match:
  assign duplicate_cluster_id
else:
  new object_id
```

### Enrichment
```
geocode address
pull market benchmarks
detect owner vs agent
extract urgency keywords
```

### Lead Scoring
```
score = freshness*0.25 + owner*0.20 + under_market*0.20 +
        uniqueness*0.10 + urgency*0.10 + completeness*0.10 -
        spam*0.05
```

## 8. Folder Structure (Suggested)

```
ingestion/
  connectors/
  discovery/
  pipelines/
  parsers/
  enrichment/
  scoring/
  dedup/
  models/
  api/
```

## 9. Example Parser Configs

### Avito-like Classifieds
```json
{
  "source_type": "classifieds",
  "start_urls": ["https://example.com/commercial"],
  "list": {"item_selector": ".item", "link_selector": "a.item-link", "link_attr": "href"},
  "detail": {
    "fields": {
      "title": {"selector": "h1"},
      "price": {"selector": ".price"},
      "area_sqm": {"selector": ".area"},
      "address_raw": {"selector": ".address"}
    }
  }
}
```

### Agency Site
```json
{
  "source_type": "agency_site",
  "start_urls": ["https://agency.ru/properties"],
  "list": {"item_selector": ".property-card", "link_selector": "a", "link_attr": "href"},
  "detail": {
    "fields": {
      "title": {"selector": ".property-title"},
      "description": {"selector": ".property-desc"},
      "contact_phone": {"selector": "a[href^='tel:']", "attr": "href"}
    }
  }
}
```

### Business Center Site
```json
{
  "source_type": "business_center",
  "start_urls": ["https://bc.ru/offices"],
  "list": {"item_selector": ".office-row", "link_selector": "a", "link_attr": "href"},
  "detail": {
    "fields": {
      "title": {"selector": "h1"},
      "area_sqm": {"selector": ".area"},
      "price": {"selector": ".rent"},
      "floor": {"selector": ".floor"}
    }
  }
}
```
