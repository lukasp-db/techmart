# Techmart Data Foundation — Design Spec

**Date:** 2026-08-30
**Author:** Lukas Peterson (Databricks Solutions Architect)
**Status:** Approved (design); pending implementation plan

## Purpose

`retail_bi` generates the synthetic data foundation and semantic layer for **Techmart**,
a fictitious omnichannel big-box electronics retailer. It is the shared, reusable backbone
for a 3-part "state-of-the-art Business Intelligence on Databricks" blog series (see
[blog series notes](../../blog-series/2026-08-30-bi-series-blog-notes.md)).

This spec covers the **data**: domains, dimensional model, finance and AI-enablement data,
operational/write-back tables, scale/parameterization, realism rules, and the operational
considerations for shipping as a public, parameterized repo. It does **not** cover the blog
content or the demo apps/dashboards — those are downstream, each getting its own spec/plan.

## Techmart business model

Omnichannel big-box **computer & electronics** retailer (think Best Buy), category focus on
computers, components, and accessories, plus consumer electronics (cameras, phones,
printers), major appliances, and DIY/networking parts (e.g., ethernet wiring). Channels:
physical stores, e-commerce, mobile app, and a 3rd-party marketplace. Deep SKU hierarchies
and long-tail parts are a deliberate feature.

## Personas the data must serve

- **Executive** — company-wide KPIs, trends, budget vs. actual; table-heavy visuals.
- **Merchandising** — category/brand/SKU performance, sell-through, inventory productivity,
  long-lived vendor relationships, right product mix. (Primary retail persona.)
- **Finance** — net sales, margin, COGS, inventory valuation, budget/forecast vs. actual, GL rollups.
- **Store / Ops** — store-level sales & inventory, replenishment.
- **Supply / Planning** — demand forecasts, replenishment decisions (Blog 3 write-back).
- **Data / Platform** — the SA tuning physical layout and semantic modeling (Blog 2).

## Alignment to the Databricks retail industry model

Aligned to the **Databricks retail industry data model v2**
(`databricks-industry-solutions/lakehouse-industry-data-models`, `data-models/retail/v2`),
which spans 21 domains. We anchor on the **MVM (Minimum Viable Model)** and pull in select
ECM entities where a blog needs them (finance `finance_budget`/`plan_version`, service
`service_case`). Naming follows the model's `snake_case` entity names (`sku`, `brand`,
`vendor`, `location`, `category`, `item_hierarchy`, etc.). Each gold table maps to one or
more industry-model entities; the mapping is documented per table below.

## Architecture (gold-first, "Approach A")

- **Unity Catalog, single catalog, schema-per-domain (prefixed to coexist with other demos):**
  - `techmart_core` — conformed dimensions + core facts
  - `techmart_finance` — GL, budget/plan, inventory valuation
  - `techmart_ai` — forecasts, unstructured text, features
  - `techmart_ops` — operational write-back state (Lakebase/Postgres)
  - `techmart_semantic` — metric views / documented views for Genie & BI
- **Gold-first:** generate directly into a clean Kimball dimensional (star) model. No bronze/silver
  built speculatively. Medallion features (deletion vectors, MERGE, DVs during ETL) are referenced
  as *narrative*; a thin bronze slice is added later **only** if a specific Blog 2 demo needs
  something to MERGE/ingest into.
- **Lakebase (Postgres)** holds operational write-back state, synced back to the lakehouse.
- **Naming:** gold tables use `dim_`/`fact_` prefixes; surrogate keys (`*_sk`, BIGINT) for joins,
  business keys (`*_id`) preserved; every table and column carries a `COMMENT` (fuels Genie).

## Bus matrix (conformed dimensions × facts)

The core of the "sales ↔ inventory bridged by common dimensions" requirement. `Date + Product
+ Store` conform across Sales, Inventory, Forecast, Budget, and Valuation, enabling honest
drill-across metrics (sell-through, weeks-of-supply, GMROI, forecast accuracy, budget attainment).

| Fact \ Dimension | Date | Product | Store | Customer | Employee | Vendor | Promotion | Channel |
|---|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| Sales (line item) | ✅ | ✅ | ✅ | ✅ | ✅ | | ✅ | ✅ |
| Fulfillment (BOPIS/ship-from-store) | ✅ | ✅ | ✅ | ✅ | | | | ✅ |
| Returns / RMA | ✅ | ✅ | ✅ | ✅ | ✅ | | | ✅ |
| Inventory snapshot (store×SKU×day) | ✅ | ✅ | ✅ | | | | | |
| Inventory movement | ✅ | ✅ | ✅ | | | ✅ | | |
| Web/app clickstream | ✅ | ✅ | | ✅ | | | | ✅ |
| Loyalty activity | ✅ | | ✅ | ✅ | | | | ✅ |
| Sales forecast (AI) | ✅ | ✅ | ✅ | | | | | |
| Budget / plan (finance) | ✅ | ✅ | ✅ | | | | | |
| GL actuals (finance) | ✅ | ✅* | ✅* | | | | | |
| Inventory valuation (finance) | ✅ | ✅ | ✅ | | | ✅ | | |

*GL rolls up via product→category and store→cost-center hierarchies, not at SKU grain.

## Product hierarchy (6 levels)

Maps to industry-model `item_hierarchy` + `brand` + `category`.

| Level | Name | Techmart examples |
|---|---|---|
| 1 | Division | Computing · Consumer Electronics · Appliances · Networking & DIY · Services |
| 2 | Department | Laptops · Desktops · PC Components · Cameras · Mobile · Printers · Major Appliances · Networking |
| 3 | Category (Class) | Gaming Laptops · Graphics Cards · Mirrorless Cameras · Routers |
| 4 | Subcategory (Subclass) | 15″ Gaming Laptops · NVIDIA GPUs · Full-Frame Mirrorless · Cat6 Ethernet Cable |
| 5 | Brand | Dell · ASUS · NVIDIA · Canon · Ubiquiti |
| 6 | SKU / Item | Specific model + variant (color/config) |

## Conformed dimensions (`techmart_core`)

All dimensions except `dim_date`/`dim_channel` are **SCD Type 2** with control columns:
`effective_start_ts`, `effective_end_ts`, `is_current`, `version`.

### `dim_product` (grain: SKU; SCD2)
Maps to `product.sku`/`item_hierarchy`/`brand`, `merchandising.category`.
- Keys: `product_sk`, `sku`, `gtin`/`upc`, `model_number`
- Descriptive: `product_name`, `product_description` *(rich text → GenAI)*, `manufacturer`,
  `brand_id`/`brand_name`
- Hierarchy (id + name each): `division_*`, `department_*`, `category_*`, `subcategory_*`
- Sourcing: `primary_vendor_sk`, `private_label_flag`, `is_marketplace`, `marketplace_seller_id`
- Attributes: `uom`, `color`, `spec_attributes` (VARIANT/JSON specs — CPU/RAM/screen/etc.),
  `weight`, `dimensions`
- Economics: `msrp`, `list_price`, `standard_cost` (current; history in pricing/facts)
- Lifecycle: `lifecycle_status` (active/clearance/discontinued), `launch_date`, `discontinue_date`

### `dim_date` (grain: day)
Maps to `analytics.retail_calendar`.
`date_sk` (yyyymmdd), `date`, `day_of_week`, `day_name`, `week`, `iso_week`, `month`,
`month_name`, `quarter`, `year`, fiscal 4-5-4 (`fiscal_week`, `fiscal_period`,
`fiscal_quarter`, `fiscal_year`), `is_weekend`, `is_holiday`, `holiday_name`,
`selling_season` (Back-to-School, Holiday, …).

### `dim_store` (grain: location; SCD2)
Maps to `store.location`/`region`/`format`.
`store_sk`, `store_id`, `store_name`, `store_format` (Flagship/Standard/Outlet/Online-only),
`region_*`, `district_*`, `market_*`, address (`city`,`state`,`postal_code`,`country`,`lat`,`long`),
`square_footage`, `open_date`, `remodel_date`, `close_date`, `status`, `is_ship_from_store`,
`is_bopis_enabled`, `cost_center_id` (→ finance).

### `dim_customer` (grain: customer; SCD2)
Maps to `customer.account`/`profile`/`address`.
`customer_sk`, `customer_id`, `customer_type` (Retail/Commercial-B2B), name, `email`, `phone`
(synthetic), address, `loyalty_member_flag`, `loyalty_tier`, `loyalty_enroll_date`,
`acquisition_channel`, `segment` (DIY-Pro, Gamer, Home-Office, SMB, …), `household_id`,
`email_opt_in`.

### `dim_vendor` (grain: vendor; SCD2)
Maps to `supplier.vendor` + `vendor_scorecard` + `lead_time_agreement`. Carries the
"long-lived vendor relationship" merchandising story.
`vendor_sk`, `vendor_id`, `vendor_name`, `vendor_type` (Manufacturer/Distributor/Marketplace-Seller),
`parent_company`, `country`, `primary_category`, `relationship_start_date`, `preferred_flag`,
`vendor_scorecard_rating`, `avg_lead_time_days`, `payment_terms`, `active_flag`.

### `dim_employee` (grain: associate; SCD2)
Maps to `workforce.associate` + `merchandising.buyer`.
`employee_sk`, `employee_id`, `full_name`, `role` (Cashier/Sales-Associate/Manager/Buyer/Planner),
`store_sk` (home store), `hire_date`, `term_date`, `manager_employee_sk`, `status`.

### `dim_promotion` (grain: promotion/offer; SCD2-lite)
Maps to `promotion.promo_offer`/`promo_campaign`/`vendor_promo_agreement`.
`promotion_sk`, `promotion_id`, `promo_name`, `promo_type` (Markdown/BOGO/Bundle/Coupon/Vendor-Funded),
`discount_method`, `discount_value`, `start_date`, `end_date`, `channel_scope`,
`funding_source` (Retailer/Vendor), `campaign_id`, `campaign_name`.

### `dim_channel` (small conformed dim)
`channel_sk`, `channel_id`, `channel_name` (In-Store/Web/Mobile-App/Marketplace/Call-Center),
`channel_type` (Physical/Digital).

## Core facts (`techmart_core`)

### `fact_sales_line` — central fact; grain: sales transaction line
Maps to `order.pos_transaction_line` + `order.order_line` (unified across channels).
- Degenerate: `transaction_id`, `line_number`, `receipt_id`
- FKs: `date_sk`, `product_sk`, `store_sk`, `customer_sk`, `employee_sk`, `promotion_sk`, `channel_sk`
- Measures: `quantity`, `unit_price`, `unit_cost`, `gross_sales_amount`, `discount_amount`,
  `net_sales_amount`, `tax_amount`, `cogs_amount`, `gross_margin_amount`, `loyalty_points_earned`
- Flags: `is_return` (false here), `is_marketplace`, `tender_type`

### `fact_fulfillment` — grain: fulfillment line (online)
Maps to `fulfillment.fulfillment_line`/`fulfillment_order`.
FKs: `date_sk`, `product_sk`, `store_sk` (fulfilling node), `customer_sk`, `channel_sk`.
Attrs: `order_id`, `fulfillment_type` (BOPIS/Ship-from-Store/DC-Delivery/Curbside), `quantity`,
`promised_date_sk`, `actual_ship_date_sk`, `delivery_date_sk`, `sla_met_flag`, `shipping_cost`.

### `fact_returns` — grain: return line
Maps to `returns.rma`/`rma_line`/`refund`.
FKs: `date_sk`, `product_sk`, `store_sk`, `customer_sk`, `employee_sk`, `channel_sk`.
Attrs: `rma_id`, `original_transaction_id`, `return_reason`, `disposition`
(Restock/Liquidate/RTV/Scrap), `quantity`, `refund_amount`, `restocking_fee`, `is_fraud_suspected`.

### `fact_inventory_snapshot` — grain: store × SKU × day (the big one; primary perf-tuning target)
Maps to `inventory.stock_position`.
FKs: `date_sk`, `product_sk`, `store_sk`.
Measures: `on_hand_qty`, `on_order_qty`, `in_transit_qty`, `reserved_qty`, `available_qty`,
`safety_stock_qty`, `reorder_point`, `unit_cost`, `on_hand_retail_value`, `on_hand_cost_value`,
`days_of_supply`, `is_out_of_stock`.

### `fact_inventory_movement` — grain: movement event
Maps to `inventory.stock_ledger`/`adjustment`/`goods_receipt`/`shrinkage_event`.
FKs: `date_sk`, `product_sk`, `store_sk`, `vendor_sk`.
Attrs: `movement_type` (Receipt/Transfer/Adjustment/Shrink/Return-to-Vendor), `quantity`,
`unit_cost`, `reference_doc_id`, `reason_code`.

### `fact_web_events` — grain: clickstream event (high-volume, semi-structured)
Maps to `ecommerce.web_session`/`product_page_view`/`search_query`/`cart`. Good for perf +
Lakehouse/RT story.
FKs: `date_sk`, `product_sk` (nullable), `customer_sk` (nullable for anon), `channel_sk`.
Attrs: `session_id`, `event_ts`, `event_type` (page_view/search/add_to_cart/checkout/purchase),
`search_term`, `device_type`, `referrer`, `cart_value`.

### `fact_loyalty_activity` — grain: loyalty ledger event
Maps to `loyalty.points_ledger`.
FKs: `date_sk`, `customer_sk`, `store_sk`, `channel_sk`.
Attrs: `activity_type` (Earn/Redeem/Expire/Adjust), `points`, `points_balance`, `tier_at_event`,
`related_transaction_id`.

## Finance (`techmart_finance`)

### `dim_gl_account`
Maps to `finance.gl_account`/`chart_of_accounts`.
`gl_account_sk`, `account_number`, `account_name`, `account_type` (Revenue/COGS/Opex/Asset),
`statement` (P&L/Balance-Sheet), `parent_account`, rollup levels.

### `fact_gl_actuals` — grain: account × cost-center(store) × fiscal period
Maps to `finance.journal_entry_line` (rolled up).
FKs: `date_sk` (period-end), `gl_account_sk`, `store_sk` (cost center), `department_id`.
Measures: `actual_amount`, `currency`.

### `fact_budget_plan` — grain: department × store/region × fiscal period × plan_version
Maps to `finance.finance_budget`/`plan_version`/`scenario`. Powers budget-vs-actual exec views.
FKs: `date_sk`, `store_sk`, `department_id`, `gl_account_sk`.
Measures: `plan_amount`, `plan_units`. Attrs: `plan_version` (Budget/Forecast/Latest-Estimate),
`scenario`.

### `fact_inventory_valuation` — grain: store × category × fiscal period
Finance view of inventory; reconciles to `fact_inventory_snapshot`.
FKs: `date_sk`, `store_sk`, `product_sk`/`category_id`, `vendor_sk`.
Measures: `on_hand_cost_value`, `on_hand_retail_value`, `cogs_amount`, `markdown_amount`,
`shrink_amount`, `gmroi`.

### Reconciliation design (deliberate, teachable)
`fact_sales_line` produces **operational gross sales**. Finance **net sales** = gross − returns −
allowances, recognized on finance's period calendar. Small, documented timing / returns /
allowance deltas are injected so "why doesn't merch's number match finance's?" is a real,
resolvable demo. The semantic layer exposes each metric with one authoritative definition —
the single-source-of-truth payoff.

## AI enablement (`techmart_ai`)

### `fact_sales_forecast` — grain: product(or subcategory) × store × week × forecast_version
Maps to `supplychain.demand_forecast`. Pairs with actuals for forecast accuracy and with
`fact_budget_plan` for the AI-vs-plan story.
FKs: `date_sk`, `product_sk`, `store_sk`.
Measures: `forecast_qty`, `forecast_amount`, `lower_bound`, `upper_bound`, `model_name`,
`forecast_generated_date`.

### `product_review` (text)
Maps to `ecommerce.product_review`. Fuels `ai_query` sentiment/summarization + structured-⋈-
unstructured demos.
`review_id`, `product_sk`, `customer_sk`, `date_sk`, `rating`, `review_title`,
`review_text` (LLM-generated), `verified_purchase`, `helpful_votes`.

### `service_case` (text) — the "Geek Squad" analog
Maps to `service.service_case`.
`case_id`, `customer_sk`, `product_sk`, `store_sk`, `date_sk`, `case_type` (Repair/Warranty/Support),
`channel`, `status`, `case_notes` (text), `resolution_notes`, `csat_score`.

### Injected anomalies (documented catalog)
A holiday demand spike, a vendor supply disruption (category stockouts), a pricing error
(margin dip), a return-fraud cluster, and a data-quality blemish — so anomaly detection /
"explain this dip" narratives have real, findable signal.

## Operational write-back (`techmart_ops`, Lakebase/Postgres)

Transactional tables an app/user mutates, synced back to the lakehouse.

### `replenishment_order`
Maps to `inventory.replenishment_order`/`reorder_policy`.
`replen_id`, `product_sk`, `store_sk`, `suggested_qty`, `approved_qty`, `status`
(Suggested/Approved/Rejected/Ordered), `reorder_point`, `created_by`, `approved_by`, `updated_at`.

### `forecast_override`
Human-in-the-loop over `fact_sales_forecast`.
`override_id`, `product_sk`, `store_sk`, `week`, `ai_forecast_qty`, `override_qty`,
`override_reason`, `planner_id`, `updated_at`.

### Sync pattern
Lakebase is source-of-truth for operational state; changes flow back to `techmart_core`/
`techmart_ai` (CDC/scheduled sync — documented). Blog 3 shows the read-analytics ⋈
write-operations loop.

## Semantic layer (`techmart_semantic`)

Metric views / documented views over gold:
- Conformed **metrics** with single definitions: net sales, gross margin %, sell-through,
  weeks-of-supply, GMROI, forecast accuracy/MAPE, budget attainment.
- Persona-oriented view groupings (Exec / Merch / Finance / Store).
- Synonyms + sample values + rich comments for Genie; PK/FK constraints declared for BI tools.

## Scale, parameterization & realism

- **Parameterized config** (`scale_profile`: demo-lean / **showcase (default)** / stress) controls
  #stores, #SKUs, history length, sales-line volume.
  - **Showcase (demo default):** ~3 yrs history, ~1,000 stores, ~200K SKUs, ~500M–1B sales lines,
    daily inventory snapshots across stores/SKUs.
- **Realism rules:** referential integrity across all FKs; seasonality (holiday / back-to-school
  peaks, weekend lift); category-level trends; SCD2 histories on dims; price/cost drift;
  long-tail SKU distribution; anomalies injected per the catalog.
- **Generation approach:** Spark-native (`dbldatagen`) for large facts on a cluster; local
  (Polars/Mimesis) for dims; LLM for `product_review`/`service_case` text. Idempotent,
  re-runnable, config-driven.

## Operational considerations

- **Public repo, secret-free:** fully synthetic; no real Dick's/Foot Locker or other account data;
  no committed workspace URLs/tokens (env/secrets + `.gitignore`); clear README + license so the
  community can run it at any scale.
- **Target environment:** Databricks field-eng workspace (single catalog, `techmart_*` schemas);
  a Lakebase instance for `techmart_ops`. Credentials/workspace wiring is an implementation step,
  not committed to the repo.
- **Traceability artifacts to build:** industry-model → Techmart table mapping, and a
  "which blog uses what" matrix.

## Out of scope (downstream, separate specs)

- The blog content itself (see blog series notes).
- The demo apps, dashboards, Genie spaces, and Excel add-on configurations.
- A full medallion/ingestion pipeline (gold-first by design).
