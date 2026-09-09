# Sales Magnitude & Seasonality Recalibration — Design

**Date:** 2026-09-09
**Status:** Draft for review
**Author:** Lukas Peterson (with Claude)

## Problem

Trailing-12-month net sales on the showcase dataset are **~$1.19T** (all-time
~$3.31T over 3 years). For a 1,000-store electronics retailer this is ~26×
too high — Best Buy is $43.5B across 965 stores (FY2024); a mid-size national
challenger is ~$25B. The dataset should read like a believable **~$25B/yr**
electronics chain.

### Root causes (measured on `stable_classic_ppke9o`)

1. **Product prices are uniform $9.99–$2,999.99, mean ≈ $1,505/unit**
   (`dim_product.py:168`, `msrp_raw` uniform draw), and price has **no relation
   to category** — a Cat6 cable is as likely to be priced $2,500 as a gaming
   laptop. This makes the average sales line **$4,415**.
2. **Line quantity is uniform 1–5 (avg 3 units/line)** — high for electronics,
   where most lines are a single unit.
3. **Product selection is price-blind** — `fact_sales_line` picks `product_sk`
   via `pow(u,3)` on an arbitrary key, so a $2,500 item is as likely to sell as
   a $10 one. Real electronics retail is the opposite: cheap accessories
   dominate unit volume, big-ticket items are rare but high-dollar.

Secondary: **seasonality is mild** (`date_seasonality_weights`, `lookups.py`):
weekend ×1.5, Holiday ×2.5, Back-to-School ×1.8, Black Friday ×3.0 (a single
day). Thanksgiving / the Cyber-5 window / Cyber Monday are not distinguished,
and troughs are shallow, so the annual cycle is muted.

## Goals

- TTM net sales ≈ **$25B** (± ~10%) at showcase scale.
- **Realistic per-category prices** — a cable is $5–60, a gaming laptop
  $900–3,000; every SKU's price makes sense in dashboards/Genie.
- **Realistic unit mix** — cheap items dominate unit volume; big-ticket items
  are a small share of units but a large share of dollars. With ~750M lines
  (~268M in the trailing year) the $25B target implies a blended **net line of
  ~$93** — i.e. a blended **list of ~$85/line** at qty ≈ 1.15 and ~5% average
  discount. That is consistent with real electronics line ASPs (~$80) and is
  reached by an ~80%-sub-$60-units mix (≈80% of units at mean ~$35 + ≈20% at
  mean ~$285 → ~$85).
- **Pronounced, legible seasonality** — a clear annual cycle with Back-to-School
  and a sharp Cyber-5 (Thanksgiving → Cyber Monday) peak, plus a summer sale
  event and a real post-holiday trough.
- Preserve the ~750M-line data volume (the "big data" showcase headline).
- Keep the model deterministic (`uniform_hash`, no `rand()`), and keep all
  measure/RI invariants and schemas unchanged.

## Non-goals

- No schema changes to any fact or dimension.
- No change to catalog size, store count, customer count, or web-event volume.
- Category **unit-share weights** and **price bands** live in the taxonomy /
  reference layer (shared across all scale profiles), not per-profile.

## Design

Five interacting levers. Levers 1–3 fix magnitude + price realism together;
lever 4 is quantity realism; lever 5 is seasonality.

### 1. Category price bands (`reference/taxonomy.py` + `dim_product.py`)

Attach a `(price_low, price_high)` band to each of the 24 leaf **categories**
(`Category` dataclass gains two float fields; authored in `_RAW`). `dim_product`
draws `msrp` **log-uniform within the product's category band**:

```
msrp = price_low * pow(price_high / price_low, u)     # u = uniform_hash(product_sk, salt="msrp")
```

Log-uniform gives a bounded, right-skewed spread inside each band (many nearer
the low end, a tail toward the high end) and is deterministic. `list_price`
(`msrp × (1 - disc)`, disc 0–15%) and `standard_cost` (`msrp × 0.5–0.8`) derive
from msrp exactly as today, so margins are preserved. The uniform `msrp_raw`
draw is removed.

Because `dim_product` already carries `category_id`, the band is looked up by
joining the authored bands (24 rows, broadcast) on `category_id`.

**Initial bands** (low, high in USD; tuned in calibration):

| Category | Band | | Category | Band |
|---|---|---|---|---|
| Gaming Laptops | 900–3000 | | Inkjet Printers | 40–400 |
| Ultrabooks | 700–2200 | | Laser Printers | 120–700 |
| Business Laptops | 500–1800 | | Refrigerators | 600–3500 |
| Gaming Desktops | 800–3500 | | Laundry | 400–2200 |
| All-in-Ones | 600–2200 | | Kitchen (Small Appl.) | 25–500 |
| Graphics Cards | 200–2000 | | Home (Small Appl.) | 60–900 |
| Storage Drives | 40–500 | | Routers | 40–700 |
| Memory | 30–300 | | Switches | 25–500 |
| Mirrorless Cameras | 500–4000 | | Ethernet Cabling | 5–60 |
| Action Cameras | 150–700 | | Connectors & Tools | 5–90 |
| Smartphones | 250–1500 | | Protection Plans | 30–400 |
| Tablets | 150–1300 | | Installation | 60–350 |

### 2. Category unit-share weights (`reference/`)

Author a per-category **unit-share weight** (a relative volume weight). Cheap,
high-turn categories (cabling, connectors, memory, storage, small-kitchen,
protection plans) carry the large majority of weight; big-ticket categories
(laptops, desktops, major appliances, cameras) carry small weights. The weights
are shaped so that **~80% of unit volume comes from sub-$60 categories**, which
pulls the blended list to ~$85/line (net ~$93 after qty and discount) while
still selling laptops and refrigerators at realistic prices.

Stored as a new reference module (e.g. `reference/pricing.py`) exporting the
bands + weights keyed by `category_id`, so taxonomy stays structural and the
economic knobs live in one tunable place.

### 3. Two-stage price-aware basket selection (`fact_sales_line.py`)

Replace the single price-blind `product_sk = floor(pow(u,3)·N)+1` pick with a
**two-stage deterministic pick** per line:

- **Stage 1 — category:** pick a `category_id` weighted by its unit-share
  weight, via the cumulative-weight inverse-CDF over the 24 categories using
  `u1 = uniform_hash(transaction_id, line_number, salt="cat")` (a `when()` chain
  over cumulative weights; 24 branches).
- **Stage 2 — product within category:** build a broadcast product lookup from
  `dim_product` with a within-category index
  `cat_local_idx = row_number() over (partition by category_id order by
  product_sk) - 1` and per-category size `cat_size`. Compute
  `local = floor(u2 · cat_size_c)` with
  `u2 = uniform_hash(transaction_id, line_number, salt="prod")`, then **join**
  the line to the product lookup on `(category_id, cat_local_idx)` to resolve
  `product_sk` and its economics.

Within-category selection is uniform (realism comes from category weights +
bands). The lookup is ~`num_skus` rows (200k), broadcast into the lines join.
Determinism is preserved (all picks are hashes of stable keys; the join is
deterministic).

### 4. Quantity realism (`fact_sales_line.py`)

Replace uniform 1–5 with a right-skewed distribution averaging **~1.15 units/
line** (most lines single-unit): quantity values `[1,2,3,4]` with weights
`[88, 9, 2, 1]` (avg ≈ 1.16), via the existing deterministic hash pick.

### 5. Seasonality (`spark/calendar.py` + `lookups.py`)

**Calendar:** add **Cyber Monday** (`Thanksgiving + 4 days`) to
`holiday_name()`.

**`date_seasonality_weights`** — deepen and enrich (baseline 100):

- Weekend ×1.4.
- **Back-to-School** (selling_season) ×2.2.
- **Holiday** (Nov–Dec) base ×2.0, ramping within December toward Christmas
  (e.g. ×1.5 early-Dec rising to ×3.0 the week before Christmas) via
  `fiscal_week`/`day-of-month`.
- **Cyber-5** distinct days (override, not multiplicative-stacked beyond a cap):
  Thanksgiving ×2.5, **Black Friday ×5.0**, the Sat/Sun between ×3.0,
  **Cyber Monday ×5.0**.
- **Summer sale event** (~Prime Day): a fixed mid-July window ×2.0.
- **Post-holiday trough** (Jan–Feb / "Post-Holiday" season) ×0.7.
- Retain the mild YoY growth trend (×(1 + 0.08·(year − min_year))).

The function feeds **both** `fact_sales_line` and `fact_web_events` date
distributions, so traffic and sales share one seasonal shape. The periodic
inventory snapshot's per-period seasonal factor is derived from actual sales,
so inventory build-ahead follows automatically.

### Calibration to $25B

The blended line value is a function of the price bands, the unit-share weights,
the quantity mean, and the discount rate. Bands and the seasonal shape are held
fixed; the **unit-share weights are the tuning knob**. The plan includes a
**dry-run calibration step**: build sales at a reduced `rows=` on the real dims,
measure TTM net (extrapolated to showcase line count), and adjust the weight
vector (shift share between cheap and mid tiers) until TTM net lands in
$22–28B. No new `scale_profile` lever is required; `sales_lines_target` stays
750M.

## Blast radius & regeneration

`dim_product` pricing changes, so **every downstream table is regenerated**:
- Core: `dim_product`, `fact_sales_line`, and everything derived from sales
  (`fact_returns`, `fact_fulfillment`, `fact_loyalty_activity`), plus
  `fact_inventory_snapshot` (velocity + values) and `fact_web_events` (date
  distribution).
- Finance: `fact_gl_actuals`, `fact_budget_plan`, `fact_inventory_valuation`.
- AI / ops / semantic layers as needed (prices/quantities feed forecasts,
  reviews reference products, metric views recompute).

This is a **full `techmart-generate` run** at showcase scale (not the surgical
inventory-only path). The AI-text step (~140k `ai_query` calls) is the main
cost; whether to re-run the AI layer is a rollout decision (the price change
does not alter review/case *text*, only joined economics), captured in Rollout.

## Testing

- **`dim_product`**: every product's `msrp`/`list_price` falls within its
  category band; blended catalog mean is far below the old ~$1,505; determinism.
- **Pricing reference**: bands present for all 24 categories; weights positive
  and sum-normalizable; a unit test computes the expected weighted-mean line
  value from bands×weights×qty and asserts it is in the calibrated range.
- **`fact_sales_line`**: two-stage pick yields valid `product_sk` in range and
  RI to `dim_product`; category unit-share roughly matches weights (within
  tolerance on a sample); quantity mean ≈ 1.15; measure chain invariants
  (net = gross − discount, margins) hold; determinism.
- **Seasonality (`test_lookups`)**: Cyber Monday recognized; Black Friday and
  Cyber Monday are the top-weighted days; Back-to-School and Holiday elevated;
  post-holiday trough < baseline; weights are all ≥ 1 after flooring.
- **Calendar**: `holiday_name` returns "Cyber Monday" on Thanksgiving+4.
- Existing date-spread and web-events tests updated for the new weights.

## Rollout (executed by Lukas)

1. Merge; `databricks bundle deploy` at showcase scale.
2. Run `techmart-generate` (`--only` the core + finance tasks first; decide
   whether to re-run AI/ops/semantic — see blast radius).
3. Validate: `SELECT SUM(net_sales_amount)` over the trailing 12 months ≈ $25B;
   per-category avg `unit_price` within bands; a monthly net-sales time series
   shows the Back-to-School bump, the Cyber-5 spike, and the January trough.

## Risks

- **Calibration iterations** — hitting $25B may take a couple of weight-vector
  passes; isolated to the weights, measurable via the dry-run step.
- **Two-stage join cost** — a 200k-row broadcast join against the exploded
  lines; broadcast keeps it cheap, but the plan verifies the lookup is
  broadcast (not shuffled).
- **Within-category uniform pick** ignores per-product popularity; acceptable —
  category weighting carries the realism. A per-product popularity skew is a
  possible follow-up.
- **AI-layer coherence** — if AI tables are not re-run, their joined economics
  update on read via the new `dim_product`, but any *materialized* AI outputs
  referencing old prices would be stale; the rollout step decides.
