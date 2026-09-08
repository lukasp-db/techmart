# Periodic, Sales-Calibrated Inventory Snapshot — Design

**Date:** 2026-09-08
**Status:** Approved for planning
**Author:** Lukas Peterson (with Claude)

## Problem

In the deployed showcase data, `fact_inventory_snapshot` spans only **7 days**
while every other fact spans the full **3-year** history. This blocks the full
inventory analytics suite (on-hand value trend, weeks/days-of-supply trend,
out-of-stock trend, GMROI over time) and starves the finance model
`fact_inventory_valuation`, which only picks up whichever fiscal period-end
happens to land inside that 7-day window (≈ one period).

### Root cause (not a smoke/demo misconfiguration)

`fact_inventory_snapshot` is generated as a **full daily cross-join**: every
store × every SKU × every day, for the last `inventory_snapshot_days` days. At
showcase scale (1,000 stores × 200,000 SKUs = 200M rows/day) a daily grid over
3 years (1,095 days) would be ~219B rows — infeasible — so the `showcase`
profile deliberately clamps it to `inventory_snapshot_days: 7`
(`config/scale_profiles.yaml`). It was never a profile-level (smoke vs.
showcase) mistake.

Two design flaws compound the problem, both previously logged as calibration
debt (memory note "D1"):

1. **Daily grain** can't scale across a long horizon.
2. **Every store carries every SKU** (full cross-join) — unrealistic, and the
   source of the ~27× annual-COGS inventory and the row explosion.

### Audit result — only one model is affected

| Fact | Time span today | Mechanism |
| --- | --- | --- |
| `fact_sales_line` | Full 3 yr | seasonality-weighted over all `dim_date` |
| `fact_web_events` | Full 3 yr | seasonality-weighted |
| `fact_returns`, `fact_fulfillment`, `fact_loyalty_activity` | Full 3 yr | derived from sales (date-shifted) |
| `fact_inventory_movement` | Full 3 yr | events spread over all `date_sk` |
| **`fact_inventory_snapshot`** | **7 days** | store × SKU × day full grid |

So "apply across the board" resolves to: **rewrite one generator**
(`fact_inventory_snapshot`), fix its wiring/config/tests, and verify the two
downstream consumers still read correctly. No other fact needs regeneration.

## Goals

- Inventory snapshots span the **full history** at a sustainable cadence.
- Inventory is **calibrated to sales velocity** (retire the ~27×-COGS debt) so
  weeks/days-of-supply and GMROI are believable.
- Inventory value shows a **realistic seasonal trend** across the history.
- **No schema change** and **no downstream contract break** (grain preserved).
- Total row volume stays in the same order of magnitude as today.

## Non-goals

- New inventory time-series **dashboard widgets** — deferred to a separate
  follow-up. This task delivers the data and verifies existing widgets.
- Regenerating any other fact table.
- Changing the dimensional model or the valuation schema.

## Design

### 1. Cadence — fiscal-period-end, full history

Replace the "last N days" daily grid with snapshots taken on **fiscal
period-end dates** obtained from `period_end_lookup(dim_date)`
(`src/techmart/finance/periods.py`) — ~36 dates over 3 years (one per fiscal
period). Snapshot **grain is unchanged**: `(date_sk, store_sk, product_sk)`.

Rationale:
- A periodic snapshot is the standard dimensional-modeling pattern for
  long-horizon stock positions.
- Period-end alignment means `fact_inventory_valuation` (which already joins
  the snapshot on `period_end_date_sk`) captures **all** periods instead of ~1,
  with no change to its own logic.

### 2. Assortment — realistic per-store SKU subset

Introduce deterministic assortment membership so a store carries only a subset
of the catalog. A `(store, product)` pair is carried when:

```
uniform_hash(store_sk, product_sk, salt="assort") < p(store, product)
```

where `p` is a per-pair carry probability weighted by:

- **Product popularity** — mirrors the sales `pow(u, 3)` long tail. Low-numbered
  (hot) SKUs approach universal distribution; high-numbered (tail) SKUs are
  carried in few stores. Popularity score derived from `product_sk` rank:
  `pop = (1 - (product_sk - 1) / num_products)`.
- **Store breadth** — Flagship / larger `square_footage` stores carry more;
  Outlet / Online-only carry less. Derived from `dim_store.store_format` and/or
  `square_footage`.

`p` is scaled so the **average** carry fraction across all pairs targets a new
`assortment_rate` lever (showcase ≈ 0.20 → ~40k SKUs/store).

Determinism: pure hash of stable keys (no `rand()`), consistent with the
existing generator conventions in `facts/gen.py`.

Accepted simplification: assortment is defined by the hash filter, independent
of whether that exact pair appears in sales. A store may occasionally sell a
SKU outside its "assorted" set (treatable as special order); this is not
inspected at pair grain in the analytics and is an acceptable demo-fidelity
tradeoff. Documented so it is a decision, not an accident.

### 3. On-hand — calibrated to sales velocity

Roll `fact_sales_line` up to per-`(store_sk, product_sk)` demand:

```
avg_daily_units = SUM(quantity) / history_days      -- per (store, product)
```

For each assortment pair × period-end, compute on-hand:

```
on_hand ≈ target_wos_weeks * (avg_daily_units * 7) * seasonal_factor(period) * noise
```

- `target_wos_weeks` — new lever (≈ 8). Because
  `days_of_supply = on_hand / avg_daily_units ≈ target_wos_weeks * 7 * seasonal *
  noise`, this puts weeks/days-of-supply in a sane band **by construction** and
  makes GMROI believable — retiring the ~27×-COGS debt.
- `seasonal_factor(period)` — a small per-fiscal-period demand index derived
  from sales (period units / average period units), so inventory value **swings
  with the season** across the ~36 snapshots (e.g., pre-Q4 build), giving a
  realistic trend rather than a flat line.
- `noise` — deterministic `uniform_hash(store, product, date, salt=...)` jitter,
  keyed to include `date_sk` so successive period-ends differ.
- **Slow-mover floor** — assortment pairs with little/no sales get a small
  positive `avg_daily_units` floor so they still show plausible low stock and a
  nonzero cost value.

Downstream measures — `reserved_qty`, `available_qty`, `safety_stock_qty`,
`reorder_point`, `on_order_qty`, `in_transit_qty`, `on_hand_retail_value`,
`on_hand_cost_value`, `days_of_supply`, `is_out_of_stock` — are derived from the
calibrated `avg_daily_units` / `on_hand`, preserving all existing measure
invariants (non-negative quantities, `available ≤ on_hand`, OOS ⇔ on_hand = 0,
cost = on_hand × unit_cost).

### 4. Wiring — snapshot becomes a sales-linked fact

`build_fact_inventory_snapshot` gains a `fact_sales_line=` parameter and moves
**after** sales in the generation order, reading back the persisted sales
table — the exact pattern already used by `fact_returns` / `fact_fulfillment` /
`fact_loyalty_activity`. Affected call sites:

- `src/techmart/jobs/generate_facts.py` (serverless DAB entrypoint)
- `notebooks/generate_facts.py` (notebook path)
- tests (see Testing)

The `dim_store` and `dim_date` dependencies remain (assortment uses store
attributes; period-ends come from `dim_date`). `dim_product` remains (economics
+ category).

### 5. Config

- Add `assortment_rate` (float) and `target_wos_weeks` (numeric) to every
  profile in `config/scale_profiles.yaml` and to `ScaleProfile` in
  `src/techmart/config.py` (with sensible defaults).
- **Retire `inventory_snapshot_days`** — cadence is now period-end-driven.
  Remove the field and its `ScaleProfile` default, and update the one test that
  passes it. `smoke` (1 yr ≈ 12 period-ends) stays fast.

Illustrative showcase values: `assortment_rate: 0.20`, `target_wos_weeks: 8`.
Exact per-profile values are tuned during implementation against the volume and
WOS-band checks.

### 6. Downstream consumers — verify, don't rewrite

- **`fact_inventory_valuation`** (`finance/`): no code change. It now aggregates
  all ~36 period-ends; the existing snapshot tie-out strengthens rather than
  breaks. GMROI stays finite (guarded by `greatest(..., 1.0)`).
- **`mv_inventory` metric view** (`semantic/metric_views.py`) and **dashboard
  datasets** (`dashboards/datasets.py`): these treat inventory as point-in-time
  "latest snapshot only." Verify that "latest" resolves to `MAX(date_sk)`
  (cadence-agnostic) so it correctly reads the latest period-end. No summing of
  stock across periods in the point-in-time widgets.
- Any inventory **trend** widget is a deliberate follow-up (non-goal here).

## Volume (showcase: 1,000 stores × 200,000 SKUs)

| Scenario | Snapshots | Rows |
| --- | --- | --- |
| Today (7 daily × full grid) | 7 | ~1.4B |
| Naïve daily × 3 yr (rejected) | 1,095 | ~219B |
| **This design** (~20% assortment × ~36 period-ends) | 36 | **~1.4B** |

Roughly flat total volume, now spanning 3 years with realistic assortment
instead of 7 days of everything-everywhere. `assortment_rate` is the volume dial
if tuning is needed.

## Testing

`tests/test_fact_inventory_snapshot.py` (rewrite the grid-count assumptions):
- Schema and grain uniqueness on `(date_sk, store_sk, product_sk)`.
- **Every `date_sk` is a fiscal period-end** (join to `period_end_lookup`; no
  non-period-end dates).
- **Spans full history** — min snapshot date near `start_date`, max near
  `end_date` (≈ `history_years * 12` distinct snapshot dates).
- **Assortment sparsity** — distinct pairs < full `stores × skus` grid; and
  popularity ordering sanity (hot SKUs carried in more stores than tail SKUs).
- **WOS sanity band** — `days_of_supply` centered near
  `target_wos_weeks * 7` (bounded), not the pathological old range.
- Measure invariants (non-negative, `available ≤ on_hand`, OOS flag,
  cost = on_hand × unit_cost).
- Determinism across two builds.

`tests/test_fact_inventory_valuation.py`:
- Existing tie-out (snapshot period-end cost sum = valuation cost sum) now
  covers all periods — keep and confirm it passes.
- Add: valuation `date_sk` set equals the full set of period-ends (spans
  history), not a single period.
- GMROI finite.

Update any fixtures that construct `ScaleProfile` with `inventory_snapshot_days`.

## Rollout / regeneration (executed by Lukas)

1. Merge the code + tests.
2. `databricks bundle deploy` to the target workspace.
3. Run the facts generation job (regenerates `fact_inventory_snapshot`), then
   the finance job (regenerates `fact_inventory_valuation`).
4. Validate: snapshot distinct `date_sk` count ≈ `history_years * 12`, min date
   near `start_date`; `days_of_supply` distribution in the target band; valuation
   spans all periods.

No other fact table needs regeneration.

## Risks

- **Sales aggregation cost** — rolling 750M sales lines to `(store, product)` is
  a large shuffle, but a one-time job step at generation on a suitably sized
  warehouse (consistent with existing fact jobs).
- **Assortment tuning** — `assortment_rate` may need one tuning pass to hit both
  the volume target and believable per-store breadth; it is an isolated lever.
- **Determinism with sales dependency** — the snapshot now depends on the
  persisted sales table; the read-back-then-build pattern (already used by other
  linked facts) keeps it deterministic.
