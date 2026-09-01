# Techmart Seed Decorrelation Sweep — Design

**Date:** 2026-09-01
**Author:** Lukas Peterson (Databricks SA)
**Status:** Approved for planning

## Problem

In Phase 6, `fact_sales_line` was found to concentrate each store's sales into
~2 distinct `date_sk` values. Root cause: the dbldatagen generator used
`randomSeedMethod="fixed"`, which gives **every** seeded `random=True` column the
*same* random seed. All such columns then draw the identical underlying uniform
sequence and come out rank-correlated. The fix was
`randomSeedMethod="hash_fieldname"`, which seeds each column from a hash of its
field name — independent per-column streams, still fully deterministic given the
run seed.

Only `fact_sales_line` was fixed at the time. Every other dbldatagen-based
builder still uses `randomSeedMethod="fixed"` and carries the same latent
correlation. This sweep audits and fixes all of them, and adds tests so the bug
cannot silently return.

## Scope

### Affected builders (4 code sites)

`randomSeedMethod="fixed"` appears in exactly four places. Changing each to
`"hash_fieldname"` is the entire fix:

| Code site | Builders fixed | Seeded independent columns | Correlation symptom under `"fixed"` |
|-----------|----------------|----------------------------|--------------------------------------|
| `src/techmart/spark/dim_builder.py:39` | **5 dims** via the shared `build_scd2_dim`: dim_store, dim_vendor, dim_promotion, dim_employee, dim_customer | 5–10 each | e.g. dim_customer first-name index tracks city index tracks segment |
| `src/techmart/spark/dimensions/dim_product.py:126` | dim_product | ~15 | color / price / vendor / lifecycle / launch-date all move together |
| `src/techmart/facts/fact_inventory_movement.py:52` | fact_inventory_movement | 5 (`date_sk`, `product_sk`, `store_sk`, `movement_type`, `abs_qty`) | dates concentrate per store/product |
| `src/techmart/facts/fact_web_events.py:59` | fact_web_events | 5 (`date_sk`, `channel_sk`, `device_num`, `referrer_num`, `num_events`) | device / channel mix degenerate per day |

### Explicitly out of scope / unaffected

- **`fact_sales_line`** — already on `hash_fieldname` (the original fix). Untouched.
- **dim_date, dim_channel** — built with `spark.createDataFrame`, not dbldatagen. No
  random seed involved. Exempt.
- **Columns derived by `expr` / `F.hash` / `uniform_hash`** — these do not draw from
  the seeded stream, so they are unaffected by `randomSeedMethod` regardless. This
  includes all FKs in fact_web_events (`customer_sk`, `product_sk`, event_ts, etc.),
  `vendor_sk` / `reference_doc_id` / `reason_code` / `unit_cost` in
  fact_inventory_movement, and most of dim_product's derived hierarchy/pricing fields.
  Only the truly seeded `random=True` columns change behavior.

### Invariants preserved

- **Referential integrity.** FK value ranges come from `dim_counts` (actual dim
  `.count()`) or hash expressions, never from the seed method. RI stays 0 orphans.
- **Basket coherence** (fact_sales_line) — untouched by this change.
- **Determinism.** `hash_fieldname` is deterministic given the run seed; re-runs with
  the same seed are still reproducible.
- **No interface / config changes.** No new `ScaleProfile` levers, no new
  `databricks.yml` vars, no changed function signatures. Same output schemas.

## Approach

Standardize every dbldatagen builder on `randomSeedMethod="hash_fieldname"`.
`"fixed"` was never appropriate wherever two or more seeded columns must be
independent, which is every builder here. This is the same, proven correction
already applied to `fact_sales_line`.

## Testing

**One focused independence/spread test per affected builder** (8 total: the 5
shared-builder dims, dim_product, fact_inventory_movement, fact_web_events),
modeled on the existing `tests/test_fact_sales_line_date_spread.py`.

Assertion style: **marginal distinctness / paired-combo spread** — deterministic
under a fixed seed, robust, no statistical flakiness. Each test picks two columns
that *should* be independent and asserts their joint spread is far above what a
correlated (`"fixed"`) build would produce:

- **dim builders** — distinct combinations of two seeded pools greatly exceed
  either pool alone (e.g. dim_customer `(city, segment)`, dim_product
  `(color, lifecycle_status)`). A correlated build collapses combos toward the
  size of a single pool; a decorrelated build approaches the product of the two.
- **fact_inventory_movement** — average per-`store_sk` distinct `date_sk` (and
  `product_sk`) is well above the ~1–2 a correlated build yields.
- **fact_web_events** — `device_type` (and `channel_sk`) mix is present across
  distinct `date_sk` values rather than one device dominating each day.

Thresholds are chosen with margin so the test fails hard on a `"fixed"` regression
but never flakes on the decorrelated build. Each test documents, in a comment, the
correlated-vs-decorrelated expectation (as the existing spread test does).

**Existing per-builder tests** (`test_dim_{store,vendor,promotion,employee,customer,product}_spark.py`,
`test_fact_inventory_movement.py`, `test_fact_web_events.py`) assert on generated
values and some distributions will shift. Any that break are updated in the same
task that introduces the shift, verified by running the full suite.

## Validation

**Local:** full test suite green, including the 8 new independence tests.

**Workspace (this PR):** re-deploy smoke to field-eng-east
(`databricks bundle deploy -t dev -p field-eng-east` then
`databricks bundle run generate_facts -t dev -p field-eng-east`, CLI v1.6.0 at
`/opt/homebrew/bin/databricks`) and spot-check that the shifted distributions land
cleanly:

- dim attribute spread looks natural (no single-value collapse across seeded pools);
- per-`store_sk` `date_sk` spread in fact_inventory_movement and per-`date_sk`
  device/channel spread in fact_web_events are healthy;
- RI still 0 orphans across the affected facts;
- finance reconciliation still exact (finance derives from core facts, so its
  totals shift with the new distributions but must still reconcile).

Deploy/run is a controller step, not a subagent implementer task.

## Cadence

brainstorm (done) → spec (this doc) → implementation plan → subagent-driven
development → workspace re-validation → PR.
