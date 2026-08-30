# "State-of-the-Art BI on Databricks" — Blog Series Notes

**Date started:** 2026-08-30
**Author:** Lukas Peterson (Databricks SA)
**Companion to:** [Techmart Data Foundation spec](../superpowers/specs/2026-08-30-techmart-data-foundation-design.md)

Living scratchpad for the blog series. Captures each blog's premise, narrative hooks, and
ideas surfaced during brainstorming so nothing gets lost before drafting. Not a spec — a
memory-jogger. Add freely.

## Why now / framing

Written from the field: currently working the **Dick's Sporting Goods / Foot Locker** account,
which is actively adopting Databricks for BI. Good moment to write a *general* series on
Databricks' BI capabilities. The series is grounded in one fictitious org — **Techmart**, a
big-box computer/electronics retailer (Best Buy analog) — so all three blogs share a single,
believable data foundation instead of disconnected toy datasets.

**Throughline of the series: AI as a first-class part of BI**, not a bolt-on.

## Shared foundation: Techmart

- Omnichannel big-box electronics: computers, components, accessories, consumer electronics
  (cameras/phones/printers), appliances, DIY/networking (e.g., ethernet wiring). Stores +
  e-comm + mobile + 3rd-party marketplace.
- Deep 6-level product hierarchy (Division → Department → Category → Subcategory → Brand → SKU).
- Conformed dimensions bridge sales ↔ inventory (and forecast, budget, valuation) — the
  drill-across backbone.
- Aligned to the **Databricks retail industry data model v2** for credibility.
- Personas: Executive, Merchandising (primary), Finance, Store/Ops, Supply/Planning.
- Repo is public + parameterized so readers can run it at any scale; showcase scale for demos.

---

## Blog 1 — Presentation interfaces & the use cases they support

**Premise:** Survey the Databricks BI presentation surfaces and *which use case each is best for*.
Same Techmart data, four front doors:

- **Databricks Apps** — custom, interactive, operational/bespoke experiences.
- **Databricks Dashboards (AI/BI)** — governed, curated analytics; exec/merch table-heavy visuals.
- **Genie** — natural-language Q&A over the semantic layer.
- **Excel Add-on** — meet finance/merch analysts where they already live; live aggregatable metrics.

**Narrative hooks / memory-joggers:**
- Match interface → persona → use case (execs want KPIs + table visuals; analysts want Excel;
  ad-hoc askers want Genie; operational users want Apps).
- Genie shines *because* of the semantic-layer investment (comments, synonyms, sample values,
  metric definitions, PK/FK). Call this out — Genie quality is a modeling outcome, not magic.
- **AI-as-part-of-BI** shows up here: AI forecasts surfaced alongside actuals; GenAI over
  unstructured text (product reviews, service cases) mixed into dashboards; "explain this dip."
- Structured ⋈ unstructured in one semantic model is an on-trend moment (reviews + sales).

## Blog 2 — Technical architecture & features for optimal performance

**Premise:** How to make BI on Databricks fast and well-modeled. Deep dives start at the **gold**
layer (post-ingestion). Four pillars:

1. **Physical table tuning** — liquid clustering, partitioning, file sizing, MVs; the big
   perf-tuning target is `fact_inventory_snapshot` (store × SKU × day) and `fact_sales_line`.
2. **Logical semantic-layer data modeling** — conformed dimensions, star schema, metric views,
   single authoritative metric definitions.
3. **Presentation interface features** — features in the surfaces themselves that aid performance.
4. **Engine enhancements** — **Lakehouse/RT powered by Reyden** (real-time query engine story);
   high-cardinality, high-volume data (clickstream, sales, inventory snapshots) to make wins visible.

**Narrative hooks / memory-joggers:**
- Data scale is deliberate: ~500M–1B sales lines + daily inventory snapshots so before/after
  tuning has real drama; 3 holiday cycles give forecasting seasonal signal.
- **Medallion features referenced but not the focus:** deletion vectors (helpful during ETL
  throughout the medallion), MERGE. Start technical deep dives at gold. Optionally seed a thin
  bronze slice *only* if a demo needs something to MERGE into.
- Bus matrix is a reusable artifact — could seed a future "how to plan your retail model" blog.

## Blog 3 — Lakehouse & Lakebase for operational BI & apps (write-back)

**Premise:** Operational BI where analytics and action close the loop. **Lakebase (Postgres)**
holds transactional write-back state; the lakehouse holds analytics; they sync.

**Two anchor write-back scenarios:**
1. **Inventory / replenishment management** — app/user adjusts reorder points and approves
   replenishment; syncs back. Canonical retail ops story; pairs with the sales↔inventory
   conformed-dimension bridge.
2. **Human-in-the-loop forecast overrides** — planners accept/adjust AI forecasts
   (`forecast_override` over `fact_sales_forecast`), which drive replenishment. Closes the loop
   between the AI story and operational action.

**Narrative hooks / memory-joggers:**
- The "read analytics ⋈ write operations" loop is the headline.
- Ties the AI forecasting story (Blog 1/2) to real operational decisions.

---

## The finance / reconciliation story (cross-cutting, esp. Blog 1 exec/finance views)

One of the richest narrative threads. Finance and merchandising disagree on the "same" number —
and a well-governed semantic layer is what resolves it.

- **Model choice:** finance reuses conformed dimensions (product, store, date, org hierarchy) but
  gets its **own facts** — GL actuals, budget/plan/forecast, inventory valuation.
- **Deliberate reconciliation gaps:** operational **gross sales** (`fact_sales_line`) ≠ finance
  **net sales** (= gross − returns − allowances, recognized on finance's period calendar). Small,
  documented deltas (returns, allowances, rev-rec timing) are injected on purpose.
- **The payoff:** "why doesn't merch's number match finance's?" becomes a real, resolvable demo.
  The semantic layer exposes each metric with one authoritative definition — single source of truth.
- **Budget vs. actual** is a staple exec/finance visual and a natural place to bring AI forecasts
  into the same picture (Budget vs. Forecast vs. Actual vs. AI-forecast).

## AI moments to weave throughout

- **AI forecasting** — forecast vs. actual vs. budget; forecast accuracy / MAPE.
- **GenAI over unstructured text** — `ai_query` sentiment/summarization on `product_review` and
  `service_case`; mix with structured sales in one semantic model.
- **Injected anomalies** — holiday demand spike, vendor supply disruption, pricing error/margin dip,
  return-fraud cluster, data-quality blemish → anomaly detection & "explain this dip" narratives.
- **Genie** — NL-to-SQL quality as a payoff of semantic-layer investment.

## Parking lot / future ideas

- A future blog: "How to plan your retail data model" using the bus matrix as the centerpiece.
- Consider a thin-bronze deletion-vector/MERGE mini-demo if Blog 2 wants an ingestion beat.
