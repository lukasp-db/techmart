import { useCallback, useEffect, useMemo, useState } from "react";
import {
  api,
  type AdjustmentIn,
  type AdjustmentRow,
  type FiltersResp,
  type ForecastResp,
} from "./api";
import { ForecastChart } from "./components/ForecastChart";
import { AdjustModal } from "./components/AdjustModal";
import { fmtDate, fmtDateTime, fmtInt, fmtUsd } from "./format";

type Level = "category" | "product";
type Version = "improved" | "baseline" | "both";

const PRESETS = [8, 13, 26, 52];

export default function App() {
  const [filters, setFilters] = useState<FiltersResp | null>(null);
  const [level, setLevel] = useState<Level>("category");
  const [selId, setSelId] = useState<string>("");
  const [preset, setPreset] = useState<number>(13);
  const [version, setVersion] = useState<Version>("improved");

  const [forecast, setForecast] = useState<ForecastResp | null>(null);
  const [loadingFc, setLoadingFc] = useState(false);
  const [adjustments, setAdjustments] = useState<AdjustmentRow[]>([]);
  const [modalOpen, setModalOpen] = useState(false);
  const [toast, setToast] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const fw0 = filters?.week_min.fiscal_week ?? 1;
  const fy = filters?.week_min.fiscal_year ?? 2025;
  const fw1 = Math.min(fw0 + preset - 1, filters?.week_max.fiscal_week ?? 52);

  // Load filter metadata + adjustments once (categories first).
  useEffect(() => {
    api
      .filters()
      .then((f) => {
        setFilters(f);
        setSelId(f.categories[0] ?? "");
      })
      .catch((e) => setError(String(e)));
    refreshAdjustments();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // When level switches to product, load products + default the selection.
  useEffect(() => {
    if (!filters) return;
    if (level === "category") {
      setSelId((prev) => (filters.categories.includes(prev) ? prev : filters.categories[0]));
    } else {
      api.filters().then((f) => {
        setFilters((cur) => (cur ? { ...cur, products: f.products } : f));
        setSelId(String(f.products[0]?.product_sk ?? ""));
      });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [level]);

  const loadForecast = useCallback(() => {
    if (!filters || !selId) return;
    // Guard the level/selId race: a product id must be numeric. If the level
    // just flipped to "product" but selId is still a category name, wait.
    if (level === "product" && Number.isNaN(Number(selId))) return;
    setLoadingFc(true);
    setError(null);
    api
      .forecast({
        level,
        id: selId,
        fy_start: fy,
        fw_start: fw0,
        fy_end: fy,
        fw_end: fw1,
        version,
      })
      .then(setForecast)
      .catch((e) => setError(String(e)))
      .finally(() => setLoadingFc(false));
  }, [filters, selId, level, fy, fw0, fw1, version]);

  useEffect(() => {
    loadForecast();
  }, [loadForecast]);

  function refreshAdjustments() {
    api
      .adjustments(50)
      .then((r) => setAdjustments(r.rows))
      .catch(() => setAdjustments([]));
  }

  async function submitAdjustment(body: AdjustmentIn) {
    const res = await api.submitAdjustment(body);
    setAdjustments((prev) => [res.row, ...prev]);
    setModalOpen(false);
    setToast(
      `Override submitted for ${res.row.product_name ?? "product " + res.row.product_sk} — FW${res.row.fiscal_week}`
    );
    setTimeout(() => setToast(null), 4000);
  }

  const options = useMemo(() => {
    if (!filters) return [] as { value: string; label: string }[];
    return level === "category"
      ? filters.categories.map((c) => ({ value: c, label: c }))
      : filters.products.map((p) => ({ value: String(p.product_sk), label: p.label }));
  }, [filters, level]);

  const summary = forecast?.summary;
  // Improved series (for the weekly table + modal prefill).
  const improvedRows = useMemo(
    () => (forecast?.series ?? []).filter((s) => s.version === "improved"),
    [forecast]
  );
  const tableRows = useMemo(() => {
    // Prefer improved rows; fall back to baseline if version=baseline.
    const rows = version === "baseline" ? forecast?.series.filter((s) => s.version === "baseline") : improvedRows;
    return rows ?? [];
  }, [forecast, improvedRows, version]);

  const selLabel =
    level === "category"
      ? selId
      : filters?.products.find((p) => String(p.product_sk) === selId)?.label ?? selId;

  return (
    <div className="app">
      <header className="header">
        <div className="brand">
          TechMart <span className="accent">— Demand Forecast Adjustments</span>
        </div>
        <div className="sub">Weekly demand planning · chain-level aggregate</div>
      </header>

      {/* Filter bar */}
      <div className="filterbar">
        <div className="field">
          <label>View by</label>
          <div className="toggle">
            <button
              className={level === "category" ? "active" : ""}
              onClick={() => setLevel("category")}
            >
              Category
            </button>
            <button
              className={level === "product" ? "active" : ""}
              onClick={() => setLevel("product")}
            >
              Product
            </button>
          </div>
        </div>
        <div className="field">
          <label htmlFor="sel-picker">{level === "category" ? "Category" : "Product"}</label>
          <select
            id="sel-picker"
            name="picker"
            value={selId}
            onChange={(e) => setSelId(e.target.value)}
          >
            {options.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </div>
        <div className="field">
          <label>Timeframe (first N weeks)</label>
          <div className="presets">
            {PRESETS.map((p) => (
              <button
                key={p}
                className={preset === p ? "active" : ""}
                onClick={() => setPreset(p)}
              >
                {p}w
              </button>
            ))}
          </div>
        </div>
        <div className="field">
          <label>Version</label>
          <div className="toggle">
            {(["improved", "baseline", "both"] as Version[]).map((v) => (
              <button
                key={v}
                className={version === v ? "active" : ""}
                onClick={() => setVersion(v)}
              >
                {v[0].toUpperCase() + v.slice(1)}
              </button>
            ))}
          </div>
        </div>
        <div style={{ marginLeft: "auto" }}>
          <button
            className="btn btn-primary"
            onClick={() => setModalOpen(true)}
            disabled={!filters}
          >
            + Adjust Forecast
          </button>
        </div>
      </div>

      {error && <div className="card" style={{ marginBottom: 16, color: "#b04a35" }}>{error}</div>}

      {/* KPI tiles */}
      <div className="kpis">
        <div className="kpi">
          <div className="label">Total forecast units</div>
          <div className="value">
            {summary ? fmtInt(summary.total_qty) : "—"}
            <span className="unit">units</span>
          </div>
        </div>
        <div className="kpi">
          <div className="label">Total forecast value</div>
          <div className="value">{summary ? fmtUsd(summary.total_amount) : "—"}</div>
        </div>
        <div className="kpi">
          <div className="label">Avg weekly units</div>
          <div className="value">
            {summary ? fmtInt(summary.avg_weekly_qty) : "—"}
            <span className="unit">/ wk</span>
          </div>
        </div>
      </div>

      {/* Chart */}
      <div className="card grid-2">
        <div>
          <div className="toolbar">
            <h3 className="section-title" style={{ margin: 0 }}>
              Weekly forecast — {selLabel} · FW{fw0}–FW{fw1} {fy}
            </h3>
          </div>
          {loadingFc ? (
            <div className="loading">Loading forecast…</div>
          ) : forecast && forecast.series.length ? (
            <ForecastChart series={forecast.series} version={version} />
          ) : (
            <div className="empty">No forecast rows for this selection.</div>
          )}
        </div>
      </div>

      {/* Weekly table */}
      <div className="card" style={{ marginBottom: 20 }}>
        <h3 className="section-title">Weekly series</h3>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Week ending</th>
                <th>FW</th>
                <th className="num">Forecast qty</th>
                <th className="num">Forecast $</th>
                <th className="num">Lower</th>
                <th className="num">Upper</th>
              </tr>
            </thead>
            <tbody>
              {tableRows.map((r) => (
                <tr key={`${r.fiscal_year}-${r.fiscal_week}`}>
                  <td>{fmtDate(r.week_end_date)}</td>
                  <td>FW{r.fiscal_week}</td>
                  <td className="num">{fmtInt(r.forecast_qty)}</td>
                  <td className="num">{fmtUsd(r.forecast_amount)}</td>
                  <td className="num">{fmtInt(r.lower_bound)}</td>
                  <td className="num">{fmtInt(r.upper_bound)}</td>
                </tr>
              ))}
              {!tableRows.length && (
                <tr>
                  <td colSpan={6} className="empty">
                    No rows.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Adjustments table */}
      <div className="card">
        <div className="toolbar">
          <h3 className="section-title" style={{ margin: 0 }}>
            Forecast adjustments (newest first)
          </h3>
          <button className="btn btn-ghost" onClick={refreshAdjustments}>
            Refresh
          </button>
        </div>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Product / Category</th>
                <th>Store</th>
                <th>Week</th>
                <th className="num">AI qty</th>
                <th className="num">Override qty</th>
                <th className="num">Δ%</th>
                <th>Reason</th>
                <th>Planner</th>
                <th>Created</th>
                <th>Source</th>
              </tr>
            </thead>
            <tbody>
              {adjustments.map((a) => {
                const delta =
                  a.ai_forecast_qty && a.override_qty != null
                    ? ((a.override_qty - a.ai_forecast_qty) / a.ai_forecast_qty) * 100
                    : null;
                return (
                  <tr key={a.override_id}>
                    <td>
                      {a.product_name ?? `#${a.product_sk}`}
                      <div style={{ color: "var(--muted)", fontSize: 11 }}>
                        {a.category_name ?? ""}
                      </div>
                    </td>
                    <td>{a.store_sk}</td>
                    <td>FW{a.fiscal_week}</td>
                    <td className="num">{fmtInt(a.ai_forecast_qty)}</td>
                    <td className="num">{fmtInt(a.override_qty)}</td>
                    <td className="num">
                      {delta == null ? (
                        "—"
                      ) : (
                        <span className={delta >= 0 ? "delta-pos" : "delta-neg"}>
                          {delta >= 0 ? "+" : ""}
                          {delta.toFixed(0)}%
                        </span>
                      )}
                    </td>
                    <td>{a.override_reason}</td>
                    <td>{a.planner_id}</td>
                    <td>{fmtDateTime(a.created_at)}</td>
                    <td>
                      <span className={`badge ${a.source}`}>{a.source}</span>
                    </td>
                  </tr>
                );
              })}
              {!adjustments.length && (
                <tr>
                  <td colSpan={10} className="empty">
                    No adjustments yet.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {modalOpen && filters && (
        <AdjustModal
          filters={filters}
          defaults={{
            product_sk: level === "product" ? Number(selId) : undefined,
            fiscal_year: fy,
            fiscal_week: fw0,
            ai_forecast_qty: improvedRows[0]?.forecast_qty ?? undefined,
          }}
          onClose={() => setModalOpen(false)}
          onSubmit={submitAdjustment}
        />
      )}

      {toast && <div className="toast">{toast}</div>}
    </div>
  );
}
