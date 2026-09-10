import { useMemo, useState } from "react";
import type { AdjustmentIn, FiltersResp } from "../api";

type Props = {
  filters: FiltersResp;
  defaults: {
    product_sk?: number;
    fiscal_year: number;
    fiscal_week: number;
    ai_forecast_qty?: number;
  };
  onClose: () => void;
  onSubmit: (body: AdjustmentIn) => Promise<void>;
};

export function AdjustModal({ filters, defaults, onClose, onSubmit }: Props) {
  const [productSk, setProductSk] = useState<string>(
    defaults.product_sk ? String(defaults.product_sk) : String(filters.products[0]?.product_sk ?? "")
  );
  const [storeSk, setStoreSk] = useState("101");
  const [fiscalWeek, setFiscalWeek] = useState(String(defaults.fiscal_week));
  const [aiQty, setAiQty] = useState(
    defaults.ai_forecast_qty != null ? defaults.ai_forecast_qty.toFixed(1) : ""
  );
  const [overrideQty, setOverrideQty] = useState(
    defaults.ai_forecast_qty != null ? defaults.ai_forecast_qty.toFixed(1) : ""
  );
  const [reason, setReason] = useState(filters.reasons[0]);
  const [planner, setPlanner] = useState(filters.planners[0]);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const productOptions = useMemo(() => filters.products, [filters.products]);

  async function submit() {
    setErr(null);
    const ov = Number(overrideQty);
    if (!productSk) return setErr("Select a product.");
    if (Number.isNaN(ov) || ov < 0) return setErr("Override qty must be a number >= 0.");
    const w = Number(fiscalWeek);
    if (Number.isNaN(w) || w < 1 || w > 53) return setErr("Fiscal week must be 1–53.");
    setBusy(true);
    try {
      await onSubmit({
        product_sk: Number(productSk),
        store_sk: Number(storeSk) || 0,
        fiscal_year: defaults.fiscal_year,
        fiscal_week: w,
        ai_forecast_qty: Number(aiQty) || 0,
        override_qty: ov,
        override_reason: reason,
        planner_id: planner,
      });
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
      setBusy(false);
    }
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h2>Adjust Forecast</h2>
        <div className="modal-sub">
          Submit a demand override. This is a mock — it appends to the session, no
          write-back to Lakebase.
        </div>
        <div className="form-grid">
          <div className="field full">
            <label htmlFor="adj-product">Product</label>
            <select
              id="adj-product"
              name="product"
              value={productSk}
              onChange={(e) => setProductSk(e.target.value)}
            >
              {productOptions.map((p) => (
                <option key={p.product_sk} value={p.product_sk}>
                  {p.label}
                </option>
              ))}
            </select>
          </div>
          <div className="field">
            <label htmlFor="adj-store">Store SK</label>
            <input
              id="adj-store"
              name="store_sk"
              type="number"
              value={storeSk}
              onChange={(e) => setStoreSk(e.target.value)}
            />
          </div>
          <div className="field">
            <label htmlFor="adj-week">Fiscal week ({defaults.fiscal_year})</label>
            <input
              id="adj-week"
              name="fiscal_week"
              type="number"
              value={fiscalWeek}
              onChange={(e) => setFiscalWeek(e.target.value)}
            />
          </div>
          <div className="field">
            <label htmlFor="adj-aiqty">AI forecast qty</label>
            <input
              id="adj-aiqty"
              name="ai_forecast_qty"
              type="number"
              value={aiQty}
              onChange={(e) => setAiQty(e.target.value)}
            />
          </div>
          <div className="field">
            <label htmlFor="adj-ovqty">New override qty</label>
            <input
              id="adj-ovqty"
              name="override_qty"
              type="number"
              value={overrideQty}
              onChange={(e) => setOverrideQty(e.target.value)}
            />
          </div>
          <div className="field">
            <label htmlFor="adj-reason">Reason</label>
            <select
              id="adj-reason"
              name="override_reason"
              value={reason}
              onChange={(e) => setReason(e.target.value)}
            >
              {filters.reasons.map((r) => (
                <option key={r} value={r}>
                  {r}
                </option>
              ))}
            </select>
          </div>
          <div className="field">
            <label htmlFor="adj-planner">Planner</label>
            <select
              id="adj-planner"
              name="planner_id"
              value={planner}
              onChange={(e) => setPlanner(e.target.value)}
            >
              {filters.planners.map((p) => (
                <option key={p} value={p}>
                  {p}
                </option>
              ))}
            </select>
          </div>
        </div>
        {err && <div className="form-err">{err}</div>}
        <div className="modal-actions">
          <button className="btn btn-ghost" onClick={onClose} disabled={busy}>
            Cancel
          </button>
          <button className="btn btn-primary" onClick={submit} disabled={busy}>
            {busy ? "Submitting…" : "Submit adjustment"}
          </button>
        </div>
      </div>
    </div>
  );
}
