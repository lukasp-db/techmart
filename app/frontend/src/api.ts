export type WeekRef = { fiscal_year: number; fiscal_week: number };

export type FiltersResp = {
  categories: string[];
  products: { product_sk: number; label: string }[];
  week_min: WeekRef;
  week_max: WeekRef;
  reasons: string[];
  planners: string[];
};

export type SeriesPoint = {
  fiscal_year: number;
  fiscal_week: number;
  week_end_date: string | null;
  version: string;
  forecast_qty: number | null;
  forecast_amount: number | null;
  lower_bound: number | null;
  upper_bound: number | null;
};

export type ForecastResp = {
  series: SeriesPoint[];
  summary: { total_qty: number; total_amount: number; avg_weekly_qty: number };
};

export type AdjustmentRow = {
  override_id: string;
  product_sk: number;
  product_name: string | null;
  category_name: string | null;
  store_sk: number;
  fiscal_year: number;
  fiscal_week: number;
  ai_forecast_qty: number | null;
  override_qty: number | null;
  override_reason: string;
  planner_id: string;
  created_at: string | null;
  source: string;
};

export type AdjustmentIn = {
  product_sk: number;
  store_sk: number;
  fiscal_year: number;
  fiscal_week: number;
  ai_forecast_qty: number;
  override_qty: number;
  override_reason: string;
  planner_id: string;
};

async function j<T>(url: string, init?: RequestInit): Promise<T> {
  const r = await fetch(url, init);
  if (!r.ok) {
    const txt = await r.text().catch(() => r.statusText);
    throw new Error(`${r.status}: ${txt}`);
  }
  return r.json();
}

export const api = {
  filters: (category?: string) =>
    j<FiltersResp>(
      "/api/filters" + (category ? `?category=${encodeURIComponent(category)}` : "")
    ),
  forecast: (p: {
    level: "category" | "product";
    id: string;
    fy_start: number;
    fw_start: number;
    fy_end: number;
    fw_end: number;
    version: "improved" | "baseline" | "both";
  }) => {
    const qs = new URLSearchParams({
      level: p.level,
      id: p.id,
      fy_start: String(p.fy_start),
      fw_start: String(p.fw_start),
      fy_end: String(p.fy_end),
      fw_end: String(p.fw_end),
      version: p.version,
    });
    return j<ForecastResp>(`/api/forecast?${qs}`);
  },
  adjustments: (limit = 100) =>
    j<{ rows: AdjustmentRow[]; session_count: number }>(
      `/api/adjustments?limit=${limit}`
    ),
  submitAdjustment: (body: AdjustmentIn) =>
    j<{ status: string; row: AdjustmentRow }>("/api/adjustments", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
};
