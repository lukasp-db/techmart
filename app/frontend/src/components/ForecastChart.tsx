import { useMemo, useRef, useState } from "react";
import type { SeriesPoint } from "../api";
import { palette } from "../theme";
import { fmtDate, fmtInt } from "../format";

type Version = "improved" | "baseline" | "both";

type Row = {
  key: string;
  week_end_date: string | null;
  improved: number | null;
  baseline: number | null;
  lower: number | null;
  upper: number | null;
};

function pivot(series: SeriesPoint[], version: Version): Row[] {
  const bandV = version === "baseline" ? "baseline" : "improved";
  const byWeek = new Map<string, Row>();
  for (const p of series) {
    const key = `${p.fiscal_year}-${p.fiscal_week}`;
    if (!byWeek.has(key))
      byWeek.set(key, {
        key,
        week_end_date: p.week_end_date,
        improved: null,
        baseline: null,
        lower: null,
        upper: null,
      });
    const row = byWeek.get(key)!;
    if (p.version === "improved") row.improved = p.forecast_qty;
    if (p.version === "baseline") row.baseline = p.forecast_qty;
    if (p.version === bandV) {
      row.lower = p.lower_bound;
      row.upper = p.upper_bound;
    }
  }
  return Array.from(byWeek.values()).sort((a, b) => {
    const [ay, aw] = a.key.split("-").map(Number);
    const [by, bw] = b.key.split("-").map(Number);
    return ay - by || aw - bw;
  });
}

const W = 760;
const H = 320;
const M = { top: 16, right: 20, bottom: 40, left: 64 };
const IW = W - M.left - M.right;
const IH = H - M.top - M.bottom;

function niceTicks(max: number, count = 4): number[] {
  if (max <= 0) return [0];
  const raw = max / count;
  const mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const norm = raw / mag;
  const step = (norm >= 5 ? 10 : norm >= 2 ? 5 : norm >= 1 ? 2 : 1) * mag;
  const ticks: number[] = [];
  for (let v = 0; v <= max + step; v += step) ticks.push(v);
  return ticks;
}

export function ForecastChart({
  series,
  version,
}: {
  series: SeriesPoint[];
  version: Version;
}) {
  const svgRef = useRef<SVGSVGElement>(null);
  const [hover, setHover] = useState<number | null>(null);
  const data = useMemo(() => pivot(series, version), [series, version]);

  const showImproved = version === "improved" || version === "both";
  const showBaseline = version === "baseline" || version === "both";
  const bandLabel = version === "baseline" ? "Baseline range" : "Improved range";

  const yMax = useMemo(() => {
    let m = 0;
    for (const r of data) {
      for (const v of [r.improved, r.baseline, r.upper]) if (v != null) m = Math.max(m, v);
    }
    return m || 1;
  }, [data]);

  const ticks = niceTicks(yMax);
  const yTop = ticks[ticks.length - 1] || yMax;

  const x = (i: number) =>
    M.left + (data.length <= 1 ? IW / 2 : (i / (data.length - 1)) * IW);
  const y = (v: number) => M.top + IH - (v / yTop) * IH;

  const linePath = (get: (r: Row) => number | null) => {
    let d = "";
    let started = false;
    data.forEach((r, i) => {
      const v = get(r);
      if (v == null) return;
      d += `${started ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)} `;
      started = true;
    });
    return d.trim();
  };

  const bandPath = useMemo(() => {
    const up = data.filter((r) => r.upper != null && r.lower != null);
    if (!up.length) return "";
    let d = "";
    up.forEach((r, i) => {
      const idx = data.indexOf(r);
      d += `${i ? "L" : "M"}${x(idx).toFixed(1)},${y(r.upper!).toFixed(1)} `;
    });
    for (let i = up.length - 1; i >= 0; i--) {
      const idx = data.indexOf(up[i]);
      d += `L${x(idx).toFixed(1)},${y(up[i].lower!).toFixed(1)} `;
    }
    return d + "Z";
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, yTop]);

  const bandColor = version === "baseline" ? palette.navy : palette.coral;

  function onMove(e: React.MouseEvent) {
    const rect = svgRef.current!.getBoundingClientRect();
    const px = ((e.clientX - rect.left) / rect.width) * W;
    if (data.length < 2) return setHover(0);
    const i = Math.round(((px - M.left) / IW) * (data.length - 1));
    setHover(Math.max(0, Math.min(data.length - 1, i)));
  }

  const hoverRow = hover != null ? data[hover] : null;
  const xLabelEvery = Math.ceil(data.length / 8) || 1;

  return (
    <div style={{ position: "relative" }}>
      <svg
        ref={svgRef}
        viewBox={`0 0 ${W} ${H}`}
        width="100%"
        role="img"
        aria-label="Weekly forecast chart"
        onMouseMove={onMove}
        onMouseLeave={() => setHover(null)}
        style={{ display: "block" }}
      >
        {/* Y gridlines + labels */}
        {ticks.map((t) => (
          <g key={t}>
            <line x1={M.left} x2={W - M.right} y1={y(t)} y2={y(t)} stroke={palette.border} />
            <text
              x={M.left - 8}
              y={y(t) + 4}
              textAnchor="end"
              fontSize={11}
              fill={palette.muted}
            >
              {fmtInt(t)}
            </text>
          </g>
        ))}
        {/* X labels */}
        {data.map((r, i) =>
          i % xLabelEvery === 0 ? (
            <text
              key={r.key}
              x={x(i)}
              y={H - M.bottom + 18}
              textAnchor="middle"
              fontSize={11}
              fill={palette.muted}
            >
              {fmtDate(r.week_end_date)}
            </text>
          ) : null
        )}
        {/* Band */}
        {bandPath && <path d={bandPath} fill={bandColor} fillOpacity={0.12} stroke="none" />}
        {/* Lines */}
        {showBaseline && (
          <path d={linePath((r) => r.baseline)} fill="none" stroke={palette.navy} strokeWidth={2} />
        )}
        {showImproved && (
          <path d={linePath((r) => r.improved)} fill="none" stroke={palette.coral} strokeWidth={2.5} />
        )}
        {/* Hover guide + markers */}
        {hoverRow && (
          <>
            <line
              x1={x(hover!)}
              x2={x(hover!)}
              y1={M.top}
              y2={M.top + IH}
              stroke={palette.muted}
              strokeDasharray="3 3"
            />
            {showImproved && hoverRow.improved != null && (
              <circle cx={x(hover!)} cy={y(hoverRow.improved)} r={4} fill={palette.coral} />
            )}
            {showBaseline && hoverRow.baseline != null && (
              <circle cx={x(hover!)} cy={y(hoverRow.baseline)} r={4} fill={palette.navy} />
            )}
          </>
        )}
      </svg>

      {/* Legend */}
      <div style={{ display: "flex", gap: 18, justifyContent: "center", fontSize: 12, marginTop: 4 }}>
        {showImproved && <LegendItem color={palette.coral} label="Improved (gbt_v2)" />}
        {showBaseline && <LegendItem color={palette.navy} label="Baseline (seasonal_naive)" />}
        <LegendItem color={bandColor} label={bandLabel} band />
      </div>

      {/* Tooltip */}
      {hoverRow && (
        <div
          style={{
            position: "absolute",
            top: 8,
            left: `${(x(hover!) / W) * 100}%`,
            transform: "translateX(-50%)",
            background: "#fff",
            border: `1px solid ${palette.border}`,
            borderRadius: 10,
            boxShadow: "0 6px 20px rgba(34,40,63,0.14)",
            padding: "8px 11px",
            fontSize: 12,
            pointerEvents: "none",
            whiteSpace: "nowrap",
          }}
        >
          <div style={{ fontWeight: 600, marginBottom: 4 }}>
            Week ending {fmtDate(hoverRow.week_end_date)}
          </div>
          {showImproved && (
            <div style={{ color: palette.coral }}>Improved: {fmtInt(hoverRow.improved)}</div>
          )}
          {showBaseline && (
            <div style={{ color: palette.navy }}>Baseline: {fmtInt(hoverRow.baseline)}</div>
          )}
          {hoverRow.lower != null && hoverRow.upper != null && (
            <div style={{ color: palette.muted }}>
              {bandLabel}: {fmtInt(hoverRow.lower)} – {fmtInt(hoverRow.upper)}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function LegendItem({ color, label, band }: { color: string; label: string; band?: boolean }) {
  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: 6, color: palette.navy }}>
      <span
        style={{
          width: 16,
          height: band ? 10 : 3,
          background: color,
          opacity: band ? 0.25 : 1,
          borderRadius: 2,
          display: "inline-block",
        }}
      />
      {label}
    </span>
  );
}
