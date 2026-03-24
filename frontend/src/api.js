import { useState, useEffect } from "react";

const API = "https://0t5tmbp5ub.execute-api.us-west-2.amazonaws.com/prod";

const CLUSTER_LABELS = ["Stable", "Transitional", "High-Risk"];
const CLUSTER_COLORS = ["#3b82c4", "#c9932a", "#e85d26"];

const METRICS = [
  { key: "violation_rate",    label: "Violation Rate",   format: v => `${parseFloat(v).toFixed(1)}%`, color: "#e85d26" },
  { key: "open_violations",   label: "Open Violations",  format: v => v.toLocaleString(),              color: "#c9932a" },
  { key: "median_risk_score", label: "Risk Score",       format: v => `${parseFloat(v).toFixed(2)}/10`, color: "#9b3a6b" },
  { key: "total_buildings",   label: "Buildings",        format: v => v.toLocaleString(),              color: "#3b82c4" },
];

function MiniBar({ value, max, color }) {
  const pct = Math.min((value / (max || 1)) * 100, 100);
  return (
    <div style={{ width: "100%", height: 4, background: "rgba(255,255,255,0.08)", borderRadius: 2, overflow: "hidden" }}>
      <div style={{ width: `${pct}%`, height: "100%", background: color, borderRadius: 2, transition: "width 0.6s ease" }} />
    </div>
  );
}

function StatCard({ label, value, sub, color }) {
  return (
    <div style={{ background: "rgba(255,255,255,0.03)", border: "1px solid rgba(255,255,255,0.08)", borderRadius: 12, padding: "20px 24px", display: "flex", flexDirection: "column", gap: 6 }}>
      <div style={{ fontSize: 11, letterSpacing: "0.12em", textTransform: "uppercase", color: "rgba(255,255,255,0.4)", fontFamily: "'DM Mono', monospace" }}>{label}</div>
      <div style={{ fontSize: 26, fontWeight: 700, color, fontFamily: "'Playfair Display', serif", lineHeight: 1 }}>{value}</div>
      {sub && <div style={{ fontSize: 12, color: "rgba(255,255,255,0.35)", fontFamily: "'DM Mono', monospace" }}>{sub}</div>}
    </div>
  );
}

function ScatterPlot({ data, xKey, yKey, selected, onSelect }) {
  if (!data.length) return null;
  const xVals = data.map(d => parseFloat(d[xKey]) || 0);
  const yVals = data.map(d => parseFloat(d[yKey]) || 0);
  const xMin = Math.min(...xVals), xMax = Math.max(...xVals) || 1;
  const yMin = Math.min(...yVals), yMax = Math.max(...yVals) || 1;
  const pad = 36, W = 460, H = 260;
  const cx = v => pad + ((v - xMin) / (xMax - xMin)) * (W - pad * 2);
  const cy = v => H - pad - ((v - yMin) / (yMax - yMin)) * (H - pad * 2);
  const LABELS = { violation_rate: "Violation Rate (%)", open_violations: "Open Violations", median_risk_score: "Risk Score", total_buildings: "Buildings" };

  return (
    <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{ display: "block" }}>
      {[0.25, 0.5, 0.75].map(t => (
        <line key={t} x1={pad} y1={pad + t * (H - pad * 2)} x2={W - pad} y2={pad + t * (H - pad * 2)}
          stroke="rgba(255,255,255,0.05)" strokeWidth={1} />
      ))}
      <text x={W / 2} y={H - 4} textAnchor="middle" fill="rgba(255,255,255,0.25)" fontSize={9} fontFamily="DM Mono, monospace">{LABELS[xKey]}</text>
      <text x={10} y={H / 2} textAnchor="middle" fill="rgba(255,255,255,0.25)" fontSize={9} fontFamily="DM Mono, monospace" transform={`rotate(-90,10,${H / 2})`}>{LABELS[yKey]}</text>
      {data.map((d, i) => {
        const x = cx(parseFloat(d[xKey]) || 0), y = cy(parseFloat(d[yKey]) || 0);
        const color = CLUSTER_COLORS[d.cluster] || "#3b82c4";
        const sel = selected === i;
        return (
          <g key={i} onClick={() => onSelect(sel ? null : i)} style={{ cursor: "pointer" }}>
            {sel && <circle cx={x} cy={y} r={14} fill={color} opacity={0.15} />}
            <circle cx={x} cy={y} r={sel ? 7 : 5} fill={color} opacity={sel ? 1 : 0.7}
              stroke={sel ? "white" : "transparent"} strokeWidth={1.5} />
            {sel && <text x={x + 9} y={y + 4} fontSize={9} fill="white" fontFamily="DM Mono, monospace">{d.neighborhood.split(" ")[0]}</text>}
          </g>
        );
      })}
    </svg>
  );
}

export default function App() {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selected, setSelected] = useState(null);
  const [activeMetric, setActiveMetric] = useState("violation_rate");
  const [activeTab, setActiveTab] = useState("overview");
  const [sortKey, setSortKey] = useState("violation_rate");
  const [sortDir, setSortDir] = useState("desc");
  const [filterCluster, setFilterCluster] = useState(null);
  const [scatterX, setScatterX] = useState("violation_rate");
  const [scatterY, setScatterY] = useState("median_risk_score");

  // Building search state
  const [searchQ, setSearchQ] = useState("");
  const [searchResults, setSearchResults] = useState([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [building, setBuilding] = useState(null);
  const [buildingLoading, setBuildingLoading] = useState(false);

  useEffect(() => {
    fetch(`${API}/neighborhoods`)
      .then(r => r.json())
      .then(d => {
        // Filter out neighborhoods with < 10 buildings (avoids rate distortion)
        const filtered = (d.neighborhoods || []).filter(n => n.total_buildings >= 10);
        setData(filtered);
        setLoading(false);
      })
      .catch(e => { setError(e.message); setLoading(false); });
  }, []);

  async function searchAddress(q) {
    if (q.length < 2) { setSearchResults([]); return; }
    setSearchLoading(true);
    const res = await fetch(`${API}/search?q=${encodeURIComponent(q)}&type=address`);
    const d = await res.json();
    setSearchResults(d.results || []);
    setSearchLoading(false);
  }

  async function loadBuilding(blklot) {
    setBuildingLoading(true);
    setBuilding(null);
    const res = await fetch(`${API}/building?blklot=${blklot}`);
    const d = await res.json();
    setBuilding(d);
    setBuildingLoading(false);
    setSearchResults([]);
    setSearchQ("");
  }

  const metric = METRICS.find(m => m.key === activeMetric);
  const sorted = [...data]
    .filter(d => filterCluster === null || d.cluster === filterCluster)
    .sort((a, b) => {
      const av = parseFloat(a[sortKey]) || 0, bv = parseFloat(b[sortKey]) || 0;
      return sortDir === "desc" ? bv - av : av - bv;
    });

  const maxMetricVal = Math.max(...data.map(d => parseFloat(d[activeMetric]) || 0)) || 1;
  const selData = selected !== null ? data[selected] : null;

  const cityStats = {
    totalBuildings: data.reduce((s, d) => s + (d.total_buildings || 0), 0),
    totalOpen: data.reduce((s, d) => s + (d.open_violations || 0), 0),
    highRisk: data.filter(d => d.cluster === 2).length,
    avgRisk: data.length ? (data.reduce((s, d) => s + parseFloat(d.median_risk_score || 0), 0) / data.length).toFixed(2) : 0,
  };

  if (loading) return (
    <div style={{ minHeight: "100vh", background: "#0d0f14", display: "flex", alignItems: "center", justifyContent: "center" }}>
      <div style={{ color: "rgba(255,255,255,0.4)", fontFamily: "'DM Mono', monospace", fontSize: 12, letterSpacing: "0.15em" }}>
        LOADING SF HOUSING DATA...
      </div>
    </div>
  );

  if (error) return (
    <div style={{ minHeight: "100vh", background: "#0d0f14", display: "flex", alignItems: "center", justifyContent: "center" }}>
      <div style={{ color: "#e85d26", fontFamily: "'DM Mono', monospace", fontSize: 12 }}>Error: {error}</div>
    </div>
  );

  return (
    <div style={{ minHeight: "100vh", background: "#0d0f14", color: "white", fontFamily: "'DM Sans', sans-serif" }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;700;900&family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap');
        *{box-sizing:border-box;margin:0;padding:0}
        ::selection{background:rgba(232,93,38,0.3)}
        ::-webkit-scrollbar{width:4px}
        ::-webkit-scrollbar-thumb{background:rgba(255,255,255,0.1);border-radius:2px}
        .tab-btn{background:none;border:none;cursor:pointer;padding:10px 20px;font-family:'DM Mono',monospace;font-size:11px;letter-spacing:0.1em;text-transform:uppercase;transition:all 0.2s}
        .tab-btn.active{color:#e85d26;border-bottom:1px solid #e85d26}
        .tab-btn:not(.active){color:rgba(255,255,255,0.35);border-bottom:1px solid transparent}
        .row-hover:hover{background:rgba(255,255,255,0.03)}
        .sort-th{cursor:pointer;user-select:none;transition:color 0.15s}
        .sort-th:hover{color:#e85d26}
        .chip{border-radius:20px;padding:4px 12px;font-size:10px;letter-spacing:0.08em;font-family:'DM Mono',monospace;text-transform:uppercase;cursor:pointer;transition:all 0.15s;border:1px solid}
        .metric-btn{background:none;border:1px solid rgba(255,255,255,0.1);border-radius:8px;padding:8px 14px;color:rgba(255,255,255,0.5);font-family:'DM Mono',monospace;font-size:10px;letter-spacing:0.08em;cursor:pointer;transition:all 0.15s;text-transform:uppercase}
        .search-input{width:100%;background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.12);border-radius:10px;padding:14px 18px;color:white;font-family:'DM Sans',sans-serif;font-size:15px;outline:none;transition:border-color 0.2s}
        .search-input:focus{border-color:rgba(232,93,38,0.5)}
        .search-input::placeholder{color:rgba(255,255,255,0.25)}
        @keyframes fadeUp{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:none}}
        .fade-up{animation:fadeUp 0.4s ease forwards}
        @keyframes pulse{0%,100%{opacity:0.4}50%{opacity:1}}
        .pulse{animation:pulse 1.5s ease infinite}
      `}</style>

      {/* Header */}
      <div style={{ borderBottom: "1px solid rgba(255,255,255,0.07)", padding: "0 40px" }}>
        <div style={{ maxWidth: 1100, margin: "0 auto", display: "flex", alignItems: "center", justifyContent: "space-between", height: 64 }}>
          <div style={{ display: "flex", alignItems: "baseline", gap: 12 }}>
            <span style={{ fontFamily: "'Playfair Display', serif", fontSize: 20, fontWeight: 900, color: "#e85d26" }}>SF</span>
            <span style={{ fontFamily: "'DM Mono', monospace", fontSize: 11, letterSpacing: "0.15em", textTransform: "uppercase", color: "rgba(255,255,255,0.5)" }}>Housing Intelligence</span>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <div style={{ width: 6, height: 6, borderRadius: "50%", background: "#4ade80" }} className="pulse" />
            <span style={{ fontSize: 11, fontFamily: "'DM Mono', monospace", color: "rgba(255,255,255,0.3)", letterSpacing: "0.08em" }}>LIVE · {data.length} NEIGHBORHOODS</span>
          </div>
        </div>
      </div>

      <div style={{ maxWidth: 1100, margin: "0 auto", padding: "0 40px 60px" }}>

        {/* Hero */}
        <div style={{ padding: "48px 0 36px", borderBottom: "1px solid rgba(255,255,255,0.05)" }}>
          <div style={{ fontSize: 11, fontFamily: "'DM Mono', monospace", letterSpacing: "0.15em", textTransform: "uppercase", color: "rgba(255,255,255,0.3)", marginBottom: 16 }}>
            Real SF Open Data · KMeans Clustering · Live API
          </div>
          <h1 style={{ fontFamily: "'Playfair Display', serif", fontSize: "clamp(32px,5vw,52px)", fontWeight: 900, lineHeight: 1.05, marginBottom: 16, maxWidth: 680 }}>
            Should I rent<br /><span style={{ color: "#e85d26" }}>this place?</span>
          </h1>
          <p style={{ color: "rgba(255,255,255,0.45)", fontSize: 15, maxWidth: 560, lineHeight: 1.7, fontWeight: 300 }}>
            50,000 SF buildings scored by violation history, recency, and severity. Real data from the Department of Building Inspection.
          </p>
        </div>

        {/* City stats */}
        <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 12, padding: "32px 0" }}>
          <StatCard label="Total Buildings" value={cityStats.totalBuildings.toLocaleString()} sub="in dataset" color="#3b82c4" />
          <StatCard label="Open Violations" value={cityStats.totalOpen.toLocaleString()} sub="citywide" color="#e85d26" />
          <StatCard label="High-Risk Zones" value={`${cityStats.highRisk} nbhds`} sub="cluster 2" color="#9b3a6b" />
          <StatCard label="Avg Risk Score" value={`${cityStats.avgRisk}/10`} sub="across all neighborhoods" color="#c9932a" />
        </div>

        {/* Tabs */}
        <div style={{ borderBottom: "1px solid rgba(255,255,255,0.07)", marginBottom: 32, display: "flex" }}>
          {[["overview","Neighborhood Data"],["scatter","Cluster Plot"],["search","Building Search"]].map(([id, label]) => (
            <button key={id} className={`tab-btn ${activeTab === id ? "active" : ""}`} onClick={() => setActiveTab(id)}>{label}</button>
          ))}
        </div>

        {/* TAB: Overview */}
        {activeTab === "overview" && (
          <div className="fade-up">
            <div style={{ display: "flex", gap: 24, alignItems: "center", marginBottom: 24, flexWrap: "wrap" }}>
              <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                {METRICS.map(m => (
                  <button key={m.key} className="metric-btn"
                    style={{ borderColor: activeMetric === m.key ? m.color : undefined, color: activeMetric === m.key ? m.color : undefined }}
                    onClick={() => { setActiveMetric(m.key); setSortKey(m.key); }}>
                    {m.label}
                  </button>
                ))}
              </div>
              <div style={{ display: "flex", gap: 6, marginLeft: "auto" }}>
                <button className="chip" onClick={() => setFilterCluster(null)}
                  style={{ borderColor: filterCluster === null ? "rgba(255,255,255,0.5)" : "rgba(255,255,255,0.15)", color: filterCluster === null ? "white" : "rgba(255,255,255,0.4)" }}>All</button>
                {CLUSTER_LABELS.map((l, i) => (
                  <button key={i} className="chip" onClick={() => setFilterCluster(filterCluster === i ? null : i)}
                    style={{ borderColor: filterCluster === i ? CLUSTER_COLORS[i] : "rgba(255,255,255,0.12)", color: filterCluster === i ? CLUSTER_COLORS[i] : "rgba(255,255,255,0.4)" }}>
                    {l}
                  </button>
                ))}
              </div>
            </div>

            <div style={{ borderRadius: 14, overflow: "hidden", border: "1px solid rgba(255,255,255,0.07)" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ background: "rgba(255,255,255,0.04)", borderBottom: "1px solid rgba(255,255,255,0.07)" }}>
                    {[
                      { key: "neighborhood", label: "Neighborhood" },
                      { key: "cluster", label: "Cluster" },
                      { key: "total_buildings", label: "Buildings" },
                      { key: "open_violations", label: "Open Violations" },
                      { key: "violation_rate", label: "Viol. Rate" },
                      { key: "median_risk_score", label: "Risk Score" },
                    ].map(col => (
                      <th key={col.key} className="sort-th"
                        onClick={() => { setSortKey(col.key); setSortDir(sortKey === col.key && sortDir === "desc" ? "asc" : "desc"); }}
                        style={{ padding: "12px 16px", textAlign: "left", fontSize: 10, letterSpacing: "0.12em", textTransform: "uppercase", fontFamily: "'DM Mono', monospace", fontWeight: 500, color: sortKey === col.key ? "#e85d26" : "rgba(255,255,255,0.35)" }}>
                        {col.label} {sortKey === col.key ? (sortDir === "desc" ? "↓" : "↑") : ""}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {sorted.map((d, i) => {
                    const idx = data.indexOf(d);
                    return (
                      <tr key={d.neighborhood} className="row-hover"
                        onClick={() => setSelected(selected === idx ? null : idx)}
                        style={{ borderBottom: "1px solid rgba(255,255,255,0.04)", cursor: "pointer", background: selected === idx ? "rgba(232,93,38,0.07)" : "transparent" }}>
                        <td style={{ padding: "13px 16px" }}>
                          <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
                            <span style={{ fontSize: 13, fontWeight: 500 }}>{d.neighborhood}</span>
                            <MiniBar value={parseFloat(d[activeMetric]) || 0} max={maxMetricVal} color={metric.color} />
                          </div>
                        </td>
                        <td style={{ padding: "13px 16px" }}>
                          <span style={{ fontSize: 10, fontFamily: "'DM Mono', monospace", padding: "3px 8px", borderRadius: 4, background: `${CLUSTER_COLORS[d.cluster]}20`, color: CLUSTER_COLORS[d.cluster] }}>
                            {CLUSTER_LABELS[d.cluster] || "Unknown"}
                          </span>
                        </td>
                        <td style={{ padding: "13px 16px", fontFamily: "'DM Mono', monospace", fontSize: 12, color: "rgba(255,255,255,0.7)" }}>{(d.total_buildings || 0).toLocaleString()}</td>
                        <td style={{ padding: "13px 16px", fontFamily: "'DM Mono', monospace", fontSize: 12 }}>
                          <span style={{ color: d.open_violations > 500 ? "#e85d26" : d.open_violations > 200 ? "#c9932a" : "#4ade80" }}>
                            {(d.open_violations || 0).toLocaleString()}
                          </span>
                        </td>
                        <td style={{ padding: "13px 16px", fontFamily: "'DM Mono', monospace", fontSize: 12 }}>
                          <span style={{ color: parseFloat(d.violation_rate) > 50 ? "#e85d26" : parseFloat(d.violation_rate) > 20 ? "#c9932a" : "rgba(255,255,255,0.7)" }}>
                            {parseFloat(d.violation_rate || 0).toFixed(1)}%
                          </span>
                        </td>
                        <td style={{ padding: "13px 16px" }}>
                          <div style={{ display: "flex", gap: 2 }}>
                            {Array.from({ length: 10 }).map((_, j) => (
                              <div key={j} style={{ width: 5, height: 12, borderRadius: 1, background: j < parseFloat(d.median_risk_score) ? "#9b3a6b" : "rgba(255,255,255,0.08)" }} />
                            ))}
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            {/* Detail panel */}
            {selData && (
              <div className="fade-up" style={{ marginTop: 20, background: "rgba(255,255,255,0.025)", border: `1px solid ${CLUSTER_COLORS[selData.cluster]}40`, borderRadius: 14, padding: 28, display: "grid", gridTemplateColumns: "1fr 1fr", gap: 24 }}>
                <div>
                  <div style={{ fontSize: 10, fontFamily: "'DM Mono', monospace", letterSpacing: "0.15em", color: "rgba(255,255,255,0.35)", textTransform: "uppercase", marginBottom: 8 }}>Selected Neighborhood</div>
                  <div style={{ fontFamily: "'Playfair Display', serif", fontSize: 26, fontWeight: 700, marginBottom: 8 }}>{selData.neighborhood}</div>
                  <span style={{ fontSize: 10, fontFamily: "'DM Mono', monospace", padding: "3px 10px", borderRadius: 20, background: `${CLUSTER_COLORS[selData.cluster]}25`, color: CLUSTER_COLORS[selData.cluster], border: `1px solid ${CLUSTER_COLORS[selData.cluster]}40` }}>
                    {CLUSTER_LABELS[selData.cluster]}
                  </span>
                  <div style={{ marginTop: 20, display: "flex", flexDirection: "column", gap: 10 }}>
                    {[
                      ["Total Buildings", (selData.total_buildings || 0).toLocaleString()],
                      ["Open Violations", (selData.open_violations || 0).toLocaleString()],
                      ["Violation Rate", `${parseFloat(selData.violation_rate || 0).toFixed(1)}%`],
                      ["Top Violation", selData.top_violation_type?.slice(0, 40) + "..." || "—"],
                    ].map(([k, v]) => (
                      <div key={k} style={{ display: "flex", justifyContent: "space-between", fontSize: 13, paddingBottom: 8, borderBottom: "1px solid rgba(255,255,255,0.05)" }}>
                        <span style={{ color: "rgba(255,255,255,0.45)" }}>{k}</span>
                        <span style={{ fontFamily: "'DM Mono', monospace", fontWeight: 500, maxWidth: 200, textAlign: "right", fontSize: 11 }}>{v}</span>
                      </div>
                    ))}
                  </div>
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
                  <div style={{ fontSize: 10, fontFamily: "'DM Mono', monospace", letterSpacing: "0.15em", color: "rgba(255,255,255,0.35)", textTransform: "uppercase", marginBottom: 4 }}>Risk Indicators</div>
                  {METRICS.map(m => (
                    <div key={m.key} style={{ display: "flex", flexDirection: "column", gap: 5 }}>
                      <div style={{ display: "flex", justifyContent: "space-between", fontSize: 11 }}>
                        <span style={{ color: "rgba(255,255,255,0.45)", fontFamily: "'DM Mono', monospace" }}>{m.label}</span>
                        <span style={{ color: m.color, fontFamily: "'DM Mono', monospace", fontWeight: 600 }}>{m.format(selData[m.key])}</span>
                      </div>
                      <MiniBar value={parseFloat(selData[m.key]) || 0} max={Math.max(...data.map(d => parseFloat(d[m.key]) || 0)) || 1} color={m.color} />
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        {/* TAB: Scatter */}
        {activeTab === "scatter" && (
          <div className="fade-up">
            <div style={{ display: "flex", gap: 24, marginBottom: 24, flexWrap: "wrap", alignItems: "center" }}>
              {["X","Y"].map((axis, ai) => {
                const key = ai === 0 ? scatterX : scatterY;
                const setKey = ai === 0 ? setScatterX : setScatterY;
                return (
                  <div key={axis} style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                    <label style={{ fontSize: 10, fontFamily: "'DM Mono', monospace", color: "rgba(255,255,255,0.3)", letterSpacing: "0.1em", textTransform: "uppercase" }}>{axis} Axis</label>
                    <select value={key} onChange={e => setKey(e.target.value)}
                      style={{ background: "rgba(255,255,255,0.06)", border: "1px solid rgba(255,255,255,0.1)", borderRadius: 8, padding: "8px 12px", color: "white", fontFamily: "'DM Mono', monospace", fontSize: 11, outline: "none" }}>
                      {METRICS.map(m => <option key={m.key} value={m.key}>{m.label}</option>)}
                    </select>
                  </div>
                );
              })}
              <div style={{ display: "flex", gap: 16, marginLeft: "auto", alignItems: "center" }}>
                {CLUSTER_LABELS.map((l, i) => (
                  <div key={i} style={{ display: "flex", alignItems: "center", gap: 6, cursor: "pointer" }}
                    onClick={() => setFilterCluster(filterCluster === i ? null : i)}>
                    <div style={{ width: 8, height: 8, borderRadius: "50%", background: CLUSTER_COLORS[i], opacity: filterCluster === null || filterCluster === i ? 1 : 0.3 }} />
                    <span style={{ fontSize: 11, fontFamily: "'DM Mono', monospace", color: filterCluster === null || filterCluster === i ? "rgba(255,255,255,0.7)" : "rgba(255,255,255,0.25)" }}>{l}</span>
                  </div>
                ))}
              </div>
            </div>

            <div style={{ display: "grid", gridTemplateColumns: "1fr 280px", gap: 20 }}>
              <div style={{ background: "rgba(255,255,255,0.02)", border: "1px solid rgba(255,255,255,0.07)", borderRadius: 14, padding: 20 }}>
                <ScatterPlot
                  data={filterCluster === null ? data : data.filter(d => d.cluster === filterCluster)}
                  xKey={scatterX} yKey={scatterY}
                  selected={selected} onSelect={setSelected}
                />
                <div style={{ textAlign: "center", fontSize: 11, color: "rgba(255,255,255,0.25)", fontFamily: "'DM Mono', monospace", marginTop: 8 }}>Click a point to select</div>
              </div>
              <div style={{ display: "flex", flexDirection: "column", gap: 6, maxHeight: 320, overflowY: "auto" }}>
                <div style={{ fontSize: 10, fontFamily: "'DM Mono', monospace", letterSpacing: "0.12em", color: "rgba(255,255,255,0.3)", textTransform: "uppercase", marginBottom: 4 }}>Neighborhoods</div>
                {data.map((d, i) => (
                  <div key={i} onClick={() => setSelected(selected === i ? null : i)}
                    style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer", padding: "6px 10px", borderRadius: 8, background: selected === i ? "rgba(255,255,255,0.05)" : "transparent", transition: "background 0.15s" }}>
                    <div style={{ width: 7, height: 7, borderRadius: "50%", background: CLUSTER_COLORS[d.cluster], flexShrink: 0 }} />
                    <span style={{ fontSize: 12, color: selected === i ? "white" : "rgba(255,255,255,0.55)", flex: 1 }}>{d.neighborhood}</span>
                    <span style={{ fontSize: 10, fontFamily: "'DM Mono', monospace", color: "rgba(255,255,255,0.3)" }}>{parseFloat(d.violation_rate || 0).toFixed(0)}%</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

        {/* TAB: Building Search */}
        {activeTab === "search" && (
          <div className="fade-up" style={{ maxWidth: 740 }}>
            <p style={{ color: "rgba(255,255,255,0.45)", fontSize: 14, lineHeight: 1.7, marginBottom: 24, fontWeight: 300 }}>
              Search any SF address to see its full violation history, risk score, and how it compares to similar buildings nearby.
            </p>

            <div style={{ position: "relative", marginBottom: 8 }}>
              <input className="search-input" placeholder="Search address — e.g. Mission, Market, Haight..."
                value={searchQ}
                onChange={e => { setSearchQ(e.target.value); searchAddress(e.target.value); }}
              />
              {searchLoading && (
                <div style={{ position: "absolute", right: 16, top: "50%", transform: "translateY(-50%)", fontSize: 11, fontFamily: "'DM Mono', monospace", color: "rgba(255,255,255,0.3)" }}>searching...</div>
              )}
            </div>

            {searchResults.length > 0 && (
              <div style={{ background: "rgba(255,255,255,0.04)", border: "1px solid rgba(255,255,255,0.1)", borderRadius: 10, overflow: "hidden", marginBottom: 20 }}>
                {searchResults.map((r, i) => (
                  <div key={i} onClick={() => loadBuilding(r.blklot)}
                    style={{ padding: "12px 16px", cursor: "pointer", borderBottom: i < searchResults.length - 1 ? "1px solid rgba(255,255,255,0.06)" : "none", display: "flex", alignItems: "center", gap: 12 }}
                    onMouseEnter={e => e.currentTarget.style.background = "rgba(255,255,255,0.04)"}
                    onMouseLeave={e => e.currentTarget.style.background = "transparent"}>
                    <div style={{ width: 8, height: 8, borderRadius: "50%", background: CLUSTER_COLORS[r.cluster] || "#3b82c4", flexShrink: 0 }} />
                    <div style={{ flex: 1 }}>
                      <div style={{ fontSize: 13, color: "white" }}>{r.address}</div>
                      <div style={{ fontSize: 11, fontFamily: "'DM Mono', monospace", color: "rgba(255,255,255,0.35)", marginTop: 2 }}>{r.neighborhood} · {r.units} units</div>
                    </div>
                    <div style={{ fontFamily: "'DM Mono', monospace", fontSize: 12, color: parseFloat(r.risk_score) > 7 ? "#e85d26" : parseFloat(r.risk_score) > 4 ? "#c9932a" : "#4ade80" }}>
                      risk {parseFloat(r.risk_score || 0).toFixed(1)}
                    </div>
                  </div>
                ))}
              </div>
            )}

            {buildingLoading && (
              <div style={{ padding: 40, textAlign: "center", color: "rgba(255,255,255,0.3)", fontFamily: "'DM Mono', monospace", fontSize: 12 }}>Loading building profile...</div>
            )}

            {building && !buildingLoading && (
              <div className="fade-up">
                {/* Building header */}
                <div style={{ background: "rgba(255,255,255,0.025)", border: "1px solid rgba(255,255,255,0.08)", borderRadius: 14, padding: 24, marginBottom: 16 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 16 }}>
                    <div>
                      <div style={{ fontSize: 10, fontFamily: "'DM Mono', monospace", color: "rgba(255,255,255,0.3)", letterSpacing: "0.12em", textTransform: "uppercase", marginBottom: 6 }}>Building Profile</div>
                      <div style={{ fontFamily: "'Playfair Display', serif", fontSize: 22, fontWeight: 700 }}>{building.building?.address}</div>
                      <div style={{ fontSize: 13, color: "rgba(255,255,255,0.45)", marginTop: 4 }}>{building.building?.neighborhood} · {building.building?.property_type}</div>
                    </div>
                    <div style={{ textAlign: "right" }}>
                      <div style={{ fontFamily: "'Playfair Display', serif", fontSize: 36, fontWeight: 900, color: parseFloat(building.building?.risk_score) > 7 ? "#e85d26" : parseFloat(building.building?.risk_score) > 4 ? "#c9932a" : "#4ade80" }}>
                        {parseFloat(building.building?.risk_score || 0).toFixed(1)}
                      </div>
                      <div style={{ fontSize: 10, fontFamily: "'DM Mono', monospace", color: "rgba(255,255,255,0.3)", letterSpacing: "0.1em" }}>RISK SCORE / 10</div>
                    </div>
                  </div>
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12 }}>
                    {[
                      ["Units", building.building?.units || "—"],
                      ["Year Built", building.building?.year_built || "—"],
                      ["Total Violations", building.violations?.length || 0],
                    ].map(([k, v]) => (
                      <div key={k} style={{ background: "rgba(255,255,255,0.03)", borderRadius: 8, padding: "12px 14px" }}>
                        <div style={{ fontSize: 10, fontFamily: "'DM Mono', monospace", color: "rgba(255,255,255,0.35)", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 4 }}>{k}</div>
                        <div style={{ fontSize: 18, fontWeight: 600, fontFamily: "'Playfair Display', serif" }}>{v}</div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Violations */}
                {building.violations?.length > 0 && (
                  <div style={{ background: "rgba(255,255,255,0.02)", border: "1px solid rgba(255,255,255,0.07)", borderRadius: 14, overflow: "hidden", marginBottom: 16 }}>
                    <div style={{ padding: "14px 20px", borderBottom: "1px solid rgba(255,255,255,0.07)", fontSize: 11, fontFamily: "'DM Mono', monospace", color: "rgba(255,255,255,0.4)", letterSpacing: "0.12em", textTransform: "uppercase" }}>
                      Violation History ({building.violations.length})
                    </div>
                    {building.violations.slice(0, 10).map((v, i) => (
                      <div key={i} style={{ padding: "12px 20px", borderBottom: i < 9 ? "1px solid rgba(255,255,255,0.04)" : "none", display: "flex", gap: 16, alignItems: "flex-start" }}>
                        <div style={{ width: 8, height: 8, borderRadius: "50%", background: v.status === "open" ? "#e85d26" : "rgba(255,255,255,0.2)", flexShrink: 0, marginTop: 4 }} />
                        <div style={{ flex: 1 }}>
                          <div style={{ fontSize: 13, color: "rgba(255,255,255,0.8)", lineHeight: 1.4 }}>{v.complaint_type?.slice(0, 80)}{v.complaint_type?.length > 80 ? "..." : ""}</div>
                          <div style={{ fontSize: 11, fontFamily: "'DM Mono', monospace", color: "rgba(255,255,255,0.3)", marginTop: 3 }}>
                            Filed: {v.date_filed ? new Date(v.date_filed).toLocaleDateString() : "—"}
                            {v.date_closed && ` · Closed: ${new Date(v.date_closed).toLocaleDateString()}`}
                          </div>
                        </div>
                        <span style={{ fontSize: 10, fontFamily: "'DM Mono', monospace", padding: "2px 8px", borderRadius: 4, background: v.status === "open" ? "rgba(232,93,38,0.15)" : "rgba(255,255,255,0.06)", color: v.status === "open" ? "#e85d26" : "rgba(255,255,255,0.4)" }}>
                          {v.status}
                        </span>
                      </div>
                    ))}
                  </div>
                )}

                {/* Similar buildings */}
                {building.similarBuildings?.length > 0 && (
                  <div style={{ background: "rgba(255,255,255,0.02)", border: "1px solid rgba(255,255,255,0.07)", borderRadius: 14, overflow: "hidden" }}>
                    <div style={{ padding: "14px 20px", borderBottom: "1px solid rgba(255,255,255,0.07)", fontSize: 11, fontFamily: "'DM Mono', monospace", color: "rgba(255,255,255,0.4)", letterSpacing: "0.12em", textTransform: "uppercase" }}>
                      Similar Buildings Nearby
                    </div>
                    {building.similarBuildings.map((b, i) => (
                      <div key={i} onClick={() => loadBuilding(b.blklot)}
                        style={{ padding: "12px 20px", borderBottom: i < building.similarBuildings.length - 1 ? "1px solid rgba(255,255,255,0.04)" : "none", display: "flex", alignItems: "center", gap: 12, cursor: "pointer" }}
                        onMouseEnter={e => e.currentTarget.style.background = "rgba(255,255,255,0.03)"}
                        onMouseLeave={e => e.currentTarget.style.background = "transparent"}>
                        <div style={{ flex: 1 }}>
                          <div style={{ fontSize: 13, color: "rgba(255,255,255,0.8)" }}>{b.address}</div>
                          <div style={{ fontSize: 11, fontFamily: "'DM Mono', monospace", color: "rgba(255,255,255,0.3)", marginTop: 2 }}>{b.units} units · built {b.year_built || "—"}</div>
                        </div>
                        <div style={{ fontFamily: "'Playfair Display', serif", fontSize: 20, fontWeight: 700, color: parseFloat(b.risk_score) > 7 ? "#e85d26" : parseFloat(b.risk_score) > 4 ? "#c9932a" : "#4ade80" }}>
                          {parseFloat(b.risk_score || 0).toFixed(1)}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}