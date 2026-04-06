// ─── Config ───────────────────────────────────────────────────────────────────

const DEFAULT_FILTERS = {
  date_preset: "60d",
  category: "All",
  product: "All",
  department: "All",
  channel: "All",
  efc: "All",
  status: "All",
  include_fc1: [],
  exclude_fc1: [],
  include_fc2: [],
  exclude_fc2: [],
  include_bot_action: [],
  exclude_bot_action: [],
};

const KPI_CONFIG = [
  { key: "tickets",              label: "Total tickets",       fmt: "n",  lowerIsBetter: true  },
  { key: "installation_tickets", label: "Installation",        fmt: "n",  lowerIsBetter: true  },
  { key: "bot_resolved",         label: "Bot resolved",        fmt: "n",  lowerIsBetter: false },
  { key: "repeat_tickets",       label: "Repeat tickets",      fmt: "n",  lowerIsBetter: true  },
  { key: "open_tickets",         label: "Open tickets",        fmt: "n",  lowerIsBetter: true  },
  { key: "no_reopen_rate",       label: "No-reopen rate",      fmt: "%",  lowerIsBetter: false },
];

// ─── State ────────────────────────────────────────────────────────────────────

const state = {
  apiBaseUrl: (window.QUBO_APP_CONFIG?.apiBaseUrl || window.location.origin || "").replace(/\/$/, ""),
  filters: { ...DEFAULT_FILTERS },
  issueView: "highest_volume",
  payload: null,
  options: {},
  focusActiveTab: "fc1",
  ruleSearch: {
    include_fc1: "", exclude_fc1: "",
    include_fc2: "", exclude_fc2: "",
    include_bot_action: "", exclude_bot_action: "",
  },
};

// ─── DOM refs ─────────────────────────────────────────────────────────────────

const els = {
  headline:          document.getElementById("headline"),
  summary:           document.getElementById("summary"),
  sourceBadge:       document.getElementById("sourceBadge"),
  lastUpdated:       document.getElementById("lastUpdated"),
  alertBar:          document.getElementById("alertBar"),
  activeRules:       document.getElementById("activeRules"),
  kpiStrip:          document.getElementById("kpiStrip"),
  timelineChart:     document.getElementById("timelineChart"),
  timelineLegend:    document.getElementById("timelineLegend"),
  spotlightCards:    document.getElementById("spotlightCards"),
  productHealthTable:document.getElementById("productHealthTable"),
  issueBoard:        document.getElementById("issueBoard"),
  botFunnel:         document.getElementById("botFunnel"),
  botLeakyIssues:    document.getElementById("botLeakyIssues"),
  botBestIssues:     document.getElementById("botBestIssues"),
  departmentMix:     document.getElementById("departmentMix"),
  channelMix:        document.getElementById("channelMix"),
  statusMix:         document.getElementById("statusMix"),
  installationMix:   document.getElementById("installationMix"),
  qualityCards:      document.getElementById("qualityCards"),
  pipelineHealth:    document.getElementById("pipelineHealth"),
  datePreset:        document.getElementById("datePreset"),
  categoryFilter:    document.getElementById("categoryFilter"),
  productFilter:     document.getElementById("productFilter"),
  efcFilter:         document.getElementById("efcFilter"),
  departmentFilter:  document.getElementById("departmentFilter"),
  channelFilter:     document.getElementById("channelFilter"),
  statusFilter:      document.getElementById("statusFilter"),
  issueTabs:         document.getElementById("issueTabs"),
  qfExcludeInstallation: document.getElementById("qfExcludeInstallation"),
  resetFilters:          document.getElementById("resetFilters"),
};

// ─── Boot ─────────────────────────────────────────────────────────────────────

bindEvents();
loadDashboard();

// ─── Event binding ────────────────────────────────────────────────────────────

function bindEvents() {
  [
    [els.datePreset,       "date_preset"],
    [els.categoryFilter,   "category"],
    [els.productFilter,    "product"],
    [els.departmentFilter, "department"],
    [els.channelFilter,    "channel"],
    [els.efcFilter,        "efc"],
    [els.statusFilter,     "status"],
  ].forEach(([el, key]) => {
    el.addEventListener("change", () => { state.filters[key] = el.value; loadDashboard(); });
  });

  els.resetFilters.addEventListener("click", () => {
    state.filters = structuredClone(DEFAULT_FILTERS);
    Object.keys(state.ruleSearch).forEach((k) => { state.ruleSearch[k] = ""; });
    state.issueView = "highest_volume";
    syncControls();
    renderAdvancedFilters();
    renderActiveRules();
    loadDashboard();
  });

  els.qfExcludeInstallation.addEventListener("click", toggleExcludeInstallation);

  els.issueTabs.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-issue-view]");
    if (!btn) return;
    state.issueView = btn.dataset.issueView;
    syncControls();
    renderIssueBoard(state.payload?.issue_views || {});
  });

  document.getElementById("focusTabs").addEventListener("click", (e) => {
    const btn = e.target.closest("[data-focus-tab]");
    if (!btn) return;
    state.focusActiveTab = btn.dataset.focusTab;
    document.querySelectorAll("[data-focus-tab]").forEach((b) => {
      b.classList.toggle("active", b.dataset.focusTab === state.focusActiveTab);
    });
    renderAdvancedFilters();
  });

  document.getElementById("openFocusModal").addEventListener("click", () => {
    document.getElementById("focusModal").classList.remove("hidden");
    renderAdvancedFilters();
  });
  document.getElementById("dismissFocusModal").addEventListener("click", () => {
    document.getElementById("focusModal").classList.add("hidden");
  });
  document.getElementById("closeFocusModal").addEventListener("click", () => {
    document.getElementById("focusModal").classList.add("hidden");
  });

  document.getElementById("runPipelineBtn").addEventListener("click", async () => {
    const btn = document.getElementById("runPipelineBtn");
    btn.disabled = true;
    btn.textContent = "Starting…";
    try {
      await fetch(apiUrl("/api/pipeline/run"), { method: "POST" });
      btn.textContent = "Running…";
      const poll = setInterval(async () => {
        try {
          const res = await fetch(apiUrl("/api/pipeline/status"));
          const s = await res.json();
          if (!s.running) {
            clearInterval(poll);
            btn.disabled = false;
            btn.textContent = "Run pipeline";
            loadDashboard();
          }
        } catch {
          clearInterval(poll);
          btn.disabled = false;
          btn.textContent = "Run pipeline";
        }
      }, 3000);
    } catch {
      btn.disabled = false;
      btn.textContent = "Run pipeline";
    }
  });
}

// ─── Data loading ─────────────────────────────────────────────────────────────

async function loadDashboard() {
  renderSkeleton();
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(state.filters)) {
    if (Array.isArray(v)) v.forEach((item) => params.append(k, item));
    else params.set(k, String(v));
  }
  try {
    const res = await fetch(`${apiUrl("/api/dashboard")}?${params}`);
    if (!res.ok) throw new Error(`API ${res.status}`);
    const payload = await res.json();
    state.payload = payload;
    state.options = payload.filter_options || {};
    hydrateFilters(state.options);
    renderAdvancedFilters();
    renderActiveRules();
    renderDashboard(payload);
  } catch (err) {
    renderError(err);
  }
}

// ─── Dashboard render ─────────────────────────────────────────────────────────

function renderDashboard(p) {
  const meta     = p.meta || {};
  const pipeline = p.pipeline_health || {};

  // Header
  els.headline.textContent    = meta.title   || "Qubo Support Board";
  els.summary.textContent     = meta.subtitle || "";
  els.sourceBadge.textContent = meta.source_mode === "clickhouse" ? "Live data" : "Sample data";
  els.sourceBadge.className   = "badge" + (meta.source_mode === "clickhouse" ? " live" : " warn");
  els.lastUpdated.textContent = `Refreshed ${fmtDateTime(pipeline.last_run_at || meta.window_end || "")}`;

  // Alert bar — key data quality signals always visible
  const dq = p.data_quality || {};
  const missingProdPct = dq.actionable_issue_rate != null
    ? `${fmtPct(1 - (dq.actionable_issue_rate || 0))} of tickets lack full coding`
    : "";
  els.alertBar.textContent = missingProdPct
    ? `Data quality notice: ${missingProdPct}. Blank-product rows are shown separately in the product table.`
    : "";

  renderKpis(p.kpis || {});
  renderTimeline(p.timeline || []);
  renderSpotlight(p.spotlight || []);
  renderProductHealth(p.product_health || [], p.category_health || []);
  renderIssueBoard(p.issue_views || {});
  renderBotSection(p.bot_summary || {});
  renderMixSection(p.service_ops || {});
  renderQuality(dq);
  renderPipeline(pipeline);
}

// ─── KPI strip ────────────────────────────────────────────────────────────────

function renderKpis(kpis) {
  els.kpiStrip.innerHTML = KPI_CONFIG.map((cfg) => {
    const m = kpis[cfg.key] || { value: 0, change: 0 };
    const val = cfg.fmt === "%" ? fmtPct(m.value) : fmtNum(m.value);
    const delta = Number(m.change || 0);
    const good  = cfg.lowerIsBetter ? delta <= 0 : delta >= 0;
    const deltaHtml = delta !== 0
      ? `<div class="kpi-delta ${good ? "fg-green" : "fg-red"}">${delta > 0 ? "▲" : "▼"} ${fmtPct(Math.abs(delta))} vs prior</div>`
      : `<div class="kpi-delta fg-muted">— vs prior</div>`;
    return `
      <div class="kpi-item">
        <div class="kpi-val">${val}</div>
        <div class="kpi-key">${escHtml(cfg.label)}</div>
        ${deltaHtml}
      </div>`;
  }).join("");
}

// ─── Timeline ─────────────────────────────────────────────────────────────────

const SERIES = [
  { key: "tickets",               label: "Tickets",      color: "#2563eb" },
  { key: "installation_tickets",  label: "Installation", color: "#d97706" },
  { key: "bot_resolved_tickets",  label: "Bot resolved", color: "#16a34a" },
  { key: "repeat_tickets",        label: "Repeat",       color: "#7c3aed" },
];

function renderTimeline(points) {
  if (!points.length) {
    els.timelineChart.innerHTML = empty("No timeline data in selected window.");
    els.timelineLegend.innerHTML = "";
    return;
  }

  const W = 900, H = 220, px = 28, py = 16;
  const maxV = Math.max(...points.flatMap((p) => SERIES.map((s) => +(p[s.key] || 0))), 1);
  const step  = (W - px * 2) / Math.max(points.length - 1, 1);
  const toY   = (v) => H - py - (v / maxV) * (H - py * 2);

  const grid = [0.25, 0.5, 0.75].map((r) => {
    const y = py + r * (H - py * 2);
    return `<line x1="${px}" y1="${y}" x2="${W - px}" y2="${y}" stroke="#e2e8f0" stroke-width="1"/>`;
  }).join("");

  const paths = SERIES.map((s) => {
    const d = points.map((pt, i) => {
      const x = px + step * i;
      const y = toY(+(pt[s.key] || 0));
      return `${i === 0 ? "M" : "L"} ${x} ${y}`;
    }).join(" ");
    return `<path d="${d}" fill="none" stroke="${s.color}" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>`;
  }).join("");

  const interval = Math.ceil(points.length / 7);
  const xlabels = points.map((pt, i) => {
    if (i % interval !== 0 && i !== points.length - 1) return "";
    const x = px + step * i;
    return `<text x="${x}" y="${H - 2}" text-anchor="middle" font-size="10" fill="#94a3b8">${escHtml(shortDate(pt.date))}</text>`;
  }).join("");

  els.timelineChart.innerHTML =
    `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" style="width:100%;height:${H}px">${grid}${paths}${xlabels}</svg>`;

  els.timelineLegend.innerHTML = SERIES.map((s) =>
    `<div class="legend-item"><div class="legend-dot" style="background:${s.color}"></div>${escHtml(s.label)}</div>`
  ).join("");
}

// ─── Spotlight ────────────────────────────────────────────────────────────────

function renderSpotlight(items) {
  if (!items.length) {
    els.spotlightCards.innerHTML = empty("No highlights for the current view.");
    return;
  }
  els.spotlightCards.innerHTML = items.map((item) => `
    <div class="alert-item">
      <div class="alert-item-title">${escHtml(item.title)}</div>
      <div class="alert-item-detail">${escHtml(item.detail)}</div>
    </div>`).join("");
}

// ─── Product health table ─────────────────────────────────────────────────────

function renderProductHealth(productRows, categoryRows) {
  // Combine: show category-level summary, then product family rows
  // Blank/unknown product rows get dimmed and footnoted
  const allRows = productRows.length ? productRows : categoryRows;
  if (!allRows.length) {
    els.productHealthTable.innerHTML = empty("No product data in selected window.");
    return;
  }

  const maxTickets = Math.max(...allRows.map((r) => +(r.tickets || 0)), 1);

  const BLANK_NAMES = new Set(["", "-", "null", "blank product", "blank", "other", "others", "unknown"]);
  const isBlank = (r) => BLANK_NAMES.has((r.product_family || r.product_category || "").trim().toLowerCase());

  const mainRows  = allRows.filter((r) => !isBlank(r));
  const blankRows = allRows.filter((r) => isBlank(r));

  const renderRow = (r, dimmed = false) => {
    const titleKey   = r.product_family ?? r.product_category ?? "Other";
    const subKey     = r.product_category && r.product_family ? r.product_category : "";
    const pct        = (+(r.tickets || 0) / maxTickets * 100).toFixed(1);
    const instRate   = +(r.installation_rate || 0);
    const repeatRate = +(r.repeat_rate || 0);
    const botRes     = +(r.bot_resolved_rate || 0);
    const openRate   = +(r.open_rate || r.open_tickets > 0 ? (r.open_tickets / Math.max(r.tickets, 1)) : 0);

    const instClass   = instRate >= 0.30 ? "fg-red" : instRate >= 0.12 ? "fg-amber" : "";
    const repeatClass = repeatRate >= 0.20 ? "fg-red" : repeatRate >= 0.10 ? "fg-amber" : "";

    return `
      <tr class="${dimmed ? "dimmed" : ""}">
        <td class="num rank fg-muted">${dimmed ? "—" : ""}</td>
        <td>
          <div class="cell-name">${escHtml(titleKey)}</div>
          ${subKey ? `<div class="cell-sub">${escHtml(subKey)}</div>` : ""}
          <div class="bar-track"><div class="bar-fill" style="width:${pct}%"></div></div>
        </td>
        <td class="num">${fmtNum(r.tickets)}</td>
        <td class="num ${instClass}">${fmtPct(instRate)}</td>
        <td class="num ${repeatClass}">${fmtPct(repeatRate)}</td>
        <td class="num">${fmtPct(botRes)}</td>
        <td class="num fg-muted">${escHtml(r.top_efc || "—")}</td>
      </tr>`;
  };

  els.productHealthTable.innerHTML = `
    <table class="data-table">
      <thead>
        <tr>
          <th class="rank">#</th>
          <th>Product</th>
          <th class="num">Tickets</th>
          <th class="num">Install%</th>
          <th class="num">Repeat%</th>
          <th class="num">Bot res%</th>
          <th class="num">Top EFC</th>
        </tr>
      </thead>
      <tbody>
        ${mainRows.map((r, i) => renderRow(r).replace('<td class="num rank fg-muted">—</td>', `<td class="num rank">${i + 1}</td>`)).join("")}
        ${blankRows.length ? `
          <tr><td colspan="7" style="padding:10px 10px 2px;font-size:11px;color:var(--muted);font-weight:600;text-transform:uppercase;letter-spacing:.07em">Unclassified / Blank product (data quality gap)</td></tr>
          ${blankRows.map((r) => renderRow(r, true)).join("")}
        ` : ""}
      </tbody>
    </table>
  `;
}

// ─── Issue board ──────────────────────────────────────────────────────────────

function renderIssueBoard(issueViews) {
  const issues = issueViews[state.issueView] || [];
  if (!issues.length) {
    els.issueBoard.innerHTML = empty("No issues found for this view.");
    return;
  }

  const maxVol = Math.max(...issues.map((r) => +(r.volume || 0)), 1);

  els.issueBoard.innerHTML = `
    <table class="data-table">
      <thead>
        <tr>
          <th class="rank">#</th>
          <th>Issue (FC2)</th>
          <th>Product</th>
          <th>EFC</th>
          <th class="num">Tickets</th>
          <th class="num">Install%</th>
          <th class="num">Repeat%</th>
          <th class="num">Transfer%</th>
          <th class="num">Bot res%</th>
        </tr>
      </thead>
      <tbody>
        ${issues.map((issue, i) => {
          const instRate     = +(issue.installation_rate  || 0);
          const repeatRate   = +(issue.repeat_rate        || 0);
          const transferRate = +(issue.bot_transfer_rate  || 0);
          const botRes       = +(issue.bot_resolved_rate  || 0);
          const pct          = (+(issue.volume || 0) / maxVol * 100).toFixed(1);

          const instClass     = instRate     >= 0.30 ? "fg-red"   : instRate     >= 0.12 ? "fg-amber" : "";
          const repeatClass   = repeatRate   >= 0.20 ? "fg-red"   : repeatRate   >= 0.10 ? "fg-amber" : "";
          const transferClass = transferRate >= 0.25 ? "fg-red"   : transferRate >= 0.15 ? "fg-amber" : "";
          const botResClass   = botRes       >= 0.50 ? "fg-green" : "";

          return `
            <tr>
              <td class="rank">${i + 1}</td>
              <td>
                <div class="cell-name">${escHtml(issue.fault_code_level_2 || "Unclassified")}</div>
                <div class="bar-track"><div class="bar-fill" style="width:${pct}%"></div></div>
              </td>
              <td>
                <div class="cell-name">${escHtml(issue.product_family || "—")}</div>
                <div class="cell-sub">${escHtml(issue.product_category || "")}</div>
              </td>
              <td class="fg-muted" style="font-size:12px">${escHtml(issue.executive_fault_code || "—")}</td>
              <td class="num">${fmtNum(issue.volume)}</td>
              <td class="num ${instClass}">${fmtPct(instRate)}</td>
              <td class="num ${repeatClass}">${fmtPct(repeatRate)}</td>
              <td class="num ${transferClass}">${fmtPct(transferRate)}</td>
              <td class="num ${botResClass}">${fmtPct(botRes)}</td>
            </tr>`;
        }).join("")}
      </tbody>
    </table>`;
}

// ─── Bot section ──────────────────────────────────────────────────────────────

function renderBotSection(botSummary) {
  renderBotFunnel(botSummary.overview || {});
  renderIssueList(els.botLeakyIssues, botSummary.leaky_issues  || [], "No high-transfer issues found.");
  renderIssueList(els.botBestIssues,  botSummary.best_issues   || [], "No high bot-resolved issues found.");
}

function renderBotFunnel(ov) {
  const total      = +(ov.chat_tickets || 0);
  const resolved   = +(ov.bot_resolved_tickets    || 0);
  const transferred= +(ov.bot_transferred_tickets || 0);
  const blank      = +(ov.blank_chat_tickets       || 0);

  if (!total) {
    els.botFunnel.innerHTML = empty("No chat data in the current window.");
    return;
  }

  const rows = [
    { label: "Chat sessions", n: total,       pct: 1,              color: "#2563eb" },
    { label: "Bot resolved",  n: resolved,    pct: resolved / total,    color: "#16a34a" },
    { label: "Transferred",   n: transferred, pct: transferred / total, color: "#d97706" },
    { label: "Blank chat",    n: blank,       pct: blank / total,       color: "#94a3b8" },
  ];

  els.botFunnel.innerHTML = rows.map((r) => `
    <div class="funnel-row">
      <div class="funnel-label">${escHtml(r.label)}</div>
      <div class="funnel-bar">
        <div class="funnel-fill" style="width:${(r.pct * 100).toFixed(1)}%;background:${r.color};opacity:0.7"></div>
      </div>
      <div class="funnel-val">
        ${fmtNum(r.n)}
        ${r.pct < 1 ? `<span class="funnel-pct"> ${fmtPct(r.pct)}</span>` : ""}
      </div>
    </div>`).join("");
}

function renderIssueList(container, issues, emptyMsg) {
  if (!issues.length) { container.innerHTML = empty(emptyMsg); return; }
  container.innerHTML = issues.slice(0, 5).map((issue) => `
    <div class="issue-row">
      <div class="issue-row-name">${escHtml(issue.product_family || "—")} · ${escHtml(issue.fault_code_level_2 || "Unclassified")}</div>
      <div class="issue-row-meta">${escHtml(issue.executive_fault_code || "")} · Bot res ${fmtPct(issue.bot_resolved_rate)} · Transfer ${fmtPct(issue.bot_transfer_rate)}</div>
    </div>`).join("");
}

// ─── Mix bars ─────────────────────────────────────────────────────────────────

function renderMixSection(ops) {
  renderMixList(els.departmentMix,  ops.department_mix   || []);
  renderMixList(els.channelMix,     ops.channel_mix      || []);
  renderMixList(els.statusMix,      ops.status_mix       || []);
  renderMixList(els.installationMix,ops.installation_mix || []);
}

function renderMixList(container, items) {
  if (!items.length) { container.innerHTML = empty("No data."); return; }
  const maxN = Math.max(...items.map((r) => +(r.count || 0)), 1);
  container.innerHTML = items.map((r) => `
    <div class="mix-bar-row">
      <div>
        <div class="mix-bar-label">${escHtml(r.label || "Unknown")}</div>
        <div class="mix-bar-track">
          <div class="mix-bar-fill" style="width:${(+(r.count || 0) / maxN * 100).toFixed(1)}%"></div>
        </div>
      </div>
      <div class="mix-bar-num">
        ${fmtNum(r.count)}
        <div class="mix-bar-pct">${fmtPct(r.share)}</div>
      </div>
    </div>`).join("");
}

// ─── Data quality ─────────────────────────────────────────────────────────────

function renderQuality(dq) {
  const cards = [
    {
      val: fmtPct(dq.actionable_issue_rate || 0),
      key: "Actionable issue rate",
      detail: `${fmtNum(dq.actionable_issue_tickets || 0)} tickets with full issue coding. Target: high.`,
    },
    {
      val: fmtPct(dq.usable_issue_rate || 0),
      key: "Usable issue coverage",
      detail: `${fmtNum(dq.usable_issue_tickets || 0)} tickets have enough coding for issue views.`,
    },
    {
      val: fmtNum(dq.blank_fault_code_l1_tickets || 0),
      key: "Blank FC1 tickets",
      detail: "No FC1 coding. These fall out of issue analysis.",
    },
    {
      val: fmtNum(dq.blank_fault_code_l2_tickets || 0),
      key: "Blank FC2 tickets",
      detail: "~31% of tickets are missing FC2 per source data reference.",
    },
    {
      val: fmtNum(dq.missing_issue_outside_bot_tickets || 0),
      key: "Missing issue (non-chat)",
      detail: "Non-chat tickets with no issue coding.",
    },
    {
      val: fmtNum(dq.email_department_reassigned_tickets || 0),
      key: "Email dept remapped",
      detail: "Rows moved from Email dept → Call Center team + Email channel.",
    },
  ];

  els.qualityCards.innerHTML = cards.map((c) => `
    <div class="quality-stat">
      <div class="quality-stat-val">${escHtml(c.val)}</div>
      <div class="quality-stat-key">${escHtml(c.key)}</div>
      <div class="quality-stat-detail">${escHtml(c.detail)}</div>
    </div>`).join("");
}

// ─── Pipeline ─────────────────────────────────────────────────────────────────

function renderPipeline(pipeline) {
  const runs    = pipeline.recent_runs || [];
  const dotClass= pipeline.status === "ok" ? "" : pipeline.status === "running" ? "warn" : "error";
  els.pipelineHealth.innerHTML = `
    <div class="pipeline-summary">
      <div class="pipeline-dot ${dotClass}"></div>
      <strong>${escHtml(pipeline.status || "Unknown")}</strong>
      <span class="subtle">· Last run ${fmtDateTime(pipeline.last_run_at || "")}</span>
      <span class="subtle">· ${fmtNum(pipeline.rows_inserted || 0)} rows inserted</span>
    </div>
    <div class="pipeline-recent">
      ${runs.map((r) => `
        <div class="pipeline-run-row">
          <span class="pipeline-run-status">${escHtml(r.status || "")}</span>
          <span>${escHtml(fmtDateTime(r.finished_at || r.started_at || ""))}</span>
          <span>${fmtNum(r.rows_inserted || 0)} rows</span>
        </div>`).join("")}
    </div>`;
}

// ─── Focus rules modal ────────────────────────────────────────────────────────

function renderAdvancedFilters() {
  const TAB = {
    fc1:       { title: "FC1",        include_key: "include_fc1",        exclude_key: "exclude_fc1",        options: state.options.fc1         || [] },
    fc2:       { title: "FC2",        include_key: "include_fc2",        exclude_key: "exclude_fc2",        options: state.options.fc2         || [] },
    bot_action:{ title: "Bot action", include_key: "include_bot_action", exclude_key: "exclude_bot_action", options: state.options.bot_actions || [] },
  };
  const cfg = TAB[state.focusActiveTab] || TAB.fc1;
  const inc = document.getElementById("focusIncludePane");
  const exc = document.getElementById("focusExcludePane");
  if (!inc || !exc) return;
  renderRuleCard(inc, { title: cfg.title, key: cfg.include_key, opposite: cfg.exclude_key, mode: "include", options: cfg.options });
  renderRuleCard(exc, { title: cfg.title, key: cfg.exclude_key, opposite: cfg.include_key, mode: "exclude", options: cfg.options });
}

function renderRuleCard(container, cfg) {
  const selected = state.filters[cfg.key];
  const blocked  = new Set(state.filters[cfg.opposite]);
  const search   = (state.ruleSearch[cfg.key] || "").trim().toLowerCase();
  const available= cfg.options.filter((v) => !blocked.has(v) && (!search || v.toLowerCase().includes(search)));

  container.innerHTML = `
    <div class="rule-card">
      <div class="rule-card-head">
        <div class="rule-title">${escHtml(cfg.title)} — ${cfg.mode}</div>
        <button class="rule-clear" type="button" data-clear="${cfg.key}">Clear</button>
      </div>
      <div class="rule-selection">
        ${selected.length
          ? selected.map((v) => `<span class="rule-chip ${cfg.mode}">${escHtml(v)}</span>`).join("")
          : '<span class="subtle">None selected</span>'}
      </div>
      <input class="rule-search" type="search" placeholder="Search ${escHtml(cfg.title)}" data-rule-search="${cfg.key}" value="${escHtml(state.ruleSearch[cfg.key] || "")}">
      <div class="rule-option-list">
        ${available.map((v) => `
          <label class="rule-option ${selected.includes(v) ? `active-${cfg.mode}` : ""}">
            <input type="checkbox" data-rule-key="${cfg.key}" value="${escHtml(v)}" ${selected.includes(v) ? "checked" : ""}>
            <span>${escHtml(v)}</span>
          </label>`).join("")}
      </div>
    </div>`;

  const searchBox = container.querySelector("[data-rule-search]");
  searchBox?.addEventListener("input", () => {
    state.ruleSearch[cfg.key] = searchBox.value;
    renderAdvancedFilters();
  });

  container.querySelectorAll("[data-rule-key]").forEach((input) => {
    input.addEventListener("change", () => {
      const vals = new Set(state.filters[input.dataset.ruleKey]);
      if (input.checked) vals.add(input.value); else vals.delete(input.value);
      state.filters[input.dataset.ruleKey] = [...vals];
      renderAdvancedFilters();
      renderActiveRules();
      loadDashboard();
    });
  });

  container.querySelector("[data-clear]")?.addEventListener("click", () => {
    state.filters[cfg.key] = [];
    renderAdvancedFilters();
    renderActiveRules();
    loadDashboard();
  });
}

// ─── Quick filter helpers ─────────────────────────────────────────────────────

function isExcludeInstallationActive() {
  return (
    state.filters.exclude_fc1.some((v) => /instal/i.test(v)) ||
    state.filters.exclude_fc2.some((v) => /instal/i.test(v))
  );
}

function toggleExcludeInstallation() {
  if (isExcludeInstallationActive()) {
    state.filters.exclude_fc1 = state.filters.exclude_fc1.filter((v) => !/instal/i.test(v));
    state.filters.exclude_fc2 = state.filters.exclude_fc2.filter((v) => !/instal/i.test(v));
  } else {
    const i1 = (state.options.fc1 || []).filter((v) => /instal/i.test(v));
    const i2 = (state.options.fc2 || []).filter((v) => /instal/i.test(v));
    state.filters.include_fc1 = state.filters.include_fc1.filter((v) => !/instal/i.test(v));
    state.filters.include_fc2 = state.filters.include_fc2.filter((v) => !/instal/i.test(v));
    state.filters.exclude_fc1 = [...new Set([...state.filters.exclude_fc1, ...i1])];
    state.filters.exclude_fc2 = [...new Set([...state.filters.exclude_fc2, ...i2])];
  }
  syncQuickPills();
  renderAdvancedFilters();
  renderActiveRules();
  loadDashboard();
}

function syncQuickPills() {
  els.qfExcludeInstallation.classList.toggle("active", isExcludeInstallationActive());
}

// ─── Active rules chips ───────────────────────────────────────────────────────

function renderActiveRules() {
  const chips = [];

  const singles = [
    [state.filters.date_preset !== "60d",   `Period: ${datePresetLabel(state.filters.date_preset)}`],
    [state.filters.category   !== "All",    `Category: ${state.filters.category}`],
    [state.filters.product    !== "All",    `Product: ${state.filters.product}`],
    [state.filters.efc        !== "All",    `EFC: ${state.filters.efc}`],
    [state.filters.department !== "All",    `Team: ${state.filters.department}`],
    [state.filters.channel    !== "All",    `Channel: ${state.filters.channel}`],
    [state.filters.status     !== "All",    `Status: ${state.filters.status}`],
  ];
  singles.forEach(([show, label]) => {
    if (show) chips.push(`<span class="chip">${escHtml(label)}</span>`);
  });

  if (isExcludeInstallationActive()) chips.push(`<span class="chip exclude">Excl. Installation</span>`);

  const noInstFc1 = state.filters.exclude_fc1.filter((v) => !/instal/i.test(v));
  const noInstFc2 = state.filters.exclude_fc2.filter((v) => !/instal/i.test(v));

  pushChips(chips, "Incl FC1",  state.filters.include_fc1, "include");
  pushChips(chips, "Excl FC1",  noInstFc1,                  "exclude");
  pushChips(chips, "Incl FC2",  state.filters.include_fc2, "include");
  pushChips(chips, "Excl FC2",  noInstFc2,                  "exclude");
  pushChips(chips, "Incl bot",  state.filters.include_bot_action, "include");
  pushChips(chips, "Excl bot",  state.filters.exclude_bot_action, "exclude");

  els.activeRules.innerHTML = chips.join("");
}

function pushChips(arr, prefix, values, mode) {
  values.forEach((v) => arr.push(`<span class="chip ${mode}">${escHtml(`${prefix}: ${v}`)}</span>`));
}

// ─── Hydrate filter dropdowns ─────────────────────────────────────────────────

function hydrateFilters(options) {
  setOpts(els.categoryFilter,   ["All", ...(options.categories || [])], state.filters.category);
  setOpts(els.productFilter,    ["All", ...(options.products   || [])], state.filters.product);
  setOpts(els.efcFilter,        ["All", ...(options.efcs       || [])], state.filters.efc);
  setOpts(els.departmentFilter, ["All", ...(options.departments|| [])], state.filters.department);
  setOpts(els.channelFilter,    ["All", ...(options.channels   || [])], state.filters.channel);
  setOpts(els.statusFilter,     ["All", ...(options.statuses   || [])], state.filters.status);
  syncControls();
}

function setOpts(select, options, selected) {
  select.innerHTML = options.map((v) => `<option value="${escHtml(v)}">${escHtml(v)}</option>`).join("");
  select.value = options.includes(selected) ? selected : "All";
}

function syncControls() {
  els.datePreset.value       = state.filters.date_preset;
  els.categoryFilter.value   = state.filters.category;
  els.productFilter.value    = state.filters.product;
  els.efcFilter.value        = state.filters.efc;
  els.departmentFilter.value = state.filters.department;
  els.channelFilter.value    = state.filters.channel;
  els.statusFilter.value     = state.filters.status;
  els.issueTabs.querySelectorAll(".tab").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.issueView === state.issueView);
  });
  syncQuickPills();
}

// ─── Loading / error states ───────────────────────────────────────────────────

function renderSkeleton() {
  const p = `<div class="empty-state">Loading…</div>`;
  [
    els.kpiStrip, els.timelineChart, els.spotlightCards, els.productHealthTable,
    els.issueBoard, els.botFunnel, els.botLeakyIssues, els.botBestIssues,
    els.departmentMix, els.channelMix, els.statusMix, els.installationMix,
    els.qualityCards, els.pipelineHealth,
  ].forEach((el) => { el.innerHTML = p; });
}

function renderError(err) {
  const msg = err?.message || "Failed to load.";
  els.headline.textContent  = "Board unavailable";
  els.summary.textContent   = msg;
  els.sourceBadge.textContent = "Error";
  els.sourceBadge.className = "badge warn";
  [
    els.kpiStrip, els.timelineChart, els.spotlightCards, els.productHealthTable,
    els.issueBoard, els.botFunnel, els.botLeakyIssues, els.botBestIssues,
    els.departmentMix, els.channelMix, els.statusMix, els.installationMix,
    els.qualityCards, els.pipelineHealth,
  ].forEach((el) => { el.innerHTML = `<div class="error-state">${escHtml(msg)}</div>`; });
}

// ─── Utility ──────────────────────────────────────────────────────────────────

function apiUrl(path) { return `${state.apiBaseUrl}${path}`; }

function empty(text) { return `<div class="empty-state">${escHtml(text)}</div>`; }

function fmtNum(v) { return new Intl.NumberFormat("en-IN").format(Number(v || 0)); }

function fmtPct(v) { return `${(Number(v || 0) * 100).toFixed(1)}%`; }

function shortDate(v) {
  if (!v) return "";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return String(v);
  return d.toLocaleDateString("en-IN", { day: "numeric", month: "short" });
}

function fmtDateTime(v) {
  if (!v || v === "Unknown") return "—";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return String(v);
  return d.toLocaleString("en-IN", { day: "2-digit", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit" });
}

function datePresetLabel(v) {
  return { "14d": "Last 14 days", "30d": "Last 30 days", "history": "Full history" }[v] || "Last 60 days";
}

function escHtml(v) {
  return String(v ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#x27;");
}
