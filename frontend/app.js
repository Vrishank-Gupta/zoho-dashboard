const DEFAULT_FILTERS = {
  date_start: "",
  date_end: "",
  categories: [],
  products: [],
  efcs: [],
  departments: [],
  channels: [],
  statuses: [],
  bot_actions: [],
  include_fc1: [],
  exclude_fc1: [],
  include_fc2: [],
  exclude_fc2: [],
  include_bot_action: [],
  exclude_bot_action: [],
};

const KPI_CONFIG = [
  { key: "tickets", label: "Tickets", format: "number", lowerIsBetter: false },
  { key: "installation_tickets", label: "Installation tickets", format: "number", lowerIsBetter: false },
  { key: "bot_resolved", label: "Bot resolved", format: "number", lowerIsBetter: false },
  { key: "repeat_tickets", label: "Repeat tickets", format: "number", lowerIsBetter: true },
  { key: "open_tickets", label: "Open tickets", format: "number", lowerIsBetter: true },
  { key: "no_reopen_rate", label: "No-reopen rate", format: "percent", lowerIsBetter: false },
];

const CONTROLS = [
  { key: "categories", label: "Category", optionsKey: "categories" },
  { key: "products", label: "Product", optionsKey: "products" },
  { key: "efcs", label: "EFC", optionsKey: "efcs" },
  { key: "departments", label: "Support team", optionsKey: "departments" },
  { key: "channels", label: "Channel", optionsKey: "channels" },
  { key: "statuses", label: "Status", optionsKey: "statuses" },
  { key: "bot_actions", label: "Bot action", optionsKey: "bot_actions" },
  { key: "include_fc1", label: "FC1 include", optionsKey: "fc1", oppositeKey: "exclude_fc1" },
  { key: "exclude_fc1", label: "FC1 exclude", optionsKey: "fc1", oppositeKey: "include_fc1" },
  { key: "include_fc2", label: "FC2 include", optionsKey: "fc2", oppositeKey: "exclude_fc2" },
  { key: "exclude_fc2", label: "FC2 exclude", optionsKey: "fc2", oppositeKey: "include_fc2" },
  { key: "include_bot_action", label: "Bot action include", optionsKey: "bot_actions", oppositeKey: "exclude_bot_action" },
  { key: "exclude_bot_action", label: "Bot action exclude", optionsKey: "bot_actions", oppositeKey: "include_bot_action" },
];

const ISSUE_VIEWS = [
  { key: "highest_volume", label: "By volume" },
  { key: "bot_leakage", label: "High transfer" },
  { key: "repeat_heavy", label: "Repeat-heavy" },
  { key: "installation_tickets", label: "Installation" },
];

const PRODUCT_VIEWS = [
  { key: "product", label: "By product" },
  { key: "category", label: "By category" },
];

const PRODUCT_SORTS = [
  { key: "tickets", label: "Volume" },
  { key: "installation_rate", label: "Installation %" },
  { key: "open_rate", label: "Open %" },
  { key: "bot_resolved_rate", label: "Bot resolved %" },
];

const TIMELINE_METRICS = [
  { key: "tickets", label: "Tickets" },
  { key: "installation_tickets", label: "Installation" },
  { key: "bot_resolved_tickets", label: "Bot resolved" },
  { key: "repeat_tickets", label: "Repeat" },
];

const BUCKET_MODES = [
  { key: "auto", label: "Auto" },
  { key: "weekly", label: "Weekly" },
  { key: "monthly", label: "Monthly" },
];

const QUICK_PRESETS = [
  { key: "30d", label: "Last 30 days", days: 29 },
  { key: "60d", label: "Last 60 days", days: 59 },
  { key: "90d", label: "Last 90 days", days: 89 },
  { key: "all", label: "All data", days: null },
];

const state = {
  apiBaseUrl: (window.QUBO_APP_CONFIG?.apiBaseUrl || window.location.origin || "").replace(/\/$/, ""),
  filters: structuredClone(DEFAULT_FILTERS),
  payload: null,
  options: {},
  openFilter: null,
  searches: {},
  issueView: "highest_volume",
  productView: "product",
  productSort: "tickets",
  timelineMetric: "tickets",
  timelineBucket: "auto",
  botBucket: "auto",
  activePreset: "60d",
};

const els = {
  headline: document.getElementById("headline"),
  summary: document.getElementById("summary"),
  sourceBadge: document.getElementById("sourceBadge"),
  lastUpdated: document.getElementById("lastUpdated"),
  dateStart: document.getElementById("dateStart"),
  dateEnd: document.getElementById("dateEnd"),
  quickPresets: document.getElementById("quickPresets"),
  filterGrid: document.getElementById("filterGrid"),
  activeChips: document.getElementById("activeChips"),
  resetFilters: document.getElementById("resetFilters"),
  kpiStrip: document.getElementById("kpiStrip"),
  timelineChart: document.getElementById("timelineChart"),
  spotlightCards: document.getElementById("spotlightCards"),
  productHealthTable: document.getElementById("productHealthTable"),
  issueBoard: document.getElementById("issueBoard"),
  botOverview: document.getElementById("botOverview"),
  botTrendChart: document.getElementById("botTrendChart"),
  botLeakyIssues: document.getElementById("botLeakyIssues"),
  botBestIssues: document.getElementById("botBestIssues"),
  departmentMix: document.getElementById("departmentMix"),
  channelMix: document.getElementById("channelMix"),
  statusMix: document.getElementById("statusMix"),
  installationMix: document.getElementById("installationMix"),
  pipelineHealth: document.getElementById("pipelineHealth"),
  runPipelineBtn: document.getElementById("runPipelineBtn"),
  issueTabs: document.getElementById("issueTabs"),
  productViewTabs: document.getElementById("productViewTabs"),
  productSortTabs: document.getElementById("productSortTabs"),
  timelineMetricTabs: document.getElementById("timelineMetricTabs"),
  timelineBucketTabs: document.getElementById("timelineBucketTabs"),
  botBucketTabs: document.getElementById("botBucketTabs"),
  drilldownModal: document.getElementById("drilldownModal"),
  drilldownEyebrow: document.getElementById("drilldownEyebrow"),
  drilldownTitle: document.getElementById("drilldownTitle"),
  drilldownSubtitle: document.getElementById("drilldownSubtitle"),
  drilldownBody: document.getElementById("drilldownBody"),
};

boot();

function boot() {
  bindEvents();
  renderSegmented(els.issueTabs, ISSUE_VIEWS, state.issueView, (value) => {
    state.issueView = value;
    renderIssueBoard(state.payload?.issue_views || {});
  });
  renderSegmented(els.productViewTabs, PRODUCT_VIEWS, state.productView, (value) => {
    state.productView = value;
    renderProductHealth();
  });
  renderSegmented(els.productSortTabs, PRODUCT_SORTS, state.productSort, (value) => {
    state.productSort = value;
    renderProductHealth();
  });
  renderSegmented(els.timelineMetricTabs, TIMELINE_METRICS, state.timelineMetric, (value) => {
    state.timelineMetric = value;
    renderTimeline(state.payload?.timeline || []);
  });
  renderSegmented(els.timelineBucketTabs, BUCKET_MODES, state.timelineBucket, (value) => {
    state.timelineBucket = value;
    renderTimeline(state.payload?.timeline || []);
  });
  renderSegmented(els.botBucketTabs, BUCKET_MODES, state.botBucket, (value) => {
    state.botBucket = value;
    renderBotTrend(state.payload?.timeline || []);
  });
  loadDashboard();
}

function bindEvents() {
  els.resetFilters.addEventListener("click", () => {
    state.filters = structuredClone(DEFAULT_FILTERS);
    state.searches = {};
    state.activePreset = "60d";
    renderDateToolbar();
    renderFilterControls();
    renderActiveChips();
    loadDashboard();
  });

  els.dateStart.addEventListener("change", () => {
    state.filters.date_start = els.dateStart.value;
    state.activePreset = "";
    loadDashboard();
  });

  els.dateEnd.addEventListener("change", () => {
    state.filters.date_end = els.dateEnd.value;
    state.activePreset = "";
    loadDashboard();
  });

  document.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;

    const trigger = target.closest("[data-filter-trigger]");
    if (trigger) {
      const key = trigger.dataset.filterTrigger;
      state.openFilter = state.openFilter === key ? null : key;
      renderFilterControls();
      return;
    }

    const removeChip = target.closest("[data-remove-chip]");
    if (removeChip) {
      const key = removeChip.dataset.removeChip;
      const value = removeChip.dataset.value;
      if (key && value) {
        state.filters[key] = (state.filters[key] || []).filter((item) => item !== value);
        renderFilterControls();
        renderActiveChips();
        loadDashboard();
      }
      return;
    }

    if (target.closest("[data-close-modal]")) {
      closeDrilldown();
      return;
    }

    if (!target.closest("[data-filter-panel]")) {
      state.openFilter = null;
      renderFilterControls();
    }
  });

  els.runPipelineBtn.addEventListener("click", runPipeline);
  document.getElementById("closeDrilldown").addEventListener("click", closeDrilldown);
}

async function loadDashboard() {
  renderLoading();
  const params = buildQueryParams(state.filters);
  try {
    const response = await fetch(`${apiUrl("/api/dashboard")}?${params.toString()}`);
    if (!response.ok) throw new Error(`API ${response.status}`);
    const payload = await response.json();
    state.payload = payload;
    state.options = payload.filter_options || {};
    reconcileFilterState();
    renderDateToolbar();
    renderFilterControls();
    renderActiveChips();
    renderDashboard(payload);
  } catch (error) {
    renderError(error);
  }
}

function renderDashboard(payload) {
  const meta = payload.meta || {};
  const pipeline = payload.pipeline_health || {};
  els.headline.textContent = meta.title || "Qubo Support Executive Board";
  els.summary.textContent = meta.subtitle || "";
  els.sourceBadge.textContent = meta.source_mode === "clickhouse" ? "Live data" : "Sample data";
  els.sourceBadge.className = `status-badge ${meta.source_mode === "clickhouse" ? "live" : "warn"}`;
  els.lastUpdated.textContent = `Last refresh: ${fmtDateTime(pipeline.last_run_at || meta.window_end || "")}`;
  renderKpis(payload.kpis || {});
  renderTimeline(payload.timeline || []);
  renderSpotlight(payload.spotlight || []);
  renderProductHealth();
  renderIssueBoard(payload.issue_views || {});
  renderBotSummary(payload.bot_summary || {});
  renderMixList(els.departmentMix, payload.service_ops?.department_mix || []);
  renderMixList(els.channelMix, payload.service_ops?.channel_mix || []);
  renderMixList(els.statusMix, payload.service_ops?.status_mix || []);
  renderMixList(els.installationMix, payload.service_ops?.installation_mix || []);
  renderPipeline(pipeline);
}

function renderDateToolbar() {
  const bounds = state.options.date_bounds || {};
  els.dateStart.min = bounds.min || "";
  els.dateStart.max = bounds.max || "";
  els.dateEnd.min = bounds.min || "";
  els.dateEnd.max = bounds.max || "";
  if (!state.filters.date_start && bounds.max) state.filters.date_start = clampIsoDate(shiftIsoDate(bounds.max, -59), bounds.min, bounds.max);
  if (!state.filters.date_end && bounds.max) state.filters.date_end = bounds.max;
  els.dateStart.value = state.filters.date_start || "";
  els.dateEnd.value = state.filters.date_end || "";
  els.quickPresets.innerHTML = QUICK_PRESETS.map((preset) => `
    <button class="preset-btn ${state.activePreset === preset.key ? "active" : ""}" type="button" data-preset="${preset.key}">${escHtml(preset.label)}</button>
  `).join("");
  els.quickPresets.querySelectorAll("[data-preset]").forEach((button) => {
    button.addEventListener("click", () => {
      if (!bounds.max) return;
      state.activePreset = button.dataset.preset || "";
      if (state.activePreset === "all") {
        state.filters.date_start = bounds.min || bounds.max;
        state.filters.date_end = bounds.max;
      } else {
        const preset = QUICK_PRESETS.find((item) => item.key === state.activePreset);
        state.filters.date_start = clampIsoDate(shiftIsoDate(bounds.max, -(preset?.days || 0)), bounds.min, bounds.max);
        state.filters.date_end = bounds.max;
      }
      els.dateStart.value = state.filters.date_start;
      els.dateEnd.value = state.filters.date_end;
      loadDashboard();
    });
  });
}

function renderFilterControls() {
  els.filterGrid.innerHTML = CONTROLS.map((control) => {
    const options = getControlOptions(control);
    const selected = state.filters[control.key] || [];
    const summary = summarizeSelection(options, selected);
    const search = state.searches[control.key] || "";
    const blocked = new Set(control.oppositeKey ? (state.filters[control.oppositeKey] || []) : []);
    const visible = options.filter((item) => !blocked.has(item.label) && item.label.toLowerCase().includes(search.toLowerCase()));
    return `
      <div class="filter-control ${state.openFilter === control.key ? "open" : ""}">
        <div class="control-label">${escHtml(control.label)}</div>
        <button class="control-trigger" type="button" data-filter-trigger="${escHtml(control.key)}">
          <span class="control-summary">
            <span class="control-main">${escHtml(summary.main)}</span>
            ${summary.count ? `<span class="control-count">${escHtml(summary.count)}</span>` : ""}
          </span>
          <span class="control-caret">${state.openFilter === control.key ? "▲" : "▼"}</span>
        </button>
        <div class="control-panel" data-filter-panel="${escHtml(control.key)}">
          <div class="control-tools">
            <input type="search" value="${escHtml(search)}" data-filter-search="${escHtml(control.key)}" placeholder="Search ${escHtml(control.label)}">
            <button type="button" data-filter-clear="${escHtml(control.key)}">Clear</button>
          </div>
          <div class="option-list">
            ${visible.length ? visible.map((item) => `
              <label class="option-item">
                <input type="checkbox" data-filter-option="${escHtml(control.key)}" value="${escHtml(item.label)}" ${selected.includes(item.label) ? "checked" : ""}>
                <span>${escHtml(item.label)}</span>
                <span class="option-count">${fmtNum(item.count)}</span>
              </label>`).join("") : '<div class="empty-state">No values match the current search.</div>'}
          </div>
        </div>
      </div>`;
  }).join("");

  els.filterGrid.querySelectorAll("[data-filter-search]").forEach((input) => {
    input.addEventListener("input", () => {
      state.searches[input.dataset.filterSearch] = input.value;
      renderFilterControls();
    });
  });
  els.filterGrid.querySelectorAll("[data-filter-clear]").forEach((button) => {
    button.addEventListener("click", () => {
      const key = button.dataset.filterClear;
      state.filters[key] = [];
      renderFilterControls();
      renderActiveChips();
      loadDashboard();
    });
  });
  els.filterGrid.querySelectorAll("[data-filter-option]").forEach((input) => {
    input.addEventListener("change", () => {
      toggleFilterValue(input.dataset.filterOption, input.value, input.checked);
      renderFilterControls();
      renderActiveChips();
      loadDashboard();
    });
  });
}

function renderActiveChips() {
  const chips = [];
  if (state.filters.date_start || state.filters.date_end) {
    chips.push(`<span class="chip">Date: ${escHtml(shortDate(state.filters.date_start))} - ${escHtml(shortDate(state.filters.date_end))}</span>`);
  }
  CONTROLS.forEach((control) => {
    (state.filters[control.key] || []).forEach((value) => {
      chips.push(`
        <span class="chip ${control.key.startsWith("exclude_") ? "exclude" : ""}">
          ${escHtml(`${control.label}: ${value}`)}
          <button class="chip-remove" type="button" data-remove-chip="${escHtml(control.key)}" data-value="${escHtml(value)}">×</button>
        </span>`);
    });
  });
  els.activeChips.innerHTML = chips.join("");
}

function renderKpis(kpis) {
  els.kpiStrip.innerHTML = KPI_CONFIG.map((config) => {
    const metric = kpis[config.key] || { value: 0, change: 0 };
    const delta = Number(metric.change || 0);
    const positive = config.lowerIsBetter ? delta <= 0 : delta >= 0;
    const deltaLabel = delta === 0 ? "No change vs prior window" : `${delta > 0 ? "▲" : "▼"} ${fmtPct(Math.abs(delta))} vs prior window`;
    return `
      <div class="kpi-card">
        <div class="kpi-value">${config.format === "percent" ? fmtPct(metric.value) : fmtNum(metric.value)}</div>
        <div class="kpi-label">${escHtml(config.label)}</div>
        <div class="kpi-delta ${delta === 0 ? "" : positive ? "good" : "bad"}">${escHtml(deltaLabel)}</div>
      </div>`;
  }).join("");
}

function renderTimeline(points) {
  const bucketed = bucketTimeline(points, state.timelineBucket);
  const metricKey = state.timelineMetric;
  const color = metricKey === "installation_tickets" ? "#c97a18" : metricKey === "bot_resolved_tickets" ? "#17845f" : metricKey === "repeat_tickets" ? "#8b5cf6" : "#2563eb";
  renderBarChart(els.timelineChart, {
    points: bucketed.map((item) => ({ label: item.label, value: item[metricKey] || 0 })),
    color,
    yLabel: TIMELINE_METRICS.find((item) => item.key === metricKey)?.label || "Tickets",
  });
}

function renderSpotlight(items) {
  els.spotlightCards.innerHTML = items.length ? items.map((item) => `
    <div class="spotlight-card">
      <h3>${escHtml(item.title)}</h3>
      <p>${escHtml(item.detail)}</p>
    </div>`).join("") : '<div class="empty-state">No highlights in the selected range.</div>';
}

function renderProductHealth() {
  const payload = state.payload || {};
  const rows = state.productView === "category" ? [...(payload.category_health || [])] : [...(payload.product_health || [])];
  if (!rows.length) {
    els.productHealthTable.innerHTML = '<div class="empty-state">No product data for this view.</div>';
    return;
  }
  rows.sort((a, b) => Number(b[state.productSort] || 0) - Number(a[state.productSort] || 0));
  const body = rows.slice(0, 18).map((row, index) => `
    <tr class="click-row" data-product-row="${state.productView === "category" ? "" : escHtml(JSON.stringify({ category: row.product_category, product_name: row.product_name }))}">
      <td class="num">${index + 1}</td>
      <td>
        <div class="name-stack">
          <div class="name-main">${escHtml(state.productView === "category" ? row.product_category : row.product_name || "Other")}</div>
          <div class="name-sub">${escHtml(state.productView === "category" ? row.top_issue_detail || "No issue detail" : row.product_category || "Other")}</div>
        </div>
      </td>
      <td class="num">${fmtNum(row.tickets)}</td>
      <td class="num">${fmtPct(row.installation_rate)}</td>
      <td class="num">${fmtPct(row.repeat_rate)}</td>
      <td class="num">${fmtPct(row.bot_resolved_rate)}</td>
      <td class="num">${fmtPct(row.open_rate)}</td>
      <td><span class="metric-pill">${escHtml(row.top_efc || "Blank")}</span></td>
    </tr>`).join("");

  els.productHealthTable.innerHTML = `
    <table class="data-table">
      <thead>
        <tr>
          <th class="num">#</th>
          <th>${state.productView === "category" ? "Category" : "Product"}</th>
          <th class="num">Tickets</th>
          <th class="num">Installation %</th>
          <th class="num">Repeat %</th>
          <th class="num">Bot resolved %</th>
          <th class="num">Open %</th>
          <th>Top EFC</th>
        </tr>
      </thead>
      <tbody>${body}</tbody>
    </table>`;

  els.productHealthTable.querySelectorAll("[data-product-row]").forEach((row) => {
    const meta = row.dataset.productRow;
    if (!meta) return;
    row.addEventListener("click", () => {
      const parsed = JSON.parse(meta);
      openProductDrilldown(parsed.category, parsed.product_name);
    });
  });
}

function renderIssueBoard(issueViews) {
  const items = issueViews[state.issueView] || [];
  if (!items.length) {
    els.issueBoard.innerHTML = '<div class="empty-state">No issues match the current filters.</div>';
    return;
  }
  els.issueBoard.innerHTML = items.map((issue) => `
    <button class="issue-card" type="button" data-issue-id="${escHtml(issue.issue_id)}">
      <div class="issue-head">
        <div class="issue-title">${escHtml(issue.fault_code_level_2 || "Unclassified")}</div>
        <div class="issue-subtitle">${escHtml(issue.product_name || "Other")} · ${escHtml(issue.executive_fault_code || "Blank")} · ${escHtml(issue.fault_code_level_1 || "Unclassified")}</div>
      </div>
      <div class="issue-metrics">
        <div class="issue-metric"><div class="issue-metric-label">Tickets</div><div class="issue-metric-value">${fmtNum(issue.volume)}</div></div>
        <div class="issue-metric"><div class="issue-metric-label">Installation %</div><div class="issue-metric-value">${fmtPct(issue.installation_rate)}</div></div>
        <div class="issue-metric"><div class="issue-metric-label">Bot resolved %</div><div class="issue-metric-value">${fmtPct(issue.bot_resolved_rate)}</div></div>
        <div class="issue-metric"><div class="issue-metric-label">Transfer %</div><div class="issue-metric-value">${fmtPct(issue.bot_transfer_rate)}</div></div>
      </div>
    </button>`).join("");
  els.issueBoard.querySelectorAll("[data-issue-id]").forEach((button) => {
    button.addEventListener("click", () => openIssueDrilldown(button.dataset.issueId));
  });
}

function renderBotSummary(botSummary) {
  const overview = botSummary.overview || {};
  els.botOverview.innerHTML = [
    ["Chat volume", overview.chat_tickets],
    ["Bot resolved", overview.bot_resolved_tickets],
    ["Transferred", overview.bot_transferred_tickets],
    ["Blank chat", overview.blank_chat_tickets],
  ].map(([label, value]) => `
    <div class="mini-stat">
      <div class="mini-stat-key">${escHtml(label)}</div>
      <div class="mini-stat-value">${fmtNum(value || 0)}</div>
    </div>`).join("");
  renderBotTrend(state.payload?.timeline || []);
  renderIssueList(els.botLeakyIssues, botSummary.leaky_issues || [], "No high-transfer issues found.");
  renderIssueList(els.botBestIssues, botSummary.best_issues || [], "No high bot-resolved issues found.");
}

function renderBotTrend(points) {
  const bucketed = bucketTimeline(points, state.botBucket);
  if (!bucketed.length) {
    els.botTrendChart.innerHTML = '<div class="empty-state">No bot trend data in the selected range.</div>';
    return;
  }
  const width = 900;
  const height = 250;
  const pad = { top: 18, right: 50, bottom: 34, left: 48 };
  const innerW = width - pad.left - pad.right;
  const innerH = height - pad.top - pad.bottom;
  const maxTickets = Math.max(...bucketed.map((item) => item.tickets || 0), 1);
  const step = innerW / Math.max(bucketed.length, 1);
  const barW = Math.min(44, step * 0.62);
  const linePoints = bucketed.map((item, index) => {
    const pct = item.tickets ? (item.bot_resolved_tickets || 0) / item.tickets : 0;
    return {
      x: pad.left + step * index + step / 2,
      y: pad.top + innerH - pct * innerH,
      pct,
      label: item.label,
    };
  });
  const linePath = linePoints.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");
  els.botTrendChart.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" style="width:100%;height:250px">
      ${[0, 0.5, 1].map((ratio) => {
        const y = pad.top + innerH - ratio * innerH;
        return `<line x1="${pad.left}" x2="${width - pad.right}" y1="${y}" y2="${y}" stroke="#e2e8f0"></line><text x="${width - pad.right + 8}" y="${y + 4}" font-size="10" fill="#64748b">${Math.round(ratio * 100)}%</text>`;
      }).join("")}
      ${bucketed.map((item, index) => {
        const barH = ((item.tickets || 0) / maxTickets) * innerH;
        const x = pad.left + step * index + (step - barW) / 2;
        const y = pad.top + innerH - barH;
        return `<rect x="${x}" y="${y}" width="${barW}" height="${barH}" rx="8" fill="rgba(37,99,235,0.18)" stroke="rgba(37,99,235,0.45)"></rect><text x="${x + barW / 2}" y="${height - 10}" text-anchor="middle" font-size="10" fill="#64748b">${escHtml(item.label)}</text>`;
      }).join("")}
      <path d="${linePath}" fill="none" stroke="#17845f" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></path>
      ${linePoints.map((point) => `<circle cx="${point.x}" cy="${point.y}" r="4.5" fill="#17845f"></circle><text x="${point.x}" y="${Math.max(12, point.y - 10)}" text-anchor="middle" font-size="10" fill="#17845f">${(point.pct * 100).toFixed(0)}%</text>`).join("")}
    </svg>`;
}

function renderMixList(container, rows) {
  if (!rows.length) {
    container.innerHTML = '<div class="empty-state">No data for this view.</div>';
    return;
  }
  const max = Math.max(...rows.map((row) => Number(row.count || 0)), 1);
  container.innerHTML = `<div class="mix-list">${rows.slice(0, 8).map((row) => `
    <div class="mix-row">
      <div>
        <div class="mix-row-head">
          <span class="mix-label">${escHtml(row.label || "Unknown")}</span>
          <span class="mix-value">${fmtPct(row.share)}</span>
        </div>
        <div class="mix-bar"><div class="mix-fill" style="width:${(Number(row.count || 0) / max) * 100}%"></div></div>
      </div>
      <div class="mix-value">${fmtNum(row.count)}</div>
    </div>`).join("")}</div>`;
}

function renderPipeline(pipeline) {
  const statusClass = pipeline.status === "ok" ? "" : pipeline.status === "running" ? "warn" : "error";
  const recent = pipeline.recent_runs || [];
  els.pipelineHealth.innerHTML = `
    <div class="pipeline-summary">
      <span class="pipeline-dot ${statusClass}"></span>
      <strong>${escHtml(pipeline.status || "Unknown")}</strong>
      <span class="mix-value">Last run ${escHtml(fmtDateTime(pipeline.last_run_at || ""))}</span>
      <span class="mix-value">${fmtNum(pipeline.rows_inserted || 0)} rows inserted</span>
    </div>
    <div class="pipeline-runs">
      ${recent.map((run) => `
        <div class="pipeline-run">
          <strong>${escHtml(run.status || "Unknown")}</strong>
          <span>${escHtml(fmtDateTime(run.finished_at || run.started_at || ""))}</span>
          <span>${fmtNum(run.rows_inserted || 0)} rows</span>
        </div>`).join("") || '<div class="empty-state">No recent pipeline runs available.</div>'}
    </div>`;
}

async function runPipeline() {
  els.runPipelineBtn.disabled = true;
  els.runPipelineBtn.textContent = "Starting...";
  try {
    await fetch(apiUrl("/api/pipeline/run"), { method: "POST" });
    els.runPipelineBtn.textContent = "Running...";
    setTimeout(() => {
      els.runPipelineBtn.disabled = false;
      els.runPipelineBtn.textContent = "Run pipeline";
      loadDashboard();
    }, 4000);
  } catch {
    els.runPipelineBtn.disabled = false;
    els.runPipelineBtn.textContent = "Run pipeline";
  }
}

async function openProductDrilldown(category, productName) {
  els.drilldownModal.classList.remove("hidden");
  els.drilldownEyebrow.textContent = "Product drilldown";
  els.drilldownTitle.textContent = productName;
  els.drilldownSubtitle.textContent = category;
  els.drilldownBody.innerHTML = '<div class="empty-state">Loading details...</div>';
  try {
    const params = buildQueryParams(state.filters);
    params.set("category", category);
    params.set("product_name", productName);
    const response = await fetch(`${apiUrl("/api/drilldown/product")}?${params.toString()}`);
    const payload = await response.json();
    renderDrilldownPanels(payload.drilldown || {});
  } catch (error) {
    els.drilldownBody.innerHTML = `<div class="error-state">${escHtml(error.message || "Failed to load details.")}</div>`;
  }
}

async function openIssueDrilldown(issueId) {
  els.drilldownModal.classList.remove("hidden");
  els.drilldownEyebrow.textContent = "Issue drilldown";
  els.drilldownTitle.textContent = "Loading issue";
  els.drilldownSubtitle.textContent = "";
  els.drilldownBody.innerHTML = '<div class="empty-state">Loading details...</div>';
  try {
    const params = buildQueryParams(state.filters);
    const response = await fetch(`${apiUrl(`/api/drilldown/issue/${encodeURIComponent(issueId)}`)}?${params.toString()}`);
    const payload = await response.json();
    const issue = payload.issue || {};
    els.drilldownTitle.textContent = issue.fault_code_level_2 || "Issue detail";
    els.drilldownSubtitle.textContent = `${issue.product_name || "Other"} · ${issue.executive_fault_code || "Blank"} · ${issue.fault_code_level_1 || "Unclassified"}`;
    renderDrilldownPanels(payload.drilldown || {});
  } catch (error) {
    els.drilldownBody.innerHTML = `<div class="error-state">${escHtml(error.message || "Failed to load details.")}</div>`;
  }
}

function renderDrilldownPanels(drilldown) {
  const timeline = bucketTimeline((drilldown.timeline || []).map((row) => ({
    date: row.metric_date,
    tickets: row.tickets,
    installation_tickets: row.installation_tickets,
    bot_resolved_tickets: row.bot_resolved_tickets,
    repeat_tickets: 0,
  })), "weekly");
  els.drilldownBody.innerHTML = `
    <div class="drilldown-stack">
      <div class="mini-panel"><h3>Trend</h3>${renderMiniChartSvg(timeline)}</div>
      <div class="mini-panel"><h3>Resolution summary</h3>${renderMiniBars(drilldown.resolutions || [])}</div>
    </div>
    <div class="drilldown-stack">
      <div class="mini-panel"><h3>Bot actions</h3>${renderMiniBars(drilldown.bot_actions || [])}</div>
      <div class="mini-panel"><h3>Status summary</h3>${renderMiniBars(drilldown.statuses || [])}</div>
      <div class="mini-panel"><h3>${drilldown.departments ? "Support team split" : "Issue buckets"}</h3>${renderMiniBars(drilldown.departments || drilldown.efcs || drilldown.fc2 || [])}</div>
    </div>`;
}

function closeDrilldown() {
  els.drilldownModal.classList.add("hidden");
}

function renderMiniBars(rows) {
  if (!rows.length) return '<div class="empty-state">No summary available.</div>';
  const max = Math.max(...rows.map((row) => Number(row.tickets || row.count || 0)), 1);
  return `<div class="mini-bars">${rows.slice(0, 8).map((row) => {
    const value = Number(row.tickets || row.count || 0);
    return `<div class="mini-bar-row"><div class="mix-row-head"><span class="mix-label">${escHtml(row.label || "Unknown")}</span><span class="mix-value">${fmtNum(value)}</span></div><div class="mini-bar-track"><div class="mini-bar-fill" style="width:${(value / max) * 100}%"></div></div></div>`;
  }).join("")}</div>`;
}

function renderMiniChartSvg(points) {
  if (!points.length) return '<div class="empty-state">No trend available.</div>';
  const width = 520;
  const height = 220;
  const pad = { top: 16, right: 10, bottom: 28, left: 40 };
  const innerW = width - pad.left - pad.right;
  const innerH = height - pad.top - pad.bottom;
  const max = Math.max(...points.map((point) => Number(point.tickets || 0)), 1);
  const barW = innerW / Math.max(points.length, 1) * 0.65;
  return `<svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" style="width:100%;height:220px">${points.map((point, index) => {
    const step = innerW / Math.max(points.length, 1);
    const value = Number(point.tickets || 0);
    const barH = (value / max) * innerH;
    const x = pad.left + step * index + (step - barW) / 2;
    const y = pad.top + innerH - barH;
    return `<rect x="${x}" y="${y}" width="${barW}" height="${barH}" rx="6" fill="rgba(37,99,235,0.2)" stroke="rgba(37,99,235,0.45)"></rect><text x="${x + barW / 2}" y="${height - 10}" text-anchor="middle" font-size="10" fill="#64748b">${escHtml(point.label)}</text>`;
  }).join("")}</svg>`;
}

function renderIssueList(container, issues, emptyMessage) {
  container.innerHTML = issues.length ? issues.slice(0, 6).map((issue) => `
    <div class="issue-list-item">
      <div class="issue-list-item-title">${escHtml(issue.fault_code_level_2 || "Unclassified")}</div>
      <div class="issue-list-item-meta">${escHtml(issue.product_name || "Other")} · Bot resolved ${fmtPct(issue.bot_resolved_rate)} · Transfer ${fmtPct(issue.bot_transfer_rate)}</div>
    </div>`).join("") : `<div class="empty-state">${escHtml(emptyMessage)}</div>`;
}

function renderSegmented(container, options, active, onSelect) {
  container.innerHTML = options.map((option) => `
    <button class="segment-btn ${option.key === active ? "active" : ""}" type="button" data-segment="${escHtml(option.key)}">${escHtml(option.label)}</button>
  `).join("");
  container.querySelectorAll("[data-segment]").forEach((button) => {
    button.addEventListener("click", () => {
      onSelect(button.dataset.segment);
      renderSegmented(container, options, button.dataset.segment, onSelect);
    });
  });
}

function renderBarChart(container, { points, color, yLabel }) {
  if (!points.length) {
    container.innerHTML = '<div class="empty-state">No trend data in the selected range.</div>';
    return;
  }
  const width = 900;
  const height = 290;
  const pad = { top: 18, right: 16, bottom: 34, left: 46 };
  const innerW = width - pad.left - pad.right;
  const innerH = height - pad.top - pad.bottom;
  const max = Math.max(...points.map((point) => Number(point.value || 0)), 1);
  const step = innerW / Math.max(points.length, 1);
  const barW = Math.min(42, step * 0.65);
  container.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" style="width:100%;height:290px">
      ${[0.25, 0.5, 0.75].map((ratio) => {
        const y = pad.top + innerH - innerH * ratio;
        return `<line x1="${pad.left}" x2="${width - pad.right}" y1="${y}" y2="${y}" stroke="#e2e8f0"></line>`;
      }).join("")}
      ${points.map((point, index) => {
        const value = Number(point.value || 0);
        const barH = (value / max) * innerH;
        const x = pad.left + step * index + (step - barW) / 2;
        const y = pad.top + innerH - barH;
        return `<rect x="${x}" y="${y}" width="${barW}" height="${barH}" rx="8" fill="${color}" opacity="0.78"></rect><text x="${x + barW / 2}" y="${height - 10}" text-anchor="middle" font-size="10" fill="#64748b">${escHtml(point.label)}</text>`;
      }).join("")}
      <text x="12" y="${pad.top + 12}" font-size="11" fill="#64748b">${escHtml(yLabel)}</text>
    </svg>`;
}

function buildQueryParams(filters) {
  const params = new URLSearchParams();
  Object.entries(filters).forEach(([key, value]) => {
    if (Array.isArray(value)) value.forEach((item) => params.append(key, item));
    else if (value) params.set(key, value);
  });
  return params;
}

function reconcileFilterState() {
  const bounds = state.options.date_bounds || {};
  if (bounds.max && (!state.filters.date_end || state.filters.date_end > bounds.max)) state.filters.date_end = bounds.max;
  if (bounds.min && (!state.filters.date_start || state.filters.date_start < bounds.min)) {
    state.filters.date_start = clampIsoDate(state.filters.date_start || bounds.min, bounds.min, bounds.max || bounds.min);
  }
  const validProducts = new Set(getControlOptions({ key: "products", optionsKey: "products" }).map((item) => item.label));
  state.filters.products = state.filters.products.filter((value) => validProducts.has(value));
  CONTROLS.forEach((control) => {
    const valid = new Set(getControlOptions(control).map((item) => item.label));
    state.filters[control.key] = (state.filters[control.key] || []).filter((value) => valid.has(value));
  });
}

function getControlOptions(control) {
  if (control.key === "products") {
    const mapping = state.options.products_by_category || {};
    const selectedCategories = state.filters.categories || [];
    const categories = selectedCategories.length ? selectedCategories : Object.keys(mapping);
    const counts = new Map();
    categories.forEach((category) => {
      (mapping[category] || []).forEach((product) => counts.set(product, (counts.get(product) || 0) + 1));
    });
    return [...counts.entries()].map(([label, count]) => ({ label, count })).sort((a, b) => b.count - a.count || a.label.localeCompare(b.label));
  }
  const raw = state.options[control.optionsKey] || [];
  return raw.map((label, index) => ({ label, count: raw.length - index }));
}

function summarizeSelection(options, selected) {
  if (!selected.length) return { main: "All selected", count: options.length ? `${options.length} available` : "" };
  if (selected.length === 1) return { main: selected[0], count: "1 selected" };
  return { main: selected[0], count: `+${selected.length - 1} more` };
}

function toggleFilterValue(key, value, checked) {
  const next = new Set(state.filters[key] || []);
  if (checked) next.add(value);
  else next.delete(value);
  state.filters[key] = [...next];
  const control = CONTROLS.find((item) => item.key === key);
  if (control?.oppositeKey && checked) {
    state.filters[control.oppositeKey] = (state.filters[control.oppositeKey] || []).filter((item) => item !== value);
  }
  if (key === "categories") {
    const allowed = new Set(getControlOptions({ key: "products", optionsKey: "products" }).map((item) => item.label));
    state.filters.products = state.filters.products.filter((item) => allowed.has(item));
  }
}

function bucketTimeline(points, mode) {
  if (!points.length) return [];
  const bucketMode = resolveBucketMode(points, mode);
  const grouped = new Map();
  points.forEach((point) => {
    const rawDate = toDate(point.date || point.metric_date);
    if (!rawDate) return;
    const key = bucketMode === "monthly" ? `${rawDate.getFullYear()}-${String(rawDate.getMonth() + 1).padStart(2, "0")}` : isoDate(startOfWeek(rawDate));
    const label = bucketMode === "monthly" ? rawDate.toLocaleDateString("en-IN", { month: "short", year: "2-digit" }) : rawDate.toLocaleDateString("en-IN", { day: "numeric", month: "short" });
    if (!grouped.has(key)) grouped.set(key, { label, tickets: 0, installation_tickets: 0, bot_resolved_tickets: 0, repeat_tickets: 0 });
    const current = grouped.get(key);
    current.tickets += Number(point.tickets || 0);
    current.installation_tickets += Number(point.installation_tickets || 0);
    current.bot_resolved_tickets += Number(point.bot_resolved_tickets || 0);
    current.repeat_tickets += Number(point.repeat_tickets || 0);
  });
  return [...grouped.entries()].sort((a, b) => a[0].localeCompare(b[0])).map(([, value]) => value);
}

function resolveBucketMode(points, mode) {
  if (mode !== "auto") return mode;
  const dates = points.map((point) => toDate(point.date || point.metric_date)).filter(Boolean);
  if (!dates.length) return "weekly";
  const min = dates.reduce((acc, value) => value < acc ? value : acc, dates[0]);
  const max = dates.reduce((acc, value) => value > acc ? value : acc, dates[0]);
  return Math.round((max - min) / 86400000) > 45 ? "monthly" : "weekly";
}

function renderLoading() {
  const loading = '<div class="empty-state">Loading board...</div>';
  [
    els.kpiStrip,
    els.timelineChart,
    els.spotlightCards,
    els.productHealthTable,
    els.issueBoard,
    els.botOverview,
    els.botTrendChart,
    els.botLeakyIssues,
    els.botBestIssues,
    els.departmentMix,
    els.channelMix,
    els.statusMix,
    els.installationMix,
    els.pipelineHealth,
  ].forEach((element) => { element.innerHTML = loading; });
}

function renderError(error) {
  const message = error?.message || "Failed to load board.";
  els.headline.textContent = "Board unavailable";
  els.summary.textContent = message;
  els.sourceBadge.textContent = "Error";
  els.sourceBadge.className = "status-badge warn";
  [
    els.kpiStrip,
    els.timelineChart,
    els.spotlightCards,
    els.productHealthTable,
    els.issueBoard,
    els.botOverview,
    els.botTrendChart,
    els.botLeakyIssues,
    els.botBestIssues,
    els.departmentMix,
    els.channelMix,
    els.statusMix,
    els.installationMix,
    els.pipelineHealth,
  ].forEach((element) => { element.innerHTML = `<div class="error-state">${escHtml(message)}</div>`; });
}

function apiUrl(path) {
  return `${state.apiBaseUrl}${path}`;
}

function fmtNum(value) {
  return new Intl.NumberFormat("en-IN").format(Number(value || 0));
}

function fmtPct(value) {
  return `${(Number(value || 0) * 100).toFixed(1)}%`;
}

function fmtDateTime(value) {
  const date = toDate(value);
  if (!date) return "—";
  return date.toLocaleString("en-IN", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function shortDate(value) {
  const date = toDate(value);
  if (!date) return "";
  return date.toLocaleDateString("en-IN", { day: "2-digit", month: "short", year: "numeric" });
}

function shiftIsoDate(value, days) {
  const date = toDate(value);
  if (!date) return "";
  date.setDate(date.getDate() + days);
  return isoDate(date);
}

function clampIsoDate(value, min, max) {
  const date = toDate(value);
  const minDate = toDate(min);
  const maxDate = toDate(max);
  if (!date) return min || max || "";
  if (minDate && date < minDate) return isoDate(minDate);
  if (maxDate && date > maxDate) return isoDate(maxDate);
  return isoDate(date);
}

function startOfWeek(value) {
  const date = new Date(value);
  const day = date.getDay();
  const diff = (day === 0 ? -6 : 1) - day;
  date.setDate(date.getDate() + diff);
  date.setHours(0, 0, 0, 0);
  return date;
}

function isoDate(value) {
  return value.toISOString().slice(0, 10);
}

function toDate(value) {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function escHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#x27;");
}
