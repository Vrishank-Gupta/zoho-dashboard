const DEFAULT_FILTERS = {
  date_start: "",
  date_end: "",
  exclude_installation: false,
  exclude_blank_chat: false,
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
  { key: "no_reopen_rate", label: "No-reopen rate", format: "percent", lowerIsBetter: false },
];

const CONTROLS = [
  { key: "categories", label: "Category", optionsKey: "categories" },
  { key: "products", label: "Product", optionsKey: "products" },
  { key: "efcs", label: "EFC", optionsKey: "efcs" },
  { key: "departments", label: "Support team", optionsKey: "departments" },
  { key: "channels", label: "Channel", optionsKey: "channels" },
  { key: "bot_actions", label: "Bot action", optionsKey: "bot_actions" },
  { key: "include_fc1", label: "FC1 include", optionsKey: "fc1", oppositeKey: "exclude_fc1" },
  { key: "exclude_fc1", label: "FC1 exclude", optionsKey: "fc1", oppositeKey: "include_fc1" },
  { key: "include_fc2", label: "FC2 include", optionsKey: "fc2", oppositeKey: "exclude_fc2" },
  { key: "exclude_fc2", label: "FC2 exclude", optionsKey: "fc2", oppositeKey: "include_fc2" },
  { key: "include_bot_action", label: "Bot action include", optionsKey: "bot_actions", oppositeKey: "exclude_bot_action" },
  { key: "exclude_bot_action", label: "Bot action exclude", optionsKey: "bot_actions", oppositeKey: "include_bot_action" },
];

const PRIMARY_CONTROL_KEYS = new Set(["categories", "products", "efcs", "departments", "channels", "bot_actions"]);

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

const VIEW_TABS = [
  { key: "dashboard", label: "Dashboard" },
  { key: "mapping", label: "Mapping Studio" },
];

const DEFAULT_EXCLUDED_SELECTIONS = {
  products: new Set(["blank product", "blankproduct"]),
  efcs: new Set(["blank", "unclassified"]),
};

const MAPPING_STUDIO_TABS = [
  { key: "products", label: "Product to Category" },
  { key: "fc2", label: "FC2 to EFC" },
];

const DRILLDOWN_TABS = {
  product: [
    { key: "overview", label: "Overview" },
    { key: "analysis", label: "Issue analysis" },
  ],
  category: [
    { key: "overview", label: "Overview" },
    { key: "products", label: "Product trend" },
    { key: "issues", label: "Issue movement" },
  ],
  issue: [
    { key: "overview", label: "Overview" },
    { key: "analysis", label: "Actions & outcomes" },
  ],
};

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

const VISUAL_THEME = {
  blue: "#4d8ef4",
  green: "#1ec55a",
  amber: "#f5a010",
  red: "#f03c3c",
  purple: "#9b6dff",
  teal: "#05b6d4",
  orange: "#f97316",
  slate: "#98a0bd",
  text: "#e4e7f0",
  muted: "#98a0bd",
  grid: "rgba(255,255,255,0.10)",
  donutTrack: "rgba(255,255,255,0.08)",
  barFill: "rgba(77, 142, 244, 0.42)",
  barStroke: "rgba(77, 142, 244, 0.9)",
};

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
  productSortDirection: "desc",
  timelineMetric: "tickets",
  timelineBucket: "auto",
  botBucket: "auto",
  activePreset: "60d",
  advancedFiltersOpen: false,
  defaultSelectionsApplied: false,
  activeView: loadSessionJson("quboActiveView", "dashboard"),
  mappingOverrides: loadSessionJson("quboMappingOverrides", { product_category_overrides: {}, efc_overrides: {} }),
  mappingDraft: { product_category_overrides: {}, efc_overrides: {} },
  mappingSearches: { global: "", products: "", fc2: "" },
  mappingShowOverriddenOnly: loadSessionJson("quboMappingShowOverriddenOnly", false),
  mappingStudioData: null,
  mappingStudioLoading: false,
  mappingStudioTab: loadSessionJson("quboMappingStudioTab", "products"),
  issueWidgetFilters: { categories: [], products: [] },
  issueWidgetOpenFilter: null,
  categoryDrilldownBucket: "auto",
  currentDrilldown: null,
  currentDrilldownKind: null,
  currentDrilldownTab: "overview",
  currentCategoryDrilldown: null,
};
state.mappingDraft = cloneMappingOverrides(state.mappingOverrides);

const els = {
  headline: document.getElementById("headline"),
  summary: document.getElementById("summary"),
  sourceBadge: document.getElementById("sourceBadge"),
  lastUpdated: document.getElementById("lastUpdated"),
  freshnessNote: document.getElementById("freshnessNote"),
  viewTabs: document.getElementById("viewTabs"),
  mappingStudioView: document.getElementById("mappingStudioView"),
  mappingStudioSummary: document.getElementById("mappingStudioSummary"),
  mappingStudioTabs: document.getElementById("mappingStudioTabs"),
  mappingProductPanel: document.getElementById("mappingProductPanel"),
  mappingFc2Panel: document.getElementById("mappingFc2Panel"),
  mappingGlobalSearch: document.getElementById("mappingGlobalSearch"),
  mappingProductSearch: document.getElementById("mappingProductSearch"),
  mappingFc2Search: document.getElementById("mappingFc2Search"),
  mappingShowOverriddenOnly: document.getElementById("mappingShowOverriddenOnly"),
  mappingProductTable: document.getElementById("mappingProductTable"),
  mappingFc2Table: document.getElementById("mappingFc2Table"),
  applyMappingOverrides: document.getElementById("applyMappingOverrides"),
  resetMappingOverrides: document.getElementById("resetMappingOverrides"),
  exportMappingOverrides: document.getElementById("exportMappingOverrides"),
  dateStart: document.getElementById("dateStart"),
  dateEnd: document.getElementById("dateEnd"),
  quickPresets: document.getElementById("quickPresets"),
  reportingShortcuts: document.getElementById("reportingShortcuts"),
  primaryFilterGrid: document.getElementById("primaryFilterGrid"),
  secondaryFilterGrid: document.getElementById("secondaryFilterGrid"),
  toggleAdvancedFilters: document.getElementById("toggleAdvancedFilters"),
  resetFilters: document.getElementById("resetFilters"),
  kpiStrip: document.getElementById("kpiStrip"),
  timelineChart: document.getElementById("timelineChart"),
  productHealthTable: document.getElementById("productHealthTable"),
  issueBoard: document.getElementById("issueBoard"),
  issueWidgetFilters: document.getElementById("issueWidgetFilters"),
  botOverview: document.getElementById("botOverview"),
  botTrendChart: document.getElementById("botTrendChart"),
  botLeakyIssues: document.getElementById("botLeakyIssues"),
  botBestIssues: document.getElementById("botBestIssues"),
  categoryDonut: document.getElementById("categoryDonut"),
  channelDonut: document.getElementById("channelDonut"),
  botActionDonut: document.getElementById("botActionDonut"),
  departmentMix: document.getElementById("departmentMix"),
  installationMix: document.getElementById("installationMix"),
  pipelineHealth: document.getElementById("pipelineHealth"),
  runPipelineBtn: document.getElementById("runPipelineBtn"),
  issueTabs: document.getElementById("issueTabs"),
  productViewTabs: document.getElementById("productViewTabs"),
  timelineMetricTabs: document.getElementById("timelineMetricTabs"),
  timelineBucketTabs: document.getElementById("timelineBucketTabs"),
  botBucketTabs: document.getElementById("botBucketTabs"),
  drilldownModal: document.getElementById("drilldownModal"),
  drilldownEyebrow: document.getElementById("drilldownEyebrow"),
  drilldownTitle: document.getElementById("drilldownTitle"),
  drilldownSubtitle: document.getElementById("drilldownSubtitle"),
  drilldownTabs: document.getElementById("drilldownTabs"),
  drilldownBody: document.getElementById("drilldownBody"),
};

boot();

function boot() {
  bindEvents();
  renderSegmented(els.viewTabs, VIEW_TABS, state.activeView, (value) => {
    state.activeView = value;
    saveSessionJson("quboActiveView", value);
    renderActiveView();
  });
  renderSegmented(els.mappingStudioTabs, MAPPING_STUDIO_TABS, state.mappingStudioTab, (value) => {
    state.mappingStudioTab = value;
    saveSessionJson("quboMappingStudioTab", value);
    renderMappingStudioLayout();
  });
  renderSegmented(els.issueTabs, ISSUE_VIEWS, state.issueView, (value) => {
    state.issueView = value;
    renderIssueBoard(state.payload?.issue_views || {});
  });
  renderSegmented(els.productViewTabs, PRODUCT_VIEWS, state.productView, (value) => {
    state.productView = value;
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
  els.toggleAdvancedFilters.addEventListener("click", () => {
    state.advancedFiltersOpen = !state.advancedFiltersOpen;
    renderFilterControls();
  });

  els.resetFilters.addEventListener("click", () => {
    state.filters = structuredClone(DEFAULT_FILTERS);
    state.searches = {};
    state.activePreset = "60d";
    state.advancedFiltersOpen = false;
    state.defaultSelectionsApplied = false;
    state.issueWidgetFilters = { categories: [], products: [] };
    state.issueWidgetOpenFilter = null;
    renderDateToolbar();
    renderFilterControls();
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

    if (!target.closest("[data-filter-panel]") && !target.closest("[data-widget-filter-panel]")) {
      state.openFilter = null;
      state.issueWidgetOpenFilter = null;
      renderFilterControls();
      renderIssueWidgetFilters();
    }
  });

  els.runPipelineBtn.addEventListener("click", runPipeline);
  document.getElementById("closeDrilldown").addEventListener("click", closeDrilldown);
  els.mappingProductSearch?.addEventListener("input", () => {
    state.mappingSearches.products = els.mappingProductSearch.value;
    renderMappingStudio(state.mappingStudioData || {});
  });
  els.mappingFc2Search?.addEventListener("input", () => {
    state.mappingSearches.fc2 = els.mappingFc2Search.value;
    renderMappingStudio(state.mappingStudioData || {});
  });
  els.mappingShowOverriddenOnly?.addEventListener("change", () => {
    state.mappingShowOverriddenOnly = Boolean(els.mappingShowOverriddenOnly.checked);
    saveSessionJson("quboMappingShowOverriddenOnly", state.mappingShowOverriddenOnly);
    renderMappingStudio(state.mappingStudioData || {});
  });
  els.applyMappingOverrides?.addEventListener("click", () => {
    state.mappingOverrides = normalizeMappingOverrides(state.mappingDraft);
    saveSessionJson("quboMappingOverrides", state.mappingOverrides);
    state.mappingStudioData = null;
    loadDashboard();
  });
  els.resetMappingOverrides?.addEventListener("click", () => {
    state.mappingOverrides = { product_category_overrides: {}, efc_overrides: {} };
    state.mappingDraft = cloneMappingOverrides(state.mappingOverrides);
    saveSessionJson("quboMappingOverrides", state.mappingOverrides);
    state.mappingStudioData = null;
    renderMappingStudio(state.mappingStudioData || {});
    loadDashboard();
  });
  els.exportMappingOverrides?.addEventListener("click", exportMappingOverrides);
}

async function loadDashboard() {
  renderLoading();
  const params = buildQueryParams(state.filters);
  try {
    const response = await fetch(`${apiUrl("/api/dashboard")}?${params.toString()}`);
    if (!response.ok) throw new Error(`API ${response.status}`);
    const payload = await response.json();
    state.payload = payload;
    state.mappingStudioData = null;
    state.options = payload.filter_options || {};
    const appliedDefaults = applyDefaultSelections();
    if (appliedDefaults) {
      renderDateToolbar();
      renderFilterControls();
      loadDashboard();
      return;
    }
    reconcileFilterState();
    reconcileIssueWidgetFilters();
    renderDateToolbar();
    renderFilterControls();
    renderDashboard(payload);
  } catch (error) {
    renderError(error);
  }
}

function applyDefaultSelections() {
  if (state.defaultSelectionsApplied) return false;
  let changed = false;
  Object.entries(DEFAULT_EXCLUDED_SELECTIONS).forEach(([key, excluded]) => {
    if ((state.filters[key] || []).length) return;
    const control = CONTROLS.find((item) => item.key === key);
    if (!control) return;
    const values = getControlOptions(control)
      .map((item) => item.label)
      .filter((label) => !excluded.has(String(label || "").trim().toLowerCase()));
    state.filters[key] = values;
    changed = true;
  });
  state.defaultSelectionsApplied = true;
  return changed;
}

function renderDashboard(payload) {
  const meta = payload.meta || {};
  const pipeline = payload.pipeline_health || {};
  els.headline.textContent = meta.title || "Qubo Support Executive Board";
  els.summary.textContent = meta.subtitle || "";
  els.sourceBadge.textContent = meta.source_mode === "clickhouse" ? "Live data" : "Sample data";
  els.sourceBadge.className = `status-badge ${meta.source_mode === "clickhouse" ? "live" : "warn"}`;
  els.lastUpdated.textContent = `Last refresh: ${fmtDateTime(pipeline.last_run_at || meta.window_end || "")}`;
  const freshness = meta.freshness || {};
  const freshnessText = freshness.source_max_date && freshness.clickhouse_max_date
    ? `Source max date: ${prettyIsoDate(freshness.source_max_date)} | ClickHouse max date: ${prettyIsoDate(freshness.clickhouse_max_date)} | Status: ${freshness.status || "Unavailable"}`
    : "";
  if (freshnessText) {
    els.freshnessNote.classList.remove("hidden");
    els.freshnessNote.textContent = `Sync: ${prettyIsoDate(freshness.clickhouse_max_date)} · ${freshness.status || "Unavailable"}`;
    els.freshnessNote.setAttribute("title", freshnessText);
  } else {
    els.freshnessNote.classList.add("hidden");
    els.freshnessNote.textContent = "";
    els.freshnessNote.removeAttribute("title");
  }
  renderKpis(payload.kpis || {});
  renderTimeline(payload.timeline || []);
  renderProductHealth();
  renderIssueWidgetFilters();
  renderIssueBoard(payload.issue_views || {});
  renderBotSummary(payload.bot_summary || {});
  renderDonut(els.categoryDonut, payload.service_ops?.category_mix || [], "Category");
  renderDonut(els.channelDonut, payload.service_ops?.channel_mix || [], "Channel");
  renderDonut(els.botActionDonut, payload.service_ops?.bot_action_mix || [], "Bot action");
  renderMixList(els.departmentMix, payload.service_ops?.department_mix || []);
  renderMixList(els.installationMix, payload.service_ops?.installation_mix || []);
  renderPipeline(pipeline);
  renderActiveView();
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
  renderReportingShortcuts();
}

function renderReportingShortcuts() {
  const shortcuts = [
    { key: "exclude_installation", label: "Exclude installation tickets" },
    { key: "exclude_blank_chat", label: "Exclude blank chats" },
  ];
  els.reportingShortcuts.innerHTML = shortcuts.map((item) => `
    <button class="shortcut-pill ${state.filters[item.key] ? "active" : ""}" type="button" data-shortcut="${escHtml(item.key)}">${escHtml(item.label)}</button>
  `).join("");
  els.reportingShortcuts.querySelectorAll("[data-shortcut]").forEach((button) => {
    button.addEventListener("click", () => {
      const key = button.dataset.shortcut;
      state.filters[key] = !state.filters[key];
      loadDashboard();
    });
  });
}

function renderFilterControls() {
  const activeElement = document.activeElement;
  const activeFilterKey = activeElement instanceof HTMLInputElement ? activeElement.dataset.filterSearch || "" : "";
  const activeSelectionStart = activeElement instanceof HTMLInputElement ? activeElement.selectionStart ?? null : null;
  els.toggleAdvancedFilters.textContent = state.advancedFiltersOpen ? "Hide advanced" : "Advanced filters";
  const renderControls = (controls) => controls.map((control) => {
    const options = getControlOptions(control);
    const selected = state.filters[control.key] || [];
    const summary = summarizeSelection(control, options, selected);
    const search = state.searches[control.key] || "";
    const blocked = new Set(control.oppositeKey ? (state.filters[control.oppositeKey] || []) : []);
    const visible = options.filter((item) => !blocked.has(item.label) && formatOptionLabel(control, item.label).toLowerCase().includes(search.toLowerCase()));
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
                <span>${escHtml(formatOptionLabel(control, item.label))}</span>
                <span class="option-count">${fmtNum(item.count)}</span>
              </label>`).join("") : '<div class="empty-state">No values match the current search.</div>'}
          </div>
        </div>
      </div>`;
  }).join("");

  els.primaryFilterGrid.innerHTML = renderControls(CONTROLS.filter((control) => PRIMARY_CONTROL_KEYS.has(control.key)));
  els.secondaryFilterGrid.innerHTML = renderControls(CONTROLS.filter((control) => !PRIMARY_CONTROL_KEYS.has(control.key)));
  els.secondaryFilterGrid.classList.toggle("hidden", !state.advancedFiltersOpen);

  [els.primaryFilterGrid, els.secondaryFilterGrid].forEach((grid) => {
    grid.querySelectorAll("[data-filter-search]").forEach((input) => {
      input.addEventListener("input", () => {
        state.searches[input.dataset.filterSearch] = input.value;
        renderFilterControls();
      });
    });
    grid.querySelectorAll("[data-filter-clear]").forEach((button) => {
      button.addEventListener("click", () => {
        const key = button.dataset.filterClear;
        state.filters[key] = [];
        renderFilterControls();
        loadDashboard();
      });
    });
    grid.querySelectorAll("[data-filter-option]").forEach((input) => {
      input.addEventListener("change", () => {
        toggleFilterValue(input.dataset.filterOption, input.value, input.checked);
        renderFilterControls();
        loadDashboard();
      });
    });
  });

  if (activeFilterKey) {
    const nextInput = document.querySelector(`[data-filter-search="${CSS.escape(activeFilterKey)}"]`);
    if (nextInput instanceof HTMLInputElement) {
      nextInput.focus();
      if (activeSelectionStart !== null) {
        nextInput.setSelectionRange(activeSelectionStart, activeSelectionStart);
      }
    }
  }
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
  const color = metricKey === "installation_tickets"
    ? VISUAL_THEME.amber
    : metricKey === "bot_resolved_tickets"
      ? VISUAL_THEME.green
      : metricKey === "repeat_tickets"
        ? VISUAL_THEME.purple
        : VISUAL_THEME.blue;
  renderBarChart(els.timelineChart, {
    points: bucketed.map((item) => ({ label: item.label, value: item[metricKey] || 0 })),
    color,
    yLabel: TIMELINE_METRICS.find((item) => item.key === metricKey)?.label || "Tickets",
  });
}

function renderProductHealth() {
  const payload = state.payload || {};
  const rows = state.productView === "category" ? [...(payload.category_health || [])] : [...(payload.product_health || [])];
  if (!rows.length) {
    els.productHealthTable.innerHTML = '<div class="empty-state">No product data for this view.</div>';
    return;
  }
  rows.sort((a, b) => {
    const delta = Number(b[state.productSort] || 0) - Number(a[state.productSort] || 0);
    return state.productSortDirection === "desc" ? delta : -delta;
  });
  const sortHeader = (key, label, numeric = false) => {
    const active = state.productSort === key;
    const arrow = !active ? "↕" : state.productSortDirection === "desc" ? "↓" : "↑";
    const classes = numeric ? "num" : "";
    return `<th class="${classes}"><button class="sort-header" type="button" data-product-sort="${escHtml(key)}">${escHtml(label)} <span class="sort-indicator">${arrow}</span></button></th>`;
  };
  const body = rows.slice(0, 18).map((row, index) => `
    <tr class="click-row"
      data-product-row="${state.productView === "category" ? "" : escHtml(JSON.stringify({ category: row.product_category, product_name: row.product_name }))}"
      data-category-row="${state.productView === "category" ? escHtml(row.product_category || "") : ""}">
      <td class="num">${index + 1}</td>
      <td>
        <div class="name-stack">
          <div class="name-main">${escHtml(state.productView === "category" ? row.product_category : row.product_name || "Other")}</div>
          <div class="name-sub">${escHtml(state.productView === "category" ? `Top issue: ${row.top_issue_detail || "No issue detail"}` : `Category: ${row.product_category || "Other"}`)}</div>
        </div>
      </td>
      <td class="num">${fmtNum(row.tickets)}</td>
      <td class="num">${fmtPct(row.installation_rate)}</td>
      <td class="num">${fmtPct(row.repeat_rate)}</td>
      <td class="num">${fmtPct(row.bot_resolved_rate)}</td>
      <td><span class="metric-pill">${escHtml(row.top_efc || "Blank")}</span></td>
    </tr>`).join("");

  els.productHealthTable.innerHTML = `
    <table class="data-table">
      <thead>
        <tr>
          <th class="num">#</th>
          ${sortHeader(state.productView === "category" ? "product_category" : "product_name", state.productView === "category" ? "Category" : "Product")}
          ${sortHeader("tickets", "Tickets", true)}
          ${sortHeader("installation_rate", "Installation %", true)}
          ${sortHeader("repeat_rate", "Repeat %", true)}
          ${sortHeader("bot_resolved_rate", "Bot resolved %", true)}
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
  els.productHealthTable.querySelectorAll("[data-category-row]").forEach((row) => {
    const category = row.dataset.categoryRow;
    if (!category) return;
    row.addEventListener("click", () => openCategoryDrilldown(category));
  });
  els.productHealthTable.querySelectorAll("[data-product-sort]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      const key = button.dataset.productSort;
      if (!key) return;
      if (state.productSort === key) {
        state.productSortDirection = state.productSortDirection === "desc" ? "asc" : "desc";
      } else {
        state.productSort = key;
        state.productSortDirection = key === "product_name" || key === "product_category" ? "asc" : "desc";
      }
      renderProductHealth();
    });
  });
}

function renderIssueBoard(issueViews) {
  const items = filterIssueWidgetItems(issueViews[state.issueView] || []);
  if (!items.length) {
    els.issueBoard.innerHTML = '<div class="empty-state">No issues match the current filters.</div>';
    return;
  }
  els.issueBoard.innerHTML = items.map((issue) => `
    <button class="issue-card" type="button" data-issue-id="${escHtml(issue.issue_id)}">
      <div class="issue-head">
        <div class="issue-title">${escHtml(issue.fault_code_level_2 || "Unclassified")}</div>
        <div class="issue-subtitle">${escHtml(issue.product_category || "Other")} · ${escHtml(issue.product_name || "Other")} · ${escHtml(issue.executive_fault_code || "Blank")} · ${escHtml(issue.fault_code_level_1 || "Unclassified")}</div>
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

function renderIssueWidgetFilters() {
  if (!els.issueWidgetFilters) return;
  const options = getIssueWidgetOptions();
  const definitions = [
    { key: "categories", label: "Category", options: options.categories },
    { key: "products", label: "Product", options: options.products },
  ];
  els.issueWidgetFilters.innerHTML = definitions.map((definition) => {
    const selected = state.issueWidgetFilters[definition.key] || [];
    const summary = summarizeSelection({ key: definition.key, optionsKey: definition.key }, definition.options, selected);
    return `
      <div class="filter-control widget-filter-control ${state.issueWidgetOpenFilter === definition.key ? "open" : ""}">
        <button class="control-trigger widget-control-trigger" type="button" data-widget-filter-trigger="${escHtml(definition.key)}">
          <span class="control-summary">
            <span class="control-main">${escHtml(`${definition.label}: ${summary.main}`)}</span>
            ${summary.count ? `<span class="control-count">${escHtml(summary.count)}</span>` : ""}
          </span>
          <span class="control-caret">${state.issueWidgetOpenFilter === definition.key ? "▲" : "▼"}</span>
        </button>
        <div class="control-panel widget-control-panel" data-widget-filter-panel="${escHtml(definition.key)}">
          <div class="option-list">
            ${definition.options.length ? definition.options.map((item) => `
              <label class="option-item">
                <input type="checkbox" data-widget-filter-option="${escHtml(definition.key)}" value="${escHtml(item.label)}" ${selected.includes(item.label) ? "checked" : ""}>
                <span>${escHtml(item.label)}</span>
                <span class="option-count">${fmtNum(item.count)}</span>
              </label>
            `).join("") : '<div class="empty-state">No values in the current view.</div>'}
          </div>
        </div>
      </div>`;
  }).join("");

  els.issueWidgetFilters.querySelectorAll("[data-widget-filter-trigger]").forEach((button) => {
    button.addEventListener("click", () => {
      const key = button.dataset.widgetFilterTrigger;
      state.issueWidgetOpenFilter = state.issueWidgetOpenFilter === key ? null : key;
      renderIssueWidgetFilters();
    });
  });
  els.issueWidgetFilters.querySelectorAll("[data-widget-filter-option]").forEach((input) => {
    input.addEventListener("change", () => {
      toggleIssueWidgetFilterValue(input.dataset.widgetFilterOption, input.value, input.checked);
      renderIssueWidgetFilters();
      renderIssueBoard(state.payload?.issue_views || {});
    });
  });
}

function getIssueWidgetOptions() {
  const issueViews = state.payload?.issue_views || {};
  const allItems = Object.values(issueViews).flat();
  const categoryCounts = new Map();
  const productCounts = new Map();
  allItems.forEach((item) => {
    const category = item.product_category || "Other";
    categoryCounts.set(category, (categoryCounts.get(category) || 0) + Number(item.volume || 0));
  });
  const selectedCategories = state.issueWidgetFilters.categories.length
    ? new Set(state.issueWidgetFilters.categories)
    : new Set(categoryCounts.keys());
  allItems.forEach((item) => {
    const category = item.product_category || "Other";
    if (!selectedCategories.has(category)) return;
    const product = item.product_name || "Other";
    productCounts.set(product, (productCounts.get(product) || 0) + Number(item.volume || 0));
  });
  return {
    categories: [...categoryCounts.entries()].map(([label, count]) => ({ label, count })).sort((a, b) => b.count - a.count || a.label.localeCompare(b.label)),
    products: [...productCounts.entries()].map(([label, count]) => ({ label, count })).sort((a, b) => b.count - a.count || a.label.localeCompare(b.label)),
  };
}

function filterIssueWidgetItems(items) {
  const selectedCategories = state.issueWidgetFilters.categories;
  const selectedProducts = state.issueWidgetFilters.products;
  return items.filter((item) => {
    const categoryOkay = !selectedCategories.length || selectedCategories.includes(item.product_category || "Other");
    const productOkay = !selectedProducts.length || selectedProducts.includes(item.product_name || "Other");
    return categoryOkay && productOkay;
  });
}

function toggleIssueWidgetFilterValue(key, value, checked) {
  const next = new Set(state.issueWidgetFilters[key] || []);
  if (checked) next.add(value);
  else next.delete(value);
  state.issueWidgetFilters[key] = [...next];
  if (key === "categories") {
    const validProducts = new Set(getIssueWidgetOptions().products.map((item) => item.label));
    state.issueWidgetFilters.products = state.issueWidgetFilters.products.filter((item) => validProducts.has(item));
  }
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
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="xMidYMid meet" style="width:100%;height:250px">
      ${[0, 0.5, 1].map((ratio) => {
        const y = pad.top + innerH - ratio * innerH;
        return `<line x1="${pad.left}" x2="${width - pad.right}" y1="${y}" y2="${y}" stroke="${VISUAL_THEME.grid}"></line><text x="${width - pad.right + 8}" y="${y + 4}" font-size="10" fill="${VISUAL_THEME.muted}">${Math.round(ratio * 100)}%</text>`;
      }).join("")}
      ${[0, 0.5, 1].map((ratio) => {
        const y = pad.top + innerH - ratio * innerH;
        return `<text x="${pad.left - 8}" y="${y + 4}" text-anchor="end" font-size="10" fill="${VISUAL_THEME.muted}">${fmtNum(Math.round(maxTickets * ratio))}</text>`;
      }).join("")}
      ${bucketed.map((item, index) => {
        const barH = ((item.tickets || 0) / maxTickets) * innerH;
        const x = pad.left + step * index + (step - barW) / 2;
        const y = pad.top + innerH - barH;
        return `<rect x="${x}" y="${y}" width="${barW}" height="${barH}" rx="8" fill="${VISUAL_THEME.barFill}" stroke="${VISUAL_THEME.barStroke}"></rect><text x="${x + barW / 2}" y="${Math.max(12, y - 6)}" text-anchor="middle" font-size="10" fill="${VISUAL_THEME.blue}">${fmtNum(item.tickets || 0)}</text><text x="${x + barW / 2}" y="${height - 10}" text-anchor="middle" font-size="10" fill="${VISUAL_THEME.muted}">${escHtml(item.label)}</text>`;
      }).join("")}
      <path d="${linePath}" fill="none" stroke="${VISUAL_THEME.green}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></path>
      ${linePoints.map((point) => `<circle cx="${point.x}" cy="${point.y}" r="4.5" fill="${VISUAL_THEME.green}"></circle><text x="${point.x}" y="${Math.max(12, point.y - 10)}" text-anchor="middle" font-size="10" fill="${VISUAL_THEME.green}">${(point.pct * 100).toFixed(0)}%</text>`).join("")}
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

function renderDonut(container, rows, label) {
  if (!rows.length) {
    container.innerHTML = `<div class="empty-state">No ${escHtml(label.toLowerCase())} data for this view.</div>`;
    return;
  }
  const palette = [VISUAL_THEME.blue, VISUAL_THEME.green, VISUAL_THEME.amber, VISUAL_THEME.purple, VISUAL_THEME.red, VISUAL_THEME.teal, VISUAL_THEME.slate];
  const items = rows.slice(0, 5);
  const total = items.reduce((sum, row) => sum + Number(row.count || 0), 0) || 1;
  let current = 0;
  const radius = 52;
  const circumference = 2 * Math.PI * radius;
  const arcs = items.map((row, index) => {
    const share = Number(row.count || 0) / total;
    const length = share * circumference;
    const arc = `<circle cx="70" cy="70" r="${radius}" fill="none" stroke="${palette[index % palette.length]}" stroke-width="18" stroke-dasharray="${length} ${circumference - length}" stroke-dashoffset="${-current}" transform="rotate(-90 70 70)"></circle>`;
    current += length;
    return arc;
  }).join("");
  container.innerHTML = `
    <div class="donut-card">
      <svg viewBox="0 0 140 140" width="180" height="180" aria-label="${escHtml(label)} breakdown">
        <circle cx="70" cy="70" r="${radius}" fill="none" stroke="${VISUAL_THEME.donutTrack}" stroke-width="18"></circle>
        ${arcs}
        <text x="70" y="64" text-anchor="middle" font-size="11" fill="${VISUAL_THEME.muted}">${escHtml(label)}</text>
        <text x="70" y="82" text-anchor="middle" font-size="18" font-weight="800" fill="${VISUAL_THEME.text}">${fmtNum(total)}</text>
      </svg>
      <div class="donut-legend">
        ${items.map((row, index) => `<div class="donut-legend-item"><span class="donut-swatch" style="background:${palette[index % palette.length]}"></span><span>${escHtml(formatBotActionLabel(row.label || "Unknown"))}</span><strong>${fmtPct(row.share)}</strong></div>`).join("")}
      </div>
    </div>`;
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

function renderActiveView() {
  const dashboardPanels = document.querySelectorAll("[data-dashboard-panel='true']");
  const mappingOpen = state.activeView === "mapping";
  dashboardPanels.forEach((panel) => panel.classList.toggle("hidden", mappingOpen));
  els.mappingStudioView?.classList.toggle("hidden", !mappingOpen);
  if (mappingOpen) {
    if (state.mappingStudioData) renderMappingStudio(state.mappingStudioData);
    else if (!state.mappingStudioLoading) loadMappingStudio();
  }
}

function renderMappingStudioLayout() {
  els.mappingProductPanel?.classList.toggle("hidden", state.mappingStudioTab !== "products");
  els.mappingFc2Panel?.classList.toggle("hidden", state.mappingStudioTab !== "fc2");
}

function renderMappingStudio(mappingStudio) {
  if (state.mappingStudioLoading && !mappingStudio?.product_rows && !mappingStudio?.fc2_rows) {
    els.mappingStudioSummary.innerHTML = '<div class="mapping-banner">Loading mapping studio…</div>';
    els.mappingProductTable.innerHTML = '<div class="empty-state">Loading product mapping…</div>';
    els.mappingFc2Table.innerHTML = '<div class="empty-state">Loading FC2 to EFC mapping…</div>';
    return;
  }
  state.mappingDraft = mergeDraftWithPayload(state.mappingDraft, mappingStudio);
  const active = mappingStudio.active_overrides || {};
  if (els.mappingGlobalSearch) els.mappingGlobalSearch.value = state.mappingSearches.global || "";
  if (els.mappingShowOverriddenOnly) els.mappingShowOverriddenOnly.checked = !!state.mappingShowOverriddenOnly;
  renderMappingStudioLayout();
  els.mappingStudioSummary.innerHTML = `
    <div class="mapping-banner">Workbook mapping stays as the base. Edits here are session-only and affect only your current analysis.</div>
    <div class="mapping-stat">
      <span class="mapping-stat-value">${fmtNum(active.products || 0)}</span>
      <span class="mapping-stat-label">Product overrides</span>
    </div>
    <div class="mapping-stat">
      <span class="mapping-stat-value">${fmtNum(active.efcs || 0)}</span>
      <span class="mapping-stat-label">FC2 overrides</span>
    </div>
  `;

  const globalSearch = (state.mappingSearches.global || "").toLowerCase();
  const productSearch = (state.mappingSearches.products || "").toLowerCase();
  const productRows = (mappingStudio.product_rows || []).filter((row) => {
    const matchesGlobal = !globalSearch || [row.product_name, row.base_category, row.effective_category].some((value) => String(value || "").toLowerCase().includes(globalSearch));
    const matchesLocal = !productSearch || row.product_name.toLowerCase().includes(productSearch);
    const isOverridden = hasDraftOverride("product_category_overrides", row.product_name, row.base_category);
    const matchesOverrideOnly = !state.mappingShowOverriddenOnly || isOverridden;
    return matchesGlobal && matchesLocal && matchesOverrideOnly;
  });
  const categoryOptions = mappingStudio.category_options || [];
  els.mappingProductTable.innerHTML = renderMappingTable({
    rows: productRows,
    idKey: "product_name",
    valueKey: "effective_category",
    selectKey: "product_category_overrides",
    labelColumns: [
      { key: "product_name", label: "Product" },
      { key: "base_category", label: "Workbook category" },
    ],
    valueLabel: "Session category",
    options: categoryOptions,
    countKey: "tickets",
    countLabel: "Tickets",
  });

  const fc2Search = (state.mappingSearches.fc2 || "").toLowerCase();
  const fc2Rows = (mappingStudio.fc2_rows || []).filter((row) => {
    const matchesGlobal = !globalSearch || [row.fault_code_level_2, row.fault_code_level_1, row.base_efc, row.effective_efc].some((value) => String(value || "").toLowerCase().includes(globalSearch));
    const matchesLocal = !fc2Search || row.fault_code_level_2.toLowerCase().includes(fc2Search);
    const isOverridden = hasDraftOverride("efc_overrides", row.fault_code_level_2, row.base_efc);
    const matchesOverrideOnly = !state.mappingShowOverriddenOnly || isOverridden;
    return matchesGlobal && matchesLocal && matchesOverrideOnly;
  });
  const efcOptions = mappingStudio.efc_options || [];
  els.mappingFc2Table.innerHTML = renderMappingTable({
    rows: fc2Rows,
    idKey: "fault_code_level_2",
    valueKey: "effective_efc",
    selectKey: "efc_overrides",
    labelColumns: [
      { key: "fault_code_level_2", label: "FC2" },
      { key: "fault_code_level_1", label: "Fallback FC1" },
      { key: "base_efc", label: "Workbook/base EFC" },
    ],
    valueLabel: "Session EFC",
    options: efcOptions,
    countKey: "tickets",
    countLabel: "Tickets",
  });

  document.querySelectorAll("[data-mapping-select]").forEach((input) => {
    input.addEventListener("change", () => {
      const scope = input.dataset.mappingScope;
      const key = input.dataset.mappingKey;
      if (!scope || !key) return;
      const nextValue = input.value;
      if (!nextValue) delete state.mappingDraft[scope][key];
      else state.mappingDraft[scope][key] = nextValue;
      renderMappingStudio(mappingStudio);
    });
  });
  document.querySelectorAll("[data-mapping-revert]").forEach((button) => {
    button.addEventListener("click", () => {
      const scope = button.dataset.mappingRevert;
      const key = button.dataset.mappingKey;
      if (!scope || !key) return;
      delete state.mappingDraft[scope][key];
      renderMappingStudio(mappingStudio);
    });
  });
}

function renderMappingTable({ rows, idKey, valueKey, selectKey, labelColumns, valueLabel, options, countKey, countLabel }) {
  if (!rows.length) return '<div class="empty-state">No rows available for the current selection.</div>';
  return `
    <div class="mini-table-wrap mapping-table-wrap">
        <table class="mini-table mapping-table mapping-table-fixed">
        <thead>
          <tr>
            ${labelColumns.map((column) => `<th>${escHtml(column.label)}</th>`).join("")}
            <th>${escHtml(valueLabel)}</th>
            <th>${escHtml(countLabel)}</th>
          </tr>
        </thead>
        <tbody>
          ${rows.map((row) => {
            const overrideValue = state.mappingDraft[selectKey][String(row[idKey]).toLowerCase()] || "";
            const effectiveValue = overrideValue || row[valueKey] || "";
            const baseValue = selectKey === "product_category_overrides" ? row.base_category : row.base_efc;
            const isOverridden = hasDraftOverride(selectKey, row[idKey], baseValue);
            return `<tr class="${isOverridden ? "mapping-overridden" : ""}">
              ${labelColumns.map((column) => `<td>${escHtml(row[column.key] || "")}</td>`).join("")}
              <td>
                <div class="mapping-select-row">
                  <select class="mapping-select" data-mapping-select="true" data-mapping-scope="${escHtml(selectKey)}" data-mapping-key="${escHtml(String(row[idKey]).toLowerCase())}">
                    <option value="">Use workbook</option>
                    ${options.map((option) => `<option value="${escHtml(option)}" ${effectiveValue === option ? "selected" : ""}>${escHtml(option)}</option>`).join("")}
                  </select>
                  ${isOverridden ? `<button class="mapping-revert" type="button" data-mapping-revert="${escHtml(selectKey)}" data-mapping-key="${escHtml(String(row[idKey]).toLowerCase())}">Revert</button>` : ""}
                </div>
              </td>
              <td class="num">${fmtNum(row[countKey] || 0)}</td>
            </tr>`;
          }).join("")}
        </tbody>
      </table>
      </div>`;
}

async function loadMappingStudio() {
  state.mappingStudioLoading = true;
  renderMappingStudio(state.mappingStudioData || {});
  try {
    const params = buildQueryParams(state.filters);
    const response = await fetch(`${apiUrl("/api/mapping-studio")}?${params.toString()}`);
    if (!response.ok) throw new Error(`API ${response.status}`);
    const payload = await response.json();
    state.mappingStudioData = payload.mapping_studio || {};
    renderMappingStudio(state.mappingStudioData);
  } catch (error) {
    els.mappingStudioSummary.innerHTML = `<div class="error-state">${escHtml(error.message || "Failed to load mapping studio.")}</div>`;
    els.mappingProductTable.innerHTML = '<div class="error-state">Could not load product mapping.</div>';
    els.mappingFc2Table.innerHTML = '<div class="error-state">Could not load FC2 to EFC mapping.</div>';
  } finally {
    state.mappingStudioLoading = false;
  }
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
  state.currentDrilldownKind = "product";
  state.currentDrilldownTab = "overview";
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
    state.currentDrilldown = payload.drilldown || {};
    renderDrilldownPanels(state.currentDrilldown);
  } catch (error) {
    els.drilldownBody.innerHTML = `<div class="error-state">${escHtml(error.message || "Failed to load details.")}</div>`;
  }
}

async function openCategoryDrilldown(category) {
  state.categoryDrilldownBucket = "auto";
  state.currentDrilldownKind = "category";
  state.currentDrilldownTab = "overview";
  els.drilldownModal.classList.remove("hidden");
  els.drilldownEyebrow.textContent = "Category drilldown";
  els.drilldownTitle.textContent = category;
  els.drilldownSubtitle.textContent = "Product mix, issues, resolutions, and bot actions for this category";
  els.drilldownBody.innerHTML = '<div class="empty-state">Loading details...</div>';
  try {
    const params = buildQueryParams(state.filters);
    params.set("category", category);
    const response = await fetch(`${apiUrl("/api/drilldown/category")}?${params.toString()}`);
    const payload = await response.json();
    state.currentCategoryDrilldown = payload.drilldown || {};
    state.currentDrilldown = state.currentCategoryDrilldown;
    renderCategoryDrilldownPanels(state.currentCategoryDrilldown);
  } catch (error) {
    els.drilldownBody.innerHTML = `<div class="error-state">${escHtml(error.message || "Failed to load details.")}</div>`;
  }
}

async function openIssueDrilldown(issueId) {
  state.currentDrilldownKind = "issue";
  state.currentDrilldownTab = "overview";
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
    els.drilldownSubtitle.textContent = `${issue.product_name || "Other"} · ${issue.executive_fault_code || "Others"} · ${issue.fault_code_level_1 || "Unclassified"}`;
    state.currentDrilldown = payload.drilldown || {};
    renderDrilldownPanels(state.currentDrilldown);
  } catch (error) {
    els.drilldownBody.innerHTML = `<div class="error-state">${escHtml(error.message || "Failed to load details.")}</div>`;
  }
}

function renderDrilldownPanels(drilldown) {
  const summary = drilldown.summary?.[0] || {};
  const timeline = bucketTimeline((drilldown.timeline || []).map((row) => ({
    date: row.metric_date,
    tickets: row.tickets,
    installation_tickets: row.installation_tickets,
    bot_resolved_tickets: row.bot_resolved_tickets,
    repeat_tickets: 0,
  })), "weekly");
  renderDrilldownTabs();
  const overview = `
    <section class="drilldown-section">
      <div class="mini-summary-grid">
        ${renderMiniStat("Tickets", summary.tickets || 0)}
        ${renderMiniStat("Installation", ratio(summary.installation_tickets, summary.tickets), true)}
        ${renderMiniStat("Bot resolved", ratio(summary.bot_resolved_tickets, summary.tickets), true)}
        ${renderMiniStat("Blank chat", ratio(summary.blank_chat_tickets, summary.tickets), true)}
      </div>
    </section>
    <section class="drilldown-section">
      <div class="mini-panel feature-panel">${renderMiniChartSvg(timeline)}</div>
    </section>
    <section class="drilldown-section">
      <div class="drilldown-kpi-rail">
        ${renderInsightCard("Top EFC", ((drilldown.efcs || [])[0]?.label || "Others"), ((drilldown.efcs || [])[0]?.tickets || 0))}
        ${renderInsightCard("Top resolution", ((drilldown.resolutions || [])[0]?.label || "Unknown"), ((drilldown.resolutions || [])[0]?.tickets || 0))}
        ${renderInsightCard("Top bot action", formatBotActionLabel((drilldown.bot_actions || [])[0]?.label || "No bot action"), ((drilldown.bot_actions || [])[0]?.tickets || 0))}
      </div>
    </section>`;
  const analysis = `
    <section class="drilldown-section">
      <div class="drilldown-two-col">
        <div class="mini-panel"><h3>Issue distribution</h3>${renderMiniTable(drilldown.issue_matrix || [], [
          { key: "executive_fault_code", label: "EFC" },
          { key: "issue_detail", label: "FC2" },
          { key: "tickets", label: "Tickets", format: "number" },
          { key: "bot_resolved_tickets", label: "Bot resolved", format: "number" },
        ])}</div>
        <div class="drilldown-stack">
          <div class="mini-panel"><h3>Resolution summary</h3>${renderMiniBars(drilldown.resolutions || [])}</div>
          <div class="mini-panel"><h3>Bot actions</h3>${renderMiniBars(drilldown.bot_actions || [], formatBotActionLabel)}</div>
          <div class="mini-panel"><h3>Issue buckets</h3>${renderMiniBars(drilldown.efcs || drilldown.fc1 || drilldown.fc2 || [])}</div>
        </div>
      </div>
    </section>`;
  els.drilldownBody.innerHTML = state.currentDrilldownTab === "analysis" ? analysis : overview;
}

function renderCategoryDrilldownPanels(drilldown) {
  const summary = drilldown.summary?.[0] || {};
  const timeline = bucketTimeline((drilldown.timeline || []).map((row) => ({
    date: row.metric_date,
    tickets: row.tickets,
    installation_tickets: row.installation_tickets,
    bot_resolved_tickets: row.bot_resolved_tickets,
    repeat_tickets: 0,
  })), state.categoryDrilldownBucket);
  const productTrend = buildCategoryProductTrendModel(drilldown.product_daily || [], state.categoryDrilldownBucket);
  const faultMatrices = buildCategoryFaultMatrixModel(drilldown.product_fault_daily || [], state.categoryDrilldownBucket);
  renderDrilldownTabs();
  const overview = `
    <section class="drilldown-section">
      <div class="mini-summary-grid">
        ${renderMiniStat("Tickets", summary.tickets || 0)}
        ${renderMiniStat("Installation", ratio(summary.installation_tickets, summary.tickets), true)}
        ${renderMiniStat("Bot resolved", ratio(summary.bot_resolved_tickets, summary.tickets), true)}
        ${renderMiniStat("Blank chat", ratio(summary.blank_chat_tickets, summary.tickets), true)}
      </div>
    </section>
    <section class="drilldown-section">
      <div class="mini-panel feature-panel">${renderMiniChartSvg(timeline)}</div>
    </section>
    <section class="drilldown-section">
      <div class="drilldown-two-col">
        <div class="mini-panel"><h3>Products in category</h3>${renderMiniTable(drilldown.products || [], [
          { key: "label", label: "Product" },
          { key: "tickets", label: "Tickets", format: "number" },
          { key: "installation_tickets", label: "Installation", format: "percentOfTickets" },
          { key: "bot_resolved_tickets", label: "Bot resolved", format: "percentOfTickets" },
        ])}</div>
        <div class="mini-panel"><h3>Issue hotspots</h3>${renderMiniTable(drilldown.issues || [], [
          { key: "executive_fault_code", label: "EFC" },
          { key: "label", label: "FC2" },
          { key: "tickets", label: "Tickets", format: "number" },
          { key: "installation_tickets", label: "Installation", format: "percentOfTickets" },
        ])}</div>
      </div>
    </section>
    <section class="drilldown-section">
      <div class="drilldown-kpi-rail">
        ${renderInsightCard("Top EFC", ((drilldown.efcs || [])[0]?.label || "Others"), ((drilldown.efcs || [])[0]?.tickets || 0))}
        ${renderInsightCard("Top resolution", ((drilldown.resolutions || [])[0]?.label || "Unknown"), ((drilldown.resolutions || [])[0]?.tickets || 0))}
        ${renderInsightCard("Top bot action", formatBotActionLabel((drilldown.bot_actions || [])[0]?.label || "No bot action"), ((drilldown.bot_actions || [])[0]?.tickets || 0))}
      </div>
    </section>`;
  const productsView = `
    <div class="mini-panel analysis-panel">
      <div class="analysis-panel-head">
        <div>
          <h3>Product trend by period</h3>
          <div class="analysis-caption">Compare week-on-week or month-on-month volume across every product in this category.</div>
        </div>
        <div class="panel-actions">${renderCategoryBucketTabs()}</div>
      </div>
      ${renderCategoryProductTrendTable(productTrend)}
    </div>`;
  const issuesView = `
    <div class="mini-panel analysis-panel">
      <div class="analysis-panel-head">
        <div>
          <h3>Fault-code movement by product</h3>
          <div class="analysis-caption">Top issue buckets for each product across the selected periods.</div>
        </div>
        <div class="analysis-chip">${escHtml(productTrend.bucketMode === "monthly" ? "Monthly view" : "Weekly view")}</div>
      </div>
      ${renderCategoryFaultMatrices(faultMatrices)}
    </div>
    <section class="drilldown-section">
      <div class="drilldown-two-col">
        <div class="mini-panel"><h3>Resolution summary</h3>${renderMiniBars(drilldown.resolutions || [])}</div>
        <div class="mini-panel"><h3>Bot actions</h3>${renderMiniBars(drilldown.bot_actions || [], formatBotActionLabel)}</div>
      </div>
    </section>`;
  els.drilldownBody.innerHTML = state.currentDrilldownTab === "products" ? productsView : state.currentDrilldownTab === "issues" ? issuesView : overview;
  els.drilldownBody.querySelectorAll("[data-category-bucket]").forEach((button) => {
    button.addEventListener("click", () => {
      state.categoryDrilldownBucket = button.dataset.categoryBucket || "auto";
      renderCategoryDrilldownPanels(state.currentCategoryDrilldown || drilldown);
    });
  });
}

function closeDrilldown() {
  els.drilldownModal.classList.add("hidden");
  state.currentCategoryDrilldown = null;
  state.currentDrilldown = null;
  state.currentDrilldownKind = null;
  state.currentDrilldownTab = "overview";
}

function renderDrilldownTabs() {
  const tabs = DRILLDOWN_TABS[state.currentDrilldownKind] || [];
  if (!tabs.length) {
    els.drilldownTabs.innerHTML = "";
    els.drilldownTabs.classList.add("hidden");
    return;
  }
  els.drilldownTabs.classList.remove("hidden");
  renderSegmented(els.drilldownTabs, tabs, state.currentDrilldownTab, (value) => {
    state.currentDrilldownTab = value;
    if (state.currentDrilldownKind === "category") renderCategoryDrilldownPanels(state.currentCategoryDrilldown || {});
    else if (state.currentDrilldownKind === "product" || state.currentDrilldownKind === "issue") renderDrilldownPanels(state.currentDrilldown || {});
  });
}

function renderInsightCard(label, title, value) {
  return `<div class="insight-card"><div class="insight-label">${escHtml(label)}</div><div class="insight-title">${escHtml(title || "Unknown")}</div><div class="insight-value">${escHtml(fmtNum(value || 0))}</div></div>`;
}

function renderCategoryBucketTabs() {
  return BUCKET_MODES.map((option) => `
    <button class="segment-btn ${state.categoryDrilldownBucket === option.key ? "active" : ""}" type="button" data-category-bucket="${escHtml(option.key)}">${escHtml(option.label)}</button>
  `).join("");
}

function buildCategoryProductTrendModel(rows, mode) {
  if (!rows.length) return { bucketMode: "weekly", periods: [], rows: [] };
  const bucketMode = resolveBucketMode(rows.map((row) => ({ metric_date: row.metric_date })), mode);
  const periodMap = new Map();
  const productMap = new Map();
  rows.forEach((row) => {
    const bucket = getBucketDescriptor(row.metric_date, bucketMode);
    const productName = row.product_name || "Unknown";
    const value = Number(row.tickets || 0);
    if (!periodMap.has(bucket.key)) periodMap.set(bucket.key, bucket.label);
    if (!productMap.has(productName)) productMap.set(productName, { total: 0, periods: new Map() });
    const product = productMap.get(productName);
    product.total += value;
    product.periods.set(bucket.key, (product.periods.get(bucket.key) || 0) + value);
  });
  const periods = [...periodMap.entries()].sort((a, b) => a[0].localeCompare(b[0])).map(([key, label]) => ({ key, label }));
  const max = Math.max(...[...productMap.values()].flatMap((product) => periods.map((period) => product.periods.get(period.key) || 0)), 1);
  const rowsOut = [...productMap.entries()]
    .map(([product_name, product]) => ({
      product_name,
      total: product.total,
      cells: periods.map((period) => ({
        key: period.key,
        label: period.label,
        tickets: product.periods.get(period.key) || 0,
      })),
    }))
    .sort((a, b) => b.total - a.total || a.product_name.localeCompare(b.product_name))
    .map((row) => ({
      ...row,
      cells: row.cells.map((cell) => ({ ...cell, intensity: cell.tickets / max })),
    }));
  return { bucketMode, periods, rows: rowsOut };
}

function buildCategoryFaultMatrixModel(rows, mode) {
  if (!rows.length) return { bucketMode: "weekly", products: [] };
  const bucketMode = resolveBucketMode(rows.map((row) => ({ metric_date: row.metric_date })), mode);
  const productMap = new Map();
  rows.forEach((row) => {
    const bucket = getBucketDescriptor(row.metric_date, bucketMode);
    const productName = row.product_name || "Unknown";
    const key = buildFaultKey(row.executive_fault_code, row.fault_code_level_2);
    if (!productMap.has(productName)) productMap.set(productName, { total: 0, periods: new Map(), faults: new Map() });
    const product = productMap.get(productName);
    product.total += Number(row.tickets || 0);
    product.periods.set(bucket.key, bucket.label);
    if (!product.faults.has(key)) {
      product.faults.set(key, {
        efc: row.executive_fault_code || "Blank",
        fc2: row.fault_code_level_2 || "",
        total: 0,
        periods: new Map(),
      });
    }
    const fault = product.faults.get(key);
    const value = Number(row.tickets || 0);
    fault.total += value;
    fault.periods.set(bucket.key, (fault.periods.get(bucket.key) || 0) + value);
  });
  const products = [...productMap.entries()]
    .map(([product_name, product]) => {
      const periods = [...product.periods.entries()].sort((a, b) => a[0].localeCompare(b[0])).map(([key, label]) => ({ key, label }));
      const sortedFaults = [...product.faults.values()].sort((a, b) => b.total - a.total || faultPrimaryLabel(a).localeCompare(faultPrimaryLabel(b)));
      const topFaults = sortedFaults.slice(0, 6);
      if (sortedFaults.length > 6) {
        const other = { efc: "Other issues", fc2: "", total: 0, periods: new Map() };
        sortedFaults.slice(6).forEach((fault) => {
          other.total += fault.total;
          fault.periods.forEach((value, key) => {
            other.periods.set(key, (other.periods.get(key) || 0) + value);
          });
        });
        if (other.total) topFaults.push(other);
      }
      const max = Math.max(...topFaults.flatMap((fault) => periods.map((period) => fault.periods.get(period.key) || 0)), 1);
      return {
        product_name,
        total: product.total,
        periods,
        faults: topFaults.map((fault) => ({
          ...fault,
          primary: faultPrimaryLabel(fault),
          secondary: faultSecondaryLabel(fault),
          cells: periods.map((period) => {
            const tickets = fault.periods.get(period.key) || 0;
            return { key: period.key, label: period.label, tickets, intensity: tickets / max };
          }),
        })),
      };
    })
    .sort((a, b) => b.total - a.total || a.product_name.localeCompare(b.product_name));
  return { bucketMode, products };
}

function getBucketDescriptor(value, mode) {
  const rawDate = toDate(value);
  if (!rawDate) return { key: "unknown", label: "Unknown" };
  if (mode === "monthly") {
    return {
      key: `${rawDate.getFullYear()}-${String(rawDate.getMonth() + 1).padStart(2, "0")}`,
      label: rawDate.toLocaleDateString("en-IN", { timeZone: "Asia/Kolkata", month: "short", year: "2-digit" }),
    };
  }
  const weekStart = startOfWeek(rawDate);
  return {
    key: isoDate(weekStart),
    label: weekStart.toLocaleDateString("en-IN", { timeZone: "Asia/Kolkata", day: "numeric", month: "short" }),
  };
}

function buildFaultKey(efc, fc2) {
  return `${String(efc || "Others")}||${String(fc2 || "")}`;
}

function faultPrimaryLabel(fault) {
  return fault.fc2 && fault.fc2 !== fault.efc ? fault.fc2 : fault.efc || "Blank";
}

function faultSecondaryLabel(fault) {
  const primary = faultPrimaryLabel(fault);
  return fault.efc && fault.efc !== primary ? fault.efc : "";
}

function renderCategoryProductTrendTable(model) {
  if (!model.rows.length) return '<div class="empty-state">No product trend available in the selected range.</div>';
  return `
    <div class="heatmap-wrap">
      <table class="heatmap-table">
        <thead>
          <tr>
            <th>Product</th>
            ${model.periods.map((period) => `<th>${escHtml(period.label)}</th>`).join("")}
            <th class="num">Total</th>
          </tr>
        </thead>
        <tbody>
          ${model.rows.map((row) => `
            <tr>
              <td class="heatmap-row-label">${escHtml(row.product_name)}</td>
              ${row.cells.map((cell) => `
                <td>
                  <div class="heat-cell" style="background: rgba(77, 142, 244, ${0.12 + cell.intensity * 0.42})">
                    ${cell.tickets ? escHtml(fmtNum(cell.tickets)) : "—"}
                  </div>
                </td>
              `).join("")}
              <td class="num"><strong>${escHtml(fmtNum(row.total))}</strong></td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>`;
}

function renderCategoryFaultMatrices(model) {
  if (!model.products.length) return '<div class="empty-state">No fault-code movement available in the selected range.</div>';
  return `
    <div class="fault-matrix-grid">
      ${model.products.map((product) => `
        <section class="fault-product-card">
          <div class="fault-product-head">
            <div>
              <h4>${escHtml(product.product_name)}</h4>
              <div class="fault-product-meta">${escHtml(fmtNum(product.total))} tickets across ${escHtml(String(product.periods.length))} ${product.periods.length === 1 ? "period" : "periods"}</div>
            </div>
          </div>
          <div class="heatmap-wrap">
            <table class="heatmap-table compact">
              <thead>
                <tr>
                  <th>Issue bucket</th>
                  ${product.periods.map((period) => `<th>${escHtml(period.label)}</th>`).join("")}
                  <th class="num">Total</th>
                </tr>
              </thead>
              <tbody>
                ${product.faults.map((fault) => `
                  <tr>
                    <td class="heatmap-row-label">
                      <div class="fault-label-stack">
                        <strong>${escHtml(fault.primary)}</strong>
                        ${fault.secondary ? `<span>${escHtml(fault.secondary)}</span>` : ""}
                      </div>
                    </td>
                    ${fault.cells.map((cell) => `
                      <td>
                        <div class="heat-cell issue" style="background: rgba(30, 197, 90, ${0.10 + cell.intensity * 0.38})">
                          ${cell.tickets ? escHtml(fmtNum(cell.tickets)) : "—"}
                        </div>
                      </td>
                    `).join("")}
                    <td class="num"><strong>${escHtml(fmtNum(fault.total))}</strong></td>
                  </tr>
                `).join("")}
              </tbody>
            </table>
          </div>
        </section>
      `).join("")}
    </div>`;
}

function renderMiniBars(rows, labelFormatter = null) {
  if (!rows.length) return '<div class="empty-state">No summary available.</div>';
  const max = Math.max(...rows.map((row) => Number(row.tickets || row.count || 0)), 1);
  return `<div class="mini-bars">${rows.slice(0, 8).map((row) => {
    const value = Number(row.tickets || row.count || 0);
    const label = labelFormatter ? labelFormatter(row.label || "Unknown") : (row.label || "Unknown");
    return `<div class="mini-bar-row"><div class="mix-row-head"><span class="mix-label">${escHtml(label)}</span><span class="mix-value">${fmtNum(value)}</span></div><div class="mini-bar-track"><div class="mini-bar-fill" style="width:${(value / max) * 100}%"></div></div></div>`;
  }).join("")}</div>`;
}

function renderMiniTable(rows, columns) {
  if (!rows.length) return '<div class="empty-state">No summary available.</div>';
  return `
    <div class="mini-table-wrap">
      <table class="mini-table">
        <thead>
          <tr>${columns.map((column) => `<th>${escHtml(column.label)}</th>`).join("")}</tr>
        </thead>
        <tbody>
          ${rows.slice(0, 12).map((row) => `
            <tr>
              ${columns.map((column) => `<td>${formatMiniCell(row, column)}</td>`).join("")}
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>`;
}

function formatMiniCell(row, column) {
  const value = row[column.key];
  if (column.format === "number") return escHtml(fmtNum(value || 0));
  if (column.format === "percentOfTickets") return escHtml(fmtPct(ratio(value, row.tickets)));
  return escHtml(formatBotActionLabelIfNeeded(column.key, value));
}

function renderMiniStat(label, value, isPercent = false) {
  return `<div class="mini-stat"><div class="mini-stat-key">${escHtml(label)}</div><div class="mini-stat-value">${isPercent ? fmtPct(value) : fmtNum(value)}</div></div>`;
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
  return `<svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="xMidYMid meet" style="width:100%;height:220px">
    ${[0, 0.5, 1].map((ratio) => {
      const y = pad.top + innerH - innerH * ratio;
      return `<line x1="${pad.left}" x2="${width - pad.right}" y1="${y}" y2="${y}" stroke="${VISUAL_THEME.grid}"></line><text x="${pad.left - 8}" y="${y + 4}" text-anchor="end" font-size="10" fill="${VISUAL_THEME.muted}">${fmtNum(Math.round(max * ratio))}</text>`;
    }).join("")}
    ${points.map((point, index) => {
    const step = innerW / Math.max(points.length, 1);
    const value = Number(point.tickets || 0);
    const barH = (value / max) * innerH;
    const x = pad.left + step * index + (step - barW) / 2;
    const y = pad.top + innerH - barH;
    return `<rect x="${x}" y="${y}" width="${barW}" height="${barH}" rx="6" fill="${VISUAL_THEME.barFill}" stroke="${VISUAL_THEME.barStroke}"></rect><text x="${x + barW / 2}" y="${Math.max(12, y - 6)}" text-anchor="middle" font-size="10" fill="${VISUAL_THEME.blue}">${fmtNum(value)}</text><text x="${x + barW / 2}" y="${height - 10}" text-anchor="middle" font-size="10" fill="${VISUAL_THEME.muted}">${escHtml(point.label)}</text>`;
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
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="xMidYMid meet" style="width:100%;height:290px">
      ${[0.25, 0.5, 0.75].map((ratio) => {
        const y = pad.top + innerH - innerH * ratio;
        return `<line x1="${pad.left}" x2="${width - pad.right}" y1="${y}" y2="${y}" stroke="${VISUAL_THEME.grid}"></line>`;
      }).join("")}
      ${[0, 0.5, 1].map((ratio) => {
        const y = pad.top + innerH - innerH * ratio;
        return `<text x="${pad.left - 8}" y="${y + 4}" text-anchor="end" font-size="10" fill="${VISUAL_THEME.muted}">${fmtNum(Math.round(max * ratio))}</text>`;
      }).join("")}
      ${points.map((point, index) => {
        const value = Number(point.value || 0);
        const barH = (value / max) * innerH;
        const x = pad.left + step * index + (step - barW) / 2;
        const y = pad.top + innerH - barH;
        return `<rect x="${x}" y="${y}" width="${barW}" height="${barH}" rx="8" fill="${color}" opacity="0.84"></rect><text x="${x + barW / 2}" y="${Math.max(12, y - 6)}" text-anchor="middle" font-size="10" fill="${color}">${fmtNum(value)}</text><text x="${x + barW / 2}" y="${height - 10}" text-anchor="middle" font-size="10" fill="${VISUAL_THEME.muted}">${escHtml(point.label)}</text>`;
      }).join("")}
      <text x="12" y="${pad.top + 12}" font-size="11" fill="${VISUAL_THEME.muted}">${escHtml(yLabel)}</text>
    </svg>`;
}

function buildQueryParams(filters) {
  const params = new URLSearchParams();
  Object.entries(filters).forEach(([key, value]) => {
    if (Array.isArray(value)) value.forEach((item) => params.append(key, item));
    else if (value) params.set(key, value);
  });
  const overrides = normalizeMappingOverrides(state.mappingOverrides);
  if (Object.keys(overrides.product_category_overrides).length || Object.keys(overrides.efc_overrides).length) {
    params.set("mapping_overrides", JSON.stringify(overrides));
  }
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

function reconcileIssueWidgetFilters() {
  const options = getIssueWidgetOptions();
  const validCategories = new Set(options.categories.map((item) => item.label));
  const validProducts = new Set(options.products.map((item) => item.label));
  state.issueWidgetFilters.categories = state.issueWidgetFilters.categories.filter((value) => validCategories.has(value));
  state.issueWidgetFilters.products = state.issueWidgetFilters.products.filter((value) => validProducts.has(value));
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

function summarizeSelection(control, options, selected) {
  if (!selected.length) return { main: "All selected", count: options.length ? `${options.length}` : "" };
  if (selected.length === 1) return { main: formatOptionLabel(control, selected[0]), count: "1 selected" };
  return { main: formatOptionLabel(control, selected[0]), count: `+${selected.length - 1} more` };
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
    const label = bucketMode === "monthly"
      ? rawDate.toLocaleDateString("en-IN", { timeZone: "Asia/Kolkata", month: "short", year: "2-digit" })
      : rawDate.toLocaleDateString("en-IN", { timeZone: "Asia/Kolkata", day: "numeric", month: "short" });
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
    els.productHealthTable,
    els.issueBoard,
    els.botOverview,
    els.botTrendChart,
    els.botLeakyIssues,
    els.botBestIssues,
    els.categoryDonut,
    els.channelDonut,
    els.botActionDonut,
    els.departmentMix,
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
    els.productHealthTable,
    els.issueBoard,
    els.botOverview,
    els.botTrendChart,
    els.botLeakyIssues,
    els.botBestIssues,
    els.categoryDonut,
    els.channelDonut,
    els.botActionDonut,
    els.departmentMix,
    els.installationMix,
    els.pipelineHealth,
  ].forEach((element) => { element.innerHTML = `<div class="error-state">${escHtml(message)}</div>`; });
}

function loadSessionJson(key, fallback) {
  try {
    const raw = window.sessionStorage.getItem(key);
    return raw ? JSON.parse(raw) : fallback;
  } catch {
    return fallback;
  }
}

function saveSessionJson(key, value) {
  try {
    window.sessionStorage.setItem(key, JSON.stringify(value));
  } catch {}
}

function cloneMappingOverrides(value) {
  return {
    product_category_overrides: { ...(value?.product_category_overrides || {}) },
    efc_overrides: { ...(value?.efc_overrides || {}) },
  };
}

function normalizeMappingOverrides(value) {
  const normalizeScope = (scope) => Object.fromEntries(
    Object.entries(scope || {})
      .map(([key, item]) => [String(key).trim().toLowerCase(), String(item).trim()])
      .filter(([key, item]) => key && item)
  );
  return {
    product_category_overrides: normalizeScope(value?.product_category_overrides),
    efc_overrides: normalizeScope(value?.efc_overrides),
  };
}

function mergeDraftWithPayload(draft, mappingStudio) {
  const next = cloneMappingOverrides(draft);
  const productKeys = new Set((mappingStudio.product_rows || []).map((row) => String(row.product_name).toLowerCase()));
  const fc2Keys = new Set((mappingStudio.fc2_rows || []).map((row) => String(row.fault_code_level_2).toLowerCase()));
  next.product_category_overrides = Object.fromEntries(Object.entries(next.product_category_overrides).filter(([key]) => productKeys.has(key)));
  next.efc_overrides = Object.fromEntries(Object.entries(next.efc_overrides).filter(([key]) => fc2Keys.has(key)));
  return next;
}

function hasDraftOverride(scope, key, baseValue) {
  const overrideValue = state.mappingDraft?.[scope]?.[String(key).toLowerCase()];
  return Boolean(overrideValue && overrideValue !== baseValue);
}

function exportMappingOverrides() {
  const payload = normalizeMappingOverrides(state.mappingDraft);
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = "qubo-mapping-overrides-session.json";
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
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
    timeZone: "Asia/Kolkata",
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: true,
  });
}

function shortDate(value) {
  const date = toDate(value);
  if (!date) return "";
  return date.toLocaleDateString("en-IN", { timeZone: "Asia/Kolkata", day: "2-digit", month: "short", year: "numeric" });
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
  const normalized = typeof value === "string" && value.includes("T") && !/[zZ]|[+\-]\d{2}:\d{2}$/.test(value)
    ? `${value}Z`
    : value;
  const date = new Date(normalized);
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

function formatBotActionLabel(value) {
  if (value === "No bot action") return "No recorded bot action";
  if (value === "Other bot/system action") return "Other unmapped bot/system action";
  return value;
}

function prettyIsoDate(value) {
  const date = toDate(value);
  if (!date) return value || "";
  return date.toLocaleDateString("en-IN", { timeZone: "Asia/Kolkata", day: "2-digit", month: "short", year: "numeric" });
}

function formatOptionLabel(control, value) {
  if (!value) return "Unknown";
  if (control.optionsKey === "bot_actions" || control.key.includes("bot_action")) {
    return formatBotActionLabel(value);
  }
  return value;
}

function formatBotActionLabelIfNeeded(key, value) {
  if (key.includes("bot") || key === "label") {
    return formatBotActionLabel(value);
  }
  return value ?? "Unknown";
}

function ratio(numerator, denominator) {
  const n = Number(numerator || 0);
  const d = Number(denominator || 0);
  return d ? n / d : 0;
}
