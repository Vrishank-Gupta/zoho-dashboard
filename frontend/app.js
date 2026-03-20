// ── State ──────────────────────────────────────────────────────────────────
const state = {
  apiBase: (window.QUBO_APP_CONFIG?.apiBaseUrl || "").replace(/\/$/, ""),
  filters: { date_preset: "60d", products: [], models: [], fault_codes: [], channels: [], bot_actions: [], quick_exclusions: [] },
  issueView: "biggest_burden",
  payload: null,
  loading: false,
  quickFilterLoadingKey: null,
  requestSeq: 0,
};
let inflightController = null;

// ── Element refs ────────────────────────────────────────────────────────────
const $ = (id) => document.getElementById(id);
const els = {
  sourceBadge: $("sourceBadge"),
  viewStatus: $("viewStatus"),
  lastUpdated: $("lastUpdated"),
  runPipelineBtn: $("runPipelineBtn"),
  refreshPipelineBtn: $("refreshPipelineBtn"),
  metaDate: $("metaDate"),
  metaSource: $("metaSource"),
  kpiRow: $("kpiRow"),
  volumeChart: $("volumeChart"),
  volumeInsight: $("volumeInsight"),
  channelChart: $("channelChart"),
  channelLegend: $("channelLegend"),
  channelInsight: $("channelInsight"),
  fieldSummary: $("fieldSummary"),
  botOutcomesChart: $("botOutcomesChart"),
  resolutionList: $("resolutionList"),
  productMatrix: $("productMatrix"),
  productMatrixInsight: $("productMatrixInsight"),
  issueTabs: $("issueTabs"),
  issueBoard: $("issueBoard"),
  botKpiRow: $("botKpiRow"),
  botProductMatrix: $("botProductMatrix"),
  botProductInsight: $("botProductInsight"),
  botBestIssues: $("botBestIssues"),
  botLeakyIssues: $("botLeakyIssues"),
  fieldTrendInsight: $("fieldTrendInsight"),
  fieldVisitByProductInsight: $("fieldVisitByProductInsight"),
  qualityCards: $("qualityCards"),
  pipelineHealth: $("pipelineHealth"),
  datePreset: $("datePreset"),
  productFilter: $("productFilter"),
  modelFilter: $("modelFilter"),
  faultCodeFilter: $("faultCodeFilter"),
  channelFilter: $("channelFilter"),
  botActionFilter: $("botActionFilter"),
  resetFilters: $("resetFilters"),
  drawer: $("issueDrawer"),
  drawerBackdrop: $("drawerBackdrop"),
  drawerTitle: $("drawerTitle"),
  drawerContent: $("drawerContent"),
  closeDrawer: $("closeDrawer"),
  sidebarMeta: $("sidebarMeta"),
  chartTooltip: $("chartTooltip"),
};

// ── Init ────────────────────────────────────────────────────────────────────
bindEvents();
renderDashboardLoading();
loadDashboard();

// ── Events ──────────────────────────────────────────────────────────────────
function bindEvents() {
  els.datePreset.addEventListener("change", () => { state.filters.date_preset = els.datePreset.value; loadDashboard(); });

  els.resetFilters.addEventListener("click", () => {
    state.filters = { date_preset: "60d", products: [], models: [], fault_codes: [], channels: [], bot_actions: [], quick_exclusions: [] };
    state.issueView = "biggest_burden";
    syncControls();
    loadDashboard();
  });

  // Close all multi-select panels on outside click
  document.addEventListener("click", () => {
    document.querySelectorAll(".ms-panel").forEach((p) => p.classList.add("hidden"));
  });

  els.runPipelineBtn.addEventListener("click", triggerPipeline);
  els.refreshPipelineBtn.addEventListener("click", () => refreshPipelineStatus());

  els.issueTabs.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-view]");
    if (!btn) return;
    state.issueView = btn.dataset.view;
    els.issueTabs.querySelectorAll(".tab").forEach((t) => t.classList.toggle("active", t === btn));
    if (state.payload) renderIssueBoard(state.payload.issue_views || {});
  });

  document.querySelectorAll(".nav-item").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".nav-item").forEach((n) => n.classList.remove("active"));
      btn.classList.add("active");
      $(btn.dataset.target)?.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  });

  els.drawerBackdrop.addEventListener("click", closeDrawer);
  els.closeDrawer.addEventListener("click", closeDrawer);
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      if (!els.drawer.classList.contains("hidden")) closeDrawer();
      document.querySelectorAll(".ms-panel").forEach((p) => p.classList.add("hidden"));
    }
  });
}

function setLoading(isLoading) {
  state.loading = isLoading;
  document.body.classList.toggle("is-loading", isLoading);
  document.body.classList.toggle("dashboard-loading", isLoading);
  document.body.setAttribute("aria-busy", isLoading ? "true" : "false");
  els.viewStatus.textContent = isLoading ? "Updating data" : "Ready";
  els.datePreset.disabled = isLoading;
  els.resetFilters.disabled = isLoading;
  if (els.refreshPipelineBtn) els.refreshPipelineBtn.disabled = isLoading;
  setSurfaceLoading(isLoading);
  if (!isLoading) {
    state.quickFilterLoadingKey = null;
  }
}

function renderDashboardLoading() {
  els.kpiRow.innerHTML = new Array(5).fill('<div class="kpi-card skeleton-card"><div class="skeleton-line lg"></div><div class="skeleton-line md"></div><div class="skeleton-line sm"></div></div>').join("");
  [
    els.volumeChart,
    els.channelChart,
    els.fieldSummary,
    els.botOutcomesChart,
    els.resolutionList,
    els.productMatrix,
    els.issueBoard,
    els.botKpiRow,
    els.botProductMatrix,
    els.botBestIssues,
    els.botLeakyIssues,
    els.qualityCards,
    els.pipelineHealth,
  ].forEach((el) => {
    if (!el) return;
    el.innerHTML = loadingStateHtml();
  });
  [els.volumeInsight, els.channelInsight, els.fieldTrendInsight, els.productMatrixInsight, els.fieldVisitByProductInsight, els.botProductInsight]
    .forEach((el) => { if (el) el.innerHTML = ""; });
}

function loadingStateHtml(lines = 4) {
  return `
    <div class="loading-state" aria-live="polite">
      <div class="loading-spinner"></div>
      <div class="loading-copy">Fetching data...</div>
      <div class="loading-skeleton">
        ${new Array(lines).fill('<div class="skeleton-line"></div>').join("")}
      </div>
    </div>
  `;
}

function drawerLoadingHtml(label = "Loading details...") {
  return `
    <div class="drawer-loading" aria-live="polite">
      <div class="loading-spinner large"></div>
      <div class="loading-copy">${esc(label)}</div>
      <div class="loading-note">Pulling the latest breakdown for this selection.</div>
      <div class="loading-skeleton">
        <div class="skeleton-line lg"></div>
        <div class="skeleton-line"></div>
        <div class="skeleton-line"></div>
        <div class="skeleton-line sm"></div>
      </div>
    </div>
  `;
}

function setSurfaceLoading(isLoading) {
  document.querySelectorAll(".chart-card, .ops-card, .data-card, .kpi-card, .widget-qf-chip, .issue-card, .pt-row, .imi-card, .drill-issue-row, .category-cloud-word")
    .forEach((el) => {
      el.classList.toggle("surface-loading", isLoading);
      if (isLoading) el.setAttribute("aria-disabled", "true");
      else el.removeAttribute("aria-disabled");
    });
}

// ── Data loading ─────────────────────────────────────────────────────────────
async function loadDashboard() {
  // Cancel any previous in-flight requests before starting new ones.
  if (inflightController) inflightController.abort();
  inflightController = new AbortController();
  const signal = inflightController.signal;

  const requestId = ++state.requestSeq;

  // Legacy params deliberately omit quick_exclusions (and bot_actions) — these
  // flags trigger slow source-mode Python recomputation on the legacy path.
  // V2 endpoints handle them via SQL and overwrite the relevant widgets after.
  const legacyParams = new URLSearchParams({ date_preset: state.filters.date_preset });
  if (state.filters.products.length)    legacyParams.set("products",    state.filters.products.join(","));
  if (state.filters.models.length)      legacyParams.set("models",      state.filters.models.join(","));
  if (state.filters.fault_codes.length) legacyParams.set("fault_codes", state.filters.fault_codes.join(","));
  if (state.filters.channels.length)    legacyParams.set("channels",    state.filters.channels.join(","));

  setLoading(true);
  const orgV2Promise = fetchOrgLevelV2Bundle(signal).catch(() => null);
  try {
    const payload = await fetchJson(`${state.apiBase}/api/dashboard?${legacyParams}`, { timeoutMs: 20000, signal });
    if (requestId !== state.requestSeq) return;
    state.payload = payload;
    hydrateFilters({
      ...(payload.filter_options || {}),
      models_by_product: payload.filter_options?.models_by_product
        || Object.fromEntries(Object.entries(payload.model_breakdown || {}).map(([family, rows]) => [family, rows.map((row) => row.model)])),
    });
    renderDashboard(payload);
    refreshPipelineStatus({ silent: true });
    const orgV2 = await orgV2Promise;
    if (requestId !== state.requestSeq || !orgV2) return;
    state.payload = mergeOrgLevelPayload(state.payload, orgV2);
    renderDashboard(state.payload);
  } catch (err) {
    if (requestId !== state.requestSeq) return;
    showError(err.message);
  } finally {
    if (requestId === state.requestSeq) setLoading(false);
  }
}

async function fetchOrgLevelV2Bundle(signal) {
  const params = new URLSearchParams({ date_preset: state.filters.date_preset });
  if (state.filters.products.length) params.set("products", state.filters.products.join(","));
  if (state.filters.models.length) params.set("models", state.filters.models.join(","));
  if (state.filters.fault_codes.length) params.set("fault_codes", state.filters.fault_codes.join(","));
  if (state.filters.channels.length) params.set("channels", state.filters.channels.join(","));
  if (state.filters.bot_actions.length) params.set("bot_actions", state.filters.bot_actions.join(","));
  if (state.filters.quick_exclusions.length) params.set("quick_exclusions", state.filters.quick_exclusions.join(","));
  const q = params.toString();
  // Per-endpoint error isolation: one slow or missing endpoint won't null out the whole bundle.
  const tryFetch = (url) => fetchJson(url, { timeoutMs: 12000, signal }).catch(() => null);
  const [summary, trend, productBurden, modelBreakdown, channelMix, issueBoard] = await Promise.all([
    tryFetch(`${state.apiBase}/api/v2/summary?${q}`),
    tryFetch(`${state.apiBase}/api/v2/trend?${q}`),
    tryFetch(`${state.apiBase}/api/v2/product-burden?${q}`),
    tryFetch(`${state.apiBase}/api/v2/model-breakdown?${q}`),
    tryFetch(`${state.apiBase}/api/v2/channel-mix?${q}`),
    tryFetch(`${state.apiBase}/api/v2/issues?${q}`),
  ]);
  // If all critical endpoints failed, signal fallback by returning null.
  if (!summary && !trend && !productBurden) return null;
  return { summary, trend, productBurden, modelBreakdown, channelMix, issueBoard };
}

function mergeOrgLevelPayload(basePayload, orgV2) {
  if (!basePayload || !orgV2) return basePayload;
  const groupedModels = {};
  for (const row of (orgV2.modelBreakdown?.rows || [])) {
    if (!groupedModels[row.product_family]) groupedModels[row.product_family] = [];
    groupedModels[row.product_family].push({
      model: row.model_name,
      tickets: row.tickets,
      repair_field_visit_rate: row.repair_field_visit_rate || 0,
      installation_field_visit_rate: row.installation_field_visit_rate || 0,
      repeat_rate: row.repeat_rate || 0,
      bot_deflection_rate: row.bot_deflection_rate || 0,
      bot_transfer_rate: row.bot_transfer_rate || 0,
      blank_chat_rate: 0,
    });
  }
  const merged = {
    ...basePayload,
    meta: {
      ...(basePayload.meta || {}),
      semantic_v2_org: true,
    },
    kpis: orgV2.summary?.kpis || basePayload.kpis,
    timeline: orgV2.trend?.timeline || basePayload.timeline,
    product_health: orgV2.productBurden?.rows || basePayload.product_health,
    model_breakdown: Object.keys(groupedModels).length ? groupedModels : basePayload.model_breakdown,
    issue_views: orgV2.issueBoard?.issue_views || basePayload.issue_views,
    service_ops: {
      ...(basePayload.service_ops || {}),
      channel_mix: orgV2.channelMix?.rows || basePayload.service_ops?.channel_mix || [],
    },
  };
  if (!merged.executive_summary && merged.product_health?.length) {
    merged.executive_summary = {
      headline: "CS Snapshot",
      summary: `${merged.product_health[0].product_family} is carrying the highest ticket load in the selected window.`,
    };
  }
  return merged;
}

// ── Render all ───────────────────────────────────────────────────────────────
function renderDashboard(p) {
  const summary = p.executive_summary || {};
  const pipeline = p.pipeline_health || {};
  const bot = p.bot_summary || {};
  const ops = p.service_ops || {};
  const timeline = p.timeline || [];
  const products = p.product_health || [];
  const botProducts = bot.by_product || [];

  // topbar / meta
  els.sourceBadge.textContent = p.meta?.warehouse_mode ? "Live data" : "Sample data";
  els.sourceBadge.style.background = p.meta?.warehouse_mode ? "rgba(31,102,69,0.1)" : "rgba(153,96,16,0.1)";
  els.sourceBadge.style.color = p.meta?.warehouse_mode ? "#1f6645" : "#996010";
  els.lastUpdated.textContent = pipeline.last_run_at ? `Last run: ${pipeline.last_run_at}` : "";
  els.metaDate.textContent = pipeline.last_run_at || "-";
  els.metaSource.textContent = p.meta?.source_mode === "mysql" ? "MySQL warehouse" : "Sample data";

  renderKpis(p.kpis || {});
  renderVolumeChart(timeline, state.filters.date_preset);
  renderVolumeInsight(timeline, state.filters.date_preset);
  renderChannelChart(ops.channel_mix || []);
  renderChannelInsight(ops.channel_mix || []);
  renderFieldTrendChart(timeline, state.filters.date_preset);
  renderFieldTrendInsight(timeline, state.filters.date_preset);
  renderBarList(els.fieldSummary, buildFieldRows(p.kpis || {}, ops.field_service_split || []), { countFmt: (v) => fmtNum(v) });
  renderBarList(els.botOutcomesChart, (ops.bot_outcomes || []).map((r) => ({ label: r.label, count: r.count, share: r.share })), { countFmt: (v) => fmtNum(v) });
  renderBarList(els.resolutionList, (ops.resolution_mix || []).slice(0, 5).map((r) => ({ label: r.label, count: r.count, share: r.share })), { countFmt: (v) => fmtNum(v) });
  renderProductInsights(products);
  renderProductMatrix(products);
  renderFieldVisitProductInsight(products);
  renderFieldVisitByProduct(products);
  renderModelBreakdown(p.model_breakdown || {});
  renderIssueBoard(p.issue_views || {});
  renderBotKpis(bot.overview || {});
  renderBotProductInsight(botProducts);
  renderBotProductMatrix(botProducts);
  renderMiniIssueList(els.botBestIssues, bot.best_issues || [], "No chatbot wins yet.");
  renderMiniIssueList(els.botLeakyIssues, bot.leaky_issues || [], "No leakage detected.");
  renderQuality(p.cleaning_summary || {});
  renderPipeline(pipeline);
  renderWidgetQuickFilters();
}


const QUICK_EXCLUSION_OPTIONS = [
  { key: "installations", label: "No Installations" },
  { key: "blank_chat", label: "No Blank Chat" },
  { key: "duplicate_tickets", label: "No Duplicates" },
  { key: "sales_marketing", label: "No Sales/Marketing" },
];

function quickFilterBarHtml() {
  return `<div class="widget-quick-filters">
    ${QUICK_EXCLUSION_OPTIONS.map((option) => `
      <button type="button" class="widget-qf-chip${state.filters.quick_exclusions.includes(option.key) ? " active" : ""}${state.quickFilterLoadingKey === option.key ? " loading" : ""}" data-quick-exclusion="${option.key}" ${state.loading ? "disabled" : ""}>
        ${state.quickFilterLoadingKey === option.key ? `<span class="widget-qf-spinner" aria-hidden="true"></span>` : ""}
        ${esc(option.label)}
      </button>`).join("")}
  </div>`;
}

function renderWidgetQuickFilters() {
  document.querySelectorAll(".chart-card, .data-card").forEach((card) => {
    const header = card.querySelector(".chart-header, .data-card-header");
    if (!header) return;
    let host = card.querySelector(".widget-quick-filters");
    if (!host) {
      const wrapper = document.createElement("div");
      wrapper.innerHTML = quickFilterBarHtml();
      host = wrapper.firstElementChild;
      header.insertAdjacentElement("afterend", host);
    } else {
      host.outerHTML = quickFilterBarHtml();
    }
  });
  document.querySelectorAll("[data-quick-exclusion]").forEach((btn) => {
    wireAction(btn, () => toggleQuickExclusion(btn.dataset.quickExclusion));
  });
}

function toggleQuickExclusion(key) {
  if (state.loading) return;
  const next = new Set(state.filters.quick_exclusions);
  if (next.has(key)) next.delete(key);
  else next.add(key);
  state.quickFilterLoadingKey = key;
  state.filters.quick_exclusions = [...next];
  renderWidgetQuickFilters();
  // Quick exclusions are handled entirely by v2 via SQL WHERE clauses.
  // Re-calling loadDashboard() would re-fetch legacy (which strips exclusions) and
  // flash incorrect numbers. Instead just re-fetch the v2 bundle and re-merge.
  loadV2Only();
}

async function loadV2Only() {
  if (inflightController) inflightController.abort();
  inflightController = new AbortController();
  const signal = inflightController.signal;
  const requestId = ++state.requestSeq;
  setLoading(true);
  try {
    const orgV2 = await fetchOrgLevelV2Bundle(signal);
    if (requestId !== state.requestSeq || !orgV2) return;
    state.payload = mergeOrgLevelPayload(state.payload, orgV2);
    renderDashboard(state.payload);
  } catch (err) {
    if (requestId !== state.requestSeq) return;
    showError(err.message);
  } finally {
    if (requestId === state.requestSeq) setLoading(false);
  }
}

function renderInsightStrip(container, cards, emptyMsg = "No insights available.") {
  if (!container) return;
  if (!cards.length) {
    container.innerHTML = `<div class="chart-insight-empty">${esc(emptyMsg)}</div>`;
    return;
  }
  container.innerHTML = cards.map((card) => `
    <div class="chart-insight-card">
      <div class="chart-insight-label">${esc(card.label)}</div>
      <div class="chart-insight-value">${esc(card.value)}</div>
      <div class="chart-insight-note">${esc(card.note)}</div>
    </div>`).join("");
}

function renderVolumeInsight(points, datePreset) {
  const data = bucketTimeline(points, datePreset);
  if (!data.length) {
    renderInsightStrip(els.volumeInsight, [], "No trend context available.");
    return;
  }
  const peak = [...data].sort((a, b) => b.tickets - a.tickets)[0];
  const latest = data[data.length - 1];
  const avgTickets = Math.round(data.reduce((sum, row) => sum + row.tickets, 0) / data.length);
  const latestVsAvg = avgTickets > 0 ? ((latest.tickets - avgTickets) / avgTickets) : 0;
  renderInsightStrip(els.volumeInsight, [
    { label: "Peak period", value: peak.label, note: `${fmtNum(peak.tickets)} tickets, bot resolved ${fmtPct(peak.bot_rate)}` },
    { label: "Current run-rate", value: fmtNum(latest.tickets), note: `${latestVsAvg >= 0 ? "+" : ""}${fmtPct(latestVsAvg)} vs period average` },
    { label: "Average period", value: fmtNum(avgTickets), note: `Repair ${fmtNum(latest.repair_field)} and installation ${fmtNum(latest.install_field)} in latest bucket` },
  ]);
}

function renderChannelInsight(channelMix) {
  const rows = [...channelMix].sort((a, b) => Number(b.count || 0) - Number(a.count || 0));
  const total = rows.reduce((sum, row) => sum + Number(row.count || 0), 0) || 1;
  const top = rows[0];
  const second = rows[1];
  renderInsightStrip(els.channelInsight, top ? [
    { label: "Primary channel", value: top.label, note: `${fmtPct((top.count || 0) / total)} of ticket intake` },
    { label: "Next largest", value: second ? second.label : "N/A", note: second ? `${fmtPct((second.count || 0) / total)} share` : "No second channel in selection" },
  ] : [], "No channel mix available.");
}

function renderFieldTrendInsight(points, datePreset) {
  const data = bucketTimeline(points, datePreset);
  if (!data.length) {
    renderInsightStrip(els.fieldTrendInsight, [], "No field visit trend available.");
    return;
  }
  const ranked = data.map((row) => ({ ...row, totalField: Number(row.repair_field || 0) + Number(row.install_field || 0) }))
    .sort((a, b) => b.totalField - a.totalField);
  const peak = ranked[0];
  const latest = data[data.length - 1];
  renderInsightStrip(els.fieldTrendInsight, [
    { label: "Highest burden", value: peak.label, note: `${fmtNum(peak.totalField)} field visits, repair ${fmtNum(peak.repair_field)}` },
    { label: "Latest period", value: fmtNum(Number(latest.repair_field || 0) + Number(latest.install_field || 0)), note: `Repair ${fmtNum(latest.repair_field)} and installation ${fmtNum(latest.install_field)}` },
    { label: "Mix signal", value: peak.repair_field >= peak.install_field ? "Repair-led" : "Installation-led", note: `Use this chart to separate product-quality pressure from onboarding pressure` },
  ]);
}

function renderProductInsights(products) {
  if (!products.length) {
    renderInsightStrip(els.productMatrixInsight, [], "No product insight available.");
    return;
  }
  const ranked = [...products].sort((a, b) => Number(b.ticket_volume || 0) - Number(a.ticket_volume || 0));
  const byRepeat = [...products].sort((a, b) => Number(b.repeat_rate || 0) - Number(a.repeat_rate || 0))[0];
  const byRepair = [...products].sort((a, b) => Number(b.repair_field_visit_rate || 0) - Number(a.repair_field_visit_rate || 0))[0];
  renderInsightStrip(els.productMatrixInsight, [
    { label: "Largest portfolio load", value: ranked[0].product_family, note: `${fmtNum(ranked[0].ticket_volume)} tickets, top issue ${cleanCopy(ranked[0].top_issue || "Unavailable")}` },
    { label: "Highest repeat risk", value: byRepeat.product_family, note: `${fmtPct(byRepeat.repeat_rate)} repeat rate` },
    { label: "Highest service cost", value: byRepair.product_family, note: `${fmtPct(byRepair.repair_field_visit_rate)} repair visit rate` },
  ]);
}

function renderFieldVisitProductInsight(products) {
  const rows = products
    .filter((p) => !NON_PRODUCT_FAMILIES.has(p.product_family))
    .filter((p) => (Number(p.repair_field_visit_rate || 0) + Number(p.installation_field_visit_rate || 0)) > 0);
  if (!rows.length) {
    renderInsightStrip(els.fieldVisitByProductInsight, [], "No field visit product insight available.");
    return;
  }
  const ranked = [...rows].sort((a, b) =>
    (Number(b.repair_field_visit_rate || 0) + Number(b.installation_field_visit_rate || 0)) -
    (Number(a.repair_field_visit_rate || 0) + Number(a.installation_field_visit_rate || 0))
  );
  const top = ranked[0];
  const installLed = [...rows].sort((a, b) => Number(b.installation_field_visit_rate || 0) - Number(a.installation_field_visit_rate || 0))[0];
  const repairLed = [...rows].sort((a, b) => Number(b.repair_field_visit_rate || 0) - Number(a.repair_field_visit_rate || 0))[0];
  renderInsightStrip(els.fieldVisitByProductInsight, [
    { label: "Highest total burden", value: top.product_family, note: `${fmtPct((top.repair_field_visit_rate || 0) + (top.installation_field_visit_rate || 0))} field visits across ${fmtNum(top.ticket_volume)} tickets` },
    { label: "Most repair-heavy", value: repairLed.product_family, note: `${fmtPct(repairLed.repair_field_visit_rate || 0)} repair visits` },
    { label: "Most installation-heavy", value: installLed.product_family, note: `${fmtPct(installLed.installation_field_visit_rate || 0)} installation visits` },
  ]);
}

function renderBotProductInsight(rows) {
  if (!rows.length) {
    renderInsightStrip(els.botProductInsight, [], "No chatbot product insight available.");
    return;
  }
  const byResolved = [...rows].sort((a, b) => Number(b.bot_resolved_rate || 0) - Number(a.bot_resolved_rate || 0))[0];
  const byTransfer = [...rows].sort((a, b) => Number(b.bot_transferred_rate || 0) - Number(a.bot_transferred_rate || 0))[0];
  const byBlank = [...rows].sort((a, b) => Number(b.blank_chat_rate || 0) - Number(a.blank_chat_rate || 0))[0];
  renderInsightStrip(els.botProductInsight, [
    { label: "Best containment", value: byResolved.product_family, note: `${fmtPct(byResolved.bot_resolved_rate || 0)} chatbot resolved` },
    { label: "Largest leakage", value: byTransfer.product_family, note: `${fmtPct(byTransfer.bot_transferred_rate || 0)} transferred to agent` },
    { label: "Highest abandonment", value: byBlank.product_family, note: `${fmtPct(byBlank.blank_chat_rate || 0)} blank chat rate` },
  ]);
}

// ── KPIs ─────────────────────────────────────────────────────────────────────
function renderKpis(kpis) {
  const cards = [
    {
      key: "total_tickets", label: "Total tickets", type: "count", accent: "blue", badWhenUp: false,
      tip: "Total tickets in the selected date window and filter set.", navTarget: "section-org",
    },
    {
      key: "repeat_rate", label: "Repeat rate", type: "pct", accent: "red", badWhenUp: true,
      tip: "Repeat rate = tickets that reopened on the same device + issue within 30 days / total tickets. Higher is worse and indicates unresolved issues.",
    },
    {
      key: "bot_deflection_rate", label: "Chatbot resolved", type: "pct", accent: "green", badWhenUp: false,
      tip: "Chatbot resolved rate = tickets fully closed by the chatbot without agent handoff / all chat tickets. Higher is better.",
    },
    {
      key: "bot_transfer_rate", label: "Chatbot transfer", type: "pct", accent: "amber", badWhenUp: true,
      tip: "Chatbot transfer rate = sessions where chatbot escalated to a live agent / chat tickets. High values mean chatbot containment is weak.",
    },
    {
      key: "repair_field_visit_rate", label: "Repair visit rate", type: "pct", accent: "purple", badWhenUp: true,
      tip: "Repair visit rate = tickets that resulted in a field repair visit / total tickets. Indicates service cost and product quality pressure.",
    },
  ];
  els.kpiRow.innerHTML = cards.map(({ key, label, type, accent, badWhenUp, tip }) => {
    const m = kpis[key] || { value: 0, change: 0 };
    const val = type === "count" ? fmtNum(m.value) : fmtPct(m.value);
    const chg = Number(m.change || 0);
    const improving = badWhenUp ? chg < 0 : chg >= 0;
    const deltaClass = chg === 0 ? "neutral" : improving ? "good" : "bad";
    const deltaSign = chg > 0 ? "+" : "";
    return `
      <div class="kpi-card accent-${accent}">
        <div class="kpi-label">${esc(label)} <button class="info-btn" data-tip="${esc(tip)}">?</button></div>
        <div class="kpi-value">${val}</div>
        <div class="kpi-delta ${deltaClass}">${deltaSign}${fmtPct(chg)} vs prior period</div>
      </div>`;
  }).join("");
}

// ── Shared bucket/label helpers ────────────────────────────────────────────────
function getBucketFns(datePreset) {
  const MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  if (datePreset === "14d") {
    return {
      bucketFn: (d) => d,
      labelFn: (k) => { const dt = new Date(k + "T00:00:00"); return `${dt.getDate()} ${MONTHS[dt.getMonth()]}`; },
    };
  } else if (datePreset === "history") {
    return {
      bucketFn: (d) => d.slice(0, 7),
      labelFn: (k) => { const [y, m] = k.split("-"); return `${MONTHS[+m-1]}'${y.slice(2)}`; },
    };
  } else {
    return {
      bucketFn: (d) => { const dt = new Date(d + "T00:00:00"); const day = dt.getDay(); const diff = (day === 0 ? -6 : 1 - day); dt.setDate(dt.getDate() + diff); return dt.toISOString().slice(0, 10); },
      labelFn: (k) => {
        const dt = new Date(k + "T00:00:00");
        const end = new Date(dt); end.setDate(end.getDate() + 6);
        const sm = MONTHS[dt.getMonth()], em = MONTHS[end.getMonth()];
        return sm === em
          ? `${dt.getDate()}-${end.getDate()} ${sm}`
          : `${dt.getDate()} ${sm}-${end.getDate()} ${em}`;
      },
    };
  }
}

function bucketTimeline(points, datePreset) {
  const { bucketFn, labelFn } = getBucketFns(datePreset);
  const bucketed = {};
  for (const p of points) {
    if (!p.date) continue;
    const key = bucketFn(p.date);
    if (!bucketed[key]) bucketed[key] = { tickets: 0, repair_field: 0, install_field: 0, bot_resolved: 0 };
    bucketed[key].tickets += Number(p.tickets || 0);
    bucketed[key].repair_field += Number(p.repair_field || 0);
    bucketed[key].install_field += Number(p.install_field || 0);
    bucketed[key].bot_resolved += Number(p.bot_resolved || 0);
  }
  return Object.keys(bucketed).sort().map((k) => ({
    key: k, label: labelFn(k), ...bucketed[k],
    bot_rate: bucketed[k].tickets > 0 ? bucketed[k].bot_resolved / bucketed[k].tickets : 0,
  }));
}

// ── Volume chart — CXO-style bars + bot resolved trend line ──────────────────
function renderVolumeChart(points, datePreset) {
  const el = els.volumeChart;
  if (!points.length) { el.innerHTML = empty("No timeline data."); return; }

  const data = bucketTimeline(points, datePreset);
  if (!data.length) { el.innerHTML = empty("No data."); return; }

  const maxTickets = Math.max(...data.map((d) => d.tickets), 1);
  const W = 680, H = 230, padL = 52, padB = 34, padT = 24, padR = 14;
  const chartW = W - padL - padR, chartH = H - padB - padT;
  const step = chartW / data.length;
  const barW = Math.max(10, Math.floor(step * 0.72));
  const toBarY = (v) => padT + chartH - (v / maxTickets) * chartH;
  const toLineY = (v) => padT + chartH - v * chartH; // v is 0-1 fraction
  const avgTickets = data.reduce((sum, row) => sum + row.tickets, 0) / data.length;
  const avgY = toBarY(avgTickets);

  // Grid lines
  const gridLines = [0, 0.25, 0.5, 0.75, 1.0].map((f) => {
    const y = toBarY(f * maxTickets);
    const label = f === 0 ? "0" : fmtNum(Math.round(f * maxTickets));
    return `<line x1="${padL}" y1="${y}" x2="${W - padR}" y2="${y}" stroke="#ddd8cf" stroke-width="1"/>
            <text x="${padL - 6}" y="${y + 4}" text-anchor="end" font-size="9" fill="#6b6659">${label}</text>`;
  }).join("");

  // Bars — data-* attributes drive the floating tooltip
  const barsHtml = data.map((d, i) => {
    const x = padL + i * step + (step - barW) / 2;
    const barY = toBarY(d.tickets), barH = padT + chartH - barY;
    const repH = Math.round(barH * (d.repair_field / Math.max(d.tickets, 1)));
    const insH = Math.round(barH * (d.install_field / Math.max(d.tickets, 1)));
    const showNum = barW >= 22;
    const numY = Math.max(barY - 4, padT + 10);
    return `<g class="vol-bar chart-interactive" style="cursor:pointer" tabindex="0" role="button"
      data-key="${esc(d.key)}" data-label="${esc(d.label)}" data-tickets="${d.tickets}"
      data-repair="${d.repair_field}" data-install="${d.install_field}"
      data-botn="${d.bot_resolved}" data-botr="${(d.bot_rate * 100).toFixed(1)}">
      <rect x="${x - 4}" y="${padT}" width="${barW + 8}" height="${chartH}" fill="transparent"/>
      <rect x="${x}" y="${barY}" width="${barW}" height="${barH}" fill="#3d3d3d" rx="2"/>
      <rect x="${x}" y="${padT+chartH-insH}" width="${barW}" height="${insH}" fill="#7a5218" rx="2" opacity="0.9"/>
      <rect x="${x}" y="${padT+chartH-repH-insH}" width="${barW}" height="${repH}" fill="#8a3520" rx="2" opacity="0.9"/>
      ${showNum ? `<text x="${x+barW/2}" y="${numY}" text-anchor="middle" font-size="8.5" font-weight="700" fill="#8a8070">${fmtNum(d.tickets)}</text>` : ""}
    </g>`;
  }).join("");

  // Bot resolved % trend line
  const linePoints = data.map((d, i) => `${padL + i * step + step/2},${toLineY(d.bot_rate)}`).join(" ");
  const dotsHtml = data.map((d, i) => {
    const cx = padL + i * step + step / 2, cy = toLineY(d.bot_rate);
    const showPct = barW >= 22;
    return `<circle cx="${cx}" cy="${cy}" r="3.5" fill="#2a6b50" stroke="#faf8f4" stroke-width="1.5"/>
            ${showPct ? `<text x="${cx}" y="${cy - 7}" text-anchor="middle" font-size="8.5" font-weight="700" fill="#2a6b50">${(d.bot_rate * 100).toFixed(0)}%</text>` : ""}`;
  }).join("");

  // X labels
  const nth = Math.max(1, Math.ceil(data.length / 13));
  const xLabels = data.map((d, i) => {
    if (i % nth !== 0) return "";
    return `<text x="${padL + i * step + step/2}" y="${H - 4}" text-anchor="middle" font-size="9" fill="#6b6659">${esc(d.label)}</text>`;
  }).join("");

  el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:${H}px">
    ${gridLines}${barsHtml}
    <line x1="${padL}" y1="${avgY}" x2="${W - padR}" y2="${avgY}" stroke="#b0a898" stroke-dasharray="4 4" stroke-width="1"/>
    <text x="${W - padR}" y="${avgY - 6}" text-anchor="end" font-size="9" font-weight="700" fill="#8a8070">Avg ${fmtNum(Math.round(avgTickets))}</text>
    <polyline points="${linePoints}" fill="none" stroke="#2a6b50" stroke-width="2" stroke-linejoin="round"/>
    ${dotsHtml}${xLabels}
  </svg>`;

  bindTip(el, ".vol-bar", (g) => ttHtml(g.dataset.label, [
    { label: "Total tickets",    val: fmtNum(g.dataset.tickets) },
    { label: "Repair visits",    val: fmtNum(g.dataset.repair),  color: "#b5300a" },
    { label: "Installation",     val: fmtNum(g.dataset.install), color: "#996010" },
    { label: "Bot resolved",     val: fmtNum(g.dataset.botn),    color: "#1f6645" },
    { label: "Bot resolved %",   val: g.dataset.botr + "%",      color: "#1f6645" },
  ]));
  bindClick(el, ".vol-bar", (g) => openWeekDrawer(g.dataset.key, g.dataset.label, g.dataset));
}

// ── Field visit trend chart (repair vs installation bars by period) ────────────
function renderFieldTrendChart(points, datePreset) {
  const el = document.getElementById("fieldTrendChart");
  if (!el) return;
  const data = bucketTimeline(points, datePreset);
  if (!data.length) { el.innerHTML = empty("No field visit data."); return; }

  const maxField = Math.max(...data.map((d) => d.repair_field + d.install_field), 1);
  const W = 500, H = 200, padL = 44, padB = 34, padT = 20, padR = 14;
  const chartW = W - padL - padR, chartH = H - padB - padT;
  const step = chartW / data.length;
  const barW = Math.max(8, Math.floor(step * 0.7));
  const halfW = Math.floor(barW / 2) - 1;
  const toY = (v) => padT + chartH - (v / maxField) * chartH;
  const avgField = data.reduce((sum, row) => sum + Number(row.repair_field || 0) + Number(row.install_field || 0), 0) / data.length;
  const avgY = toY(avgField);

  const gridLines = [0, 0.5, 1.0].map((f) => {
    const y = toY(f * maxField);
    return `<line x1="${padL}" y1="${y}" x2="${W - padR}" y2="${y}" stroke="#ddd8cf" stroke-width="1"/>
            <text x="${padL - 5}" y="${y + 4}" text-anchor="end" font-size="9" fill="#6b6659">${fmtNum(Math.round(f * maxField))}</text>`;
  }).join("");

  const barsHtml = data.map((d, i) => {
    const cx = padL + i * step + step / 2;
    const repY = toY(d.repair_field), repH = padT + chartH - repY;
    const insY = toY(d.install_field), insH = padT + chartH - insY;
    return `<g class="field-bar chart-interactive" style="cursor:pointer" tabindex="0" role="button" data-key="${esc(d.key)}" data-label="${esc(d.label)}" data-repair="${d.repair_field}" data-install="${d.install_field}">
      <rect x="${cx - halfW - 2}" y="${repY}" width="${halfW}" height="${repH}" fill="#8a3520" rx="2" opacity="0.85"/>
      <rect x="${cx + 2}" y="${insY}" width="${halfW}" height="${insH}" fill="#7a5218" rx="2" opacity="0.85"/>
      <rect x="${cx - halfW - 2}" y="${padT}" width="${barW + 4}" height="${chartH}" fill="transparent"/>
    </g>`;
  }).join("");

  const nth = Math.max(1, Math.ceil(data.length / 12));
  const xLabels = data.map((d, i) => {
    if (i % nth !== 0) return "";
    return `<text x="${padL + i * step + step/2}" y="${H - 4}" text-anchor="middle" font-size="9" fill="#6b6659">${esc(d.label)}</text>`;
  }).join("");

  el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:${H}px">
    ${gridLines}
    <line x1="${padL}" y1="${avgY}" x2="${W - padR}" y2="${avgY}" stroke="#b0a898" stroke-dasharray="4 4" stroke-width="1"/>
    <text x="${W - padR}" y="${avgY - 6}" text-anchor="end" font-size="9" font-weight="700" fill="#8a8070">Avg ${fmtNum(Math.round(avgField))}</text>
    ${barsHtml}${xLabels}
  </svg>`;

  bindTip(el, ".field-bar", (g) => ttHtml(g.dataset.label, [
    { label: "Repair visits",       val: fmtNum(g.dataset.repair),   color: "#b5300a" },
    { label: "Installation visits", val: fmtNum(g.dataset.install),  color: "#996010" },
  ]));
  bindClick(el, ".field-bar", (g) => openWeekDrawer(g.dataset.key, g.dataset.label, {}));
}

// ── Month-wise channel mix (stacked area lines, like CXO deck) ────────────────
function renderChannelTrendChart(points, datePreset, channelMix) {
  const el = document.getElementById("channelTrendChart");
  if (!el) return;
  // We only have total tickets per period, not channel breakdown per period.
  // Instead, show the channel mix as a simple visual reference with proportional bars.
  if (!channelMix.length) { el.innerHTML = empty("No channel data."); return; }

  const CHAN_COLORS = { "Chat": "#2a5580", "Phone": "#5a3875", "Email": "#2a6b50", "WhatsApp": "#7a5218", "Web": "#226060", "Others": "#5c5248" };
  const total = channelMix.reduce((s, r) => s + Number(r.count || 0), 0) || 1;
  const W = 300, H = 200, padL = 72, padR = 12, padT = 12, padB = 12;
  const rowH = Math.floor((H - padT - padB) / Math.min(channelMix.length, 6));

  const barsHtml = channelMix.slice(0, 6).map((item, i) => {
    const y = padT + i * rowH;
    const share = Number(item.count || 0) / total;
    const bW = Math.max(4, share * (W - padL - padR - 60));
    const col = CHAN_COLORS[item.label] || "#5c5248";
    return `<g class="chan-bar chart-interactive" style="cursor:pointer" tabindex="0" role="button" data-label="${esc(item.label)}" data-count="${item.count}" data-share="${share.toFixed(4)}">
      <rect x="0" y="${y}" width="${W}" height="${rowH}" fill="transparent"/>
      <text x="${padL - 6}" y="${y + rowH/2 + 4}" text-anchor="end" font-size="11" font-weight="600" fill="#cbd5e1">${esc(item.label)}</text>
      <rect x="${padL}" y="${y + 4}" width="${W - padL - padR - 18}" height="${rowH - 8}" fill="#111827" rx="3"/>
      <rect x="${padL}" y="${y + 4}" width="${bW}" height="${rowH - 8}" fill="${col}" rx="3" opacity="0.85"/>
      <text x="${padL + bW + 6}" y="${y + rowH/2 + 4}" font-size="11" font-weight="700" fill="${col}">${fmtPct(share)}</text>
      <text x="${padL + bW + 48}" y="${y + rowH/2 + 4}" font-size="10" fill="#6b6659">${fmtNum(item.count)}</text>
    </g>`;
  }).join("");

  el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:${H}px">${barsHtml}</svg>`;

  bindTip(el, ".chan-bar", (g) => ttHtml(g.dataset.label, [
    { label: "Tickets", val: fmtNum(g.dataset.count) },
    { label: "Share",   val: fmtPct(Number(g.dataset.share)) },
  ]));
  bindClick(el, ".chan-bar", (g) => openChannelDrawer(g.dataset.label, g.dataset.count, g.dataset.share));
}

// ── Channel donut chart ───────────────────────────────────────────────────────
function renderChannelChart(items) {
  const el = els.channelChart;
  const leg = els.channelLegend;
  if (!items.length) { el.innerHTML = empty("No channel data."); return; }

  const colors = ["#2a5580", "#5a3875", "#2a6b50", "#7a5218", "#226060", "#5c5248"];
  const total = items.reduce((s, r) => s + Number(r.count || 0), 0) || 1;
  const cx = 90, cy = 90, r = 72, inner = 40;
  let angle = -Math.PI / 2;

  const slices = items.slice(0, 6).map((item, i) => {
    const share = Number(item.count || 0) / total;
    const sweep = share * 2 * Math.PI;
    const startAngle = angle;
    angle += sweep;
    const x1 = cx + r * Math.cos(startAngle);
    const y1 = cy + r * Math.sin(startAngle);
    const x2 = cx + r * Math.cos(angle);
    const y2 = cy + r * Math.sin(angle);
    const x3 = cx + inner * Math.cos(angle);
    const y3 = cy + inner * Math.sin(angle);
    const x4 = cx + inner * Math.cos(startAngle);
    const y4 = cy + inner * Math.sin(startAngle);
    const large = sweep > Math.PI ? 1 : 0;
    const midAngle = startAngle + sweep / 2;
    const labelR = r + 16;
    const lx = cx + labelR * Math.cos(midAngle);
    const ly = cy + labelR * Math.sin(midAngle);
    return {
      path: `M ${x1} ${y1} A ${r} ${r} 0 ${large} 1 ${x2} ${y2} L ${x3} ${y3} A ${inner} ${inner} 0 ${large} 0 ${x4} ${y4} Z`,
      color: colors[i % colors.length], label: item.label, count: item.count, share,
      lx, ly, sweep,
    };
  });

  const labelHtml = slices
    .filter((s) => s.sweep > 0.25)
    .map((s) => `<text x="${s.lx}" y="${s.ly}" text-anchor="middle" dominant-baseline="middle" font-size="10" font-weight="700" fill="${s.color}">${(s.share * 100).toFixed(0)}%</text>`)
    .join("");

  el.innerHTML = `<svg viewBox="0 0 180 180" style="width:180px;height:180px">
    ${slices.map((s) => `<path class="donut-slice chart-interactive" style="cursor:pointer" tabindex="0" role="button" d="${s.path}" fill="${s.color}" opacity="0.9" data-label="${esc(s.label)}" data-count="${s.count}" data-share="${s.share.toFixed(4)}"/>`).join("")}
    ${labelHtml}
    <text x="${cx}" y="${cy - 8}" text-anchor="middle" font-size="10" fill="#6b6659">Total</text>
    <text x="${cx}" y="${cy + 10}" text-anchor="middle" font-size="16" font-weight="800" fill="#1c1c1c">${fmtNum(total)}</text>
  </svg>`;

  bindTip(el, ".donut-slice", (path) => ttHtml(path.dataset.label, [
    { label: "Tickets", val: fmtNum(path.dataset.count) },
    { label: "Share",   val: fmtPct(Number(path.dataset.share)) },
  ]));
  bindClick(el, ".donut-slice", (path) => openChannelDrawer(path.dataset.label, path.dataset.count, path.dataset.share));

  leg.innerHTML = slices.map((s) => `
    <div class="donut-row" style="cursor:pointer" tabindex="0" role="button" data-channel-filter="${esc(s.label)}">
      <div class="donut-label"><span class="legend-dot" style="background:${s.color}"></span>${esc(s.label)}</div>
      <div class="donut-value" style="color:${s.color}">${fmtPct(s.share)}<span class="donut-count">${fmtNum(s.count)}</span></div>
    </div>`).join("");
  leg.querySelectorAll("[data-channel-filter]").forEach((row) => {
    wireAction(row, () => filterByChannel(row.dataset.channelFilter));
  });
}

function filterByChannel(label) {
  const idx = state.filters.channels.indexOf(label);
  if (idx >= 0) state.filters.channels.splice(idx, 1);
  else state.filters.channels.push(label);
  loadDashboard();
}

// ── Bar list (field, bot outcomes, resolution) ────────────────────────────────
function buildFieldRows(kpis, split) {
  const repair = split.find((r) => r.label?.toLowerCase().includes("repair")) || {};
  const install = split.find((r) => r.label?.toLowerCase().includes("install")) || {};
  return [
    {
      label: "Repair visits",
      count: repair.count || 0,
      share: kpis.repair_field_visit_rate?.value || 0,
      detail: "Field repairs driven by service or product defects",
    },
    {
      label: "Installation visits",
      count: install.count || 0,
      share: kpis.installation_field_visit_rate?.value || 0,
      detail: "Onboarding and installation-related dispatches",
    },
  ];
}

function renderBarList(container, rows, options = {}) {
  if (!container) return;
  if (!rows.length) { container.innerHTML = empty("No data."); return; }
  container.innerHTML = buildBreakdownRows(rows, options);
}

// ── Product matrix ────────────────────────────────────────────────────────────
function renderProductMatrix(products) {
  const el = els.productMatrix;
  if (!products.length) { el.innerHTML = empty("No product data."); return; }
  const maxVol = Math.max(...products.map((p) => p.ticket_volume), 1);

  el.innerHTML = `
    <div class="pt-header">
      <div>Product</div>
      <div style="text-align:right">Tickets</div>
      <div style="text-align:right">Repair %</div>
      <div style="text-align:right">Repeat %</div>
      <div style="text-align:right">Bot res.</div>
      <div>Top issue</div>
    </div>
    ${products.map((p) => {
      const repairCls = p.repair_field_visit_rate >= 0.12 ? "red" : "muted";
      const repeatCls = p.repeat_rate >= 0.15 ? "amber" : "muted";
      const botCls = p.bot_deflection_rate >= 0.2 ? "green" : "muted";
      return `
        <div class="pt-row" tabindex="0" role="button" data-product="${esc(p.product_family)}">
          <div class="pt-name" title="${esc(p.product_family)}">${esc(p.product_family)}</div>
          <div class="pt-bar-wrap" style="text-align:right">
            <div class="pt-val">${fmtNum(p.ticket_volume)}</div>
            <div class="bar-track" style="margin-top:3px"><div class="bar-fill" style="width:${(p.ticket_volume / maxVol) * 100}%"></div></div>
          </div>
          <div class="pt-val ${repairCls}">${fmtPct(p.repair_field_visit_rate)}</div>
          <div class="pt-val ${repeatCls}">${fmtPct(p.repeat_rate)}</div>
          <div class="pt-val ${botCls}">${fmtPct(p.bot_deflection_rate)}</div>
          <div class="pt-val muted" style="font-size:11px;text-align:left;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(cleanCopy(p.top_issue))}">${esc(cleanCopy(p.top_issue))}</div>
        </div>`;
    }).join("")}`;

  // clicking a product row opens the product drill-down drawer
  el.querySelectorAll(".pt-row").forEach((row) => {
    row.style.cursor = "pointer";
    wireAction(row, () => openProductDrawer(row.dataset.product));
  });
}

// ── Field visit by product chart ─────────────────────────────────────────────
const NON_PRODUCT_FAMILIES = new Set(["Blank Chats", "Others", "Miscellaneous", "Logistics / Non-product"]);

function renderFieldVisitByProduct(products) {
  const el = document.getElementById("fieldVisitByProduct");
  if (!el) return;
  const rows = products
    .filter((p) => !NON_PRODUCT_FAMILIES.has(p.product_family))
    .filter((p) => (p.repair_field_visit_rate + (p.installation_field_visit_rate || 0)) > 0)
    .sort((a, b) => (b.repair_field_visit_rate + (b.installation_field_visit_rate || 0)) - (a.repair_field_visit_rate + (a.installation_field_visit_rate || 0)));
  if (!rows.length) { el.innerHTML = empty("No field visit data."); return; }
  const maxTotal = Math.max(...rows.map((r) => r.repair_field_visit_rate + (r.installation_field_visit_rate || 0)), 0.01);
  el.innerHTML = `
    <div class="fv-table">
      <div class="fv-header">
        <div>Product</div>
        <div>Top models</div>
        <div style="text-align:right">Tickets</div>
        <div style="text-align:right">Repair %</div>
        <div style="text-align:right">Installation %</div>
        <div>Total field burden</div>
      </div>
      ${rows.map((r) => {
        const totalRate = Number(r.repair_field_visit_rate || 0) + Number(r.installation_field_visit_rate || 0);
        const topModels = ((state.payload?.model_breakdown || {})[r.product_family] || [])
          .filter((m) => (m.model || "") !== r.product_family)
          .slice(0, 2)
          .map((m) => m.model);
        return `
          <div class="fv-row" tabindex="0" role="button" data-product="${esc(r.product_family)}" data-repair="${Number(r.repair_field_visit_rate || 0).toFixed(4)}" data-install="${Number(r.installation_field_visit_rate || 0).toFixed(4)}" data-total="${totalRate.toFixed(4)}" data-vol="${r.ticket_volume || 0}">
            <div class="fv-product">
              <div class="fv-product-name">${esc(r.product_family)}</div>
              <div class="fv-product-note">${cleanCopy(r.top_issue || "No dominant issue")}</div>
            </div>
            <div class="fv-models">
              ${topModels.length
                ? topModels.map((model) => `<span class="fv-model-chip">${esc(model)}</span>`).join("")
                : `<span class="fv-model-fallback">Model mix loads on click</span>`}
            </div>
            <div class="fv-num">${fmtNum(r.ticket_volume)}</div>
            <div class="fv-num fv-repair">${fmtPct(r.repair_field_visit_rate || 0)}</div>
            <div class="fv-num fv-install">${fmtPct(r.installation_field_visit_rate || 0)}</div>
            <div class="fv-burden">
              <div class="fv-burden-bar">
                <div class="fv-burden-total" style="width:${(totalRate / maxTotal) * 100}%">
                  <div class="fv-burden-repair" style="width:${totalRate > 0 ? (Number(r.repair_field_visit_rate || 0) / totalRate) * 100 : 0}%"></div>
                  <div class="fv-burden-install" style="width:${totalRate > 0 ? (Number(r.installation_field_visit_rate || 0) / totalRate) * 100 : 0}%"></div>
                </div>
              </div>
              <div class="fv-burden-label">${fmtPct(totalRate)}</div>
            </div>
          </div>`;
      }).join("")}
    </div>`;

  el.querySelectorAll(".fv-row").forEach((row) => {
    wireAction(row, () => openProductDrawer(row.dataset.product));
  });
}

// ── Model breakdown ───────────────────────────────────────────────────────────
function renderModelBreakdown(breakdown) {
  const el = document.getElementById("modelBreakdown");
  const card = document.getElementById("modelBreakdownCard");
  if (!el) return;
  const families = Object.keys(breakdown || {});
  if (!families.length) { card.style.display = "none"; return; }
  card.style.display = "";

  el.innerHTML = families.map((family) => {
    const models = breakdown[family] || [];
    if (!models.length) return "";
    const maxVol = Math.max(...models.map((m) => m.tickets), 1);
    const resolvedModels = models.map((m) => ({
      ...m,
      display_model: m.model === family ? "Unmapped / family-level" : m.model,
    }));
    return `
      <div class="model-family-block">
        <div class="model-family-title">${esc(family)}</div>
        <div class="model-family-note">Models within ${esc(family)}. If a row says "Unmapped / family-level", the source ticket did not carry a specific model cleanly.</div>
        <div class="pt-header model-header">
          <div>Model</div>
          <div style="text-align:right">Tickets</div>
          <div style="text-align:right">Repair %</div>
          <div style="text-align:right">Repeat %</div>
          <div style="text-align:right">Bot res.</div>
          <div style="text-align:right">Transfer %</div>
        </div>
        ${resolvedModels.map((m) => {
          const repairCls = m.repair_field_visit_rate >= 0.12 ? "red" : "muted";
          const repeatCls = m.repeat_rate >= 0.15 ? "amber" : "muted";
          const botCls = m.bot_deflection_rate >= 0.2 ? "green" : "muted";
          const xferCls = m.bot_transfer_rate >= 0.2 ? "amber" : "muted";
          return `
            <div class="pt-row model-row">
              <div class="pt-name" title="${esc(m.display_model)}">${esc(m.display_model)}</div>
              <div class="pt-bar-wrap" style="text-align:right">
                <div class="pt-val">${fmtNum(m.tickets)}</div>
                <div class="bar-track" style="margin-top:3px"><div class="bar-fill" style="width:${(m.tickets / maxVol) * 100}%"></div></div>
              </div>
              <div class="pt-val ${repairCls}">${fmtPct(m.repair_field_visit_rate)}</div>
              <div class="pt-val ${repeatCls}">${fmtPct(m.repeat_rate)}</div>
              <div class="pt-val ${botCls}">${fmtPct(m.bot_deflection_rate)}</div>
              <div class="pt-val ${xferCls}">${fmtPct(m.bot_transfer_rate)}</div>
            </div>`;
        }).join("")}
      </div>`;
  }).join("");
}

// ── Issue board ───────────────────────────────────────────────────────────────
function renderIssueBoard(views) {
  const issues = views[state.issueView] || [];
  const el = els.issueBoard;
  if (!issues.length) { el.innerHTML = empty("No issues for this view."); return; }

  el.innerHTML = issues.map((issue) => {
    const resolutions = issue.top_resolutions || [];
    const maxRes = resolutions.length ? Math.max(...resolutions.map((r) => r.count)) : 1;
    const RES_COLORS = ["#3d3d3d", "#2a5580", "#7a5218", "#2a6b50"];
    const resBarsHtml = resolutions.map((r, i) => {
      const pct = Math.round((r.count / maxRes) * 100);
      return `<div class="irb-row">
        <div class="irb-label">${esc(r.label)} <span class="irb-count">(${fmtNum(r.count)})</span></div>
        <div class="irb-track"><div class="irb-bar" style="width:${pct}%;background:${RES_COLORS[i] || RES_COLORS[0]}"></div></div>
      </div>`;
    }).join("");
    const repairPct = issue.repair_field_visit_rate > 0 ? `<span class="issue-stat-pct" style="color:#8a3520">${fmtPct(issue.repair_field_visit_rate)} repair</span>` : "";
    const repeatPct = issue.repeat_rate > 0.05 ? `<span class="issue-stat-pct" style="color:#7a5218">${fmtPct(issue.repeat_rate)} repeat</span>` : "";
    return `
    <div class="issue-card" tabindex="0" role="button" data-issue-id="${esc(issue.issue_id)}">
      <div class="issue-head-row">
        <div class="issue-name-col">
          <div class="issue-fc2" title="${esc(issue.fault_code_level_2)}">${esc(issue.fault_code_level_2)}</div>
          <div class="issue-product">${esc(issue.product_family)} · ${esc(issue.fault_code)}</div>
        </div>
        <div class="issue-stat-col">
          <span class="issue-stat-vol">${fmtNum(issue.volume)}</span>
          ${repairPct}${repeatPct}
        </div>
      </div>
      ${resBarsHtml ? `<div class="issue-resolution-bars">${resBarsHtml}</div>` : ""}
      <div class="issue-foot">
        <span>Prior period: ${fmtNum(issue.previous_volume)}</span>
        <span>Details &rarr;</span>
      </div>
    </div>`;
  }).join("");

  el.querySelectorAll("[data-issue-id]").forEach((card) => {
    wireAction(card, () => openIssue(card.dataset.issueId));
  });
}

// ── Bot KPIs ──────────────────────────────────────────────────────────────────
function renderBotKpis(ov) {
  const cards = [
    { label: "Chatbot resolved", value: fmtPct(ov.bot_resolved_rate || 0), count: fmtNum(ov.bot_resolved_tickets || 0), accent: "green",
      tip: "Chatbot resolved rate = sessions fully handled by the chatbot / total chat sessions. No human agent involved." },
    { label: "Transferred to agent", value: fmtPct(ov.bot_transferred_rate || 0), count: fmtNum(ov.bot_transferred_tickets || 0), accent: "amber",
      tip: "Transfer rate = sessions handed off to a live agent / total chat sessions. High value indicates chatbot is not containing demand." },
    { label: "Blank chat", value: fmtPct(ov.blank_chat_rate || 0), count: fmtNum(ov.blank_chat_tickets || 0), accent: "red",
      tip: "Blank chat rate = sessions where the user started but sent no message (dropped off) / total chat sessions." },
    { label: "Return after blank", value: fmtPct(ov.blank_chat_return_rate || 0), count: `${fmtNum(ov.blank_chat_returned_7d || 0)} returns`, accent: "blue",
      tip: "Return rate = blank-chat users who come back via any channel within 7 days / total blank-chat sessions. Indicates deferred demand." },
    { label: "Recovered in bot", value: fmtPct(ov.blank_chat_recovery_rate || 0), count: `${fmtNum(ov.blank_chat_resolved_7d || 0)} resolved`, accent: "green",
      tip: "Recovery rate = blank-chat users who eventually get resolved by the chatbot within 7 days / total blank-chat sessions." },
  ];
  els.botKpiRow.innerHTML = cards.map(({ label, value, count, accent, tip }) => `
    <div class="kpi-card accent-${accent}">
      <div class="kpi-label">${esc(label)} <button class="info-btn" data-tip="${esc(tip)}">?</button></div>
      <div class="kpi-value">${value}</div>
      <div class="kpi-delta neutral">${count}</div>
    </div>`).join("");
}

// ── Bot product matrix ────────────────────────────────────────────────────────
function renderBotProductMatrix(rows) {
  const el = els.botProductMatrix;
  if (!rows.length) { el.innerHTML = empty("No chatbot product data."); return; }
  el.innerHTML = `
    <div class="bt-header">
      <div>Product</div>
      <div style="text-align:right">Chat tickets</div>
      <div style="text-align:right">Resolved</div>
      <div style="text-align:right">Transferred</div>
      <div style="text-align:right">Blank chat</div>
      <div style="text-align:right">Blank return</div>
    </div>
    ${rows.map((r) => `
      <div class="bt-row">
        <div class="bt-name">${esc(r.product_family)}</div>
        <div class="bt-val">${fmtNum(r.chat_tickets)}</div>
        <div class="bt-val ${r.bot_resolved_rate >= 0.2 ? "green" : ""}">${fmtPct(r.bot_resolved_rate)}</div>
        <div class="bt-val ${r.bot_transferred_rate >= 0.45 ? "red" : ""}">${fmtPct(r.bot_transferred_rate)}</div>
        <div class="bt-val ${r.blank_chat_rate >= 0.2 ? "amber" : ""}">${fmtPct(r.blank_chat_rate)}</div>
        <div class="bt-val">${fmtPct(r.blank_chat_return_rate)}</div>
      </div>`).join("")}`;
}

// ── Mini issue list ───────────────────────────────────────────────────────────
function renderMiniIssueList(el, issues, emptyMsg) {
  if (!issues.length) { el.innerHTML = empty(emptyMsg); return; }
  el.innerHTML = issues.slice(0, 5).map((issue) => `
    <div class="imi-card" tabindex="0" role="button" data-issue-id="${esc(issue.issue_id)}">
      <div class="imi-name">${esc(issue.product_family)} - ${esc(issue.fault_code_level_2)}</div>
      <div class="imi-sub">Bot resolved ${fmtPct(issue.bot_deflection_rate)} | Transfer ${fmtPct(issue.bot_transfer_rate || 0)}</div>
    </div>`).join("");
  el.querySelectorAll("[data-issue-id]").forEach((c) => wireAction(c, () => openIssue(c.dataset.issueId)));
}

// ── Quality ───────────────────────────────────────────────────────────────────
function renderQuality(clean) {
  const total = clean.total_tickets || 1;
  const cards = [
    { label: "Total tickets", value: fmtNum(clean.total_tickets || 0), note: "All tickets in source" },
    { label: "Issue-classifiable", value: fmtPct((clean.actionable_issue_tickets || 0) / total), note: `${fmtNum(clean.actionable_issue_tickets || 0)} tickets with full issue coding` },
    { label: "Missing fault code", value: fmtNum(clean.blank_fault_code_tickets || 0), note: "Tickets with no Fault Code 1" },
    { label: "Unknown product", value: fmtNum(clean.unknown_product_tickets || 0), note: "Could not map to a product family" },
    { label: "Blank chat sessions", value: fmtNum(clean.dropped_in_bot_tickets || 0), note: "Dropped off in chatbot" },
    { label: "Missing issue (non-chat)", value: fmtNum(clean.missing_issue_outside_bot_tickets || 0), note: "Non-chat tickets without issue code" },
  ];
  els.qualityCards.innerHTML = cards.map((c) => `
    <div class="qc">
      <div class="qc-label">${esc(c.label)}</div>
      <div class="qc-value">${c.value}</div>
      <div class="qc-note">${esc(c.note)}</div>
    </div>`).join("");
}

// ── Pipeline ──────────────────────────────────────────────────────────────────
function renderPipeline(pl) {
  const rows = [
    { name: "Status", status: pl.status || "Unknown" },
    { name: "Last run", status: pl.last_run_at || "Never" },
    { name: "Duration", status: `${pl.duration_minutes || 0} min` },
    ...(pl.tables || []).map((t) => ({ name: t.table, status: t.status })),
  ];
  els.pipelineHealth.innerHTML = rows.map((r) => `
    <div class="pl-row">
      <div class="pl-name">${esc(r.name)}</div>
      <div class="pl-status ${r.status === "Fresh" || r.status === "Success" ? "ok" : r.status === "Sample" ? "sample" : ""}">${esc(r.status)}</div>
    </div>`).join("");
}

// ── Pipeline actions ──────────────────────────────────────────────────────────
async function refreshPipelineStatus(opts = {}) {
  try {
    const res = await fetch(`${state.apiBase}/api/pipeline/status`);
    if (!res.ok) return;
    const pl = await res.json();
    if (pl.running) {
      els.viewStatus.textContent = "Pipeline";
      els.runPipelineBtn.disabled = true;
      els.runPipelineBtn.textContent = "Running...";
    } else {
      if (!state.loading) els.viewStatus.textContent = "Ready";
      els.runPipelineBtn.disabled = false;
      els.runPipelineBtn.textContent = "Run pipeline";
      if (pl.last_status === "Failed") {
        els.lastUpdated.textContent = `Pipeline failed: ${pl.last_message || ""}`;
      }
    }
  } catch (_) {}
}

async function triggerPipeline() {
  if (!window.confirm("Start the pipeline run now? This is an admin action and can refresh executive data.")) {
    return;
  }
  els.runPipelineBtn.disabled = true;
  els.runPipelineBtn.textContent = "Starting...";
  try {
    const res = await fetch(`${state.apiBase}/api/pipeline/run`, { method: "POST" });
    if (!res.ok) throw new Error(`${res.status}`);
    await res.json();
    setTimeout(() => refreshPipelineStatus(), 1500);
  } catch (err) {
    els.runPipelineBtn.disabled = false;
    els.runPipelineBtn.textContent = "Run pipeline";
    els.lastUpdated.textContent = `Trigger failed: ${err.message}`;
  }
}

// ── Issue drawer ──────────────────────────────────────────────────────────────
async function openIssue(issueId) {
  els.drawer.classList.remove("hidden");
  els.drawerTitle.textContent = "Issue analysis";
  els.drawerContent.innerHTML = drawerLoadingHtml("Loading issue details...");
  const params = new URLSearchParams({ date_preset: state.filters.date_preset });
  if (state.filters.products.length)    params.set("products",    state.filters.products.join(","));
  if (state.filters.models.length)      params.set("models",      state.filters.models.join(","));
  if (state.filters.fault_codes.length) params.set("fault_codes", state.filters.fault_codes.join(","));
  if (state.filters.channels.length)    params.set("channels",    state.filters.channels.join(","));
  if (state.filters.bot_actions.length) params.set("bot_actions", state.filters.bot_actions.join(","));
  if (state.filters.quick_exclusions.length) params.set("quick_exclusions", state.filters.quick_exclusions.join(","));
  try {
    const payload = await fetchJson(`${state.apiBase}/api/issues/${encodeURIComponent(issueId)}?${params}`, { timeoutMs: 25000 });
    renderDrawer(payload);
  } catch (err) {
    els.drawerContent.innerHTML = `<div class="error-state">Failed to load: ${esc(err.message)}</div>`;
  }
}

function renderDrawer(payload) {
  const issue = payload.issue;
  const tickets = payload.tickets || [];

  if (!issue) {
    els.drawerContent.innerHTML = `<div class="empty-state">No issue data found.</div>`;
    return;
  }

  els.drawerTitle.textContent = `${issue.product_family} - ${issue.fault_code_level_2}`;

  // Build word cloud from sub-issues (using top_symptom, top_defect, top_repair + ticket evidence)
  const wordFreq = {};
  const addWord = (text, weight) => {
    if (text && text !== "Unknown") {
      const key = text.trim();
      wordFreq[key] = (wordFreq[key] || 0) + weight;
    }
  };
  addWord(issue.top_symptom, 8);
  addWord(issue.top_defect, 6);
  addWord(issue.top_repair, 4);
  for (const t of tickets) {
    addWord(t.symptom, 2);
    addWord(t.defect, 1.5);
    addWord(t.repair, 1);
  }

  // repair rate determines color in cloud
  const repairRate = issue.repair_field_visit_rate || 0;
  const cloudColor = repairRate >= 0.15 ? "#8a3520" : repairRate >= 0.08 ? "#7a5218" : "#2a5580";

  const cloudWords = Object.entries(wordFreq)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 20);
  const maxFreq = cloudWords[0]?.[1] || 1;
  const cloudHtml = cloudWords.map(([word, freq]) => {
    const size = 12 + Math.round((freq / maxFreq) * 20);
    const opacity = 0.55 + (freq / maxFreq) * 0.45;
    return `<span class="cloud-word" style="font-size:${size}px;color:${cloudColor};opacity:${opacity}" title="${esc(word)}">${esc(word)}</span>`;
  }).join("");

  els.drawerContent.innerHTML = `
    <div class="drawer-issue-summary">
      <div class="dis-fc1">${esc(issue.fault_code)}</div>
      <div class="dis-fc2">${esc(issue.fault_code_level_2)}</div>
      <div class="dis-product">${esc(issue.product_family)}</div>
      <div class="dis-chips">
        <span class="chip chip-blue">${fmtNum(issue.volume)} tickets</span>
        ${issue.repair_field_visit_rate > 0.02 ? `<span class="chip chip-red">Repair ${fmtPct(issue.repair_field_visit_rate)}</span>` : ""}
        ${issue.repeat_rate > 0.02 ? `<span class="chip chip-amber">Repeat ${fmtPct(issue.repeat_rate)}</span>` : ""}
        ${issue.bot_deflection_rate > 0.05 ? `<span class="chip chip-green">Chatbot ${fmtPct(issue.bot_deflection_rate)}</span>` : ""}
        ${issue.bot_transfer_rate > 0.05 ? `<span class="chip chip-amber">Transfer ${fmtPct(issue.bot_transfer_rate)}</span>` : ""}
      </div>
      <div class="dis-insight">${esc(cleanCopy(issue.insight))}</div>
    </div>

    <div class="word-cloud-section">
      <div class="word-cloud-title">Issue signals - what customers report</div>
      <div class="word-cloud">${cloudHtml || '<span style="color:#8b9ab5;font-size:13px">No signal text available.</span>'}</div>
      <div style="margin-top:10px;font-size:11px;color:#8b9ab5">Word size = frequency. Color intensity = severity. Based on symptom, defect, and repair fields.</div>
    </div>

    <div class="ticket-section">
      <div class="ts-title">Recent tickets (${tickets.length})</div>
      ${tickets.length ? `
        <table class="ticket-table">
          <thead>
            <tr>
              <th>Ticket ID</th>
              <th>Date</th>
              <th>Product</th>
              <th>Channel</th>
              <th>Resolution</th>
              <th>Bot action</th>
              <th>Symptom / Defect</th>
            </tr>
          </thead>
          <tbody>
            ${tickets.map((t) => `
              <tr>
                <td>${esc(t.ticket_id)}</td>
                <td>${esc((t.created_at || "").slice(0, 10))}</td>
                <td>${esc(t.product || t.product_family || "")}</td>
                <td>${esc(t.channel || "")}</td>
                <td>${esc(t.resolution || "")}</td>
                <td>${esc(t.bot_action || "")}</td>
                <td>${esc(t.symptom || "")}${t.defect ? ` / ${esc(t.defect)}` : ""}</td>
              </tr>`).join("")}
          </tbody>
        </table>` : `<div class="empty-state">No ticket evidence available.</div>`}
    </div>`;
}

function closeDrawer() { els.drawer.classList.add("hidden"); }

// ── Filters ───────────────────────────────────────────────────────────────────
function hydrateFilters(opts) {
  buildMultiSelect(els.productFilter,    opts.products    || [], state.filters.products,    (v) => {
    state.filters.products = v;
    if (!v.length) {
      state.filters.models = [];
    } else {
      const modelsByProduct = opts.models_by_product || {};
      const validModels = new Set(v.flatMap((product) => modelsByProduct[product] || []));
      state.filters.models = state.filters.models.filter((model) => validModels.has(model));
    }
    loadDashboard();
  });
  const modelsByProduct = opts.models_by_product || {};
  const modelOptions = state.filters.products.length
    ? [...new Set(state.filters.products.flatMap((product) => modelsByProduct[product] || []))]
    : [];
  state.filters.models = state.filters.models.filter((model) => modelOptions.includes(model));
  buildMultiSelect(els.modelFilter,      modelOptions,            state.filters.models,      (v) => { state.filters.models      = v; loadDashboard(); });
  buildMultiSelect(els.faultCodeFilter,  opts.fault_codes || [], state.filters.fault_codes,  (v) => { state.filters.fault_codes  = v; loadDashboard(); });
  buildMultiSelect(els.channelFilter,    opts.channels    || [], state.filters.channels,    (v) => { state.filters.channels    = v; loadDashboard(); });
  const botActionOptions = [...new Set([...(opts.bot_actions || []).map(normalizeBotActionOption), "Non bot tickets"])];
  state.filters.bot_actions = state.filters.bot_actions.map(normalizeBotActionOption);
  buildMultiSelect(els.botActionFilter,  botActionOptions, state.filters.bot_actions,  (v) => { state.filters.bot_actions  = v.map(normalizeBotActionOption); loadDashboard(); });
  syncControls();
}

function normalizeBotActionOption(v) {
  const text = cleanCopy(v || "");
  if (text.toLowerCase().includes("cancelled") && text.toLowerCase().includes("existing ticket")) {
    return "Cancelled - existing ticket";
  }
  return text;
}

function syncControls() {
  els.datePreset.value = state.filters.date_preset;
  els.issueTabs.querySelectorAll(".tab").forEach((t) => t.classList.toggle("active", t.dataset.view === state.issueView));
  // Sync multi-select labels to current state (in case state changed outside hydrateFilters)
  _syncMultiSelectLabel(els.productFilter,   state.filters.products);
  _syncMultiSelectLabel(els.modelFilter,     state.filters.models);
  _syncMultiSelectLabel(els.faultCodeFilter, state.filters.fault_codes);
  _syncMultiSelectLabel(els.channelFilter,   state.filters.channels);
  _syncMultiSelectLabel(els.botActionFilter, state.filters.bot_actions);
}

function _syncMultiSelectLabel(el, selected) {
  const trigger = el.querySelector(".ms-trigger");
  if (!trigger) return;
  const label = trigger.querySelector(".ms-label");
  if (!label) return;
  if (!selected.length) { label.textContent = "All"; trigger.classList.remove("ms-active"); }
  else if (selected.length === 1) { label.textContent = selected[0]; trigger.classList.add("ms-active"); }
  else { label.textContent = `${selected[0]} +${selected.length - 1}`; trigger.classList.add("ms-active"); }
  // Sync checkboxes
  el.querySelectorAll(".ms-item:not(.ms-clear) input[type=checkbox]").forEach((cb) => {
    cb.checked = selected.includes(cb.value);
  });
}

function buildMultiSelect(el, opts, selected, onChange) {
  const selSet = new Set(selected);
  let committedSelected = [...selected];
  el.innerHTML = `
    <div class="ms-trigger">
      <span class="ms-label">All</span>
      <span class="ms-caret">▾</span>
    </div>
    <div class="ms-panel hidden">
      <input type="text" class="ms-search" placeholder="Search...">
      <label class="ms-item ms-clear">
        <input type="checkbox" value="__clear__">
        <span>All (clear)</span>
      </label>
      <div class="ms-divider"></div>
      <div class="ms-items">
        ${opts.map((o) => `
          <label class="ms-item" data-val="${esc(o)}">
            <input type="checkbox" value="${esc(o)}"${selSet.has(o) ? " checked" : ""}>
            <span>${esc(o)}</span>
          </label>`).join("")}
      </div>
      <div class="ms-divider"></div>
      <div class="ms-footer">
        <button type="button" class="ms-btn ms-cancel">Cancel</button>
        <button type="button" class="ms-btn ms-apply">Apply</button>
      </div>
    </div>`;

  const trigger = el.querySelector(".ms-trigger");
  const panel   = el.querySelector(".ms-panel");
  const search  = el.querySelector(".ms-search");
  const clearInput = el.querySelector(".ms-clear input");
  const cancelBtn = el.querySelector(".ms-cancel");
  const applyBtn = el.querySelector(".ms-apply");

  function getSelected() {
    return [...el.querySelectorAll(".ms-item:not(.ms-clear) input:checked")].map((i) => i.value);
  }
  function updateLabel() {
    const vals = getSelected();
    const lbl = trigger.querySelector(".ms-label");
    if (!vals.length) { lbl.textContent = "All"; trigger.classList.remove("ms-active"); }
    else if (vals.length === 1) { lbl.textContent = vals[0]; trigger.classList.add("ms-active"); }
    else { lbl.textContent = `${vals[0]} +${vals.length - 1}`; trigger.classList.add("ms-active"); }
  }
  function syncDraft(vals) {
    const selectedSet = new Set(vals);
    el.querySelectorAll(".ms-item:not(.ms-clear) input").forEach((i) => {
      i.checked = selectedSet.has(i.value);
    });
    clearInput.checked = false;
    updateLabel();
  }
  syncDraft(committedSelected);

  trigger.addEventListener("click", (e) => {
    e.stopPropagation();
    document.querySelectorAll(".ms-panel:not(.hidden)").forEach((p) => { if (p !== panel) p.classList.add("hidden"); });
    panel.classList.toggle("hidden");
    if (!panel.classList.contains("hidden")) {
      syncDraft(committedSelected);
      search.value = "";
      filterItems("");
      search.focus();
    }
  });
  panel.addEventListener("click", (e) => e.stopPropagation());

  function filterItems(q) {
    const lq = q.toLowerCase();
    el.querySelectorAll(".ms-item:not(.ms-clear)").forEach((item) => {
      item.style.display = !lq || item.dataset.val?.toLowerCase().includes(lq) ? "" : "none";
    });
  }
  search.addEventListener("input", () => filterItems(search.value));
  search.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      syncDraft(committedSelected);
      panel.classList.add("hidden");
    }
  });

  clearInput.addEventListener("change", () => {
    el.querySelectorAll(".ms-item:not(.ms-clear) input").forEach((i) => { i.checked = false; });
    clearInput.checked = false;
    updateLabel();
  });

  el.querySelectorAll(".ms-item:not(.ms-clear) input").forEach((input) => {
    input.addEventListener("change", () => { updateLabel(); });
  });

  cancelBtn.addEventListener("click", () => {
    syncDraft(committedSelected);
    panel.classList.add("hidden");
  });

  applyBtn.addEventListener("click", () => {
    const nextSelected = getSelected();
    const changed = nextSelected.length !== committedSelected.length
      || nextSelected.some((value, idx) => value !== committedSelected[idx]);
    committedSelected = [...nextSelected];
    panel.classList.add("hidden");
    if (changed) onChange(nextSelected);
  });
}

// ── Error display ─────────────────────────────────────────────────────────────
function showError(msg) {
  els.viewStatus.textContent = "Error";
  els.sourceBadge.textContent = "Load failed";
  els.sourceBadge.style.background = "rgba(181,48,10,0.1)";
  els.sourceBadge.style.color = "#b5300a";
  [els.kpiRow, els.volumeChart, els.channelChart, els.fieldSummary, els.botOutcomesChart,
   els.resolutionList, els.productMatrix, els.issueBoard, els.botKpiRow,
   els.botProductMatrix, els.botBestIssues, els.botLeakyIssues, els.qualityCards, els.pipelineHealth]
    .forEach((el) => { el.innerHTML = `<div class="error-state">${esc(msg)}</div>`; });
}

// ── Chart tooltip engine ──────────────────────────────────────────────────────
function showTip(e, html) {
  els.chartTooltip.innerHTML = html;
  els.chartTooltip.classList.add("visible");
  _moveTip(e);
}

function showTipForElement(el, html) {
  const rect = el.getBoundingClientRect();
  showTip({ clientX: rect.left + rect.width / 2, clientY: rect.top + rect.height / 2 }, html);
}
function _moveTip(e) {
  const tt = els.chartTooltip;
  const ttW = tt.offsetWidth, ttH = tt.offsetHeight;
  let left = e.clientX + 14, top = e.clientY - ttH / 2;
  if (left + ttW > window.innerWidth - 8) left = e.clientX - ttW - 14;
  if (top < 8) top = 8;
  if (top + ttH > window.innerHeight - 8) top = window.innerHeight - ttH - 8;
  tt.style.left = left + "px";
  tt.style.top = top + "px";
}
function hideTip() { els.chartTooltip.classList.remove("visible"); }

// Build tooltip HTML: title + rows of {label, val, color}
function ttHtml(title, rows) {
  const r = rows.map(({ label, val, color }) =>
    `<div class="tt-row"><span class="tt-lbl"${color ? ` style="color:${color}"` : ""}>${esc(label)}</span><span class="tt-val">${esc(String(val))}</span></div>`
  ).join("");
  return `<div class="tt-title">${esc(title)}</div>${r}`;
}

// Bind tooltip events on a rendered container's child elements by selector
function bindTip(container, selector, buildFn) {
  container.querySelectorAll(selector).forEach((el) => {
    el.addEventListener("mouseenter", (e) => showTip(e, buildFn(el)));
    el.addEventListener("mousemove", _moveTip);
    el.addEventListener("mouseleave", hideTip);
    el.addEventListener("focus", () => showTipForElement(el, buildFn(el)));
    el.addEventListener("blur", hideTip);
  });
}

function bindClick(container, selector, fn) {
  container.querySelectorAll(selector).forEach((el) => {
    wireAction(el, () => { hideTip(); fn(el); });
  });
}

function wireAction(el, fn) {
  el.addEventListener("click", fn);
  el.addEventListener("keydown", (e) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      fn();
    }
  });
}

// ── Drill-down drawers (client-side from state.payload) ───────────────────────
function openDrawer(title, html) {
  els.drawerTitle.textContent = title;
  els.drawerContent.innerHTML = html;
  els.drawer.classList.remove("hidden");
  wireDrawerActions();
}

function wireDrawerActions() {
  els.drawerContent.querySelectorAll("[data-action='filter-product']").forEach((btn) => {
    wireAction(btn, () => filterToProduct(btn.dataset.product));
  });
  els.drawerContent.querySelectorAll("[data-action='filter-channel']").forEach((btn) => {
    wireAction(btn, () => filterToChannel(btn.dataset.channel));
  });
  els.drawerContent.querySelectorAll("[data-issue-id]").forEach((row) => {
    wireAction(row, () => openIssue(row.dataset.issueId));
  });
}

async function fetchScopedDashboard(overrides = {}) {
  const filters = {
    date_preset: overrides.date_preset ?? state.filters.date_preset,
    products: overrides.products ?? [...state.filters.products],
    models: overrides.models ?? [...state.filters.models],
    fault_codes: overrides.fault_codes ?? [...state.filters.fault_codes],
    channels: overrides.channels ?? [...state.filters.channels],
    bot_actions: overrides.bot_actions ?? [...state.filters.bot_actions],
    quick_exclusions: overrides.quick_exclusions ?? [...state.filters.quick_exclusions],
  };
  const params = new URLSearchParams({ date_preset: filters.date_preset });
  if (filters.products.length) params.set("products", filters.products.join(","));
  if (filters.models.length) params.set("models", filters.models.join(","));
  if (filters.fault_codes.length) params.set("fault_codes", filters.fault_codes.join(","));
  if (filters.channels.length) params.set("channels", filters.channels.join(","));
  if (filters.bot_actions.length) params.set("bot_actions", filters.bot_actions.join(","));
  if (filters.quick_exclusions.length) params.set("quick_exclusions", filters.quick_exclusions.join(","));
  return fetchJson(`${state.apiBase}/api/dashboard?${params}`, { timeoutMs: 20000 });
}

function collectIssuesFromPayload(payload, productFamily = null) {
  const views = payload?.issue_views || {};
  const seen = new Set();
  const issues = [];
  for (const viewIssues of Object.values(views)) {
    for (const issue of (viewIssues || [])) {
      if (productFamily && issue.product_family !== productFamily) continue;
      if (seen.has(issue.issue_id)) continue;
      seen.add(issue.issue_id);
      issues.push(issue);
    }
  }
  return issues.sort((a, b) => (b.volume || 0) - (a.volume || 0));
}

function aggregateIssueCategories(issues) {
  const total = issues.reduce((sum, issue) => sum + Number(issue.volume || 0), 0) || 1;
  const grouped = new Map();
  for (const issue of issues) {
    const key = issue.fault_code || "Unclassified";
    if (!grouped.has(key)) {
      grouped.set(key, { label: key, volume: 0, repairNum: 0, repeatNum: 0, botNum: 0, issueCount: 0 });
    }
    const row = grouped.get(key);
    const volume = Number(issue.volume || 0);
    row.volume += volume;
    row.repairNum += volume * Number(issue.repair_field_visit_rate || 0);
    row.repeatNum += volume * Number(issue.repeat_rate || 0);
    row.botNum += volume * Number(issue.bot_deflection_rate || 0);
    row.issueCount += 1;
  }
  return [...grouped.values()].map((row) => ({
    label: row.label,
    count: row.volume,
    share: row.volume / total,
    detail: `${row.issueCount} issue types`,
    metrics: [
      `Repair ${fmtPct(row.repairNum / Math.max(row.volume, 1))}`,
      `Repeat ${fmtPct(row.repeatNum / Math.max(row.volume, 1))}`,
      `Bot ${fmtPct(row.botNum / Math.max(row.volume, 1))}`,
    ],
  })).sort((a, b) => b.count - a.count);
}

function buildBreakdownRows(items, options = {}) {
  const rows = items || [];
  if (!rows.length) return `<div class="empty-state">${esc(options.empty || "No breakdown available.")}</div>`;
  const max = Math.max(...rows.map((row) => Number(row.count || 0)), 1);
  return `<div class="breakdown-list">${rows.map((row) => `
    <div class="breakdown-row">
      <div class="breakdown-head">
        <div>
          <div class="breakdown-label">${esc(row.label)}</div>
          ${row.detail ? `<div class="breakdown-detail">${esc(row.detail)}</div>` : ""}
        </div>
        <div class="breakdown-value">
          <div>${options.countFmt ? options.countFmt(row.count) : fmtNum(row.count)}</div>
          <div class="breakdown-share">${row.share != null ? fmtPct(row.share) : ""}</div>
        </div>
      </div>
      <div class="breakdown-bar"><div class="breakdown-fill" style="width:${(Number(row.count || 0) / max) * 100}%"></div></div>
      ${row.metrics?.length ? `<div class="breakdown-metrics">${row.metrics.map((metric) => `<span class="breakdown-metric">${esc(metric)}</span>`).join("")}</div>` : ""}
    </div>`).join("")}</div>`;
}

function buildIssueRows(issues, limit = 6) {
  const rows = issues.slice(0, limit).map((issue) => `
    <div class="drill-issue-row" tabindex="0" role="button" data-issue-id="${esc(issue.issue_id)}">
      <div class="dir-name">${esc(issue.fault_code_level_2 || issue.fault_code)}</div>
      <div class="dir-chips">
        <span class="chip chip-blue">${fmtNum(issue.volume)} tickets</span>
        ${(issue.repair_field_visit_rate || 0) > 0.03 ? `<span class="chip chip-red">Repair ${fmtPct(issue.repair_field_visit_rate)}</span>` : ""}
        ${(issue.repeat_rate || 0) > 0.05 ? `<span class="chip chip-amber">Repeat ${fmtPct(issue.repeat_rate)}</span>` : ""}
        ${(issue.bot_deflection_rate || 0) > 0.05 ? `<span class="chip chip-green">Bot ${fmtPct(issue.bot_deflection_rate)}</span>` : ""}
        ${(issue.bot_transfer_rate || 0) > 0.1 ? `<span class="chip chip-amber">Transfer ${fmtPct(issue.bot_transfer_rate)}</span>` : ""}
      </div>
      <div class="breakdown-detail">${esc(cleanCopy(issue.insight || ""))}</div>
    </div>`);
  return rows.length ? `<div class="drill-issue-list">${rows.join("")}</div>` : `<div class="empty-state">No issue data available.</div>`;
}

function renderStoryCallout(title, lines) {
  return `<div class="story-card">
    <div class="story-title">${esc(title)}</div>
    ${lines.map((line) => `<div class="story-line">${esc(line)}</div>`).join("")}
  </div>`;
}

// Week drill-down — from volume or field trend bar click
function openWeekDrawer(weekKey, weekLabel, ds) {
  const isWeekly = state.filters.date_preset === "30d" || state.filters.date_preset === "60d";
  const points = (state.payload?.timeline || []).filter((p) => {
    if (!p.date || !weekKey) return false;
    const d = new Date(p.date + "T00:00:00");
    const start = new Date(weekKey + "T00:00:00");
    const end = new Date(start); end.setDate(end.getDate() + (isWeekly ? 7 : 1));
    return d >= start && d < end;
  });

  const total   = Number(ds.tickets || 0) || points.reduce((s, p) => s + Number(p.tickets || 0), 0);
  const repair  = Number(ds.repair  || 0) || points.reduce((s, p) => s + Number(p.repair_field || 0), 0);
  const install = Number(ds.install || 0) || points.reduce((s, p) => s + Number(p.install_field || 0), 0);
  const botN    = Number(ds.botn    || 0) || points.reduce((s, p) => s + Number(p.bot_resolved || 0), 0);
  const botR    = total > 0 ? `${(botN / total * 100).toFixed(1)}%` : "-";

  // Compare to period average
  const allBucketed = bucketTimeline(state.payload?.timeline || [], state.filters.date_preset);
  const avgT = allBucketed.length > 0 ? Math.round(allBucketed.reduce((s, b) => s + b.tickets, 0) / allBucketed.length) : 0;
  const diff = avgT > 0 ? ((total - avgT) / avgT * 100).toFixed(0) : null;
  const diffColor = diff > 0 ? "#b5300a" : "#1f6645";

  // Daily mini-chart (only for weekly buckets where we have multiple daily points)
  const DAYS = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"];
  const W2 = 340, H2 = 88, padL2 = 8, padR2 = 8, padT2 = 20, padB2 = 18;
  const maxD = Math.max(...points.map((p) => Number(p.tickets || 0)), 1);
  const bW2 = Math.max(6, Math.floor((W2 - padL2 - padR2) / Math.max(points.length, 1) * 0.7));
  const step2 = points.length > 0 ? (W2 - padL2 - padR2) / points.length : 1;
  const dailyBars = points.map((p, i) => {
    const v = Number(p.tickets || 0);
    const bH = Math.max(2, (v / maxD) * (H2 - padT2 - padB2));
    const cx = padL2 + i * step2 + step2 / 2;
    const y = H2 - padB2 - bH;
    const dt = new Date(p.date + "T00:00:00");
    const dayLbl = DAYS[(dt.getDay() + 6) % 7];
    return `<rect x="${cx - bW2/2}" y="${y}" width="${bW2}" height="${bH}" fill="#1e4f82" rx="2" opacity="0.85"/>
      <text x="${cx}" y="${H2 - 4}" text-anchor="middle" font-size="8" fill="#6b6659">${dayLbl}</text>
      <text x="${cx}" y="${y - 3}" text-anchor="middle" font-size="7.5" fill="#8a8070">${fmtNum(v)}</text>`;
  }).join("");

  const topProducts = (state.payload?.product_health || []).slice(0, 5).map((product) => ({
    label: product.product_family,
    count: product.ticket_volume,
    share: product.ticket_volume / Math.max((state.payload?.kpis?.total_tickets?.value || total), 1),
    detail: cleanCopy(product.top_issue || "Top issue unavailable"),
    metrics: [
      `Repair ${fmtPct(product.repair_field_visit_rate)}`,
      `Repeat ${fmtPct(product.repeat_rate)}`,
      `Bot ${fmtPct(product.bot_deflection_rate)}`,
    ],
  }));
  const categoryRows = aggregateIssueCategories(collectIssuesFromPayload(state.payload)).slice(0, 5);
  const activeChannels = (state.payload?.service_ops?.channel_mix || []).slice(0, 5);

  openDrawer(`${isWeekly ? "Week: " : ""}${weekLabel}`, `
    <div class="drill-kpi-row">
      <div class="drill-kpi"><div class="dk-val">${fmtNum(total)}</div><div class="dk-lbl">Total tickets</div></div>
      <div class="drill-kpi"><div class="dk-val" style="color:#8a3520">${fmtPct(total > 0 ? repair / total : 0)}</div><div class="dk-lbl">Repair visit %</div></div>
      <div class="drill-kpi"><div class="dk-val" style="color:#7a5218">${fmtPct(total > 0 ? install / total : 0)}</div><div class="dk-lbl">Installation %</div></div>
      <div class="drill-kpi"><div class="dk-val" style="color:#2a6b50">${botR}</div><div class="dk-lbl">Bot resolved %</div></div>
    </div>
    ${diff !== null ? `<div class="drill-vs">vs period average: <strong style="color:${diffColor}">${diff > 0 ? "+" : ""}${diff}%</strong> tickets &nbsp;(period avg: ${fmtNum(avgT)} / ${isWeekly ? "week" : "day"})</div>` : ""}
    ${isWeekly && points.length > 1 ? `
      <div class="drill-sub-title">Day-by-day breakdown</div>
      <svg viewBox="0 0 ${W2} ${H2}" style="width:100%;height:${H2}px;max-width:420px">${dailyBars}</svg>` : ""}
    <div class="insight-grid">
      ${renderStoryCallout("What stands out", [
        diff !== null ? `${diff > 0 ? "Above" : "Below"} period average by ${Math.abs(Number(diff))}% tickets.` : "No prior-period comparison available.",
        `Repair + installation visits account for ${fmtPct(total > 0 ? (repair + install) / total : 0)} of tickets.`,
        `Bot resolved share in this period is ${botR}.`,
      ])}
      ${renderStoryCallout("How to use this view", [
        "Use this pane to spot unusual load spikes before drilling into a product or issue.",
        "Then click a product bar or issue card to see which categories and models are driving demand.",
      ])}
    </div>
    <div class="drawer-two-col">
      <div>
        <div class="drill-sub-title">Top products in current portfolio</div>
        ${buildBreakdownRows(topProducts, { empty: "No product mix available." })}
      </div>
      <div>
        <div class="drill-sub-title">Issue categories in current portfolio</div>
        ${buildBreakdownRows(categoryRows, { empty: "No category mix available." })}
      </div>
    </div>
    <div class="drill-sub-title">Channel context</div>
    ${buildBreakdownRows(activeChannels, { empty: "No channel context available." })}
  `);
}

// Product drill-down — from field visit by product bar click
function openProductDrawer(productFamily) {
  const ph      = (state.payload?.product_health || []).find((p) => p.product_family === productFamily) || {};
  const botRow  = (state.payload?.bot_summary?.by_product || []).find((r) => r.product_family === productFamily) || {};
  const models  = (state.payload?.model_breakdown || {})[productFamily] || [];

  // Collect all issues for this product from all views (deduplicate by fault_code_level_2)
  const views = state.payload?.issue_views || {};
  const seen = new Set();
  const issues = [];
  for (const viewIssues of Object.values(views)) {
    for (const iss of (viewIssues || [])) {
      if (iss.product_family === productFamily) {
        const key = iss.fault_code_level_2 || iss.fault_code;
        if (!seen.has(key)) { seen.add(key); issues.push(iss); }
      }
    }
  }
  issues.sort((a, b) => (b.volume || 0) - (a.volume || 0));

  // Word cloud: issue names + symptom/defect signals weighted by volume
  const wordFreq = {};
  const addW = (t, w) => { if (t && t !== "Unknown" && t !== "-" && t.length > 1) { const k = t.trim(); wordFreq[k] = (wordFreq[k] || 0) + w; } };
  for (const iss of issues) {
    addW(iss.fault_code_level_2, (iss.volume || 1));
    addW(iss.top_symptom, Math.round((iss.volume || 1) * 0.5));
    addW(iss.top_defect,  Math.round((iss.volume || 1) * 0.4));
    addW(iss.top_repair,  Math.round((iss.volume || 1) * 0.2));
  }
  const cloudWords = Object.entries(wordFreq).sort((a, b) => b[1] - a[1]).slice(0, 28);
  const maxWF = cloudWords[0]?.[1] || 1;
  const cloudHtml = cloudWords.map(([word, freq]) => {
    const sz = 11 + Math.round((freq / maxWF) * 16);
    const op = 0.5 + (freq / maxWF) * 0.5;
    return `<span class="cloud-word" style="font-size:${sz}px;color:#1e4f82;opacity:${op}">${esc(word)}</span>`;
  }).join("");

  // Top issues rows
  const issueRows = issues.slice(0, 10).map((iss) => `
    <div class="drill-issue-row" tabindex="0" role="button" data-issue-id="${esc(iss.issue_id || iss.fault_code_level_2 || iss.fault_code)}">
      <div class="dir-name">${esc(iss.fault_code_level_2 || iss.fault_code)}</div>
      <div class="dir-chips">
        <span class="chip chip-blue">${fmtNum(iss.volume)} tickets</span>
        ${(iss.repair_field_visit_rate || 0) > 0.03 ? `<span class="chip chip-red">Repair ${fmtPct(iss.repair_field_visit_rate)}</span>` : ""}
        ${(iss.repeat_rate || 0) > 0.05 ? `<span class="chip chip-amber">Repeat ${fmtPct(iss.repeat_rate)}</span>` : ""}
        ${(iss.bot_deflection_rate || 0) > 0.05 ? `<span class="chip chip-green">Bot ${fmtPct(iss.bot_deflection_rate)}</span>` : ""}
        ${(iss.bot_transfer_rate || 0) > 0.1 ? `<span class="chip chip-amber">Transfer ${fmtPct(iss.bot_transfer_rate)}</span>` : ""}
      </div>
    </div>`).join("");

  // Model rows
  const maxMvol = Math.max(...models.map((m) => m.tickets), 1);
  const modelRows = models.slice(0, 10).map((m) => `
    <div class="drill-model-row">
      <div class="dmr-name" title="${esc(m.model)}">${esc(m.model)}</div>
      <div class="dmr-bar"><div class="dmr-fill" style="width:${(m.tickets / maxMvol) * 100}%"></div></div>
      <div class="dmr-val">${fmtNum(m.tickets)}</div>
      <div class="dmr-val ${(m.repair_field_visit_rate || 0) >= 0.12 ? "red" : ""}">${fmtPct(m.repair_field_visit_rate)}</div>
      <div class="dmr-val ${(m.bot_deflection_rate || 0) >= 0.2 ? "green" : ""}">${fmtPct(m.bot_deflection_rate)}</div>
    </div>`).join("");

  openDrawer(`${productFamily} - Deep Dive`, `
    <div class="drill-kpi-row">
      <div class="drill-kpi"><div class="dk-val">${fmtNum(ph.ticket_volume)}</div><div class="dk-lbl">Tickets</div></div>
      <div class="drill-kpi"><div class="dk-val" style="color:#8a3520">${fmtPct(ph.repair_field_visit_rate)}</div><div class="dk-lbl">Repair %</div></div>
      <div class="drill-kpi"><div class="dk-val" style="color:#7a5218">${fmtPct(ph.repeat_rate)}</div><div class="dk-lbl">Repeat %</div></div>
      <div class="drill-kpi"><div class="dk-val" style="color:#2a6b50">${fmtPct(ph.bot_deflection_rate)}</div><div class="dk-lbl">Bot resolved</div></div>
    </div>

    ${botRow.chat_tickets > 0 ? `
    <div class="drill-sub-title">Chatbot performance</div>
    <div class="drill-kpi-row" style="grid-template-columns:repeat(3,1fr)">
      <div class="drill-kpi"><div class="dk-val">${fmtNum(botRow.chat_tickets)}</div><div class="dk-lbl">Chat sessions</div></div>
      <div class="drill-kpi"><div class="dk-val" style="color:#2a6b50">${fmtPct(botRow.bot_resolved_rate)}</div><div class="dk-lbl">Bot resolved</div></div>
      <div class="drill-kpi"><div class="dk-val" style="color:#8a3520">${fmtPct(botRow.bot_transferred_rate)}</div><div class="dk-lbl">Transferred</div></div>
    </div>` : ""}

    <div class="drill-sub-title">Top issues - ${issues.length} total (click any to drill deeper)</div>
    <div class="drill-issue-list">${issueRows || `<div class="empty-state">No issue data found for this product.</div>`}</div>

    ${models.length ? `
    <div class="drill-sub-title">Model breakdown</div>
    <div class="drill-model-header">
      <div>Model</div><div></div>
      <div style="text-align:right">Tickets</div>
      <div style="text-align:right">Repair %</div>
      <div style="text-align:right">Bot %</div>
    </div>
    <div class="drill-model-list">${modelRows}</div>` : ""}

    ${cloudHtml ? `
    <div class="drill-sub-title">Issue signals - word cloud</div>
    <div class="word-cloud">${cloudHtml}</div>
    <div style="font-size:11px;color:var(--muted);margin-top:8px">Word size = ticket volume. Based on issue names, reported symptoms, and defects.</div>` : ""}

    <div class="drill-action">
      <button class="btn-primary" data-action="filter-product" data-product="${esc(productFamily)}">Filter dashboard to ${esc(productFamily)}</button>
    </div>
  `);

  // Wire issue row clicks → existing issue detail drawer
  els.drawerContent.querySelectorAll(".drill-issue-row[data-issue-id]").forEach((row) => {
    wireAction(row, () => openIssue(row.dataset.issueId));
  });
}

function filterToProduct(product) {
  closeDrawer();
  state.filters.products = [product];
  state.filters.models = [];
  loadDashboard();
}

// Channel drill-down — from donut slice or channel mix bar click
function openChannelDrawer(channel, count, share) {
  const CHAN_COLORS = { "Chat": "#2a5580", "Phone": "#5a3875", "Email": "#2a6b50", "WhatsApp": "#7a5218", "Web": "#226060", "Others": "#5c5248" };
  const channelMix = (state.payload?.service_ops?.channel_mix || []);
  const total = channelMix.reduce((s, r) => s + Number(r.count || 0), 0) || 1;
  const sorted = [...channelMix].sort((a, b) => Number(b.count || 0) - Number(a.count || 0));
  const maxC = sorted[0] ? Number(sorted[0].count || 0) : 1;

  const barRows = sorted.map((r) => {
    const isThis = r.label === channel;
    const col = CHAN_COLORS[r.label] || "#5c5248";
    return `
      <div class="drill-chan-row${isThis ? " drill-chan-active" : ""}">
        <div class="dcr-name" style="color:${col}">${esc(r.label)}</div>
        <div class="dcr-bar-wrap"><div class="dcr-fill" style="width:${(Number(r.count || 0) / maxC) * 100}%;background:${col}"></div></div>
        <div class="dcr-val">${fmtNum(r.count)}</div>
        <div class="dcr-pct">${fmtPct(Number(r.count || 0) / total)}</div>
      </div>`;
  }).join("");

  openDrawer(`${channel} - Channel Analysis`, `
    <div class="drill-kpi-row" style="grid-template-columns:repeat(2,1fr)">
      <div class="drill-kpi"><div class="dk-val">${fmtNum(count)}</div><div class="dk-lbl">Tickets (this period)</div></div>
      <div class="drill-kpi"><div class="dk-val">${fmtPct(Number(share))}</div><div class="dk-lbl">Share of volume</div></div>
    </div>
    <div class="drill-sub-title">Channel comparison</div>
    <div class="drill-chan-list">${barRows}</div>
    <div class="drill-action">
      <button class="btn-primary" data-action="filter-channel" data-channel="${esc(channel)}">Filter dashboard to ${esc(channel)}</button>
    </div>
  `);
}

function filterToChannel(channel) {
  closeDrawer();
  state.filters.channels = [channel];
  loadDashboard();
}

function buildProductDrawerHtml(productFamily, payload, options = {}) {
  const ph = (payload?.product_health || []).find((p) => p.product_family === productFamily) || {};
  const botRow = (payload?.bot_summary?.by_product || []).find((r) => r.product_family === productFamily) || {};
  const models = (payload?.model_breakdown || {})[productFamily] || [];
  const issues = collectIssuesFromPayload(payload, productFamily);
  const categories = aggregateIssueCategories(issues).slice(0, 6);
  const channels = (payload?.service_ops?.channel_mix || []).slice(0, 6);
  const resolutions = (payload?.service_ops?.resolution_mix || []).slice(0, 5);
  const topIssue = issues[0];
  const topCategory = categories[0];
  const storyLines = [
    topIssue ? `${topIssue.fault_code_level_2} is the largest issue with ${fmtNum(topIssue.volume)} tickets.` : "No dominant issue identified yet.",
    topCategory ? `${topCategory.label} contributes ${fmtPct(topCategory.share)} of this product's coded load.` : "No category concentration available.",
    `Repair rate is ${fmtPct(ph.repair_field_visit_rate || 0)} and repeat rate is ${fmtPct(ph.repeat_rate || 0)}.`,
  ];
  const wordFreq = {};
  const addW = (t, w) => { if (t && t !== "Unknown" && t !== "-" && t.length > 1) { const k = t.trim(); wordFreq[k] = (wordFreq[k] || 0) + w; } };
  for (const issue of issues) {
    addW(issue.fault_code_level_2, issue.volume || 1);
    addW(issue.top_symptom, Math.round((issue.volume || 1) * 0.5));
    addW(issue.top_defect, Math.round((issue.volume || 1) * 0.4));
    addW(issue.top_repair, Math.round((issue.volume || 1) * 0.2));
  }
  const cloudWords = Object.entries(wordFreq).sort((a, b) => b[1] - a[1]).slice(0, 24);
  const maxWF = cloudWords[0]?.[1] || 1;
  const cloudHtml = cloudWords.map(([word, freq]) => {
    const sz = 11 + Math.round((freq / maxWF) * 16);
    const op = 0.5 + (freq / maxWF) * 0.5;
    return `<span class="cloud-word" style="font-size:${sz}px;color:#1e4f82;opacity:${op}">${esc(word)}</span>`;
  }).join("");
  const maxMvol = Math.max(...models.map((m) => m.tickets), 1);
  const modelRows = models.slice(0, 10).map((m) => `
    <div class="drill-model-row">
      <div class="dmr-name" title="${esc(m.model === productFamily ? "Unmapped / family-level" : m.model)}">${esc(m.model === productFamily ? "Unmapped / family-level" : m.model)}</div>
      <div class="dmr-bar"><div class="dmr-fill" style="width:${(m.tickets / maxMvol) * 100}%"></div></div>
      <div class="dmr-val">${fmtNum(m.tickets)}</div>
      <div class="dmr-val ${(m.repair_field_visit_rate || 0) >= 0.12 ? "red" : ""}">${fmtPct(m.repair_field_visit_rate)}</div>
      <div class="dmr-val ${(m.bot_deflection_rate || 0) >= 0.2 ? "green" : ""}">${fmtPct(m.bot_deflection_rate)}</div>
    </div>`).join("");
  return `
    <div class="drill-kpi-row">
      <div class="drill-kpi"><div class="dk-val">${fmtNum(ph.ticket_volume || 0)}</div><div class="dk-lbl">Tickets</div></div>
      <div class="drill-kpi"><div class="dk-val" style="color:#8a3520">${fmtPct(ph.repair_field_visit_rate || 0)}</div><div class="dk-lbl">Repair %</div></div>
      <div class="drill-kpi"><div class="dk-val" style="color:#7a5218">${fmtPct(ph.installation_field_visit_rate || 0)}</div><div class="dk-lbl">Installation %</div></div>
      <div class="drill-kpi"><div class="dk-val" style="color:#2a6b50">${fmtPct(ph.bot_deflection_rate || 0)}</div><div class="dk-lbl">Bot resolved</div></div>
    </div>
    <div class="insight-grid">
      ${renderStoryCallout("Product readout", storyLines)}
      ${renderStoryCallout("Breakdown status", [
        models.length ? `${models.filter((m) => m.model !== productFamily).length || 0} identifiable models in this slice.` : "Model mix not yet identified.",
        channels[0] ? `${channels[0].label} is the largest channel for this product.` : "No channel split available.",
        options.loading ? "More detail is still loading." : "Detailed breakdown is ready.",
      ])}
    </div>
    ${options.loading ? `<div class="drawer-inline-loader">${drawerLoadingHtml("Loading detailed product breakdown...")}</div>` : ""}
    ${botRow.chat_tickets > 0 ? `
    <div class="drill-sub-title">Chatbot performance</div>
    <div class="drill-kpi-row" style="grid-template-columns:repeat(4,1fr)">
      <div class="drill-kpi"><div class="dk-val">${fmtNum(botRow.chat_tickets)}</div><div class="dk-lbl">Chat sessions</div></div>
      <div class="drill-kpi"><div class="dk-val" style="color:#2a6b50">${fmtPct(botRow.bot_resolved_rate)}</div><div class="dk-lbl">Bot resolved</div></div>
      <div class="drill-kpi"><div class="dk-val" style="color:#8a3520">${fmtPct(botRow.bot_transferred_rate)}</div><div class="dk-lbl">Transferred</div></div>
      <div class="drill-kpi"><div class="dk-val" style="color:#7a5218">${fmtPct(botRow.blank_chat_rate || 0)}</div><div class="dk-lbl">Blank chat</div></div>
    </div>` : ""}
    <div class="drawer-two-col">
      <div>
        <div class="drill-sub-title">Category concentration</div>
        ${buildBreakdownRows(categories, { empty: "No category concentration available." })}
      </div>
      <div>
        <div class="drill-sub-title">Channel mix</div>
        ${buildBreakdownRows(channels, { empty: "No channel mix available." })}
      </div>
    </div>
    <div class="drawer-two-col">
      <div>
        <div class="drill-sub-title">Resolution mix</div>
        ${buildBreakdownRows(resolutions, { empty: "No resolution mix available." })}
      </div>
      <div>
        <div class="drill-sub-title">Top issues</div>
        ${buildIssueRows(issues, 6)}
      </div>
    </div>
    ${models.length ? `
    <div class="drill-sub-title">Model breakdown for ${esc(productFamily)}</div>
    <div class="drill-model-header">
      <div>Model</div><div></div>
      <div style="text-align:right">Tickets</div>
      <div style="text-align:right">Repair %</div>
      <div style="text-align:right">Bot %</div>
    </div>
    <div class="drill-model-list">${modelRows}</div>` : ""}
    ${cloudHtml ? `
    <div class="drill-sub-title">Signal cloud</div>
    <div class="word-cloud">${cloudHtml}</div>
    <div style="font-size:11px;color:var(--muted);margin-top:8px">Word size reflects ticket concentration across issue, symptom, defect, and repair signals.</div>` : ""}
    <div class="drill-action">
      <button class="btn-primary" data-action="filter-product" data-product="${esc(productFamily)}">Filter dashboard to ${esc(productFamily)}</button>
    </div>`;
}

async function openProductDrawer(productFamily) {
  openDrawer(`${productFamily} - Deep Dive`, buildProductDrawerHtml(productFamily, state.payload, { loading: true }));
  try {
    const drill = await fetchV2ProductDrill(productFamily);
    els.drawerTitle.textContent = `${productFamily} - Deep Dive`;
    els.drawerContent.innerHTML = buildProductDrawerHtmlFromV2(productFamily, drill);
    wireDrawerActions();
  } catch (err) {
    // V2 drill failed — fall back to legacy scoped dashboard.
    try {
      const payload = await fetchScopedDashboard({ products: [productFamily] });
      els.drawerTitle.textContent = `${productFamily} - Deep Dive`;
      els.drawerContent.innerHTML = buildProductDrawerHtml(productFamily, payload);
      wireDrawerActions();
    } catch (legacyErr) {
      els.drawerContent.innerHTML = `<div class="error-state">Failed to load product breakdown: ${esc(legacyErr.message)}</div>`;
    }
  }
}

async function fetchV2ProductDrill(productFamily) {
  const params = new URLSearchParams({ date_preset: state.filters.date_preset });
  if (state.filters.models.length) params.set("models", state.filters.models.join(","));
  if (state.filters.fault_codes.length) params.set("fault_codes", state.filters.fault_codes.join(","));
  if (state.filters.channels.length) params.set("channels", state.filters.channels.join(","));
  if (state.filters.bot_actions.length) params.set("bot_actions", state.filters.bot_actions.join(","));
  if (state.filters.quick_exclusions.length) params.set("quick_exclusions", state.filters.quick_exclusions.join(","));
  return fetchJson(`${state.apiBase}/api/v2/drill/product/${encodeURIComponent(productFamily)}?${params}`, { timeoutMs: 15000 });
}

function buildProductDrawerHtmlFromV2(productFamily, drill) {
  const kpis = drill.summary || {};
  const models = drill.models || [];
  const categories = (drill.categories || []).slice(0, 6);
  const issues = drill.issues || [];
  const channels = (drill.channels || []).slice(0, 6);
  const tickets = kpis.total_tickets?.value || 0;
  const repair = kpis.repair_field_visit_rate?.value || 0;
  const installation = kpis.installation_field_visit_rate?.value || 0;
  const botRate = kpis.bot_deflection_rate?.value || 0;
  const repeatRate = kpis.repeat_rate?.value || 0;
  const topIssue = issues[0];
  const topCat = categories[0];

  const wordFreq = {};
  const addW = (t, w) => { if (t && t !== "Unknown" && t !== "-" && t.length > 1) { const k = t.trim(); wordFreq[k] = (wordFreq[k] || 0) + w; } };
  for (const iss of issues) addW(iss.label, iss.count || 1);
  const cloudWords = Object.entries(wordFreq).sort((a, b) => b[1] - a[1]).slice(0, 24);
  const maxWF = cloudWords[0]?.[1] || 1;
  const cloudHtml = cloudWords.map(([word, freq]) => {
    const sz = 11 + Math.round((freq / maxWF) * 16);
    const op = 0.5 + (freq / maxWF) * 0.5;
    return `<span class="cloud-word" style="font-size:${sz}px;color:#1e4f82;opacity:${op}">${esc(word)}</span>`;
  }).join("");

  const maxMvol = Math.max(...models.map((m) => m.tickets), 1);
  const modelRows = models.slice(0, 10).map((m) => `
    <div class="drill-model-row">
      <div class="dmr-name" title="${esc(m.model_name === productFamily ? "Unmapped / family-level" : m.model_name)}">${esc(m.model_name === productFamily ? "Unmapped / family-level" : m.model_name)}</div>
      <div class="dmr-bar"><div class="dmr-fill" style="width:${(m.tickets / maxMvol) * 100}%"></div></div>
      <div class="dmr-val">${fmtNum(m.tickets)}</div>
      <div class="dmr-val ${(m.repair_rate || 0) >= 0.12 ? "red" : ""}">${fmtPct(m.repair_rate || 0)}</div>
      <div class="dmr-val ${(m.bot_rate || 0) >= 0.2 ? "green" : ""}">${fmtPct(m.bot_rate || 0)}</div>
    </div>`).join("");

  const issueRows = issues.slice(0, 8).map((iss) => `
    <div class="drill-issue-row">
      <div class="dir-name">${esc(iss.label)}</div>
      <div class="dir-chips">
        <span class="chip chip-blue">${fmtNum(iss.count)} tickets</span>
        ${(iss.repair_rate || 0) > 0.03 ? `<span class="chip chip-red">Repair ${fmtPct(iss.repair_rate)}</span>` : ""}
        ${(iss.repeat_rate || 0) > 0.05 ? `<span class="chip chip-amber">Repeat ${fmtPct(iss.repeat_rate)}</span>` : ""}
        ${(iss.bot_rate || 0) > 0.05 ? `<span class="chip chip-green">Bot ${fmtPct(iss.bot_rate)}</span>` : ""}
      </div>
    </div>`).join("");

  const catRows = categories.map((c) => ({ label: c.label, count: c.count, share: c.share, detail: "", metrics: [] }));
  const channelRows = channels.map((c) => ({ label: c.label, count: c.count, share: c.share }));

  return `
    <div class="drill-kpi-row">
      <div class="drill-kpi"><div class="dk-val">${fmtNum(tickets)}</div><div class="dk-lbl">Tickets</div></div>
      <div class="drill-kpi"><div class="dk-val" style="color:#8a3520">${fmtPct(repair)}</div><div class="dk-lbl">Repair %</div></div>
      <div class="drill-kpi"><div class="dk-val" style="color:#7a5218">${fmtPct(installation)}</div><div class="dk-lbl">Installation %</div></div>
      <div class="drill-kpi"><div class="dk-val" style="color:#2a6b50">${fmtPct(botRate)}</div><div class="dk-lbl">Bot resolved</div></div>
    </div>
    <div class="insight-grid">
      ${renderStoryCallout("Product readout", [
        topIssue ? `${topIssue.label} is the largest issue with ${fmtNum(topIssue.count)} tickets.` : "No dominant issue identified yet.",
        topCat ? `${topCat.label} contributes ${fmtPct(topCat.share)} of this product's coded load.` : "No category concentration available.",
        `Repair rate is ${fmtPct(repair)} and repeat rate is ${fmtPct(repeatRate)}.`,
      ])}
      ${renderStoryCallout("Breakdown status", [
        models.length ? `${models.filter((m) => m.model_name !== productFamily).length} identifiable models in this slice.` : "Model mix not yet identified.",
        channels[0] ? `${channels[0].label} is the largest channel for this product.` : "No channel split available.",
      ])}
    </div>
    <div class="drawer-two-col">
      <div>
        <div class="drill-sub-title">Category concentration</div>
        ${buildBreakdownRows(catRows, { empty: "No category concentration available." })}
      </div>
      <div>
        <div class="drill-sub-title">Channel mix</div>
        ${buildBreakdownRows(channelRows, { empty: "No channel mix available." })}
      </div>
    </div>
    <div class="drill-sub-title">Top issues</div>
    <div class="drill-issue-list">${issueRows || `<div class="empty-state">No issue data available.</div>`}</div>
    ${models.length ? `
    <div class="drill-sub-title">Model breakdown for ${esc(productFamily)}</div>
    <div class="drill-model-header">
      <div>Model</div><div></div>
      <div style="text-align:right">Tickets</div>
      <div style="text-align:right">Repair %</div>
      <div style="text-align:right">Bot %</div>
    </div>
    <div class="drill-model-list">${modelRows}</div>` : ""}
    ${cloudHtml ? `
    <div class="drill-sub-title">Signal cloud</div>
    <div class="word-cloud">${cloudHtml}</div>
    <div style="font-size:11px;color:var(--muted);margin-top:8px">Word size reflects ticket concentration across issue signals.</div>` : ""}
    <div class="drill-action">
      <button class="btn-primary" data-action="filter-product" data-product="${esc(productFamily)}">Filter dashboard to ${esc(productFamily)}</button>
    </div>`;
}

async function openChannelDrawer(channel, count, share) {
  const CHAN_COLORS = { "Chat": "#2a5580", "Phone": "#5a3875", "Email": "#2a6b50", "WhatsApp": "#7a5218", "Web": "#226060", "Others": "#5c5248" };
  openDrawer(`${channel} - Channel Analysis`, drawerLoadingHtml("Loading channel breakdown..."));
  try {
    const payload = await fetchScopedDashboard({ channels: [channel] });
    const kpis = payload.kpis || {};
    const scopedIssues = collectIssuesFromPayload(payload);
    const products = (payload.product_health || []).slice(0, 6).map((product) => ({
      label: product.product_family,
      count: product.ticket_volume,
      share: (kpis.total_tickets?.value || 0) > 0 ? product.ticket_volume / kpis.total_tickets.value : 0,
      detail: cleanCopy(product.top_issue || "Top issue unavailable"),
      metrics: [
        `Repair ${fmtPct(product.repair_field_visit_rate)}`,
        `Repeat ${fmtPct(product.repeat_rate)}`,
        `Bot ${fmtPct(product.bot_deflection_rate)}`,
      ],
    }));
    const categories = aggregateIssueCategories(scopedIssues).slice(0, 6);
    const resolutions = (payload.service_ops?.resolution_mix || []).slice(0, 5);
    const outcomes = (payload.service_ops?.bot_outcomes || []).slice(0, 5);
    const headlineIssue = scopedIssues[0];

    els.drawerTitle.textContent = `${channel} - Channel Analysis`;
    els.drawerContent.innerHTML = `
      <div class="drill-kpi-row">
        <div class="drill-kpi"><div class="dk-val">${fmtNum(kpis.total_tickets?.value || count)}</div><div class="dk-lbl">Tickets</div></div>
        <div class="drill-kpi"><div class="dk-val" style="color:#8a3520">${fmtPct(kpis.repair_field_visit_rate?.value || 0)}</div><div class="dk-lbl">Repair %</div></div>
        <div class="drill-kpi"><div class="dk-val" style="color:#7a5218">${fmtPct(kpis.repeat_rate?.value || 0)}</div><div class="dk-lbl">Repeat %</div></div>
        <div class="drill-kpi"><div class="dk-val" style="color:${CHAN_COLORS[channel] || "#1f6645"}">${fmtPct(Number(share))}</div><div class="dk-lbl">Share of volume</div></div>
      </div>

      <div class="insight-grid">
        ${renderStoryCallout("Channel role", [
          headlineIssue ? `${headlineIssue.fault_code_level_2} is the biggest issue coming through ${channel}.` : `No dominant issue identified for ${channel}.`,
          `Repair pressure in this channel is ${fmtPct(kpis.repair_field_visit_rate?.value || 0)}.`,
          `Repeat demand in this channel is ${fmtPct(kpis.repeat_rate?.value || 0)}.`,
        ])}
        ${renderStoryCallout("What to look for", [
          "Use product concentration to identify which business line is driving this channel.",
          "Use issue concentration to separate service workflow demand from product-quality demand.",
        ])}
      </div>

      <div class="drawer-two-col">
        <div>
          <div class="drill-sub-title">Product concentration</div>
          ${buildBreakdownRows(products, { empty: "No product concentration available." })}
        </div>
        <div>
          <div class="drill-sub-title">Issue categories</div>
          ${buildBreakdownRows(categories, { empty: "No issue category mix available." })}
        </div>
      </div>

      <div class="drawer-two-col">
        <div>
          <div class="drill-sub-title">Top issues</div>
          ${buildIssueRows(scopedIssues, 6)}
        </div>
        <div>
          <div class="drill-sub-title">${channel === "Chat" ? "Chatbot outcomes" : "Resolution mix"}</div>
          ${buildBreakdownRows(channel === "Chat" ? outcomes : resolutions, { empty: "No operational mix available." })}
        </div>
      </div>

      <div class="drill-action">
        <button class="btn-primary" data-action="filter-channel" data-channel="${esc(channel)}">Filter dashboard to ${esc(channel)}</button>
      </div>`;
    wireDrawerActions();
  } catch (err) {
    els.drawerContent.innerHTML = `<div class="error-state">Failed to load channel breakdown: ${esc(err.message)}</div>`;
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────────
async function fetchPeriodBreakdown(startDate, endDate) {
  const params = new URLSearchParams({
    start_date: startDate,
    end_date: endDate,
    date_preset: state.filters.date_preset,
  });
  if (state.filters.products.length) params.set("products", state.filters.products.join(","));
  if (state.filters.models.length) params.set("models", state.filters.models.join(","));
  if (state.filters.fault_codes.length) params.set("fault_codes", state.filters.fault_codes.join(","));
  if (state.filters.channels.length) params.set("channels", state.filters.channels.join(","));
  if (state.filters.bot_actions.length) params.set("bot_actions", state.filters.bot_actions.join(","));
  if (state.filters.quick_exclusions.length) params.set("quick_exclusions", state.filters.quick_exclusions.join(","));
  // Try v2 first — handles all filters via SQL, no source-mode recomputation.
  try {
    return await fetchJson(`${state.apiBase}/api/v2/period-breakdown?${params}`, { timeoutMs: 15000 });
  } catch {
    return fetchJson(`${state.apiBase}/api/period-breakdown?${params}`, { timeoutMs: 25000 });
  }
}

async function fetchJson(url, { timeoutMs = 15000, signal = null } = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  // Chain external cancellation signal so aborting the caller also cancels this fetch.
  const onCancel = () => controller.abort();
  signal?.addEventListener("abort", onCancel);
  try {
    const res = await fetch(url, { signal: controller.signal });
    if (!res.ok) throw new Error(`API ${res.status}`);
    return await res.json();
  } catch (err) {
    if (err?.name === "AbortError") {
      throw new Error(`Timed out after ${Math.round(timeoutMs / 1000)}s`);
    }
    throw err;
  } finally {
    clearTimeout(timeout);
    signal?.removeEventListener("abort", onCancel);
  }
}

function buildCategoryCloud(categories, selectedLabel) {
  if (!categories.length) return `<div class="empty-state">No category mix available for this period.</div>`;
  const max = Math.max(...categories.map((item) => Number(item.count || 0)), 1);
  return `<div class="category-cloud">${categories.map((item) => {
    const scale = Number(item.count || 0) / max;
    const size = 12 + Math.round(scale * 16);
    return `
      <button type="button" class="category-cloud-word${item.label === selectedLabel ? " active" : ""}" data-period-category="${esc(item.label)}" style="font-size:${size}px">
        ${esc(item.label)}
        <span>${fmtNum(item.count)}</span>
      </button>`;
  }).join("")}</div>`;
}

function buildFc2SplitRows(rows) {
  if (!rows.length) return `<div class="empty-state">No FC2 split available for this category.</div>`;
  return buildBreakdownRows(rows.map((row) => ({
    label: row.label,
    count: row.count,
    share: row.share,
    detail: "Within selected category",
    metrics: [
      `Repair ${fmtPct(row.repair_rate || 0)}`,
      `Repeat ${fmtPct(row.repeat_rate || 0)}`,
      `Bot ${fmtPct(row.bot_rate || 0)}`,
    ],
  })));
}

function renderPeriodCategoryDetail(periodPayload, selectedLabel) {
  const cloudHost = els.drawerContent.querySelector("[data-role='period-category-cloud']");
  const titleHost = els.drawerContent.querySelector("[data-role='period-fc2-title']");
  const detailHost = els.drawerContent.querySelector("[data-role='period-fc2-detail']");
  if (!cloudHost || !titleHost || !detailHost) return;
  cloudHost.innerHTML = buildCategoryCloud(periodPayload.categories || [], selectedLabel);
  titleHost.textContent = selectedLabel ? `${selectedLabel} - FC2 split` : "FC2 split";
  detailHost.innerHTML = buildFc2SplitRows((periodPayload.fc2_by_category || {})[selectedLabel] || []);
  cloudHost.querySelectorAll("[data-period-category]").forEach((btn) => {
    wireAction(btn, () => renderPeriodCategoryDetail(periodPayload, btn.dataset.periodCategory));
  });
}

async function openWeekDrawer(weekKey, weekLabel, ds) {
  const isWeekly = state.filters.date_preset === "30d" || state.filters.date_preset === "60d";
  const points = (state.payload?.timeline || []).filter((p) => {
    if (!p.date || !weekKey) return false;
    const d = new Date(p.date + "T00:00:00");
    const start = new Date(weekKey + "T00:00:00");
    const end = new Date(start);
    end.setDate(end.getDate() + (isWeekly ? 7 : 1));
    return d >= start && d < end;
  });

  const total = Number(ds.tickets || 0) || points.reduce((s, p) => s + Number(p.tickets || 0), 0);
  const repair = Number(ds.repair || 0) || points.reduce((s, p) => s + Number(p.repair_field || 0), 0);
  const install = Number(ds.install || 0) || points.reduce((s, p) => s + Number(p.install_field || 0), 0);
  const botN = Number(ds.botn || 0) || points.reduce((s, p) => s + Number(p.bot_resolved || 0), 0);
  const allBucketed = bucketTimeline(state.payload?.timeline || [], state.filters.date_preset);
  const avgT = allBucketed.length > 0 ? Math.round(allBucketed.reduce((s, b) => s + b.tickets, 0) / allBucketed.length) : 0;
  const diff = avgT > 0 ? ((total - avgT) / avgT * 100).toFixed(0) : null;
  const diffColor = diff > 0 ? "#b5300a" : "#1f6645";

  const DAYS = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"];
  const W2 = 340, H2 = 88, padL2 = 8, padR2 = 8, padT2 = 20, padB2 = 18;
  const maxD = Math.max(...points.map((p) => Number(p.tickets || 0)), 1);
  const bW2 = Math.max(6, Math.floor((W2 - padL2 - padR2) / Math.max(points.length, 1) * 0.7));
  const step2 = points.length > 0 ? (W2 - padL2 - padR2) / points.length : 1;
  const dailyBars = points.map((p, i) => {
    const v = Number(p.tickets || 0);
    const bH = Math.max(2, (v / maxD) * (H2 - padT2 - padB2));
    const cx = padL2 + i * step2 + step2 / 2;
    const y = H2 - padB2 - bH;
    const dt = new Date(p.date + "T00:00:00");
    const dayLbl = DAYS[(dt.getDay() + 6) % 7];
    return `<rect x="${cx - bW2/2}" y="${y}" width="${bW2}" height="${bH}" fill="#1e4f82" rx="2" opacity="0.85"/>
      <text x="${cx}" y="${H2 - 4}" text-anchor="middle" font-size="8" fill="#6b6659">${dayLbl}</text>
      <text x="${cx}" y="${y - 3}" text-anchor="middle" font-size="7.5" fill="#8a8070">${fmtNum(v)}</text>`;
  }).join("");

  const start = new Date(weekKey + "T00:00:00");
  const end = new Date(start);
  end.setDate(end.getDate() + (isWeekly ? 6 : 0));
  const startDate = start.toISOString().slice(0, 10);
  const endDate = end.toISOString().slice(0, 10);

  openDrawer(`${isWeekly ? "Week: " : ""}${weekLabel}`, `
    <div class="drawer-report-header">
      <div class="briefing-label">Period Brief</div>
      <div class="drawer-report-title">${isWeekly ? "Weekly" : "Daily"} operating view for ${esc(weekLabel)}</div>
      <div class="drawer-report-copy">
        ${fmtNum(total)} tickets in this period.${diff !== null ? ` This is ${diff > 0 ? "above" : "below"} the run-rate by ${Math.abs(Number(diff))}%.` : ""}
      </div>
    </div>
    <div class="drill-kpi-row">
      <div class="drill-kpi"><div class="dk-val">${fmtNum(total)}</div><div class="dk-lbl">Total tickets</div></div>
      <div class="drill-kpi"><div class="dk-val" style="color:#8a3520">${fmtPct(total > 0 ? repair / total : 0)}</div><div class="dk-lbl">Repair visit %</div></div>
      <div class="drill-kpi"><div class="dk-val" style="color:#7a5218">${fmtPct(total > 0 ? install / total : 0)}</div><div class="dk-lbl">Installation %</div></div>
      <div class="drill-kpi"><div class="dk-val" style="color:#2a6b50">${fmtPct(total > 0 ? botN / total : 0)}</div><div class="dk-lbl">Bot resolved %</div></div>
    </div>
    <div class="drawer-inline-loader">${drawerLoadingHtml("Loading period breakdown...")}</div>`);
  try {
    const periodPayload = await fetchPeriodBreakdown(startDate, endDate);
    const scopedKpis = periodPayload.kpis || {};
    const scopedProducts = (periodPayload.products || []).slice(0, 6).map((product) => ({
      label: product.product_family,
      count: product.ticket_volume,
      share: (scopedKpis.total_tickets?.value || 0) > 0 ? product.ticket_volume / scopedKpis.total_tickets.value : 0,
      detail: cleanCopy(product.top_issue || "Top issue unavailable"),
      metrics: [
        `Repair ${fmtPct(product.repair_field_visit_rate || 0)}`,
        `Repeat ${fmtPct(product.repeat_rate || 0)}`,
        `Bot ${fmtPct(product.bot_deflection_rate || 0)}`,
      ],
    }));
    const categories = periodPayload.categories || [];
    const selectedCategory = categories[0]?.label || "";

    els.drawerTitle.textContent = `${isWeekly ? "Week: " : ""}${weekLabel}`;
    els.drawerContent.innerHTML = `
      <div class="drawer-report-header">
        <div class="briefing-label">Period Brief</div>
        <div class="drawer-report-title">${isWeekly ? "Weekly" : "Daily"} operating view for ${esc(weekLabel)}</div>
        <div class="drawer-report-copy">
          ${diff !== null
            ? `${fmtNum(scopedKpis.total_tickets?.value || total)} tickets in this period, ${diff > 0 ? "above" : "below"} the run-rate by ${Math.abs(Number(diff))}% versus the selected-window average.`
            : `${fmtNum(scopedKpis.total_tickets?.value || total)} tickets in this period.`}
          ${categories[0] ? ` ${categories[0].label} is the largest issue category in this slice.` : ""}
        </div>
      </div>
      <div class="drill-kpi-row">
        <div class="drill-kpi"><div class="dk-val">${fmtNum(scopedKpis.total_tickets?.value || total)}</div><div class="dk-lbl">Total tickets</div></div>
        <div class="drill-kpi"><div class="dk-val" style="color:#8a3520">${fmtPct(scopedKpis.repair_field_visit_rate?.value || (total > 0 ? repair / total : 0))}</div><div class="dk-lbl">Repair visit %</div></div>
        <div class="drill-kpi"><div class="dk-val" style="color:#7a5218">${fmtPct(scopedKpis.installation_field_visit_rate?.value || (total > 0 ? install / total : 0))}</div><div class="dk-lbl">Installation %</div></div>
        <div class="drill-kpi"><div class="dk-val" style="color:#2a6b50">${fmtPct(scopedKpis.bot_deflection_rate?.value || (total > 0 ? botN / total : 0))}</div><div class="dk-lbl">Bot resolved %</div></div>
      </div>
      ${diff !== null ? `<div class="drill-vs">vs period average: <strong style="color:${diffColor}">${diff > 0 ? "+" : ""}${diff}%</strong> tickets &nbsp;(period avg: ${fmtNum(avgT)} / ${isWeekly ? "week" : "day"})</div>` : ""}
      ${isWeekly && points.length > 1 ? `
        <div class="drill-sub-title">Within-period pattern</div>
        <svg viewBox="0 0 ${W2} ${H2}" style="width:100%;height:${H2}px;max-width:420px">${dailyBars}</svg>` : ""}
      <div class="drawer-two-col">
        <div>
          <div class="drill-sub-title">Product concentration</div>
          ${buildBreakdownRows(scopedProducts, { empty: "No product mix available." })}
        </div>
        <div>
          <div class="drill-sub-title">Category concentration</div>
          <div data-role="period-category-cloud">${buildCategoryCloud(categories, selectedCategory)}</div>
        </div>
      </div>
      <div>
        <div class="drill-sub-title" data-role="period-fc2-title">${selectedCategory ? `${esc(selectedCategory)} - FC2 split` : "FC2 split"}</div>
        <div data-role="period-fc2-detail">${buildFc2SplitRows((periodPayload.fc2_by_category || {})[selectedCategory] || [])}</div>
      </div>
    `;
    renderPeriodCategoryDetail(periodPayload, selectedCategory);
    wireDrawerActions();
  } catch (err) {
    els.drawerContent.innerHTML = `<div class="error-state">Failed to load period breakdown: ${esc(err.message)}</div>`;
  }
}

function fmtNum(v) { return new Intl.NumberFormat("en-IN").format(Number(v || 0)); }
function fmtPct(v) { return `${(Number(v || 0) * 100).toFixed(1)}%`; }
function cleanCopy(v) {
  return String(v ?? "")
    .replace(/â€”|—/g, " - ")
    .replace(/â€“|–/g, " - ")
    .replace(/â†’|→/g, " -> ")
    .replace(/â€¦|…/g, "...")
    .replace(/Â·/g, " · ")
    .replace(/Ã·/g, "/")
    .replace(/\s+/g, " ")
    .trim();
}
function esc(v) {
  return String(v ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}
function empty(msg) { return `<div class="empty-state">${esc(msg)}</div>`; }
