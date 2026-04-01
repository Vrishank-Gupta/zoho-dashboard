const state = {
  apiBaseUrl: (window.QUBO_APP_CONFIG?.apiBaseUrl || window.location.origin || "").replace(/\/$/, ""),
  filters: {
    date_preset: "60d",
    product: "All",
    department: "All",
    issue: "All",
  },
  issueView: "biggest_burden",
};

const els = {
  headline: document.getElementById("headline"),
  summary: document.getElementById("summary"),
  sourceBadge: document.getElementById("sourceBadge"),
  lastUpdated: document.getElementById("lastUpdated"),
  runPipelineBtn: document.getElementById("runPipelineBtn"),
  refreshPipelineBtn: document.getElementById("refreshPipelineBtn"),
  sidebarNote: document.getElementById("sidebarNote"),
  kpiGrid: document.getElementById("kpiGrid"),
  timelineChart: document.getElementById("timelineChart"),
  watchlist: document.getElementById("watchlist"),
  fieldVisitSummary: document.getElementById("fieldVisitSummary"),
  productMatrix: document.getElementById("productMatrix"),
  issueBoard: document.getElementById("issueBoard"),
  actionQueue: document.getElementById("actionQueue"),
  botKpis: document.getElementById("botKpis"),
  botProductMatrix: document.getElementById("botProductMatrix"),
  botBestIssues: document.getElementById("botBestIssues"),
  botLeakyIssues: document.getElementById("botLeakyIssues"),
  fieldSplit: document.getElementById("fieldSplit"),
  departmentMix: document.getElementById("departmentMix"),
  channelMix: document.getElementById("channelMix"),
  botOutcomes: document.getElementById("botOutcomes"),
  qualityCards: document.getElementById("qualityCards"),
  sourceNotes: document.getElementById("sourceNotes"),
  pipelineHealth: document.getElementById("pipelineHealth"),
  issueTabs: document.getElementById("issueTabs"),
  drawer: document.getElementById("issueDrawer"),
  drawerBackdrop: document.getElementById("drawerBackdrop"),
  drawerContent: document.getElementById("drawerContent"),
  closeDrawer: document.getElementById("closeDrawer"),
  datePreset: document.getElementById("datePreset"),
  productFilter: document.getElementById("productFilter"),
  departmentFilter: document.getElementById("departmentFilter"),
  issueFilter: document.getElementById("issueFilter"),
  resetFilters: document.getElementById("resetFilters"),
};

bindEvents();
loadDashboard();

function bindEvents() {
  [
    [els.datePreset, "date_preset"],
    [els.productFilter, "product"],
    [els.departmentFilter, "department"],
    [els.issueFilter, "issue"],
  ].forEach(([element, key]) => {
    element.addEventListener("change", () => {
      state.filters[key] = element.value;
      loadDashboard();
    });
  });

  els.resetFilters.addEventListener("click", () => {
    state.filters = {
      date_preset: "60d",
      product: "All",
      department: "All",
      issue: "All",
    };
    state.issueView = "biggest_burden";
    syncControls();
    loadDashboard();
  });
  els.runPipelineBtn.addEventListener("click", triggerPipeline);
  els.refreshPipelineBtn.addEventListener("click", () => refreshPipelineStatus());

  els.issueTabs.addEventListener("click", (event) => {
    const button = event.target.closest("[data-issue-view]");
    if (!button) return;
    state.issueView = button.dataset.issueView;
    els.issueTabs.querySelectorAll(".tab-btn").forEach((item) => item.classList.toggle("active", item === button));
    if (state.payload) {
      renderIssueBoard(state.payload.issue_views || {});
    }
  });

  document.querySelectorAll(".nav-item").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll(".nav-item").forEach((item) => item.classList.remove("active"));
      button.classList.add("active");
      document.getElementById(button.dataset.target)?.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  });

  els.drawerBackdrop.addEventListener("click", closeDrawer);
  els.closeDrawer.addEventListener("click", closeDrawer);
}

async function loadDashboard() {
  renderLoading();
  const params = new URLSearchParams({
    ...Object.fromEntries(Object.entries(state.filters).map(([key, value]) => [key, String(value)])),
  });
  try {
    const response = await fetch(`${apiUrl("/api/dashboard")}?${params.toString()}`);
    if (!response.ok) throw new Error(`Dashboard API returned ${response.status}`);
    const payload = await response.json();
    state.payload = payload;
    hydrateFilters(payload.filter_options || {});
    renderDashboard(payload);
    refreshPipelineStatus({ silent: true });
  } catch (error) {
    renderError(error);
  }
}

function renderLoading() {
  const loading = placeholder("Loading dashboard");
  [
    els.kpiGrid,
    els.timelineChart,
    els.watchlist,
    els.fieldVisitSummary,
    els.productMatrix,
    els.issueBoard,
    els.actionQueue,
    els.botKpis,
    els.botProductMatrix,
    els.botBestIssues,
    els.botLeakyIssues,
    els.fieldSplit,
    els.departmentMix,
    els.channelMix,
    els.botOutcomes,
    els.qualityCards,
    els.sourceNotes,
    els.pipelineHealth,
  ].forEach((el) => { el.innerHTML = loading; });
}

function renderDashboard(payload) {
  const summary = payload.executive_summary || {};
  const clean = payload.cleaning_summary || {};
  const pipeline = payload.pipeline_health || {};
  const bot = payload.bot_summary || {};
  const topProduct = (payload.product_health || [])[0];
  const topRepair = (payload.issue_views?.repair_heavy || [])[0];
  const topLeak = (payload.issue_views?.agent_leakage || [])[0];

  els.headline.textContent = summary.headline || "Support health";
  els.summary.textContent = summary.summary || "";
  els.sourceBadge.textContent = payload.meta?.warehouse_mode ? "Warehouse live" : "Sample mode";
  els.lastUpdated.textContent = `Last pipeline run: ${pipeline.last_run_at || "Unknown"}`;
  els.sidebarNote.innerHTML = `
    <div class="eyebrow">Weekly readout</div>
    <h3>${topProduct ? topProduct.product_family : "Support"} is carrying the largest support burden.</h3>
    <p>${topRepair ? `${topRepair.fault_code_level_2} is the main repair pressure point.` : "Repair visits remain the main severity signal."}${topLeak ? ` ${topLeak.fault_code_level_2} is leaking heavily from bot to agent.` : ""}</p>
  `;

  renderKpis(payload.kpis || {});
  renderTimeline(payload.timeline || []);
  renderWatchlist(payload);
  renderFieldVisits(payload);
  renderProductMatrix(payload.product_health || []);
  renderIssueBoard(payload.issue_views || {});
  renderActionQueue(payload.action_queue || []);
  renderBotSection(bot);
  renderMetricList(els.fieldSplit, payload.service_ops?.field_service_split || []);
  renderMetricList(els.departmentMix, payload.service_ops?.department_mix || []);
  renderMetricList(els.channelMix, payload.service_ops?.channel_mix || []);
  renderMetricList(els.botOutcomes, payload.service_ops?.bot_outcomes || []);
  renderQuality(clean);
  renderSourceNotes(clean, payload.meta || {});
  renderPipelineHealth(pipeline, payload.meta || {});
}

function renderKpis(kpis) {
  const cards = [
    ["total_tickets", "Ticket volume", "count", false, "Total ticket load in the selected window after the active filter set is applied."],
    ["repair_field_visit_rate", "Repair visit rate", "percent", true, "Share of tickets that convert into Field Service for repair work. This is the main cost and severity signal."],
    ["installation_field_visit_rate", "Installation visit rate", "percent", false, "Share of tickets that convert into Field Service for installation support. Operationally relevant, but not a product-failure signal."],
    ["bot_deflection_rate", "Bot resolved", "percent", false, "Share of tickets closed by the bot without a handoff to human support."],
    ["fcr", "FCR", "percent", false, "First contact resolution for Call Center tickets where reopen count is reliably zero."],
    ["repeat_rate", "Repeat rate", "percent", true, "Share of tickets that recur on the same device and issue pattern within 30 days."],
  ];
  els.kpiGrid.innerHTML = cards.map(([key, label, type, badWhenUp, tip]) => {
    const metric = kpis[key] || { value: 0, change: 0 };
    const improving = badWhenUp ? metric.change < 0 : metric.change >= 0;
    return `
      <article class="kpi-card">
        <div class="title-row compact"><div class="kpi-label">${label}</div>${infoDot(tip)}</div>
        <div class="kpi-value">${type === "count" ? formatNumber(metric.value) : formatPercent(metric.value)}</div>
        <div class="delta ${improving ? "good" : "bad"}">${formatDelta(metric.change)}</div>
        <div class="subtle">vs prior period</div>
      </article>
    `;
  }).join("");
}

function renderTimeline(points) {
  if (!points.length) {
    els.timelineChart.innerHTML = emptyState("No timeline data.");
    return;
  }
  const width = 880;
  const height = 240;
  const pad = 18;
  const lines = [
    ["tickets", "#2167e8"],
    ["repair_field", "#d94a3b"],
    ["bot_resolved", "#189467"],
  ];
  const max = Math.max(...points.flatMap((point) => lines.map(([key]) => Number(point[key] || 0))), 1);
  const step = (width - pad * 2) / Math.max(points.length - 1, 1);
  const toY = (value) => height - pad - (value / max) * (height - pad * 2);
  const paths = lines.map(([key, color]) => {
    const d = points.map((point, index) => `${index === 0 ? "M" : "L"} ${pad + step * index} ${toY(Number(point[key] || 0))}`).join(" ");
    return `<path d="${d}" fill="none" stroke="${color}" stroke-width="3.2" stroke-linecap="round"></path>`;
  }).join("");
  els.timelineChart.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" style="width:100%;height:260px">${paths}</svg>
    <div class="chip-row">
      <span class="chip">Tickets</span>
      <span class="chip bad">Repair visits</span>
      <span class="chip good">Bot resolved</span>
    </div>
  `;
}

function renderWatchlist(payload) {
  const topProduct = (payload.product_health || [])[0];
  const repairIssue = (payload.issue_views?.repair_heavy || [])[0];
  const risingIssue = (payload.issue_views?.rising || [])[0];
  const bot = payload.bot_summary?.overview || {};
  const cards = [
    topProduct ? {
      title: "Largest product burden",
      detail: `${topProduct.product_family} has ${formatNumber(topProduct.ticket_volume)} tickets. ${formatPercent(topProduct.repair_field_visit_rate)} of this product's tickets convert into repair visits.`,
    } : null,
    repairIssue ? {
      title: "Highest repair-weighted issue",
      detail: `${repairIssue.product_family} - ${repairIssue.fault_code_level_2}. ${formatPercent(repairIssue.repair_field_visit_rate)} of tickets for this issue convert into repair visits.`,
      issueId: repairIssue.issue_id,
    } : null,
    risingIssue ? {
      title: "Fastest rising material issue",
      detail: `${risingIssue.product_family} - ${risingIssue.fault_code_level_2} is up from ${formatNumber(risingIssue.previous_volume)} to ${formatNumber(risingIssue.volume)}.`,
      issueId: risingIssue.issue_id,
    } : null,
    {
      title: "Bot abandonment",
      detail: `${formatPercent(bot.blank_chat_rate || 0)} of chat sessions end as blank chat. ${formatPercent(bot.blank_chat_return_rate || 0)} of abandoned users return within 7 days.`,
    },
  ].filter(Boolean);

  els.watchlist.innerHTML = cards.map((card) => `
    <article class="watch-card ${card.issueId ? "js-issue" : ""}" ${card.issueId ? `data-issue-id="${card.issueId}"` : ""}>
      <div>
        <div class="watch-title">${card.title}</div>
        <p class="subtle">${card.detail}</p>
      </div>
    </article>
  `).join("");
  bindIssueClicks(els.watchlist);
}

function renderFieldVisits(payload) {
  const totalTickets = Number(payload.kpis?.total_tickets?.value || 0);
  const repairRate = Number(payload.kpis?.repair_field_visit_rate?.value || 0);
  const installRate = Number(payload.kpis?.installation_field_visit_rate?.value || 0);
  const totalFieldRate = repairRate + installRate;
  const split = payload.service_ops?.field_service_split || [];
  const repairSplit = split.find((item) => (item.label || "").toLowerCase().includes("repair")) || { count: 0, share: 0 };
  const installSplit = split.find((item) => (item.label || "").toLowerCase().includes("installation")) || { count: 0, share: 0 };
  const totalFieldCount = Number(repairSplit.count || 0) + Number(installSplit.count || 0);

  els.fieldVisitSummary.innerHTML = `
    <article class="field-card">
      <div class="label">Total field load</div>
      <div class="value">${formatPercent(totalFieldRate)}</div>
      <div class="detail">${formatNumber(totalFieldCount)} field visits in the selected view. This is the share of total tickets that move to Field Service.</div>
    </article>
    <article class="field-card">
      <div class="label">Repair visits</div>
      <div class="value">${formatPercent(repairRate)}</div>
      <div class="detail">${formatNumber(repairSplit.count || 0)} repair visits. ${formatPercent(repairSplit.share || 0)} of all field visits, and ${formatPercent(repairRate)} of total tickets.</div>
    </article>
    <article class="field-card">
      <div class="label">Installation visits</div>
      <div class="value">${formatPercent(installRate)}</div>
      <div class="detail">${formatNumber(installSplit.count || 0)} installation visits. ${formatPercent(installSplit.share || 0)} of all field visits, and ${formatPercent(installRate)} of total tickets.</div>
    </article>
  `;
}

function renderProductMatrix(products) {
  if (!products.length) {
    els.productMatrix.innerHTML = emptyState("No product data.");
    return;
  }
  const maxVolume = Math.max(...products.map((item) => item.ticket_volume), 1);
  els.productMatrix.innerHTML = `
    <div class="matrix-header">
      <div class="matrix-main">Product family</div>
      <div class="matrix-metrics">
        <div>Volume</div>
        <div>Repair rate</div>
        <div>Repeat</div>
        <div>Bot resolved</div>
        <div>Top issue</div>
      </div>
    </div>
    ${products.map((item) => `
      <div class="matrix-row">
        <div class="matrix-main">
          <div class="watch-title">${item.product_family}</div>
        <div class="subtle">Service burden ${formatNumber(item.service_burden)}</div>
        <div class="bar-track"><div class="bar-fill" style="width:${(item.ticket_volume / maxVolume) * 100}%"></div></div>
        </div>
        <div class="matrix-metrics">
          <div class="matrix-metric"><div class="label">Tickets</div><div class="value">${formatNumber(item.ticket_volume)}</div></div>
          <div class="matrix-metric"><div class="label">Repair</div><div class="value ${item.repair_field_visit_rate >= 0.12 ? "bad" : ""}">${formatPercent(item.repair_field_visit_rate)}</div></div>
          <div class="matrix-metric"><div class="label">Repeat</div><div class="value ${item.repeat_rate >= 0.2 ? "warn" : ""}">${formatPercent(item.repeat_rate)}</div></div>
          <div class="matrix-metric"><div class="label">Bot</div><div class="value">${formatPercent(item.bot_deflection_rate)}</div></div>
          <div class="matrix-metric"><div class="label">Top issue</div><div class="value">${escapeHtml(item.top_issue)}</div></div>
        </div>
      </div>
    `).join("")}
  `;
}

function renderIssueBoard(issueViews) {
  const issues = issueViews[state.issueView] || [];
  if (!issues.length) {
    els.issueBoard.innerHTML = emptyState("No issues for this view.");
    return;
  }
  els.issueBoard.innerHTML = issues.map((issue) => `
    <article class="issue-card" data-issue-id="${issue.issue_id}">
      <div>
        <div class="issue-title">${issue.fault_code_level_2}</div>
        <div class="issue-subtitle">${issue.product_family} - ${issue.fault_code} / ${issue.fault_code_level_1}</div>
      </div>
      <div class="chip-row">
        <span class="chip">${formatNumber(issue.volume)} recent</span>
        <span class="chip bad">Repair ${formatPercent(issue.repair_field_visit_rate)}</span>
        <span class="chip warn">Repeat ${formatPercent(issue.repeat_rate)}</span>
        ${issue.bot_transfer_rate ? `<span class="chip warn">Transfer ${formatPercent(issue.bot_transfer_rate)}</span>` : ""}
      </div>
      <p class="subtle">${issue.insight}</p>
      <div class="issue-foot">
        <span>Prior period ${formatNumber(issue.previous_volume)}</span>
        <span>Ticket evidence</span>
      </div>
    </article>
  `).join("");
  bindIssueClicks(els.issueBoard);
}

function renderActionQueue(actions) {
  if (!actions.length) {
    els.actionQueue.innerHTML = emptyState("No actions.");
    return;
  }
  els.actionQueue.innerHTML = actions.map((item) => `
    <article class="watch-card js-issue" data-issue-id="${item.issue_id}">
      <div>
        <div class="watch-title">${item.title}</div>
        <p class="subtle">${item.detail}</p>
      </div>
    </article>
  `).join("");
  bindIssueClicks(els.actionQueue);
}

function renderBotSection(bot) {
  const overview = bot.overview || {};
  els.botKpis.innerHTML = [
    ["Bot resolved", formatPercent(overview.bot_resolved_rate || 0), `${formatNumber(overview.bot_resolved_tickets || 0)} sessions`, "Sessions fully handled by the bot without human intervention."],
    ["Agent transfer", formatPercent(overview.bot_transferred_rate || 0), `${formatNumber(overview.bot_transferred_tickets || 0)} sessions`, "Sessions that start in bot but move to an agent, indicating automation leakage."],
    ["Blank chat", formatPercent(overview.blank_chat_rate || 0), `${formatNumber(overview.blank_chat_tickets || 0)} sessions`, "Sessions abandoned after starting the bot journey."],
    ["Return after blank", formatPercent(overview.blank_chat_return_rate || 0), `${formatNumber(overview.blank_chat_returned_7d || 0)} returns`, "Share of abandoned bot users who return within 7 days on any tracked support journey."],
    ["Recovered in bot", formatPercent(overview.blank_chat_recovery_rate || 0), `${formatNumber(overview.blank_chat_resolved_7d || 0)} recoveries`, "Share of abandoned bot users who later return and end up bot-resolved within 7 days."],
  ].map(([label, value, detail, tip]) => `
    <article class="bot-kpi">
      <div class="title-row compact"><div class="kpi-label">${label}</div>${infoDot(tip)}</div>
      <div class="value">${value}</div>
      <div class="subtle">${detail}</div>
    </article>
  `).join("");

  renderBotProductMatrix(bot.by_product || []);
  renderIssueList(els.botBestIssues, bot.best_issues || [], "No strong bot-win issues.");
  renderIssueList(els.botLeakyIssues, bot.leaky_issues || [], "No bot leakage issues.");
}

function renderBotProductMatrix(rows) {
  if (!rows.length) {
    els.botProductMatrix.innerHTML = emptyState("No bot product data.");
    return;
  }
  const maxVolume = Math.max(...rows.map((item) => item.chat_tickets), 1);
  els.botProductMatrix.innerHTML = `
    <div class="matrix-header">
      <div class="matrix-main">Product</div>
      <div class="matrix-metrics">
        <div>Chat</div>
        <div>Resolved</div>
        <div>Transferred</div>
        <div>Blank</div>
        <div>Blank return</div>
      </div>
    </div>
    ${rows.map((item) => `
      <div class="matrix-row">
        <div class="matrix-main">
          <div class="watch-title">${item.product_family}</div>
          <div class="bar-track"><div class="bar-fill" style="width:${(item.chat_tickets / maxVolume) * 100}%"></div></div>
        </div>
        <div class="matrix-metrics">
          <div class="matrix-metric"><div class="label">Chat</div><div class="value">${formatNumber(item.chat_tickets)}</div></div>
          <div class="matrix-metric"><div class="label">Resolved</div><div class="value good">${formatPercent(item.bot_resolved_rate)}</div></div>
          <div class="matrix-metric"><div class="label">Transferred</div><div class="value ${item.bot_transferred_rate >= 0.45 ? "bad" : ""}">${formatPercent(item.bot_transferred_rate)}</div></div>
          <div class="matrix-metric"><div class="label">Blank</div><div class="value ${item.blank_chat_rate >= 0.2 ? "warn" : ""}">${formatPercent(item.blank_chat_rate)}</div></div>
          <div class="matrix-metric"><div class="label">Return</div><div class="value">${formatPercent(item.blank_chat_return_rate)}</div></div>
        </div>
      </div>
    `).join("")}
  `;
}

function renderIssueList(container, issues, emptyLabel) {
  if (!issues.length) {
    container.innerHTML = emptyState(emptyLabel);
    return;
  }
  container.innerHTML = issues.slice(0, 5).map((issue) => `
    <article class="watch-card js-issue" data-issue-id="${issue.issue_id}">
      <div>
        <div class="watch-title">${issue.product_family} - ${issue.fault_code_level_2}</div>
        <p class="subtle">Bot resolved ${formatPercent(issue.bot_deflection_rate)} - transferred ${formatPercent(issue.bot_transfer_rate || 0)}</p>
      </div>
    </article>
  `).join("");
  bindIssueClicks(container);
}

function renderMetricList(container, items) {
  if (!items.length) {
    container.innerHTML = emptyState("No data.");
    return;
  }
  const max = Math.max(...items.map((item) => Number(item.count || 0)), 1);
  container.innerHTML = items.map((item) => `
    <div class="metric-row">
      <div style="flex:1">
        <div class="issue-subtitle">${escapeHtml(item.label)}</div>
        <div class="bar-track"><div class="bar-fill" style="width:${(Number(item.count || 0) / max) * 100}%"></div></div>
      </div>
      <div style="min-width:90px;text-align:right">
        <div class="watch-title">${formatNumber(item.count || 0)}</div>
        <div class="subtle">${formatPercent(item.share || 0)}</div>
      </div>
    </div>
  `).join("");
}

function renderQuality(clean) {
  els.qualityCards.innerHTML = [
    ["Issue coverage", formatPercent(clean.actionable_issue_rate || 0), `${formatNumber(clean.actionable_issue_tickets || 0)} tickets are fully classifiable for issue analysis.`],
    ["Blank FC level 1", formatNumber(clean.blank_fault_code_l1_tickets || 0), "Tickets where the middle issue level is still missing."],
    ["Missing FC outside bot", formatNumber(clean.missing_issue_outside_bot_tickets || 0), "Non-chat journeys without issue coding."],
    ["Blank chat sessions", formatNumber(clean.dropped_in_bot_tickets || 0), "Tracked in the bot section as abandonment."],
    ["Email department remapped", formatNumber(clean.email_department_reassigned_tickets || 0), "Email department rows are counted under Call Center and Email channel."],
  ].map(([label, value, detail]) => `
    <article class="quality-card">
      <h4>${label}</h4>
      <div class="big">${value}</div>
      <p class="subtle">${detail}</p>
    </article>
  `).join("");
}

function renderSourceNotes(clean, meta) {
  const cards = [
    {
      title: "Department remap",
      detail: "Hero Electronix and Email department rows are rolled into Call Center before analytics are computed.",
    },
    {
      title: "Channel cleanup",
      detail: "Chat, WhatsApp, Whats App, and Bot are folded into Chat. Web and all unexpected channels are grouped into Others.",
    },
    {
      title: "Fault-code hierarchy",
      detail: `${formatNumber(clean.blank_fault_code_tickets || 0)} rows miss level 0, ${formatNumber(clean.blank_fault_code_l1_tickets || 0)} miss level 1, and ${formatNumber(clean.blank_fault_code_l2_tickets || 0)} miss level 2 in the current filtered view.`,
    },
    {
      title: "Firmware source gap",
      detail: "Software version is not available in the current source table, so version-specific analytics are intentionally suppressed in the UI.",
    },
  ];
  els.sourceNotes.innerHTML = cards.map((card) => `
    <article class="version-card">
      <div class="watch-title">${escapeHtml(card.title)}</div>
      <p class="subtle">${escapeHtml(card.detail)}</p>
    </article>
  `).join("");
}

function renderPipelineHealth(pipeline, meta) {
  els.pipelineHealth.innerHTML = `
    <div class="pipeline-summary">
      <div class="watch-title">${escapeHtml(pipeline.status || "Unknown")}</div>
      <p class="subtle">Latest analytics refresh for the ClickHouse-backed dashboard dataset.</p>
      <div class="chip-row">
        <span class="chip">Last run ${escapeHtml(pipeline.last_run_at || "Unknown")}</span>
        <span class="chip">${escapeHtml(String(pipeline.duration_minutes || 0))} min</span>
        <span class="chip">${escapeHtml(state.apiBaseUrl)}</span>
      </div>
    </div>
    ${(pipeline.tables || []).map((table) => `
      <div class="pipeline-row">
        <div class="watch-title">${escapeHtml(table.table)}</div>
        <div class="subtle">${escapeHtml(table.status)}</div>
      </div>
    `).join("")}
  `;
}

async function refreshPipelineStatus(options = {}) {
  try {
    const response = await fetch(apiUrl("/api/pipeline/status"));
    if (!response.ok) throw new Error(`Pipeline status returned ${response.status}`);
    const payload = await response.json();
    renderPipelineStatus(payload);
  } catch (error) {
    if (!options.silent) {
      els.lastUpdated.textContent = `Pipeline status unavailable: ${error.message}`;
    }
  }
}

function renderPipelineStatus(status) {
  if (status.running) {
    els.sourceBadge.textContent = "Pipeline running";
    els.runPipelineBtn.disabled = true;
    els.runPipelineBtn.textContent = "Pipeline running";
    els.lastUpdated.textContent = `Started ${status.last_started_at || "Unknown"} by ${status.requested_by || "Unknown"}`;
    return;
  }

  els.runPipelineBtn.disabled = false;
  els.runPipelineBtn.textContent = "Run pipeline";

  if (status.last_status === "Failed") {
    els.sourceBadge.textContent = "Pipeline failed";
    els.lastUpdated.textContent = status.last_message || "Pipeline failed";
  }
}

async function triggerPipeline() {
  els.runPipelineBtn.disabled = true;
  els.runPipelineBtn.textContent = "Starting...";
  try {
    const response = await fetch(apiUrl("/api/pipeline/run"), { method: "POST" });
    if (!response.ok) throw new Error(`Pipeline trigger returned ${response.status}`);
    const payload = await response.json();
    renderPipelineStatus(payload.status || {});
    window.setTimeout(() => refreshPipelineStatus({ silent: true }), 1500);
  } catch (error) {
    els.runPipelineBtn.disabled = false;
    els.runPipelineBtn.textContent = "Run pipeline";
    els.lastUpdated.textContent = error.message;
  }
}

async function openIssue(issueId) {
  els.drawer.classList.remove("hidden");
  els.drawerContent.innerHTML = placeholder("Loading ticket evidence");
  const params = new URLSearchParams({
    ...Object.fromEntries(Object.entries(state.filters).map(([key, value]) => [key, String(value)])),
  });
  try {
    const response = await fetch(`${apiUrl(`/api/issues/${encodeURIComponent(issueId)}`)}?${params.toString()}`);
    if (!response.ok) throw new Error(`Issue API returned ${response.status}`);
    const payload = await response.json();
    renderDrawer(payload);
  } catch (error) {
    els.drawerContent.innerHTML = errorState(error.message);
  }
}

function renderDrawer(payload) {
  if (!payload.issue) {
    els.drawerContent.innerHTML = emptyState("No ticket evidence found under the current filter state.");
    return;
  }
  const issue = payload.issue;
  const tickets = payload.tickets || [];
  els.drawerContent.innerHTML = `
    <section class="panel" style="padding:18px">
      <div class="eyebrow">Issue evidence</div>
      <h3>${escapeHtml(issue.fault_code_level_2)}</h3>
      <p class="subtle">${escapeHtml(issue.product_family)} - ${escapeHtml(issue.fault_code)} / ${escapeHtml(issue.fault_code_level_1 || "Unclassified")}</p>
      <div class="chip-row">
        <span class="chip bad">Repair ${formatPercent(issue.repair_field_visit_rate)}</span>
        <span class="chip warn">Repeat ${formatPercent(issue.repeat_rate)}</span>
        <span class="chip">Bot resolved ${formatPercent(issue.bot_deflection_rate)}</span>
        <span class="chip warn">Transferred ${formatPercent(issue.bot_transfer_rate || 0)}</span>
      </div>
      <p class="subtle">${escapeHtml(issue.insight)}</p>
    </section>
    <section class="panel" style="padding:18px">
      <div class="eyebrow">Recent tickets</div>
      ${tickets.length ? `
        <table class="ticket-table">
          <thead>
            <tr>
              <th>Ticket</th>
              <th>Created</th>
              <th>Product</th>
              <th>Department</th>
              <th>Channel</th>
              <th>Status</th>
              <th>Resolution</th>
              <th>Evidence</th>
            </tr>
          </thead>
          <tbody>
            ${tickets.map((ticket) => `
              <tr>
                <td>${escapeHtml(ticket.ticket_id)}</td>
                <td>${escapeHtml(String(ticket.created_at || "").slice(0, 10))}</td>
                <td>${escapeHtml(ticket.product || ticket.product_family || "Unknown")}</td>
                <td>${escapeHtml(ticket.department || "Unknown")}</td>
                <td>${escapeHtml(ticket.channel || "Unknown")}</td>
                <td>${escapeHtml(ticket.status || "Unknown")}</td>
                <td>${escapeHtml(ticket.resolution || "Unknown")}</td>
                <td>
                  <div><strong>FC L1:</strong> ${escapeHtml(ticket.fault_code_level_1 || "Unknown")}</div>
                  <div><strong>Symptom:</strong> ${escapeHtml(ticket.symptom || "Unknown")}</div>
                  <div><strong>Defect:</strong> ${escapeHtml(ticket.defect || "Unknown")}</div>
                  <div><strong>Repair:</strong> ${escapeHtml(ticket.repair || "Unknown")}</div>
                </td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      ` : emptyState("No ticket evidence returned.")}
    </section>
  `;
}

function bindIssueClicks(container) {
  container.querySelectorAll("[data-issue-id]").forEach((node) => {
    node.addEventListener("click", () => openIssue(node.dataset.issueId));
  });
}

function closeDrawer() {
  els.drawer.classList.add("hidden");
}

function hydrateFilters(options) {
  setOptions(els.productFilter, ["All", ...(options.products || [])], state.filters.product);
  setOptions(els.departmentFilter, ["All", ...(options.departments || [])], state.filters.department);
  setOptions(els.issueFilter, ["All", ...(options.issues || [])], state.filters.issue);
  syncControls();
}

function syncControls() {
  els.datePreset.value = state.filters.date_preset;
  els.productFilter.value = state.filters.product;
  els.departmentFilter.value = state.filters.department;
  els.issueFilter.value = state.filters.issue;
  els.issueTabs.querySelectorAll(".tab-btn").forEach((button) => button.classList.toggle("active", button.dataset.issueView === state.issueView));
}

function setOptions(select, values, current) {
  select.innerHTML = values.map((value) => `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`).join("");
  if (!values.includes(current)) current = "All";
  select.value = current;
}

function renderError(error) {
  const message = error?.message || "Dashboard failed to load.";
  els.headline.textContent = "Dashboard unavailable";
  els.summary.textContent = "The API returned an error before the dashboard could render.";
  els.sourceBadge.textContent = "Load failed";
  els.lastUpdated.textContent = message;
  [els.kpiGrid, els.timelineChart, els.watchlist, els.productMatrix, els.issueBoard, els.actionQueue, els.botKpis, els.botProductMatrix, els.botBestIssues, els.botLeakyIssues, els.fieldSplit, els.departmentMix, els.channelMix, els.botOutcomes, els.qualityCards, els.sourceNotes, els.pipelineHealth].forEach((el) => { el.innerHTML = errorState(message); });
  els.fieldVisitSummary.innerHTML = errorState(message);
}

function apiUrl(path) {
  return `${state.apiBaseUrl}${path}`;
}

function placeholder(text) {
  return `<div class="empty-state">${escapeHtml(text)}...</div>`;
}

function emptyState(text) {
  return `<div class="empty-state">${escapeHtml(text)}</div>`;
}

function errorState(text) {
  return `<div class="error-state">${escapeHtml(text)}</div>`;
}

function formatNumber(value) {
  return new Intl.NumberFormat("en-IN").format(Number(value || 0));
}

function formatPercent(value) {
  return `${(Number(value || 0) * 100).toFixed(1)}%`;
}

function formatDelta(value) {
  const numeric = Number(value || 0) * 100;
  const sign = numeric > 0 ? "+" : "";
  return `${sign}${numeric.toFixed(1)}%`;
}

function infoDot(text) {
  return `<button class="info-dot" type="button" data-tip="${escapeHtml(text)}">i</button>`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
