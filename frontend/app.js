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
  {
    key: "tickets",
    label: "Tickets",
    format: "count",
    help: "Total tickets in the selected view.",
  },
  {
    key: "installation_tickets",
    label: "Installation Tickets",
    format: "count",
    help: "Tickets where FC1 or FC2 contains the substring 'instal'.",
  },
  {
    key: "bot_resolved",
    label: "Bot Resolved",
    format: "count",
    help: "Tickets closed by bot without transfer to a human agent.",
  },
  {
    key: "repeat_tickets",
    label: "Repeat Tickets",
    format: "count",
    help: "Same serial number and same fault code seen again within 30 days.",
  },
  {
    key: "open_tickets",
    label: "Open Tickets",
    format: "count",
    help: "Tickets whose status looks open, escalated, pending, or in progress.",
  },
  {
    key: "no_reopen_rate",
    label: "No-reopen rate",
    format: "percent",
    help: "Share of Call Center tickets where reopen count is zero.",
  },
];

const ISSUE_TAB_TITLES = {
  highest_volume: "Highest volume",
  installation_tickets: "Installation tickets",
  repeat_heavy: "Repeat-heavy",
  bot_leakage: "High transfer",
};

const state = {
  apiBaseUrl: (window.QUBO_APP_CONFIG?.apiBaseUrl || window.location.origin || "").replace(/\/$/, ""),
  filters: { ...DEFAULT_FILTERS },
  issueView: "highest_volume",
  payload: null,
  options: {},
  focusActiveTab: "fc1",
  ruleSearch: {
    include_fc1: "",
    exclude_fc1: "",
    include_fc2: "",
    exclude_fc2: "",
    include_bot_action: "",
    exclude_bot_action: "",
  },
};

const els = {
  headline: document.getElementById("headline"),
  summary: document.getElementById("summary"),
  sourceBadge: document.getElementById("sourceBadge"),
  lastUpdated: document.getElementById("lastUpdated"),
  sidebarNote: document.getElementById("sidebarNote"),
  activeRules: document.getElementById("activeRules"),
  kpiGrid: document.getElementById("kpiGrid"),
  timelineChart: document.getElementById("timelineChart"),
  spotlightCards: document.getElementById("spotlightCards"),
  categoryMatrix: document.getElementById("categoryMatrix"),
  productMatrix: document.getElementById("productMatrix"),
  issueBoard: document.getElementById("issueBoard"),
  botKpis: document.getElementById("botKpis"),
  botProductMatrix: document.getElementById("botProductMatrix"),
  botBestIssues: document.getElementById("botBestIssues"),
  botLeakyIssues: document.getElementById("botLeakyIssues"),
  installationMix: document.getElementById("installationMix"),
  departmentMix: document.getElementById("departmentMix"),
  channelMix: document.getElementById("channelMix"),
  statusMix: document.getElementById("statusMix"),
  qualityCards: document.getElementById("qualityCards"),
  pipelineHealth: document.getElementById("pipelineHealth"),
  datePreset: document.getElementById("datePreset"),
  categoryFilter: document.getElementById("categoryFilter"),
  productFilter: document.getElementById("productFilter"),
  departmentFilter: document.getElementById("departmentFilter"),
  channelFilter: document.getElementById("channelFilter"),
  efcFilter: document.getElementById("efcFilter"),
  statusFilter: document.getElementById("statusFilter"),
  resetFilters: document.getElementById("resetFilters"),
  issueTabs: document.getElementById("issueTabs"),
  qfExcludeInstallation: document.getElementById("qfExcludeInstallation"),
};

bindEvents();
loadDashboard();

function bindEvents() {
  [
    [els.datePreset, "date_preset"],
    [els.categoryFilter, "category"],
    [els.productFilter, "product"],
    [els.departmentFilter, "department"],
    [els.channelFilter, "channel"],
    [els.efcFilter, "efc"],
    [els.statusFilter, "status"],
  ].forEach(([element, key]) => {
    element.addEventListener("change", () => {
      state.filters[key] = element.value;
      loadDashboard();
    });
  });

  els.resetFilters.addEventListener("click", () => {
    state.filters = structuredClone(DEFAULT_FILTERS);
    Object.keys(state.ruleSearch).forEach((key) => {
      state.ruleSearch[key] = "";
    });
    state.issueView = "highest_volume";
    syncControls();
    renderAdvancedFilters();
    renderActiveRules();
    loadDashboard();
  });

  els.qfExcludeInstallation.addEventListener("click", () => {
    toggleExcludeInstallation();
  });

  document.getElementById("focusTabs").addEventListener("click", (event) => {
    const btn = event.target.closest("[data-focus-tab]");
    if (!btn) return;
    state.focusActiveTab = btn.dataset.focusTab;
    document.querySelectorAll("[data-focus-tab]").forEach((b) => {
      b.classList.toggle("active", b.dataset.focusTab === state.focusActiveTab);
    });
    renderAdvancedFilters();
  });

  els.issueTabs.addEventListener("click", (event) => {
    const button = event.target.closest("[data-issue-view]");
    if (!button) return;
    state.issueView = button.dataset.issueView;
    syncControls();
    renderIssueBoard(state.payload?.issue_views || {});
  });
}

async function loadDashboard() {
  renderLoading();
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(state.filters)) {
    if (Array.isArray(value)) {
      value.forEach((item) => params.append(key, item));
    } else {
      params.set(key, String(value));
    }
  }

  try {
    const response = await fetch(`${apiUrl("/api/dashboard")}?${params.toString()}`);
    if (!response.ok) {
      throw new Error(`Dashboard API returned ${response.status}`);
    }
    const payload = await response.json();
    state.payload = payload;
    state.options = payload.filter_options || {};
    hydrateQuickFilters(state.options);
    renderAdvancedFilters();
    renderActiveRules();
    renderDashboard(payload);
  } catch (error) {
    renderError(error);
  }
}

function renderLoading() {
  const loading = placeholder("Loading board");
  [
    els.kpiGrid,
    els.timelineChart,
    els.spotlightCards,
    els.categoryMatrix,
    els.productMatrix,
    els.issueBoard,
    els.botKpis,
    els.botProductMatrix,
    els.botBestIssues,
    els.botLeakyIssues,
    els.installationMix,
    els.departmentMix,
    els.channelMix,
    els.statusMix,
    els.qualityCards,
    els.pipelineHealth,
  ].forEach((element) => {
    element.innerHTML = loading;
  });
}

function renderDashboard(payload) {
  const meta = payload.meta || {};
  const pipeline = payload.pipeline_health || {};
  const categoryRows = payload.category_health || [];
  const issueRows = payload.issue_views?.highest_volume || [];
  const topCategory = categoryRows[0];
  const topIssue = issueRows[0];

  els.headline.textContent = meta.title || "Qubo Support Executive Board";
  els.summary.textContent = meta.subtitle || "Executive view of support volume, issue mix, and bot outcomes.";
  els.sourceBadge.textContent = meta.source_mode === "clickhouse" ? "Live data" : "No data loaded";
  els.lastUpdated.textContent = `Last refresh: ${formatDateTime(pipeline.last_run_at || meta.window_end || "")}`;
  els.sidebarNote.innerHTML = `
    <div class="eyebrow">Current Focus</div>
    <h3>${escapeHtml(topCategory ? `${topCategory.product_category} is leading the current ticket mix.` : "The board will highlight the strongest driver once data is available.")}</h3>
    <p>${escapeHtml(topIssue ? `${topIssue.fault_code_level_2} is the biggest FC2 theme in the current slice.` : "Use the focus rules above to narrow the board by FC1, FC2, or bot action.")}</p>
  `;

  renderKpis(payload.kpis || {});
  renderTimeline(payload.timeline || []);
  renderSpotlight(payload.spotlight || []);
  renderHealthMatrix(els.categoryMatrix, payload.category_health || [], "product_category", false);
  renderHealthMatrix(els.productMatrix, payload.product_health || [], "product_family", true);
  renderIssueBoard(payload.issue_views || {});
  renderBotSummary(payload.bot_summary || {});
  renderMetricList(els.installationMix, payload.service_ops?.installation_mix || []);
  renderMetricList(els.departmentMix, payload.service_ops?.department_mix || []);
  renderMetricList(els.channelMix, payload.service_ops?.channel_mix || []);
  renderMetricList(els.statusMix, payload.service_ops?.status_mix || []);
  renderQuality(payload.data_quality || {});
  renderPipelineHealth(payload.pipeline_health || {});
}

function renderKpis(kpis) {
  els.kpiGrid.innerHTML = KPI_CONFIG.map((item) => {
    const metric = kpis[item.key] || { value: 0, change: 0 };
    return `
      <article class="kpi-card">
        <div class="kpi-label">${escapeHtml(item.label)}</div>
        <div class="kpi-value">${item.format === "percent" ? formatPercent(metric.value) : formatNumber(metric.value)}</div>
        <div class="kpi-help">${escapeHtml(item.help)}</div>
        <div class="delta ${metric.change >= 0 ? "good" : "bad"}">${formatDelta(metric.change)}</div>
        <div class="subtle">vs prior period</div>
      </article>
    `;
  }).join("");
}

function renderTimeline(points) {
  if (!points.length) {
    els.timelineChart.innerHTML = emptyState("No timeline data for the selected filters.");
    return;
  }

  const width = 920;
  const height = 250;
  const padX = 24;
  const padY = 18;
  const series = [
    { key: "tickets", label: "Tickets", color: "#2563eb" },
    { key: "installation_tickets", label: "Installation Tickets", color: "#d68a1b" },
    { key: "bot_resolved_tickets", label: "Bot Resolved", color: "#198754" },
    { key: "repeat_tickets", label: "Repeat Tickets", color: "#6e59cf" },
  ];

  const maxValue = Math.max(
    ...points.flatMap((point) => series.map((line) => Number(point[line.key] || 0))),
    1,
  );
  const step = (width - padX * 2) / Math.max(points.length - 1, 1);
  const toY = (value) => height - padY - (value / maxValue) * (height - padY * 2);

  const gridLines = [0.25, 0.5, 0.75].map((ratio) => {
    const y = padY + ratio * (height - padY * 2);
    return `<line x1="${padX}" y1="${y}" x2="${width - padX}" y2="${y}" stroke="#dde5ef" stroke-width="1" />`;
  }).join("");

  const paths = series.map((line) => {
    const d = points.map((point, index) => {
      const x = padX + step * index;
      const y = toY(Number(point[line.key] || 0));
      return `${index === 0 ? "M" : "L"} ${x} ${y}`;
    }).join(" ");
    return `<path d="${d}" fill="none" stroke="${line.color}" stroke-width="3" stroke-linecap="round" />`;
  }).join("");

  const labels = points.map((point, index) => {
    if (points.length > 7 && index % Math.ceil(points.length / 6) !== 0 && index !== points.length - 1) {
      return "";
    }
    const x = padX + step * index;
    return `<text x="${x}" y="${height - 2}" text-anchor="middle" font-size="11" fill="#6d7b8f">${escapeHtml(shortDate(point.date))}</text>`;
  }).join("");

  els.timelineChart.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" style="width:100%;height:260px">
      ${gridLines}
      ${paths}
      ${labels}
    </svg>
    <div class="timeline-legend">
      <div class="chip-row">
        ${series.map((line) => `<span class="chip" style="background:${hexToSoft(line.color)};color:${line.color}">${escapeHtml(line.label)}</span>`).join("")}
      </div>
    </div>
  `;
}

function renderSpotlight(items) {
  if (!items.length) {
    els.spotlightCards.innerHTML = emptyState("No highlights in the current view.");
    return;
  }

  els.spotlightCards.innerHTML = items.map((item) => `
    <article class="watch-card">
      <div class="watch-title">${escapeHtml(item.title)}</div>
      <p class="subtle">${escapeHtml(item.detail)}</p>
    </article>
  `).join("");
}

function renderHealthMatrix(container, rows, titleKey, showCategory) {
  if (!rows.length) {
    container.innerHTML = emptyState("No data for the selected filters.");
    return;
  }

  const maxTickets = Math.max(...rows.map((row) => Number(row.tickets || 0)), 1);
  container.innerHTML = `
    <div class="matrix-header">
      <div class="matrix-main">${showCategory ? "Product family" : "Category"}</div>
      <div class="matrix-metrics">
        <div>Tickets</div>
        <div>Installation</div>
        <div>Repeat</div>
        <div>Bot Resolved</div>
        <div>Top EFC</div>
      </div>
    </div>
    ${rows.map((row) => `
      <div class="matrix-row">
        <div class="matrix-main">
          <div class="watch-title">${escapeHtml(row[titleKey] || "Other")}</div>
          ${showCategory ? `<div class="subtle">${escapeHtml(row.product_category || "Other")}</div>` : ""}
          <div class="bar-track"><div class="bar-fill" style="width:${((Number(row.tickets || 0) / maxTickets) * 100).toFixed(1)}%"></div></div>
        </div>
        <div class="matrix-metrics">
          <div class="matrix-metric"><div class="label">Tickets</div><div class="value">${formatNumber(row.tickets)}</div></div>
          <div class="matrix-metric"><div class="label">Installation</div><div class="value">${formatPercent(row.installation_rate)}</div></div>
          <div class="matrix-metric"><div class="label">Repeat</div><div class="value">${formatPercent(row.repeat_rate)}</div></div>
          <div class="matrix-metric"><div class="label">Bot Resolved</div><div class="value">${formatPercent(row.bot_resolved_rate)}</div></div>
          <div class="matrix-metric"><div class="label">Top EFC</div><div class="value">${escapeHtml(row.top_efc || "Blank")}</div></div>
        </div>
      </div>
    `).join("")}
  `;
}

function renderIssueBoard(issueViews) {
  const issues = issueViews[state.issueView] || [];
  if (!issues.length) {
    els.issueBoard.innerHTML = emptyState("No issues found for this view.");
    return;
  }

  els.issueBoard.innerHTML = issues.map((issue) => `
    <article class="issue-card">
      <div class="issue-title">${escapeHtml(issue.fault_code_level_2 || "Unclassified")}</div>
      <div class="issue-subtitle">${escapeHtml(issue.product_category || "Other")} / ${escapeHtml(issue.product_family || "Other")} / ${escapeHtml(issue.executive_fault_code || "Blank")}</div>
      <div class="chip-row">
        <span class="chip">${formatNumber(issue.volume)} tickets</span>
        <span class="chip warn">Installation ${formatPercent(issue.installation_rate)}</span>
        <span class="chip">Repeat ${formatPercent(issue.repeat_rate)}</span>
        <span class="chip ${Number(issue.bot_transfer_rate || 0) >= 0.20 ? "bad" : ""}">Transfer ${formatPercent(issue.bot_transfer_rate)}</span>
        <span class="chip good">Bot Resolved ${formatPercent(issue.bot_resolved_rate)}</span>
      </div>
      <p class="subtle">${escapeHtml(issue.insight || "")}</p>
    </article>
  `).join("");
}

function renderBotSummary(botSummary) {
  const overview = botSummary.overview || {};
  els.botKpis.innerHTML = [
    ["Chat journeys", formatNumber(overview.chat_tickets || 0)],
    ["Bot Resolved", formatNumber(overview.bot_resolved_tickets || 0)],
    ["Transferred", formatNumber(overview.bot_transferred_tickets || 0)],
    ["Blank chat", formatNumber(overview.blank_chat_tickets || 0)],
    ["Return in 7d", formatNumber(overview.blank_chat_returned_7d || 0)],
  ].map(([label, value]) => `
    <article class="bot-kpi">
      <div class="kpi-label">${escapeHtml(label)}</div>
      <div class="value">${value}</div>
    </article>
  `).join("");

  renderBotProductMatrix(botSummary.by_product || []);
  renderIssueList(els.botBestIssues, botSummary.best_issues || [], "No high bot-resolved issues.");
  renderIssueList(els.botLeakyIssues, botSummary.leaky_issues || [], "No high-transfer issues.");
}

function renderBotProductMatrix(rows) {
  if (!rows.length) {
    els.botProductMatrix.innerHTML = emptyState("No bot rows in the selected slice.");
    return;
  }

  const maxTickets = Math.max(...rows.map((row) => Number(row.chat_tickets || 0)), 1);
  els.botProductMatrix.innerHTML = `
    <div class="matrix-header">
      <div class="matrix-main">Product</div>
      <div class="matrix-metrics">
        <div>Chat</div>
        <div>Bot Resolved</div>
        <div>Transferred</div>
        <div>Blank chat</div>
        <div>Return</div>
      </div>
    </div>
    ${rows.map((row) => `
      <div class="matrix-row">
        <div class="matrix-main">
          <div class="watch-title">${escapeHtml(row.product_family || "Other")}</div>
          <div class="subtle">${escapeHtml(row.product_category || "Other")}</div>
          <div class="bar-track"><div class="bar-fill" style="width:${((Number(row.chat_tickets || 0) / maxTickets) * 100).toFixed(1)}%"></div></div>
        </div>
        <div class="matrix-metrics">
          <div class="matrix-metric"><div class="label">Chat</div><div class="value">${formatNumber(row.chat_tickets)}</div></div>
          <div class="matrix-metric"><div class="label">Bot Resolved</div><div class="value">${formatPercent(row.bot_resolved_rate)}</div></div>
          <div class="matrix-metric"><div class="label">Transferred</div><div class="value">${formatPercent(row.bot_transferred_rate)}</div></div>
          <div class="matrix-metric"><div class="label">Blank</div><div class="value">${formatPercent(row.blank_chat_rate)}</div></div>
          <div class="matrix-metric"><div class="label">Return</div><div class="value">${formatPercent(row.blank_chat_return_rate)}</div></div>
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
    <article class="watch-card">
      <div class="watch-title">${escapeHtml(issue.product_family || "Other")} - ${escapeHtml(issue.fault_code_level_2 || "Unclassified")}</div>
      <p class="subtle">${escapeHtml(issue.executive_fault_code || "Blank")} | Bot Resolved ${formatPercent(issue.bot_resolved_rate)} | Transfer ${formatPercent(issue.bot_transfer_rate)}</p>
    </article>
  `).join("");
}

function renderMetricList(container, items) {
  if (!items.length) {
    container.innerHTML = emptyState("No rows in this view.");
    return;
  }
  const maxCount = Math.max(...items.map((item) => Number(item.count || 0)), 1);
  container.innerHTML = items.map((item) => `
    <div class="metric-row">
      <div>
        <div class="issue-subtitle">${escapeHtml(item.label || "Unknown")}</div>
        <div class="bar-track"><div class="bar-fill" style="width:${((Number(item.count || 0) / maxCount) * 100).toFixed(1)}%"></div></div>
      </div>
      <div style="min-width:84px;text-align:right">
        <div class="watch-title">${formatNumber(item.count || 0)}</div>
        <div class="subtle">${formatPercent(item.share || 0)}</div>
      </div>
    </div>
  `).join("");
}

function renderQuality(data) {
  const cards = [
    {
      label: "Actionable issue coverage",
      value: formatPercent(data.actionable_issue_rate || 0),
      detail: `${formatNumber(data.actionable_issue_tickets || 0)} tickets remain in issue analysis after coding and installation logic.`,
    },
    {
      label: "Usable issue coverage",
      value: formatPercent(data.usable_issue_rate || 0),
      detail: `${formatNumber(data.usable_issue_tickets || 0)} tickets have enough coding to stay in issue views.`,
    },
    {
      label: "Blank FC1",
      value: formatNumber(data.blank_fault_code_l1_tickets || 0),
      detail: "Tickets with no FC1 coding.",
    },
    {
      label: "Blank FC2",
      value: formatNumber(data.blank_fault_code_l2_tickets || 0),
      detail: "Tickets with no FC2 coding.",
    },
    {
      label: "Missing issue outside chat",
      value: formatNumber(data.missing_issue_outside_bot_tickets || 0),
      detail: "Non-chat tickets missing issue coding.",
    },
    {
      label: "Email remapped",
      value: formatNumber(data.email_department_reassigned_tickets || 0),
      detail: "Rows moved from Email department into Call Center and Email channel.",
    },
  ];

  els.qualityCards.innerHTML = cards.map((card) => `
    <article class="quality-card">
      <h4>${escapeHtml(card.label)}</h4>
      <div class="big">${escapeHtml(card.value)}</div>
      <p class="subtle">${escapeHtml(card.detail)}</p>
    </article>
  `).join("");
}

function renderPipelineHealth(pipeline) {
  const recentRuns = pipeline.recent_runs || [];
  els.pipelineHealth.innerHTML = `
    <div class="pipeline-summary">
      <div class="watch-title">${escapeHtml(pipeline.status || "Unknown")}</div>
      <p class="subtle">Latest analytics refresh status.</p>
      <div class="chip-row">
        <span class="chip">Last run ${escapeHtml(formatDateTime(pipeline.last_run_at || ""))}</span>
        <span class="chip">${formatNumber(pipeline.rows_inserted || 0)} rows inserted</span>
        <span class="chip">${formatNumber(pipeline.duration_minutes || 0)} min</span>
      </div>
    </div>
    ${recentRuns.map((run) => `
      <div class="pipeline-row">
        <div>
          <div class="watch-title">${escapeHtml(run.status || "Unknown")}</div>
          <div class="subtle">${escapeHtml(formatDateTime(run.finished_at || run.started_at || ""))}</div>
        </div>
        <div class="subtle">${formatNumber(run.rows_inserted || 0)} inserted</div>
      </div>
    `).join("")}
  `;
}

function hydrateQuickFilters(options) {
  setSelectOptions(els.categoryFilter, ["All", ...(options.categories || [])], state.filters.category);
  setSelectOptions(els.productFilter, ["All", ...(options.products || [])], state.filters.product);
  setSelectOptions(els.departmentFilter, ["All", ...(options.departments || [])], state.filters.department);
  setSelectOptions(els.channelFilter, ["All", ...(options.channels || [])], state.filters.channel);
  setSelectOptions(els.efcFilter, ["All", ...(options.efcs || [])], state.filters.efc);
  setSelectOptions(els.statusFilter, ["All", ...(options.statuses || [])], state.filters.status);
  syncControls();
}

function renderAdvancedFilters() {
  const TAB_CONFIG = {
    fc1: { title: "FC1", include_key: "include_fc1", exclude_key: "exclude_fc1", options: state.options.fc1 || [] },
    fc2: { title: "FC2", include_key: "include_fc2", exclude_key: "exclude_fc2", options: state.options.fc2 || [] },
    bot_action: { title: "Bot action", include_key: "include_bot_action", exclude_key: "exclude_bot_action", options: state.options.bot_actions || [] },
  };
  const cfg = TAB_CONFIG[state.focusActiveTab] || TAB_CONFIG.fc1;
  const includePane = document.getElementById("focusIncludePane");
  const excludePane = document.getElementById("focusExcludePane");
  if (!includePane || !excludePane) return;
  renderRuleCard(includePane, { title: cfg.title, key: cfg.include_key, opposite: cfg.exclude_key, mode: "include", options: cfg.options });
  renderRuleCard(excludePane, { title: cfg.title, key: cfg.exclude_key, opposite: cfg.include_key, mode: "exclude", options: cfg.options });
}

function renderRuleCard(container, config) {
  const selected = state.filters[config.key];
  const blocked = new Set(state.filters[config.opposite]);
  const search = (state.ruleSearch[config.key] || "").trim().toLowerCase();
  const available = config.options.filter((item) => {
    if (blocked.has(item)) return false;
    if (!search) return true;
    return item.toLowerCase().includes(search);
  });

  container.innerHTML = `
    <div class="rule-card">
      <div class="rule-card-head">
        <div class="rule-title">${escapeHtml(config.title)}</div>
        <button class="rule-clear" type="button" data-clear="${config.key}">Clear</button>
      </div>
      <div class="rule-selection">
        ${selected.length
          ? selected.map((value) => `<span class="rule-chip ${config.mode}">${escapeHtml(value)}</span>`).join("")
          : '<span class="subtle">No selection</span>'}
      </div>
      <input class="rule-search" type="search" placeholder="Search ${escapeHtml(config.title)}" data-rule-search="${config.key}" value="${escapeHtml(state.ruleSearch[config.key] || "")}">
      <div class="rule-option-list">
        ${available.map((value) => `
          <label class="rule-option ${selected.includes(value) ? `active-${config.mode}` : ""}">
            <input type="checkbox" data-rule-key="${config.key}" value="${escapeHtml(value)}" ${selected.includes(value) ? "checked" : ""}>
            <span>${escapeHtml(value)}</span>
          </label>
        `).join("")}
      </div>
    </div>
  `;

  const searchBox = container.querySelector("[data-rule-search]");
  searchBox?.addEventListener("input", () => {
    state.ruleSearch[config.key] = searchBox.value;
    renderAdvancedFilters();
  });

  container.querySelectorAll("[data-rule-key]").forEach((input) => {
    input.addEventListener("change", () => {
      const key = input.dataset.ruleKey;
      const values = new Set(state.filters[key]);
      if (input.checked) {
        values.add(input.value);
      } else {
        values.delete(input.value);
      }
      state.filters[key] = [...values];
      renderAdvancedFilters();
      renderActiveRules();
      loadDashboard();
    });
  });

  const clearButton = container.querySelector("[data-clear]");
  clearButton?.addEventListener("click", () => {
    state.filters[config.key] = [];
    renderAdvancedFilters();
    renderActiveRules();
    loadDashboard();
  });
}

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
    const instalFc1 = (state.options.fc1 || []).filter((v) => /instal/i.test(v));
    const instalFc2 = (state.options.fc2 || []).filter((v) => /instal/i.test(v));
    state.filters.include_fc1 = state.filters.include_fc1.filter((v) => !/instal/i.test(v));
    state.filters.include_fc2 = state.filters.include_fc2.filter((v) => !/instal/i.test(v));
    state.filters.exclude_fc1 = [...new Set([...state.filters.exclude_fc1, ...instalFc1])];
    state.filters.exclude_fc2 = [...new Set([...state.filters.exclude_fc2, ...instalFc2])];
  }
  syncQuickPills();
  renderAdvancedFilters();
  renderActiveRules();
  loadDashboard();
}

function syncQuickPills() {
  els.qfExcludeInstallation.classList.toggle("active", isExcludeInstallationActive());
}

function renderActiveRules() {
  const chips = [];
  const singles = [
    ["date_preset", state.filters.date_preset !== "60d" ? datePresetLabel(state.filters.date_preset) : ""],
    ["category", state.filters.category !== "All" ? `Category: ${state.filters.category}` : ""],
    ["product", state.filters.product !== "All" ? `Product: ${state.filters.product}` : ""],
    ["efc", state.filters.efc !== "All" ? `EFC: ${state.filters.efc}` : ""],
    ["department", state.filters.department !== "All" ? `Team: ${state.filters.department}` : ""],
    ["channel", state.filters.channel !== "All" ? `Channel: ${state.filters.channel}` : ""],
    ["status", state.filters.status !== "All" ? `Status: ${state.filters.status}` : ""],
  ];

  singles.forEach(([, label]) => {
    if (label) chips.push(`<span class="rule-chip">${escapeHtml(label)}</span>`);
  });

  if (isExcludeInstallationActive()) {
    chips.push(`<span class="rule-chip exclude">Excl. Installation</span>`);
  }
  const nonInstallExcludeFc1 = state.filters.exclude_fc1.filter((v) => !/instal/i.test(v));
  const nonInstallExcludeFc2 = state.filters.exclude_fc2.filter((v) => !/instal/i.test(v));

  appendRuleChips(chips, "Include FC1", state.filters.include_fc1, "include");
  appendRuleChips(chips, "Exclude FC1", nonInstallExcludeFc1, "exclude");
  appendRuleChips(chips, "Include FC2", state.filters.include_fc2, "include");
  appendRuleChips(chips, "Exclude FC2", nonInstallExcludeFc2, "exclude");
  appendRuleChips(chips, "Include bot", state.filters.include_bot_action, "include");
  appendRuleChips(chips, "Exclude bot", state.filters.exclude_bot_action, "exclude");

  els.activeRules.innerHTML = chips.length ? chips.join("") : '<span class="subtle">No extra focus rules applied.</span>';
}

function appendRuleChips(target, prefix, values, mode) {
  values.forEach((value) => {
    target.push(`<span class="rule-chip ${mode}">${escapeHtml(`${prefix}: ${value}`)}</span>`);
  });
}

function syncControls() {
  els.datePreset.value = state.filters.date_preset;
  els.categoryFilter.value = state.filters.category;
  els.productFilter.value = state.filters.product;
  els.departmentFilter.value = state.filters.department;
  els.channelFilter.value = state.filters.channel;
  els.efcFilter.value = state.filters.efc;
  els.statusFilter.value = state.filters.status;
  els.issueTabs.querySelectorAll(".tab-btn").forEach((button) => {
    button.classList.toggle("active", button.dataset.issueView === state.issueView);
  });
  syncQuickPills();
}

function setSelectOptions(select, options, selectedValue) {
  select.innerHTML = options.map((value) => `
    <option value="${escapeHtml(value)}">${escapeHtml(value)}</option>
  `).join("");
  select.value = options.includes(selectedValue) ? selectedValue : "All";
}

function renderError(error) {
  const message = error?.message || "Board failed to load.";
  els.headline.textContent = "Board unavailable";
  els.summary.textContent = "The API returned an error before the board could render.";
  els.sourceBadge.textContent = "Load failed";
  els.lastUpdated.textContent = message;
  els.sidebarNote.innerHTML = errorState(message);
  [
    els.kpiGrid,
    els.timelineChart,
    els.spotlightCards,
    els.categoryMatrix,
    els.productMatrix,
    els.issueBoard,
    els.botKpis,
    els.botProductMatrix,
    els.botBestIssues,
    els.botLeakyIssues,
    els.installationMix,
    els.departmentMix,
    els.channelMix,
    els.statusMix,
    els.qualityCards,
    els.pipelineHealth,
  ].forEach((element) => {
    element.innerHTML = errorState(message);
  });
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

function datePresetLabel(value) {
  if (value === "14d") return "Last 14 days";
  if (value === "30d") return "Last 30 days";
  if (value === "history") return "Full history";
  return "Last 60 days";
}

function shortDate(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleDateString("en-IN", { day: "numeric", month: "short" });
}

function formatDateTime(value) {
  if (!value || value === "Unknown") return "Unknown";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString("en-IN", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
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

function hexToSoft(hex) {
  const clean = hex.replace("#", "");
  const bigint = parseInt(clean, 16);
  const r = (bigint >> 16) & 255;
  const g = (bigint >> 8) & 255;
  const b = bigint & 255;
  return `rgba(${r}, ${g}, ${b}, 0.12)`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
