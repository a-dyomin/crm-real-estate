const apiPrefix = document.body.dataset.apiPrefix;
const token = localStorage.getItem("cre_token");

const leadStages = [
  ["new_lead", "Новый лид"],
  ["qualification", "Квалификация"],
  ["no_answer", "Недозвон"],
  ["call_center_tasks", "Задачи КЦ"],
  ["sent_to_commission", "Отправлен на комиссию"],
  ["final_no_answer", "Конечный недозвон"],
  ["deferred_demand", "Отложенный спрос"],
  ["poor_quality_lead", "Некачественный лид"],
  ["high_quality_lead", "Качественный лид"],
];

const leadStatusLabels = Object.fromEntries(leadStages);

const dealStages = [
  ["new", "New"],
  ["negotiation", "Negotiation"],
  ["due_diligence", "Due Diligence"],
  ["closed_won", "Closed Won"],
  ["closed_lost", "Closed Lost"],
];

const state = {
  user: null,
  currentTab: "home",
  dragEntity: null,
  parserSources: [],
  parserResults: {
    page: 1,
    pageSize: 20,
    pages: 1,
    total: 0,
    query: "",
  },
};

if (!token) {
  window.location.href = "/login";
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

async function api(path, options = {}) {
  const headers = {
    ...(options.headers || {}),
    Authorization: `Bearer ${localStorage.getItem("cre_token") || ""}`,
  };
  if (!headers["Content-Type"] && options.body && !(options.body instanceof FormData)) {
    headers["Content-Type"] = "application/json";
  }
  const response = await fetch(`${apiPrefix}${path}`, { ...options, headers });
  if (response.status === 401) {
    localStorage.removeItem("cre_token");
    window.location.href = "/login";
    throw new Error("Unauthorized");
  }
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `HTTP ${response.status}`);
  }
  if (response.status === 204) return null;
  return response.json();
}

function setActiveTab(tabName) {
  state.currentTab = tabName;
  document.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.tab === tabName);
  });
  document.querySelectorAll(".tab").forEach((tab) => {
    tab.classList.toggle("active", tab.id === `tab-${tabName}`);
  });
}

function formatMoney(value) {
  if (value == null) return "-";
  return new Intl.NumberFormat("ru-RU").format(Number(value));
}

function formatContact(item) {
  return [item.contact_name, item.contact_phone, item.contact_email].filter(Boolean).join(" | ") || "-";
}

function formatDateTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("ru-RU");
}

function resolveRecordingUrl(url) {
  if (!url) return "";
  if (url.startsWith("http://") || url.startsWith("https://")) return url;
  if (url.startsWith("/media/")) return url;
  if (url.startsWith("/")) return url;
  return `/${url}`;
}

function buildKanban(containerId, items, stages, kind) {
  const container = document.getElementById(containerId);
  container.innerHTML = "";
  const grouped = Object.fromEntries(stages.map(([key]) => [key, []]));
  for (const item of items) {
    if (!grouped[item.status]) grouped[item.status] = [];
    grouped[item.status].push(item);
  }

  for (const [statusKey, label] of stages) {
    const col = document.createElement("div");
    col.className = "kanban-col";
    col.innerHTML = `<h3>${label} (${grouped[statusKey]?.length || 0})</h3>`;
    const zone = document.createElement("div");
    zone.className = "kanban-dropzone";
    zone.dataset.kind = kind;
    zone.dataset.status = statusKey;
    zone.addEventListener("dragover", (event) => event.preventDefault());
    zone.addEventListener("drop", async () => {
      const entity = state.dragEntity;
      if (!entity || entity.kind !== kind) return;
      await moveKanbanCard(kind, entity.id, statusKey);
    });

    for (const item of grouped[statusKey] || []) {
      const card = document.createElement("article");
      card.className = "kanban-card";
      card.draggable = true;
      card.dataset.id = String(item.id);
      card.dataset.kind = kind;
      card.innerHTML = `
        <strong>#${item.id} ${escapeHtml(item.title)}</strong>
        <div class="muted">${escapeHtml(formatContact(item))}</div>
      `;
      if (kind === "deal") {
        card.innerHTML += `<div class="muted">Amount: ${formatMoney(item.value_rub)}</div>`;
      }
      card.addEventListener("dragstart", () => {
        state.dragEntity = { kind, id: item.id };
      });
      zone.appendChild(card);
    }

    col.appendChild(zone);
    container.appendChild(col);
  }
}

async function moveKanbanCard(kind, id, targetStatus) {
  if (kind === "lead") {
    await api(`/leads/${id}/status`, { method: "PATCH", body: JSON.stringify({ status: targetStatus }) });
    await loadLeadsKanban();
  } else {
    await api(`/deals/${id}/status`, { method: "PATCH", body: JSON.stringify({ status: targetStatus }) });
    await loadDealsKanban();
  }
  await loadDashboard();
}

async function loadDashboard() {
  const cards = document.getElementById("dashboardCards");
  const leadStatus = document.getElementById("leadStatusSummary");
  const dealStatus = document.getElementById("dealStatusSummary");
  cards.innerHTML = "";
  leadStatus.innerHTML = "";
  dealStatus.innerHTML = "";

  try {
    const data = await api("/dashboard/summary");
    const cardEntries = [
      ["Parser total", data.parser_total],
      ["Leads total", data.leads_total],
      ["Deals total", data.deals_total],
      ["Calls total", data.calls_total],
      ["Missed calls", data.calls_missed],
      ["Calls transcribed", data.calls_transcribed],
      ["Lead->Deal %", data.conversion_lead_to_deal_percent],
      ["Pipeline RUB", formatMoney(data.pipeline_value_rub)],
    ];
    for (const [label, value] of cardEntries) {
      const card = document.createElement("div");
      card.className = "card";
      card.innerHTML = `<div class="label">${escapeHtml(label)}</div><div class="value">${escapeHtml(value)}</div>`;
      cards.appendChild(card);
    }
    Object.entries(data.leads_by_status).forEach(([key, value]) => {
      const el = document.createElement("div");
      el.className = "status-item";
      el.innerHTML = `<span>${escapeHtml(leadStatusLabels[key] || key)}</span><strong>${value}</strong>`;
      leadStatus.appendChild(el);
    });
    Object.entries(data.deals_by_status).forEach(([key, value]) => {
      const el = document.createElement("div");
      el.className = "status-item";
      el.innerHTML = `<span>${escapeHtml(key)}</span><strong>${value}</strong>`;
      dealStatus.appendChild(el);
    });
  } catch (error) {
    cards.innerHTML = `<div class="card"><div class="label">Dashboard unavailable</div><div>${escapeHtml(error.message)}</div></div>`;
  }
}

async function loadLeadsKanban() {
  const leads = await api("/leads");
  buildKanban("leadKanban", leads, leadStages, "lead");
}

async function loadDealsKanban() {
  const deals = await api("/deals");
  buildKanban("dealKanban", deals, dealStages, "deal");
}

async function loadParserHub(options = {}) {
  if (options.resetPage) {
    state.parserResults.page = 1;
  }
  if (typeof options.query === "string") {
    state.parserResults.query = options.query.trim();
  }

  const searchInput = document.getElementById("parserSearchInput");
  if (searchInput) {
    if (typeof options.query === "string") {
      searchInput.value = options.query;
    } else if (state.parserResults.query && !searchInput.value) {
      searchInput.value = state.parserResults.query;
    }
  }

  const query = (searchInput?.value || state.parserResults.query || "").trim();
  state.parserResults.query = query;

  const params = new URLSearchParams();
  params.set("page", String(state.parserResults.page));
  params.set("page_size", String(state.parserResults.pageSize));
  if (query) {
    params.set("q", query);
  }

  const response = await api(`/parser/results?${params.toString()}`);
  const isLegacyList = Array.isArray(response);
  const results = isLegacyList ? response : Array.isArray(response.items) ? response.items : [];

  if (!isLegacyList) {
    state.parserResults.page = Number(response.page || 1);
    state.parserResults.pages = Number(response.pages || 1);
    state.parserResults.total = Number(response.total || 0);
    state.parserResults.pageSize = Number(response.page_size || state.parserResults.pageSize);
  } else {
    state.parserResults.page = 1;
    state.parserResults.pages = 1;
    state.parserResults.total = results.length;
  }

  const rows = document.getElementById("parserRows");
  rows.innerHTML = "";
  for (const record of results) {
    const postUrl = record.telegram_post_url || record.raw_url || "";
    const postUrlCell = postUrl
      ? `<a href="${escapeHtml(postUrl)}" target="_blank" rel="noreferrer">Open</a>`
      : "-";
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${record.id}</td>
      <td>${escapeHtml(record.source_channel)}</td>
      <td>${escapeHtml(record.status)}</td>
      <td>${escapeHtml(record.title)}<br /><span class="muted">${escapeHtml(record.normalized_address || "-")}</span></td>
      <td>${postUrlCell}</td>
      <td>${escapeHtml(formatContact(record))}</td>
      <td class="actions">
        <button data-action="to-lead" data-id="${record.id}" class="secondary">To Lead</button>
        <button data-action="to-deal" data-id="${record.id}" class="secondary">To Deal</button>
        <button data-action="reject" data-id="${record.id}" class="danger">Reject</button>
      </td>
    `;
    rows.appendChild(tr);
  }

  const pageInfo = document.getElementById("parserPageInfo");
  if (pageInfo) {
    pageInfo.textContent = `Страница ${state.parserResults.page} из ${state.parserResults.pages}`;
  }
  const meta = document.getElementById("parserResultsMeta");
  if (meta) {
    meta.textContent = `Найдено: ${state.parserResults.total}. Показано: ${results.length}.`;
  }
  const prevBtn = document.getElementById("parserPrevPageBtn");
  if (prevBtn instanceof HTMLButtonElement) {
    prevBtn.disabled = state.parserResults.page <= 1;
  }
  const nextBtn = document.getElementById("parserNextPageBtn");
  if (nextBtn instanceof HTMLButtonElement) {
    nextBtn.disabled = state.parserResults.page >= state.parserResults.pages;
  }
}

function normalizeTelegramChannel(value) {
  return String(value || "")
    .trim()
    .replace(/^@/, "")
    .split("/", 1)[0]
    .toLowerCase();
}

function getTelegramSourceConfig(source) {
  const extra = source.extra_config && typeof source.extra_config === "object" ? source.extra_config : {};
  const search =
    extra.telegram_search && typeof extra.telegram_search === "object" ? extra.telegram_search : {};
  const filters =
    extra.telegram_filters && typeof extra.telegram_filters === "object" ? extra.telegram_filters : {};

  const discoveredRaw = Array.isArray(search.discovered_channels) ? search.discovered_channels : [];
  const discovered = discoveredRaw
    .filter((row) => row && typeof row === "object")
    .map((row) => {
      const username = normalizeTelegramChannel(row.username);
      const title = String(row.title || `@${username || "unknown"}`);
      const lastSeenAt = row.last_seen_at || null;
      const matchedQueries = Array.isArray(row.matched_queries) ? row.matched_queries : [];
      return { username, title, lastSeenAt, matchedQueries };
    })
    .filter((row) => row.username);

  const allowedRaw = Array.isArray(search.allowed_channels) ? search.allowed_channels : [];
  const allowedSet = new Set(allowedRaw.map((item) => normalizeTelegramChannel(item)).filter(Boolean));

  return {
    extra,
    search,
    filters,
    discovered,
    allowedSet,
    whitelistEnabled: Boolean(search.whitelist_enabled),
    udmurtiaOnly: Boolean(filters.udmurtia_only),
  };
}

async function patchTelegramSourceConfig(sourceId, updater) {
  const source = state.parserSources.find((item) => Number(item.id) === Number(sourceId));
  if (!source) return;

  const extra = source.extra_config && typeof source.extra_config === "object"
    ? JSON.parse(JSON.stringify(source.extra_config))
    : {};
  if (!extra.telegram_search || typeof extra.telegram_search !== "object") extra.telegram_search = {};
  if (!extra.telegram_filters || typeof extra.telegram_filters !== "object") extra.telegram_filters = {};

  updater(extra.telegram_search, extra.telegram_filters, extra);

  await api(`/parser/sources/${sourceId}`, {
    method: "PATCH",
    body: JSON.stringify({ extra_config: extra }),
  });
  await loadParserSources();
  await loadParserRuns();
}

function renderTelegramCatalog(sources, canManageSources) {
  const container = document.getElementById("telegramCatalog");
  if (!container) return;
  container.innerHTML = "";

  const telegramSources = sources.filter((source) => source.source_channel === "telegram");
  if (!telegramSources.length) {
    container.textContent = "No telegram sources configured.";
    container.classList.add("muted");
    return;
  }
  container.classList.remove("muted");

  for (const source of telegramSources) {
    const cfg = getTelegramSourceConfig(source);
    const discoveredCount = cfg.discovered.length;
    const allowedCount = cfg.allowedSet.size;

    const card = document.createElement("div");
    card.className = "telegram-source-card";
    card.innerHTML = `
      <div class="telegram-source-head">
        <div>
          <strong>${escapeHtml(source.name)}</strong>
          <div class="muted">mode: ${escapeHtml(
            source.extra_config && source.extra_config.mode ? String(source.extra_config.mode) : "html"
          )}</div>
        </div>
        <div class="telegram-inline-actions">
          <span class="muted">channels: ${discoveredCount}</span>
          <span class="muted">whitelist: ${allowedCount}</span>
          <span class="muted">udmurtia_only: ${cfg.udmurtiaOnly ? "on" : "off"}</span>
        </div>
      </div>
      <div class="telegram-inline-actions">
        ${
          canManageSources
            ? `<button class="secondary" data-action="toggle-whitelist" data-source-id="${source.id}">
                 ${cfg.whitelistEnabled ? "Disable whitelist" : "Enable whitelist"}
               </button>
               <button class="secondary" data-action="toggle-udmurtia" data-source-id="${source.id}">
                 ${cfg.udmurtiaOnly ? "Disable Udmurtia filter" : "Enable Udmurtia filter"}
               </button>`
            : ""
        }
      </div>
      <div class="telegram-channels"></div>
    `;

    const channelsEl = card.querySelector(".telegram-channels");
    if (channelsEl) {
      if (!cfg.discovered.length) {
        channelsEl.innerHTML = `<div class="muted">No discovered channels yet. Run parser to populate catalog.</div>`;
      } else {
        for (const channel of cfg.discovered) {
          const row = document.createElement("div");
          row.className = "telegram-channel-row";
          const inWhitelist = cfg.allowedSet.has(channel.username);
          row.innerHTML = `
            <div class="telegram-channel-main">
              <strong>@${escapeHtml(channel.username)}</strong>
              <span class="muted">${escapeHtml(channel.title)}</span>
              <span class="muted">last seen: ${escapeHtml(formatDateTime(channel.lastSeenAt))}</span>
            </div>
            <div class="telegram-inline-actions">
              ${
                canManageSources
                  ? `<button class="secondary" data-action="toggle-channel" data-source-id="${source.id}" data-username="${escapeHtml(
                      channel.username
                    )}">
                       ${inWhitelist ? "Remove" : "Whitelist"}
                     </button>`
                  : ""
              }
            </div>
          `;
          channelsEl.appendChild(row);
        }
      }
    }
    container.appendChild(card);
  }
}

async function loadParserSources() {
  const sources = await api("/parser/sources");
  state.parserSources = sources;
  const rows = document.getElementById("sourceRows");
  rows.innerHTML = "";
  const canManageSources = ["admin", "manager"].includes(state.user?.role || "");
  for (const source of sources) {
    const mode = source.extra_config && source.extra_config.mode ? String(source.extra_config.mode) : "html";
    const actionCell = canManageSources
      ? `<button data-source-id="${source.id}" data-source-active="${String(!source.is_active)}" class="secondary">
          ${source.is_active ? "Disable" : "Enable"}
        </button>`
      : "-";
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${source.id}</td>
      <td>${escapeHtml(source.name)}</td>
      <td>${escapeHtml(source.source_channel)}</td>
      <td><a href="${escapeHtml(source.source_url)}" target="_blank" rel="noreferrer">${escapeHtml(source.source_url)}</a></td>
      <td>${source.poll_minutes} min<br /><span class="muted">${escapeHtml(mode)}</span></td>
      <td>${escapeHtml(formatDateTime(source.last_run_at))}</td>
      <td>${source.is_active ? "active" : "inactive"}${source.last_error ? `<br /><span class="muted">${escapeHtml(source.last_error)}</span>` : ""}</td>
      <td class="actions">
        ${actionCell}
      </td>
    `;
    rows.appendChild(tr);
  }
  renderTelegramCatalog(sources, canManageSources);
}

async function loadParserRuns() {
  const runs = await api("/parser/runs?limit=15");
  const rows = document.getElementById("parserRunRows");
  rows.innerHTML = "";
  for (const run of runs) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${run.id}</td>
      <td>${escapeHtml(run.status)}</td>
      <td>${escapeHtml(run.trigger)}</td>
      <td>${run.fetched_count}</td>
      <td>${run.inserted_count}</td>
      <td>${run.error_count}</td>
      <td>${escapeHtml(formatDateTime(run.started_at))}</td>
    `;
    rows.appendChild(tr);
  }
}

async function loadCalls() {
  const calls = await api("/calls");
  const rows = document.getElementById("callRows");
  rows.innerHTML = "";
  for (const call of calls) {
    const recordingUrl = resolveRecordingUrl(call.recording_url);
    const transcriptDetails = call.transcript_text
      ? `<details><summary>${escapeHtml(call.transcript_status)}</summary><div class="muted">${escapeHtml(
          call.summary_text || ""
        )}</div><div>${escapeHtml(call.transcript_text)}</div></details>`
      : `<span class="muted">${escapeHtml(call.transcript_status)}</span>`;

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${call.id}</td>
      <td>${escapeHtml(formatDateTime(call.started_at))}</td>
      <td>${escapeHtml(call.from_number || "-")}</td>
      <td>${escapeHtml(call.to_number || "-")}</td>
      <td>${escapeHtml(call.status)}</td>
      <td>${call.duration_sec ?? "-"}</td>
      <td>${recordingUrl ? `<audio controls preload="none" src="${escapeHtml(recordingUrl)}"></audio>` : "-"}</td>
      <td>${transcriptDetails}</td>
      <td class="actions">
        <button data-call-action="transcribe" data-call-id="${call.id}" class="secondary">Transcribe</button>
        <button data-call-action="to-lead" data-call-id="${call.id}" class="secondary">To Lead</button>
      </td>
    `;
    rows.appendChild(tr);
  }
}

async function loadUsers() {
  const rows = document.getElementById("userRows");
  rows.innerHTML = "";
  const users = await api("/users");
  for (const user of users) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${user.id}</td>
      <td>${escapeHtml(user.full_name)}</td>
      <td>${escapeHtml(user.email)}</td>
      <td>${escapeHtml(user.role)}</td>
      <td>${user.is_active ? "active" : "blocked"}</td>
      <td class="actions">
        <button data-user-id="${user.id}" data-is-active="${String(!user.is_active)}" class="secondary">
          ${user.is_active ? "Block" : "Activate"}
        </button>
      </td>
    `;
    rows.appendChild(tr);
  }
}

async function onTabChange(tabName) {
  setActiveTab(tabName);
  if (tabName === "home") await loadDashboard();
  if (tabName === "leads") await loadLeadsKanban();
  if (tabName === "deals") await loadDealsKanban();
  if (tabName === "parser") await Promise.all([loadParserHub(), loadParserSources(), loadParserRuns()]);
  if (tabName === "calls") await loadCalls();
  if (tabName === "users") await loadUsers();
}

async function boot() {
  const me = await api("/auth/me");
  state.user = me.user;
  document.getElementById("currentUserInfo").textContent = `${me.user.full_name} (${me.user.role})`;

  if (me.user.role !== "admin") {
    document.getElementById("usersTabBtn").style.display = "none";
  }
  if (!["admin", "manager"].includes(me.user.role)) {
    document.getElementById("sourceForm").style.display = "none";
  }
  if (!["admin", "manager", "sales", "call_center"].includes(me.user.role)) {
    document.getElementById("runParserNowBtn").style.display = "none";
  }

  document.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.addEventListener("click", () => onTabChange(btn.dataset.tab));
  });

  document.getElementById("logoutBtn").addEventListener("click", async () => {
    try {
      await api("/auth/logout", { method: "POST" });
    } catch (error) {
      // ignore logout transport issues
    }
    localStorage.removeItem("cre_token");
    window.location.href = "/login";
  });

  document.getElementById("refreshLeadsBtn").addEventListener("click", loadLeadsKanban);
  document.getElementById("createLeadBtn").addEventListener("click", () => {
    alert("Форма создания лида будет добавлена следующим шагом.");
  });
  document.getElementById("refreshDealsBtn").addEventListener("click", loadDealsKanban);
  document.getElementById("refreshCallsBtn").addEventListener("click", loadCalls);

  document.getElementById("parserRows").addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLButtonElement)) return;
    const id = target.dataset.id;
    const action = target.dataset.action;
    if (!id || !action) return;
    if (action === "to-lead") await api(`/parser/results/${id}/to-lead`, { method: "POST", body: "{}" });
    if (action === "to-deal") await api(`/parser/results/${id}/to-deal`, { method: "POST", body: "{}" });
    if (action === "reject") await api(`/parser/results/${id}/reject`, { method: "POST", body: "{}" });
    await Promise.all([loadParserHub(), loadParserRuns(), loadLeadsKanban(), loadDealsKanban(), loadDashboard()]);
  });

  document.getElementById("parserSearchForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    state.parserResults.page = 1;
    await loadParserHub({ resetPage: true });
  });

  document.getElementById("parserSearchResetBtn").addEventListener("click", async () => {
    const input = document.getElementById("parserSearchInput");
    if (input instanceof HTMLInputElement) {
      input.value = "";
    }
    state.parserResults.page = 1;
    state.parserResults.query = "";
    await loadParserHub({ resetPage: true, query: "" });
  });

  document.getElementById("parserPrevPageBtn").addEventListener("click", async () => {
    if (state.parserResults.page <= 1) return;
    state.parserResults.page -= 1;
    await loadParserHub();
  });

  document.getElementById("parserNextPageBtn").addEventListener("click", async () => {
    if (state.parserResults.page >= state.parserResults.pages) return;
    state.parserResults.page += 1;
    await loadParserHub();
  });

  document.getElementById("sourceForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const payload = Object.fromEntries(new FormData(event.target).entries());
    payload.poll_minutes = Number(payload.poll_minutes || 1440);
    payload.max_items_per_run = Number(payload.max_items_per_run || 20);
    const mode = payload.mode || "html";
    const extraRaw = (payload.extra_config_json || "").trim();
    delete payload.mode;
    delete payload.extra_config_json;
    let extraConfig = {};
    if (extraRaw) {
      try {
        const parsed = JSON.parse(extraRaw);
        if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
          extraConfig = parsed;
        }
      } catch (error) {
        alert(`Invalid extra JSON: ${error.message}`);
        return;
      }
    }
    extraConfig.mode = mode;
    payload.extra_config = extraConfig;
    await api("/parser/sources", { method: "POST", body: JSON.stringify(payload) });
    event.target.reset();
    await Promise.all([loadParserSources(), loadParserRuns()]);
  });

  document.getElementById("sourceRows").addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLButtonElement)) return;
    const sourceId = target.dataset.sourceId;
    const sourceActive = target.dataset.sourceActive;
    if (!sourceId || sourceActive == null) return;
    await api(`/parser/sources/${sourceId}`, {
      method: "PATCH",
      body: JSON.stringify({ is_active: sourceActive === "true" }),
    });
    await loadParserSources();
  });

  document.getElementById("telegramCatalog").addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLButtonElement)) return;
    const action = target.dataset.action;
    const sourceId = target.dataset.sourceId;
    if (!action || !sourceId) return;

    if (action === "toggle-whitelist") {
      await patchTelegramSourceConfig(sourceId, (search) => {
        search.whitelist_enabled = !Boolean(search.whitelist_enabled);
        if (!Array.isArray(search.allowed_channels)) search.allowed_channels = [];
      });
      return;
    }

    if (action === "toggle-udmurtia") {
      await patchTelegramSourceConfig(sourceId, (_search, filters) => {
        filters.udmurtia_only = !Boolean(filters.udmurtia_only);
      });
      return;
    }

    if (action === "toggle-channel") {
      const username = normalizeTelegramChannel(target.dataset.username || "");
      if (!username) return;
      await patchTelegramSourceConfig(sourceId, (search) => {
        const current = Array.isArray(search.allowed_channels) ? search.allowed_channels : [];
        const normalized = new Set(current.map((item) => normalizeTelegramChannel(item)).filter(Boolean));
        if (normalized.has(username)) normalized.delete(username);
        else normalized.add(username);
        search.allowed_channels = Array.from(normalized).sort();
      });
    }
  });

  document.getElementById("runParserNowBtn").addEventListener("click", async () => {
    await api("/parser/run-now", { method: "POST", body: "{}" });
    state.parserResults.page = 1;
    await Promise.all([loadParserRuns(), loadParserHub({ resetPage: true }), loadDashboard()]);
  });

  document.getElementById("ingestForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const formData = new FormData(event.target);
    const item = Object.fromEntries(formData.entries());
    item.source_external_id = `${item.source_channel}-${Date.now()}`;
    item.area_sqm = item.area_sqm ? Number(item.area_sqm) : null;
    item.price_rub = item.price_rub ? Number(item.price_rub) : null;
    item.payload = { entered_from_ui: true };
    await api("/parser/ingest", { method: "POST", body: JSON.stringify({ items: [item] }) });
    event.target.reset();
    state.parserResults.page = 1;
    await Promise.all([loadParserHub({ resetPage: true }), loadParserRuns(), loadDashboard()]);
  });

  document.getElementById("callManualForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const payload = Object.fromEntries(new FormData(event.target).entries());
    payload.duration_sec = payload.duration_sec ? Number(payload.duration_sec) : null;
    payload.provider = "manual";
    payload.direction = "inbound";
    payload.started_at = new Date().toISOString();
    await api("/calls/manual", { method: "POST", body: JSON.stringify(payload) });
    event.target.reset();
    await Promise.all([loadCalls(), loadDashboard()]);
  });

  document.getElementById("recordingUploadForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.target;
    const formData = new FormData(form);
    const callId = formData.get("call_id");
    const file = formData.get("recording_file");
    if (!callId || !(file instanceof File)) return;
    const uploadData = new FormData();
    uploadData.append("file", file);
    await api(`/calls/${callId}/upload-recording`, { method: "POST", body: uploadData, headers: {} });
    form.reset();
    await loadCalls();
  });

  document.getElementById("callRows").addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLButtonElement)) return;
    const action = target.dataset.callAction;
    const callId = target.dataset.callId;
    if (!action || !callId) return;
    if (action === "transcribe") {
      await api(`/calls/${callId}/transcribe`, { method: "POST", body: "{}" });
      await Promise.all([loadCalls(), loadDashboard()]);
    }
    if (action === "to-lead") {
      await api(`/calls/${callId}/to-lead`, { method: "POST", body: "{}" });
      await Promise.all([loadCalls(), loadLeadsKanban(), loadDashboard()]);
    }
  });

  const usersForm = document.getElementById("newUserForm");
  usersForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const payload = Object.fromEntries(new FormData(usersForm).entries());
    await api("/users", { method: "POST", body: JSON.stringify(payload) });
    usersForm.reset();
    await loadUsers();
  });

  document.getElementById("userRows").addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLButtonElement)) return;
    const userId = target.dataset.userId;
    const isActive = target.dataset.isActive;
    if (!userId || !isActive) return;
    await api(`/users/${userId}/active`, {
      method: "PATCH",
      body: JSON.stringify({ is_active: isActive === "true" }),
    });
    await loadUsers();
  });

  await onTabChange("home");
}

boot().catch((error) => {
  alert(`Application boot error: ${error.message}`);
});
