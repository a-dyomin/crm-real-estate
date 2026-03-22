const apiPrefix = document.body.dataset.apiPrefix || "/api/v1";

function safeLocalStorage() {
  try {
    return window.localStorage;
  } catch (error) {
    return null;
  }
}

function safeSessionStorage() {
  try {
    return window.sessionStorage;
  } catch (error) {
    return null;
  }
}

function getStoredToken() {
  const local = safeLocalStorage();
  if (local) {
    const token = local.getItem("cre_token");
    if (token) return token;
  }
  const session = safeSessionStorage();
  if (session) return session.getItem("cre_token");
  return null;
}

function clearStoredToken() {
  const local = safeLocalStorage();
  if (local) local.removeItem("cre_token");
  const session = safeSessionStorage();
  if (session) session.removeItem("cre_token");
}

const token = getStoredToken();

const discoverySeedTypeLabels = {
  domain: "Домен",
  url: "URL",
  telegram_channel: "Telegram",
  keyword: "Ключевое слово",
};

const discoverySourceTypeLabels = {
  classifieds: "Классифайды",
  agency: "Агентство",
  developer: "Девелопер",
  business_center: "Бизнес-центр",
  mall: "ТЦ/ритейл",
  auction: "Банкротство",
  government: "Госресурсы",
  telegram: "Telegram",
  directory: "Справочник",
  aggregator: "Агрегатор",
  unknown: "Неизвестно",
};

const discoveryStatusLabels = {
  new: "Новый",
  classified: "Классифицирован",
  matched: "Шаблон найден",
  ready_for_activation: "Готов к активации",
  active: "Активирован",
  paused: "Пауза",
  rejected: "Отклонен",
  error: "Ошибка",
};

const discoveryPriorityLabels = {
  low: "Низкий",
  medium: "Средний",
  high: "Высокий",
  urgent: "Срочный",
};

const discoveryHealthLabels = {
  new: "Новый",
  healthy: "OK",
  degraded: "Снижение",
  blocked: "Блок",
  failed: "Ошибка",
};

const state = {
  user: null,
  discovery: { limit: 50 },
  autonomy: { sourceLimit: 50 },
  sourceNameMap: new Map(),
};

if (!token) {
  window.location.href = "/login";
}

function coalesce() {
  for (let i = 0; i < arguments.length; i += 1) {
    const value = arguments[i];
    if (value !== null && value !== undefined) return value;
  }
  return null;
}

function escapeHtml(value) {
  return String(coalesce(value, ""))
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

async function api(path, options = {}) {
  const headers = {
    ...(options.headers || {}),
    Authorization: "Bearer " + (getStoredToken() || ""),
  };
  if (!headers["Content-Type"] && options.body && !(options.body instanceof FormData)) {
    headers["Content-Type"] = "application/json";
  }
  const response = await fetch(`${apiPrefix}${path}`, { ...options, headers });
  if (response.status === 401) {
    clearStoredToken();
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

function formatDateTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("ru-RU");
}

function formatChannel(value) {
  const normalized = String(value || "").toLowerCase();
  const map = {
    avito: "Avito",
    cian: "Циан",
    domclick: "Домклик",
    yandex: "Яндекс Недвижимость",
    telegram: "Telegram",
    bankrupt: "Банкротство",
    web: "Сайт",
    manual: "Ручной",
  };
  return map[normalized] || value || "-";
}

function formatDiscoveryScore(value) {
  if (value == null) return "-";
  const num = Number(value);
  if (Number.isNaN(num)) return "-";
  return num.toFixed(1);
}

function formatDiscoveryLabel(map, value) {
  if (!value) return "-";
  return map[value] || value;
}

async function loadAutonomySummary() {
  const summaryEl = document.getElementById("autonomySummary");
  if (!summaryEl) return;
  const data = await api("/parser/autonomy/summary");
  const rows = [
    ["Следующий discovery", data.next_discovery_at ? formatDateTime(data.next_discovery_at) : "-"],
    ["Следующий парсинг", data.next_parse_at ? formatDateTime(data.next_parse_at) : "-"],
    ["Последний discovery", data.last_discovery_run_at ? formatDateTime(data.last_discovery_run_at) : "-"],
    ["Последний парсинг", data.last_parse_run_at ? formatDateTime(data.last_parse_run_at) : "-"],
    ["Активные источники", data.active_sources],
    ["Авто-активировано", data.auto_activated_sources],
    ["Проблемные источники", data.failed_sources],
  ];
  summaryEl.innerHTML = rows
    .map(([label, value]) => `<div class="status-item"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></div>`)
    .join("");
  const nowLabel = document.getElementById("autonomyNowLabel");
  if (nowLabel) {
    nowLabel.textContent = `MSK: ${formatDateTime(data.now_msk)}`;
  }
}

async function loadJobRuns() {
  const rows = document.getElementById("jobRunRows");
  if (!rows) return;
  const runs = await api("/parser/autonomy/job-runs?limit=20");
  rows.innerHTML = "";
  for (const run of runs) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${run.id}</td>
      <td>${escapeHtml(run.job_key)}</td>
      <td>${escapeHtml(run.status)}</td>
      <td>${escapeHtml(formatDateTime(run.started_at))}</td>
      <td>${escapeHtml(formatDateTime(run.finished_at))}</td>
      <td>${escapeHtml(run.error_message || "-")}</td>
    `;
    rows.appendChild(tr);
  }
}

async function loadAutonomySources() {
  const rows = document.getElementById("autonomySourceRows");
  if (!rows) return;
  const limitSelect = document.getElementById("autonomySourceLimit");
  if (limitSelect) {
    state.autonomy.sourceLimit = Number(limitSelect.value || state.autonomy.sourceLimit || 50);
  }
  const sources = await api(`/parser/autonomy/sources?state=active&limit=${state.autonomy.sourceLimit}`);
  state.sourceNameMap = new Map(sources.map((source) => [Number(source.id), source.name]));
  rows.innerHTML = "";
  for (const source of sources) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${source.id}</td>
      <td>${escapeHtml(source.name)}</td>
      <td>${escapeHtml(formatChannel(source.source_channel))}</td>
      <td>${escapeHtml(source.source_state || "-")}</td>
      <td>${escapeHtml(source.health_status || "-")}</td>
      <td>${escapeHtml(coalesce(source.parse_priority, "-"))}</td>
      <td>${escapeHtml(formatDateTime(source.next_scheduled_parse_at))}</td>
      <td>${source.auto_discovered ? "auto" : "manual"}</td>
      <td>${escapeHtml(formatDateTime(source.last_success_at))}</td>
      <td>${escapeHtml(source.last_error || "-")}</td>
      <td>${escapeHtml(coalesce(source.listings_parsed_last_run, "-"))}</td>
      <td>${escapeHtml(coalesce(source.contacts_extracted_last_run, "-"))}</td>
      <td>${escapeHtml(coalesce(source.contacts_rejected_last_run, "-"))}</td>
      <td>${escapeHtml(coalesce(source.leads_published_last_run, "-"))}</td>
    `;
    rows.appendChild(tr);
  }
}

async function loadSourceRuns() {
  const rows = document.getElementById("sourceRunRows");
  if (!rows) return;
  const runs = await api("/parser/source-runs?limit=40");
  rows.innerHTML = "";
  for (const run of runs) {
    const sourceName = state.sourceNameMap.get(Number(run.parser_source_id)) || `#${run.parser_source_id}`;
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${run.id}</td>
      <td>${escapeHtml(sourceName)}</td>
      <td>${escapeHtml(run.status)}</td>
      <td>${run.fetched_count}</td>
      <td>${run.listings_parsed}</td>
      <td>${run.contacts_extracted}</td>
      <td>${run.contacts_rejected}</td>
      <td>${run.leads_published}</td>
      <td>${escapeHtml(formatDateTime(run.started_at))}</td>
      <td>${escapeHtml(run.error_message || "-")}</td>
    `;
    rows.appendChild(tr);
  }
}

async function loadParserRuns() {
  const rows = document.getElementById("parserRunRows");
  if (!rows) return;
  const runs = await api("/parser/runs?limit=20");
  rows.innerHTML = "";
  for (const run of runs) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${run.id}</td>
      <td>${escapeHtml(run.status)}</td>
      <td>${escapeHtml(run.trigger)}</td>
      <td>${run.fetched_count}</td>
      <td>${run.inserted_count}</td>
      <td>${escapeHtml(coalesce(run.objects_resolved, "-"))}</td>
      <td>${escapeHtml(coalesce(run.identities_scored, "-"))}</td>
      <td>${escapeHtml(coalesce(run.owners_published, "-"))}</td>
      <td>${escapeHtml(coalesce(run.leads_auto_created, "-"))}</td>
      <td>${escapeHtml(coalesce(run.call_center_created, "-"))}</td>
      <td>${escapeHtml(coalesce(run.rejected_count, "-"))}</td>
      <td>${run.error_count}</td>
      <td>${escapeHtml(formatDateTime(run.started_at))}</td>
    `;
    rows.appendChild(tr);
  }
}

async function loadDiscoverySeeds() {
  const rows = document.getElementById("discoverySeedRows");
  if (!rows) return;
  const seeds = await api("/parser/discovery/seeds");
  rows.innerHTML = "";
  for (const seed of seeds) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${seed.id}</td>
      <td>${escapeHtml(formatDiscoveryLabel(discoverySeedTypeLabels, seed.seed_type))}</td>
      <td>${escapeHtml(seed.value)}</td>
      <td>${escapeHtml(seed.region || "-")}</td>
      <td>${seed.priority}</td>
      <td>${seed.enabled ? "активен" : "пауза"}</td>
      <td class="actions">
        <button data-seed-id="${seed.id}" data-seed-enabled="${String(!seed.enabled)}" class="secondary">
          ${seed.enabled ? "Выключить" : "Включить"}
        </button>
      </td>
    `;
    rows.appendChild(tr);
  }
}

async function loadDiscoveryRuns() {
  const rows = document.getElementById("discoveryRunRows");
  if (!rows) return;
  const runs = await api("/parser/discovery/runs?limit=12");
  rows.innerHTML = "";
  for (const run of runs) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${run.id}</td>
      <td>${escapeHtml(run.status)}</td>
      <td>${run.seed_count}</td>
      <td>${run.candidate_count}</td>
      <td>${run.matched_count}</td>
      <td>${run.error_count}</td>
      <td>${escapeHtml(formatDateTime(run.started_at))}</td>
    `;
    rows.appendChild(tr);
  }
}

async function loadDiscoveredSources() {
  const rows = document.getElementById("discoveredSourceRows");
  if (!rows) return;
  const limitSelect = document.getElementById("discoveryLimitSelect");
  if (limitSelect) {
    state.discovery.limit = Number(limitSelect.value || state.discovery.limit || 50);
  }
  const sources = await api(`/parser/discovery/sources?limit=${state.discovery.limit}`);
  rows.innerHTML = "";
  for (const source of sources) {
    const rootUrl = source.root_url || "";
    const linkCell = rootUrl
      ? `<a href="${escapeHtml(rootUrl)}" target="_blank" rel="noreferrer">${escapeHtml(source.domain)}</a>`
      : escapeHtml(source.domain);
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${linkCell}</td>
      <td>${escapeHtml(formatDiscoveryLabel(discoverySourceTypeLabels, source.source_type))}</td>
      <td>${escapeHtml(formatDiscoveryLabel(discoveryStatusLabels, source.discovery_status))}</td>
      <td>${formatDiscoveryScore(source.relevance_score)}</td>
      <td>${formatDiscoveryScore(source.listing_density_score)}</td>
      <td>${formatDiscoveryScore(source.contact_richness_score)}</td>
      <td>${formatDiscoveryScore(source.update_frequency_score)}</td>
      <td>${escapeHtml(source.parser_template_key || "-")}</td>
      <td>${escapeHtml(formatDiscoveryLabel(discoveryPriorityLabels, source.onboarding_priority))}</td>
      <td>${escapeHtml(formatDiscoveryLabel(discoveryHealthLabels, source.health_status))}</td>
      <td>${escapeHtml(formatDateTime(source.last_seen_at))}</td>
    `;
    rows.appendChild(tr);
  }
}

async function runDiscoveryNow() {
  const button = document.getElementById("discoveryRunBtn");
  if (button) button.disabled = true;
  try {
    await api("/parser/discovery/run", { method: "POST", body: "{}" });
  } finally {
    if (button) button.disabled = false;
  }
  await Promise.all([loadDiscoveryRuns(), loadDiscoveredSources()]);
}

async function createDiscoverySeed(event) {
  event.preventDefault();
  const form = event.target;
  const formData = new FormData(form);
  const payload = {
    seed_type: String(formData.get("seed_type") || "").trim(),
    value: String(formData.get("value") || "").trim(),
    region: String(formData.get("region") || "").trim() || null,
    priority: Number(formData.get("priority") || 0),
    enabled: Boolean(
      form.querySelector("input[name='enabled']") &&
        form.querySelector("input[name='enabled']").checked
    ),
  };
  if (!payload.seed_type || !payload.value) return;
  await api("/parser/discovery/seeds", { method: "POST", body: JSON.stringify(payload) });
  form.reset();
  const enabledInput = form.querySelector("input[name='enabled']");
  if (enabledInput) enabledInput.checked = true;
  await loadDiscoverySeeds();
}

async function toggleDiscoverySeed(seedId, enabled) {
  await api(`/parser/discovery/seeds/${seedId}`, {
    method: "PATCH",
    body: JSON.stringify({ enabled }),
  });
  await loadDiscoverySeeds();
}

async function loadAll() {
  await Promise.all([
    loadAutonomySummary(),
    loadJobRuns(),
    loadAutonomySources(),
    loadSourceRuns(),
    loadParserRuns(),
    loadDiscoverySeeds(),
    loadDiscoveryRuns(),
    loadDiscoveredSources(),
  ]);
}

async function loadAutonomyHub() {
  await Promise.all([loadAutonomySummary(), loadJobRuns(), loadAutonomySources(), loadSourceRuns()]);
}

async function boot() {
  const me = await api("/auth/me");
  state.user = me.user;
  const userInfo = document.getElementById("currentUserInfo");
  if (userInfo) userInfo.textContent = `${me.user.full_name} (${me.user.role})`;
  if (me.user.role !== "admin") {
    window.location.href = "/";
    return;
  }

  const logoutBtn = document.getElementById("logoutBtn");
  if (logoutBtn) {
    logoutBtn.addEventListener("click", async () => {
      try {
        await api("/auth/logout", { method: "POST" });
      } catch (error) {
        // ignore logout transport issues
      }
      clearStoredToken();
      window.location.href = "/login";
    });
  }

  const refreshBtn = document.getElementById("refreshParserSettingsBtn");
  if (refreshBtn) refreshBtn.addEventListener("click", loadAll);
  const refreshAutonomyBtn = document.getElementById("refreshAutonomyBtn");
  if (refreshAutonomyBtn) refreshAutonomyBtn.addEventListener("click", loadAutonomyHub);
  const refreshSourceRunsBtn = document.getElementById("refreshSourceRunsBtn");
  if (refreshSourceRunsBtn) refreshSourceRunsBtn.addEventListener("click", loadSourceRuns);
  const refreshRunsBtn = document.getElementById("refreshParserRunsBtn");
  if (refreshRunsBtn) refreshRunsBtn.addEventListener("click", loadParserRuns);
  const discoveryRunBtn = document.getElementById("discoveryRunBtn");
  if (discoveryRunBtn) discoveryRunBtn.addEventListener("click", runDiscoveryNow);

  const discoverySeedForm = document.getElementById("discoverySeedForm");
  if (discoverySeedForm) discoverySeedForm.addEventListener("submit", createDiscoverySeed);

  const seedRows = document.getElementById("discoverySeedRows");
  if (seedRows) {
    seedRows.addEventListener("click", async (event) => {
      const target = event.target;
      if (!(target instanceof HTMLButtonElement)) return;
      const seedId = target.dataset.seedId;
      const nextEnabled = target.dataset.seedEnabled;
      if (!seedId || !nextEnabled) return;
      await toggleDiscoverySeed(seedId, nextEnabled === "true");
    });
  }

  const discoveryLimitSelect = document.getElementById("discoveryLimitSelect");
  if (discoveryLimitSelect) {
    discoveryLimitSelect.addEventListener("change", loadDiscoveredSources);
  }
  const autonomyLimitSelect = document.getElementById("autonomySourceLimit");
  if (autonomyLimitSelect) {
    autonomyLimitSelect.addEventListener("change", loadAutonomySources);
  }

  await loadAll();
  setInterval(loadAll, 60000);
}

boot().catch((error) => {
  alert(`Application boot error: ${error.message}`);
});
