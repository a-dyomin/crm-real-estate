const apiPrefix = document.body.dataset.apiPrefix;
const token = localStorage.getItem("cre_token");

const leadStages = [
  ["new", "Новые"],
  ["qualified", "Квалифицированы"],
  ["appointment_set", "Назначена встреча"],
  ["converted", "Конвертированы"],
  ["disqualified", "Дисквалифицированы"],
];

const dealStages = [
  ["new", "Новые"],
  ["negotiation", "Переговоры"],
  ["due_diligence", "Проверка"],
  ["closed_won", "Успешно закрыты"],
  ["closed_lost", "Проиграны"],
];

const state = {
  user: null,
  currentTab: "home",
  dragEntity: null,
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
        card.innerHTML += `<div class="muted">Сумма: ${formatMoney(item.value_rub)}</div>`;
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
      ["Parser Total", data.parser_total],
      ["Leads Total", data.leads_total],
      ["Deals Total", data.deals_total],
      ["Конверсия L->D %", data.conversion_lead_to_deal_percent],
      ["Pipeline ₽", formatMoney(data.pipeline_value_rub)],
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
      el.innerHTML = `<span>${escapeHtml(key)}</span><strong>${value}</strong>`;
      leadStatus.appendChild(el);
    });
    Object.entries(data.deals_by_status).forEach(([key, value]) => {
      const el = document.createElement("div");
      el.className = "status-item";
      el.innerHTML = `<span>${escapeHtml(key)}</span><strong>${value}</strong>`;
      dealStatus.appendChild(el);
    });
  } catch (error) {
    cards.innerHTML = `<div class="card"><div class="label">Нет доступа к аналитике</div><div>${escapeHtml(error.message)}</div></div>`;
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

async function loadParserHub() {
  const results = await api("/parser/results");
  const rows = document.getElementById("parserRows");
  rows.innerHTML = "";
  for (const record of results) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${record.id}</td>
      <td>${escapeHtml(record.source_channel)}</td>
      <td>${escapeHtml(record.status)}</td>
      <td>${escapeHtml(record.title)}<br /><span class="muted">${escapeHtml(record.normalized_address || "-")}</span></td>
      <td>${escapeHtml(formatContact(record))}</td>
      <td class="actions">
        <button data-action="to-lead" data-id="${record.id}" class="secondary">В лид</button>
        <button data-action="to-deal" data-id="${record.id}" class="secondary">В сделку</button>
        <button data-action="reject" data-id="${record.id}" class="danger">Отклонить</button>
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
          ${user.is_active ? "Блокировать" : "Активировать"}
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
  if (tabName === "parser") await loadParserHub();
  if (tabName === "users") await loadUsers();
}

async function boot() {
  const me = await api("/auth/me");
  state.user = me.user;
  document.getElementById("currentUserInfo").textContent = `${me.user.full_name} (${me.user.role})`;

  if (me.user.role !== "admin") {
    document.getElementById("usersTabBtn").style.display = "none";
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
  document.getElementById("refreshDealsBtn").addEventListener("click", loadDealsKanban);

  document.getElementById("parserRows").addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLButtonElement)) return;
    const id = target.dataset.id;
    const action = target.dataset.action;
    if (!id || !action) return;
    if (action === "to-lead") await api(`/parser/results/${id}/to-lead`, { method: "POST", body: "{}" });
    if (action === "to-deal") await api(`/parser/results/${id}/to-deal`, { method: "POST", body: "{}" });
    if (action === "reject") await api(`/parser/results/${id}/reject`, { method: "POST", body: "{}" });
    await Promise.all([loadParserHub(), loadLeadsKanban(), loadDealsKanban(), loadDashboard()]);
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
    await Promise.all([loadParserHub(), loadDashboard()]);
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
  alert(`Ошибка запуска интерфейса: ${error.message}`);
});

