const apiPrefix = document.body.dataset.apiPrefix || "/api/v1";
const featureOwnerIntel = document.body.dataset.featureOwnerIntelligence === "true";
window.__appScriptLoaded = true;
window.__bootStage = "loaded";
window.__appBooted = false;

if (!Object.fromEntries) {
  Object.fromEntries = function (entries) {
    const result = {};
    if (!entries || typeof entries[Symbol.iterator] !== "function") {
      return result;
    }
    for (const pair of entries) {
      if (!pair || pair.length < 2) continue;
      result[pair[0]] = pair[1];
    }
    return result;
  };
}

if (!Array.prototype.includes) {
  Array.prototype.includes = function (value) {
    for (let i = 0; i < this.length; i += 1) {
      if (this[i] === value || (Number.isNaN(this[i]) && Number.isNaN(value))) return true;
    }
    return false;
  };
}

if (!String.prototype.includes) {
  String.prototype.includes = function (search, start) {
    return this.indexOf(search, start || 0) !== -1;
  };
}
if (window.__setBootStatus) {
  window.__setBootStatus("JS loaded");
}

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

function setStoredToken(value) {
  const local = safeLocalStorage();
  if (local) {
    local.setItem("cre_token", value);
    return;
  }
  const session = safeSessionStorage();
  if (session) session.setItem("cre_token", value);
}

function clearStoredToken() {
  const local = safeLocalStorage();
  if (local) local.removeItem("cre_token");
  const session = safeSessionStorage();
  if (session) session.removeItem("cre_token");
}

const token = getStoredToken();

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

const leadSourceOptions = [
  "Не выбрано",
  "Звонок",
  "Telegram",
  "Avito",
  "Avito – ДЕПО Консалтинг",
  "WhatsApp",
  "Рекомендация",
  "Веб-сайт",
  "Существующий клиент",
  "Avito – Avito Бизнес-центр Короленко",
  "Актуализация базы",
  "Avito – Avito ПКЦ Гагаринский",
  "Avito – Avito Депо",
  "Avito – Avito База Южная",
  "Avito – Avito Строительная База Южная",
  "Avito – Avito Дом купца Оглоблина",
  "Avito – Avito Энергия",
  "Робот",
  "ОКВЭД",
  "Поиск объекта",
  "ГЦК (генерация целевых клиентов)",
  "Чат C+",
  "Мониторинг арендаторов",
];

const leadNeedOptions = [
  "Не выбрано",
  "Аренда",
  "Субаренда",
  "Купить помещение",
  "Продать помещение",
  "Продать ЗУ",
  "Купить ЗУ",
  "Инвестор",
];

const leadDistrictOptions = [
  "Не выбрано",
  "Устиновский",
  "Первомайский",
  "Индустриальный",
  "Ленинский",
  "Октябрьский",
  "Завьяловский",
  "Любой",
];

const leadAddressOptions = [
  "Не выбрано",
  "ул. Буммашевская, 5Б (Депо)",
  "ул. Гагарина, 1 (ПКЦ Гагаринский)",
  "ул. Ленина, 46 (Ленина, 46)",
  "ул. Максима Горького, 86 (ИП Агашин Д.В.)",
  "ул. Максима Горького, 88 (ИП Агашин Д.В.)",
  "ул. Максима Горького, 63А (ИП Гимранов А.Р.)",
  "ул. Маяковского, 41 (СБЮ)",
  "ул. Маяковского, 43 (БЮ)",
  "ул. Буммашевская, 53 (Энергия)",
  "ул. 8 Марта, 87",
  "ул. Милиционная, 4",
  "ул. К. Маркса, 188/1",
  "Сторонние адреса",
];

const leadPropertyTypeOptions = [
  "Не выбрано",
  "Офис",
  "Склад",
  "Производство",
  "Торговля",
  "Открытая площадка",
  "Light Industrial",
  "Земельный участок",
  "Аутсорсинг",
  "Иное",
  "Свободное назначение",
];

const leadAreaRangeOptions = [
  "Не выбрано",
  "До 10",
  "10–30",
  "30–80",
  "80–150",
  "150–300",
  "300–500",
  "500–800",
  "800–1000",
  "1000–1500",
  "1500–2000",
  "2000 и более",
];

const leadActivityOptions = [
  "Не выбрано",
  "Офисная",
  "Бьюти",
  "Медицина",
  "Образование",
  "Строительство",
  "Спорт",
  "Пищевая деятельность",
  "Автомобильная деятельность",
  "Деревообрабатывающая деятельность",
  "Вендинг",
  "Нет данных (Чат авито)",
  "Маркетплейсы",
  "Непродовольственный товар",
  "Другое",
  "Сервис / Услуги",
  "Металлообработка",
];

const leadUrgencyOptions = [
  "Не выбрано",
  "В течении недели",
  "В течении месяца",
  "До 6 месяцев",
  "Не срочно (от 6 мес)",
];

const EMPTY_OPTION_LABEL = "Не выбрано";
const leadStatusLabels = Object.fromEntries(leadStages);

const dealStages = [
  ["new", "New"],
  ["negotiation", "Negotiation"],
  ["due_diligence", "Due Diligence"],
  ["closed_won", "Closed Won"],
  ["closed_lost", "Closed Lost"],
];

const parserRegionOptions = [
  { value: "", label: "Все" },
  { value: "RU-UDM", label: "Удмуртия (RU-UDM)" },
];

const parserDealOptions = [
  { value: "", label: "Все" },
  { value: "rent", label: "Аренда" },
  { value: "sale", label: "Продажа" },
];

const parserPropertyOptions = [
  { value: "", label: "Все" },
  { value: "office", label: "Офис" },
  { value: "warehouse", label: "Склад" },
  { value: "industrial", label: "Производство" },
  { value: "retail", label: "Торговля" },
  { value: "land", label: "Земельный участок" },
  { value: "free_purpose", label: "Свободное назначение" },
  { value: "other", label: "Иное" },
];

const parserSourceOptions = [
  { value: "", label: "Все" },
  { value: "avito", label: "Avito" },
  { value: "cian", label: "Циан" },
  { value: "domclick", label: "Домклик" },
  { value: "yandex", label: "Яндекс Недвижимость" },
  { value: "telegram", label: "Telegram" },
  { value: "bankrupt", label: "Банкротство" },
  { value: "web", label: "Сайт" },
];

const parserStatusLabels = {
  new: "Новый",
  possible_duplicate: "Проверка",
  duplicate: "В обработке",
  converted_to_lead: "В лидах",
  converted_to_deal: "В сделке",
  rejected: "Скрыт",
};

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
  currentTab: "home",
  dragEntity: null,
  dragSuppressUntil: 0,
  parserAutoRefreshTimer: null,
  parserSources: [],
  users: [],
  parserResults: {
    page: 1,
    pageSize: 20,
    pages: 1,
    total: 0,
    query: "",
    quickTab: "all",
    items: [],
    filteredItems: [],
    filters: {
      region: "",
      dealType: "",
      propertyType: "",
      source: "",
      onlyNew: false,
      onlyHot: false,
      onlyOwner: false,
      onlyDuplicates: false,
      onlyPriceDrop: false,
      updatedFrom: "",
      updatedTo: "",
    },
  },
  ownerIntel: {
    items: [],
    filters: {
      query: "",
      onlyHigh: true,
      onlySingle: false,
      onlyNew: false,
      onlyLowCompetition: false,
    },
  },
  discovery: {
    limit: 50,
  },
  autonomy: {
    sourceLimit: 50,
  },
  discoverySeeds: [],
  discoveryRuns: [],
  discoveredSources: [],
  leadCard: {
    lead: null,
    original: null,
    events: [],
  },
  parserDetail: {
    record: null,
    duplicates: null,
  },
};

if (!token) {
  window.location.href = "/login";
}

if (typeof window.fetch !== "function") {
  if (window.__setBootStatus) {
    window.__setBootStatus("JS error: fetch недоступен");
  }
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
  const timeoutMs = typeof options.timeoutMs === "number" ? options.timeoutMs : 15000;
  const headers = Object.assign({}, options.headers || {});
  headers.Authorization = "Bearer " + (getStoredToken() || "");
  if (!headers["Content-Type"] && options.body && !(options.body instanceof FormData)) {
    headers["Content-Type"] = "application/json";
  }
  const fetchPromise = fetch(apiPrefix + path, Object.assign({}, options, { headers }));
  let response;
  if (timeoutMs > 0) {
    response = await Promise.race([
      fetchPromise,
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error(`Request timeout (${path})`)), timeoutMs)
      ),
    ]);
  } else {
    response = await fetchPromise;
  }
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

function setActiveTab(tabName) {
  state.currentTab = tabName;
  document.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.tab === tabName);
  });
  document.querySelectorAll(".tab").forEach((tab) => {
    tab.classList.toggle("active", tab.id === `tab-${tabName}`);
  });
}

function startParserAutoRefresh() {
  if (state.parserAutoRefreshTimer) return;
  state.parserAutoRefreshTimer = setInterval(async () => {
    if (state.currentTab !== "parser") return;
    if (state.parserResults.quickTab === "owners" && featureOwnerIntel) {
      await loadOwnerContacts();
    } else {
      await loadParserHub();
    }
  }, 60000);
}

function stopParserAutoRefresh() {
  if (state.parserAutoRefreshTimer) {
    clearInterval(state.parserAutoRefreshTimer);
    state.parserAutoRefreshTimer = null;
  }
}

function formatMoney(value) {
  if (value == null) return "-";
  return new Intl.NumberFormat("ru-RU").format(Number(value));
}

function formatContact(item) {
  const parts = [item.contact_name, item.contact_phone, item.contact_email].filter(Boolean);
  if (parts.length) return parts.join(" | ");
  return "Телефон скрыт или недоступен";
}

function formatListingType(value) {
  const normalized = String(value || "").toLowerCase();
  if (!normalized) return "-";
  if (normalized === "rent") return "Аренда";
  if (normalized === "sale") return "Продажа";
  return value;
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

function formatAddress(item) {
  const parts = [];
  if (item.city) parts.push(item.city);
  if (item.address_district) {
    const district = item.address_district.toLowerCase().includes("район")
      ? item.address_district
      : `${item.address_district} район`;
    parts.push(district);
  }
  if (item.address_street) {
    parts.push(item.address_street);
  } else if (item.normalized_address) {
    parts.push(item.normalized_address);
  }
  return parts.filter(Boolean).join(", ");
}

function formatLeadScore(payload) {
  if (!payload || typeof payload.lead_score !== "number") return "-";
  return payload.lead_score.toFixed(1);
}

function formatLeadTier(payload) {
  const tier = payload ? payload.monetization_tier : undefined;
  if (tier === "premium") return "Premium";
  if (tier === "archive") return "Archive";
  return "-";
}

function getLeadScoreValue(payload) {
  if (!payload || typeof payload.lead_score !== "number") return null;
  return payload.lead_score;
}

function getLeadBreakdown(payload) {
  if (!payload || typeof payload.lead_score_breakdown !== "object") return {};
  return payload.lead_score_breakdown || {};
}

function getOwnerIntelScore(payload) {
  if (!payload) return null;
  const score = coalesce(payload.owner_probability_score, payload.owner_intel_score, payload.ownerIntelScore, null);
  if (typeof score === "number") return score;
  if (payload.owner_intel && typeof payload.owner_intel.score === "number") return payload.owner_intel.score;
  return null;
}

function getOwnerIntelClass(payload) {
  if (!payload) return null;
  return payload.owner_intel_class || payload.ownerIntelClass || null;
}

function resolvePriority(payload) {
  const score = getLeadScoreValue(payload);
  if (score == null || Number.isNaN(score)) {
    return { key: "low", label: "Низкий", score: null };
  }
  if (score >= 80) return { key: "hot", label: "Горячий", score };
  if (score >= 65) return { key: "high", label: "Высокий", score };
  if (score >= 45) return { key: "medium", label: "Средний", score };
  return { key: "low", label: "Низкий", score };
}

function resolvePriorityReasons(record) {
  const payload = record && record.payload ? record.payload : {};
  const breakdown = getLeadBreakdown(payload);
  const reasons = [];
  if (payload.below_market_flag || coalesce(breakdown.under_market_score, 0) >= 60) reasons.push("ниже рынка");
  const ownerIntelScore = getOwnerIntelScore(payload);
  if (ownerIntelScore != null && ownerIntelScore >= 60) {
    reasons.push("собственник");
  } else if (coalesce(breakdown.owner_probability_score, 0) >= 60) {
    reasons.push("собственник");
  }
  const freshness = payload.lead_score_freshness_hours;
  if (typeof freshness === "number" && freshness <= 24) reasons.push("новый объект");
  if (coalesce(breakdown.urgency_score, 0) >= 60) reasons.push("срочно");
  if (coalesce(breakdown.uniqueness_score, 0) >= 70) reasons.push("редкий объект");
  if (isParserPriceDrop(record) && !reasons.includes("снижена цена")) reasons.push("снижена цена");
  return reasons.slice(0, 3);
}

function normalizeText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function resolvePropertyType(record) {
  const payload = record && record.payload ? record.payload : {};
  const hint = normalizeText(payload.property_type || payload.propertyType || "");
  const text = normalizeText(`${(record && record.title) || ""} ${(record && record.description) || ""}`);
  const merged = `${hint} ${text}`.trim();
  if (merged.includes("склад")) return { key: "warehouse", label: "Склад" };
  if (merged.includes("офис")) return { key: "office", label: "Офис" };
  if (merged.includes("производ") || merged.includes("индустри")) return { key: "industrial", label: "Производство" };
  if (merged.includes("торгов") || merged.includes("магаз")) return { key: "retail", label: "Торговля" };
  if (merged.includes("земел") || merged.includes("участ")) return { key: "land", label: "Земельный участок" };
  if (merged.includes("свобод") || merged.includes("псн")) return { key: "free_purpose", label: "Свободное назначение" };
  return { key: "other", label: "Иное" };
}

function resolveOwnerLabel(payload) {
  const breakdown = getLeadBreakdown(payload);
  const ownerIntelScore = getOwnerIntelScore(payload);
  const score = coalesce(ownerIntelScore, breakdown.owner_probability_score, null);
  if (score == null) return { label: "Неизвестно", key: "unknown" };
  if (score >= 65) return { label: "Собственник", key: "owner" };
  if (score <= 35) return { label: "Агент", key: "agent" };
  return { label: "Возможно", key: "maybe" };
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

function buildParserPhotoCell(item) {
  if (item.image_url) {
    const safeUrl = escapeHtml(item.image_url);
    return `<img class="parser-thumb" src="${safeUrl}" alt="Фото объекта" loading="lazy" referrerpolicy="no-referrer" />`;
  }
  return `<div class="parser-thumb parser-thumb--placeholder">—</div>`;
}

function formatDateTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("ru-RU");
}

function formatParserStatus(value) {
  if (!value) return "-";
  return parserStatusLabels[value] || value;
}

function buildReasonBadges(reasons) {
  if (!reasons.length) return "";
  return `<div class="parser-badges">${reasons
    .map((reason) => `<span class="tag">${escapeHtml(reason)}</span>`)
    .join("")}</div>`;
}

function buildPriorityCell(record) {
  const priority = resolvePriority(record.payload);
  const scoreText = priority.score != null ? Math.round(priority.score) : null;
  return `
    <div class="priority-cell">
      <span class="priority-badge priority-${priority.key}">${escapeHtml(priority.label)}</span>
      ${scoreText != null ? `<span class="priority-score">${scoreText}</span>` : ""}
    </div>
  `;
}

function formatArea(value) {
  if (!value) return "-";
  const rounded = Math.round(Number(value));
  if (Number.isNaN(rounded)) return "-";
  return `${rounded} м²`;
}

function buildObjectCell(record) {
  const property = resolvePropertyType(record);
  const areaLabel = formatArea(record.area_sqm);
  const usePropertyTitle = property.key !== "other" && areaLabel !== "-";
  const titlePrimary = usePropertyTitle ? `${property.label} ${areaLabel}` : record.title;
  const secondary = [formatListingType(record.listing_type), property.key !== "other" ? property.label : ""]
    .filter((item) => item && item !== "-")
    .join(" • ");
  const reasons = resolvePriorityReasons(record);
  const duplicateLabel =
    record.status === "duplicate" || record.status === "possible_duplicate"
      ? `<div class="parser-duplicate">Есть дубли: 1</div>`
      : "";
  return `
    <div class="parser-object">
      <div class="parser-object-media">${buildParserPhotoCell(record)}</div>
      <div class="parser-object-text">
        <div class="parser-object-title">${escapeHtml(titlePrimary || record.title)}</div>
        <div class="parser-object-meta">${escapeHtml(secondary || record.title)}</div>
        ${buildReasonBadges(reasons)}
        ${duplicateLabel}
      </div>
    </div>
  `;
}

function buildDealTypeCell(record) {
  const property = resolvePropertyType(record);
  const dealLabel = formatListingType(record.listing_type);
  const parts = [dealLabel, property.key !== "other" ? property.label : ""].filter((item) => item && item !== "-");
  return parts.length ? escapeHtml(parts.join(" • ")) : "-";
}

function buildLocationCell(record) {
  const mainParts = [];
  if (record.city) mainParts.push(record.city);
  if (record.address_street) {
    mainParts.push(record.address_street);
  } else if (record.normalized_address) {
    mainParts.push(record.normalized_address);
  }
  const main = mainParts.join(", ");
  let district = record.address_district ? record.address_district : "";
  if (district && !district.toLowerCase().includes("район")) {
    district = `${district} район`;
  }
  return `
    <div class="parser-location">
      <div>${escapeHtml(main || "-")}</div>
      <div class="muted">${escapeHtml(district || "")}</div>
    </div>
  `;
}

function buildPriceCell(record) {
  if (!record.price_rub) return "-";
  const priceLabel = `${formatMoney(record.price_rub)} ₽`;
  const area = Number(record.area_sqm);
  let perMeter = "";
  if (area && !Number.isNaN(area)) {
    const per = Number(record.price_rub) / area;
    if (Number.isFinite(per)) {
      perMeter = `${formatMoney(per.toFixed(0))} ₽/м²`;
    }
  }
  return `
    <div class="parser-price">
      <div>${escapeHtml(priceLabel)}</div>
      ${perMeter ? `<div class="muted">${escapeHtml(perMeter)}</div>` : ""}
    </div>
  `;
}

function buildAreaCell(record) {
  return escapeHtml(formatArea(record.area_sqm));
}

function buildContactCell(record) {
  const payload = record.payload || {};
  const phone = formatRussianPhone(record.contact_phone || "");
  const contactLabel = resolveOwnerLabel(payload);
  const name = record.contact_name || "";
  const contactLine = phone || "Телефон скрыт";
  return `
    <div class="parser-contact">
      <div>${escapeHtml(contactLine)}</div>
      ${name ? `<div class="muted">${escapeHtml(name)}</div>` : ""}
      <span class="contact-badge contact-${contactLabel.key}">${escapeHtml(contactLabel.label)}</span>
    </div>
  `;
}

function buildSourceCell(record) {
  const sourceName = formatChannel(record.source_channel);
  const link = record.telegram_post_url || record.raw_url || "";
  const linkHtml = link
    ? `<a class="icon-link" href="${escapeHtml(link)}" target="_blank" rel="noreferrer">↗</a>`
    : "";
  return `
    <div class="parser-source">
      <span>${escapeHtml(sourceName || "-")}</span>
      ${linkHtml}
    </div>
  `;
}

function getPhoneDigits(value) {
  return String(value || "").replace(/\D/g, "");
}

function formatRussianPhone(value) {
  const digitsRaw = getPhoneDigits(value);
  if (!digitsRaw) return "";
  let normalized = digitsRaw;
  if (normalized[0] === "8") normalized = `7${normalized.slice(1)}`;
  if (normalized[0] !== "7") normalized = `7${normalized}`;
  normalized = normalized.slice(0, 11);
  const rest = normalized.slice(1);
  let formatted = "+7";
  if (rest.length) formatted += ` (${rest.slice(0, 3)}`;
  if (rest.length >= 3) formatted += ")";
  if (rest.length > 3) formatted += ` ${rest.slice(3, 6)}`;
  if (rest.length > 6) formatted += `-${rest.slice(6, 8)}`;
  if (rest.length > 8) formatted += `-${rest.slice(8, 10)}`;
  return formatted;
}

function isValidRussianPhone(value) {
  if (!value) return true;
  return /^\+7 \\(\\d{3}\\) \\d{3}-\\d{2}-\\d{2}$/.test(value);
}

function resolveRecordingUrl(url) {
  if (!url) return "";
  if (url.startsWith("http://") || url.startsWith("https://")) return url;
  if (url.startsWith("/media/")) return url;
  if (url.startsWith("/")) return url;
  return `/${url}`;
}

function cloneJson(value) {
  return value ? JSON.parse(JSON.stringify(value)) : value;
}

function canEditLead() {
  return ["admin", "call_center", "sales", "manager"].includes(
    state.user && state.user.role ? state.user.role : ""
  );
}

function setFieldError(input, hasError) {
  if (!input) return;
  const label = input.closest("label");
  if (label) label.classList.toggle("field-error", hasError);
}

function populateSelect(selectEl, options) {
  if (!(selectEl instanceof HTMLSelectElement)) return;
  selectEl.innerHTML = "";
  options.forEach((option) => {
    const opt = document.createElement("option");
    opt.value = option;
    opt.textContent = option;
    selectEl.appendChild(opt);
  });
}

function populateSelectOptions(selectEl, options) {
  if (!(selectEl instanceof HTMLSelectElement)) return;
  selectEl.innerHTML = "";
  options.forEach((option) => {
    const opt = document.createElement("option");
    opt.value = option.value;
    opt.textContent = option.label;
    selectEl.appendChild(opt);
  });
}

function normalizeSelectValue(value) {
  if (value == null) return "";
  const text = String(value).trim();
  if (!text || text === EMPTY_OPTION_LABEL) return "";
  return text;
}

function setSelectValue(selectEl, value) {
  if (!(selectEl instanceof HTMLSelectElement)) return;
  const options = Array.from(selectEl.options).map((opt) => opt.value);
  let target = normalizeSelectValue(value);
  if (!target) {
    target = options.includes(EMPTY_OPTION_LABEL) ? EMPTY_OPTION_LABEL : options[0] || "";
  }
  if (!options.includes(target)) {
    target = options[0] || "";
  }
  selectEl.value = target;
}

function getSelectValue(selectEl) {
  if (!(selectEl instanceof HTMLSelectElement)) return null;
  const value = selectEl.value;
  if (!value || value === EMPTY_OPTION_LABEL) return null;
  return value;
}

function setMultiSelectValue(selectEl, values) {
  if (!(selectEl instanceof HTMLSelectElement)) return;
  const selected = new Set(Array.isArray(values) ? values : []);
  let hasSelection = false;
  Array.from(selectEl.options).forEach((opt) => {
    const isSelected = selected.has(opt.value);
    opt.selected = isSelected;
    if (isSelected) hasSelection = true;
  });
  if (!hasSelection) {
    const placeholder = Array.from(selectEl.options).find((opt) => opt.value === EMPTY_OPTION_LABEL);
    if (placeholder) placeholder.selected = true;
  }
}

function getMultiSelectValue(selectEl) {
  if (!(selectEl instanceof HTMLSelectElement)) return null;
  const values = Array.from(selectEl.selectedOptions).map((opt) => opt.value);
  const filtered = values.filter((value) => value && value !== EMPTY_OPTION_LABEL);
  return filtered.length ? filtered : null;
}

function populateLeadSelects() {
  populateSelect(document.getElementById("leadSourceSelect"), leadSourceOptions);
  populateSelect(document.getElementById("leadNeedSelect"), leadNeedOptions);
  populateSelect(document.getElementById("leadDistrictSelect"), leadDistrictOptions);
  populateSelect(document.getElementById("leadAddressSelect"), leadAddressOptions);
  populateSelect(document.getElementById("leadPropertyTypeSelect"), leadPropertyTypeOptions);
  populateSelect(document.getElementById("leadAreaRangeSelect"), leadAreaRangeOptions);
  populateSelect(document.getElementById("leadActivitySelect"), leadActivityOptions);
  populateSelect(document.getElementById("leadUrgencySelect"), leadUrgencyOptions);
}

function populateParserFilters() {
  populateSelectOptions(document.getElementById("parserRegionFilter"), parserRegionOptions);
  populateSelectOptions(document.getElementById("parserDealFilter"), parserDealOptions);
  populateSelectOptions(document.getElementById("parserPropertyFilter"), parserPropertyOptions);
  populateSelectOptions(document.getElementById("parserSourceFilter"), parserSourceOptions);
}

function getDefaultParserFilters() {
  return {
    region: "",
    dealType: "",
    propertyType: "",
    source: "",
    onlyNew: false,
    onlyHot: false,
    onlyOwner: false,
    onlyDuplicates: false,
    onlyPriceDrop: false,
    updatedFrom: "",
    updatedTo: "",
  };
}

function readParserFiltersFromForm() {
  const filters = state.parserResults.filters;
  const regionEl = document.getElementById("parserRegionFilter");
  const dealEl = document.getElementById("parserDealFilter");
  const propertyEl = document.getElementById("parserPropertyFilter");
  const sourceEl = document.getElementById("parserSourceFilter");
  const searchInput = document.getElementById("parserSearchInput");
  const updatedFrom = document.getElementById("parserUpdatedFrom");
  const updatedTo = document.getElementById("parserUpdatedTo");
  const onlyNew = document.getElementById("parserOnlyNew");
  const onlyHot = document.getElementById("parserOnlyHot");
  const onlyOwner = document.getElementById("parserOnlyOwner");
  const onlyDuplicates = document.getElementById("parserOnlyDuplicates");
  const onlyPriceDrop = document.getElementById("parserOnlyPriceDrop");

  filters.region = regionEl instanceof HTMLSelectElement ? regionEl.value : "";
  filters.dealType = dealEl instanceof HTMLSelectElement ? dealEl.value : "";
  filters.propertyType = propertyEl instanceof HTMLSelectElement ? propertyEl.value : "";
  filters.source = sourceEl instanceof HTMLSelectElement ? sourceEl.value : "";
  filters.updatedFrom = updatedFrom instanceof HTMLInputElement ? updatedFrom.value : "";
  filters.updatedTo = updatedTo instanceof HTMLInputElement ? updatedTo.value : "";
  filters.onlyNew = onlyNew instanceof HTMLInputElement ? onlyNew.checked : false;
  filters.onlyHot = onlyHot instanceof HTMLInputElement ? onlyHot.checked : false;
  filters.onlyOwner = onlyOwner instanceof HTMLInputElement ? onlyOwner.checked : false;
  filters.onlyDuplicates = onlyDuplicates instanceof HTMLInputElement ? onlyDuplicates.checked : false;
  filters.onlyPriceDrop = onlyPriceDrop instanceof HTMLInputElement ? onlyPriceDrop.checked : false;
  if (searchInput instanceof HTMLInputElement) {
    state.parserResults.query = searchInput.value.trim();
  }
}

function applyParserFiltersToForm() {
  const filters = state.parserResults.filters;
  const regionEl = document.getElementById("parserRegionFilter");
  const dealEl = document.getElementById("parserDealFilter");
  const propertyEl = document.getElementById("parserPropertyFilter");
  const sourceEl = document.getElementById("parserSourceFilter");
  const searchInput = document.getElementById("parserSearchInput");
  const updatedFrom = document.getElementById("parserUpdatedFrom");
  const updatedTo = document.getElementById("parserUpdatedTo");
  const onlyNew = document.getElementById("parserOnlyNew");
  const onlyHot = document.getElementById("parserOnlyHot");
  const onlyOwner = document.getElementById("parserOnlyOwner");
  const onlyDuplicates = document.getElementById("parserOnlyDuplicates");
  const onlyPriceDrop = document.getElementById("parserOnlyPriceDrop");

  if (regionEl instanceof HTMLSelectElement) regionEl.value = filters.region;
  if (dealEl instanceof HTMLSelectElement) dealEl.value = filters.dealType;
  if (propertyEl instanceof HTMLSelectElement) propertyEl.value = filters.propertyType;
  if (sourceEl instanceof HTMLSelectElement) sourceEl.value = filters.source;
  if (searchInput instanceof HTMLInputElement) searchInput.value = state.parserResults.query || "";
  if (updatedFrom instanceof HTMLInputElement) updatedFrom.value = filters.updatedFrom || "";
  if (updatedTo instanceof HTMLInputElement) updatedTo.value = filters.updatedTo || "";
  if (onlyNew instanceof HTMLInputElement) onlyNew.checked = Boolean(filters.onlyNew);
  if (onlyHot instanceof HTMLInputElement) onlyHot.checked = Boolean(filters.onlyHot);
  if (onlyOwner instanceof HTMLInputElement) onlyOwner.checked = Boolean(filters.onlyOwner);
  if (onlyDuplicates instanceof HTMLInputElement) onlyDuplicates.checked = Boolean(filters.onlyDuplicates);
  if (onlyPriceDrop instanceof HTMLInputElement) onlyPriceDrop.checked = Boolean(filters.onlyPriceDrop);
}

function resetParserFilters() {
  state.parserResults.filters = getDefaultParserFilters();
  state.parserResults.query = "";
  state.parserResults.quickTab = "all";
  setParserQuickTab("all");
  applyParserFiltersToForm();
}

function setParserQuickTab(tab) {
  const nextTab = tab === "owners" && !featureOwnerIntel ? "all" : tab;
  state.parserResults.quickTab = nextTab;
  document.querySelectorAll(".parser-tab").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.parserTab === nextTab);
  });
  toggleParserView(nextTab === "owners");
}

function isParserHot(record) {
  const score = getLeadScoreValue(record.payload);
  return score != null && score >= 80;
}

function isParserOwner(record) {
  const label = resolveOwnerLabel(record.payload);
  return label.key === "owner";
}

function isParserDuplicate(record) {
  return record.status === "duplicate" || record.status === "possible_duplicate" || Boolean(record.duplicate_of_id);
}

function isParserPriceDrop(record) {
  const breakdown = getLeadBreakdown(record.payload);
  if (record && record.payload && record.payload.below_market_flag) return true;
  if (coalesce(breakdown.under_market_score, 0) >= 65) return true;
  const text = normalizeText(`${record.title || ""} ${record.description || ""}`);
  return text.includes("сниж") || text.includes("скид") || text.includes("ниже рынка");
}

function isParserArchive(record) {
  return record.payload && record.payload.monetization_tier === "archive";
}

function applyParserClientFilters(results) {
  const filters = state.parserResults.filters;
  const quickTab = state.parserResults.quickTab;
  const filtered = results.filter((record) => {
    if (filters.onlyNew && record.status !== "new") return false;
    if (filters.onlyHot && !isParserHot(record)) return false;
    if (filters.onlyOwner && !isParserOwner(record)) return false;
    if (filters.onlyDuplicates && !isParserDuplicate(record)) return false;
    if (filters.onlyPriceDrop && !isParserPriceDrop(record)) return false;
    if (filters.source && record.source_channel !== filters.source) return false;
    if (filters.dealType && record.listing_type !== filters.dealType) return false;
    if (filters.propertyType) {
      const prop = resolvePropertyType(record);
      if (prop.key !== filters.propertyType) return false;
    }
    if (filters.region) {
      const regionText = `${record.region_code || ""} ${record.city || ""}`.toLowerCase();
      if (!regionText.includes(filters.region.toLowerCase())) return false;
    }
    if (quickTab === "new" && record.status !== "new") return false;
    if (quickTab === "hot" && !isParserHot(record)) return false;
    if (quickTab === "owners" && !isParserOwner(record)) return false;
    if (quickTab === "duplicates" && !isParserDuplicate(record)) return false;
    if (quickTab === "archive" && !isParserArchive(record)) return false;
    return true;
  });
  return filtered.sort((a, b) => {
    const scoreA = getLeadScoreValue(a.payload) || 0;
    const scoreB = getLeadScoreValue(b.payload) || 0;
    if (scoreB !== scoreA) return scoreB - scoreA;
    const ownerA = getOwnerIntelScore(a.payload) || 0;
    const ownerB = getOwnerIntelScore(b.payload) || 0;
    if (ownerB !== ownerA) return ownerB - ownerA;
    const dateA = new Date(a.updated_at || 0).getTime();
    const dateB = new Date(b.updated_at || 0).getTime();
    return dateB - dateA;
  });
}

async function ensureUsersLoaded() {
  if (state.users.length) return state.users;
  try {
    const users = await api("/users");
    state.users = users;
    return users;
  } catch (error) {
    state.users = state.user ? [state.user] : [];
    return state.users;
  }
}

function populateOwnerSelect(users) {
  const selectEl = document.getElementById("leadOwnerSelect");
  if (!(selectEl instanceof HTMLSelectElement)) return;
  selectEl.innerHTML = "";
  const emptyOpt = document.createElement("option");
  emptyOpt.value = "";
  emptyOpt.textContent = "Не назначен";
  selectEl.appendChild(emptyOpt);
  users.forEach((user) => {
    const opt = document.createElement("option");
    opt.value = String(user.id);
    opt.textContent = user.full_name || user.email || `ID ${user.id}`;
    selectEl.appendChild(opt);
  });
}

function updateLeadCardMeta(lead) {
  const titleEl = document.getElementById("leadCardTitle");
  const metaEl = document.getElementById("leadCardMeta");
  if (!lead) {
    if (titleEl) titleEl.textContent = "";
    if (metaEl) metaEl.textContent = "";
    return;
  }
  if (titleEl) titleEl.textContent = `#${lead.id}`;
  if (metaEl) {
    const statusLabel = leadStatusLabels[lead.status] || lead.status;
    metaEl.textContent = `Статус: ${statusLabel} • Создан: ${formatDateTime(lead.created_at)} • Обновлен: ${formatDateTime(
      lead.updated_at
    )}`;
  }
}

function fillLeadForm(lead) {
  const titleInput = document.getElementById("leadTitleInput");
  const phoneInput = document.getElementById("leadPhoneInput");
  const sourceSelect = document.getElementById("leadSourceSelect");
  const needSelect = document.getElementById("leadNeedSelect");
  const districtSelect = document.getElementById("leadDistrictSelect");
  const addressSelect = document.getElementById("leadAddressSelect");
  const propertyTypeSelect = document.getElementById("leadPropertyTypeSelect");
  const areaRangeSelect = document.getElementById("leadAreaRangeSelect");
  const activitySelect = document.getElementById("leadActivitySelect");
  const urgencySelect = document.getElementById("leadUrgencySelect");
  const sourceDetailsInput = document.getElementById("leadSourceDetailsInput");
  const ownerSelect = document.getElementById("leadOwnerSelect");

  if (titleInput) titleInput.value = (lead && lead.title) || "";
  if (phoneInput) phoneInput.value = formatRussianPhone((lead && lead.contact_phone) || "");
  if (sourceDetailsInput) sourceDetailsInput.value = (lead && lead.source_details) || "";

  setSelectValue(sourceSelect, lead ? lead.lead_source : undefined);
  setSelectValue(needSelect, lead ? lead.need_type : undefined);
  setSelectValue(addressSelect, lead ? lead.object_address : undefined);
  setSelectValue(propertyTypeSelect, lead ? lead.property_type : undefined);
  setSelectValue(areaRangeSelect, lead ? lead.area_range : undefined);
  setSelectValue(activitySelect, lead ? lead.business_activity : undefined);
  setSelectValue(urgencySelect, lead ? lead.urgency : undefined);
  const districtValue = Array.isArray(lead && lead.search_districts)
    ? lead.search_districts[0]
    : lead
      ? lead.search_districts
      : undefined;
  setSelectValue(districtSelect, districtValue);

  if (ownerSelect instanceof HTMLSelectElement) {
    ownerSelect.value = lead && lead.owner_user_id ? String(lead.owner_user_id) : "";
  }

  if (titleInput) setFieldError(titleInput, false);
}

function readLeadForm() {
  const titleInput = document.getElementById("leadTitleInput");
  const phoneInput = document.getElementById("leadPhoneInput");
  const sourceSelect = document.getElementById("leadSourceSelect");
  const needSelect = document.getElementById("leadNeedSelect");
  const districtSelect = document.getElementById("leadDistrictSelect");
  const addressSelect = document.getElementById("leadAddressSelect");
  const propertyTypeSelect = document.getElementById("leadPropertyTypeSelect");
  const areaRangeSelect = document.getElementById("leadAreaRangeSelect");
  const activitySelect = document.getElementById("leadActivitySelect");
  const urgencySelect = document.getElementById("leadUrgencySelect");
  const sourceDetailsInput = document.getElementById("leadSourceDetailsInput");
  const ownerSelect = document.getElementById("leadOwnerSelect");

  const districtValue = getSelectValue(districtSelect);
  const payload = {
    title: titleInput ? titleInput.value.trim() : "",
    contact_phone: phoneInput ? phoneInput.value.trim() : null,
    lead_source: getSelectValue(sourceSelect),
    need_type: getSelectValue(needSelect),
    search_districts: districtValue ? [districtValue] : null,
    object_address: getSelectValue(addressSelect),
    property_type: getSelectValue(propertyTypeSelect),
    area_range: getSelectValue(areaRangeSelect),
    business_activity: getSelectValue(activitySelect),
    urgency: getSelectValue(urgencySelect),
    source_details: sourceDetailsInput ? sourceDetailsInput.value.trim() : null,
    owner_user_id: ownerSelect && ownerSelect.value ? Number(ownerSelect.value) : null,
  };

  if (Number.isNaN(payload.owner_user_id)) payload.owner_user_id = null;
  return payload;
}

function renderLeadStageBar(lead) {
  const stageBar = document.getElementById("leadStageBar");
  if (!stageBar) return;
  stageBar.innerHTML = "";

  const currentIndex = leadStages.findIndex(([key]) => key === lead.status);
  leadStages.forEach(([key, label], index) => {
    const stageEl = document.createElement("div");
    stageEl.className = "lead-stage";
    if (index < currentIndex) stageEl.classList.add("done");
    if (index === currentIndex) stageEl.classList.add("active");
    stageEl.textContent = label;

    if (canEditLead()) {
      stageEl.role = "button";
      stageEl.tabIndex = 0;
      stageEl.addEventListener("click", () => updateLeadStatus(key));
      stageEl.addEventListener("keydown", (event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          updateLeadStatus(key);
        }
      });
    }
    stageBar.appendChild(stageEl);
  });
}

function renderLeadEvents(events) {
  const body = document.getElementById("leadEventTableBody");
  if (!body) return;
  body.innerHTML = "";

  if (!events || !events.length) {
    body.innerHTML = '<tr><td colspan="4" class="muted">История пока пустая.</td></tr>';
    return;
  }

  const eventTypeLabels = {
    created: "Создание",
    status_changed: "Этап",
    field_changed: "Изменение поля",
    owner_changed: "Ответственный",
    comment: "Комментарий",
    system: "Системное событие",
  };

  const sorted = [...events].sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
  sorted.forEach((event) => {
    const label = eventTypeLabels[event.event_type] || event.event_type;
    const author = event.author_name || "Система";
    const row = document.createElement("tr");
    row.className = `lead-event-row ${event.event_type}`;
    row.innerHTML = `
      <td>${escapeHtml(label)}</td>
      <td>${escapeHtml(author)}</td>
      <td>${escapeHtml(formatDateTime(event.created_at))}</td>
      <td>${escapeHtml(event.message || "")}</td>
    `;
    body.appendChild(row);
  });
}

function setLeadCardTab(tabName) {
  document.querySelectorAll(".lead-tab-btn").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.leadTab === tabName);
  });
  document.querySelectorAll(".lead-card-pane").forEach((pane) => {
    pane.classList.toggle("active", pane.id === `leadTab${tabName === "history" ? "History" : "Params"}`);
  });
}

async function createLead() {
  if (!canEditLead()) return;
  const now = new Date();
  const dateLabel = now.toLocaleDateString("ru-RU");
  const timeLabel = now.toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" });
  const payload = { title: `Новый лид ${dateLabel} ${timeLabel}` };
  try {
    const lead = await api("/leads", { method: "POST", body: JSON.stringify(payload) });
    await Promise.all([loadLeadsKanban(), loadDashboard()]);
    await openLeadCard(lead.id);
  } catch (error) {
    alert(`Не удалось создать лид: ${error.message}`);
  }
}

async function refreshLeadEvents(leadId) {
  const events = await api(`/leads/${leadId}/events`);
  state.leadCard.events = events;
  renderLeadEvents(events);
}

async function reloadLeadCard(leadId) {
  const [lead, events] = await Promise.all([api(`/leads/${leadId}`), api(`/leads/${leadId}/events`)]);
  state.leadCard.lead = lead;
  state.leadCard.original = cloneJson(lead);
  state.leadCard.events = events;
  fillLeadForm(lead);
  renderLeadEvents(events);
  updateLeadCardMeta(lead);
}

async function openLeadCard(leadId) {
  const modal = document.getElementById("leadCardModal");
  if (!modal) return;
  try {
    populateLeadSelects();
    const users = await ensureUsersLoaded();
    populateOwnerSelect(users);
    await reloadLeadCard(leadId);
    setLeadCardTab("params");
    modal.classList.add("active");
    const editable = canEditLead();
    const saveBtn = document.getElementById("leadCardSaveBtn");
    if (saveBtn instanceof HTMLButtonElement) saveBtn.disabled = !editable;
  } catch (error) {
    alert(`Не удалось открыть карточку лида: ${error.message}`);
  }
}

function closeLeadCard() {
  const modal = document.getElementById("leadCardModal");
  if (modal) modal.classList.remove("active");
  state.leadCard.lead = null;
  state.leadCard.original = null;
  state.leadCard.events = [];
}

function cancelLeadCardChanges() {
  if (!state.leadCard.original) return;
  fillLeadForm(state.leadCard.original);
}

async function updateLeadStatus(statusKey) {
  const lead = state.leadCard.lead;
  if (!lead || lead.status === statusKey || !canEditLead()) return;
  try {
    await api(`/leads/${lead.id}/status`, { method: "PATCH", body: JSON.stringify({ status: statusKey }) });
    await reloadLeadCard(lead.id);
    await Promise.all([loadLeadsKanban(), loadDashboard()]);
  } catch (error) {
    alert(`Не удалось обновить этап: ${error.message}`);
  }
}

async function saveLeadCard() {
  const lead = state.leadCard.lead;
  if (!lead || !canEditLead()) return;
  const titleInput = document.getElementById("leadTitleInput");
  const phoneInput = document.getElementById("leadPhoneInput");
  if (phoneInput instanceof HTMLInputElement) {
    phoneInput.value = formatRussianPhone(phoneInput.value);
  }
  const payload = readLeadForm();
  if (!payload.title) {
    setFieldError(titleInput, true);
    if (titleInput) titleInput.focus();
    return;
  }
  setFieldError(titleInput, false);
  if (phoneInput instanceof HTMLInputElement && payload.contact_phone) {
    if (!isValidRussianPhone(phoneInput.value)) {
      setFieldError(phoneInput, true);
      phoneInput.focus();
      alert("Введите номер в формате +7 (XXX) XXX-XX-XX.");
      return;
    }
    setFieldError(phoneInput, false);
  }

  try {
    const updated = await api(`/leads/${lead.id}`, { method: "PATCH", body: JSON.stringify(payload) });
    state.leadCard.lead = updated;
    state.leadCard.original = cloneJson(updated);
    fillLeadForm(updated);
    updateLeadCardMeta(updated);
    await refreshLeadEvents(updated.id);
    await Promise.all([loadLeadsKanban(), loadDashboard()]);
  } catch (error) {
    alert(`Не удалось сохранить лид: ${error.message}`);
  }
}

async function submitLeadComment(event) {
  event.preventDefault();
  const lead = state.leadCard.lead;
  if (!lead) return;
  const input = document.getElementById("leadCommentInput");
  const message = input ? input.value.trim() : "";
  if (!message) return;
  try {
    await api(`/leads/${lead.id}/comments`, { method: "POST", body: JSON.stringify({ message }) });
    if (input) input.value = "";
    await refreshLeadEvents(lead.id);
  } catch (error) {
    alert(`Не удалось добавить комментарий: ${error.message}`);
  }
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
    col.innerHTML = `<h3>${label} (${(grouped[statusKey] && grouped[statusKey].length) || 0})</h3>`;
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
        state.dragSuppressUntil = Date.now() + 250;
      });
      card.addEventListener("dragend", () => {
        state.dragEntity = null;
      });
      card.addEventListener("click", () => {
        if (Date.now() < state.dragSuppressUntil) return;
        if (kind === "lead") openLeadCard(item.id);
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
    cards.innerHTML = `<div class="card"><div class="label">Dashboard unavailable</div><div>${escapeHtml(
      error.message
    )}</div></div>`;
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
  if (state.parserResults.quickTab === "owners" && !featureOwnerIntel) {
    state.parserResults.quickTab = "all";
  }
  if (typeof options.query === "string") {
    state.parserResults.query = options.query.trim();
    const searchInput = document.getElementById("parserSearchInput");
    if (searchInput instanceof HTMLInputElement) {
      searchInput.value = state.parserResults.query;
    }
  }
  if (!options.skipSync) {
    readParserFiltersFromForm();
  }

  const query = state.parserResults.query;
  const filters = state.parserResults.filters;
  const params = new URLSearchParams();
  params.set("page", String(state.parserResults.page));
  params.set("page_size", String(state.parserResults.pageSize));
  if (query) {
    params.set("q", query);
  }
  if (filters.source) params.set("source", filters.source);
  if (filters.dealType) params.set("deal_type", filters.dealType);
  if (filters.region) params.set("region", filters.region);
  if (filters.updatedFrom) params.set("updated_from", filters.updatedFrom);
  if (filters.updatedTo) params.set("updated_to", filters.updatedTo);
  if (filters.onlyNew || state.parserResults.quickTab === "new") params.set("status", "new");
  if (filters.onlyDuplicates || state.parserResults.quickTab === "duplicates") {
    params.set("duplicates_only", "true");
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

  state.parserResults.items = results;
  const filteredResults = applyParserClientFilters(results);
  state.parserResults.filteredItems = filteredResults;

  const rows = document.getElementById("parserRows");
  if (rows) {
    rows.innerHTML = "";
    for (const record of filteredResults) {
      const tr = document.createElement("tr");
      const isRejected = record.status === "rejected";
      const isLead = record.status === "converted_to_lead";
      const isDeal = record.status === "converted_to_deal";
      const iconLead = `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 5a4 4 0 1 0 0 8 4 4 0 0 0 0-8Z"/><path d="M4 20a6.5 6.5 0 0 1 13 0"/><path d="M19 8v6"/><path d="M16 11h6"/></svg>`;
      const iconDeal = `<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="6" width="18" height="12" rx="2"/><path d="M3 10h18"/><path d="M8 14h4"/></svg>`;
      const iconHide = `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="m3 3 18 18"/><path d="M10.6 10.6a2 2 0 0 0 2.8 2.8"/><path d="M7.1 7.1A10.4 10.4 0 0 0 3 12s3.5 6 9 6a9.7 9.7 0 0 0 4.5-1.1"/><path d="M14.8 9.2A4 4 0 0 0 9.2 14.8"/><path d="M17 17c2.1-1.4 3.5-3.4 4-5-1.2-2.4-4-5-9-5-1 0-2 .1-3 .4"/></svg>`;
      const iconDetails = `<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="M12 10v6"/><path d="M12 7h.01"/></svg>`;
      tr.innerHTML = `
        <td>${buildPriorityCell(record)}</td>
        <td>${buildObjectCell(record)}</td>
        <td>${buildDealTypeCell(record)}</td>
        <td>${buildLocationCell(record)}</td>
        <td>${buildPriceCell(record)}</td>
        <td>${buildAreaCell(record)}</td>
        <td>${buildContactCell(record)}</td>
        <td>${buildSourceCell(record)}</td>
        <td>${escapeHtml(formatDateTime(record.updated_at))}</td>
        <td>${escapeHtml(formatParserStatus(record.status))}</td>
        <td class="actions">
          <button data-action="to-lead" data-id="${record.id}" class="icon-action" title="В лиды" aria-label="В лиды" ${isLead || isDeal ? "disabled" : ""}>
            ${iconLead}
          </button>
          <button data-action="to-deal" data-id="${record.id}" class="icon-action" title="В сделку" aria-label="В сделку" ${isDeal ? "disabled" : ""}>
            ${iconDeal}
          </button>
          <button data-action="reject" data-id="${record.id}" class="icon-action" title="Скрыть" aria-label="Скрыть" ${isRejected ? "disabled" : ""}>
            ${iconHide}
          </button>
          <button data-action="details" data-id="${record.id}" class="icon-action" title="Детали" aria-label="Детали">
            ${iconDetails}
          </button>
        </td>
      `;
      rows.appendChild(tr);
    }
  }

  const pageInfo = document.getElementById("parserPageInfo");
  if (pageInfo) {
    pageInfo.textContent = `Страница ${state.parserResults.page} из ${state.parserResults.pages}`;
  }
  const meta = document.getElementById("parserResultsMeta");
  if (meta) {
    const filteredCount = filteredResults.length;
    const base = `Найдено: ${state.parserResults.total}. На странице: ${filteredCount}.`;
    meta.textContent = filteredCount < results.length ? `${base} Фильтры применены.` : base;
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

function toggleParserView(ownerView) {
  const listingSection = document.getElementById("parserListingSection");
  const ownerSection = document.getElementById("parserOwnerSection");
  const showOwners = Boolean(ownerView && featureOwnerIntel);
  if (listingSection) listingSection.style.display = showOwners ? "none" : "block";
  if (ownerSection) ownerSection.style.display = showOwners ? "block" : "none";
}

function readOwnerFiltersFromForm() {
  const queryInput = document.getElementById("ownerSearchInput");
  const onlyHigh = document.getElementById("ownerOnlyHigh");
  const onlySingle = document.getElementById("ownerOnlySingle");
  const onlyNew = document.getElementById("ownerOnlyNew");
  const onlyLow = document.getElementById("ownerOnlyLowCompetition");
  state.ownerIntel.filters = {
    query: queryInput && queryInput.value ? queryInput.value.trim() : "",
    onlyHigh: Boolean(onlyHigh && onlyHigh.checked),
    onlySingle: Boolean(onlySingle && onlySingle.checked),
    onlyNew: Boolean(onlyNew && onlyNew.checked),
    onlyLowCompetition: Boolean(onlyLow && onlyLow.checked),
  };
}

function applyOwnerFiltersToForm() {
  const filters = state.ownerIntel.filters;
  const queryInput = document.getElementById("ownerSearchInput");
  if (queryInput) queryInput.value = filters.query || "";
  const onlyHigh = document.getElementById("ownerOnlyHigh");
  if (onlyHigh) onlyHigh.checked = Boolean(filters.onlyHigh);
  const onlySingle = document.getElementById("ownerOnlySingle");
  if (onlySingle) onlySingle.checked = Boolean(filters.onlySingle);
  const onlyNew = document.getElementById("ownerOnlyNew");
  if (onlyNew) onlyNew.checked = Boolean(filters.onlyNew);
  const onlyLow = document.getElementById("ownerOnlyLowCompetition");
  if (onlyLow) onlyLow.checked = Boolean(filters.onlyLowCompetition);
}

function resetOwnerFilters() {
  state.ownerIntel.filters = {
    query: "",
    onlyHigh: true,
    onlySingle: false,
    onlyNew: false,
    onlyLowCompetition: false,
  };
  applyOwnerFiltersToForm();
}

function filterOwnerItems(items) {
  const query = state.ownerIntel.filters.query.toLowerCase();
  if (!query) return items;
  return items.filter((item) => {
    const text = [
      item.display_value,
      item.display_name,
      item.key_value,
      ...(item.organizations || []),
    ]
      .join(" ")
      .toLowerCase();
    return text.includes(query);
  });
}

function buildOwnerExplanation(explanation) {
  if (!explanation) return "<span class=\"muted\">-</span>";
  const ownerSignals = explanation.owner_signals || [];
  const agentSignals = explanation.agent_signals || [];
  const graphSignals = explanation.graph_signals || [];
  const chips = [...ownerSignals.slice(0, 2), ...graphSignals.slice(0, 2), ...agentSignals.slice(0, 2)]
    .map((signal) => `<span class="owner-badge">${escapeHtml(signal)}</span>`)
    .join("");
  return chips || "<span class=\"muted\">-</span>";
}

async function loadOwnerContacts() {
  if (!featureOwnerIntel) {
    toggleParserView(false);
    return;
  }
  toggleParserView(true);
  readOwnerFiltersFromForm();
  const rows = document.getElementById("ownerRows");
  if (!rows) return;
  rows.innerHTML = "";
  try {
    const params = new URLSearchParams();
    if (state.ownerIntel.filters.onlyHigh) params.set("min_owner_score", "70");
    if (state.ownerIntel.filters.onlySingle) params.set("only_single_listing", "true");
    if (state.ownerIntel.filters.onlyNew) params.set("only_new_days", "7");
    if (state.ownerIntel.filters.onlyLowCompetition) params.set("only_low_competition", "true");
    params.set("limit", "200");
    const items = await api(`/parser/owner-contacts?${params.toString()}`);
    const filtered = filterOwnerItems(items || []);
    state.ownerIntel.items = filtered;
    for (const item of filtered) {
      const tr = document.createElement("tr");
      const contact = item.display_value || item.key_value;
      const probability = `${Math.round(item.owner_probability)}%`;
      const listingsLabel =
        item.active_listings != null && item.active_listings !== item.total_listings
          ? `${item.active_listings}/${item.total_listings}`
          : `${item.total_listings}`;
      const priorityScore =
        item.owner_priority_score != null ? Math.round(Number(item.owner_priority_score)) : null;
      tr.innerHTML = `
        <td>${escapeHtml(contact)}</td>
        <td><span class="owner-priority">${escapeHtml(probability)}</span></td>
        <td>${buildOwnerExplanation(item.explanation)}</td>
        <td>${escapeHtml(listingsLabel)}</td>
        <td>${item.unique_objects}</td>
        <td>${escapeHtml(item.region_cluster || "-")}</td>
        <td>${escapeHtml(item.owner_priority || "-")}${
          priorityScore != null ? ` <span class="muted">(${priorityScore})</span>` : ""
        }</td>
        <td class="actions">
        <button data-owner-id="${item.id}" class="icon-action" title="Детали" aria-label="Детали">
          <svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="M12 10v6"/><path d="M12 7h.01"/></svg>
        </button>
      </td>
    `;
      rows.appendChild(tr);
    }
    if (!filtered.length) {
      rows.innerHTML = `<tr><td colspan="8" class="muted">Нет данных</td></tr>`;
    }
  } catch (error) {
    rows.innerHTML = `<tr><td colspan="8" class="muted">Ошибка загрузки</td></tr>`;
  }
}

async function openOwnerDetail(contactId) {
  if (!featureOwnerIntel) return;
  if (!contactId) return;
  let data;
  try {
    data = await api(`/parser/owner-contacts/${contactId}`);
  } catch (error) {
    return;
  }
  const identity = data.identity;
  const main = document.getElementById("ownerDetailMain");
    if (main) {
      const fields = [
        ["Контакт", identity.display_value || identity.key_value],
        ["Класс", identity.final_class],
        ["Вероятность собственника", `${Math.round(identity.owner_probability)}%`],
        ["Агент вероятность", `${Math.round(identity.agent_probability)}%`],
        ["Уверенность", `${Math.round(identity.confidence * 100)}%`],
        ["Объявлений", identity.total_listings],
        ["Объектов", identity.unique_objects],
        ["Регион", identity.region_cluster || "-"],
        ["Приоритет", identity.owner_priority || "-"],
      ];
    main.innerHTML = fields.map(([label, value]) => buildDetailField(label, value)).join("");
  }
  const meta = document.getElementById("ownerDetailMeta");
  if (meta) {
    meta.textContent = `Источник: ${identity.source_diversity} • Активных: ${identity.active_listings}`;
  }
  const explanation = document.getElementById("ownerDetailExplanation");
  if (explanation) {
    const ownerSignals =
      data.explanation && data.explanation.owner_signals ? data.explanation.owner_signals : [];
    const agentSignals =
      data.explanation && data.explanation.agent_signals ? data.explanation.agent_signals : [];
    const graphSignals =
      data.explanation && data.explanation.graph_signals ? data.explanation.graph_signals : [];
    const summary = data.explanation && data.explanation.summary ? data.explanation.summary : "";
    const priorityReasons =
      data.explanation && data.explanation.priority_reasons ? data.explanation.priority_reasons : [];
    explanation.innerHTML = `
      <div class="owner-explanation">${ownerSignals.map((s) => `<span class="owner-badge">${escapeHtml(s)}</span>`).join("")}</div>
      <div class="owner-explanation">${graphSignals.map((s) => `<span class="owner-badge">${escapeHtml(s)}</span>`).join("")}</div>
      <div class="owner-explanation">${agentSignals.map((s) => `<span class="owner-badge">${escapeHtml(s)}</span>`).join("")}</div>
      <div class="owner-explanation">${priorityReasons
        .map((s) => `<span class="owner-badge">${escapeHtml(s)}</span>`)
        .join("")}</div>
      <div class="muted">${escapeHtml(summary)}</div>
    `;
  }
  const behavior = document.getElementById("ownerDetailBehavior");
  if (behavior) {
    const metrics = data.explanation && data.explanation.behavior ? data.explanation.behavior : {};
    const graphMetrics =
      data.graph_features || (data.explanation && data.explanation.graph_features ? data.explanation.graph_features : {});
    const rows = [
      ["Период активности", metrics.span_days ? `${metrics.span_days} дн.` : "-"],
      ["Частота (мес.)", coalesce(metrics.posting_frequency_per_month, "-")],
      ["Репосты", coalesce(metrics.repost_rate, "-")],
      ["Повтор объектов", coalesce(metrics.object_reuse_rate, "-")],
      ["Кросс-источники", coalesce(metrics.cross_source_dup_rate, "-")],
      ["Шаблонность", coalesce(metrics.template_ratio, "-")],
      ["Гео кластер", coalesce(metrics.geo_cluster_ratio, "-")],
      ["Хаб-оценка", coalesce(graphMetrics.hub_score, "-")],
      ["Гео-спред", coalesce(graphMetrics.geographic_spread_score, "-")],
      ["Концентрация", coalesce(graphMetrics.single_asset_concentration_score, "-")],
      ["Плотность", coalesce(graphMetrics.cluster_density_score, "-")],
    ];
    behavior.innerHTML =
      rows
        .map(
          ([label, value]) =>
            `<div class="owner-metric"><div class="label">${escapeHtml(label)}</div><div>${escapeHtml(String(value))}</div></div>`
        )
        .join("") || "<span class=\"muted\">Нет данных</span>";
    behavior.classList.add("owner-metrics");
  }
  const organizations = document.getElementById("ownerDetailOrganizations");
  if (organizations) {
    const orgs =
      data.linked_organizations ||
      (data.identity ? data.identity.organizations : null) ||
      (data.explanation && data.explanation.organization_signals
        ? data.explanation.organization_signals.organizations
        : null) ||
      [];
    organizations.innerHTML = orgs.length
      ? `<div class="owner-badges">${orgs
          .map((org) =>
            typeof org === "string" ? `<span class="owner-badge">${escapeHtml(org)}</span>` : `<span class="owner-badge">${escapeHtml(org.label || org.entity_id || "")}</span>`
          )
          .join("")}</div>`
      : "<span class=\"muted\">Нет данных</span>";
  }
  const addresses = document.getElementById("ownerDetailAddresses");
  if (addresses) {
    const items = data.linked_addresses || [];
    addresses.innerHTML = items.length
      ? `<div class="owner-badges">${items
          .map((addr) =>
            typeof addr === "string" ? `<span class="owner-badge">${escapeHtml(addr)}</span>` : `<span class="owner-badge">${escapeHtml(addr.label || addr.entity_id || "")}</span>`
          )
          .join("")}</div>`
      : "<span class=\"muted\">Нет данных</span>";
  }
  const objects = document.getElementById("ownerDetailObjects");
  if (objects) {
    objects.innerHTML = (data.objects || [])
      .map((obj) => `<div class="drawer-list"><div>${escapeHtml(obj.address || "-")}</div><div class="muted">${escapeHtml(String(obj.area_sqm || "-"))} м² • ${escapeHtml(String(obj.count || 0))} шт</div></div>`)
      .join("") || "<span class=\"muted\">Нет данных</span>";
  }
  const timeline = document.getElementById("ownerDetailTimeline");
  if (timeline) {
    timeline.innerHTML = (data.activity_timeline || [])
      .map((row) => `<div class="drawer-list"><div>${escapeHtml(row.date)}</div><div class="muted">${row.count} событий</div></div>`)
      .join("") || "<span class=\"muted\">Нет данных</span>";
  }
  const listings = document.getElementById("ownerDetailListings");
  if (listings) {
    listings.innerHTML = (data.listings || [])
      .map((item) => `<div class="drawer-list"><div>${escapeHtml(item.title)}</div><div class="muted">${escapeHtml(item.source_channel)} • ${escapeHtml(formatDateTime(item.updated_at))}</div></div>`)
      .join("") || "<span class=\"muted\">Нет объявлений</span>";
  }
  const drawer = document.getElementById("ownerDetailDrawer");
  if (drawer) drawer.classList.add("active");
  const evidence = document.getElementById("ownerDetailEvidence");
  if (evidence) {
    const rows = data.graph_evidence || [];
    evidence.innerHTML = rows.length
      ? rows
          .map(
            (row) =>
              `<div class="drawer-list"><div>${escapeHtml(
                row.description || row.evidence_type || "-"
              )}</div><div class="muted">${escapeHtml(row.observed_at || "")}</div></div>`
          )
          .join("")
      : '<span class="muted">Нет данных</span>';
  }
}

function closeOwnerDetail() {
  const drawer = document.getElementById("ownerDetailDrawer");
  if (drawer) drawer.classList.remove("active");
}

function findParserRecordById(id) {
  const recordId = Number(id);
  if (!recordId) return null;
  const all = state.parserResults.items || [];
  return all.find((item) => item.id === recordId) || null;
}

function buildDetailField(label, value) {
  const safeValue = value ? escapeHtml(String(value)) : "-";
  return `<div class="drawer-field"><span>${escapeHtml(label)}</span><strong>${safeValue}</strong></div>`;
}

function buildDetailFieldHtml(label, htmlValue) {
  const value = htmlValue || "-";
  return `<div class="drawer-field"><span>${escapeHtml(label)}</span><strong>${value}</strong></div>`;
}

function renderParserDuplicates() {
  const container = document.getElementById("parserDetailDuplicates");
  if (!container) return;
  const duplicates = state.parserDetail.duplicates;
  if (duplicates === null) {
    container.innerHTML = `<div class="muted">Нажмите «Дубли», чтобы загрузить связанные объявления.</div>`;
    return;
  }
  if (!duplicates.length) {
    container.innerHTML = `<div class="muted">Дублей не найдено.</div>`;
    return;
  }
  const rows = duplicates
    .map((dup) => {
      const price = dup.price_rub ? `${formatMoney(dup.price_rub)} ₽` : "-";
      const area = formatArea(dup.area_sqm);
      const source = formatChannel(dup.source_channel);
      const phone = formatRussianPhone(dup.contact_phone || "");
      const contact = phone || dup.contact_name || "-";
      return `<div class="duplicate-row">
        <strong>#${dup.id}</strong>
        <span>${escapeHtml(source)}</span>
        <span>${escapeHtml(price)}</span>
        <span>${escapeHtml(area)}</span>
        <span>${escapeHtml(contact)}</span>
      </div>`;
    })
    .join("");
  container.innerHTML = `<div class="duplicate-list">${rows}</div>`;
}

function renderParserDetail(record) {
  const titleEl = document.getElementById("parserDetailTitle");
  const metaEl = document.getElementById("parserDetailMeta");
  if (titleEl) titleEl.textContent = record.title || `Объект #${record.id}`;
  if (metaEl) {
    metaEl.textContent = `ID ${record.id} • ${formatChannel(record.source_channel)} • ${formatDateTime(
      record.updated_at
    )}`;
  }

  const mainEl = document.getElementById("parserDetailMain");
  if (mainEl) {
    const priority = resolvePriority(record.payload);
    const property = resolvePropertyType(record);
    const contact = formatRussianPhone(record.contact_phone || "");
    const price = record.price_rub ? `${formatMoney(record.price_rub)} ₽` : "-";
    mainEl.innerHTML = [
      buildDetailField("Приоритет", priority.label),
      buildDetailField("Сделка", formatListingType(record.listing_type)),
      buildDetailField("Тип", property.label),
      buildDetailField("Площадь", formatArea(record.area_sqm)),
      buildDetailField("Цена", price),
      buildDetailField("Контакт", contact || "Телефон скрыт"),
      buildDetailField("Адрес", formatAddress(record) || "-"),
    ].join("");
  }

  const historyEl = document.getElementById("parserDetailHistory");
  if (historyEl) {
    historyEl.innerHTML = [
      buildDetailField("Создано", formatDateTime(record.created_at)),
      buildDetailField("Обновлено", formatDateTime(record.updated_at)),
      buildDetailField("Статус", formatParserStatus(record.status)),
      buildDetailField("История цены", "Нет данных"),
    ].join("");
  }

  const sourceEl = document.getElementById("parserDetailSource");
  if (sourceEl) {
    const rawUrl = record.raw_url || "";
    const tgUrl = record.telegram_post_url || "";
    const sourceName = formatChannel(record.source_channel);
    const sourceLink = rawUrl
      ? `<a href="${escapeHtml(rawUrl)}" target="_blank" rel="noreferrer">Открыть источник</a>`
      : "-";
    const tgLink = tgUrl ? `<a href="${escapeHtml(tgUrl)}" target="_blank" rel="noreferrer">Открыть пост</a>` : "-";
    sourceEl.innerHTML = [
      buildDetailField("Источник", sourceName),
      buildDetailFieldHtml("Ссылка", sourceLink),
      buildDetailFieldHtml("Telegram", tgLink),
    ].join("");
  }

  renderParserDuplicates();
}

async function loadParserDuplicates(recordId) {
  try {
    const duplicates = await api(`/parser/results/${recordId}/duplicates`);
    state.parserDetail.duplicates = Array.isArray(duplicates) ? duplicates : [];
  } catch (error) {
    state.parserDetail.duplicates = [];
  }
  renderParserDuplicates();
}

async function openParserDetail(recordId, focus = "main") {
  const record = findParserRecordById(recordId);
  if (!record) return;
  state.parserDetail.record = record;
  state.parserDetail.duplicates = null;
  renderParserDetail(record);
  const drawer = document.getElementById("parserDetailDrawer");
  if (drawer) drawer.classList.add("active");
  if (focus === "duplicates") {
    await loadParserDuplicates(record.id);
  }
}

function closeParserDetail() {
  const drawer = document.getElementById("parserDetailDrawer");
  if (drawer) drawer.classList.remove("active");
  state.parserDetail.record = null;
  state.parserDetail.duplicates = null;
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
  const canManageSources = ["admin", "manager"].includes(
    state.user && state.user.role ? state.user.role : ""
  );
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
      <td>${coalesce(call.duration_sec, "-")}</td>
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

async function loadDiscoverySeeds() {
  const rows = document.getElementById("discoverySeedRows");
  if (!rows) return;
  const seeds = await api("/parser/discovery/seeds");
  state.discoverySeeds = seeds;
  rows.innerHTML = "";
  const canManage = ["admin", "manager"].includes(state.user && state.user.role ? state.user.role : "");
  for (const seed of seeds) {
    const actionCell = canManage
      ? `<button data-seed-id="${seed.id}" data-seed-enabled="${String(!seed.enabled)}" class="secondary">
           ${seed.enabled ? "Выключить" : "Включить"}
         </button>`
      : "-";
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${seed.id}</td>
      <td>${escapeHtml(formatDiscoveryLabel(discoverySeedTypeLabels, seed.seed_type))}</td>
      <td>${escapeHtml(seed.value)}</td>
      <td>${escapeHtml(seed.region || "-")}</td>
      <td>${seed.priority}</td>
      <td>${seed.enabled ? "активен" : "пауза"}</td>
      <td class="actions">${actionCell}</td>
    `;
    rows.appendChild(tr);
  }
}

async function loadDiscoveryRuns() {
  const rows = document.getElementById("discoveryRunRows");
  if (!rows) return;
  const runs = await api("/parser/discovery/runs?limit=12");
  state.discoveryRuns = runs;
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
  const limit = state.discovery.limit || 50;
  const sources = await api(`/parser/discovery/sources?limit=${limit}`);
  state.discoveredSources = sources;
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

async function loadDiscoveryHub() {
  if (!state.user || state.user.role !== "admin") return;
  await Promise.all([loadDiscoverySeeds(), loadDiscoveryRuns(), loadDiscoveredSources()]);
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
    `;
    rows.appendChild(tr);
  }
}

async function loadAutonomyHub() {
  if (!state.user || state.user.role !== "admin") return;
  await Promise.all([loadAutonomySummary(), loadJobRuns(), loadAutonomySources()]);
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

async function loadUsers() {
  const rows = document.getElementById("userRows");
  rows.innerHTML = "";
  const users = await api("/users");
  state.users = users;
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
  if (tabName === "parser") {
    startParserAutoRefresh();
  } else {
    stopParserAutoRefresh();
  }
  if (tabName === "home") await loadDashboard();
  if (tabName === "leads") await loadLeadsKanban();
  if (tabName === "deals") await loadDealsKanban();
  if (tabName === "parser") {
    if (state.parserResults.quickTab === "owners" && featureOwnerIntel) {
      await loadOwnerContacts();
    } else {
      await loadParserHub();
    }
  }
  if (tabName === "calls") await loadCalls();
  if (tabName === "users") await loadUsers();
}

async function boot() {
  window.__bootStage = "auth_start";
  if (window.__setBootStatus) {
    window.__setBootStatus("Boot: auth");
  }
  const me = await api("/auth/me", { timeoutMs: 8000 });
  state.user = me.user;
  window.__bootStage = "auth_ok";
  document.getElementById("currentUserInfo").textContent = `${me.user.full_name} (${me.user.role})`;

  if (window.__setBootStatus) {
    window.__setBootStatus("Boot: ui");
  }
  window.__bootStage = "ui_bind";

  const ownerTabBtn = document.querySelector('.parser-tab[data-parser-tab="owners"]');
  if (ownerTabBtn instanceof HTMLElement) {
    ownerTabBtn.style.display = featureOwnerIntel ? "inline-flex" : "none";
  }
  if (!featureOwnerIntel) {
    state.parserResults.quickTab = "all";
    toggleParserView(false);
  }

  if (me.user.role !== "admin") {
    const usersTab = document.getElementById("usersTabBtn");
    if (usersTab) usersTab.style.display = "none";
  }
  const parserSettingsLink = document.getElementById("parserSettingsLink");
  if (parserSettingsLink) {
    parserSettingsLink.style.display = me.user.role === "admin" ? "inline-flex" : "none";
  }
  const canManageDiscovery = me.user.role === "admin";
  const seedFormWrap = document.getElementById("discoverySeedFormWrap");
  if (seedFormWrap && !canManageDiscovery) {
    seedFormWrap.style.display = "none";
  }
  const discoveryRunBtn = document.getElementById("discoveryRunBtn");
  if (discoveryRunBtn && !canManageDiscovery) {
    discoveryRunBtn.style.display = "none";
  }
  const discoverySection = document.getElementById("discoverySection");
  if (discoverySection && !canManageDiscovery) {
    discoverySection.style.display = "none";
  }
  const autonomySection = document.getElementById("autonomySection");
  if (autonomySection && !canManageDiscovery) {
    autonomySection.style.display = "none";
  }
  populateLeadSelects();
  populateParserFilters();
  applyParserFiltersToForm();
  applyOwnerFiltersToForm();
  setParserQuickTab(state.parserResults.quickTab);

  document.querySelectorAll(".tab-btn[data-tab]").forEach((btn) => {
    btn.addEventListener("click", () => onTabChange(btn.dataset.tab));
  });

  document.getElementById("logoutBtn").addEventListener("click", async () => {
    try {
      await api("/auth/logout", { method: "POST" });
    } catch (error) {
      // ignore logout transport issues
    }
    clearStoredToken();
    window.location.href = "/login";
  });

  document.getElementById("refreshLeadsBtn").addEventListener("click", loadLeadsKanban);
  document.getElementById("createLeadBtn").addEventListener("click", createLead);
  document.getElementById("refreshDealsBtn").addEventListener("click", loadDealsKanban);
  document.getElementById("refreshCallsBtn").addEventListener("click", loadCalls);
  const refreshParserBtn = document.getElementById("refreshParserHubBtn");
  if (refreshParserBtn) {
    refreshParserBtn.addEventListener("click", async () => {
      if (state.parserResults.quickTab === "owners" && featureOwnerIntel) {
        await loadOwnerContacts();
      } else {
        await loadParserHub();
      }
    });
  }
  const refreshAutonomyBtn = document.getElementById("refreshAutonomyBtn");
  if (refreshAutonomyBtn) {
    refreshAutonomyBtn.addEventListener("click", async () => {
      await loadAutonomyHub();
    });
  }
  if (discoveryRunBtn) {
    discoveryRunBtn.addEventListener("click", runDiscoveryNow);
  }
  const discoverySeedForm = document.getElementById("discoverySeedForm");
  if (discoverySeedForm) {
    discoverySeedForm.addEventListener("submit", createDiscoverySeed);
  }
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
    discoveryLimitSelect.addEventListener("change", async () => {
      state.discovery.limit = Number(discoveryLimitSelect.value || 50);
      await loadDiscoveredSources();
    });
  }
  const autonomyLimitSelect = document.getElementById("autonomySourceLimit");
  if (autonomyLimitSelect) {
    autonomyLimitSelect.addEventListener("change", async () => {
      state.autonomy.sourceLimit = Number(autonomyLimitSelect.value || 50);
      await loadAutonomySources();
    });
  }

  const modal = document.getElementById("leadCardModal");
  if (modal) {
    modal.addEventListener("click", (event) => {
      const target = event.target;
      if (target instanceof HTMLElement && target.dataset.action === "close") {
        closeLeadCard();
      }
    });
  }

  document.getElementById("leadCardCloseBtn").addEventListener("click", closeLeadCard);
  document.getElementById("leadCardCloseBtnBottom").addEventListener("click", closeLeadCard);
  document.getElementById("leadCardSaveBtn").addEventListener("click", saveLeadCard);
  document.getElementById("leadCommentForm").addEventListener("submit", submitLeadComment);
  document.querySelectorAll(".lead-tab-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      const tabName = btn.dataset.leadTab;
      if (tabName) setLeadCardTab(tabName);
    });
  });

  const parserDrawer = document.getElementById("parserDetailDrawer");
  if (parserDrawer) {
    parserDrawer.addEventListener("click", (event) => {
      const target = event.target;
      if (target instanceof HTMLElement && target.dataset.action === "close") {
        closeParserDetail();
      }
    });
  }
  const parserDetailCloseBtn = document.getElementById("parserDetailCloseBtn");
  if (parserDetailCloseBtn) {
    parserDetailCloseBtn.addEventListener("click", closeParserDetail);
  }

  const ownerDrawer = document.getElementById("ownerDetailDrawer");
  if (ownerDrawer) {
    ownerDrawer.addEventListener("click", (event) => {
      const target = event.target;
      if (target instanceof HTMLElement && target.dataset.action === "close") {
        closeOwnerDetail();
      }
    });
  }
  const ownerDetailCloseBtn = document.getElementById("ownerDetailCloseBtn");
  if (ownerDetailCloseBtn) {
    ownerDetailCloseBtn.addEventListener("click", closeOwnerDetail);
  }

  const titleInput = document.getElementById("leadTitleInput");
  if (titleInput) {
    titleInput.addEventListener("input", () => setFieldError(titleInput, !titleInput.value.trim()));
  }

  const phoneInput = document.getElementById("leadPhoneInput");
  if (phoneInput instanceof HTMLInputElement) {
    phoneInput.addEventListener("input", () => {
      const formatted = formatRussianPhone(phoneInput.value);
      phoneInput.value = formatted;
      const digits = getPhoneDigits(formatted);
      const shouldValidate = digits.length >= 11;
      setFieldError(phoneInput, shouldValidate && !isValidRussianPhone(formatted));
    });
  }

  const parserRows = document.getElementById("parserRows");
  if (parserRows) {
    parserRows.addEventListener("click", async (event) => {
      const target = event.target;
      if (!(target instanceof Element)) return;
      const button = target.closest("button");
      if (!button) return;
      const id = button.dataset.id;
      const action = button.dataset.action;
      if (!id || !action || button.disabled) return;
      if (action === "to-lead") await api(`/parser/results/${id}/to-lead`, { method: "POST", body: "{}" });
      if (action === "to-deal") await api(`/parser/results/${id}/to-deal`, { method: "POST", body: "{}" });
      if (action === "reject") await api(`/parser/results/${id}/reject`, { method: "POST", body: "{}" });
      if (action === "details") await openParserDetail(id, "main");
      if (["to-lead", "to-deal", "reject"].includes(action)) {
        await Promise.all([loadParserHub(), loadLeadsKanban(), loadDealsKanban(), loadDashboard()]);
      }
    });
  }

  const ownerRows = document.getElementById("ownerRows");
  if (ownerRows) {
    ownerRows.addEventListener("click", async (event) => {
      if (!featureOwnerIntel) return;
      const target = event.target;
      if (!(target instanceof Element)) return;
      const button = target.closest("button");
      if (!button) return;
      const ownerId = button.dataset.ownerId;
      if (!ownerId) return;
      await openOwnerDetail(ownerId);
    });
  }

  const parserFilterForm = document.getElementById("parserFilterForm");
  if (parserFilterForm) {
    parserFilterForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      state.parserResults.page = 1;
      readParserFiltersFromForm();
      await loadParserHub({ resetPage: true, skipSync: true });
    });
  }

  const ownerFilterForm = document.getElementById("ownerFilterForm");
  if (ownerFilterForm) {
    ownerFilterForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      await loadOwnerContacts();
    });
  }

  const parserFilterResetBtn = document.getElementById("parserFilterResetBtn");
  if (parserFilterResetBtn) {
    parserFilterResetBtn.addEventListener("click", async () => {
      resetParserFilters();
      state.parserResults.page = 1;
      await loadParserHub({ resetPage: true, skipSync: true });
    });
  }

  const ownerFilterResetBtn = document.getElementById("ownerFilterResetBtn");
  if (ownerFilterResetBtn) {
    ownerFilterResetBtn.addEventListener("click", async () => {
      resetOwnerFilters();
      await loadOwnerContacts();
    });
  }

  const parserQuickTabs = document.getElementById("parserQuickTabs");
  if (parserQuickTabs) {
    parserQuickTabs.addEventListener("click", async (event) => {
      const target = event.target;
      if (!(target instanceof HTMLButtonElement)) return;
      const tab = target.dataset.parserTab;
      if (!tab) return;
      if (tab === "owners" && !featureOwnerIntel) return;
      setParserQuickTab(tab);
      if (tab === "owners") {
        await loadOwnerContacts();
      } else {
        state.parserResults.page = 1;
        await loadParserHub({ resetPage: true });
      }
    });
  }

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

  const params = new URLSearchParams(window.location.search);
  const initialTab = params.get("tab");
  const hasTab = initialTab && document.querySelector(`.tab-btn[data-tab="${initialTab}"]`);
  onTabChange(hasTab ? initialTab : "home").catch(() => {});
  window.__bootStage = "boot_done";
}

boot()
  .then(() => {
    window.__appBooted = true;
    if (window.__setBootStatus) {
      window.__setBootStatus("JS: ok", true);
    }
  })
  .catch((error) => {
    if (window.__setBootStatus) {
      window.__setBootStatus(`JS error: ${error.message || "boot failed"}`);
    }
    alert(`Application boot error: ${error.message}`);
  });
