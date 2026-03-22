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

const existingToken = getStoredToken();

async function validateExistingToken() {
  if (!existingToken) return;
  try {
    const response = await fetch(`${apiPrefix}/auth/me`, {
      headers: { Authorization: `Bearer ${existingToken}` },
    });
    if (response.ok) {
      window.location.href = "/";
      return;
    }
  } catch (error) {
    // ignore, fallback to login
  }
  clearStoredToken();
}

validateExistingToken();

document.getElementById("loginForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const errorNode = document.getElementById("loginError");
  errorNode.textContent = "";

  const payload = Object.fromEntries(new FormData(event.target).entries());
  try {
    const response = await fetch(`${apiPrefix}/auth/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(text || `HTTP ${response.status}`);
    }
    const data = await response.json();
    setStoredToken(data.access_token);
    window.location.href = "/";
  } catch (error) {
    errorNode.textContent = `Ошибка входа: ${error.message}`;
  }
});
