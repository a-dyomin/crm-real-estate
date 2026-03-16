const apiPrefix = document.body.dataset.apiPrefix;
const existingToken = localStorage.getItem("cre_token");

if (existingToken) {
  window.location.href = "/";
}

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
    localStorage.setItem("cre_token", data.access_token);
    window.location.href = "/";
  } catch (error) {
    errorNode.textContent = `Ошибка входа: ${error.message}`;
  }
});
