const form = document.querySelector("#search-form");
const statusBox = document.querySelector("#status");
const results = document.querySelector("#results");
const refreshEvents = document.querySelector("#refresh-events");
const eventsStatus = document.querySelector("#events-status");
const eventsBody = document.querySelector("#events-body");

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  results.innerHTML = "";
  statusBox.textContent = "Recherche en cours...";

  const userId = new FormData(form).get("user-id");

  try {
    const response = await fetch(`http://localhost:8000/recommendations/user/${userId}`);
    if (!response.ok) {
      throw new Error("Aucune recommandation trouvee pour cet utilisateur.");
    }

    const data = await response.json();
    statusBox.textContent = `${data.recommendations.length} recommandations trouvees`;

    for (const productId of data.recommendations) {
      const item = document.createElement("li");
      item.textContent = productId;
      results.appendChild(item);
    }
  } catch (error) {
    statusBox.textContent = error.message;
  }
});

async function loadRecentEvents() {
  eventsBody.innerHTML = "";
  eventsStatus.textContent = "Chargement des evenements...";

  try {
    const response = await fetch("http://localhost:8000/events/recent?limit=20");
    if (!response.ok) {
      throw new Error("Impossible de charger les evenements.");
    }

    const data = await response.json();
    eventsStatus.textContent = `${data.events.length} evenements affiches`;

    for (const event of data.events) {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${event.user_id}</td>
        <td>${event.product_id}</td>
        <td>${event.score}</td>
        <td>${event.time}</td>
      `;
      eventsBody.appendChild(row);
    }
  } catch (error) {
    eventsStatus.textContent = error.message;
  }
}

refreshEvents.addEventListener("click", loadRecentEvents);
loadRecentEvents();
