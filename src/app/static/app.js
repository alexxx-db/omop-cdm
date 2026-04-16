const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

// ---------- tabs ----------
$$(".tab").forEach((btn) => {
  btn.addEventListener("click", () => {
    $$(".tab").forEach((b) => b.classList.remove("active"));
    $$(".view").forEach((v) => v.classList.remove("active"));
    btn.classList.add("active");
    $(`#view-${btn.dataset.view}`).classList.add("active");
  });
});

// ---------- whoami ----------
fetch("/api/me")
  .then((r) => r.json())
  .then((j) => {
    const mode = j.auth_mode === "obo" ? "OBO" : "local";
    $("#whoami").textContent = j.email ? `${j.email} · ${mode}` : `(${mode})`;
  })
  .catch(() => {});

// ---------- diagnostics ----------
$("#diagnostics-btn").addEventListener("click", async () => {
  const panel = $("#diagnostics-panel");
  const out = $("#diagnostics-out");
  panel.hidden = false;
  out.innerHTML = `<div class="loading">Running checks…</div>`;
  try {
    const j = await apiGet("/api/me?include_diagnostics=true");
    const rows = (j.checks || []).map(
      (c) => `<div class="check ${c.status}">
        <span class="badge">${c.status === "ok" ? "✓" : c.status === "warn" ? "!" : "✗"}</span>
        <span class="name">${c.name}</span>
        <span class="detail">${c.detail || ""}</span>
      </div>`
    );
    out.innerHTML = rows.join("") || `<div class="muted">No checks returned.</div>`;
  } catch (err) {
    renderError(out, err);
  }
});

// ---------- fetch helpers ----------
async function apiGet(path) {
  const resp = await fetch(path);
  if (!resp.ok) {
    let body;
    try { body = await resp.json(); } catch { body = { detail: resp.statusText }; }
    const msg = typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail);
    throw new Error(`${resp.status}: ${msg}`);
  }
  return resp.json();
}

function renderError(target, err) {
  target.innerHTML = `<div class="error">${err.message}</div>`;
}

function renderLoading(target, label = "Loading…") {
  target.innerHTML = `<div class="loading">${label}</div>`;
}

function renderTable(target, rows, columns) {
  if (!rows || rows.length === 0) {
    target.innerHTML = `<div class="muted">No results.</div>`;
    return;
  }
  const cols = columns || Object.keys(rows[0]);
  const head = cols.map((c) => `<th>${c}</th>`).join("");
  const body = rows
    .map((r) => "<tr>" + cols.map((c) => `<td>${formatCell(r[c])}</td>`).join("") + "</tr>")
    .join("");
  target.innerHTML = `<div style="overflow:auto;max-height:60vh"><table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}

function formatCell(v) {
  if (v === null || v === undefined) return `<span class="muted">—</span>`;
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

function renderKV(target, obj, keys) {
  const entries = (keys || Object.keys(obj)).map(
    (k) => `<dt>${k}</dt><dd>${formatCell(obj[k])}</dd>`
  );
  target.innerHTML = `<dl class="kv">${entries.join("")}</dl>`;
}

function qs(params) {
  const u = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== null && v !== undefined && v !== "") u.set(k, v);
  }
  const s = u.toString();
  return s ? `?${s}` : "";
}

// ---------- view 1: concept search ----------
$("#search-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const fd = new FormData(e.target);
  const out = $("#search-out");
  renderLoading(out);
  try {
    const rows = await apiGet(
      `/api/concepts/search${qs({ q: fd.get("q"), vocab: fd.get("vocab") })}`
    );
    renderTable(out, rows, [
      "concept_id",
      "concept_name",
      "vocabulary_id",
      "domain_id",
      "concept_class_id",
    ]);
  } catch (err) {
    renderError(out, err);
  }
});

// ---------- view 2: patient lookup ----------
$("#patient-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const fd = new FormData(e.target);
  const pid = fd.get("person_id");
  const summaryOut = $("#patient-summary");
  const drugsOut = $("#patient-drugs");
  renderLoading(summaryOut);
  renderLoading(drugsOut);
  try {
    const summary = await apiGet(`/api/patients/${pid}/summary`);
    renderKV(summaryOut, summary, [
      "person_id",
      "gender",
      "race",
      "ethnicity",
      "year_of_birth",
      "age_years",
      "n_visits",
      "n_conditions",
      "n_drug_exposures",
      "observation_start",
      "observation_end",
    ]);
  } catch (err) {
    renderError(summaryOut, err);
    drugsOut.innerHTML = "";
    return;
  }
  try {
    const drugs = await apiGet(`/api/patients/${pid}/drugs?limit=500`);
    renderTable(drugsOut, drugs, [
      "drug_era_start_date",
      "drug_era_end_date",
      "drug_name",
      "drug_exposure_count",
      "gap_days",
    ]);
  } catch (err) {
    renderError(drugsOut, err);
  }
});

// ---------- view 3: cohort sizer ----------
$("#cohort-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const fd = new FormData(e.target);
  const cid = fd.get("condition_concept_id");
  const out = $("#cohort-out");
  renderLoading(out);
  try {
    const result = await apiGet(`/api/cohorts/condition/${cid}/size`);
    out.innerHTML = `
      <div>Patients with concept ${result.condition_concept_id} or any descendant:</div>
      <p class="big-number">${result.n_patients.toLocaleString()}</p>
    `;
  } catch (err) {
    renderError(out, err);
  }
});

// ---------- view 4: top conditions ----------
async function loadTop() {
  const out = $("#top-out");
  const top_n = $("#top-form input").value || 20;
  renderLoading(out);
  try {
    const rows = await apiGet(`/api/conditions/top?top_n=${top_n}`);
    renderTable(out, rows, [
      "condition_concept_id",
      "condition_name",
      "n_patients",
      "n_occurrences",
    ]);
  } catch (err) {
    renderError(out, err);
  }
}

$("#top-form").addEventListener("submit", (e) => {
  e.preventDefault();
  loadTop();
});
loadTop();  // prime the tab on first load
