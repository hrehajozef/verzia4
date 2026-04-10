/**
 * detail.js – interaktivita pre detail stránku záznamu
 *
 * Funkcie:
 *  1. Zasúvací panel autorov
 *  2. Prepínanie viditeľnosti stĺpcov
 *  3. Nastaviteľná šírka stĺpcov (drag resize)
 *  4. Editovateľné bunky (Repozitár klik-na-edit; WOS/Scopus priamo contenteditable)
 *  5. Inline diff: original text s červenými mazaniami a zelenými vloženiami
 *  6. Sledovanie zmien a ukladanie (POST save-fields)
 *  7. Crossref dáta inline v tabuľke (fetch JSON → vyplní príslušné bunky)
 */

"use strict";

// ── Pomocné funkcie ──────────────────────────────────────────────────────────

function escHtml(str) {
  return (str || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// ── 1. Zasúvací panel autorov ────────────────────────────────────────────────

(function initPanel() {
  const panel = document.getElementById("authors-panel");
  const btn   = document.getElementById("panel-toggle-btn");
  if (!panel || !btn) return;

  let open = true;

  btn.addEventListener("click", () => {
    open = !open;
    panel.classList.toggle("collapsed", !open);
    btn.title = open ? "Skryť panel autorov" : "Zobraziť panel autorov";
    const arrow = btn.querySelector(".panel-arrow");
    if (arrow) arrow.textContent = open ? "◀" : "▶";
  });
})();

// ── 2. Prepínanie viditeľnosti stĺpcov ──────────────────────────────────────

(function initColumnToggle() {
  document.querySelectorAll(".col-toggle").forEach((cb) => {
    cb.addEventListener("change", () => {
      const colName = cb.dataset.col;
      const show    = cb.checked;

      document.querySelectorAll(`td.col-${colName}, th.col-${colName}`).forEach((el) => {
        el.style.display = show ? "" : "none";
      });
      const colEl = document.querySelector(`#detail-table colgroup .col-${colName}`);
      if (colEl) colEl.style.display = show ? "" : "none";
    });
  });
})();

// ── 3. Nastaviteľná šírka stĺpcov (drag resize) ─────────────────────────────

(function initColumnResize() {
  let state = null;

  document.querySelectorAll("#detail-table thead th").forEach((th) => {
    const handle = th.querySelector(".resize-handle");
    if (!handle) return;

    handle.addEventListener("mousedown", (e) => {
      e.preventDefault();
      state = { thIndex: th.cellIndex, startX: e.clientX, startW: th.offsetWidth };
      document.body.classList.add("col-resizing");
    });
  });

  document.addEventListener("mousemove", (e) => {
    if (!state) return;
    const newW = Math.max(60, state.startW + (e.clientX - state.startX));
    const cols = document.querySelectorAll("#detail-table colgroup col");
    if (cols[state.thIndex]) cols[state.thIndex].style.width = newW + "px";
  });

  document.addEventListener("mouseup", () => {
    if (!state) return;
    state = null;
    document.body.classList.remove("col-resizing");
  });
})();

// ── 4 + 5. Editovateľné bunky a inline diff ──────────────────────────────────

const changes = {};
const dmp = typeof diff_match_patch !== "undefined" ? new diff_match_patch() : null;

function updateSaveButton() {
  const count   = Object.keys(changes).length;
  const btn     = document.getElementById("save-all-btn");
  const counter = document.getElementById("changed-count");
  if (counter) counter.textContent = count;
  if (btn) btn.style.display = count > 0 ? "inline-block" : "none";
}

function trackChange(fieldKey, newVal, origVal) {
  if (!fieldKey) return;
  const empty    = !newVal || newVal === "—";
  const origEmpty = !origVal;
  if ((empty && origEmpty) || newVal === origVal) {
    delete changes[fieldKey];
  } else {
    changes[fieldKey] = empty ? "" : newVal;
  }
  updateSaveButton();
}

// ── Diff rendering pre Repozitár bunky ───────────────────────────────────────
// Zobrazuje originálny text s inline diff: mazania červené, vloženia zelené.
// Napr. original="Smith, Katy", proposed="Smith, Kathrine"
//   → "Smith, Kat" + červené "y" + zelené "hrine"

function renderRepozitarCell(td) {
  if (!td) return;
  const original = td.dataset.original || "";
  const proposed = td.dataset.proposed || "";
  const modified = changes[td.dataset.field];

  // Ručne upravená hodnota (používateľ písal)
  if (modified !== undefined) {
    td.innerHTML =
      `<span class="badge bg-warning text-dark me-1" style="font-size:.65rem">upravené</span>` +
      escHtml(modified || "—");
    td.classList.add("cell-modified");
    return;
  }

  td.classList.remove("cell-modified");

  if (!original && !proposed) {
    td.innerHTML = '<span class="text-muted">—</span>';
    return;
  }

  if (original && proposed && original !== proposed && dmp) {
    // Diff: ukáž čo sa zmení z original na proposed
    const diffs    = dmp.diff_main(original, proposed);
    dmp.diff_cleanupSemantic(diffs);
    const diffHtml = diffs
      .map(([type, text]) => {
        const esc = escHtml(text);
        if (type ===  1) return `<span class="diff-insert">${esc}</span>`;
        if (type === -1) return `<span class="diff-delete">${esc}</span>`;
        return esc;
      })
      .join("");
    td.innerHTML =
      diffHtml +
      ` <button class="fix-badge ms-1" onclick="window.acceptFix(this.closest('td'))"` +
      ` title="Prijať návrh">✓ opraviť</button>`;
    return;
  }

  if (proposed && !original) {
    td.innerHTML =
      `<span class="proposed-add">${escHtml(proposed)}</span>` +
      ` <button class="fix-badge ms-1" onclick="window.acceptFix(this.closest('td'))"` +
      ` title="Prijať návrh">✓ použiť</button>`;
    return;
  }

  td.textContent = original;
}

// ── Inicializácia Repozitár buniek (klik → edit) ─────────────────────────────

function initRepozitarCells() {
  document.querySelectorAll("td[data-col-type='repozitar']").forEach((td) => {
    td.style.cursor = "text";

    td.addEventListener("click", (e) => {
      if (e.target.classList.contains("fix-badge")) return;
      if (td.dataset.editing === "true") return;

      td.dataset.editing = "true";
      const val =
        changes[td.dataset.field] !== undefined
          ? changes[td.dataset.field]
          : (td.dataset.proposed || td.dataset.original || "");

      td.contentEditable = "true";
      td.textContent = val;
      td.style.outline = "2px solid #0d6efd";
      td.classList.add("editing");
      td.focus();

      try {
        const range = document.createRange();
        range.selectNodeContents(td);
        range.collapse(false);
        const sel = window.getSelection();
        sel.removeAllRanges();
        sel.addRange(range);
      } catch (_) {}
    });

    td.addEventListener("blur", () => {
      td.dataset.editing = "false";
      td.contentEditable = "false";
      td.style.outline   = "";
      td.classList.remove("editing");

      const newVal  = td.textContent.trim();
      const origVal = td.dataset.original || "";
      trackChange(td.dataset.field, newVal, origVal);
      renderRepozitarCell(td);
    });

    td.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        td.blur();
      }
      if (e.key === "Escape") {
        delete changes[td.dataset.field];
        updateSaveButton();
        td.dataset.editing = "false";
        td.contentEditable = "false";
        td.style.outline   = "";
        td.classList.remove("editing");
        renderRepozitarCell(td);
      }
    });
  });
}

// ── Inicializácia WOS / Scopus buniek ────────────────────────────────────────

function initSourceCells() {
  document.querySelectorAll("td[data-col-type='wos'], td[data-col-type='scopus']").forEach((td) => {
    const fieldKey = td.dataset.field || "";

    td.addEventListener("focus", () => {
      if (td.textContent.trim() === "—") td.textContent = "";
    });

    td.addEventListener("blur", () => {
      const newVal  = td.textContent.trim();
      const origVal = td.dataset.original || "";

      if (!newVal) td.innerHTML = '<span class="text-muted">—</span>';

      if (fieldKey) {
        trackChange(fieldKey, newVal, origVal);
        td.classList.toggle("cell-modified", changes[fieldKey] !== undefined);
      }
    });

    td.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); td.blur(); }
      if (e.key === "Escape") {
        const orig = td.dataset.original || "";
        if (!orig) {
          td.innerHTML = '<span class="text-muted">—</span>';
        } else {
          td.textContent = orig;
        }
        delete changes[fieldKey];
        updateSaveButton();
        td.classList.remove("cell-modified");
        td.blur();
      }
    });
  });
}

// ── Accept fix ───────────────────────────────────────────────────────────────

window.acceptFix = function (td) {
  if (!td) return;
  const proposed = td.dataset.proposed || "";
  const fieldKey = td.dataset.field    || "";
  if (!fieldKey) return;

  changes[fieldKey] = proposed;
  updateSaveButton();
  renderRepozitarCell(td);
};

// ── 6. Uloženie zmien ────────────────────────────────────────────────────────

window.saveAllChanges = async function () {
  const entries = Object.entries(changes);
  if (!entries.length) return;

  const btn = document.getElementById("save-all-btn");
  if (btn) { btn.disabled = true; btn.textContent = "Ukladám…"; }

  try {
    const resp = await fetch(`/record/${window.RESOURCE_ID}/save-fields`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ fields: Object.fromEntries(entries) }),
    });
    const data = await resp.json();

    if (data.ok) {
      Object.keys(changes).forEach((k) => delete changes[k]);
      updateSaveButton();
      document.querySelectorAll(".cell-modified").forEach((el) =>
        el.classList.remove("cell-modified")
      );
      if (btn) {
        btn.disabled = false;
        btn.textContent = "✓ Uložené";
        btn.classList.replace("btn-warning", "btn-success");
        setTimeout(() => {
          btn.classList.replace("btn-success", "btn-warning");
          btn.style.display = "none";
        }, 2000);
      }
    } else {
      const errs = data.errors
        ? Object.entries(data.errors).map(([k, v]) => `${k}: ${v}`).join("\n")
        : (data.error || "Neznáma chyba");
      alert("Chyba pri ukladaní:\n" + errs);
      if (btn) { btn.disabled = false; btn.textContent = `Uložiť zmeny (${entries.length})`; }
    }
  } catch (err) {
    alert("Sieťová chyba: " + err.message);
    if (btn) { btn.disabled = false; }
  }
};

// ── 7. Crossref – inline plnenie tabuľky ────────────────────────────────────
// Načíta JSON z /api/crossref/<id>?doi=..., potom:
//  - by_field:  vyplní príslušnú .col-crossref bunku v zodpovedajúcom riadku
//  - extra:     pridá nové riadky do #crossref-extra-body so sekčnou hlavičkou

async function loadCrossref() {
  const table  = document.getElementById("detail-table");
  const spinner = document.getElementById("crossref-spinner");
  if (!table) return;

  const doi        = table.dataset.doi        || "";
  const resourceId = table.dataset.resourceId || window.RESOURCE_ID || "";

  if (!doi) {
    if (spinner) spinner.style.display = "none";
    return;
  }

  try {
    const url  = `/api/crossref/${resourceId}?doi=${encodeURIComponent(doi)}`;
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();

    if (!data.ok) {
      console.warn("Crossref:", data.error);
      if (spinner) spinner.style.display = "none";
      return;
    }

    // ── Vyplň inline bunky ─────────────────────────────────────────────────
    if (data.by_field) {
      Object.entries(data.by_field).forEach(([fieldKey, value]) => {
        // Nájdi crossref bunku v riadku s týmto fieldKey
        const cfCell = document.querySelector(
          `td[data-cf-field="${CSS.escape(fieldKey)}"]`
        );
        if (cfCell) {
          cfCell.textContent = value;
          cfCell.classList.remove("text-muted");
        }
      });
    }

    // ── Extra polia (bez mapovania) → nové riadky ──────────────────────────
    const extraBody = document.getElementById("crossref-extra-body");
    if (extraBody && data.extra && data.extra.length) {
      const headerRow = document.createElement("tr");
      headerRow.className = "crossref-section-header";
      headerRow.innerHTML =
        `<td colspan="5" class="text-muted small fw-semibold bg-light py-1 px-2"` +
        ` style="border-top:2px solid #dee2e6;letter-spacing:.04em;">` +
        `CROSSREF – ďalšie polia</td>`;
      extraBody.appendChild(headerRow);

      data.extra.forEach((f) => {
        const tr = document.createElement("tr");
        tr.className = "crossref-row";
        tr.innerHTML =
          `<td class="field-label text-muted small col-label">${escHtml(f.label)}</td>` +
          `<td class="col-repozitar"></td>` +
          `<td class="col-wos"></td>` +
          `<td class="col-scopus"></td>` +
          `<td class="col-crossref small">${escHtml(f.value)}</td>`;
        extraBody.appendChild(tr);
      });
    }

  } catch (err) {
    console.warn("Crossref load error:", err);
  } finally {
    if (spinner) spinner.style.display = "none";
  }
}

// ── Inicializácia po načítaní DOM ────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
  // Render diff v Repozitár bunkách
  document.querySelectorAll("td[data-col-type='repozitar']").forEach(renderRepozitarCell);

  initRepozitarCells();
  initSourceCells();
  updateSaveButton();

  // Načítaj Crossref asynchrónne
  loadCrossref();
});
