/**
 * detail.js – interaktivita pre detail stránku záznamu
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

// ── 2. Nastaviteľná šírka stĺpcov ────────────────────────────────────────────

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

// ── 2b. Presúvanie stĺpcov ───────────────────────────────────────────────────

(function initColumnDrag() {
  let resizeActive = false;

  document.querySelectorAll("#detail-table .resize-handle").forEach((h) => {
    h.addEventListener("mousedown", () => { resizeActive = true; });
  });
  document.addEventListener("mouseup", () => { resizeActive = false; });

  function moveColumn(fromIdx, toIdx) {
    const table = document.getElementById("detail-table");
    if (!table || fromIdx === toIdx) return;
    const cols = Array.from(table.querySelectorAll("colgroup col"));
    const ref  = fromIdx < toIdx ? cols[toIdx].nextSibling : cols[toIdx];
    cols[toIdx].parentNode.insertBefore(cols[fromIdx], ref);
    table.querySelectorAll("tr").forEach((tr) => {
      const cells = Array.from(tr.children);
      if (cells.length <= Math.max(fromIdx, toIdx)) return;
      const refCell = fromIdx < toIdx ? cells[toIdx].nextSibling : cells[toIdx];
      tr.insertBefore(cells[fromIdx], refCell);
    });
  }

  document.querySelectorAll("#detail-table thead th").forEach((th) => {
    th.draggable = true;
    th.addEventListener("dragstart", (e) => {
      if (resizeActive) { e.preventDefault(); return; }
      e.dataTransfer.effectAllowed = "move";
      e.dataTransfer.setData("text/plain", th.cellIndex);
      th.style.opacity = "0.5";
      document.body.classList.add("col-dragging");
    });
    th.addEventListener("dragend", () => {
      th.style.opacity = "";
      document.body.classList.remove("col-dragging");
      document.querySelectorAll("#detail-table thead th").forEach((t) => t.classList.remove("col-drag-over"));
    });
    th.addEventListener("dragover", (e) => {
      e.preventDefault();
      e.dataTransfer.dropEffect = "move";
      document.querySelectorAll("#detail-table thead th").forEach((t) => t.classList.remove("col-drag-over"));
      th.classList.add("col-drag-over");
    });
    th.addEventListener("dragleave", () => th.classList.remove("col-drag-over"));
    th.addEventListener("drop", (e) => {
      e.preventDefault();
      th.classList.remove("col-drag-over");
      moveColumn(parseInt(e.dataTransfer.getData("text/plain"), 10), th.cellIndex);
    });
  });
})();

// ── 3. Sledovanie zmien a ukladanie ──────────────────────────────────────────

const changes = {};
const dmp = typeof diff_match_patch !== "undefined" ? new diff_match_patch() : null;

function updateSaveButton() {
  const count = Object.keys(changes).length;
  [
    { btnId: "save-all-btn",        cntId: "changed-count" },
    { btnId: "save-all-btn-bottom", cntId: "changed-count-bottom" },
  ].forEach(({ btnId, cntId }) => {
    const btn = document.getElementById(btnId);
    if (!btn) return;
    btn.style.display = count > 0 ? "inline-block" : "none";
    if (count > 0 && !btn.disabled) {
      btn.classList.remove("btn-success");
      btn.classList.add("btn-warning");
      btn.innerHTML = `Uložiť do zásobníka (<span id="${cntId}">${count}</span>)`;
    }
  });
}

function trackChange(fieldKey, newVal, origVal) {
  if (!fieldKey) return;
  const empty     = !newVal || newVal === "—";
  const origEmpty = !origVal;
  if ((empty && origEmpty) || newVal === origVal) {
    delete changes[fieldKey];
  } else {
    changes[fieldKey] = empty ? "" : newVal;
  }
  updateSaveButton();
}

// ── Diff rendering pre Repozitár bunky ───────────────────────────────────────

function renderRepozitarCell(td) {
  if (!td) return;
  const original = td.dataset.original || "";
  const proposed = td.dataset.proposed || "";
  const modified = changes[td.dataset.field];

  if (modified !== undefined) {
    td.innerHTML =
      `<span class="badge bg-warning text-dark me-1" style="font-size:.65rem">upravené</span>` +
      escHtml(modified || "—");
    td.classList.add("cell-modified");
  } else {
    td.classList.remove("cell-modified");
    if (!original && !proposed) {
      td.innerHTML = '<span class="text-muted">—</span>';
    } else if (original && proposed && original !== proposed && dmp) {
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
      td.innerHTML = diffHtml +
        ` <button class="fix-badge ms-1" onclick="window.acceptFix(this.closest('td'))" title="Prijať návrh">✓ opraviť</button>`;
    } else if (proposed && !original) {
      td.innerHTML =
        `<span class="proposed-add">${escHtml(proposed)}</span>` +
        ` <button class="fix-badge ms-1" onclick="window.acceptFix(this.closest('td'))" title="Prijať návrh">✓ použiť</button>`;
    } else {
      td.textContent = original;
    }
  }

  _addFieldPicker(td);
}

// ── Faculty / OU picker – natívny <select> (renderuje nad overflow) ───────────
// Natívny <select> vždy vykreslí OS-level popup, ktorý nie je ovplyvnený
// CSS overflow: hidden ani overflow-x: auto na rodičovských elementoch.

function _addFieldPicker(td) {
  const field = td.dataset.field;
  if (field !== "utb.faculty" && field !== "utb.ou") return;
  if (td.dataset.editing === "true") return;

  const isFaculty = field === "utb.faculty";

  const select = document.createElement("select");
  select.className = "form-select form-select-sm d-inline-block ms-1";
  select.style.cssText = "width:auto; font-size:0.72rem; padding:1px 18px 1px 4px; height:auto; vertical-align:middle; max-width:160px;";

  // Placeholder
  const ph = document.createElement("option");
  ph.value = "";
  ph.textContent = "+ pridať…";
  ph.selected = true;
  ph.disabled = true;
  select.appendChild(ph);

  if (isFaculty) {
    Object.entries(window.UTB_FACULTIES || {}).forEach(([abbr, name]) => {
      const opt = document.createElement("option");
      opt.value = name;
      opt.textContent = `${abbr} – ${name}`;
      select.appendChild(opt);
    });
  } else {
    const departments = window.UTB_DEPARTMENTS || {};
    const faculties   = window.UTB_FACULTIES   || {};
    const byFaculty   = {};
    Object.entries(departments).forEach(([dept, facId]) => {
      if (!byFaculty[facId]) byFaculty[facId] = [];
      byFaculty[facId].push(dept);
    });
    Object.entries(byFaculty).forEach(([facId, depts]) => {
      const grp = document.createElement("optgroup");
      grp.label = `${facId} – ${faculties[facId] || ""}`;
      depts.forEach((dept) => {
        const opt = document.createElement("option");
        opt.value = dept;
        opt.textContent = dept;
        grp.appendChild(opt);
      });
      select.appendChild(grp);
    });
  }

  // Prevent propagation so td click handler doesn't trigger edit mode
  select.addEventListener("mousedown", (e) => e.stopPropagation());
  select.addEventListener("click",     (e) => e.stopPropagation());
  select.addEventListener("change", (e) => {
    e.stopPropagation();
    if (select.value) {
      _appendFieldValue(td, select.value);
      // reset done by renderRepozitarCell which re-creates the select
    }
  });

  td.appendChild(select);
}

function _appendFieldValue(td, value) {
  const fieldKey = td.dataset.field;
  const base = changes[fieldKey] !== undefined ? changes[fieldKey] : (td.dataset.original || "");
  const parts = base ? base.split(" || ").map((s) => s.trim()).filter(Boolean) : [];
  if (!parts.includes(value)) parts.push(value);
  changes[fieldKey] = parts.join(" || ");
  updateSaveButton();
  renderRepozitarCell(td);
}

// ── Vlastné kontextové menu pre autorov ──────────────────────────────────────
// Namiesto Bootstrap dropdown – renderuje sa cez position:fixed na body,
// takže nie je ovplyvnené overflow:hidden na .authors-panel.

function _closeAuthorMenu() {
  const m = document.getElementById("_author-ctx-menu");
  if (m) m.remove();
}

function _showAuthorMenu(anchorEl, displayName) {
  _closeAuthorMenu();

  const rect = anchorEl.getBoundingClientRect();
  const menu = document.createElement("div");
  menu.id = "_author-ctx-menu";
  menu.style.cssText = [
    "position:fixed",
    `top:${rect.bottom + 3}px`,
    `right:${window.innerWidth - rect.right}px`,
    "z-index:9999",
    "background:#fff",
    "border:1px solid #dee2e6",
    "border-radius:5px",
    "box-shadow:0 4px 12px rgba(0,0,0,.15)",
    "min-width:200px",
    "padding: 10px 10px",
    "font-size:0.82rem",
  ].join(";");

  function item(label, onClick, danger) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "dropdown-item" + (danger ? " text-danger" : "");
    btn.textContent = label;
    btn.addEventListener("click", (e) => { e.stopPropagation(); _closeAuthorMenu(); onClick(); });
    return btn;
  }

  menu.appendChild(item("Pridať k interným autorom", () => {
    window.addAuthorToInternalList(displayName);
  }));

  const hr = document.createElement("hr");
  hr.className = "dropdown-divider my-3";
  menu.appendChild(hr);

  menu.appendChild(item("Vymazať z databázy", () => {
    if (!confirm(`Odstrániť "${displayName}" z databázy?`)) return;
    fetch("/api/authors", {
      method: "DELETE",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: "display_name=" + encodeURIComponent(displayName),
    })
      .then((r) => r.text())
      .then((html) => {
        const list = document.getElementById("authors-list");
        if (list) list.innerHTML = html;
      })
      .catch((err) => console.error("Delete author error:", err));
  }, true));

  document.body.appendChild(menu);

  // Adjust if menu goes below viewport
  const menuRect = menu.getBoundingClientRect();
  if (menuRect.bottom > window.innerHeight - 8) {
    menu.style.top = `${rect.top - menuRect.height - 3}px`;
  }
}

// Close menu on outside click
document.addEventListener("click", (e) => {
  if (!e.target.closest("#_author-ctx-menu") && !e.target.closest(".author-menu-btn")) {
    _closeAuthorMenu();
  }
});

// Open menu on ⋮ button click (event delegation – works for HTMX-refreshed lists too)
document.addEventListener("click", (e) => {
  const btn = e.target.closest(".author-menu-btn");
  if (!btn) return;
  e.stopPropagation();
  const displayName = btn.dataset.displayName;
  if (!displayName) return;
  // Toggle
  const existing = document.getElementById("_author-ctx-menu");
  if (existing) { _closeAuthorMenu(); return; }
  _showAuthorMenu(btn, displayName);
});

// ── Pridanie autora do bunky interných autorov ───────────────────────────────

window.addAuthorToInternalList = function (displayName) {
  const td = document.querySelector('td[data-field="utb.contributor.internalauthor"]');
  if (!td) { alert("Bunka 'Interní autori' nebola nájdená v tabuľke."); return; }
  _appendFieldValue(td, displayName);
};

// ── Inicializácia Repozitár buniek (klik → edit) ─────────────────────────────

function initRepozitarCells() {
  document.querySelectorAll("td[data-col-type='repozitar']").forEach((td) => {
    td.style.cursor = "text";

    td.addEventListener("click", (e) => {
      // Native <select> already calls stopPropagation; this is a belt-and-suspenders check
      if (e.target.tagName === "SELECT" || e.target.tagName === "OPTION") return;
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
      if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); td.blur(); }
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
        td.innerHTML = orig ? escHtml(orig) : '<span class="text-muted">—</span>';
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

// ── Uloženie zmien ────────────────────────────────────────────────────────────

window.saveAllChanges = async function () {
  const entries = Object.entries(changes);
  if (!entries.length) return;

  const btn    = document.getElementById("save-all-btn");
  const btnBot = document.getElementById("save-all-btn-bottom");
  if (btn)    { btn.disabled    = true; btn.innerHTML    = "Ukladám…"; }
  if (btnBot) { btnBot.disabled = true; btnBot.innerHTML = "Ukladám…"; }

  try {
    const resp = await fetch(`/record/${window.RESOURCE_ID}/save-fields`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ fields: Object.fromEntries(entries) }),
    });
    const data = await resp.json();

    if (data.ok) {
      Object.keys(changes).forEach((k) => delete changes[k]);
      document.querySelectorAll(".cell-modified").forEach((el) => el.classList.remove("cell-modified"));
      if (btn)    { btn.disabled    = false; }
      if (btnBot) { btnBot.disabled = false; }
      updateSaveButton();
    } else {
      const errs = data.errors
        ? Object.entries(data.errors).map(([k, v]) => `${k}: ${v}`).join("\n")
        : (data.error || "Neznáma chyba");
      alert("Chyba pri ukladaní:\n" + errs);
      if (btn)    { btn.disabled = false; }
      if (btnBot) { btnBot.disabled = false; }
      updateSaveButton();
    }
  } catch (err) {
    alert("Sieťová chyba: " + err.message);
    if (btn)    { btn.disabled = false; }
    if (btnBot) { btnBot.disabled = false; }
    updateSaveButton();
  }
};

// ── Crossref – inline plnenie tabuľky ────────────────────────────────────────

function initSaveShortcut() {
  document.addEventListener("keydown", (e) => {
    const key = (e.key || "").toLowerCase();
    if (key !== "s" || (!e.ctrlKey && !e.metaKey)) return;

    e.preventDefault();
    const active = document.activeElement;
    if (active && active.isContentEditable) {
      active.blur();
    }
    window.setTimeout(() => {
      if (Object.keys(changes).length) {
        window.saveAllChanges();
      }
    }, 0);
  });
}

async function loadCrossref() {
  const table   = document.getElementById("detail-table");
  const spinner = document.getElementById("crossref-spinner");
  if (!table) return;

  const doi        = table.dataset.doi        || "";
  const resourceId = table.dataset.resourceId || window.RESOURCE_ID || "";

  if (!doi) { if (spinner) spinner.style.display = "none"; return; }

  try {
    const resp = await fetch(`/api/crossref/${resourceId}?doi=${encodeURIComponent(doi)}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();

    if (!data.ok) { console.warn("Crossref:", data.error); return; }

    if (data.by_field) {
      Object.entries(data.by_field).forEach(([fieldKey, value]) => {
        const cfCell = document.querySelector(`td[data-cf-field="${CSS.escape(fieldKey)}"]`);
        if (cfCell) { cfCell.textContent = value; cfCell.classList.remove("text-muted"); }
      });
    }

    const extraBody = document.getElementById("crossref-extra-body");
    if (extraBody && data.extra && data.extra.length) {
      const headerRow = document.createElement("tr");
      headerRow.className = "crossref-section-header";
      headerRow.innerHTML =
        `<td colspan="5" class="text-muted small fw-semibold bg-light py-1 px-2"` +
        ` style="border-top:2px solid #dee2e6;letter-spacing:.04em;">CROSSREF – ďalšie polia</td>`;
      extraBody.appendChild(headerRow);
      data.extra.forEach((f) => {
        const tr = document.createElement("tr");
        tr.className = "crossref-row";
        tr.innerHTML =
          `<td class="field-label text-muted small col-label">${escHtml(f.label)}</td>` +
          `<td class="col-repozitar"></td><td class="col-wos"></td><td class="col-scopus"></td>` +
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

// ── Inicializácia po načítaní DOM ─────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll("td[data-col-type='repozitar']").forEach(renderRepozitarCell);
  initRepozitarCells();
  initSourceCells();
  initSaveShortcut();
  updateSaveButton();
  loadCrossref();
});
