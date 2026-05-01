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

const VALUE_SEPARATOR_RE = /\s*\|\|\s*/;
const DETAIL_READ_ONLY = window.DETAIL_READ_ONLY === true || window.DETAIL_READ_ONLY === "true";

function splitValues(value) {
  return (value || "")
    .split(/\n|\s*\|\|\s*/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function normalizeStoredValue(value) {
  const raw = (value || "").trim();
  if (!raw) return "";
  if (!raw.includes("||") && !raw.includes("\n")) return raw;
  return splitValues(raw).join("||");
}

function valueForDisplay(value) {
  const parts = splitValues(value);
  return parts.length > 1 ? parts.join("\n") : (value || "");
}

function valueForEdit(value) {
  return normalizeStoredValue(value || "");
}

function htmlWithBreaks(value, className) {
  const parts = splitValues(value);
  if (!parts.length) return '<span class="text-muted">—</span>';
  if (parts.length === 1) {
    const cls = className ? ` class="${className}"` : "";
    return `<span${cls}>${escHtml(parts[0])}</span>`;
  }
  const cls = className ? ` ${className}` : "";
  return `<div class="multi-value-list${cls}">` +
    parts.map((part) => `<div>${escHtml(part)}</div>`).join("") +
    "</div>";
}

function diffHtml(original, proposed) {
  const left = valueForDisplay(original);
  const right = valueForDisplay(proposed);
  if (!dmp || left === right) return htmlWithBreaks(proposed || original);
  const diffs = dmp.diff_main(left, right);
  dmp.diff_cleanupSemantic(diffs);
  return diffs.map(([type, text]) => {
    const esc = escHtml(text).replace(/\n/g, "<br>");
    if (type === 1) return `<span class="diff-insert">${esc}</span>`;
    if (type === -1) return `<span class="diff-delete">${esc}</span>`;
    return esc;
  }).join("");
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

function setApproveButtonsBusy(isBusy, label) {
  ["approve-btn-top", "approve-btn-bottom"].forEach((id) => {
    const btn = document.getElementById(id);
    if (!btn) return;
    btn.disabled = isBusy;
    if (isBusy) {
      btn.dataset.originalLabel = btn.dataset.originalLabel || btn.innerHTML;
      btn.innerHTML = label || "Schvaľujem…";
    } else if (btn.dataset.originalLabel) {
      btn.innerHTML = btn.dataset.originalLabel;
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

let authorSourcesModal = null;

function _getAuthorSourcesModal() {
  if (!authorSourcesModal) {
    const el = document.getElementById("author-sources-modal");
    if (!el || !window.bootstrap) return null;
    authorSourcesModal = new bootstrap.Modal(el);
  }
  return authorSourcesModal;
}

function _internalAuthorInfoButtonHtml() {
  return (
    '<button type="button" class="author-modal-trigger ms-2" ' +
    'title="Zobrazi? zdroje autorov" aria-label="Zobrazi? zdroje autorov">?</button>'
  );
}

function _wrapInternalAuthorCell(contentHtml) {
  return (
    '<div class="d-flex align-items-start justify-content-between gap-2">' +
    `<div class="flex-grow-1 min-w-0">${contentHtml}</div>` +
    _internalAuthorInfoButtonHtml() +
    '</div>'
  );
}

function openAuthorSourcesModal() {
  const body = document.getElementById("author-sources-modal-body");
  if (!body) return;
  const rows = Array.isArray(window.authorModalData) ? window.authorModalData : [];
  if (!rows.length) {
    body.innerHTML = '<tr><td colspan="3" class="text-muted">?iadne zdroje autorov.</td></tr>';
  } else {
    body.innerHTML = rows.map((row) => {
      const nameHtml = escHtml(row.name || "?");
      const scopusAff = row.scopus_aff ? escHtml(row.scopus_aff) : '&mdash;';
      const wosAff = row.wos_aff ? escHtml(row.wos_aff) : '&mdash;';
      const rowClass = row.is_internal ? '' : 'author-source-external';
      return (
        '<tr>' +
        `<td>${nameHtml}</td>` +
        `<td class="${rowClass}">${scopusAff}</td>` +
        `<td class="${rowClass}">${wosAff}</td>` +
        '</tr>'
      );
    }).join('');
  }
  _getAuthorSourcesModal()?.show();
}

function renderRepozitarCell(td) {
  if (!td) return;
  const original = td.dataset.original || "";
  const proposed = td.dataset.proposed || "";
  const modified = changes[td.dataset.field];
  const needsAuthorInfo = td.dataset.field === "utb.contributor.internalauthor";

  if (modified !== undefined) {
    let content =
      `<span class="badge bg-warning text-dark me-1" style="font-size:.65rem">upraven?</span>` +
      htmlWithBreaks(modified);
    if (needsAuthorInfo) {
      content = _wrapInternalAuthorCell(content);
    }
    td.innerHTML = content;
    td.classList.add("cell-modified");
  } else {
    td.classList.remove("cell-modified");
    if (!original && !proposed) {
      td.innerHTML = '<span class="text-muted">&mdash;</span>';
    } else if (original && proposed && normalizeStoredValue(original) !== normalizeStoredValue(proposed)) {
      td.innerHTML = diffHtml(original, proposed);
      if (!DETAIL_READ_ONLY) {
        td.innerHTML += ` <button class="fix-badge ms-1" onclick="window.acceptFix(this.closest('td'))" title="Prija&#357; n&#225;vrh">&#10003; opravi&#357;</button>`;
      }
    } else if (proposed && !original) {
      td.innerHTML = htmlWithBreaks(proposed, "proposed-add");
      if (!DETAIL_READ_ONLY) {
        td.innerHTML += ` <button class="fix-badge ms-1" onclick="window.acceptFix(this.closest('td'))" title="Prija&#357; n&#225;vrh">&#10003; pou&#382;i&#357;</button>`;
      }
    } else {
      td.innerHTML = htmlWithBreaks(original);
    }
    if (needsAuthorInfo) {
      td.innerHTML = _wrapInternalAuthorCell(td.innerHTML);
    }
  }

  if (!DETAIL_READ_ONLY) {
    _addFieldPicker(td);
  }
}

function _appendFieldValue(td, value) {
  const fieldKey = td.dataset.field;
  const base = changes[fieldKey] !== undefined ? changes[fieldKey] : (td.dataset.original || "");
  const parts = splitValues(base);
  if (fieldKey === "utb.faculty" || fieldKey === "utb.ou") {
    parts.push(value);
  } else if (!parts.includes(value)) {
    parts.push(value);
  }
  changes[fieldKey] = parts.join("||");
  updateSaveButton();
  renderRepozitarCell(td);
}

function renderSourceCellDisplay(td) {
  if (!td || td.dataset.editing === "true") return;
  const raw = td.dataset.currentRaw || td.dataset.original || "";
  if (!raw) {
    td.innerHTML = '<span class="text-muted">???</span>';
    return;
  }
  const row = td.closest("tr");
  const rowKey = row ? row.dataset.fieldKey : "";
  if (rowKey === "dc.contributor.author") {
    const repoCell = row ? row.querySelector('td[data-col-type="repozitar"]') : null;
    const repoValue = repoCell
      ? (changes[repoCell.dataset.field] !== undefined
          ? changes[repoCell.dataset.field]
          : (repoCell.dataset.proposed || repoCell.dataset.original || ""))
      : "";
    if (repoValue && normalizeStoredValue(repoValue) !== normalizeStoredValue(raw)) {
      td.innerHTML = diffHtml(raw, repoValue);
      return;
    }
  }
  td.innerHTML = htmlWithBreaks(raw);
}

document.addEventListener("click", (e) => {
  const trigger = e.target.closest(".author-modal-trigger");
  if (!trigger) return;
  e.preventDefault();
  e.stopPropagation();
  openAuthorSourcesModal();
});

function renderCrossrefCell(cfCell, value) {
  if (!cfCell) return;
  const row = cfCell.closest("tr");
  const repoCell = row ? row.querySelector('td[data-col-type="repozitar"]') : null;
  const repoValue = repoCell
    ? (changes[repoCell.dataset.field] !== undefined
        ? changes[repoCell.dataset.field]
        : (repoCell.dataset.proposed || repoCell.dataset.original || ""))
    : "";
  cfCell.dataset.crossrefValue = value || "";
  cfCell.classList.remove("text-muted");
  if (row && value) {
    row.classList.remove("null-row");
  }
  if (repoValue && value && normalizeStoredValue(repoValue) !== normalizeStoredValue(value)) {
    cfCell.innerHTML = diffHtml(repoValue, value);
  } else {
    cfCell.innerHTML = htmlWithBreaks(value);
  }
}

let sidebarAuthorInfoModal = null;
let sidebarAuthorMenu = null;

function _getSidebarAuthorInfoModal() {
  if (!sidebarAuthorInfoModal) {
    const el = document.getElementById("author-info-modal");
    if (!el || !window.bootstrap) return null;
    sidebarAuthorInfoModal = new bootstrap.Modal(el);
  }
  return sidebarAuthorInfoModal;
}

function _currentReturnTo() {
  return window.location.pathname + window.location.search;
}

function _authorEditorUrl(rowRef) {
  const params = new URLSearchParams();
  if (rowRef) params.set("row_ref", rowRef);
  params.set("return_to", _currentReturnTo());
  return `/authors/editor?${params.toString()}`;
}

function _openAuthorEditor(rowRef) {
  window.location.href = _authorEditorUrl(rowRef);
}

function _setModalValue(id, value) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = value || "—";
}

function _renderAuthorAffiliations(affiliations) {
  if (!Array.isArray(affiliations) || !affiliations.length) {
    return "—";
  }
  return affiliations
    .map((aff) => `${aff.faculty || "—"} / ${aff.department || "—"}`)
    .join("\n");
}

async function openSidebarAuthorModal(rowRef) {
  const errorEl = document.getElementById("author-info-error");
  if (errorEl) {
    errorEl.classList.add("d-none");
    errorEl.textContent = "";
  }
  try {
    const response = await fetch(`/api/authors/row?row_ref=${encodeURIComponent(rowRef)}`);
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || "Nepodarilo sa načítať autora.");
    }
    const summary = payload.summary || {};
    const author = payload.author || {};
    _setModalValue("author-info-display-name", summary.display_name || author.display_name || "");
    _setModalValue("author-info-surname", summary.surname || author.surname || "");
    _setModalValue("author-info-given-name", summary.given_name || author.given_name || "");
    _setModalValue("author-info-middle-name", summary.middle_name || author.middle_name || "");
    _setModalValue("author-info-orcid", summary.orcid || author.orcid || "");
    _setModalValue("author-info-scopusid", summary.scopusid || author.scopusid || "");
    _setModalValue("author-info-wos-id", summary.wos_id || author.wos_id || author.researcherid || "");
    _setModalValue("author-info-email", summary.preferred_email || "");

    const affiliationsEl = document.getElementById("author-info-affiliations");
    if (affiliationsEl) {
      affiliationsEl.textContent = _renderAuthorAffiliations(summary.affiliations || []);
      affiliationsEl.style.whiteSpace = "pre-line";
    }

    const openEditor = document.getElementById("author-info-open-editor");
    if (openEditor) {
      openEditor.href = _authorEditorUrl(rowRef);
    }
    _getSidebarAuthorInfoModal()?.show();
  } catch (error) {
    if (errorEl) {
      errorEl.textContent = error.message || "Nepodarilo sa načítať autora.";
      errorEl.classList.remove("d-none");
    }
    _getSidebarAuthorInfoModal()?.show();
  }
}

function _internalAuthorTargetCell() {
  return document.querySelector('td[data-col-type="repozitar"][data-field="utb.contributor.internalauthor"]');
}

function addSidebarAuthorToInternalAuthors(authorName) {
  const td = _internalAuthorTargetCell();
  if (!td || !authorName) return;
  _appendFieldValue(td, authorName);
}

function _ensureSidebarAuthorMenu() {
  if (sidebarAuthorMenu) return sidebarAuthorMenu;
  sidebarAuthorMenu = document.createElement("div");
  sidebarAuthorMenu.className = "dropdown-menu shadow";
  sidebarAuthorMenu.style.position = "absolute";
  sidebarAuthorMenu.style.display = "none";
  sidebarAuthorMenu.innerHTML = [
    '<button type="button" class="dropdown-item" data-action="append-internal">Pridať k interným autorom</button>',
    '<button type="button" class="dropdown-item" data-action="open-editor">Otvoriť editor</button>',
  ].join("");
  document.body.appendChild(sidebarAuthorMenu);
  sidebarAuthorMenu.addEventListener("click", (event) => {
    const button = event.target.closest("[data-action]");
    if (!button) return;
    const action = button.dataset.action;
    const rowRef = sidebarAuthorMenu.dataset.rowRef || "";
    const authorName = sidebarAuthorMenu.dataset.authorName || "";
    if (action === "append-internal") {
      addSidebarAuthorToInternalAuthors(authorName);
    } else if (action === "open-editor") {
      _openAuthorEditor(rowRef);
    }
    sidebarAuthorMenu.style.display = "none";
  });
  return sidebarAuthorMenu;
}

function showSidebarAuthorMenu(button) {
  const menu = _ensureSidebarAuthorMenu();
  menu.dataset.rowRef = button.dataset.rowRef || "";
  menu.dataset.authorName = button.dataset.authorName || "";
  const rect = button.getBoundingClientRect();
  menu.style.left = `${window.scrollX + rect.left}px`;
  menu.style.top = `${window.scrollY + rect.bottom + 4}px`;
  menu.style.display = "block";
}

function initAuthorSidebar() {
  const addBtn = document.getElementById("add-author-btn");
  if (addBtn) {
    addBtn.addEventListener("click", () => _openAuthorEditor(""));
  }

  document.addEventListener("click", (event) => {
    const menuButton = event.target.closest(".author-menu-btn");
    if (menuButton) {
      event.preventDefault();
      event.stopPropagation();
      showSidebarAuthorMenu(menuButton);
      return;
    }

    const rowMain = event.target.closest(".author-row-main");
    if (rowMain) {
      const row = rowMain.closest(".author-row");
      const rowRef = row?.dataset.rowRef || "";
      if (rowRef) {
        event.preventDefault();
        openSidebarAuthorModal(rowRef);
      }
      return;
    }

    if (sidebarAuthorMenu && !event.target.closest(".dropdown-menu")) {
      sidebarAuthorMenu.style.display = "none";
    }
  });
}

document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll("td[data-col-type='repozitar']").forEach(renderRepozitarCell);
  document.querySelectorAll("td[data-col-type='wos'], td[data-col-type='scopus']").forEach(renderSourceCellDisplay);
  initRepozitarCells();
  initSourceCells();
  initAuthorSidebar();
  initSaveShortcut();
  updateSaveButton();
  loadCrossref();
});
