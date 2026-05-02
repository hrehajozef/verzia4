"use strict";
(function () {
  const ns = window.UTBDetail = window.UTBDetail || {}, dmp = typeof diff_match_patch !== "undefined" ? new diff_match_patch() : null, changes = {}, readOnly = window.DETAIL_READ_ONLY === true || window.DETAIL_READ_ONLY === "true", repoSelector = "td[data-col-type='repozitar']", sourceSelector = "td[data-col-type='wos'], td[data-col-type='scopus']";
  const escHtml = (str) => String(str || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  const splitValues = (value) => String(value || "").split(/\n|\s*\|\|\s*/).map((part) => part.trim()).filter(Boolean);
  const normalizeStoredValue = (value) => { const raw = String(value || "").trim(); return !raw ? "" : (!raw.includes("||") && !raw.includes("\n") ? raw : splitValues(raw).join("||")); };
  const valueForEdit = (value) => normalizeStoredValue(value || "");
  function htmlWithBreaks(value, className) {
    const parts = splitValues(value);
    if (!parts.length) return '<span class="text-muted">&mdash;</span>';
    if (parts.length === 1) return `<span${className ? ` class="${className}"` : ""}>${escHtml(parts[0])}</span>`;
    return `<div class="multi-value-list${className ? ` ${className}` : ""}">${parts.map((part) => `<div>${escHtml(part)}</div>`).join("")}</div>`;
  }
  function diffHtml(original, proposed) {
    const left = splitValues(original).join("\n") || String(original || ""), right = splitValues(proposed).join("\n") || String(proposed || "");
    if (!dmp || left === right) return htmlWithBreaks(proposed || original);
    const diffs = dmp.diff_main(left, right); dmp.diff_cleanupSemantic(diffs);
    return diffs.map(([type, text]) => { const escaped = escHtml(text).replace(/\n/g, "<br>"); return type === 1 ? `<span class="diff-insert">${escaped}</span>` : type === -1 ? `<span class="diff-delete">${escaped}</span>` : escaped; }).join("");
  }
  function updateSaveButton() {
    const count = Object.keys(changes).length;
    [["save-all-btn", "changed-count"], ["save-all-btn-bottom", "changed-count-bottom"]].forEach(([btnId, cntId]) => {
      const btn = document.getElementById(btnId);
      if (!btn) return;
      btn.style.display = count > 0 ? "inline-block" : "none";
      if (count > 0 && !btn.disabled) { btn.classList.remove("btn-success"); btn.classList.add("btn-warning"); btn.innerHTML = `Ulozit do zasobnika (<span id="${cntId}">${count}</span>)`; }
    });
  }
  function setApproveButtonsBusy(isBusy, label) {
    ["approve-btn-top", "approve-btn-bottom"].forEach((id) => {
      const btn = document.getElementById(id);
      if (!btn) return;
      btn.disabled = isBusy;
      if (isBusy) { btn.dataset.originalLabel = btn.dataset.originalLabel || btn.innerHTML; btn.innerHTML = label || "Schvalujem..."; }
      else if (btn.dataset.originalLabel) btn.innerHTML = btn.dataset.originalLabel;
    });
  }
  function trackChange(fieldKey, newVal, origVal) {
    if (!fieldKey) return;
    const normalizedNew = normalizeStoredValue(newVal), normalizedOrig = normalizeStoredValue(origVal);
    if ((!normalizedNew && !normalizedOrig) || normalizedNew === normalizedOrig) delete changes[fieldKey];
    else changes[fieldKey] = normalizedNew || "";
    updateSaveButton();
  }
  const repoCellValue = (td) => changes[td.dataset.field] !== undefined ? changes[td.dataset.field] : (td.dataset.proposed || td.dataset.original || "");
  function renderRepozitarCell(td) {
    if (!td) return;
    const field = td.dataset.field || "", original = td.dataset.original || "", proposed = td.dataset.proposed || "", modified = changes[field], currentValue = modified !== undefined ? modified : (proposed || original || ""), authorModal = window.UTBAuthorModal || {};
    if (field === "dc.contributor.author" && modified === undefined) { td.classList.remove("cell-modified"); td.innerHTML = htmlWithBreaks(currentValue); }
    else if (field === "utb.contributor.internalauthor" && modified === undefined) {
      const authorsCell = document.querySelector(`${repoSelector}[data-field="dc.contributor.author"]`), authorsValue = authorsCell ? repoCellValue(authorsCell) : "";
      td.classList.remove("cell-modified");
      td.innerHTML = authorsValue && normalizeStoredValue(authorsValue) !== normalizeStoredValue(currentValue) ? diffHtml(authorsValue, currentValue) : htmlWithBreaks(currentValue);
      if (authorsValue && proposed && normalizeStoredValue(original) !== normalizeStoredValue(proposed) && !readOnly) td.innerHTML += ' <button class="fix-badge ms-1" onclick="window.acceptFix(this.closest(\'td\'))" title="Prijat navrh">&#10003; opravit</button>';
    } else if (modified !== undefined) {
      td.classList.add("cell-modified");
      td.innerHTML = '<span class="badge bg-warning text-dark me-1" style="font-size:.65rem">upravene</span>' + htmlWithBreaks(modified);
    } else if (!original && !proposed) td.innerHTML = '<span class="text-muted">&mdash;</span>';
    else if (original && proposed && normalizeStoredValue(original) !== normalizeStoredValue(proposed)) {
      td.classList.remove("cell-modified"); td.innerHTML = diffHtml(original, proposed);
      if (!readOnly) td.innerHTML += ' <button class="fix-badge ms-1" onclick="window.acceptFix(this.closest(\'td\'))" title="Prijat navrh">&#10003; opravit</button>';
    } else if (proposed && !original) {
      td.classList.remove("cell-modified"); td.innerHTML = htmlWithBreaks(proposed, "proposed-add");
      if (!readOnly) td.innerHTML += ' <button class="fix-badge ms-1" onclick="window.acceptFix(this.closest(\'td\'))" title="Prijat navrh">&#10003; pouzit</button>';
    } else { td.classList.remove("cell-modified"); td.innerHTML = htmlWithBreaks(original); }
    if (field === "utb.contributor.internalauthor" && typeof authorModal.wrapInternalAuthorCell === "function") td.innerHTML = authorModal.wrapInternalAuthorCell(td.innerHTML);
    if (!readOnly) addFieldPicker(td);
  }
  function appendFieldValue(td, value) {
    const field = td.dataset.field, parts = splitValues(changes[field] !== undefined ? changes[field] : (td.dataset.original || ""));
    if (field === "utb.faculty" || field === "utb.ou") parts.push(value); else if (!parts.includes(value)) parts.push(value);
    changes[field] = parts.join("||"); updateSaveButton(); renderRepozitarCell(td);
  }
  function addFieldPicker(td) {
    const field = td.dataset.field;
    if ((field !== "utb.faculty" && field !== "utb.ou") || td.dataset.editing === "true") return;
    const select = document.createElement("select"); select.className = "form-select form-select-sm d-inline-block ms-1"; select.style.cssText = "width:auto;font-size:0.72rem;padding:1px 18px 1px 4px;height:auto;vertical-align:middle;max-width:160px;"; select.innerHTML = '<option value="" selected disabled>+ pridat...</option>';
    if (field === "utb.faculty") Object.entries(window.UTB_FACULTIES || {}).forEach(([abbr, name]) => { const opt = document.createElement("option"); opt.value = name; opt.textContent = `${abbr} â€“ ${name}`; select.appendChild(opt); });
    else {
      const departments = window.UTB_DEPARTMENTS || {}, faculties = window.UTB_FACULTIES || {}, byFaculty = {};
      Object.entries(departments).forEach(([dept, facId]) => { if (!byFaculty[facId]) byFaculty[facId] = []; byFaculty[facId].push(dept); });
      Object.entries(byFaculty).forEach(([facId, depts]) => {
        const grp = document.createElement("optgroup"); grp.label = `${facId} â€“ ${faculties[facId] || ""}`;
        depts.forEach((dept) => { const opt = document.createElement("option"); opt.value = dept; opt.textContent = dept; grp.appendChild(opt); });
        select.appendChild(grp);
      });
    }
    ["mousedown", "click"].forEach((name) => select.addEventListener(name, (event) => event.stopPropagation()));
    select.addEventListener("change", (event) => { event.stopPropagation(); if (select.value) appendFieldValue(td, select.value); });
    td.appendChild(select);
  }
  function renderSourceCellDisplay(td) {
    if (!td || td.dataset.editing === "true") return;
    const raw = td.dataset.currentRaw || td.dataset.original || "";
    if (!raw) return void (td.innerHTML = '<span class="text-muted">&mdash;</span>');
    const row = td.closest("tr");
    if (row?.dataset.fieldKey === "dc.contributor.author") {
      const repoCell = row.querySelector(repoSelector), repoValue = repoCell ? repoCellValue(repoCell) : "";
      if (repoValue && normalizeStoredValue(repoValue) !== normalizeStoredValue(raw)) return void (td.innerHTML = diffHtml(raw, repoValue));
    }
    td.innerHTML = htmlWithBreaks(raw);
  }
  function renderCrossrefCell(td, value) {
    if (!td) return;
    const row = td.closest("tr"), repoCell = row ? row.querySelector(repoSelector) : null, repoValue = repoCell ? repoCellValue(repoCell) : "";
    td.dataset.crossrefValue = value || ""; td.classList.remove("text-muted"); if (row && value) row.classList.remove("null-row");
    td.innerHTML = repoValue && value && normalizeStoredValue(repoValue) !== normalizeStoredValue(value) ? diffHtml(repoValue, value) : htmlWithBreaks(value);
  }
  function initPanel() {
    const panel = document.getElementById("authors-panel"), btn = document.getElementById("panel-toggle-btn");
    if (!panel || !btn) return;
    let open = true;
    btn.addEventListener("click", () => { open = !open; panel.classList.toggle("collapsed", !open); btn.title = open ? "Skryt panel autorov" : "Zobrazit panel autorov"; const arrow = btn.querySelector(".panel-arrow"); if (arrow) arrow.textContent = open ? "â—€" : "â–¶"; });
  }
  function initColumnResize() {
    let state = null;
    document.querySelectorAll("#detail-table thead th").forEach((th) => {
      const handle = th.querySelector(".resize-handle");
      if (handle) handle.addEventListener("mousedown", (event) => { event.preventDefault(); state = { i: th.cellIndex, x: event.clientX, w: th.offsetWidth }; document.body.classList.add("col-resizing"); });
    });
    document.addEventListener("mousemove", (event) => { if (!state) return; const cols = document.querySelectorAll("#detail-table colgroup col"), newW = Math.max(60, state.w + (event.clientX - state.x)); if (cols[state.i]) cols[state.i].style.width = `${newW}px`; });
    document.addEventListener("mouseup", () => { if (!state) return; state = null; document.body.classList.remove("col-resizing"); });
  }
  function initColumnDrag() {
    let resizeActive = false;
    document.querySelectorAll("#detail-table .resize-handle").forEach((handle) => handle.addEventListener("mousedown", () => { resizeActive = true; }));
    document.addEventListener("mouseup", () => { resizeActive = false; });
    const moveColumn = (fromIdx, toIdx) => {
      const table = document.getElementById("detail-table");
      if (!table || fromIdx === toIdx) return;
      const cols = Array.from(table.querySelectorAll("colgroup col"));
      cols[toIdx].parentNode.insertBefore(cols[fromIdx], fromIdx < toIdx ? cols[toIdx].nextSibling : cols[toIdx]);
      table.querySelectorAll("tr").forEach((tr) => { const cells = Array.from(tr.children); if (cells.length > Math.max(fromIdx, toIdx)) tr.insertBefore(cells[fromIdx], fromIdx < toIdx ? cells[toIdx].nextSibling : cells[toIdx]); });
    };
    document.querySelectorAll("#detail-table thead th").forEach((th) => {
      th.draggable = true;
      th.addEventListener("dragstart", (event) => { if (resizeActive) return event.preventDefault(); event.dataTransfer.effectAllowed = "move"; event.dataTransfer.setData("text/plain", th.cellIndex); th.style.opacity = "0.5"; document.body.classList.add("col-dragging"); });
      th.addEventListener("dragend", () => { th.style.opacity = ""; document.body.classList.remove("col-dragging"); document.querySelectorAll("#detail-table thead th").forEach((cell) => cell.classList.remove("col-drag-over")); });
      th.addEventListener("dragover", (event) => { event.preventDefault(); th.classList.add("col-drag-over"); });
      th.addEventListener("dragleave", () => th.classList.remove("col-drag-over"));
      th.addEventListener("drop", (event) => { event.preventDefault(); th.classList.remove("col-drag-over"); moveColumn(parseInt(event.dataTransfer.getData("text/plain"), 10), th.cellIndex); });
    });
  }
  function initRepozitarCells() {
    document.querySelectorAll(repoSelector).forEach((td) => {
      td.style.cursor = "text";
      td.addEventListener("click", (event) => {
        if (readOnly || td.dataset.editing === "true" || event.target.closest(".fix-badge, .author-modal-trigger") || event.target.tagName === "SELECT") return;
        td.dataset.editing = "true"; td.contentEditable = "true"; td.textContent = changes[td.dataset.field] !== undefined ? changes[td.dataset.field] : valueForEdit(td.dataset.proposed || td.dataset.original || "");
        td.style.outline = "2px solid #0d6efd"; td.classList.add("editing"); td.focus();
      });
      td.addEventListener("blur", () => { td.dataset.editing = "false"; td.contentEditable = "false"; td.style.outline = ""; td.classList.remove("editing"); trackChange(td.dataset.field, td.textContent.trim(), td.dataset.original || ""); renderRepozitarCell(td); });
      td.addEventListener("keydown", (event) => {
        if (event.key === "Enter" && !event.shiftKey) { event.preventDefault(); td.blur(); }
        if (event.key === "Escape") { delete changes[td.dataset.field]; updateSaveButton(); td.dataset.editing = "false"; td.contentEditable = "false"; td.style.outline = ""; td.classList.remove("editing"); renderRepozitarCell(td); }
      });
    });
  }
  function initSourceCells() {
    document.querySelectorAll(sourceSelector).forEach((td) => {
      const field = td.dataset.field || "";
      td.addEventListener("focus", () => { td.dataset.editing = "true"; td.textContent = changes[field] !== undefined ? changes[field] : valueForEdit(td.dataset.original || ""); });
      td.addEventListener("blur", () => { td.dataset.editing = "false"; const newVal = td.textContent.trim(); if (field) { trackChange(field, newVal, td.dataset.original || ""); td.dataset.currentRaw = changes[field] !== undefined ? changes[field] : normalizeStoredValue(newVal); td.classList.toggle("cell-modified", changes[field] !== undefined); } renderSourceCellDisplay(td); });
      td.addEventListener("keydown", (event) => {
        if (event.key === "Enter" && !event.shiftKey) { event.preventDefault(); td.blur(); }
        if (event.key === "Escape") { delete changes[field]; updateSaveButton(); td.classList.remove("cell-modified"); td.dataset.editing = "false"; renderSourceCellDisplay(td); td.blur(); }
      });
    });
  }
  async function saveAllChanges() {
    const entries = Object.entries(changes), topBtn = document.getElementById("save-all-btn"), bottomBtn = document.getElementById("save-all-btn-bottom");
    if (!entries.length) return;
    if (topBtn) { topBtn.disabled = true; topBtn.innerHTML = "Ukladam..."; }
    if (bottomBtn) { bottomBtn.disabled = true; bottomBtn.innerHTML = "Ukladam..."; }
    try {
      const response = await fetch(`/record/${window.RESOURCE_ID}/save-fields`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ fields: Object.fromEntries(entries) }) });
      const data = await response.json();
      if (!data.ok) throw new Error(data.errors ? Object.entries(data.errors).map(([key, value]) => `${key}: ${value}`).join("\n") : (data.error || "Neznama chyba"));
      Object.keys(changes).forEach((key) => delete changes[key]); document.querySelectorAll(".cell-modified").forEach((el) => el.classList.remove("cell-modified")); document.querySelectorAll(repoSelector).forEach(renderRepozitarCell); updateSaveButton();
    } catch (error) { alert(`Chyba pri ukladani:\n${error.message || error}`); }
    finally { if (topBtn) topBtn.disabled = false; if (bottomBtn) bottomBtn.disabled = false; updateSaveButton(); }
  }
  async function approveRecord() {
    const active = document.activeElement, approveMsg = document.getElementById("approve-msg"), topBtn = document.getElementById("save-all-btn"), bottomBtn = document.getElementById("save-all-btn-bottom");
    if (active && active.isContentEditable) active.blur();
    await new Promise((resolve) => window.setTimeout(resolve, 0));
    if (topBtn) topBtn.disabled = true; if (bottomBtn) bottomBtn.disabled = true; setApproveButtonsBusy(true, "Schvalujem...");
    try {
      const response = await fetch(`/record/${window.RESOURCE_ID}/approve`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ fields: Object.fromEntries(Object.entries(changes)) }) });
      const data = await response.json();
      if (!response.ok || !data.ok) throw new Error(data.errors ? Object.entries(data.errors).map(([key, value]) => `${key}: ${value}`).join("\n") : (data.error || `HTTP ${response.status}`));
      Object.keys(changes).forEach((key) => delete changes[key]); document.querySelectorAll(".cell-modified").forEach((el) => el.classList.remove("cell-modified")); updateSaveButton();
      if (approveMsg) approveMsg.innerHTML = '<div class="alert alert-success">Zaznam schvaleny.</div>';
      window.setTimeout(() => { window.location = data.redirect || "/"; }, 400);
    } catch (error) {
      const message = escHtml(error.message || String(error));
      if (approveMsg) approveMsg.innerHTML = `<div class="alert alert-danger">Chyba pri schvaleni: ${message}</div>`;
      else alert(`Chyba pri schvaleni:\n${message}`);
    } finally {
      if (topBtn) topBtn.disabled = false; if (bottomBtn) bottomBtn.disabled = false; setApproveButtonsBusy(false); updateSaveButton();
    }
  }
  function initSaveShortcut() {
    document.addEventListener("keydown", (event) => {
      const key = String(event.key || "").toLowerCase();
      if (key !== "s" || (!event.ctrlKey && !event.metaKey)) return;
      event.preventDefault();
      const active = document.activeElement;
      if (active && active.isContentEditable) active.blur();
      window.setTimeout(() => { if (Object.keys(changes).length) saveAllChanges(); }, 0);
    });
  }
  async function loadCrossref() {
    const table = document.getElementById("detail-table"), spinner = document.getElementById("crossref-spinner");
    if (!table) return;
    const doi = table.dataset.doi || "", resourceId = table.dataset.resourceId || window.RESOURCE_ID || "";
    if (!doi) { if (spinner) spinner.style.display = "none"; return; }
    try {
      const response = await fetch(`/api/crossref/${resourceId}?doi=${encodeURIComponent(doi)}`);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      if (!data.ok) return;
      Object.entries(data.by_field || {}).forEach(([fieldKey, value]) => { const td = document.querySelector(`td[data-cf-field="${CSS.escape(fieldKey)}"]`); if (td) renderCrossrefCell(td, value); });
      const extraBody = document.getElementById("crossref-extra-body");
      if (extraBody && Array.isArray(data.extra) && data.extra.length) {
        extraBody.innerHTML = "";
        extraBody.insertAdjacentHTML("beforeend", '<tr class="crossref-section-header"><td colspan="5" class="text-muted small fw-semibold bg-light py-1 px-2" style="border-top:2px solid #dee2e6;letter-spacing:.04em;">CROSSREF - dalsie polia</td></tr>');
        data.extra.forEach((field) => extraBody.insertAdjacentHTML("beforeend", `<tr class="crossref-row"><td class="field-label text-muted small col-label">${escHtml(field.label)}</td><td class="col-repozitar"></td><td class="col-wos"></td><td class="col-scopus"></td><td class="col-crossref small">${escHtml(field.value)}</td></tr>`));
      }
    } catch (error) { console.warn("Crossref load error:", error); }
    finally { if (spinner) spinner.style.display = "none"; }
  }
  window.acceptFix = (td) => { if (!td || !td.dataset.field) return; changes[td.dataset.field] = normalizeStoredValue(td.dataset.proposed || ""); updateSaveButton(); renderRepozitarCell(td); };
  window.saveAllChanges = saveAllChanges; window.approveRecord = approveRecord;
  Object.assign(ns, { escHtml, splitValues, normalizeStoredValue, valueForEdit, htmlWithBreaks, diffHtml, changes, updateSaveButton, appendFieldValue, renderRepozitarCell, renderSourceCellDisplay, renderCrossrefCell, initSaveShortcut, loadCrossref });
  document.addEventListener("DOMContentLoaded", () => {
    initPanel(); initColumnResize(); initColumnDrag(); document.querySelectorAll(repoSelector).forEach(renderRepozitarCell); document.querySelectorAll(sourceSelector).forEach(renderSourceCellDisplay);
    initRepozitarCells(); initSourceCells(); if (window.UTBAuthorModal?.init) window.UTBAuthorModal.init(); if (window.UTBSidebarAuthors?.initAuthorSidebar) window.UTBSidebarAuthors.initAuthorSidebar(); initSaveShortcut(); updateSaveButton(); loadCrossref();
  });
})();
