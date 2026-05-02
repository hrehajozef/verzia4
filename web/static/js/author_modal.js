"use strict";

(function () {
  const ns = window.UTBAuthorModal = window.UTBAuthorModal || {};
  let authorSourcesModal = null;
  let authorSourcesBound = false;

  function _detail() {
    return window.UTBDetail || {};
  }

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
      'title="Zobrazit zdroje autorov" aria-label="Zobrazit zdroje autorov">&#9432;</button>'
    );
  }

  function _wrapInternalAuthorCell(contentHtml) {
    return (
      '<div class="d-flex align-items-start justify-content-between gap-2">' +
      `<div class="flex-grow-1 min-w-0">${contentHtml}</div>` +
      _internalAuthorInfoButtonHtml() +
      "</div>"
    );
  }

  function openAuthorSourcesModal() {
    const body = document.getElementById("author-sources-modal-body");
    if (!body) return;
    const escHtml = _detail().escHtml || ((v) => v || "");
    const rows = Array.isArray(window.authorModalData) ? window.authorModalData : [];
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="3" class="text-muted">Ziadne zdroje autorov.</td></tr>';
    } else {
      body.innerHTML = rows.map((row) => {
        const rowClass = row.is_internal ? "" : "author-source-external";
        const scopusAff = row.scopus_aff ? escHtml(row.scopus_aff) : "&mdash;";
        const wosAff = row.wos_aff ? escHtml(row.wos_aff) : "&mdash;";
        return (
          "<tr>" +
          `<td>${escHtml(row.name || "?")}</td>` +
          `<td class="${rowClass}">${scopusAff}</td>` +
          `<td class="${rowClass}">${wosAff}</td>` +
          "</tr>"
        );
      }).join("");
    }
    _getAuthorSourcesModal()?.show();
  }

  function init() {
    if (authorSourcesBound) return;
    authorSourcesBound = true;
    document.addEventListener("click", (event) => {
      const trigger = event.target.closest(".author-modal-trigger");
      if (!trigger) return;
      event.preventDefault();
      event.stopPropagation();
      openAuthorSourcesModal();
    });
  }

  Object.assign(ns, {
    init,
    _getAuthorSourcesModal,
    _internalAuthorInfoButtonHtml,
    _wrapInternalAuthorCell,
    openAuthorSourcesModal,
    wrapInternalAuthorCell: _wrapInternalAuthorCell,
  });
})();
