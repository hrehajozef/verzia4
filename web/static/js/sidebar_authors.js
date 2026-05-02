"use strict";

(function () {
  const ns = window.UTBSidebarAuthors = window.UTBSidebarAuthors || {};
  let sidebarAuthorInfoModal = null;
  let sidebarAuthorMenu = null;
  let sidebarBound = false;

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
    if (el) el.textContent = value || "—";
  }

  function _renderAuthorAffiliations(affiliations) {
    if (!Array.isArray(affiliations) || !affiliations.length) return "—";
    return affiliations
      .map((aff) => `${aff.faculty || "—"} / ${aff.department || "—"}`)
      .join("\n");
  }

  function _getSidebarAuthorInfoModalBody(summary, author) {
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
    if (openEditor) openEditor.href = _authorEditorUrl(summary.row_ref || author.row_ref || "");
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
        throw new Error(payload.error || "Nepodarilo sa nacitat autora.");
      }
      _getSidebarAuthorInfoModalBody(payload.summary || {}, payload.author || {});
      _getSidebarAuthorInfoModal()?.show();
    } catch (error) {
      if (errorEl) {
        errorEl.textContent = error.message || "Nepodarilo sa nacitat autora.";
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
    const detail = window.UTBDetail || {};
    if (!td || !authorName || typeof detail.appendFieldValue !== "function") return;
    detail.appendFieldValue(td, authorName);
  }

  function _ensureSidebarAuthorMenu() {
    if (sidebarAuthorMenu) return sidebarAuthorMenu;
    sidebarAuthorMenu = document.createElement("div");
    sidebarAuthorMenu.className = "dropdown-menu shadow";
    sidebarAuthorMenu.style.position = "absolute";
    sidebarAuthorMenu.style.display = "none";
    sidebarAuthorMenu.innerHTML = [
      '<button type="button" class="dropdown-item" data-action="append-internal">Pridat k internym autorom</button>',
      '<button type="button" class="dropdown-item" data-action="open-editor">Otvorit editor</button>',
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
    if (sidebarBound) return;
    sidebarBound = true;

    const addBtn = document.getElementById("add-author-btn");
    if (addBtn) addBtn.addEventListener("click", () => _openAuthorEditor(""));

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

  Object.assign(ns, {
    _getSidebarAuthorInfoModal,
    _getSidebarAuthorInfoModalBody,
    openSidebarAuthorModal,
    addSidebarAuthorToInternalAuthors,
    _ensureSidebarAuthorMenu,
    showSidebarAuthorMenu,
    initAuthorSidebar,
  });
})();
