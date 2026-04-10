/**
 * diff_highlight.js
 *
 * Pre každú bunku v "Repozitár" stĺpci (data-original + data-proposed)
 * vypočíta character-level diff pomocou diff-match-patch a vykreslí ho
 * ako zelenú/červenú HTML.
 *
 * Spustí sa raz po načítaní stránky a znova po každom HTMX swape.
 */

(function () {
  "use strict";

  const dmp = new diff_match_patch();

  /**
   * Prevedie diff pole na HTML reťazec so zelenými/červenými spanmi.
   */
  function diffToHtml(diffs) {
    return diffs
      .map(([type, text]) => {
        const escaped = text
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;");
        if (type === 1)  return `<span class="diff-insert">${escaped}</span>`;
        if (type === -1) return `<span class="diff-delete">${escaped}</span>`;
        return escaped;
      })
      .join("");
  }

  /**
   * Aplikuje diff na všetky relevantné bunky v dokumente.
   */
  function applyDiffs() {
    document.querySelectorAll("td[data-original][data-proposed]").forEach((td) => {
      const original = td.dataset.original || "";
      const proposed = td.dataset.proposed || "";

      if (!original && !proposed) return;
      if (original === proposed)   return;

      const target = td.querySelector(".diff-target");
      if (!target) return;

      const diffs = dmp.diff_main(original, proposed);
      dmp.diff_cleanupSemantic(diffs);
      target.innerHTML = diffToHtml(diffs);
    });
  }

  // Prvé spustenie po načítaní stránky
  document.addEventListener("DOMContentLoaded", applyDiffs);

  // Opakované spustenie po každom HTMX swape (pre dynamicky načítaný obsah)
  document.addEventListener("htmx:afterSwap", applyDiffs);
})();
