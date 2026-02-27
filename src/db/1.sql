SELECT
    r.resource_id,
    r.handle,
    -- Výsledok heuristík
    r.utb_contributor_internalauthor  AS heuristic_authors,
    -- Výsledok LLM
	r."utb.wos.affiliation",
    r.llm_status,
    r.llm_result->>'faculty_guess'    AS llm_faculty,
    r.llm_result->>'confidence'       AS llm_confidence,
    r.llm_result->>'notes'            AS llm_notes,
    r.llm_result->'internal_authors'  AS llm_authors,
    r.llm_processed_at
FROM public.utb_metadata_arr r
WHERE r.needs_llm = TRUE
  AND r.llm_status = 'processed'
ORDER BY r.resource_id;