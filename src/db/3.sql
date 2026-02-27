SELECT *
FROM public.utb_metadata_arr r
WHERE r.needs_llm = TRUE
  AND r.llm_status = 'processed'
ORDER BY r.resource_id;