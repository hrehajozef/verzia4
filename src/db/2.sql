UPDATE public.utb_metadata_arr
SET llm_status = NULL
WHERE llm_status = 'processed';
