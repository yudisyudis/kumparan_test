-- simulasi delete data

DELETE FROM public.articles
WHERE id IN (
    SELECT id FROM public.articles ORDER BY random() LIMIT 1
)
RETURNING *;
