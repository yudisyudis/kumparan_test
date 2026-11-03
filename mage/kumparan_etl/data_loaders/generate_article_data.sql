-- ================================================================================================
-- Script SQL yang berfungsi mengenerate data dummy dengan jumlah tertentu untuk keperluan project
-- ================================================================================================

WITH base AS MATERIALIZED (
    SELECT
        n,
        timestamptz '2016-01-01 00:00:00+00'
          + random() * (now() - timestamptz '2016-01-01 00:00:00+00') AS created_at
        --   tentukan jumlah data dummy
    FROM generate_series(1, 1000) AS n
),
dummy AS (
    SELECT
        n,
        created_at,
        CASE WHEN n % 5 = 0
             THEN LEAST(created_at + (n % 30 + 1) * interval '1 day', now())
             ELSE created_at
        END AS updated_at
    FROM base
)
INSERT INTO public.articles (title, content, published_at, author_id, created_at, updated_at, deleted_at)
SELECT
    'Judul artikel ' || n,
    'Isi artikel dummy nomor ' || n || '. ' || repeat('Lorem ipsum dolor sit amet. ', 10),
    created_at,
    1 + floor(random() * 100)::bigint,
    created_at,
    updated_at,
    CASE WHEN n % 20 = 0 THEN updated_at END
FROM dummy
WHERE NOT EXISTS (SELECT 1 FROM public.articles)
ORDER BY created_at;

