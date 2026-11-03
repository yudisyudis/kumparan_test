# menghapus data seluruh table di postgres

import os

import psycopg2

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def reset_postgres(*args, **kwargs):
    conn = psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        port=5432,   # port internal Docker, bukan port yang dipetakan ke komputer
        dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
    )
    try:
        with conn:   # commit otomatis jika sukses, rollback jika error
            with conn.cursor() as cur:
                cur.execute(
                    'TRUNCATE TABLE public.articles, public.articles_hard_deleted RESTART IDENTITY'
                )
                cur.execute(
                    'SELECT (SELECT count(*) FROM public.articles), '
                    '(SELECT count(*) FROM public.articles_hard_deleted)'
                )
                articles, tombstones = cur.fetchone()
    finally:
        conn.close()

    print(f'Postgres dikosongkan: articles={articles}, articles_hard_deleted={tombstones}')