# mengisi chunk dengan data historis per bulan lalu diinsert secara partisi ke clickhouse

import time
from os import path

import pandas as pd

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path
from kumparan_etl.utils.articles import insert_articles, prepare_articles_df
from kumparan_etl.utils.warehouse import get_clickhouse_client

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

PIPELINE = 'data_backfill'


@data_exporter
def insert_backfill(plan, *args, **kwargs):
    if isinstance(plan, list) and plan:
        plan = plan[0]
    chunks = plan['chunks']
    if not chunks:
        print('Tidak ada chunk yang perlu diproses')
        return

    client = get_clickhouse_client()
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    total_rows = 0

    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        for chunk in chunks:
            period = pd.Period(chunk, freq='M')
            start = period.start_time.strftime('%Y-%m-%d %H:%M:%S')
            end = (period + 1).start_time.strftime('%Y-%m-%d %H:%M:%S')
            df = loader.load(f"""
                SELECT id, title, content, published_at, author_id,
                       created_at, updated_at, deleted_at
                FROM public.articles
                WHERE created_at >= '{start}+00'::timestamptz
                  AND created_at <  '{end}+00'::timestamptz
                ORDER BY id
            """)

            rows = 0
            if not df.empty:
                insert_articles(client, prepare_articles_df(df), int(time.time() * 1000))
                rows = len(df)

            # Checkpoint ditulis SETELAH data chunk berhasil masuk
            client.insert('etl_backfill_checkpoint', [[PIPELINE, chunk, rows]],
                          column_names=['pipeline', 'chunk', 'rows_written'])
            total_rows += rows
            print(f'Chunk {chunk}: {rows} baris')

    print(f'Selesai: {len(chunks)} chunk, {total_rows} baris')
