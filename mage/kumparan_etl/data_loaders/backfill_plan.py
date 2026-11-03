# inisiasi bulan sebagai chunk data

from os import path

import pandas as pd

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path
from kumparan_etl.utils.warehouse import get_clickhouse_client

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

PIPELINE = 'data_backfill'


def _utc_naive(value):
    ts = pd.Timestamp(value)
    return ts.tz_convert('UTC').tz_localize(None) if ts.tzinfo else ts


@data_loader
def backfill_plan(*args, **kwargs):
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        bounds = loader.load('SELECT min(created_at) AS first_created, now() AS now_pg FROM public.articles')

    first_created = bounds['first_created'].iloc[0]
    if pd.isna(first_created):
        print('Tabel articles kosong, tidak ada yang di-backfill')
        return {'t0': None, 'chunks': [], 'total_chunks': 0}

    client = get_clickhouse_client()
    state = client.query(
        'SELECT last_updated_at FROM etl_state FINAL WHERE pipeline = {p:String}',
        parameters={'p': PIPELINE},
    )
    if state.row_count == 0:
        t0 = pd.Timestamp(bounds['now_pg'].iloc[0]).to_pydatetime()
        client.insert('etl_state', [[PIPELINE, t0, 0, 0]],
                      column_names=['pipeline', 'last_updated_at', 'last_id', 'rows_synced'])
        print(f't0 baru dikunci: {t0.isoformat()}')
    else:
        t0 = state.first_row[0]
        print(f'Melanjutkan backfill, t0 lama dipakai: {t0.isoformat()}')

    all_chunks = [str(p) for p in pd.period_range(
        start=_utc_naive(first_created), end=_utc_naive(t0), freq='M')]

    done = client.query(
        'SELECT chunk FROM etl_backfill_checkpoint FINAL WHERE pipeline = {p:String}',
        parameters={'p': PIPELINE},
    )
    done_chunks = {row[0] for row in done.result_rows}
    pending = [c for c in all_chunks if c not in done_chunks]

    print(f'{len(pending)} dari {len(all_chunks)} chunk belum diproses')
    return {'t0': t0.isoformat(), 'chunks': pending, 'total_chunks': len(all_chunks)}
