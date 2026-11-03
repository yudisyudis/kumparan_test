# load data ke data warehouse

import time
from collections import defaultdict

import pandas as pd

from kumparan_etl.utils.warehouse import get_clickhouse_client

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

COLUMNS = [
    'id', 'title', 'content', 'published_at', 'author_id',
    'created_at', 'updated_at', 'deleted_at',
    '_is_hard_deleted', '_version',
]


def _dt(value):
    """NaT/None menjadi None (kolom Nullable), selain itu datetime Python."""
    return None if pd.isna(value) else value.to_pydatetime()


@data_exporter
def load_articles(df, *args, **kwargs):
    if df.empty:
        print('Tidak ada perubahan, tidak ada yang ditulis')
        return

    version = int(time.time() * 1000)   # waktu sync (ms), versi yang lebih besar menang

    # Kelompokkan baris per bulan created_at (UTC, sama dengan toYYYYMM di ClickHouse)
    batches = defaultdict(list)
    for r in df.itertuples(index=False):
        batches[r.created_at.strftime('%Y%m')].append((
            int(r.id), r.title, r.content, _dt(r.published_at), int(r.author_id),
            _dt(r.created_at), _dt(r.updated_at), _dt(r.deleted_at),
            0, version,
        ))

    client = get_clickhouse_client()
    for month, rows in sorted(batches.items()):
        client.insert('articles', rows, column_names=COLUMNS)

    print(f'Ditulis {len(df)} baris ke ClickHouse dalam {len(batches)} partisi bulanan')
