"""Helper bersama untuk menulis artikel ke ClickHouse (dipakai backfill dan hard delete).

Penulisan dikelompokkan per bulan created_at agar satu INSERT hanya menyentuh satu partisi
(ClickHouse menolak INSERT yang menyentuh lebih dari 100 partisi).
"""
from collections import defaultdict

import pandas as pd

COLUMNS = [
    'id', 'title', 'content', 'published_at', 'author_id',
    'created_at', 'updated_at', 'deleted_at',
    '_is_hard_deleted', '_version',
]
DATETIME_COLUMNS = ['published_at', 'created_at', 'updated_at', 'deleted_at']


def _dt(value):
    """NaT/None menjadi None (kolom Nullable), selain itu datetime Python."""
    return None if pd.isna(value) else value.to_pydatetime()


def prepare_articles_df(df):
    """Standarisasi minimal: semua timestamp UTC, content tidak NULL."""
    df = df.copy()
    for col in DATETIME_COLUMNS:
        df[col] = pd.to_datetime(df[col], utc=True)
    df['content'] = df['content'].fillna('')
    return df


def insert_articles(client, df, version, is_hard_deleted=0):
    """Tulis df (sudah lewat prepare_articles_df) per bulan. Mengembalikan jumlah partisi."""
    batches = defaultdict(list)
    for r in df.itertuples(index=False):
        batches[r.created_at.strftime('%Y%m')].append((
            int(r.id), r.title, r.content, _dt(r.published_at), int(r.author_id),
            _dt(r.created_at), _dt(r.updated_at), _dt(r.deleted_at),
            is_hard_deleted, version,
        ))
    for _, rows in sorted(batches.items()):
        client.insert('articles', rows, column_names=COLUMNS)
    return len(batches)
