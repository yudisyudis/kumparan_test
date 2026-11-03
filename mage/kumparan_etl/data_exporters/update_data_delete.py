# menandai data terdelete di data warehouse

import time

import pandas as pd

from kumparan_etl.utils.articles import insert_articles, prepare_articles_df
from kumparan_etl.utils.warehouse import get_clickhouse_client

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

PIPELINE = 'data_delete_handler'
LOOKUP_BATCH = 10_000

LOOKUP_SQL = """
    SELECT id, title, content, published_at, author_id, created_at, updated_at, deleted_at
    FROM articles FINAL
    WHERE id IN {ids:Array(UInt64)} AND _is_hard_deleted = 0
"""


@data_exporter
def apply_hard_deletes(*upstream_outputs, **kwargs):
    frames = [o for o in upstream_outputs if isinstance(o, pd.DataFrame)]
    if not frames or frames[0].empty:
        print('Tidak ada hard delete baru')
        return
    tombstones = frames[0]

    client = get_clickhouse_client()
    ids = sorted({int(x) for x in tombstones['article_id']})

    # Versi terakhir artikel di warehouse, yang belum ditandai terhapus
    found = []
    for i in range(0, len(ids), LOOKUP_BATCH):
        found.append(client.query_df(LOOKUP_SQL, parameters={'ids': ids[i:i + LOOKUP_BATCH]}))
    existing = pd.concat(found, ignore_index=True) if found else pd.DataFrame()

    written = 0
    if not existing.empty:
        version = int(time.time() * 1000)
        insert_articles(client, prepare_articles_df(existing), version, is_hard_deleted=1)
        written = len(existing)

    # Artikel yang terhapus sebelum pernah tersinkron (atau sudah ditandai) tidak punya baris
    # untuk ditandai, tetapi event-nya tetap dianggap selesai.
    last = tombstones.iloc[-1]
    client.insert(
        'etl_state',
        [[PIPELINE, pd.Timestamp(last['deleted_at']).to_pydatetime(), int(last['event_id']), written]],
        column_names=['pipeline', 'last_updated_at', 'last_id', 'rows_synced'],
    )
    print(f'{written} artikel ditandai hard delete, state digeser ke event_id {int(last["event_id"])}')
