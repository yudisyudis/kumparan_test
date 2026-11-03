# update watermark yang akan digunakan untuk siklus update data berikutnya

import pandas as pd

from kumparan_etl.utils.warehouse import PIPELINE_NAME, get_clickhouse_client

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def update_watermark(*upstream_outputs, **kwargs):
    # Urutan argumen mengikuti urutan upstream, jadi ambil saja DataFrame-nya
    frames = [o for o in upstream_outputs if isinstance(o, pd.DataFrame)]
    if not frames or frames[0].empty:
        print('Tidak ada data baru, watermark tetap')
        return

    df = frames[0]
    latest = df.sort_values(['updated_at', 'id']).iloc[-1]

    client = get_clickhouse_client()
    client.insert(
        'etl_state',
        [[
            PIPELINE_NAME,
            latest['updated_at'].to_pydatetime(),
            int(latest['id']),
            len(df),
        ]],
        column_names=['pipeline', 'last_updated_at', 'last_id', 'rows_synced'],
    )
    print(f"Watermark digeser ke {latest['updated_at']} (id {int(latest['id'])})")
