# Block: transform_articles | Type: Transformer (Python, Generic)
# Tugas: standarisasi minimal. Data mentah tetap setia ke sumber.
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

DATETIME_COLUMNS = ['published_at', 'created_at', 'updated_at', 'deleted_at']


@transformer
def transform_articles(df, *args, **kwargs):
    if df.empty:
        return df

    df = df.copy()
    for col in DATETIME_COLUMNS:
        df[col] = pd.to_datetime(df[col], utc=True)   # semua timestamp seragam UTC

    df['content'] = df['content'].fillna('')           # kolom String di ClickHouse tidak boleh NULL
    return df
