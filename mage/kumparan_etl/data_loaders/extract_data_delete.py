# mengambil informasi data terdelete

from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

# Mundur sedikit karena nomor event dari sequence bisa commit tidak berurutan.
# Aman karena langkah berikutnya idempoten (baris yang sudah ditandai dilewati).
EVENT_OVERLAP = 1000


@data_loader
def extract_tombstones(watermark, *args, **kwargs):
    start_event_id = max(int(watermark['last_event_id']) - EVENT_OVERLAP, 0)
    query = f"""
        SELECT event_id, article_id, deleted_at
        FROM public.articles_hard_deleted
        WHERE event_id > {start_event_id}
        ORDER BY event_id
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        df = loader.load(query)

    print(f'Ditemukan {len(df)} catatan hard delete sejak event_id {start_event_id}')
    return df
