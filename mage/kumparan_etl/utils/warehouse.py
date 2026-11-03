"""Helper koneksi ke ClickHouse, dipakai oleh beberapa block pipeline.

Kredensial dibaca dari environment container Mage (berasal dari .env lewat env_file).
Port 8123 ditulis tetap karena itu port HTTP ClickHouse di jaringan internal Docker,
terlepas dari port yang dipetakan ke komputer Anda.
"""
import os

import clickhouse_connect

PIPELINE_NAME = 'articles_incremental'


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=os.environ['CLICKHOUSE_HOST'],
        port=8123,
        username=os.environ['CLICKHOUSE_USER'],
        password=os.environ['CLICKHOUSE_PASSWORD'],
        database=os.environ['CLICKHOUSE_DB'],
    )
