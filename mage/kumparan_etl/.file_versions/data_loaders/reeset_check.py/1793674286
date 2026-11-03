# berfungsi mengecek apakah reset data diperbolehkan

import os

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def check_reset_allowed(*args, **kwargs):
    if os.environ.get('ALLOW_RESET', 'true').strip().lower() == 'false':
        raise PermissionError('Reset dinonaktifkan (ALLOW_RESET=false di .env).')
    print('Reset diizinkan: Postgres dan ClickHouse akan dikosongkan.')
    return {'allowed': True}