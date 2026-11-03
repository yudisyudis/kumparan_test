# Pin versi setelah first run berhasil (cek `docker compose images`), supaya reproducible.
FROM mageai/mageai:latest

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
