-- =====================================================================
-- SEMUA objek warehouse dibuat di file ini (dijalankan otomatis saat volume kosong).
-- Tidak ada tabel yang dibuat manual maupun oleh pipeline.
-- =====================================================================
-- =====================================================================
-- WAREHOUSE: ClickHouse
-- =====================================================================
CREATE DATABASE IF NOT EXISTS dwh;

-- ---------------------------------------------------------------------
-- Tabel utama. Satu baris logis per article id.
--
-- ReplacingMergeTree(_version):
--   Upsert di ClickHouse = insert versi baru, lalu baris lama dibuang saat merge.
--   Merge berjalan asinkron, jadi pembacaan harus pakai FINAL / view di bawah.
--   _version = waktu sync (ms), sehingga re-run/backfill selalu idempoten
--   dan baris yang disync belakangan selalu menang.
--
-- ORDER BY id           : kunci dedup (sorting key = identitas baris)
-- PARTITION BY created_at: created_at tidak pernah berubah, jadi semua versi
--   satu id selalu di partisi yang sama (syarat agar dedup bekerja).
--   Partisi bulanan juga memudahkan backfill/rekonsiliasi per bulan.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.articles
(
    id               UInt64,
    title            String,
    content          String CODEC(ZSTD(3)),
    published_at     Nullable(DateTime64(3, 'UTC')),
    author_id        UInt64,
    created_at       DateTime64(3, 'UTC'),
    updated_at       DateTime64(3, 'UTC'),
    deleted_at       Nullable(DateTime64(3, 'UTC')),   -- soft delete dari sumber

    _is_hard_deleted UInt8 DEFAULT 0,                  -- 1 = barisnya sudah dihapus fisik di sumber
    _version         UInt64,                           -- waktu sync dalam ms (versi untuk Replacing)
    _synced_at       DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY id;

-- ---------------------------------------------------------------------
-- View konsumsi untuk analis: versi terbaru, tanpa baris hard delete.
-- Soft delete tetap muncul (deleted_at terisi), analis memfilter sendiri
-- sesuai kebutuhan.
-- ---------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS dwh.articles_current AS
SELECT *
FROM dwh.articles FINAL
WHERE _is_hard_deleted = 0;

-- ---------------------------------------------------------------------
-- State pipeline (watermark). Watermark komposit (updated_at, id) supaya
-- baris dengan updated_at identik di batas batch tidak terlewat.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.etl_state
(
    pipeline         String,
    last_updated_at  DateTime64(3, 'UTC'),
    last_id          UInt64,
    rows_synced      UInt64,
    run_at           DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(run_at)
ORDER BY pipeline;

-- ---------------------------------------------------------------------
-- Checkpoint backfill: satu baris per chunk (bulan) yang sudah selesai dimuat
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.etl_backfill_checkpoint
(
    pipeline      String,
    chunk         String,                    -- format 'YYYY-MM'
    rows_written  UInt64,
    finished_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(finished_at)
ORDER BY (pipeline, chunk);
