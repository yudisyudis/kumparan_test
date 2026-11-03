-- =====================================================================
-- Pembuatan struktur tabel di Data Warehouse: ClickHouse
-- =====================================================================

CREATE DATABASE IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.articles
(
    id               UInt64,
    title            String,
    content          String CODEC(ZSTD(3)),
    published_at     Nullable(DateTime64(3, 'UTC')),
    author_id        UInt64,
    created_at       DateTime64(3, 'UTC'),
    updated_at       DateTime64(3, 'UTC'),
    deleted_at       Nullable(DateTime64(3, 'UTC')),   
    _is_hard_deleted UInt8 DEFAULT 0,                  -
    _version         UInt64,                           
    _synced_at       DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_version) --algoritma yang digunakan untuk handle data terupdate/berubah
PARTITION BY toYYYYMM(created_at)
ORDER BY id;

-- ---------------------------------------------------------------------
-- versi yang sudah bersih dari log data terupdate/terdelete
-- ---------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS dwh.articles_current AS
SELECT *
FROM dwh.articles FINAL
WHERE _is_hard_deleted = 0;

-- ---------------------------------------------------------------------
-- menyimpan watermark
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
