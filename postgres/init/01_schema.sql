-- =====================================================================
-- SOURCE DB: articles
-- =====================================================================

CREATE TABLE articles (
    id            BIGSERIAL    PRIMARY KEY,
    title         TEXT         NOT NULL,
    content       TEXT,
    published_at  TIMESTAMPTZ,                          
    author_id     BIGINT       NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),  -- diisi saat dibuat
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),  -- diisi saat dibuat DAN saat diubah
    deleted_at    TIMESTAMPTZ                           -- diisi saat soft delete
);

CREATE INDEX idx_articles_updated_at_id ON articles (updated_at, id);
CREATE INDEX idx_articles_author_id     ON articles (author_id);

CREATE OR REPLACE FUNCTION trg_set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER articles_set_updated_at
BEFORE UPDATE ON articles
FOR EACH ROW EXECUTE FUNCTION trg_set_updated_at();

-- ---------------------------------------------------------------------
-- membaca seluruh aktivitas hard delete dari hasil trigger
-- ---------------------------------------------------------------------
CREATE TABLE articles_hard_deleted (
    event_id    BIGSERIAL    PRIMARY KEY,
    article_id  BIGINT       NOT NULL,
    deleted_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX idx_articles_hard_deleted_at ON articles_hard_deleted (deleted_at, event_id);

CREATE OR REPLACE FUNCTION trg_log_hard_delete() RETURNS trigger AS $$
BEGIN
    INSERT INTO articles_hard_deleted (article_id) VALUES (OLD.id);
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER articles_log_hard_delete
AFTER DELETE ON articles
FOR EACH ROW EXECUTE FUNCTION trg_log_hard_delete();
