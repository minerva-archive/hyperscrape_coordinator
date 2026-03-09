BEGIN;
CREATE TABLE IF NOT EXISTS stat
(
    key         TEXT    PRIMARY KEY NOT NULL,
    value       BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS file
(
    id         TEXT     PRIMARY KEY NOT NULL,
    path       TEXT     NOT NULL,
    size       BIGINT, -- Nullable at first
    url        TEXT     NOT NULL,
    chunk_size BIGINT  NOT NULL,
    complete   BOOLEAN  NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS chunk
(
    id          TEXT    PRIMARY KEY NOT NULL,
    file_id     TEXT    NOT NULL REFERENCES file(id),
    range_start       BIGINT NOT NULL,
    range_end         BIGINT NOT NULL,
    UNIQUE(file_id, range_start)
);

CREATE TABLE IF NOT EXISTS worker_status
(
    chunk_id        TEXT        NOT NULL,
    worker_id       TEXT        NOT NULL,
    uploaded        BIGINT     NOT NULL,
    hash            TEXT,                   -- Can be null
    hash_only       BOOLEAN     NOT NULL,
    last_updated    TIMESTAMP   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chunk_id, worker_id)
);

CREATE TABLE IF NOT EXISTS worker_info
(
    id          TEXT PRIMARY KEY NOT NULL,
    discord_id  TEXT NOT NULL,
    ip          TEXT NOT NULL,
    last_seen   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS file_hash
(
    file_id         TEXT NOT NULL UNIQUE REFERENCES file(id),
    md5             TEXT NOT NULL,
    sha1            TEXT NOT NULL,
    sha256          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS leaderboard
(
    discord_id        TEXT      PRIMARY KEY NOT NULL,
    discord_username  TEXT      NOT NULL,
    avatar_url        TEXT, -- Nullable
    downloaded_chunks BIGINT NOT NULL DEFAULT 0,
    downloaded_bytes  BIGINT NOT NULL DEFAULT 0
);

-- Create a materialized view that is an ordered list of chunks
CREATE MATERIALIZED VIEW IF NOT EXISTS ordered_chunks AS
SELECT file.id AS file_id, file.size AS file_size, file.url AS file_url,
chunk.id AS chunk_id, chunk.range_start AS chunk_range_start, chunk.range_end AS chunk_range_end,
COUNT(worker_status.worker_id) AS assigned_workers
FROM file
JOIN chunk ON chunk.file_id=file.id
LEFT JOIN worker_status ON worker_status.chunk_id=chunk.id
WHERE (NOT file.complete)
AND (file.size IS NOT NULL AND file.size != 0)
GROUP BY file.id, chunk.id
ORDER BY COUNT(worker_status.worker_id) DESC;

CREATE UNIQUE INDEX IF NOT EXISTS ordered_chunk_index ON ordered_chunks(file_id, chunk_id);

COMMIT;