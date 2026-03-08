BEGIN;
CREATE TABLE IF NOT EXISTS stat
(
    key         TEXT    PRIMARY KEY NOT NULL,
    value       BIGINT NOT NULL
);

INSERT INTO stat (key, value) VALUES ('downloaded_bytes', 0);

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
    PRIMARY KEY (chunk_id, worker_id)
);

CREATE TABLE IF NOT EXISTS worker_info
(
    id       TEXT PRIMARY KEY NOT NULL,
    ip       TEXT NOT NULL
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
    downloaded_chunks INTEGER NOT NULL DEFAULT 0,
    downloaded_bytes  INTEGER NOT NULL DEFAULT 0
);
COMMIT;