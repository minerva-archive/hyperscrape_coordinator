CREATE TABLE IF NOT EXISTS file
(
    id         TEXT     PRIMARY KEY NOT NULL,
    path       TEXT     NOT NULL,
    size       INTEGER, -- Nullable at first
    url        TEXT     NOT NULL,
    chunk_size INTEGER  NOT NULL,
    complete   INTEGER  NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS file_path_index
    on file (path);

CREATE TABLE IF NOT EXISTS chunk
(
    id          TEXT    PRIMARY KEY NOT NULL,
    file_id     TEXT    NOT NULL REFERENCES file(id),
    start       INTEGER NOT NULL,
    end         INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS chunk_file_id_index
    on chunk (file_id);

CREATE TABLE IF NOT EXISTS worker_status
(
    chunk_id        TEXT        NOT NULL,
    worker_id       TEXT        NOT NULL,
    uploaded        INTEGER     NOT NULL,
    hash            TEXT,                   -- Can be null
    hash_only       INTEGER     NOT NULL,
    last_updated    INTEGER     NOT NULL,
    PRIMARY KEY (chunk_id, worker_id)
)

CREATE INDEX IF NOT EXISTS worker_status_chunk_index
    on worker_status (chunk_id);

CREATE INDEX IF NOT EXISTS worker_status_worker_index
    on worker_status (worker_id);

CREATE TABLE IF NOT EXISTS file_hash
(
    file_id         TEXT NOT NULL UNIQUE REFERENCES file(id),
    md5             TEXT NOT NULL,
    sha1            TEXT NOT NULL,
    sha256          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS file_hash_file_id_index
    on file_hash (file_id);

CREATE TABLE IF NOT EXISTS leaderboard
(
    discord_id        TEXT      PRIMARY KEY NOT NULL,
    discord_username  TEXT      NOT NULL,
    avatar_url        TEXT, -- Nullable
    downloaded_chunks INTEGER NOT NULL DEFAULT 0,
    downloaded_bytes  INTEGER NOT NULL DEFAULT 0
);

