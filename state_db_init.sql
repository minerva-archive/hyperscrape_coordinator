-- only for one time db init here

-- this schema is not very normalized for easier old state compatibility

create table if not exists file
(
    id         text    not null
        constraint file_pk
            unique,
    path       text    not null,
    size       integer,
    url        text    not null,
    chunk_size integer not null,
    complete   integer not null default 0
);

create index if not exists file_path_index
    on file (path);

create table if not exists chunk
(
    id      text    not null
        constraint chunk_pk
            unique,
    file_id text    not null
        constraint chunk_file_id_fk
            references file (id),
    start   integer not null,
    end     integer not null
);

create index if not exists chunk_file_id_index
    on chunk (file_id);

create table if not exists worker
(
    id           integer primary key,
    chunk_id     text    not null
        constraint worker_chunk_id_fk
            references chunk (id)
            on delete cascade,
    worker_id    text    not null,
    last_updated integer not null,
    uploaded     integer not null default 0,
    complete     integer not null default 0,
    hash         text
);

create index if not exists worker_chunk_id_index
    on worker (chunk_id);

create index if not exists worker_worker_id_index
    on worker (worker_id);

create table if not exists file_hash
(
    file_id text not null
        constraint file_hash_pk
            unique
        constraint file_hash_file_id_fk
            references file (id),
    md5     text not null,
    sha1    text not null,
    sha256  text not null
);

create index if not exists file_hash_file_id_index
    on file_hash (file_id);

create table if not exists leaderboard
(
    discord_id        text    not null
        constraint leaderboard_pk
            unique,
    discord_username  text    not null,
    avatar_url        text,
    downloaded_chunks integer not null default 0,
    downloaded_bytes  integer not null default 0
);

