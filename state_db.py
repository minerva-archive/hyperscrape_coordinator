import queue
import sqlite3
import threading
import time
from typing import Callable, Any


class StateDB:
    def __init__(self, db_path: str):
        self._db_path = db_path

        # we have multiple threads, so need to guarantee only one write at a time
        # sqlite is not thread safe and this is simpler than a writer queue
        # as of writing, it seems only main is writing the states during normal ops
        self._local = threading.local()
        self._write_lock = threading.Lock()
        self._write_conn = sqlite3.connect(db_path, check_same_thread=False)
        self._configure(self._write_conn)
        self._initialize_db()

        # aaaaand the performance is not cutting it, still need to batch writes
        self._write_queue = queue.Queue()

    @property
    def _conn(self):
        # reading conn for each thread
        if not hasattr(self._local, "conn"):
            self._local.conn = sqlite3.connect(self._db_path)
            self._configure(self._local.conn)
        return self._local.conn

    @staticmethod
    def _configure(conn: sqlite3.Connection):
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.row_factory = sqlite3.Row

    def _queue_write(self, query: str, params: tuple):
        self._write_queue.put((query, params))

    def flush(self):
        ops = []
        while not self._write_queue.empty():
            query, params = self._write_queue.get()
            ops.append((query, params))
        with self._write_lock:
            with self._write_conn:
                for query, params in ops:
                    self._write_conn.execute(query, params)

    def _initialize_db(self):
        # you wouldn't want to do bad things here
        with open("state_db_init.sql") as f:
            with self._write_conn:
                self._write_conn.executescript(f.read())

    # now comes business logic

    # startup / batch fetching

    def get_files(self) -> list[dict]:
        with self._conn:
            cur = self._conn.execute("SELECT * FROM file")
            return cur.fetchall()

    def get_chunks(self) -> list[dict]:
        with self._conn:
            cur = self._conn.execute("SELECT * FROM chunk")
            return cur.fetchall()

    def get_chunks_for_file(self, file_id: str) -> list[dict]:
        with self._conn:
            cur = self._conn.execute("SELECT * FROM chunk WHERE file_id = ?", (file_id,))
            return cur.fetchall()

    def get_workers_for_chunk(self, chunk_id: str) -> list[dict]:
        with self._conn:
            cur = self._conn.execute("SELECT * FROM worker WHERE chunk_id = ?", (chunk_id,))
            return cur.fetchall()

    def get_file_hashes(self):
        with self._conn:
            cur = self._conn.execute("SELECT path, md5, sha1, sha256 FROM file_hash JOIN main.file f on f.id = file_hash.file_id")
            return cur.fetchall()

    def get_leaderboard(self):
        with self._conn:
            cur = self._conn.execute("SELECT * FROM leaderboard ORDER BY downloaded_bytes DESC")
            return cur.fetchall()

    # file mutations

    def insert_file(self, file_id: str, path: str, size: int, url: str, chunk_size: int):
        self._queue_write(
            "INSERT INTO file (id, path, size, url, chunk_size) "
            "VALUES (?, ?, ?, ?, ?)",
            (file_id, path, size, url, chunk_size)
        )

    def set_file_size(self, file_id: str, size: int):
        self._queue_write(
            "UPDATE file SET size = ? WHERE id = ?",
            (size, file_id)
        )

    def set_file_chunk_size(self, file_id: str, chunk_size: int):
        self._queue_write(
            "UPDATE file SET chunk_size = ? WHERE id = ?",
            (chunk_size, file_id)
        )

    def set_file_complete(self, file_id: str):
        self._queue_write(
            "UPDATE file SET complete = 1 WHERE id = ?",
            (file_id,)
        )

    # chunk / worker mutations

    def insert_chunk(self, file_id: str, chunk_id: str, start: int, end: int):
        self._queue_write(
            "INSERT INTO chunk (file_id, id, start, end) "
            "VALUES (?, ?, ?, ?)",
            (file_id, chunk_id, start, end)
        )

    def delete_chunk(self, chunk_id: str):
        self._queue_write(
            "DELETE FROM chunk WHERE id = ?",
            (chunk_id,)
        )

    def insert_worker(self, chunk_id: str, worker_id: str):
        self._queue_write(
            "INSERT INTO worker (chunk_id, worker_id, last_updated) "
            "VALUES (?, ?, ?)",
            (chunk_id, worker_id, int(time.time()))
        )

    def delete_worker(self, chunk_id: str, worker_id: str):
        self._queue_write(
            "DELETE FROM worker WHERE chunk_id = ? AND worker_id = ?",
            (chunk_id, worker_id)
        )

    def set_worker_last_updated(self, chunk_id: str, worker_id: str):
        self._queue_write(
            "UPDATE worker SET last_updated = ? WHERE chunk_id = ? AND worker_id = ?",
            (int(time.time()), chunk_id, worker_id)
        )

    def set_worker_uploaded(self, chunk_id: str, worker_id: str, uploaded: int):
        self._queue_write(
            "UPDATE worker SET uploaded = ? WHERE chunk_id = ? AND worker_id = ?",
            (uploaded, chunk_id, worker_id)
        )

    def set_worker_hash(self, chunk_id: str, worker_id: str, hash: str):
        self._queue_write(
            "UPDATE worker SET hash = ? WHERE chunk_id = ? AND worker_id = ?",
            (hash, chunk_id, worker_id)
        )

    def set_worker_complete(self, chunk_id: str, worker_id: str, hash: str):
        self._queue_write(
            "UPDATE worker SET complete = 1, hash = ?, last_updated = ? WHERE chunk_id = ? AND worker_id = ?",
            (hash, int(time.time()), chunk_id, worker_id)
        )

    # file hash mutations

    def insert_file_hash(self, file_id: str, md5: str, sha1: str, sha256: str):
        self._queue_write(
            "INSERT INTO file_hash (file_id, md5, sha1, sha256) "
            "VALUES (?, ?, ?, ?)",
            (file_id, md5, sha1, sha256)
        )

    # leaderboard mutations

    def insert_leaderboard_entry(self, discord_id: str, discord_username: str, avatar_url: str):
        self._queue_write(
            "INSERT INTO leaderboard (discord_id, discord_username, avatar_url) "
            "VALUES (?, ?, ?) "
            "ON CONFLICT (discord_id) DO NOTHING",
            (discord_id, discord_username, avatar_url)
        )

    def update_leaderboard_downloaded_bytes(self, discord_id: str, change: int):
        self._queue_write(
            "UPDATE leaderboard SET downloaded_bytes = downloaded_bytes + ? WHERE discord_id = ?",
            (change, discord_id)
        )

    def update_leaderboard_downloaded_chunks(self, discord_id: str, change: int):
        self._queue_write(
            "UPDATE leaderboard SET downloaded_chunks = downloaded_chunks + ? WHERE discord_id = ?",
            (change, discord_id)
        )


# global singleton and we'll just use the hardcoded filename here
# each thread will have its own conn on first use
db: StateDB = StateDB("state.db")
