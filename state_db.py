import sqlite3
import threading


class StateDB:
    """!
    @brief The state's database storage system
    """

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._local = threading.local()
        self._write_lock = threading.Lock()
        self._read_conn_lock = threading.Lock()
        self._read_conns: set[sqlite3.Connection] = set()
        self._write_conn: sqlite3.Connection | None = sqlite3.connect(db_path, check_same_thread=False)
        self._configure(self._write_conn)
        self._initialize_db()

    def close(self):
        with self._write_lock:
            if (self._write_conn is None):
                return
            try:
                with self._read_conn_lock:
                    for conn in list(self._read_conns):
                        try:
                            conn.close()
                        except:
                            pass
                    self._read_conns.clear()
                self._write_conn.close()
            finally:
                self._write_conn = None

    @property
    def _conn(self):
        # read conn per thread
        if not hasattr(self._local, "conn"):
            conn = sqlite3.connect(self._db_path)
            self._configure(conn)
            self._local.conn = conn
            with self._read_conn_lock:
                self._read_conns.add(conn)
        return self._local.conn

    @staticmethod
    def _configure(conn: sqlite3.Connection):
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = normal")
        conn.execute("PRAGMA page_size = 8192")
        conn.execute("PRAGMA journal_size_limit = 6144000")
        conn.row_factory = sqlite3.Row

    def _ensure_open(self):
        if (self._write_conn is None):
            raise RuntimeError("StateDB is closed")

    def _write(self, query: str, params: tuple):
        with self._write_lock:
            self._ensure_open()
            with self._write_conn:
                self._write_conn.execute(query, params)

    def flush(self):
        # Writes are immediate; kept for compatibility with existing call sites.
        return

    def _initialize_db(self):
        with open("state_db_init.sql") as f:
            with self._write_conn:
                self._write_conn.executescript(f.read())

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

    def get_chunk_worker_status(self, chunk_id: str) -> list[dict]:
        with self._conn:
            cur = self._conn.execute("SELECT * FROM worker_status WHERE chunk_id = ?", (chunk_id,))
            return cur.fetchall()

    def get_file_hashes(self):
        with self._conn:
            cur = self._conn.execute("SELECT path, md5, sha1, sha256 FROM file_hash JOIN file on file.id = file_hash.file_id")
            return cur.fetchall()

    def get_leaderboard(self):
        with self._conn:
            cur = self._conn.execute("SELECT * FROM leaderboard ORDER BY downloaded_bytes DESC")
            return cur.fetchall()

    # file mutations

    def insert_file(self, file_id: str, path: str, size: int, url: str, chunk_size: int, complete: bool = False):
        self._write(
            "INSERT INTO file (id, path, size, url, chunk_size, complete) VALUES (?, ?, ?, ?, ?, ?)",
            (file_id, path, size, url, chunk_size, complete),
        )

    def set_file_size(self, file_id: str, size: int):
        self._write(
            "UPDATE file SET size = ? WHERE id = ?",
            (size, file_id),
        )

    def set_file_chunk_size(self, file_id: str, chunk_size: int):
        self._write(
            "UPDATE file SET chunk_size = ? WHERE id = ?",
            (chunk_size, file_id),
        )

    def set_file_complete(self, file_id: str):
        self._write(
            "UPDATE file SET complete = 1 WHERE id = ?",
            (file_id,),
        )

    # chunk / worker mutations

    def insert_chunk(self, chunk_id: str, file_id: str, start: int, end: int):
        self._write(
            "INSERT INTO chunk (id, file_id, start, end) VALUES (?, ?, ?, ?)",
            (chunk_id, file_id, start, end),
        )

    def delete_chunk(self, chunk_id: str):
        self._write(
            "DELETE FROM chunk WHERE id = ?",
            (chunk_id,),
        )

    def insert_worker_status(self, chunk_id: str, worker_id: str, uploaded: int = 0, hash: str = "", hash_only: bool = True):
        self._write(
            "INSERT OR REPLACE INTO worker_status (chunk_id, worker_id, uploaded, hash, hash_only) VALUES (?, ?, ?, ?, ?)",
            (chunk_id, worker_id, uploaded, hash, hash_only),
        )

    def delete_worker_status(self, chunk_id: str, worker_id: str):
        self._write(
            "DELETE FROM worker_status WHERE chunk_id = ? AND worker_id = ?",
            (chunk_id, worker_id),
        )

    def delete_chunk_worker_status(self, chunk_id: str):
        self._write(
            "DELETE FROM worker_status WHERE chunk_id = ?",
            (chunk_id,),
        )

    def set_worker_status_uploaded(self, chunk_id: str, worker_id: str, uploaded: int):
        self._write(
            "UPDATE worker_status SET uploaded = ? WHERE chunk_id = ? AND worker_id = ?",
            (uploaded, chunk_id, worker_id),
        )

    def set_worker_status_hash(self, chunk_id: str, worker_id: str, hash: str):
        self._write(
            "UPDATE worker_status SET hash = ? WHERE chunk_id = ? AND worker_id = ?",
            (hash, chunk_id, worker_id),
        )

    def set_worker_status_hash_only(self, chunk_id: str, worker_id: str, hash_only: bool):
        self._write(
            "UPDATE worker_status SET hash_only = ? WHERE chunk_id = ? AND worker_id = ?",
            (hash_only, chunk_id, worker_id),
        )

    # file hash mutations

    def insert_file_hash(self, file_id: str, md5: str, sha1: str, sha256: str):
        self._write(
            "INSERT INTO file_hash (file_id, md5, sha1, sha256) VALUES (?, ?, ?, ?)",
            (file_id, md5, sha1, sha256),
        )

    # leaderboard mutations

    def insert_leaderboard_entry(
        self,
        discord_id: str,
        discord_username: str,
        avatar_url: str,
        downloaded_chunks: int = 0,
        downloaded_bytes: int = 0,
    ):
        self._write(
            "INSERT INTO leaderboard (discord_id, discord_username, avatar_url, downloaded_chunks, downloaded_bytes) VALUES (?, ?, ?, ?, ?) ON CONFLICT (discord_id) DO NOTHING",
            (discord_id, discord_username, avatar_url, downloaded_chunks, downloaded_bytes),
        )

    def update_leaderboard_downloaded_bytes(self, discord_id: str, change: int):
        self._write(
            "UPDATE leaderboard SET downloaded_bytes = downloaded_bytes + ? WHERE discord_id = ?",
            (change, discord_id),
        )

    def update_leaderboard_downloaded_chunks(self, discord_id: str, change: int):
        self._write(
            "UPDATE leaderboard SET downloaded_chunks = downloaded_chunks + ? WHERE discord_id = ?",
            (change, discord_id),
        )


# global singleton and we'll just use the hardcoded filename here
# each thread will have its own conn on first use
db: StateDB = StateDB("state.db")
