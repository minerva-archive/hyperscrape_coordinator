from threading import Lock
from state_db import db
import time


class WorkerStatus():
    def __init__(self, uploaded: int = 0, hash: str|None = None, hash_only: bool = True, last_updated: int | None = None):
        if last_updated is None:
            last_updated = time.time()
        self._last_updated: int = last_updated
        self._uploaded: int = uploaded
        self._hash: str|None = hash
        self._hash_only: bool = hash_only # Whether this worker uploaded data that was ONLY hashed, by default we don't actually write downloaded data!
        self._lock: Lock = Lock()

    
###
# We store files and chunks separately because I like, hate myself
# It's also more efficient
###
class HyperscrapeChunk():
    def __init__(self, chunk_id: str, start: int, end: int, worker_status: dict[str, WorkerStatus] = None):
        if worker_status is None:
            worker_status = {}
        self._lock: Lock = Lock()
        self._chunk_id: str = chunk_id
        self._start: int = start
        self._end: int = end
        self._worker_status: dict[str, WorkerStatus] = worker_status

    def get_id(self) -> str:
        return self._chunk_id
    
    def get_start(self) -> int:
        return self._start
    
    def get_end(self) -> int:
        return self._end
    
    def get_lock(self) -> Lock:
        return self._lock
    
    def add_worker_status(self, worker_id: str):
        self._worker_status[worker_id] = WorkerStatus()

    def has_worker(self, worker_id: str):
        return worker_id in self._worker_status
    
    def get_workers(self):
        return self._worker_status.keys()
    
    def get_worker_count(self):
        return len(self._worker_status)
    
    def update_worker_status_uploaded(self, worker_id: str, uploaded: int):
        with self.get_worker_lock(worker_id):
            self.get_worker_uploaded(uploaded)
            self.mark_worker_updated(worker_id)

    def mark_worker_status_complete(self, worker_id: str, hash: str):
        with self.get_worker_lock(worker_id):
            self.get_worker_complete(worker_id, hash)
            self.mark_worker_updated(worker_id)
            # The database MUST ONLY store already completed statuses
            db.insert_worker_status(self._chunk_id, worker_id, self._worker_status[worker_id]._uploaded, self._worker_status[worker_id]._hash, self._worker_status[worker_id]._hash_only)

    def remove_worker_status(self, worker_id: str):
        if (worker_id in self._worker_status):
            with self._worker_status[worker_id]._lock:
                del self._worker_status[worker_id]
                db.delete_worker_status(self._chunk_id, worker_id)

    def get_lock(self):
        return self._lock
    
    ###
    # Worker status
    ###
    def get_worker_last_updated(self, worker_id: str):
        return self._worker_status[worker_id]._last_updated
    
    def mark_worker_updated(self, worker_id: str):
        self._worker_status[worker_id]._last_updated = time.time()
    
    def get_worker_uploaded(self, worker_id: str):
        return self._worker_status[worker_id]._uploaded
    
    def get_worker_uploaded(self, worker_id: str, uploaded: int):
        self._worker_status[worker_id]._uploaded = uploaded
        db.set_worker_status_uploaded(self._chunk_id, worker_id, uploaded)
    
    def get_worker_complete(self, worker_id: str):
        return self._worker_status[worker_id]._hash != None
    
    def set_worker_complete(self, worker_id: str, hash: str):
        self._worker_status[worker_id]._hash = hash
        db.set_worker_status_hash(self._chunk_id, worker_id, hash)

    def get_worker_hash_only(self, worker_id: str):
        return self._worker_status[worker_id]._hash_only
    
    def set_worker_hash_only(self, worker_id: str, hash_only: bool):
        self._worker_status[worker_id]._hash_only = hash_only
        db.set_worker_status_hash_only(self._chunk_id, worker_id, hash_only)

    def get_worker_hash(self, worker_id: str):
        return self._worker_status[worker_id]._hash
    
    def get_worker_lock(self, worker_id: str):
        return self._worker_status[worker_id]._lock



class HyperscrapeFile():
    def __init__(self, file_id: str, file_path: str, total_size: int|None, url: str, chunk_size: int, chunks: set[str] = None, complete: bool = False):
        if chunks is None:
            chunks = set()
        self._lock: Lock = Lock()
        self._file_id: str = file_id
        self._file_path: str = file_path
        self._total_size: int|None = total_size # In bytes
        self._url: str = url
        self._chunk_size: int = chunk_size
        self._chunks: set[str] = chunks
        self._complete: bool = complete # will need to re-init from database

    def get_id(self) -> str:
        return self._file_id
    
    def get_path(self) -> str:
        return self._file_path
    
    def get_total_size(self) -> int|None:
        return self._total_size
    
    def set_total_size(self, total_size: int):
        self._total_size = total_size
        db.set_file_size(self._file_id, total_size)

    def get_url(self) -> str:
        return self._url
    
    def get_chunk_size(self) -> int:
        return self._chunk_size
    
    def set_chunk_size(self, chunk_size: int):
        self._chunk_size = chunk_size
        db.set_file_chunk_size(self._file_id, chunk_size)

    def get_chunks(self) -> set[str]:
        return self._chunks
    
    def add_chunk(self, chunk_id: str):
        self._chunks.add(chunk_id)

    def has_chunk(self, chunk_id: str):
        return chunk_id in self._chunks

    def get_complete(self) -> bool:
        return self._complete

    def mark_complete(self):
        self._complete = True
        db.set_file_complete(self._file_id)

    def get_lock(self) -> Lock:
        return self._lock