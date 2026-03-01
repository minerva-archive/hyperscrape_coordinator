from threading import Lock
import time

class WorkerStatus():
    def __init__(self, downloaded: int = 0, uploaded: int = 0):
        self.started: int = time.time()
        self.last_updated: int = time.time()
        self.downloaded = downloaded
        self.uploaded = uploaded
        self.complete = False
        self.hash = None
    
    def mark_updated(self):
        self.last_updated = time.time()

    def mark_complete(self, hash: str):
        self.hash = hash
        self.complete = True

###
# We store files and chunks separately because I like, hate myself
# It's also more efficient
###
class HyperscrapeChunk():
    def __init__(self, chunk_id: str, start: int, end: int):
        self._lock = Lock()
        self._chunk_id = chunk_id
        self._start = start
        self._end = end
        self._worker_status: dict[str, WorkerStatus] = {}

    def __getstate__(self):
        return (self._chunk_id, self._start, self._end, self._worker_status)
    
    def __setstate__(self, state):
        self._chunk_id, self._start, self._end, self._worker_status = state
        self._lock = Lock()

    def get_id(self) -> str:
        return self._chunk_id
    
    def get_start(self) -> int:
        return self._start
    
    def get_end(self) -> int:
        return self._end
    
    def get_lock(self) -> Lock:
        return self._lock
    
    def add_worker_status(self, worker_id: str):
        self._lock.acquire()
        self._worker_status[worker_id] = WorkerStatus()
        self._lock.release()

    def has_worker(self, worker_id: str):
        return worker_id in self._worker_status
    
    def get_workers(self):
        return self._worker_status.keys()

    def get_worker_status(self, worker_id: str):
        return self._worker_status[worker_id]
    
    def get_worker_count(self):
        return len(self._worker_status)

    def update_worker_status_downloaded(self, worker_id: str, downloaded: int):
        self._lock.acquire()
        self._worker_status[worker_id].downloaded = downloaded
        self._worker_status[worker_id].mark_updated()
        self._lock.release()

    def update_worker_status_uploaded(self, worker_id: str, downloaded: int):
        self._lock.acquire()
        self._worker_status[worker_id].downloaded = downloaded
        self._worker_status[worker_id].mark_updated()
        self._lock.release()

    def mark_worker_status_complete(self, worker_id: str, hash: str):
        self._lock.acquire()
        self._worker_status[worker_id].hash = hash
        self._worker_status[worker_id].complete = True
        self._worker_status[worker_id].mark_updated()
        self._lock.release()

    def remove_worker_status(self, worker_id: str):
        self._lock.acquire()
        del self._worker_status[worker_id]
        self._lock.release()


class HyperscrapeFile():
    def __init__(self, file_id: str, file_path: str, total_size: int|None, url: str, chunk_size: int):
        self._lock = Lock()
        self._file_id = file_id
        self._file_path = file_path
        self._total_size: int|None = total_size # In bytes
        self._url = url
        self._chunk_size = chunk_size
        self._chunks: set[str] = set()
        self._complete: bool = False # Only set once the entire file is actually properly complete like actually

    def __getstate__(self):
        return (self._file_id, self._file_path, self._total_size, self._url, self._chunk_size, self._chunks, self._complete)
    
    def __setstate__(self, state):
        self._file_id, self._file_path, self._total_size, self._url, self._chunk_size, self._chunks, self._complete = state
        self._lock = Lock()

    def get_id(self) -> str:
        return self._file_id
    
    def get_path(self) -> str:
        return self._file_path
    
    def get_total_size(self) -> int|None:
        return self._total_size
    
    def set_total_size(self, total_size: int):
        self._lock.acquire()
        self._total_size = total_size
        self._lock.release()

    def get_url(self) -> str:
        return self._url
    
    def get_chunk_size(self) -> int:
        return self._chunk_size
    
    def set_chunk_size(self, chunk_size: int):
        self._lock.acquire()
        self._chunk_size = chunk_size
        self._lock.release()

    def get_chunks(self) -> set[str]:
        return self._chunks
    
    def add_chunk(self, chunk_id: str):
        self._lock.acquire()
        self._chunks.add(chunk_id)
        self._lock.release()

    def has_chunk(self, chunk_id: str):
        return chunk_id in self._chunks
    
    def clear_chunks(self):
        self._lock.acquire()
        self._chunks = []
        self._lock.release()

    def get_complete(self) -> bool:
        return self._complete

    def mark_complete(self):
        self._lock.acquire()
        self._complete = True
        self._lock.release()

    def get_lock(self) -> Lock:
        return self._lock