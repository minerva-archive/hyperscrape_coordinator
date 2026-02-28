import time

class WorkerStatus():
    def __init__(self, downloaded: int, uploaded: int):
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
        self.chunk_id = chunk_id
        self.start = start
        self.end = end
        self.worker_status: dict[str, WorkerStatus] = {}

    def cleanup_workers(self, timeout=30):
        for worker_id in list(self.worker_status.keys()):
            if ((not self.worker_status[worker_id].complete) and time.time() - self.worker_status[worker_id].last_updated > timeout):
                del self.worker_status[worker_id]

class HyperscrapeFile():
    def __init__(self, file_id: str, file_path: str, total_size: int, url: str, chunk_size: int):
        self.file_id = file_id
        if (file_path[0] == '/'):
            file_path = file_path[1:]
        self.file_path = file_path
        self.total_size = total_size # In bytes
        self.url = url
        self.chunk_size = chunk_size
        self.chunks: list[str] = []