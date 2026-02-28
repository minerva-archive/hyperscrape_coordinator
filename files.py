from uuid import uuid4

class WorkerStatus():
    def __init__(self, downloaded: int, uploaded: int):
        self.downloaded = downloaded
        self.uploaded = uploaded

###
# We store files and chunks separately because I like, hate myself
# It's also more efficient
###
class HyperscrapeChunk():
    def __init__(self, start: int, end: int):
        self.start = start
        self.end = end
        self.uploaded = False
        self.worker_status: dict[str, WorkerStatus] = {}

class HyperscrapeFile():
    def __init__(self, file_id: str, file_path: str, total_size: int, url: str, chunk_size: int):
        self.file_id = file_id
        self.file_path = file_path
        self.total_size = total_size # In bytes
        self.url = url
        self.chunk_size = chunk_size
        self.chunks: list[str] = []
        self.complete = False # Whether or not the file is totally complete or not
        self.receiver: str|None = None