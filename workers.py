from threading import Lock
from io import FileIO
import time

from fastapi import WebSocket

class Worker():
    """!
    @brief Represents a client and its connection to the coordinator
    """
    
    def __init__(self, worker_id: str, ip: str, max_concurrent: int, discord_id: str|None):
        self._file_handles: dict[str, FileIO] = {} # File handles for each chunk this worker is uploading
        self._file_paths: dict[str, str] = {} # File paths for each chunk this worker is uploading
        self._chunk_hashes: dict[str, object] = {}
        self._websocket = None
        self._worker_id = worker_id
        self._ip = ip
        self._max_concurrent = max_concurrent
        self._joined = time.time()
        self._discord_id = discord_id
        self._lock = Lock()

    def get_websocket(self) -> WebSocket:
        return self._websocket
    
    def set_websocket(self, websocket: WebSocket):
        self._websocket = websocket

    def get_chunk_hashes(self):
        return self._chunk_hashes
    
    def get_chunk_hash(self, chunk_id: str):
        return self._chunk_hashes[chunk_id]
    
    def set_chunk_hash(self, chunk_id: str, hash: object):
        self._chunk_hashes[chunk_id] = hash
    
    def remove_chunk_hash(self, chunk_id: str):
        return self._chunk_hashes[chunk_id]

    def get_file_paths(self):
        return self._file_paths
    
    def get_file_path(self, chunk_id: str):
        return self._file_paths[chunk_id]
    
    def set_file_path(self, chunk_id: str, file_path: str):
        self._file_paths[chunk_id] = file_path

    def remove_file_path(self, chunk_id: str):
        if (chunk_id in self._file_paths):
            del self._file_paths[chunk_id]
    
    def check_file_path(self, chunk_id: str):
        return self._file_paths[chunk_id]

    def get_file_handles(self):
        return self._file_handles

    def get_file_handle(self, chunk_id: str):
        return self._file_handles[chunk_id]
    
    def close_file_handle(self, chunk_id: str):
        try:
            self._file_handles[chunk_id].close()
        except:
            pass
        del self._file_handles[chunk_id]
    
    def set_file_handle(self, chunk_id: str, file_handle: FileIO):
        if (chunk_id in self._file_handles):
            self.close_file_handle(chunk_id)
        self._file_handles[chunk_id] = file_handle

    def check_file_handle(self, chunk_id: str):
        return chunk_id in self._file_handles

    def get_id(self):
        return self._worker_id
    
    def get_discord_id(self):
        return self._discord_id
    
    def get_ip(self):
        return self._ip
    
    def get_max_concurrent(self):
        return self._max_concurrent
    
    def get_joined(self):
        return self._joined
    
    def get_lock(self):
        return self._lock