import time
from threading import Lock


class Worker:
    def __init__(self, worker_id: str, ip: str, auth_nonce: str, max_concurrent: int):
        self._worker_id = worker_id
        self._ip = ip
        self._auth_nonce = auth_nonce
        self._max_concurrent = max_concurrent
        self._joined = time.time()
        self._lock = Lock()

    def get_id(self):
        return self._worker_id

    def get_ip(self):
        return self._ip

    def get_auth_nonce(self):
        return self._auth_nonce

    def get_max_concurrent(self):
        return self._max_concurrent

    def get_joined(self):
        return self._joined

    def get_lock(self):
        return self._lock
