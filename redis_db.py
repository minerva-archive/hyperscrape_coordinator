import time
import redis

from workers import Worker

class RedisDB:
    def __init__(self, redis_host: str, redis_port: int, redis_db: int):
        self.redis = redis.Redis(redis_host, redis_port, redis_db)

    def remove_worker(self, worker_id: str):
        self.remove_worker_status_last_updated(worker_id)
        self.redis.delete(f"w:{worker_id}:ip")

    def add_worker(self, worker: Worker):
        self.change_total_workers(1)
        self.redis.set(f"w:{worker.get_id()}:ip", worker.get_ip())

    def get_worker_ip(self, worker_id: str):
        return self.redis.get(f"w:{worker_id}:ip")
    
    def get_worker_status_last_updated(self, chunk_id: str, worker_id: str):
        return self.redis.get(f"ws:{chunk_id}:{worker_id}:lu")
    
    def set_worker_status_last_updated(self, chunk_id: str, worker_id: str):
        self.redis.set(f"ws:{chunk_id}:{worker_id}:lu", time.time())

    def remove_worker_status_last_updated(self, chunk_id: str, worker_id: str):
        self.redis.delete(f"ws:{chunk_id}:{worker_id}:lu")

    def get_total_workers(self):
        count: int|None = self.redis.get("total_worker_count")
        if (count == None):
            self.redis.set("total_worker_count", 0)
            return 0
        return count

    def change_total_workers(self, change: int):
        self.redis.incrby("total_worker_count", change)