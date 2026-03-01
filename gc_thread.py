import state
import time

def gc():
    current = time.time()
    for worker_id in state.workers:
        if (current - state.workers[worker_id].last_seen > state.config["general"]["worker_timeout"]):
            del state.workers[worker_id]