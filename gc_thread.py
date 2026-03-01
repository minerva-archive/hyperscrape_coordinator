import state
import time

def gc():
    last_save = time.time()
    while True:
        current = time.time()
        for worker_id in state.workers:
            if (current - state.workers[worker_id].last_seen > state.config["general"]["worker_timeout"]):
                del state.workers[worker_id]
        if (current - last_save > 120): # Save every two minutes
            state.save_data_files()
            last_save = current
        time.sleep(state.config["general"]["worker_timeout"])