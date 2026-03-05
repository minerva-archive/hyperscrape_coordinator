import time

import state


def gc():
    last_save = time.time()
    while True:
        current = time.time()
        if current - last_save > 600:  # Save every 10 minutes
            state.save_data_files()
            last_save = current
        time.sleep(state.config["general"]["worker_timeout"])
