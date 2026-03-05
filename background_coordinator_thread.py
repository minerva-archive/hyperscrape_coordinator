import time

import state


def background_coordinator():
    last_save_time = time.time()
    last_stat_calc_time = time.time()
    last_downloaded = state.downloaded_bytes
    while True:
        current = time.time()
        if current - last_stat_calc_time > 1:
            state.order_leaderboard()
            state.current_speed = (state.downloaded_bytes - last_downloaded) / (
                current - last_stat_calc_time
            )
            last_downloaded = state.downloaded_bytes
            last_stat_calc_time = current
        if current - last_save_time > 600:  # Save every 10 minutes
            state.save_data_files()
            last_save_time = current
        time.sleep(1)
