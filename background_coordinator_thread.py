import state
import time

from state_db import db


def background_coordinator():
    """!
    @brief Thread that runs in the background, handles current speed calculation
    """
    last_stat_calc_time = time.time()
    last_downloaded = state.get_downloaded_bytes()
    while True:
        current = time.time()

        # Calculate current upload speed
        if (current - last_stat_calc_time > 1):
            downloaded_now = state.get_downloaded_bytes()
            state.set_current_speed((downloaded_now - last_downloaded)/(current - last_stat_calc_time))
            last_downloaded = downloaded_now
            last_stat_calc_time = current

        # Sort the leaderboard
        with state.current_leaderboard_lock:
            state.current_leaderboard_order = sorted(
                state.current_leaderboard.keys(),
                key=lambda leaderboard_id: state.current_leaderboard[leaderboard_id].get_downloaded_bytes()
            )
        db.flush()
        time.sleep(0.1)
