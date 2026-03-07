import state
import time


def background_coordinator():
    """!
    @brief Thread that runs in the background, handles current speed calculation
    """
    last_stat_calc_time = time.time()
    last_downloaded = state.downloaded_bytes
    while True:
        current = time.time()

        # Calculate current upload speed
        if (current - last_stat_calc_time > 1):
            state.current_speed = (state.downloaded_bytes - last_downloaded)/(current - last_stat_calc_time)
            last_downloaded = state.downloaded_bytes
            last_stat_calc_time = current

        # Sort the leaderboard
        with state.current_leaderboard_lock:
            state.current_leaderboard_order.sort(key=lambda leaderboard_id: state.current_leaderboard[leaderboard_id].get_downloaded_bytes())
        time.sleep(1)