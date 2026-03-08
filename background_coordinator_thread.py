import state
import time

from state_db import StateDBConnection


async def background_coordinator():
    """!
    @brief Thread that runs in the background, handles current speed calculation
    """
    last_stat_calc_time = time.time()
    last_downloaded = 0
    while True:
        current = time.time()

        connection: StateDBConnection
        with state.db.get_connection() as connection:
            # Calculate current upload speed
            if (current - last_stat_calc_time > 1):
                state.total_file_count = await connection.get_total_file_count()
                state.total_chunk_count = await connection.get_total_chunk_count()
                state.completed_file_count = await connection.get_completed_file_count()
                state.completed_chunk_count = await connection.get_completed_chunk_count()
                state.assigned_chunk_count = await connection.get_assigned_chunk_count()
                state.uploaded_bytes = await connection.get_uploaded_bytes()
                state.completed_bytes = await connection.get_completed_bytes()
                state.total_bytes = await connection.get_total_bytes()
                state.current_speed = ((await connection.get_stat("downloaded_bytes")) - last_downloaded)/(current - last_stat_calc_time)
                last_downloaded = connection.get_stat("downloaded_bytes")
                last_stat_calc_time = current

        time.sleep(0.1)