import asyncio
import state
import time

from state_db import StateDBConnection

async def refresh_materialized_view():
    connection: StateDBConnection
    async with state.db.get_connection() as connection:
        await connection.refresh_ordered_chunks()

def central_thread():
    while not state.shutting_down:
        current = time.time()
        asyncio.run(refresh_materialized_view())
        time.sleep(max(60, time.time() - current))