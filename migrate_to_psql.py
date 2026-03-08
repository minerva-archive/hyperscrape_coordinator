from tqdm import tqdm
import asyncio
import state
import sqlite3

from state_db import StateDBConnection

async def main():
    print("Initialising local state...")
    await state.initialise()
    print("Initialised state!")

    print("Connecting to sqlite")
    connection = sqlite3.connect("./state.db")
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT() FROM file")
    file_count = cursor.fetchone()[0]

    print("Adding files...")
    pbar = tqdm(total=file_count, unit="files")
    cursor.execute("SELECT id, path, size, url, chunk_size, complete FROM file")
    connection: StateDBConnection
    async with state.db.get_connection() as connection:
        while (row := cursor.fetchone()) != None:
            await connection.insert_file(
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                row[5] == 1
            )
            pbar.update(1)

asyncio.run(main())