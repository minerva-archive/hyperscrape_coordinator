from tqdm import tqdm
import asyncio
import state
import sqlite3

from state_db import StateDBConnection
BATCH_SIZE = 10000

async def main():
    print("Initialising local state...")
    await state.initialise()
    print("Initialised state!")

    print("Connecting to sqlite")
    sqlite_connection: sqlite3.Connection = sqlite3.connect("./state.db")
    sqlite3_cursor = sqlite_connection.cursor()

    print("Clearing tables...")
    async with state.db.get_connection() as connection:
        await connection._cursor.execute("TRUNCATE chunk, file, file_hash, leaderboard, stat, worker_info, worker_status CASCADE")
    print("Done.")

    print("Adding leaderboard items...")
    sqlite3_cursor.execute("SELECT COUNT() FROM leaderboard")
    chunk_count = sqlite3_cursor.fetchone()[0]
    pbar = tqdm(total=chunk_count, unit="chunks")
    sqlite3_cursor.execute("SELECT discord_id, discord_username, avatar_url, downloaded_chunks, downloaded_bytes FROM leaderboard")
    connection: StateDBConnection
    async with state.db.get_connection() as connection:
        while (row := sqlite3_cursor.fetchone()) != None:
            await connection.insert_leaderboard_entry(
                row[0],
                row[1],
                row[2],
                row[3],
                row[4]
            )
            pbar.update(1)
    pbar.close()

    print("Adding files...")
    sqlite3_cursor.execute("SELECT COUNT() FROM file")
    file_count = sqlite3_cursor.fetchone()[0]
    pbar = tqdm(total=file_count, unit="files")
    sqlite3_cursor.execute("SELECT id, path, size, url, chunk_size, complete FROM file")
    connection: StateDBConnection
    async with state.db.get_connection() as connection:
        while (row := sqlite3_cursor.fetchone()) != None:
            await connection.insert_file(
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                bool(row[5])
            )
            pbar.update(1)
    pbar.close()

    print("Adding file hashes...")
    sqlite3_cursor.execute("SELECT COUNT() FROM file_hash")
    chunk_count = sqlite3_cursor.fetchone()[0]
    pbar = tqdm(total=chunk_count, unit="chunks")
    sqlite3_cursor.execute("SELECT file_id, md5, sha1, sha256 FROM file_hash")
    connection: StateDBConnection
    async with state.db.get_connection() as connection:
        while (row := sqlite3_cursor.fetchone()) != None:
            await connection.insert_file_hash(
                row[0],
                row[1],
                row[2],
                row[3]
            )
            pbar.update(1)
    pbar.close()

    print("Adding chunks...")
    sqlite3_cursor.execute("SELECT COUNT() FROM chunk")
    chunk_count = sqlite3_cursor.fetchone()[0]
    pbar = tqdm(total=chunk_count, unit="chunks")
    sqlite3_cursor.execute("SELECT id, file_id, start, end FROM chunk")
    connection: StateDBConnection
    async with state.db.get_connection() as connection:
        while (row := sqlite3_cursor.fetchone()) != None:
            await connection.insert_chunk(
                row[0],
                row[1],
                row[2],
                row[3]
            )
            pbar.update(1)
    pbar.close()

asyncio.run(main())