###
# Global state vars
###
import asyncio
from threading import Lock
from workers import Worker
from msgspec import json
from state_db import StateDB, StateDBConnection
import tomllib
import math
import time
import os

USER_AGENT = f"HyperscrapeServer/v1 (Created by Hackerdude for Minerva)"

global banned_ips
banned_ips = []
try:
    with open("./banned_ips.json", 'rb') as file:
        banned_ips = json.decode(file.read())
except:
    pass

global shutting_down
shutting_down = False

###
# Handles workers
###
global local_workers
global workers_lock
local_workers: dict[str, Worker] = {}
workers_lock: Lock = Lock()

###
# Cached stats
###
global total_file_count
global total_chunk_count
global completed_file_count
global completed_chunk_count
global assigned_chunk_count
global uploaded_bytes
global completed_bytes
global total_bytes
global current_speed
global total_workers
total_file_count = 0
total_chunk_count = 0
completed_file_count = 0
completed_chunk_count = 0
assigned_chunk_count = 0
uploaded_bytes = 0
completed_bytes = 0
total_bytes = 0
current_speed = 0
total_workers = 0

###
# Config + Secrets loading
###
global config
config = None
with open("./config.toml", 'rb') as file:
    config = tomllib.load(file)
os.makedirs(config["paths"]["chunk_temp_path"], exist_ok=True)
os.makedirs(config["paths"]["storage_path"], exist_ok=True)

global secrets
secrets = None
with open("./secrets.toml", 'rb') as file:
    secrets = tomllib.load(file)

###
# Databases
###
global db
db: StateDB = StateDB(config["database"]["psql_connstring"])

# Workers
async def remove_worker(worker_id: str) -> None:
    """!
    @brief Remove and disconnect a worker from the coordinator

    @param worker_id (str): The ID of the worker to remove
    """
    with workers_lock:
        worker = local_workers.get(worker_id)
    if (not worker):
        return

    with worker.get_lock():
        try:
            if (worker.get_websocket()):
                await worker.get_websocket().close()
        except:
            pass

        # Cleanup handlers
        for chunk_id in list(worker.get_file_handles().keys()):
            worker.close_file_handle(chunk_id)
            file_path = worker.get_file_paths().get(chunk_id)
            if (file_path and os.path.exists(file_path)):
                os.remove(file_path)
            worker.remove_file_path(chunk_id)
            worker.remove_chunk_hash(chunk_id)

        # Cleanup databases
        connection: StateDBConnection
        async with db.get_connection() as connection:
            await connection.remove_worker(worker.get_id())

    with workers_lock:
        local_workers.pop(worker_id, None)
        


###
# IP banning
###
def write_banned_ips():
    with open("./banned_ips.json", 'wb') as file:
        file.write(json.encode(banned_ips))

def ban_ip(ip: str):
    global banned_ips
    if (not ip in banned_ips):
        banned_ips.append(ip)
        write_banned_ips()

def unban_ip(ip: str):
    global banned_ips
    if (ip in banned_ips):
        banned_ips.remove(ip)
        write_banned_ips()


###
# Chunks
###
async def cleanup_chunk_workers(chunk_id: str) -> None:
    """!
    @brief Given a chunk, removes stale workers from it and deletes their data

    @param chunk_id (str): The chunk to remove stale workers from
    """
    connection: StateDBConnection
    async with db.get_connection() as connection:
        worker_statuses = await connection.get_chunk_worker_status(chunk_id)
        for worker_status in worker_statuses:
            if (not worker_status.worker_id in local_workers):
                continue # Not one of our workers? Ignore it
            if (
                not worker_status.hash and # Only remove incomplete chunks
                (
                    time.time() - worker_status.last_updated > config["general"]["worker_timeout"] # Or it timed out
                )
            ):
                # Cleanup worker info
                with local_workers[worker_status.worker_id].get_lock():
                    if (not worker_status.hash_only):
                        if (chunk_id in local_workers[worker_status.worker_id].get_file_handles()):
                            local_workers[worker_status.worker_id].close_file_handle(chunk_id)
                        local_workers[worker_status.worker_id].remove_file_path(chunk_id)
                    local_workers[worker_status.worker_id].remove_chunk_hash(chunk_id)
                # Remove status info
                connection.delete_worker_status(chunk_id, worker_status.worker_id)

async def initialise():
    await db.open()