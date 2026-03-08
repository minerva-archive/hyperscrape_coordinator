###
# Global state vars
###
import traceback
from uuid import uuid4
from tqdm import tqdm
from files import HyperscrapeChunk, HyperscrapeFile, WorkerStatus
from collections import defaultdict
from threading import Lock
from workers import Worker
from msgspec import json
from state_db import db
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

global workers
global files
global chunks
global file_chunk_count
global file_worker_count
global sorted_files
workers: dict[str, Worker] = {}
files: dict[str, HyperscrapeFile] = {}
chunks: dict[str, HyperscrapeChunk] = {}
file_worker_counts: dict[str, int] = {} # Count how many workers are using each file
sorted_downloadable_files: list[str] = [] # List of files to be downloaded sorted by how many workers are using it

global workers_lock
global files_lock
global chunks_lock
global hashes_lock
global stats_lock
workers_lock = Lock()
files_lock = Lock()
chunks_lock = Lock()
hashes_lock = Lock()
stats_lock = Lock()

###
# Handles storing stats and all that
###


global completed_files
global completed_chunks
global assigned_chunks
global failed_chunks
global downloaded_bytes
global completed_bytes
global total_bytes
global current_speed
completed_files = 0
completed_chunks = 0
assigned_chunks = 0
failed_chunks = 0
downloaded_bytes = 0
completed_bytes = 0
total_bytes = 0
current_speed = 0


def add_assigned_chunks(change: int):
    global assigned_chunks
    with stats_lock:
        assigned_chunks = max(0, assigned_chunks + change)
        return assigned_chunks


def add_completed_chunks(change: int):
    global completed_chunks
    with stats_lock:
        completed_chunks = max(0, completed_chunks + change)
        return completed_chunks


def add_failed_chunks(change: int):
    global failed_chunks
    with stats_lock:
        failed_chunks = max(0, failed_chunks + change)
        return failed_chunks


def add_downloaded_bytes(change: int):
    global downloaded_bytes
    with stats_lock:
        downloaded_bytes = max(0, downloaded_bytes + change)
        return downloaded_bytes


def add_completed_bytes(change: int):
    global completed_bytes
    with stats_lock:
        completed_bytes = max(0, completed_bytes + change)
        return completed_bytes


def add_completed_files(change: int):
    global completed_files
    with stats_lock:
        completed_files = max(0, completed_files + change)
        return completed_files


def get_downloaded_bytes():
    with stats_lock:
        return downloaded_bytes


def get_assigned_chunks():
    with stats_lock:
        return assigned_chunks


def set_current_speed(speed: float):
    global current_speed
    with stats_lock:
        current_speed = speed


def get_stats_snapshot():
    with stats_lock:
        return {
            "completed_files": completed_files,
            "completed_chunks": completed_chunks,
            "assigned": assigned_chunks,
            "failed": failed_chunks,
            "downloaded_bytes": downloaded_bytes,
            "completed_bytes": completed_bytes,
            "total_bytes": total_bytes,
            "current_speed": current_speed,
        }

class LeaderboardObject():
    def __init__(self,
                 discord_id: str,
                 discord_username: str,
                 avatar_url: str,
                 downloaded_chunks: int = 0,
                 downloaded_bytes: int = 0):
        
        self._discord_id = discord_id
        self._discord_username = discord_username
        self._avatar_url = avatar_url
        self._downloaded_chunks = downloaded_chunks
        self._downloaded_bytes = downloaded_bytes

    def get_discord_id(self):
        return self._discord_id
    
    def get_discord_username(self):
        return self._discord_username
    
    def get_avatar_url(self):
        return self._avatar_url
    
    def get_downloaded_chunks(self):
        return self._downloaded_chunks
    
    def get_downloaded_bytes(self):
        return self._downloaded_bytes
    
    def update_downloaded_bytes(self, change: int):
        self._downloaded_bytes += change
        db.update_leaderboard_downloaded_bytes(self._discord_id, change)
    
    def update_downloaded_chunks(self, change: int):
        self._downloaded_chunks += change
        db.update_leaderboard_downloaded_chunks(self._discord_id, change)


global current_leaderboard
global current_leaderboard_order
global current_leaderboard_lock
current_leaderboard_order: list[str] = []
current_leaderboard: dict[str, LeaderboardObject] = {} # Maps discord IDs to a leaderboard objects
current_leaderboard_lock = Lock()
###
###
###


def update_stats_bytes(discord_id: str, byte_count: int):
    current_leaderboard[discord_id].update_downloaded_bytes(byte_count)


def update_stats_chunks(discord_id: str, chunk_count: int):
    current_leaderboard[discord_id].update_downloaded_chunks(chunk_count)
   

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
# State helpers
###
# Files
def add_file(file: HyperscrapeFile) -> None:
    """!
    @brief Adds a file to the current state storing files

    @param file (HyperscrapeFile): The file to add
    """

    with files_lock:
        files[file.get_id()] = file
        file_worker_counts[file.get_id()] = 0
        sorted_downloadable_files.append(file.get_id())
        db.insert_file(file.get_id(), file.get_path(), file.get_total_size(), file.get_url(), file.get_chunk_size())


# Workers
async def remove_worker(worker_id: str) -> None:
    """!
    @brief Remove and disconnect a worker from the coordinator

    @param worker_id (str): The ID of the worker to remove
    """
    global assigned_chunks
    with workers_lock:
        worker = workers.get(worker_id)
    if (not worker):
        return

    with worker.get_lock():
        try:
            if (worker.get_websocket()):
                await worker.get_websocket().close()
        except:
            pass

        for chunk_id in list(worker.get_file_handles().keys()):
            worker.close_file_handle(chunk_id)
            file_path = worker.get_file_paths().get(chunk_id)
            if (file_path and os.path.exists(file_path)):
                os.remove(file_path)
            worker.remove_file_path(chunk_id)
            worker.remove_chunk_hash(chunk_id)
            if (chunk_id in chunks):
                chunks[chunk_id].remove_worker_status(worker.get_id())
                add_assigned_chunks(-1)

    with workers_lock:
        workers.pop(worker_id, None)


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
def cleanup_chunk_workers(chunk_id: str) -> None:
    """!
    @brief Given a chunk, removes stale workers from it and deletes their data

    @param chunk_id (str): The chunk to remove stale workers from
    """
    chunk = chunks[chunk_id]
    with chunk.get_lock():
        for worker_id in list(chunk.get_workers()):
            if (
                not chunk.get_worker_complete(worker_id) and # Only remove incomplete chunks
                (
                    (not worker_id in workers) or # If the worker is no longer present
                    time.time() - chunk.get_worker_last_updated(worker_id) > config["general"]["worker_timeout"] # Or it timed out
                )
            ):
                # Cleanup worker info
                if (worker_id in workers):
                    with workers[worker_id].get_lock():
                        if (not chunk.get_worker_hash_only(worker_id)):
                            if (chunk.get_id() in workers[worker_id].get_file_handles()):
                                workers[worker_id].close_file_handle(chunk.get_id())
                            workers[worker_id].remove_file_path(chunk.get_id())
                        workers[worker_id].remove_chunk_hash(chunk.get_id())
                # Remove status
                chunk.remove_worker_status(worker_id)

def load_state_from_db():
    """!
    @brief This loads the global state from the database
    """

    global files
    global chunks
    global current_leaderboard

    files = {}
    chunks = {}
    current_leaderboard = {}

    # build file chunks as we build chunks list
    file_chunks: dict[str, set[str]] = defaultdict(set)

    current_time = time.time()
    # rebuild chunks from db
    db_chunks = db.get_chunks()
    for db_chunk in db_chunks:
        db_worker_statuses = db.get_chunk_worker_status(db_chunk["id"])
        worker_status: dict[str, WorkerStatus] = {}
        for db_worker_status in db_worker_statuses:
            if (not db_worker_status["hash"]):
                continue # No hash - no need for this status
            worker_status[db_worker_status["worker_id"]] = WorkerStatus(
                db_worker_status["uploaded"],
                db_worker_status["hash"],
                db_worker_status["hash_only"],
                current_time # Yeah so this doesn't actually matter all that much
            )
        chunk = HyperscrapeChunk(
            db_chunk["id"],
            db_chunk["start"],
            db_chunk["end"],
            worker_status,
        )
        file_chunks[db_chunk["file_id"]].add(chunk.get_id())
        chunks[db_chunk["id"]] = chunk

    # rebuild files from db
    db_files = db.get_files()
    for db_file in db_files:
        files[db_file["id"]] = HyperscrapeFile(
            db_file["id"],
            db_file["path"],
            db_file["size"],
            db_file["url"],
            db_file["chunk_size"],
            file_chunks.get(db_file["id"], set()),
            bool(db_file["complete"]),
        )

    # rebuild current_leaderboard from db
    db_leaderboard = db.get_leaderboard()
    for db_entry in db_leaderboard:
        current_leaderboard[db_entry["discord_id"]] = LeaderboardObject(
            db_entry["discord_id"],
            db_entry["discord_username"],
            db_entry["avatar_url"],
            db_entry["downloaded_chunks"],
            db_entry["downloaded_bytes"],
        )

def load_state():
    """!
    @brief This loads the state from the db and sets up the stats objects
    """
    global files_lock
    global files
    global chunks_lock
    global chunks
    global current_leaderboard_lock
    global current_leaderboard
    global total_bytes
    global completed_files
    global completed_bytes
    global completed_chunks
    global sorted_downloadable_files
    global file_worker_counts
    global downloaded_bytes
    global assigned_chunks
    print("Loading current state...")
    generated_chunks = 0
    try:
        load_state_from_db()
        print("Generating files to download...")
        for file_id in tqdm(files):
            file = files[file_id]
            total_bytes += file.get_total_size() * config["general"]["trust_count"]

            # Regenerate chunks as needed
            if (len(file.get_chunks()) == 0 and file.get_total_size() != 0):
                print(f"Generating chunks for {file_id}...")
                current_size = 0
                with chunks_lock:
                    while current_size < file.get_total_size():
                        start = current_size
                        end = min(current_size + file.get_chunk_size(), file.get_total_size())
                        chunk_id = str(uuid4())
                        chunks[chunk_id] = HyperscrapeChunk(chunk_id, start, end)
                        file.add_chunk(chunk_id)
                        db.insert_chunk(chunk_id, file_id, start, end)
                        current_size = end
                        generated_chunks += 1

            if (file.get_complete()):
                add_completed_files(1)
                add_completed_bytes(file.get_total_size() * config["general"]["trust_count"])
                add_completed_chunks(math.ceil(file.get_total_size() / files[file_id].get_chunk_size()))
            else:
                file_worker_counts[file_id] = 0
                sorted_downloadable_files.append(file_id)

                for chunk_id in file.get_chunks():
                    for worker_id in chunks[chunk_id].get_workers():
                        file_worker_counts[file_id] += 1
                        if (chunks[chunk_id].get_worker_complete(worker_id)):
                            add_completed_chunks(1)
                            add_downloaded_bytes(chunks[chunk_id].get_end() - chunks[chunk_id].get_start())
                        else:
                            add_assigned_chunks(1)
        if (generated_chunks > 0):
            print(f"NOTE: Generated {generated_chunks} new chunks!")
        print(f"Server has {len(files)} files - of which {len(sorted_downloadable_files)} will be downloaded")
    except Exception as e:
        print("NOTE: Could not load previous file state:")
        print(e)
        traceback.print_exc()
load_state()
