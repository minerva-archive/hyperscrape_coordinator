###
# State vars
###
from collections import OrderedDict
import math
import os
from threading import Lock
import time
from uuid import uuid4
from auth_token import AuthToken
from files import HyperscrapeChunk, HyperscrapeFile, WorkerStatus
from workers import Worker
from msgspec import json
import pickle
import tomllib

USER_AGENT = f"HyperscrapeServer/v1 (Created by Hackerdude for Minerva)"

global banned_ips
banned_ips = []
try:
    with open("./banned_ips.json", 'rb') as file:
        banned_ips = json.decode(file.read())
except:
    pass

global workers
global files
global chunks
global file_chunk_count
global file_worker_count
global sorted_files
global file_hashes
workers: dict[str, Worker] = {}
files: dict[str, HyperscrapeFile] = {}
chunks: dict[str, HyperscrapeChunk] = {}
file_worker_counts: dict[str, int] = {} # Count how many workers are using each file
sorted_downloadable_files: list[str] = [] # List of files to be downloaded sorted by how many workers are using it
file_hashes: dict[str, dict[str, str]] = {}

global workers_lock
global files_lock
global chunks_lock
global hashes_lock
workers_lock = Lock()
files_lock = Lock()
chunks_lock = Lock()
hashes_lock = Lock()

###
# Handles storing stats and all that
###


global completed_files
global completed_chunks
global assigned_chunks
global failed_chunks
global downloaded_bytes
global total_bytes
global current_speed
completed_files = 0
completed_chunks = 0
assigned_chunks = 0
failed_chunks = 0
downloaded_bytes = 0
total_bytes = 0
current_speed = 0

class LeaderboardObject():
    def __init__(self, discord_id: str, discord_username: str, avatar_url: str, downloaded_chunks: int = 0, downloaded_bytes: int = 0):
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
    
    def update_downloaded_chunks(self, change: int):
        self._downloaded_chunks += change


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

def order_leaderboard():
    global current_leaderboard_order
    current_leaderboard_order = sorted(current_leaderboard, key=lambda key: current_leaderboard[key].get_downloaded_bytes(), reverse=True)

###
# State Files
def save_file_state():
    with files_lock:
        with open("./file_state.bin", 'wb') as file:
            pickle.dump(files, file, protocol=pickle.HIGHEST_PROTOCOL)

def save_chunk_state():
    with chunks_lock:
        with open("./chunk_state.bin", 'wb') as file:
            pickle.dump(chunks, file, protocol=pickle.HIGHEST_PROTOCOL)

def save_file_hashes():
    with hashes_lock:
        with open("./file_hashes.bin", 'wb') as file:
            pickle.dump(file_hashes, file, protocol=pickle.HIGHEST_PROTOCOL)

def save_leaderboard_state():
    with current_leaderboard_lock:
        with open("./leaderboard.bin", "wb") as file:
            pickle.dump(current_leaderboard, file, protocol=pickle.HIGHEST_PROTOCOL)

def save_data_files():
    save_chunk_state()
    save_file_state()
    save_file_hashes()
    save_leaderboard_state()
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
def add_file(file: HyperscrapeFile, defer_save: bool = False):
    with files_lock:
        files[file.get_id()] = file
        file_worker_counts[file.get_id()] = 0
        sorted_downloadable_files.append(file.get_id())
    if (not defer_save):
        save_data_files()

# File data structure helpers
def reorder_file_workers(file_id):
    i = 0
    while (sorted_downloadable_files[i] != file_id):
        i += 1 # Get to the current file
    sorted_downloadable_files.pop(i) # Remove it
    i = 0
    while (i < len(sorted_downloadable_files)) and file_worker_counts[file_id] < file_worker_counts[sorted_downloadable_files[i]]: # Keep going whilst the current file worker count is less (this gurantees a descending-order list)
        i += 1 # Find the new insertion point
    sorted_downloadable_files.insert(i, file_id)

# Workers
def remove_worker(worker_id: str):
    with workers_lock:
        with workers[worker_id].get_lock():
            del workers[worker_id] # Delete the worker    

# IP banning
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
def cleanup_chunk_workers(chunk_id: str):
    chunk = chunks[chunk_id]
    with chunk.get_lock():
        for worker_id in list(chunk.get_workers()):
            if (
                (not worker_id in workers) or
                ((not chunk.get_worker_status(worker_id).get_complete()) and time.time() - chunk.get_worker_status(worker_id).get_last_updated() > config["general"]["worker_timeout"])
            ):
                chunk.remove_worker_status(worker_id)

def load_files():
    global files_lock
    global files
    global file_hashes
    global chunks_lock
    global chunks
    global current_leaderboard_lock
    global current_leaderboard
    global total_bytes
    global completed_files
    global downloaded_bytes
    global completed_chunks
    global sorted_downloadable_files
    global file_worker_counts
    global assigned_chunks
    print("Loading current state...")
    try:
        with files_lock:
            with open("./file_state.bin", 'rb') as file:
                files = pickle.load(file)
            with open("./file_hashes.bin", 'rb') as file:
                file_hashes = pickle.load(file)
            with chunks_lock:
                with open("./chunk_state.bin", 'rb') as file:
                    chunks = pickle.load(file)
            print("Generating files to download...")
            for file_id in files:
                file = files[file_id]
                total_bytes += file.get_total_size() * config["general"]["trust_count"]
                if (file.get_complete()):
                    completed_files += 1
                    downloaded_bytes += file.get_total_size()
                    completed_chunks += math.ceil(file.get_total_size() / files[file_id].get_chunk_size())
                else:
                    file_worker_counts[file_id] = 0
                    sorted_downloadable_files.append(file_id)
                    for chunk_id in file.get_chunks():
                        for worker_id in chunks[chunk_id].get_workers():
                            file_worker_counts[file_id] += 1
                            if (chunks[chunk_id].get_worker_status(worker_id).get_complete()):
                                completed_chunks += 1
                                downloaded_bytes += chunks[chunk_id].get_end() - chunks[chunk_id].get_start()
                            else:
                                assigned_chunks += 1
                del file
        print(f"Server has {len(files)} files - of which {len(sorted_downloadable_files)} will be downloaded")
        with current_leaderboard_lock:
            with open("./leaderboard.bin", 'rb') as file:
                current_leaderboard = pickle.load(file)
    except Exception as e:
        print("NOTE: Could not load previous file state:")
        print(e)
        save_data_files()
load_files()