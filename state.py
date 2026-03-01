###
# State vars
###
import os
import time
from uuid import uuid4
from auth_token import AuthToken
from console import Console
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

global console
console = Console()

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

###
# State Files
def save_file_state():
    with open("./file_state.bin", 'wb') as file:
        pickle.dump(files, file, protocol=pickle.HIGHEST_PROTOCOL)

def save_chunk_state():
    with open("./chunk_state.bin", 'wb') as file:
        pickle.dump(chunks, file, protocol=pickle.HIGHEST_PROTOCOL)

def save_file_hashes():
    with open("./file_hashes.bin", 'wb') as file:
        pickle.dump(file_hashes, file, protocol=pickle.HIGHEST_PROTOCOL)

def save_data_files():
    save_chunk_state()
    save_file_state()
    save_file_hashes()
###

global config
config = None
with open("./config.toml", 'rb') as file:
    config = tomllib.load(file)
os.makedirs(config["paths"]["chunk_temp_path"], exist_ok=True)
os.makedirs(config["paths"]["storage_path"], exist_ok=True)

###
# State helpers
###
# Files
def add_file(file: HyperscrapeFile, defer_save: bool = False):
    files[file.file_id] = file
    file_worker_counts[file.file_id] = 0
    sorted_downloadable_files.append(file.file_id)
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
    del workers[worker_id] # Delete the worker

def add_worker(ip: str, max_upload: int, max_download: int, max_per_file_speed: int, threads: int):
    global workers
    worker_id = str(uuid4())
    auth_token = AuthToken(worker_id)
    workers[worker_id] = Worker(worker_id, ip, auth_token.nonce, max_upload, max_download, max_per_file_speed, threads)
    return auth_token

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
def cleanup_chunk_workers(chunk_id):
    chunk = chunks[chunk_id]
    for worker_id in list(chunk.worker_status.keys()):
        if (
            (not worker_id in workers) or
            ((not chunk.worker_status[worker_id].complete) and time.time() - chunk.worker_status[worker_id].last_updated > config["general"]["worker_timeout"])
        ):
            del chunk.worker_status[worker_id]


print("Loading current state...")
try:
    with open("./file_state.bin", 'rb') as file:
        files = pickle.load(file)
    with open("./chunk_state.bin", 'rb') as file:
        chunks = pickle.load(file)
    with open("./file_hashes.bin", 'rb') as file:
        file_hashes = pickle.load(file)
    print("Generating files to download...")
    for file_id in files:
        file = files[file_id]
        if (not files[file_id].complete):
            sorted_downloadable_files.append(file_id)
            file_worker_counts[file_id] = 0
    print(f"Server has {len(files)} files - of which {len(sorted_downloadable_files)} will be downloaded")
except Exception as e:
    print("NOTE: Could not load previous file state:")
    print(e)
    save_data_files()