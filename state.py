###
# State vars
###
import os
import pickle
import time
from threading import Lock
from uuid import uuid4

import tomllib
from auth_token import AuthToken
from files import HyperscrapeChunk, HyperscrapeFile, WorkerStatus
from msgspec import json
from workers import Worker

USER_AGENT = f"HyperscrapeServer/v1 (Created by Hackerdude for Minerva)"

global banned_ips
banned_ips = []
try:
    with open("./banned_ips.json", "rb") as file:
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
file_worker_counts: dict[str, int] = {}  # Count how many workers are using each file
sorted_downloadable_files: list[
    str
] = []  # List of files to be downloaded sorted by how many workers are using it
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
# State Files
def save_file_state():
    with open("./file_state.bin", "wb") as file:
        with files_lock:
            pickle.dump(files, file, protocol=pickle.HIGHEST_PROTOCOL)


def save_chunk_state():
    with open("./chunk_state.bin", "wb") as file:
        with chunks_lock:
            pickle.dump(chunks, file, protocol=pickle.HIGHEST_PROTOCOL)


def save_file_hashes():
    with open("./file_hashes.bin", "wb") as file:
        with hashes_lock:
            pickle.dump(file_hashes, file, protocol=pickle.HIGHEST_PROTOCOL)


def save_data_files():
    save_chunk_state()
    save_file_state()
    save_file_hashes()


###

global config
config = None
with open("./config.toml", "rb") as file:
    config = tomllib.load(file)
os.makedirs(config["paths"]["chunk_temp_path"], exist_ok=True)
os.makedirs(config["paths"]["storage_path"], exist_ok=True)


###
# State helpers
###
# Files
def add_file(file: HyperscrapeFile, defer_save: bool = False):
    with files_lock:
        files[file.get_id()] = file
        file_worker_counts[file.get_id()] = 0
        sorted_downloadable_files.append(file.get_id())
    if not defer_save:
        save_data_files()


# File data structure helpers
def reorder_file_workers(file_id):
    i = 0
    while sorted_downloadable_files[i] != file_id:
        i += 1  # Get to the current file
    sorted_downloadable_files.pop(i)  # Remove it
    i = 0
    while (
        (i < len(sorted_downloadable_files))
        and file_worker_counts[file_id]
        < file_worker_counts[sorted_downloadable_files[i]]
    ):  # Keep going whilst the current file worker count is less (this gurantees a descending-order list)
        i += 1  # Find the new insertion point
    sorted_downloadable_files.insert(i, file_id)


# Workers
def remove_worker(worker_id: str):
    with workers_lock:
        with workers[worker_id].get_lock():
            del workers[worker_id]  # Delete the worker


def add_worker(ip: str, max_concurrent: int):
    with workers_lock:
        global workers
        worker_id = str(uuid4())
        auth_token = AuthToken(worker_id)
        workers[worker_id] = Worker(worker_id, ip, auth_token.nonce, max_concurrent)
    return auth_token


# IP banning
def write_banned_ips():
    with open("./banned_ips.json", "wb") as file:
        file.write(json.encode(banned_ips))


def ban_ip(ip: str):
    global banned_ips
    if not ip in banned_ips:
        banned_ips.append(ip)
        write_banned_ips()


def unban_ip(ip: str):
    global banned_ips
    if ip in banned_ips:
        banned_ips.remove(ip)
        write_banned_ips()


###
# Chunks
def cleanup_chunk_workers(chunk_id: str):
    chunk = chunks[chunk_id]
    with chunk.get_lock():
        for worker_id in list(chunk.get_workers()):
            if (not worker_id in workers) or (
                (not chunk.get_worker_status(worker_id).get_complete())
                and time.time() - chunk.get_worker_status(worker_id).get_last_updated()
                > config["general"]["worker_timeout"]
            ):
                chunk.remove_worker_status(worker_id)


print("Loading current state...")
try:
    with open("./file_state.bin", "rb") as file:
        files = pickle.load(file)
    with open("./chunk_state.bin", "rb") as file:
        chunks = pickle.load(file)
    with open("./file_hashes.bin", "rb") as file:
        file_hashes = pickle.load(file)
    print("Generating files to download...")
    for file_id in files:
        if not files[file_id].get_complete():
            sorted_downloadable_files.append(file_id)
            file_worker_counts[file_id] = 0
    print(
        f"Server has {len(files)} files - of which {len(sorted_downloadable_files)} will be downloaded"
    )
except Exception as e:
    print("NOTE: Could not load previous file state:")
    print(e)
    save_data_files()
