###
# State vars
###
from uuid import uuid4
from files import HyperscrapeChunk, HyperscrapeFile, WorkerStatus
from receivers import Receiver
from workers import Worker
from msgspec import json
import pickle
import tomllib

global banned_ips
banned_ips = []
try:
    with open("./banned_ips.json", 'rb') as file:
        banned_ips = json.decode(file.read())
except:
    pass


###
# State Files
def save_file_state():
    with open("./file_state.bin", 'wb') as file:
        pickle.dump(files, file)

def save_chunk_state():
    with open("./chunk_state.bin", 'wb') as file:
        pickle.dump(chunks, file)
    
def save_file_mapping():
    with open("./chunk_file_mapping.bin", 'wb') as file:
        pickle.dump(chunk_to_file, file)

def save_files():
    save_chunk_state()
    save_file_mapping()
    save_file_state()
###

###
class AssignedChunks():
    def __init__(self):
        self._assigned_chunks: dict[str, list[str]] = {} # Each worker has multiple chunks _assigned_chunks[worker_id] = [chunk_id, chunk_id, chunk_id]
    
    def assign_chunk(self, worker_id, chunk_id):
        if (not worker_id in self._assigned_chunks):
            self._assigned_chunks[worker_id] = []
        self._assigned_chunks[worker_id].append(chunk_id)
        chunks[chunk_id].worker_status[worker_id] = WorkerStatus(0, 0)

    def remove_worker(self, worker_id):
        if (worker_id in self._assigned_chunks):
            for chunk_id in self._assigned_chunks[worker_id]:
                chunk = chunks[chunk_id]
                del chunk.worker_status[worker_id]
                # Fix file ordering
                file_id = chunk_to_file[chunk_id]
                file_worker_counts[file_id] -= 1
                reorder_file_workers(file_id)
            del self._assigned_chunks[worker_id]
###

global workers
global receivers
global files
global chunks
global chunk_to_file
global assigned_chunks
global file_chunk_count
global file_worker_count
global sorted_files
workers: dict[str, Worker] = {}
receivers: dict[str, Receiver] = {}
files: dict[str, HyperscrapeFile] = {}
chunks: dict[str, HyperscrapeChunk] = {}
chunk_to_file: dict[str, str] = {} # We store this for efficient chunk to file lookups (duh)
assigned_chunks: AssignedChunks = AssignedChunks()
file_worker_counts: dict[str, int] = {} # Count how many workers are using each file
sorted_downloadable_files: list[str] = [] # List of files to be downloaded sorted by how many workers are using it

try:
    with open("./file_state.bin", 'rb') as file:
        files = pickle.load(file)
    with open("./chunk_state.bin", 'rb') as file:
        chunks = pickle.load(file)
    with open("./chunk_file_mapping.bin", 'rb') as file:
        chunk_to_file = pickle.load(file)
    print("Generating files to download...")
    for file_id in files:
        file = files[file_id]
        if (not file.complete):
            sorted_downloadable_files.append(file_id)
            file_worker_counts[file_id] = 0
    print(f"Server has {len(files)} files - of which {len(sorted_downloadable_files)} will be downloaded")
except Exception as e:
    print("NOTE: Could not load previous file state:")
    print(e)
    save_files()

global config
config = None
with open("./config.toml", 'rb') as file:
    config = tomllib.load(file)

###
# State helpers
###
# Files
def add_file(file: HyperscrapeFile):
    global files
    global chunk_to_file
    files[file.file_id] = file
    current_size = 0
    while current_size < file.total_size:
        start = current_size
        end = min(current_size + file.chunk_size, file.total_size)
        chunk_id = str(uuid4())
        chunks[chunk_id] = HyperscrapeChunk(start, end)
        chunk_to_file[chunk_id] = file.file_id
        file.chunks.append(chunk_id)
        current_size = end
    file_worker_counts[file.file_id] = 0
    sorted_downloadable_files.append(file.file_id)
    save_files()

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
    # Remove status data
    assigned_chunks.remove_worker(worker_id)
    del workers[worker_id] # Delete the worker

def add_worker(ip: str, max_upload: int, max_download: int, max_per_file_speed: int, threads: int):
    global workers
    worker_id = str(uuid4())
    workers[worker_id] = Worker(worker_id, ip, max_upload, max_download, max_per_file_speed, threads)
    return worker_id

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