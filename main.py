import os
from threading import Thread
from uuid import uuid4
from files import HyperscrapeChunk, HyperscrapeFile, WorkerStatus
from gc_thread import gc
import state
from flask import Flask, request
import hashlib
import shutil

from helpers import get_request_ip, get_url_size, get_worker

import argparse

parser = argparse.ArgumentParser(
                    prog='Hyperscrape Coordinator',
                    description='',
                    epilog='Created by Hackerdude for Minerva')
parser.add_argument("-d", "--debug", help="Use Waitress", action="store_true")
args = parser.parse_args()

print("=========================")
print("=  HYPERSCRAPE SERVER   =")
print("= Created By Hackerdude =")
print("=========================")

app = Flask(__name__)

@app.route("/")
def root():
    return 'Hyperscrape Coordinator - Created by <a href="https://hackerdude.tech">Hackerdude</a>'

@app.route("/workers", methods=['POST'])
def register_worker():
    ip = get_request_ip()
    if (ip in state.banned_ips):
        return {"error": "Could not connect to worker"}, 403
    #state.workers_lock.acquire()
    #for worker_id in list(state.workers.keys()):
    #    if (state.workers[worker_id].ip == ip):
    #        state.remove_worker(worker_id)
    #state.workers_lock.release()
    data = request.json
    if (data == None or
        (not "version" in data) or
        (not "max_concurrent" in data)):
        return {"error": "Invalid Request"}, 400
    if (data["version"] > state.config['general']['version']):
        return {"error": f"Version mismatch, expected {state.config['general']['version']}"}, 400
    auth_token = state.add_worker(ip, data["max_concurrent"])
    return {
        "worker_id": auth_token.id,
        "auth_token": auth_token.as_token()
    }

@app.route("/chunks", methods=['GET'])
def get_chunks():
    worker = get_worker()
    if (not worker):
        return {"error": "Invalid token!"}, 403
    
    num_chunks_to_get = int(request.args.get("n", 250))
    # Get files with high worker counts
    # So the entire network is working together for a single file essenially
    chunks_to_download = []
    chunk_to_file = {}
    for file_id in state.sorted_downloadable_files:
        if (len(chunks_to_download) == num_chunks_to_get):
            break
        downloading_file_already = False

        # If the file doesn't have a total size then we need to do that!
        file = state.files[file_id]
        if (file.get_total_size() == None):
            file.set_total_size(get_url_size(file.get_url()))

        # If the file hasn't generated chunks then we need to do that!
        if (len(file.get_chunks()) == 0):
            current_size = 0
            state.chunks_lock.acquire()
            while current_size < file.get_total_size():
                start = current_size
                end = min(current_size + file.get_chunk_size(), file.get_total_size())
                chunk_id = str(uuid4())
                state.chunks[chunk_id] = HyperscrapeChunk(chunk_id, start, end)
                file.add_chunk(chunk_id)
                current_size = end
            state.chunks_lock.release()
        
        # Ensure the worker isn't currently downloading this file
        for chunk_id in file.get_chunks():
            # Cleanup workers that have not uploaded in a while
            state.cleanup_chunk_workers(chunk_id)
            if (state.chunks[chunk_id].has_worker(worker.get_id()) and not state.chunks[chunk_id].get_worker_status(worker.get_id()).complete):
                downloading_file_already = True # If worker is CURRENTLY downloading this FILE then we skip the entire file
                break
        
        if (downloading_file_already):
            continue

        highest_chunk_id = None
        for chunk_id in file.get_chunks():
            # Get the chunk in this file with the highest number of downloaders under trust_count
            if (state.chunks[chunk_id].get_worker_count() >= state.config["general"]["trust_count"]):
                continue
            if (state.chunks[chunk_id].has_worker(worker.get_id())):
                continue # If worker has already downloaded THIS chunk then we skip it from candidates
            if (highest_chunk_id == None or state.chunks[chunk_id].get_worker_count() > len(state.chunks[highest_chunk_id].get_worker_count())):
                highest_chunk_id = chunk_id
        if (highest_chunk_id != None):
            chunk_to_file[highest_chunk_id] = file_id
            chunks_to_download.append(highest_chunk_id)

    ###
    # We now have a list of chunks to download
    ###
    response = {}
    for chunk_id in chunks_to_download:
        chunk = state.chunks[chunk_id]
        file = state.files[chunk_to_file[chunk_id]]
        state.chunks[chunk_id].add_worker_status(worker.get_id())
        response[chunk_id] = {
            "file_id": chunk_to_file[chunk_id],
            "url": file.get_url(),
            "range": [
                chunk.get_start(),
                chunk.get_end()
            ]
        }
    return response

@app.route("/status", methods=['PUT'])
def put_status():
    worker = get_worker()
    if (not worker):
        return {"error": "Invalid token!"}, 403
    data = request.json
    for chunk_id in data:
        if (not chunk_id in state.chunks):
            continue
        if (not state.chunks[chunk_id].has_worker(worker.get_id())):
            continue
        chunk = state.chunks[chunk_id]
        if (data[chunk_id] == None):
            chunk.remove_worker_status(worker.get_id())
            continue
        chunk.update_worker_status_downloaded(worker.get_id(), data[chunk_id]["downloaded"])
        worker.update_last_seen()
    return {"ok": "ok"}, 200

@app.route("/ping", methods=['GET'])
def still_alive():
    worker = get_worker()
    if (not worker):
        return {"error": "Invalid token!"}, 403
    worker.update_last_seen()


@app.route("/upload", methods=["PUT"])
def upload_file():
    worker = get_worker()
    if (not worker):
        return {"error": "Invalid token!"}, 403
    chunk_id = request.args.get("chunk_id", None)
    file_id = request.args.get("file_id", None) # @TODO

    if (chunk_id == None or file_id == None):
        return {"error": "Invalid request"}, 400

    # Ensure the chunk exists
    if (not chunk_id in state.chunks):
        return {"error": "Unknown chunk"}, 400
    if (not state.chunks[chunk_id].has_worker(worker.get_id())):
        return {"error": "Chunk not requested"}, 400
    if (state.chunks[chunk_id].get_worker_status(worker.get_id()).complete):
        return {"error": "Chunk already complete"}, 400

    chunk = state.chunks[chunk_id]
    # Handle chunk uploading
    chunk_file_object = state.files[file_id]
    if (not chunk_file_object.has_chunk(chunk_id)):
        return {"error": "Unknown file"}, 400
    temp_storage_folder = os.path.join(state.config["paths"]["chunk_temp_path"], chunk_file_object.get_path())
    os.makedirs(temp_storage_folder, exist_ok=True)
    storage_path = os.path.join(temp_storage_folder, f"chunk_{chunk.get_id()}_{worker.get_id()}.bin")
    with open(storage_path + ".partial", "wb") as file:
        chunk_hash = hashlib.md5()
        chunk.update_worker_status_uploaded(worker.get_id(), 0)
        try:
            stream_data = request.stream.read()
            while (len(stream_data) > 0):
                chunk.update_worker_status_uploaded(worker.get_id(), chunk.get_worker_status().uploaded + len(stream_data))
                worker.update_last_seen()
                file.write(stream_data)
                chunk_hash.update(stream_data)
                stream_data = request.stream.read()
        except:
            if (os.path.exists(storage_path + ".partial")):
                os.remove(storage_path + ".partial")
                chunk.get_lock().acquire()
                chunk.remove_worker_status(worker.get_id())
                return {"error": "Error processing chunk"}, 500
    if (os.path.exists(storage_path + ".partial") and os.stat(storage_path + ".partial").st_size == worker_status.downloaded):
        chunk.mark_worker_status_complete(chunk_hash.hexdigest()) # This chunk is now complete
        os.rename(storage_path + ".partial", storage_path)
    else:
        if (os.path.exists(storage_path + ".partial")):
            os.remove(storage_path + ".partial")
        chunk.remove_worker_status(worker.get_id())
        return {"error": "Error processing chunk"}, 500

    # Check that this hash matches the others that are complete
    chunk_hashes = {}
    for worker_id in chunk.get_worker_status():
        if (worker_id == worker.get_id()):
            continue # it's us lol

        worker_status = chunk.get_worker_status(worker_id)
        if (not worker_status.complete):
            continue
        chunk_hashes[worker_status.hash] = chunk_hashes.get(worker_status.hash, 0) + 1

    if (len(chunk_hashes) > 1): # There are mismatched hashes!
        most_popular_hash = None
        for hash in chunk_hashes:
            if (most_popular_hash == None or chunk_hashes[most_popular_hash] < chunk_hashes[hash]):
                most_popular_hash = hash
        for worker_id in list(chunk.get_workers()):
            worker_status = chunk.get_worker_status(worker_id)
            if (not worker_status.complete):
                continue
            if (worker_status.hash != most_popular_hash):
                # Delete mismatched workers from chunk stuff
                chunk.remove_worker_status(worker_id)
                os.remove(os.path.join(temp_storage_folder, f"chunk_{chunk.get_id()}_{worker_id}.bin")) # Remove the chunk this worker downloaded
        return {"result": "Upload had a mismatched hash, you can ignore this"}, 200 # We've processed the upload from the client, don't come back regardless of what happened
    
    # If the hashes weren't mismatched...
    if (chunk.get_worker_count() < state.config["general"]["trust_count"]): # Check that we have all the chunks responses we need
        return {"ok": "Upload looks good so far"}, 200
    for worker_id in chunk.get_workers(): # Check that they're all complete
        worker_status = chunk.get_worker_status(worker_id)
        if (not worker_status.complete):
            return {"ok": "Upload looks good so far"}, 200 # If any of the workers aren't complete we just skip this
    
    # So all the hashes are good
    # AND we have responses that are complete for every response for this chunk?
    # We can remove the other chunks and just keep ours
    worker_ids = list(chunk.get_workers())
    for worker_id in worker_ids[1:]: # Delete all but 1
        os.remove(os.path.join(temp_storage_folder, f"chunk_{chunk.get_id()}_{worker_id}.bin"))
    os.rename(os.path.join(temp_storage_folder, f"chunk_{chunk.get_id()}_{worker_ids[0]}.bin"), os.path.join(temp_storage_folder, f"chunk_{chunk.get_start()}.bin"))

    # Check if all the other chunks are also completed
    file_complete = True
    for chunk_id in chunk_file_object.get_chunks():
        state.cleanup_chunk_workers(chunk_id)
        worker_status_count = len(state.chunks[chunk_id].get_worker_count())
        if (worker_status_count == 0 or worker_status_count < state.config["general"]["trust_count"]):
            file_complete = False # Chunk hasn't been downloaded yet
            break
        for worker_id in state.chunks[chunk_id].get_workers():
            if (not state.chunks[chunk_id].get_worker_status(worker_id).complete):
                file_complete = False # Chunk hasn't finished downloading yet
                break
    
    if (not file_complete):
        return {"ok": "This chunk is validated"}, 200 # We're not yet done with the whole file despite being done with this chunk!

    # If we are done though, then we should construct and move the entire file
    chunk_files = []
    for chunk_id in chunk_file_object.get_chunks():
        chunk = state.chunks[chunk_id]
        chunk_files.append(os.path.join(temp_storage_folder, f"chunk_{chunk.get_start()}.bin"))

    # Now we construct the final file!
    md5_hash = hashlib.md5()
    sha1_hash = hashlib.sha1()
    sha256_hash = hashlib.sha256()
    destination_path = os.path.join(state.config["paths"]["storage_path"], chunk_file_object.get_path())
    os.makedirs(os.path.dirname(destination_path), exist_ok=True)
    with open(destination_path, 'wb') as main_file:
        for chunk_file_path in chunk_files:
            with open(chunk_file_path, 'rb') as chunk_file_stream:
                read_size = 1024**2 * 10
                data = chunk_file_stream.read(read_size) # Read 10MB at a time
                while (len(data) > 0):
                    main_file.write(data)
                    md5_hash.update(data)
                    sha1_hash.update(data)
                    sha256_hash.update(data)
                    data = chunk_file_stream.read(read_size)
            os.remove(chunk_file_path)
    state.sorted_downloadable_files.remove(chunk_file_object.get_id()) # We don't want to download this again
    # write hashes to file
    state.file_hashes[chunk_file_object.get_path()] = {
        "md5": md5_hash.hexdigest(),
        "sha1": sha1_hash.hexdigest(),
        "sha256": sha256_hash.hexdigest()
    }
    shutil.rmtree(temp_storage_folder, ignore_errors=True)
    chunk_file_object.mark_complete() # Mark file as actually complete
    for chunk_id in chunk_file_object.get_chunks():
        state.chunks_lock.acquire()
        state.chunks[chunk_id].get_lock().acquire()
        del state.chunks[chunk_id]
        state.chunks_lock.release()
    chunk_file_object.clear_chunks()
    
    return {"ok": "Upload entire file complete!"}, 200

gc_thread = Thread(target=gc)
gc_thread.start()
state.console.print(f'Listening on {state.config["server"]["port"]}')
state.console.start()

if (args.debug):
    from waitress import serve
    serve(app, host="0.0.0.0", port=state.config["server"]["port"], threads=8, backlog=16)