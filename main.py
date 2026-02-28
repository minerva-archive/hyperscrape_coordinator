from uuid import uuid4
from files import HyperscrapeFile
import state
from flask import Flask, request

from helpers import get_request_ip, get_worker


app = Flask(__name__)

@app.route("/")
def root():
    return 'Hyperscrape Coordinator - Created by <a href="https://hackerdude.tech">Hackerdude</a>'

@app.route("/workers", methods=['POST'])
def register_worker():
    ip = get_request_ip()
    if (ip in state.banned_ips):
        return {"error": "Could not connect to worker"}, 403
    for worker_id in list(state.workers.keys()):
        if (state.workers[worker_id].ip == ip):
            state.remove_worker(worker_id)
    data = request.json
    if (data == None or
        (not "version" in data) or
        (not "max_upload" in data) or 
        (not "max_download" in data) or
        (not "max_per_file_speed" in data) or
        (not "threads" in data)):
        return {"error": "Invalid Request"}, 400
    if (data["version"] > state.config['general']['version']):
        return {"error": f"Version mismatch, expected {state.config['general']['version']}"}, 400
    return {
        "token": state.add_worker(ip, data["max_upload"], data["max_download"], data["max_per_file_speed"], data["threads"])
    }

@app.route("/chunks", methods=['GET'])
def get_chunks():
    worker = get_worker()
    if (not worker):
        return {"error": "Invalid token!"}, 403
    
    if (len(state.receivers) == 0):
        return {} # No receiver can ingest this!
    
    # Get top 10 files
    top_files = state.sorted_downloadable_files[:10]
    top_receivers = {}
    for file_id in top_files:
        if (state.files[file_id].receiver == None):
            continue
        top_receivers[state.files[file_id].receiver] = top_receivers.get(state.files[file_id].receiver, 0) + 1

    ideal_receiver = list(state.receivers.keys())[0]
    if (len(top_receivers) != 0):
        sorted_receivers = sorted(list(top_receivers.keys()), key=lambda el: top_receivers[el])
        ideal_receiver = sorted_receivers[0] # Get receiver with least use

    chunks_to_get = request.args.get("n", worker.max_download/worker.max_per_file_speed)
    # Get files with high worker counts
    # So the entire network is working together for a single file essenially
    chunks_to_download = []
    file_download_candidate_offset = 0
    while chunks_to_download < chunks_to_get and file_download_candidate_offset + chunks_to_get < len(state.sorted_downloadable_files):
        files_to_download_candidates = state.sorted_downloadable_files[file_download_candidate_offset:file_download_candidate_offset+chunks_to_get]
        for file_id in files_to_download_candidates:
            for chunk_id in state.files[file_id].chunks:
                # Get the chunk in this file with the lowest number of downloaders under checksum_verify
                if (len(state.chunks[chunk_id].worker_status) >= state.config["general"]["trust_count"]):
                    continue
                chunks_to_download.append(chunk_id)

    ###
    # We now have a list of chunks to download
    ###
    response = {}
    for chunk_id in chunks_to_download:
        chunk = state.chunks[chunk_id]
        file = state.files[state.chunk_to_file[chunk_id]]
        if (file.receiver == None): # If no receiver is set for this file, we pick one
            file.receiver = ideal_receiver
        state.assigned_chunks.assign_chunk(worker.worker_id, chunk_id)
        response[chunk_id] = {
            "url": file.url,
            "range": [
                chunk.start,
                chunk.end
            ],
            "destination": state.files[state.chunk_to_file[chunk_id]].destination
        }
    return response

@app.route("/status", methods=['PUT'])
def put_status():
    worker = get_worker()
    if (not worker):
        return {"error": "Invalid token!"}, 403
    data = request.json
    for chunk_id in data:
        state.chunks[chunk_id].worker_status[worker.worker_id].downloaded = data[chunk_id]["dowloaded"]
        state.chunks[chunk_id].worker_status[worker.worker_id].uploaded = data[chunk_id]["uploaded"]

state.files = {}
state.chunks = {}
state.chunk_to_file = {}
state.sorted_downloadable_files = []
state.file_worker_counts = {}
state.add_file(HyperscrapeFile(
    str(uuid4()),
    "./test/test.txt",
    156437,
    "https://myrient.erista.me/files/No-Intro/ACT%20-%20Apricot%20PC%20Xi/%5BBIOS%5D%20MS-DOS%202.11%20%28Europe%29%20%28v3.1%29%20%28Disk%201%29%20%28OS%29.zip",
    1024*25
))


if __name__ == "__main__":
    from waitress import serve
    print(f'Listening on {state.config["server"]["port"]}')
    serve(app, host="0.0.0.0", port=state.config["server"]["port"], threads=state.config["server"]["threads"])