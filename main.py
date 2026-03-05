import asyncio
from io import FileIO
import os
from threading import Thread
from uuid import uuid4

import requests
from websockets import ConnectionClosedError, ConnectionClosedOK, ServerConnection
from auth_token import AuthToken
from console import Console
from files import HyperscrapeChunk
from background_coordinator_thread import background_coordinator
import state
import hashlib
import shutil

from helpers import get_chunk_instance_temp_path, get_chunk_path, get_url_size

import argparse

from web_api import start_web_api
from workers import Worker
from ws_message import WSMessage, WSMessageType
from websockets.asyncio.server import serve


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

def register_worker(ip: str, data: dict):
    if (ip in state.banned_ips):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Could not connect to worker"})
    #with state.workers_lock:
    #    for worker_id in list(state.workers.keys()):
    #        if (state.workers[worker_id].ip == ip):
    #            state.remove_worker(worker_id)
    if (data == None or
        (not "version" in data) or
        (not "max_concurrent" in data)):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Invalid Request"})
    if (data["version"] > state.config['general']['version']):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": f"Version mismatch, expected {state.config['general']['version']}"})
    
    discord_id = None
    discord_username = None
    avatar_url = None
    if (data["discord_token"]):
        API_ENDPOINT = 'https://discord.com/api/v10'
        req_data = {
            'grant_type': 'authorization_code',
            'code': data["discord_token"],
            'redirect_uri': state.secrets["discord"]["redirect_uri"]
        }
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        r = requests.post('%s/oauth2/token' % API_ENDPOINT, data=req_data, headers=headers, auth=(state.secrets["discord"]["client_id"], state.secrets["discord"]["client_secret"]))
        if (r.status_code == 200):
            res_data = r.json()
            r = requests.get("https://discord.com/api/users/@me", headers={
                "Authorization": f"Bearer {res_data['access_token']}"
            })
            if (r.status_code == 200):
                res_data = r.json()
                discord_id = res_data["id"]
                discord_username = res_data["global_name"] # Use the display name
                avatar_url = f"https://cdn.discordapp.com/avatars/{res_data['id']}/{res_data['avatar']}.png"

    
    worker_id = str(uuid4())
    auth_token = AuthToken(worker_id)
    with state.workers_lock:
        state.workers[worker_id] = Worker(worker_id, ip, auth_token.nonce, data["max_concurrent"], discord_id)
    if (discord_id and not discord_id in state.current_leaderboard):
        with state.current_leaderboard_lock:
            state.current_leaderboard[discord_id] = state.LeaderboardObject(discord_id, discord_username, avatar_url, 0, 0)

    return WSMessage(WSMessageType.REGISTER_RESPONSE, {
        "worker_id": worker_id,
        "auth_token": auth_token.as_token()
    })

def get_chunks(worker: Worker, data: dict):
    num_chunks_to_get = int(data["count"])
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
            with state.chunks_lock:
                while current_size < file.get_total_size():
                    start = current_size
                    end = min(current_size + file.get_chunk_size(), file.get_total_size())
                    chunk_id = str(uuid4())
                    state.chunks[chunk_id] = HyperscrapeChunk(chunk_id, start, end)
                    file.add_chunk(chunk_id)
                    current_size = end
        
        # Ensure the worker isn't currently downloading this file
        for chunk_id in file.get_chunks():
            # Cleanup workers that have not uploaded in a while
            state.cleanup_chunk_workers(chunk_id)
            if (state.chunks[chunk_id].has_worker(worker.get_id()) and not state.chunks[chunk_id].get_worker_status(worker.get_id()).get_complete()):
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
            if (highest_chunk_id == None or state.chunks[chunk_id].get_worker_count() > state.chunks[highest_chunk_id].get_worker_count()):
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
        with state.chunks[chunk_id].get_lock():
            state.chunks[chunk_id].add_worker_status(worker.get_id())
        response[chunk_id] = {
            "file_id": chunk_to_file[chunk_id],
            "url": file.get_url(),
            "range": [
                chunk.get_start(),
                chunk.get_end()
            ]
        }
    state.assigned_chunks += len(chunks_to_download)
    return WSMessage(WSMessageType.CHUNK_RESPONSE, response)

def upload_chunk(worker: Worker, data: dict, file_handles: dict[str, FileIO], chunk_hashes: dict[str, object], file_paths: dict[str, str]):
    chunk_id = data.get("chunk_id", None)
    file_id = data.get("file_id", None)

    if (chunk_id == None or file_id == None):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Invalid request"})

    # Ensure the chunk exists
    if (not chunk_id in state.chunks):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Unknown chunk"})
    if (not state.chunks[chunk_id].has_worker(worker.get_id())):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Chunk not requested"})
    if (state.chunks[chunk_id].get_worker_status(worker.get_id()).get_complete()):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Chunk already complete"})

    chunk = state.chunks[chunk_id]
    # Handle chunk uploading
    chunk_file_object = state.files[file_id]
    if (not chunk_file_object.has_chunk(chunk_id)):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Unknown file"})
    temp_storage_folder = os.path.join(state.config["paths"]["chunk_temp_path"], chunk_file_object.get_path())
    # Check if a handle exits for this chunk
    chunk_path = get_chunk_instance_temp_path(chunk_file_object.get_id(), chunk_id, worker.get_id())
    if (not chunk_id in file_handles):
        os.makedirs(os.path.dirname(chunk_path), exist_ok=True)
        file_handles[chunk_id] = open(chunk_path + ".partial", 'wb')
        file_paths[chunk_id] = chunk_path + ".partial"
        chunk_hashes[chunk_id] = hashlib.md5()
    
    file_handles[chunk_id].write(data["payload"])
    chunk_hashes[chunk_id].update(data["payload"])
    if (worker.get_discord_id()):
        state.update_stats_bytes(worker.get_discord_id(), len(data["payload"]))
    state.downloaded_bytes += len(data["payload"])
    chunk.update_worker_status_uploaded(worker.get_id(), file_handles[chunk_id].tell())

    if (file_handles[chunk_id].tell() != chunk.get_end() - chunk.get_start()):
        return WSMessage(WSMessageType.OK_RESPONSE, {"ok": "Segment Received"}) # Chunk not yet finished

    chunk.mark_worker_status_complete(worker.get_id(), chunk_hashes[chunk_id].hexdigest()) # This chunk is now complete
    del chunk_hashes[chunk_id]
    file_handles[chunk_id].close() # Close the file
    del file_handles[chunk_id]
    os.rename(chunk_path + ".partial", chunk_path)
    state.assigned_chunks -= 1
    state.completed_chunks += 1
    if (worker.get_discord_id()):
        state.update_stats_chunks(worker.get_discord_id(), 1)

    # Check that this hash matches the others that are complete
    with chunk.get_lock():
        chunk_hash_set = set()
        for worker_id in chunk.get_workers():
            worker_status = chunk.get_worker_status(worker_id)
            if (not worker_status.get_complete()):
                continue
            chunk_hash_set.add(worker_status.get_hash())

        if (len(chunk_hash_set) > 1): # There are mismatched hashes!
            # We should re-download all the chunk instances we have if there is a mismatch
            for worker_id in list(chunk.get_workers()):
                worker_status = chunk.get_worker_status(worker_id)
                if (not worker_status.get_complete()):
                    continue
                chunk.remove_worker_status(worker_id)
                state.failed_chunks += 1
                state.assigned_chunks -= 1
                os.remove(get_chunk_instance_temp_path(chunk_file_object.get_id(), chunk_id, worker_id)) # Remove the chunk this worker downloaded
            return WSMessage(WSMessageType.OK_RESPONSE, {"result": "Upload had a mismatched hash, you can ignore this"}) # We've processed the upload from the client, don't come back regardless of what happened
        
        # If the hashes weren't mismatched...
        if (chunk.get_worker_count() < state.config["general"]["trust_count"]): # Check that we have all the chunks responses we need
            return WSMessage(WSMessageType.OK_RESPONSE, {"ok": "Upload looks good so far"})
        for worker_id in chunk.get_workers(): # Check that they're all complete
            worker_status = chunk.get_worker_status(worker_id)
            if (not worker_status.get_complete()):
                return WSMessage(WSMessageType.OK_RESPONSE, {"ok": "Upload looks good so far"}) # If any of the workers aren't complete we just skip this
        
        # So all the hashes are good
        # AND we have responses that are complete for every response for this chunk?
        # We can remove the other chunks and just keep ours
        # Only if it hasn't already been done...
        if (not os.path.exists(get_chunk_path(chunk_file_object.get_id(), chunk_id))):
            worker_ids = list(chunk.get_workers())
            for worker_id in worker_ids: # Delete all...
                if (worker_id == worker.get_id()): # Except ours!
                    continue
                os.remove(get_chunk_instance_temp_path(chunk_file_object.get_id(), chunk_id, worker_id))
            os.rename(chunk_path, get_chunk_path(chunk_file_object.get_id(), chunk_id))

    # Check if all the other chunks are also completed
    with chunk_file_object.get_lock():
        destination_path = os.path.join(state.config["paths"]["storage_path"], chunk_file_object.get_path())
        if (os.path.exists(destination_path)):
            return WSMessage(WSMessageType.OK_RESPONSE, {"ok": "Upload entire file complete!"})
        
        file_complete = True
        for chunk_id in chunk_file_object.get_chunks():
            state.cleanup_chunk_workers(chunk_id)
            worker_status_count = state.chunks[chunk_id].get_worker_count()
            if (worker_status_count == 0 or worker_status_count < state.config["general"]["trust_count"]):
                file_complete = False # Chunk hasn't been downloaded yet
                break
            for worker_id in state.chunks[chunk_id].get_workers():
                if (not state.chunks[chunk_id].get_worker_status(worker_id).get_complete()):
                    file_complete = False # Chunk hasn't finished downloading yet
                    break
        
        if (not file_complete):
            return WSMessage(WSMessageType.OK_RESPONSE, {"ok": "This chunk is validated"}) # We're not yet done with the whole file despite being done with this chunk!

        # If we are done though, then we should construct and move the entire file
        chunk_files = []
        for chunk_id in chunk_file_object.get_chunks():
            chunk = state.chunks[chunk_id]
            chunk_files.append(get_chunk_path(chunk_file_object.get_id(), chunk_id))

        # Now we construct the final file!
        md5_hash = hashlib.md5()
        sha1_hash = hashlib.sha1()
        sha256_hash = hashlib.sha256()
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
            with state.chunks_lock:
                with state.chunks[chunk_id].get_lock():
                    del state.chunks[chunk_id]
        chunk_file_object.clear_chunks()
    
    state.completed_files += 1
    return WSMessage(WSMessageType.OK_RESPONSE, {"ok": "Upload entire file complete!"})

def detach_chunk(worker: Worker, data: dict, file_handles: dict[str, FileIO], chunk_hashes: dict[str, object], file_paths: dict[str, str]):
    chunk_id = data["chunk_id"]
    with state.chunks[chunk_id].get_lock():
        if (chunk_id in file_handles):
            file_handles[chunk_id].close()
            del file_handles[chunk_id]
            del chunk_hashes[chunk_id]
            os.remove(file_paths[chunk_id])
            del file_paths[chunk_id]
        if (state.chunks[chunk_id].has_worker(worker.get_id())):
            state.chunks[chunk_id].remove_worker_status(worker.get_id())
            state.assigned_chunks -= 1
    return WSMessage(WSMessageType.OK_RESPONSE, {"ok", "detached"})

async def handler(websocket: ServerConnection):
    worker: Worker = None
    file_handles: dict[str, FileIO] = {} # File handles for each chunk this worker is uploading
    chunk_hashes: dict[str, object] = {}
    file_paths: dict[str, str] = {} # Chunk to path
    while True:
        try:
            data = await asyncio.wait_for(websocket.recv(), timeout=state.config["general"]["worker_timeout"]) # Timeout a worker after 10 minutes
            message: WSMessage = WSMessage.decode(data)
            response = WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Message failure!"})
            if (message.get_type() == WSMessageType.REGISTER):
                response = register_worker(websocket.remote_address[0], message.get_payload())
                worker = state.workers[response.get_payload()["worker_id"]]
            elif (message.get_type() == WSMessageType.GET_CHUNKS):
                response = get_chunks(worker, message.get_payload())
            elif (message.get_type() == WSMessageType.UPLOAD_SUBCHUNK):
                response = upload_chunk(worker, message.get_payload(), file_handles, chunk_hashes, file_paths)
            elif (message.get_type() == WSMessageType.DETACH_CHUNK):
                response = detach_chunk(worker, message.get_payload(), file_handles, chunk_hashes, file_paths)
            await websocket.send(response.encode())
        except (ConnectionClosedOK, ConnectionClosedError, TimeoutError):
            try:
                await websocket.close()
            except:
                pass
            for chunk_id in file_handles:
                file_handles[chunk_id].close()
                os.remove(file_paths[chunk_id]) # Delete our partials
                state.chunks[chunk_id].remove_worker_status(worker.get_id())
                state.assigned_chunks -= 1
            if (worker):
                with state.workers_lock:
                    del state.workers[worker.get_id()]
            break

gc_thread = Thread(target=background_coordinator)
gc_thread.start()

console = Console()

async def main():
    async with serve(handler, "", state.config["server"]["port"]) as server:
        print(f"Listening on port {state.config["server"]["port"]}")
        start_web_api()
        console.start()
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())