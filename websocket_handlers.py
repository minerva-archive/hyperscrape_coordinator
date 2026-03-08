from uuid import uuid4

import requests
import xxhash

from helpers import get_chunk_instance_temp_path, get_chunk_path, get_url_size
import state
from state_db import StateDBConnection
from workers import Worker
from ws_message import WSMessage, WSMessageType

import hashlib
import shutil
import os

async def register_worker(ip: str, data: dict) -> WSMessage:
    """!
    @brief Registers a worker with the coordinator

    @param ip (str): The IP address of the worker
    @param data (dict): The worker's registration request payload

    @return (WSMessage): The response object
    """

    if (ip in state.banned_ips):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Could not connect to worker"})
    if (data == None or
        type(data) != dict or
        (not "version" in data) or
        (not "max_concurrent" in data)):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Invalid Request"})
    if (data["version"] != state.config['general']['version']):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": f"Version mismatch, expected {state.config['general']['version']}, got {data['version']}"})
    
    discord_id = None
    discord_username = None
    avatar_url = None
    if (data.get("access_token", None)):
        r = requests.get("https://discord.com/api/users/@me", headers={
            "Authorization": f"Bearer {data['access_token']}"
        })
        if (r.status_code == 200):
            res_data = r.json()
            discord_id = res_data["id"]
            discord_username = res_data["global_name"] # Use the display name
            avatar_url = f"https://cdn.discordapp.com/avatars/{res_data['id']}/{res_data['avatar']}.png"

    
    worker_id = str(uuid4())
    with state.workers_lock:
        state.local_workers[worker_id] = Worker(worker_id, ip, data["max_concurrent"], discord_id)
    state.redis.add_worker(state.local_workers[worker_id]) # Add worker to the count
    connection: StateDBConnection
    async with state.db.get_connection() as connection:
        await connection.add_to_leaderboard(discord_id, discord_username, avatar_url)

    return WSMessage(WSMessageType.REGISTER_RESPONSE, {
        "worker_id": worker_id,
    })



async def get_chunks(worker: Worker, data: dict) -> WSMessage:
    """!
    @brief Get what chunks a worker should download

    @param worker (Worker): The current worker
    @param data (dict): The payload of the request

    @return (WSMessage): A response, if all is well it'll contain the chunks assigned to the worker
    """

    total, used, free = shutil.disk_usage(state.config["paths"]["chunk_temp_path"]) # Get storage path stats
    num_chunks_to_get = int(data["count"])

    # Get files with high worker counts
    # So the entire network is working together for a single file essenially
    current_offset = 0
    connection: StateDBConnection
    async with state.db.get_connection() as connection:
        response = {}
        while (len(response) <= num_chunks_to_get or current_offset+num_chunks_to_get > state.total_chunk_count):
            chunks = await connection.get_ordered_downloadable_files(
                        num_chunks_to_get,
                        current_offset,
                        state.config["general"]["trust_count"],
                        worker.get_id(),
                        worker.get_ip()
                    )
            
            for downloadable_chunk in chunks:
                if (len(response) == num_chunks_to_get):
                    break                
                print(downloadable_chunk)

                if (downloadable_chunk["file_size"] > free):
                    break

                free -= downloadable_chunk["file_size"]

                connection.insert_worker_status(
                    downloadable_chunk["chunk_id"],
                    worker.get_id(),
                    0,
                    None,
                    not downloadable_chunk["hash_only"] # If the entire chunk is hash only, we should make this worker NOT hash only
                )
                
                response[downloadable_chunk["chunk_id"]] = {
                    "file_id": downloadable_chunk["file_id"],
                    "url": downloadable_chunk["file_url"],
                    "range": [
                        downloadable_chunk["chunk_start"],
                        downloadable_chunk["chunk_end"]
                    ]
                }
                
    return WSMessage(WSMessageType.CHUNK_RESPONSE, response)


def upload_chunk(worker: Worker, data: dict) -> WSMessage:
    """!
    @brief Handle chunk upload requests

    @param worker (Worker): The current worker
    @param data (dict): The payload of the request

    @return (WSMessage): The response
    """

    if (type(data) != dict):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Invalid request"})
    
    chunk_id = data.get("chunk_id", None)
    file_id = data.get("file_id", None)

    if (chunk_id == None or file_id == None):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Invalid request"})

    # Ensure the chunk exists
    if (not chunk_id in state.chunks):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Unknown chunk", "chunk_id": chunk_id})
    if (not state.chunks[chunk_id].has_worker(worker.get_id())):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Chunk not requested", "chunk_id": chunk_id})
    if (state.chunks[chunk_id].get_worker_complete(worker.get_id())):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Chunk already complete", "chunk_id": chunk_id})
    if (not file_id in state.files):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Unknown file", "chunk_id": chunk_id})
    
    chunk = state.chunks[chunk_id] # Get the chunk
    chunk_file_object = state.files[file_id] # Get the file
    hash_only = chunk.get_worker_hash_only(worker.get_id()) # Get if the chunk is only a hash (so no file download)
    # Get folders
    chunk_path = get_chunk_instance_temp_path(chunk_file_object.get_id(), chunk_id, worker.get_id())
    
    # Ensure the chunk_id is from the file specified by file_id
    if (not chunk_file_object.has_chunk(chunk_id)):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Unknown file", "chunk_id": chunk_id})

    # Check if a handle exits for this chunk
    if (not chunk_id in worker.get_file_handles()):
        # We only actually download a chunk if we are the last chunk!
        if (not hash_only):
            os.makedirs(os.path.dirname(chunk_path), exist_ok=True) # Create new handle otherwise
            worker.set_file_handle(chunk_id, open(chunk_path + ".partial", 'wb'))
            worker.set_file_path(chunk_id, chunk_path + ".partial")
        worker.set_chunk_hash(chunk_id, xxhash.xxh64())
    
    # If the file should be written to, then we should write to it :p
    if (not hash_only):
        worker.get_file_handle(chunk_id).write(data["payload"])
        worker.get_file_handle(chunk_id).flush()

    # Update the hash
    worker.get_chunk_hash(chunk_id).update(data["payload"])

    # Update the stats
    if (worker.get_discord_id()):
        state.update_stats_bytes(worker.get_discord_id(), len(data["payload"]))
    state.downloaded_bytes += len(data["payload"])
    

    if (hash_only):
        chunk.update_worker_status_uploaded(worker.get_id(), len(data["payload"]))
    else:
        # We prefer this for more robust upload detection when possible (ie: when it's actually downloading the file)
        chunk.update_worker_status_uploaded(worker.get_id(), worker.get_file_handle(chunk_id).tell())
    del data["payload"] # Reduce memory consumption as we don't read the payload anymore

    # Check if the chunk instance is NOT complete
    if (chunk.get_worker_uploaded(worker.get_id()) != chunk.get_end() - chunk.get_start()):
        return WSMessage(WSMessageType.OK_RESPONSE, {"ok": "Segment Received", "chunk_id": chunk_id}) # Chunk not yet finished

    # If the chunk instance was the downloaded one, we should close the file handle and rename it
    if (not hash_only):
        worker.close_file_handle(chunk_id)
        worker.remove_file_path(chunk_id)
        os.replace(chunk_path + ".partial", chunk_path)

    # This chunk instance is now complete (marked AFTER file is succesfully renamed)
    chunk.mark_worker_status_complete(worker.get_id(), worker.get_chunk_hash(chunk_id).hexdigest())
    # We have stored the completed hexdigest - we no longer need the worker's hash object
    worker.remove_chunk_hash(chunk_id)
    
    # Update state
    state.assigned_chunks -= 1
    state.completed_chunks += 1
    if (worker.get_discord_id()):
        state.update_stats_chunks(worker.get_discord_id(), 1)

    # Check that this chunk instance hash matches the others that are complete
    with chunk.get_lock():
        chunk_hash = chunk.get_worker_hash(worker.get_id())
        chunk_hash_mismatch = False
        for worker_id in chunk.get_workers():
            # We only check completed chunk instances
            if (not chunk.get_worker_complete(worker_id)):
                continue
            if (chunk_hash != chunk.get_worker_hash(worker_id)):
                chunk_hash_mismatch = True

        # There are mismatched hashes!
        if (chunk_hash_mismatch):
            # We should re-download all the completed chunk instances we have if there is a mismatch
            for worker_id in list(chunk.get_workers()):
                # Only the complete ones
                if (not chunk.get_worker_complete(worker_id)):
                    continue

                # If this is the downloaded one, we should delete it
                if (not chunk.get_worker_hash_only(worker_id)):
                    os.remove(get_chunk_instance_temp_path(chunk_file_object.get_id(), chunk_id, worker_id)) # Remove the chunk this worker downloaded

                # Mark the chunk as needing more instances by removing the old ones
                chunk.remove_worker_status(worker_id)

                # Mark the chunk as failed
                state.failed_chunks += 1
            return WSMessage(WSMessageType.OK_RESPONSE, {"result": "Upload had a mismatched hash, you can ignore this", "chunk_id": chunk_id}) # We've processed the upload from the client, don't come back regardless of what happened
        
        # If the hashes weren't mismatched...
        if (chunk.get_worker_count() < state.config["general"]["trust_count"]): # Check that we have all the chunks responses we need
            return WSMessage(WSMessageType.OK_RESPONSE, {"ok": "Upload looks good so far", "chunk_id": chunk_id})
        for worker_id in chunk.get_workers(): # Check that they're all complete
            if (not chunk.get_worker_complete(worker_id)):
                return WSMessage(WSMessageType.OK_RESPONSE, {"ok": "Upload looks good so far", "chunk_id": chunk_id}) # If any of the workers aren't complete we just skip this
        
        # So all the hashes are good
        # AND we have responses that are complete for every response for this chunk?
        # We can remove the other chunks and just keep ours
        # Only if it hasn't already been done...
        chunk_path = None # Store the obtained chunk path
        if (not os.path.exists(get_chunk_path(chunk_file_object.get_id(), chunk_id))):
            worker_ids = list(chunk.get_workers())
            for worker_id in worker_ids:
                # Find the worker that actually uploaded the chunk
                if (not chunk.get_worker_hash_only(worker_id)):
                    chunk_path = get_chunk_instance_temp_path(chunk_file_object.get_id(), chunk_id, worker_id)
                    break

            # Move that chunk to the proper path
            os.replace(chunk_path, get_chunk_path(chunk_file_object.get_id(), chunk_id))

    # Check if all the other chunks are also completed
    with chunk_file_object.get_lock():
        destination_path = os.path.join(state.config["paths"]["storage_path"], chunk_file_object.get_path())
        if (os.path.exists(destination_path)):
            return WSMessage(WSMessageType.OK_RESPONSE, {"ok": "Upload entire file complete!", "chunk_id": chunk_id})
        
        file_complete = True
        for chunk_id in chunk_file_object.get_chunks():
            state.cleanup_chunk_workers(chunk_id)
            worker_status_count = state.chunks[chunk_id].get_worker_count()

            if (worker_status_count == 0 or worker_status_count < state.config["general"]["trust_count"]):
                # Chunk hasn't been downloaded yet
                file_complete = False
                break

            for worker_id in state.chunks[chunk_id].get_workers():
                if (not state.chunks[chunk_id].get_worker_complete(worker_id)):
                    # Chunk hasn't finished downloading yet
                    file_complete = False 
                    break
        
        # If file isn't complete but the chunk is ok, we just return a response
        if (not file_complete):
            return WSMessage(WSMessageType.OK_RESPONSE, {"ok": "This chunk is validated", "chunk_id": chunk_id}) # We're not yet done with the whole file despite being done with this chunk!

        # If we are done though, then we should construct and move the entire file
        chunk_files = []
        for chunk_id in sorted(chunk_file_object.get_chunks(), key=lambda chunk_id: state.chunks[chunk_id].get_start()):
            chunk = state.chunks[chunk_id]
            chunk_files.append(get_chunk_path(chunk_file_object.get_id(), chunk_id))

        # Now we construct the final file!
        md5_hash = hashlib.md5()
        sha1_hash = hashlib.sha1()
        sha256_hash = hashlib.sha256()
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)

        # Write to the final file...
        with open(destination_path + ".partial", 'wb') as main_file:
            for chunk_file_path in chunk_files:
                with open(chunk_file_path, 'rb') as chunk_file_stream:
                    read_size = 1024**2 * 10 # Read in 10MB increments
                    data = chunk_file_stream.read(read_size)

                    # Write and generate hashes
                    while (len(data) > 0):
                        main_file.write(data)
                        md5_hash.update(data)
                        sha1_hash.update(data)
                        sha256_hash.update(data)
                        data = chunk_file_stream.read(read_size)
                os.remove(chunk_file_path)

        # Rename the file
        os.replace(destination_path + ".partial", destination_path)
        # We don't want to download this again
        state.sorted_downloadable_files.remove(chunk_file_object.get_id())
        # Stat updates
        state.completed_bytes += chunk_file_object.get_total_size()
        # Write hashes to db
        db.insert_file_hash(
            chunk_file_object.get_id(),
            md5_hash.hexdigest(),
            sha1_hash.hexdigest(),
            sha256_hash.hexdigest()
        )

        # Delete the folder that stored the chunks
        temp_storage_folder = os.path.join(state.config["paths"]["chunk_temp_path"], chunk_file_object.get_path())
        shutil.rmtree(temp_storage_folder, ignore_errors=True)
        chunk_file_object.mark_complete() # Mark file itself as actually complete
        ## # We can delete the chunks of a complete file as they will no longer be needed
        ## for chunk_id in chunk_file_object.get_chunks():
        ##     with state.chunks_lock:
        ##         with state.chunks[chunk_id].get_lock():
        ##             del state.chunks[chunk_id]
        ##             db.delete_chunk(chunk_id)
    
    state.completed_files += 1
    return WSMessage(WSMessageType.OK_RESPONSE, {"ok": "Upload entire file complete!", "chunk_id": chunk_id})



def detach_chunk(worker: Worker, data: dict) -> WSMessage:
    """!
    @brief Detach a worker from a chunk instance, informs the coordinator it should reassign that chunk instance

    @param worker (Worker): The worker object
    @param data (dict): The payload from the request

    @return (WSMessage): The response message
    """

    if (type(data) != dict):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Invalid request"})
    
    chunk_id = data["chunk_id"]
    if (not chunk_id in state.chunks):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "No such chunk"})
    
    with state.chunks[chunk_id].get_lock():
        # Close the file handles and cleanup worker state info if applicable
        if (chunk_id in worker.get_file_handles()):
            worker.close_file_handle(chunk_id)
            os.remove(worker.get_file_path(chunk_id))
            worker.remove_file_path(chunk_id)
            worker.remove_chunk_hash(chunk_id)

        # Remove the worker status from the chunk
        # This lets the coordinator know that a "slot" has openned up for another worker to take its place
        if (state.chunks[chunk_id].has_worker(worker.get_id())):
            state.chunks[chunk_id].remove_worker_status(worker.get_id())
            state.assigned_chunks -= 1
    return WSMessage(WSMessageType.OK_RESPONSE, {"ok": "detached", "chunk_id": chunk_id})
