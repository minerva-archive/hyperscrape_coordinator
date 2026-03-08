from uuid import uuid4

import requests
import xxhash

from helpers import get_chunk_instance_temp_path, get_chunk_path, get_url_size
import state
from state_db import DBChunk, DBFile, DBWorkerStatus, StateDBConnection
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
    connection: StateDBConnection
    async with state.db.get_connection() as connection:
        response = {}
        chunks = await connection.get_ordered_downloadable_files(
                    num_chunks_to_get,
                    state.config["general"]["trust_count"],
                    worker.get_id(),
                    worker.get_ip()
                )
        
        for downloadable_chunk in chunks:
            print(downloadable_chunk)

            if (downloadable_chunk["file_size"] > free):
                break
            free -= downloadable_chunk["file_size"]

            connection.insert_worker_status( # @TODO: Batch insert!
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
                    downloadable_chunk["chunk_range_start"],
                    downloadable_chunk["chunk_range_end"]
                ]
            }
                
    return WSMessage(WSMessageType.CHUNK_RESPONSE, response)


async def upload_chunk(worker: Worker, data: dict) -> WSMessage:
    """!
    @brief Handle chunk upload requests

    @param worker (Worker): The current worker
    @param data (dict): The payload of the request

    @return (WSMessage): The response
    """

    if (type(data) != dict):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Invalid request"})
    
    chunk_id = data.get("chunk_id", None)

    connection: StateDBConnection
    async with state.db.get_connection() as connection:
        chunk: DBChunk
        chunk_file_object: DBFile
        worker_status: DBWorkerStatus
        chunk, chunk_file_object, worker_status = await connection.get_chunk_and_file_and_current_status(chunk_id)

    if (chunk == None or chunk_file_object == None or worker_status == None):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Invalid request"})

    # Ensure the chunk instance is not complete
    if (worker_status.hash):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Chunk already complete", "chunk_id": chunk_id})
    
    # Get folders
    chunk_path = get_chunk_instance_temp_path(chunk_file_object.id, chunk.id, worker.get_id())

    # Check if a handle exits for this chunk
    if (not chunk_id in worker.get_file_handles()):
        # We only actually download a chunk if we are the last chunk!
        if (not worker_status.hash_only):
            os.makedirs(os.path.dirname(chunk_path), exist_ok=True) # Create new handle otherwise
            worker.set_file_handle(chunk_id, open(chunk_path + ".partial", 'wb'))
            worker.set_file_path(chunk_id, chunk_path + ".partial")
        worker.set_chunk_hash(chunk_id, xxhash.xxh64())
    
    # If the file should be written to, then we should write to it :p
    if (not worker_status.hash_only):
        worker.get_file_handle(chunk_id).write(data["payload"])
        worker.get_file_handle(chunk_id).flush()

    # Update the hash
    worker.get_chunk_hash(chunk_id).update(data["payload"])
    
    # Update status
    if (worker_status.hash_only):
        async with state.db.get_connection() as connection:
            await connection.change_worker_status_uploaded(chunk.id, worker.get_id(), len(data["payload"]))
    else:
        # We prefer this for more robust upload detection when possible (ie: when it's actually downloading the file)
        async with state.db.get_connection() as connection:
            await connection.set_worker_status_uploaded(chunk.id, worker.get_id(), worker.get_file_handle(chunk_id).tell())
    del data["payload"] # Reduce memory consumption as we don't read the payload anymore

    # Check if the chunk instance is NOT complete
    async with state.db.get_connection() as connection:
        current_status = await connection.get_worker_status(chunk.id, worker.get_id()) # @TODO: This could be optimised
        if (current_status.uploaded != chunk.range_end - chunk.range_start):
            return WSMessage(WSMessageType.OK_RESPONSE, {"ok": "Segment Received", "chunk_id": chunk_id}) # Chunk not yet finished

    # If the chunk instance was the downloaded one, we should close the file handle and rename it
    if (not worker_status.hash_only):
        worker.close_file_handle(chunk_id)
        worker.remove_file_path(chunk_id)
        os.replace(chunk_path + ".partial", chunk_path)

    # This chunk instance is now complete (marked AFTER file is succesfully renamed)
    async with state.db.get_connection() as connection:
        await connection.set_worker_status_hash(chunk.id, worker.get_id(), worker.get_chunk_hash(chunk_id).hexdigest())
    # We have stored the completed hexdigest - we no longer need the worker's hash object
    worker.remove_chunk_hash(chunk_id)

    # Check that this chunk instance hash matches the others that are complete
    async with state.db.get_connection() as connection:
        current_status = await connection.get_worker_status(chunk.id, worker.get_id()) # @TODO: This could be optimised
        chunk_workers = await connection.get_chunk_worker_status(chunk.id)
        for worker_status in chunk_workers:
            # We only check completed chunk instances
            if (not worker_status.hash):
                continue

            # If there is a single mismatch...
            if (current_status.hash != worker_status.hash):
                # We should re-download all the completed chunk instances we have if there is a mismatch
                for worker_status in chunk_workers:
                    # Only the complete ones
                    if (not worker_status.hash):
                        continue

                    # If this is the downloaded one, we should delete it
                    if (not worker_status.hash_only):
                        os.remove(get_chunk_instance_temp_path(chunk_file_object.id, worker_status.chunk_id, worker_status.worker_id)) # Remove the chunk this worker downloaded

                    # Mark the chunk as needing more instances by removing the old ones
                    await connection.delete_worker_status(chunk.id, worker_status.worker_id)
                return WSMessage(WSMessageType.OK_RESPONSE, {"result": "Upload had a mismatched hash, you can ignore this", "chunk_id": chunk_id}) # We've processed the upload from the client, don't come back regardless of what happened
    
    # If the hashes weren't mismatched...
    if (len(chunk_workers) < state.config["general"]["trust_count"]): # Check that we have all the chunks responses we need
        return WSMessage(WSMessageType.OK_RESPONSE, {"ok": "Upload looks good so far", "chunk_id": chunk_id})
    for worker_status in chunk_workers: # Check that they're all complete
        if (not worker_status.hash):
            return WSMessage(WSMessageType.OK_RESPONSE, {"ok": "Upload looks good so far", "chunk_id": chunk_id}) # If any of the workers aren't complete we just skip this
    
    # So all the hashes are good
    # AND we have responses that are complete for every response for this chunk?
    # We can remove the other chunks and just keep ours
    # Only if it hasn't already been done...
    if (not os.path.exists(get_chunk_path(chunk_file_object.id, chunk_id))):
        for worker_status in chunk_workers:
            # Find the worker that actually uploaded the chunk
            if (not worker_status.hash_only):
                chunk_path = get_chunk_instance_temp_path(chunk_file_object.id, chunk_id, worker_id)
                break

        # Move that chunk to the proper path
        os.replace(chunk_path, get_chunk_path(chunk_file_object.id, chunk_id))


    # Check if all the other chunks are also completed
    # If this file is already uploaded
    destination_path = os.path.join(state.config["paths"]["storage_path"], chunk_file_object.path)

    file_complete = True
    async with state.db.get_connection() as connection:
        file_object_chunks = await connection.get_chunks_for_file(chunk_file_object.id)
        for chunk in file_object_chunks:
            chunk_workers = await connection.get_chunk_worker_status(chunk.id)

            if (len(chunk_workers) == 0 or len(chunk_workers) < state.config["general"]["trust_count"]):
                # Chunk hasn't been downloaded yet
                file_complete = False
                break

            for worker_status in chunk_workers:
                if (not worker_status.hash):
                    # Chunk hasn't finished downloading yet
                    file_complete = False 
                    break
    
    # If file isn't complete but the chunk is ok, we just return a response
    if (not file_complete):
        return WSMessage(WSMessageType.OK_RESPONSE, {"ok": "This chunk is validated", "chunk_id": chunk_id}) # We're not yet done with the whole file despite being done with this chunk!

    # If we are done though, then we should construct and move the entire file
    chunk_files = []
    for file_chunk in sorted(file_object_chunks, key=lambda file_chunk: file_chunk.range_start):
        chunk_files.append(get_chunk_path(chunk_file_object.id, file_chunk.id))

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

    # Delete the folder that stored the chunks
    temp_storage_folder = os.path.join(state.config["paths"]["chunk_temp_path"], chunk_file_object.path)
    shutil.rmtree(temp_storage_folder, ignore_errors=True)

    # Write hashes to db
    async with state.db.get_connection() as connection:
        await connection.insert_file_hash(
            chunk_file_object.id,
            md5_hash.hexdigest(),
            sha1_hash.hexdigest(),
            sha256_hash.hexdigest()
        )
        await connection.set_file_complete(chunk_file_object.id)
    return WSMessage(WSMessageType.OK_RESPONSE, {"ok": "Upload entire file complete!", "chunk_id": chunk_id})



async def detach_chunk(worker: Worker, data: dict) -> WSMessage:
    """!
    @brief Detach a worker from a chunk instance, informs the coordinator it should reassign that chunk instance

    @param worker (Worker): The worker object
    @param data (dict): The payload from the request

    @return (WSMessage): The response message
    """

    if (type(data) != dict):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Invalid request"})
    
    chunk_id = data["chunk_id"]
    connection: StateDBConnection
    async with state.db.get_connection() as connection:
        chunk = await connection.get_chunk(chunk_id)
    if (not chunk):
        return WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "No such chunk"})
    
    # Close the file handles and cleanup worker state info if applicable
    if (chunk_id in worker.get_file_handles()):
        worker.close_file_handle(chunk_id)
        os.remove(worker.get_file_path(chunk_id))
        worker.remove_file_path(chunk_id)
        worker.remove_chunk_hash(chunk_id)

    # Remove the worker status from the chunk
    # This lets the coordinator know that a "slot" has openned up for another worker to take its place
    async with state.db.get_connection() as connection:
        await connection.delete_worker_status(chunk_id, worker.get_id())
    return WSMessage(WSMessageType.OK_RESPONSE, {"ok": "detached", "chunk_id": chunk_id})
