import requests
import os
import state


def get_url_size(url: str):
    try:
        return int(requests.head(url, headers={
            "User-Agent": state.USER_AGENT
        }, allow_redirects=True).headers.get("Content-Length", None))
    except:
        return None
    
def get_chunk_instance_temp_path(file_id: str, chunk_id: str, worker_id: str):
    temp_storage_folder = os.path.join(state.config["paths"]["chunk_temp_path"], state.files[file_id].get_path())
    return os.path.join(temp_storage_folder, f"{chunk_id}_{worker_id}.bin")

def get_chunk_path(file_id: str, chunk_id: str):
    temp_storage_folder = os.path.join(state.config["paths"]["chunk_temp_path"], state.files[file_id].get_path())
    return os.path.join(temp_storage_folder, f"{state.chunks[chunk_id].get_start()}.bin")

