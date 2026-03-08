import requests
import os
import state

def _safe_join_under(base_dir: str, relative_path: str) -> str:
    base_abs = os.path.abspath(base_dir)
    joined_abs = os.path.abspath(os.path.join(base_abs, relative_path))
    if (os.path.commonpath([base_abs, joined_abs]) != base_abs):
        raise ValueError("Unsafe path")
    return joined_abs


def get_url_size(url: str) -> int|None:
    """!
    @brief Get the filesize of a file given the URL

    @param url (str): The URL to get the filesize for

    @return (int|None): Returns the filesize of the URL or None if it could not be obtained
    """
    try:
        return int(requests.head(url, headers={
            "User-Agent": state.USER_AGENT
        }, allow_redirects=True).headers.get("Content-Length", None))
    except:
        return None



def get_chunk_instance_temp_path(file_id: str, chunk_id: str, worker_id: str) -> str:
    """!
    @brief Get the path for a chunk instance (a chunk uploaded by a specific worker)

    @param file_id (str): The ID of the file this chunk belongs to
    @param chunk_id (str): The ID of the chunk
    @param worker_id (str): The ID of the worker uploading this chunk

    @return (str): Returns the path to the chunk's instance file
    """
    temp_storage_folder = _safe_join_under(state.config["paths"]["chunk_temp_path"], state.files[file_id].get_path())
    return os.path.join(temp_storage_folder, f"{chunk_id}_{worker_id}.bin")



def get_chunk_path(file_id: str, chunk_id: str) -> str:
    """!
    @brief Get the path for a chunk (not an instance)

    @param file_id (str): The ID of the file this chunk belongs to
    @param chunk_id (str): The ID of the chunk

    @return (str): Returns the path to the chunk's file
    """
    temp_storage_folder = _safe_join_under(state.config["paths"]["chunk_temp_path"], state.files[file_id].get_path())
    return os.path.join(temp_storage_folder, f"{state.chunks[chunk_id].get_start()}.bin")


def get_storage_file_path(file_id: str) -> str:
    return _safe_join_under(state.config["paths"]["storage_path"], state.files[file_id].get_path())
