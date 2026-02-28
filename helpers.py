import state
from flask import request

from workers import Worker

def get_request_ip() -> str|None:
    return request.headers.get("x-forwarded-for", request.remote_addr)

def get_worker() -> Worker|None:
    """!
    @brief Returns a worker for this request or None if not applicable
    """
    auth_header = request.headers.get("authorization")
    token = auth_header.split(" ")[-1]
    if (not token in state.workers):
        return None
    if (get_request_ip() != None and state.workers[token].ip != get_request_ip()):
        return None
    return state.workers[token]