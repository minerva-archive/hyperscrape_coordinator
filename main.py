from contextlib import asynccontextmanager
import os
from threading import Thread
import asyncio
import time

from background_coordinator_thread import background_coordinator
from fastapi.templating import Jinja2Templates
from websockets import InvalidUpgrade
from central_thread import central_thread
import requests
import uvicorn
import state

from fastapi import FastAPI, WebSocket, Request, WebSocketDisconnect
from state_db import StateDBConnection
from websocket_handlers import detach_chunk, get_chunks, register_worker, upload_chunk
from ws_message import WSMessage, WSMessageType
from fastapi.responses import HTMLResponse
from workers import Worker

import traceback


async def handler(websocket: WebSocket, ip_address: str):
    """!
    @brief The main websocket connection handler function

    @param websocket (WebSocket): The websocket connection
    @param ip_address (str): The IP address that this connection came from
    """
    worker: Worker = None
    while not state.shutting_down:
        # If the server is shutting down we shouldn't accept new connections and should terminate existing ones
        if (state.shutting_down):
            try:
                await websocket.send_bytes(WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Server is shutting down!"}).encode())
                await websocket.close()
            except:
                pass
            return
        

        try:
            # Read data from the socket
            data = await asyncio.wait_for(websocket.receive_bytes(), timeout=state.config["general"]["worker_timeout"]) # Timeout a worker after 10 minutes
            
            # Check that the header isn't from a legacy request
            if data[:3] == b'\x00\x80\x05':
                await websocket.send_bytes(b'\x00\x80\x05\x95G\x00\x00\x00\x00\x00\x00\x00}\x94\x8c\x05error\x94\x8c8Your worker is using a legacy protocol - PLEASE UPGRADE!\x94s.')
                await websocket.close()
                if (worker):
                    await state.remove_worker(worker.get_id())
                return
            
            # Decode the messsage
            message: WSMessage = WSMessage.decode(data)
            response = WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Message failure!"})

            # Handle the message
            if (message.get_type() == WSMessageType.REGISTER):
                response = await register_worker(ip_address, message.get_payload())
                if (not response.get_payload().get("worker_id", None)):
                    await websocket.send_bytes(response.encode())
                    await websocket.close()
                    raise Exception("Bad worker register message")
                worker = state.local_workers[response.get_payload()["worker_id"]]
                worker.set_websocket(websocket)
            elif (message.get_type() == WSMessageType.GET_CHUNKS):
                response = await get_chunks(worker, message.get_payload())
            elif (message.get_type() == WSMessageType.UPLOAD_SUBCHUNK):
                response = await upload_chunk(worker, message.get_payload())
            elif (message.get_type() == WSMessageType.DETACH_CHUNK):
                response = await detach_chunk(worker, message.get_payload())

            # Send a response
            await websocket.send_bytes(response.encode())
        except InvalidUpgrade:
            return # Unclear what causes this, it's not our problem though
        except WebSocketDisconnect:
            # If the websocket disconnects under normal circumstances
            if (worker):
                await state.remove_worker(worker.get_id())
            return
        except Exception as e:
            if (worker):
                print(f"Disconnecting worker {worker.get_id()} due to exception.")
                await state.remove_worker(worker.get_id())
            traceback.print_exc()
            print(e)
            return

# Background thread for occasional tasks
background_thread = Thread(target=background_coordinator)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await state.initialise()
    background_thread.start()
    print("Worker initialised!")
    yield
    state.shutting_down = True
    await state.db.close()

# FastAPI
app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

@app.websocket("/worker")
async def websocket_endpoint(websocket: WebSocket):
    """!
    @brief Handle the websocket connection

    @param websocket (WebSocket): The websocket connection
    """

    await websocket.accept()
    ip = websocket.headers.get("x-forwarded-for", websocket.client.host)
    await handler(websocket, ip)
    try:
        await websocket.close()
    except:
        pass


@app.get("/api/stats")
async def get_stats() -> dict:
    """!
    @brief Return stats about the current coordinator state
    """

    return {
        "total_files": state.total_file_count,
        "total_chunks": state.total_chunk_count,
        "completed_files": state.completed_file_count,
        "completed_chunks": state.completed_chunk_count,
        "assigned": state.assigned_chunk_count,
        "pending": state.total_file_count - state.completed_file_count,
        "active_workers": state.total_workers,
        "uploaded_bytes": state.uploaded_bytes,
        "completed_bytes": state.completed_bytes,
        "total_bytes": state.total_bytes,
        "current_speed": state.current_speed
    }


@app.get("/api/leaderboard")
async def get_leaderboard(limit: int = 25, offset: int = 0) -> list[dict]:
    """!
    @brief Get the current leaderboard

    @param limit (int, optional): The number of leaderboard items to get. Defaults to 25.
    @param offset (int, optional): The offset in the leaderboard to get items from. Defaults to 0.

    @return (list[dict]): The leaderboard items, in ascending rank order
    """

    response = []
    connection: StateDBConnection
    async with state.db.get_connection() as connection:
        for leaderboard_item in await connection.get_leaderboard(limit, offset):
            response.append({
                "discord_username": leaderboard_item.discord_username,
                "avatar_url": leaderboard_item.avatar_url,
                "uploaded_chunks": leaderboard_item.uploaded_chunks,
                "uploaded_bytes": leaderboard_item.uploaded_bytes
            })
    return response


@app.get("/code", response_class=HTMLResponse)
async def get_code(request: Request, code: str):
    """!
    @brief The code page for Discord auth

    @param request (Request): The request
    @param code (str): The code returned by Discord oauth
    """
    
    # Get the actual auth token from the oauth code
    discord_code = code
    API_ENDPOINT = 'https://discord.com/api/v10'
    req_data = {
        'grant_type': 'authorization_code',
        'code': discord_code,
        'redirect_uri': state.secrets["discord"]["redirect_uri"]
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    r = requests.post('%s/oauth2/token' % API_ENDPOINT, data=req_data, headers=headers, auth=(state.secrets["discord"]["client_id"], state.secrets["discord"]["client_secret"]))
    error = None
    access_token = None
    if (r.status_code == 200):
        access_token = r.json()["access_token"]
    else:
        error = "Could not load token"

    # Return it to the user
    return templates.TemplateResponse(
        "code.html",
        {
            "request": request,
            "code": access_token,
            "error": error
        }
    )
    
@app.get("/")
async def slash_index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
    
# @FIXME: This is probably redundant
@app.get("/index.html")
async def html_index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# If we are the main runner
if __name__ == "__main__":
    print("=========================")
    print("=  HYPERSCRAPE SERVER   =")
    print("= Created By Hackerdude =")
    print("=========================")

    asyncio.run(state.initialise())
    asyncio.run(state.main_initailise())
    central_thread_instance = Thread(target=central_thread)
    central_thread_instance.start()
    uvicorn.run("main:app", host="0.0.0.0", port=state.config["server"]["port"], access_log=False, workers=state.config["server"]["workers"])
    print("Shutting down main...")
    state.db.close()
    time.sleep(1)
    os._exit(0)
else:
    print(f"Starting server as {__name__}")
