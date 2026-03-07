from threading import Thread
import asyncio

from background_coordinator_thread import background_coordinator
from fastapi.templating import Jinja2Templates
from websockets import InvalidUpgrade
from console import Console
import requests
import uvicorn
import state

from fastapi import FastAPI, WebSocket, Request, WebSocketDisconnect
from websocket_handlers import detach_chunk, get_chunks, register_worker, upload_chunk
from ws_message import WSMessage, WSMessageType
from fastapi.responses import HTMLResponse
from workers import Worker

print("=========================")
print("=  HYPERSCRAPE SERVER   =")
print("= Created By Hackerdude =")
print("=========================")


async def handler(websocket: WebSocket, ip_address: str):
    """!
    @brief The main websocket connection handler function

    @param websocket (WebSocket): The websocket connection
    @param ip_address (str): The IP address that this connection came from
    """
    worker: Worker = None
    while True:
        # If the server is shutting down we shouldn't accept new connections and should terminate existing ones
        if (state.shutting_down):
            try:
                websocket.send_bytes(WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Server is shutting down!"}).encode())
                websocket.close()
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
                    state.remove_worker(worker.get_id())
                return
            
            # Decode the messsage
            message: WSMessage = WSMessage.decode(data)
            response = WSMessage(WSMessageType.ERROR_RESPONSE, {"error": "Message failure!"})

            # Handle the message
            if (message.get_type() == WSMessageType.REGISTER):
                response = register_worker(ip_address, message.get_payload())
                if (not response.get_payload().get("worker_id", None)):
                    await websocket.send_bytes(response.encode())
                    await websocket.close()
                    raise Exception("Bad worker register message")
                worker = state.workers[response.get_payload()["worker_id"]]
                worker.set_websocket(websocket)
            elif (message.get_type() == WSMessageType.GET_CHUNKS):
                response = await asyncio.to_thread(get_chunks, worker, message.get_payload())
            elif (message.get_type() == WSMessageType.UPLOAD_SUBCHUNK):
                response = await asyncio.to_thread(upload_chunk, worker, message.get_payload())
            elif (message.get_type() == WSMessageType.DETACH_CHUNK):
                response = await asyncio.to_thread(detach_chunk, worker, message.get_payload())

            # Send a response
            await websocket.send_bytes(response.encode())
        except InvalidUpgrade:
            return # Unclear what causes this, it's not our problem though
        except WebSocketDisconnect:
            # If the websocket disconnects under normal circumstances
            await state.remove_worker(worker.get_id())
            return
        except Exception as e:
            if (worker):
                print(f"Disconnecting worker {worker.get_id()} due to exception.")
                print(e)
                await state.remove_worker(worker.get_id())
                return


# Background thread for occasional tasks
background_thread = Thread(target=background_coordinator)
background_thread.start()


# Console setup
console = Console()

# FastAPI
app = FastAPI()
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
def get_stats() -> dict:
    """!
    @brief Return stats about the current coordinator state

    @return (dict): _description_
    """

    total_files = len(state.files)
    return {
        "total_files": total_files,
        "total_chunks": len(state.chunks),
        "completed_files": state.completed_files,
        "completed_chunks": state.completed_chunks,
        "assigned": state.assigned_chunks,
        "pending": total_files - state.completed_files,
        "failed": state.failed_chunks,
        "active_workers": len(state.workers),
        "downloaded_bytes": state.downloaded_bytes,
        "completed_bytes": state.completed_bytes,
        "total_bytes": state.total_bytes,
        "current_speed": state.current_speed
    }


@app.get("/api/leaderboard")
def get_leaderboard(limit: int = 25, offset: int = 0) -> list[dict]:
    """!
    @brief Get the current leaderboard

    @param limit (int, optional): The number of leaderboard items to get. Defaults to 25.
    @param offset (int, optional): The offset in the leaderboard to get items from. Defaults to 0.

    @return (list[dict]): The leaderboard items, in ascending rank order
    """

    response = []
    for leaderboard_id in state.current_leaderboard_order[offset:limit]:
        leaderboard_object = state.current_leaderboard[leaderboard_id]
        response.append({
            "discord_username": leaderboard_object.get_discord_username(),
            "avatar_url": leaderboard_object.get_avatar_url(),
            "downloaded_chunks": leaderboard_object.get_downloaded_chunks(),
            "downloaded_bytes": leaderboard_object.get_downloaded_bytes()
        })
    return response


@app.get("/code", response_class=HTMLResponse)
def get_code(request: Request, code: str):
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
def slash_index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
    
# @FIXME: This is probably redundant
@app.get("/index.html")
def html_index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# Start the main app
if __name__ == "__main__":
    console.start()
    uvicorn.run(app, host="0.0.0.0", port=state.config["server"]["port"], access_log=False)