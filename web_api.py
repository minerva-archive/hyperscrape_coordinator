from threading import Thread
import time
from flask import Flask, render_template, request
import requests
from waitress.server import create_server
import state

web_api_app = Flask(__name__, template_folder="./static/")


@web_api_app.get("/api/stats")
def get_stats():
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
        "total_bytes": state.total_bytes,
        "current_speed": state.current_speed
    }


@web_api_app.get("/api/leaderboard")
def get_leaderboard():
    limit = request.args.get("limit", 25)
    offset = request.args.get("offset", 0)
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

@web_api_app.get("/code")
def get_code():
    discord_code = request.args.get("code")
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
    return render_template("code.html", code=access_token, error=error)
    
@web_api_app.get("/")
def slash_index():
    return render_template("index.html")
    
@web_api_app.get("/index.html")
def html_index():
    return render_template("index.html")

def run_web_api():
    server = create_server(web_api_app, host='0.0.0.0', port=state.config["server"]["http"]["port"], threads=state.config["server"]["http"]["threads"], backlog=state.config["server"]["http"]["backlog"])
    while True:
        server.run()
        time.sleep(120)
        server.close()

def start_web_api():
    thread = Thread(target=run_web_api)
    thread.start()
    return thread