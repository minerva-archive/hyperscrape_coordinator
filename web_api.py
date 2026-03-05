from threading import Thread

import state
from flask import Flask, request
from waitress import serve

web_api_app = Flask(__name__)


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
        "current_speed": state.current_speed,
    }


@web_api_app.get("/api/leaderboard")
def get_leaderboard():
    limit = request.args.get("limit", 25)
    offset = request.args.get("offset", 0)
    response = []
    for leaderboard_id in state.current_leaderboard_order[offset:limit]:
        leaderboard_object = state.current_leaderboard[leaderboard_id]
        response.append(
            {
                "discord_username": leaderboard_object.get_discord_username(),
                "avatar_url": leaderboard_object.get_avatar_url(),
                "downloaded_chunks": leaderboard_object.get_downloaded_chunks(),
                "downloaded_bytes": leaderboard_object.get_downloaded_bytes(),
            }
        )
    return response


@web_api_app.get("/code")
def get_code():
    with open("./static/code.html") as file:
        return file.read()


@web_api_app.get("/")
def slash_index():
    with open("./static/index.html") as file:
        return file.read()


@web_api_app.get("/index.html")
def html_index():
    with open("./static/index.html") as file:
        return file.read()


def run_web_api():
    while True:
        try:
            serve(
                web_api_app,
                host="0.0.0.0",
                port=state.config["server"]["http"]["port"],
                threads=state.config["server"]["http"]["threads"],
                backlog=state.config["server"]["http"]["backlog"],
            )
        except:
            print("[WARN] Waitress crashed - restarting!")


def start_web_api():
    thread = Thread(target=run_web_api)
    thread.start()
    return thread
