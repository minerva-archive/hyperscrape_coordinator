from threading import Thread
from typing import Callable
from state_db import db
import time
import state
import os


class Console():
    """!
    @brief Console class handles the interactive console spawned by the coordinator when it runs
    """

    def __init__(self):
        self._should_run = True
        self._thread = Thread(target=self.main_thread)
        self._commands = {
            "help": [self.help, "Print information about available commands"],
            "list-workers": [self.list_workers, "List workers"],
            "list-files": [self.list_files, "List files"],
            "get-file": [self.get_file, "List chunks for a file and other info (specify file id as second arg)"],
            "list-downloadable": [self.list_downloadable, "List downloadable files"],
            "get-chunk": [self.get_chunk, "Get chunk info (specify chunk id as second arg)"],
            "reload-files": [self.reload_files, "Reload files (WARN: EXPERIMENTAL!)"],
            "quit": [self.quit, "Shutdown the server"]
        }

    def print(self, string):
        print(string)

    def start(self):
        self._thread.start()

    def main_thread(self):
        print("[INITIALISING MAIN CONSOLE]")
        while True:
            argv = input(">>> ").split(' ')
            command = argv[0]
            if (command in self._commands):
                try:
                    self._commands[command][0](argv)
                except Exception as e:
                    print(f"Error:\n{repr(e)}\n")
            else:
                print(f"Unknown command {command} - Use 'help' to list available commands")
            
    def help(self, argv):
        print("REGISTERED COMMANDS:")
        for command in self._commands:
            print(f"{command}\t-\t{self._commands[command][1]}")

    def reload_files(self, argv):
        state.load_state()

    def list_downloadable(self, argv):
        print("Downloadable Files:")
        self.dynamic_list(
            state.sorted_downloadable_files,
            displayFunction=lambda index, el: f"{index}. {el} ({state.files[el].get_path()})"
        )

    def list_workers(self, argv):
        print("Workers:")
        self.dynamic_list(
            list(state.workers.keys()),
            displayFunction=lambda index, worker_id: f"{index}. {worker_id}\t-\t{state.workers[worker_id].get_ip()} (joined {round(time.time() - state.workers[worker_id].get_joined(), 2)}s ago)")

    def list_files(self, argv):
        print("Files (path, complete):")
        self.dynamic_list(
            list(state.files.keys()),
            displayFunction=lambda index, file_id: f"{index}. {file_id}\t-\t{state.files[file_id].get_path()} ({state.files[file_id].get_complete()})")
    
    def get_file(self, argv):
        file_id = argv[1]
        file = state.files[file_id]
        print(f"File: {file.get_id()}")
        print(f"Path: {file.get_path()}")
        print(f"Size: {file.get_total_size()}")
        print(f"URL: {file.get_url()}")
        print(f"Chunks for {file_id}:")
        self.dynamic_list(
            file.get_chunks(),
            displayFunction=lambda index, chunk_id: f"{index}. {chunk_id}\t-\tWORKERS: ({state.chunks[chunk_id].get_worker_count()})")

    def get_chunk(self, argv):
        chunk = state.chunks[argv[1]]
        print(f"Chunk: {argv[1]}")
        print(f"Range: [{chunk.get_start()}, {chunk.get_end()}]")
        print("Workers:")
        for worker_id in chunk.get_workers():
            print(f"- {worker_id} U: {chunk.get_worker_uploaded(worker_id)}\tH: {chunk.get_worker_hash(worker_id)}\tC: {chunk.get_worker_complete(worker_id)}\tLU: {round(time.time() - chunk.get_worker_last_updated(worker_id), 2)}s ago")

    def dynamic_list(self, list: list[object], displayFunction: Callable[[int, object], str]):
        # @TODO: This is a bit broken but it functions fine
        list_index = 0
        list_size = 10
        while True:
            list_elements = list[list_index:list_index+list_size]
            i = 0
            for element in list_elements:
                print(displayFunction(list_index+i, element))
                i += 1
            print(f"({(list_index//list_size)+1}/{(len(list)//list_size)+1})")
            command = input("(U)p, (D)own, (M)ore, (L)ess, (Q)uit >>>").upper()
            if (command == "U"):
                list_index = max(0, list_index - list_size)
            elif (command == "D"):
                list_index = min(max(len(list)-list_size, 0), list_index + list_size)
            elif (command == "M"):
                list_size += 10
            elif (command == "L"):
                list_size = max(10, list_size - 10)
            elif (command == "Q"):
                break
            else:
                print("ERR: Command must be one of U, D, M, L or Q")

    def quit(self, argv):
        state.shutting_down = True
        print("Removing workers...")
        for worker_id in list(state.workers.keys()):
            state.remove_worker(worker_id)
        print("Closing database connection")
        db.close()
        print("Done!")
        print("Quitting!")
        os._exit(0)