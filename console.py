import signal
from threading import Thread
from typing import Callable
import asyncio
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

    def list_workers(self, argv):
        print("Workers:")
        self.dynamic_list(
            list(state.local_workers.keys()),
            displayFunction=lambda index, worker_id: f"{index}. {worker_id}\t-\t{state.local_workers[worker_id].get_ip()} (joined {round(time.time() - state.local_workers[worker_id].get_joined(), 2)}s ago)")

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
            asyncio.run(state.remove_worker(worker_id))
        print("Closing database connection")
        state.db.close()
        print("Quit!")
        os.kill(os.getpid(), signal.SIGINT)
