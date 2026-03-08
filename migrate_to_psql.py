import asyncio
import state
import sqlite3

async def main():
    print("Initialising local state...")
    await state.initialise()
    print("Initialised state!")

asyncio.run(main())