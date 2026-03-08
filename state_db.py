from psycopg import AsyncConnection
import psycopg_pool

class StateDB:
    """!
    @brief The state's database storage system
    """

    def __init__(self, conninfo: str):
        self._pool = psycopg_pool.AsyncConnectionPool(conninfo)

    async def get_connection(self):
        return await self._pool.connection()
    
    async def close(self):
        await self._pool.close()    

class StateDBConnection:
    def __init__(self, stateDB: StateDB):
        self.connection: AsyncConnection = None
        self.stateDB = stateDB

    async def __enter__(self):
        self.connection = await self.stateDB.get_connection()

    async def __exit__(self):
        self.connection.close()

    # Get objects
    async def get_files(self) -> list[dict]:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT * FROM file")
            return await cur.fetchall()
        
    async def get_file(self, file_id: str) -> dict:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT * FROM file WHERE id = $1", (file_id,))
            return await cur.fetchone()

    async def get_chunks(self) -> list[dict]:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT * FROM chunk")
            return await cur.fetchall()
        
    async def get_chunk(self, chunk_id: str) -> dict:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT * FROM chunk WHERE id = $1", (chunk_id,))
            return await cur.fetchone()

    async def get_chunks_for_file(self, file_id: str) -> list[dict]:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT * FROM chunk WHERE file_id = $1", (file_id,))
            return await cur.fetchall()

    async def get_chunk_worker_status(self, chunk_id: str) -> list[dict]:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT * FROM worker_status WHERE chunk_id = $1", (chunk_id,))
            return await cur.fetchall()

    async def get_file_hashes(self):
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT path, md5, sha1, sha256 FROM file_hash JOIN file on file.id = file_hash.file_id")
            return await cur.fetchall()

    async def get_leaderboard(self):
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT * FROM leaderboard ORDER BY downloaded_bytes DESC")
            return await cur.fetchall()

    # File mutations
    async def insert_file(self, file_id: str, path: str, size: int, url: str, chunk_size: int, complete: bool = False):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "INSERT INTO file (id, path, size, url, chunk_size, complete) "
                "VALUES ($1, $2, $3, $4, $5, $6)",
                (file_id, path, size, url, chunk_size, complete)
            )

    async def set_file_size(self, file_id: str, size: int):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "UPDATE file SET size = $1 WHERE id = $2",
                (size, file_id)
            )

    async def set_file_chunk_size(self, file_id: str, chunk_size: int):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "UPDATE file SET chunk_size = $1 WHERE id = $2",
                (chunk_size, file_id)
            )

    async def set_file_complete(self, file_id: str):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "UPDATE file SET complete = TRUE WHERE id = $1",
                (file_id,)
            )


    # Chunk mutations
    async def insert_chunk(self, chunk_id: str, file_id: str, start: int, end: int):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "INSERT INTO chunk (id, file_id, start, end) "
                "VALUES ($1, $2, $3, $4)",
                (chunk_id, file_id, start, end)
            )

    async def delete_chunk(self, chunk_id: str):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "DELETE FROM chunk WHERE id = $1",
                (chunk_id,)
            )

    async def insert_worker_status(self, chunk_id: str, worker_id: str, uploaded: int = 0, hash: str = "", hash_only: bool = True):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "INSERT OR REPLACE INTO worker_status (chunk_id, worker_id, uploaded, hash, hash_only) "
                "VALUES ($1, $2, $3, $4, $5)",
                (chunk_id, worker_id, uploaded, hash, hash_only)
            )

    async def delete_worker_status(self, chunk_id: str, worker_id: str):
        async with self.connection.cursor() as cur:
            await cur.execute(
            "DELETE FROM worker_status WHERE chunk_id = $1 AND worker_id = $2",
            (chunk_id, worker_id)
        )

    async def delete_chunk_worker_status(self, chunk_id: str):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "DELETE FROM worker_status WHERE chunk_id = $1",
                (chunk_id,)
            )

    # Worker status mutations
    async def set_worker_status_uploaded(self, chunk_id: str, worker_id: str, uploaded: int):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "UPDATE worker_status SET uploaded = $1 WHERE chunk_id = $2 AND worker_id = $3",
                (uploaded, chunk_id, worker_id)
            )

    async def set_worker_status_hash(self, chunk_id: str, worker_id: str, hash: str):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "UPDATE worker_status SET hash = $1 WHERE chunk_id = $2 AND worker_id = $3",
                (hash, chunk_id, worker_id)
            )

    async def set_worker_status_hash_only(self, chunk_id: str, worker_id: str, hash_only: bool):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "UPDATE worker_status SET hash_only = $1 WHERE chunk_id = $2 AND worker_id = $3",
                (hash_only, chunk_id, worker_id)
            )


    # File hash mutations
    async def insert_file_hash(self, file_id: str, md5: str, sha1: str, sha256: str):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "INSERT INTO file_hash (file_id, md5, sha1, sha256) "
                "VALUES ($1, $2, $3, $4)"
                "ON CONFLICT DO UPDATE SET file_id=$1, md5=$2, sha1=$3, sha256=$4",
                (file_id, md5, sha1, sha256)
            )

    # leaderboard mutations

    async def insert_leaderboard_entry(self,
                                 discord_id: str,
                                 discord_username: str,
                                 avatar_url: str,
                                 downloaded_chunks: int = 0,
                                 downloaded_bytes: int = 0):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "INSERT INTO leaderboard (discord_id, discord_username, avatar_url, downloaded_chunks, downloaded_bytes) "
                "VALUES ($1, $2, $3, $4, $5) "
                "ON CONFLICT (discord_id) DO NOTHING",
                (discord_id, discord_username, avatar_url, downloaded_chunks, downloaded_bytes)
            )

    async def update_leaderboard_downloaded_bytes(self, discord_id: str, change: int):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "UPDATE leaderboard SET downloaded_bytes = downloaded_bytes + $1 WHERE discord_id = $2",
                (change, discord_id)
            )

    async def update_leaderboard_downloaded_chunks(self, discord_id: str, change: int):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "UPDATE leaderboard SET downloaded_chunks = downloaded_chunks + $1 WHERE discord_id = $2",
                (change, discord_id)
            )
