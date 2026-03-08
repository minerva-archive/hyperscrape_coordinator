from contextlib import ContextDecorator
from typing import Self
from psycopg import AsyncConnection
import psycopg_pool

class DBStat():
    def __init__(self, key: str, value: str):
        self.key = key
        self.value = value

class DBFile():
    def __init__(self, id: str, path: str, size: int, url: str, chunk_size: int, complete: bool):
        self.id = id
        self.path = path
        self.size = size
        self.url = url
        self.chunk_size = chunk_size
        self.complete = complete

class DBChunk():
    def __init__(self, id: str, file_id: str, range_start: int, range_end: int):
        self.id = id
        self.file_id = file_id
        self.range_start = range_start
        self.range_end = range_end

class DBWorkerStatus():
    def __init__(self, chunk_id: str, worker_id: str, uploaded: int, hash: str|None, hash_only: bool):
        self.chunk_id = chunk_id
        self.worker_id = worker_id
        self.uploaded = uploaded
        self.hash = hash
        self.hash_only = hash_only

class DBFileHash():
    def __init__(self, file_id: str, md5: str, sha1: str, sha256: str):
        self.file_id = file_id
        self.md5 = md5
        self.sha1 = sha1
        self.sha256 = sha256

class DBLeaderboardItem():
    def __init__(self, discord_id: str, discord_username: str, avatar_url: str|None, downloaded_chunks: int, downloaded_bytes: int):
        self.discord_id = discord_id
        self.discord_username = discord_username
        self.avatar_url = avatar_url
        self.downloaded_chunks = downloaded_chunks
        self.downloaded_bytes = downloaded_bytes

class DBWorkerInfo():
    def __init__(self, id: str, ip: str):
        self.id = id
        self.ip = ip

class StateDB:
    """!
    @brief The state's database storage system
    """

    def __init__(self, conninfo: str):
        self._pool = psycopg_pool.AsyncConnectionPool(conninfo, open=False)

    async def open(self):
        await self._pool.open(wait=True)
        connection: AsyncConnection
        async with self._pool.connection() as connection:
            with open("./state_db_init.sql") as file:
                await connection.execute(file.read())

    def get_connection(self) -> StateDBConnection:
        return StateDBConnection(self)
    
    async def close(self):
        await self._pool.close()    

class StateDBConnection(ContextDecorator):
    def __init__(self, stateDB: StateDB):
        self.connection: AsyncConnection = None
        self.stateDB = stateDB

    async def __enter__(self) -> Self:
        self.connection = await self.stateDB._pool.connection()
        self.connection.execute("BEGIN")
        return self

    async def __exit__(self) -> Self:
        self.connection.execute("COMMIT")
        self.connection.close()
        return self
    
    # Misc for stats
    async def get_total_file_count(self) -> int:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT COUNT() as count FROM files")
            return await cur.fetchone()["count"]
        
    async def get_total_chunk_count(self) -> int:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT COUNT() as count FROM files")
            return await cur.fetchone()["count"]
    
    async def get_completed_file_count(self) -> int:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT COUNT() as count FROM files WHERE complete")
            return await cur.fetchone()["count"]
        
    async def get_completed_chunk_count(self) -> int:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT COUNT(DISTINCT chunk_id) as count FROM worker_status WHERE hash IS NOT NULL")
            return await cur.fetchone()["count"]
        
    async def get_assigned_chunk_count(self) -> int:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT COUNT(DISTINCT chunk_id) as count FROM worker_status")
            return await cur.fetchone()["count"]
        
    async def get_uploaded_bytes(self) -> int:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT SUM(uploaded) as sum FROM worker_status")
            return await cur.fetchone()["sum"]
        
    async def get_completed_bytes(self) -> int:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT SUM(size) as sum FROM file WHERE size IS NOT NULL AND complete")
            return await cur.fetchone()["sum"]
    
    async def get_total_bytes(self) -> int:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT SUM(size) as sum FROM file")
            return await cur.fetchone()["sum"]

    # Get objects
    async def get_stats(self) -> dict[str, DBStat]:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT * FROM stat")
            records = {}
            for record in cur:
                records[record["key"]] = DBStat(
                    record["key"],
                    record["value"]
                )
            return records
        
    # Get objects
    async def get_stat(self, key: str) -> DBStat:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT * FROM stat WHERE key = $1", (key,))
            record = await cur.fetchone()
            return DBStat(
                record["key"],
                record["value"]
            )

    async def get_file(self, file_id: str) -> DBFile:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT * FROM file WHERE id = $1", (file_id,))
            record = await cur.fetchone()
            return DBFile(
                record["id"],
                record["path"],
                record["size"],
                record["url"],
                record["chunk_size"],
                record["complete"]
            )
        
    async def get_chunk(self, chunk_id: str) -> DBChunk:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT * FROM chunk WHERE id = $1", (chunk_id,))
            record = await cur.fetchone()
            return DBChunk(
                record["id"],
                record["file_id"],
                record["range_start"],
                record["range_end"]
            )

    async def get_chunks_for_file(self, file_id: str) -> list[DBChunk]:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT * FROM chunk WHERE file_id = $1", (file_id,))
            records: list[DBChunk] = []
            for record in cur:
                records.append(DBChunk(
                    record["id"],
                    record["file_id"],
                    record["range_start"],
                    record["range_end"]
                ))
            return records

    async def get_chunk_worker_status(self, chunk_id: str) -> list[DBWorkerStatus]:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT * FROM worker_status WHERE chunk_id = $1", (chunk_id,))
            records: list[DBWorkerStatus] = []
            for record in cur:
                records.append(DBWorkerStatus(
                    record["chunk_id"],
                    record["worker_id"],
                    record["uploaded"],
                    record["hash"],
                    record["hash_only"]
                ))
            return records
        
    async def get_worker_status(self, chunk_id: str, worker_id: str) -> DBWorkerStatus:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT * FROM worker_status WHERE chunk_id = $1 AND worker_id = $2", (chunk_id, worker_id,))
            record = await cur.fetchone()
            return DBWorkerStatus(
                record["chunk_id"],
                record["worker_id"],
                record["uploaded"],
                record["hash"],
                record["hash_only"]
            )

    async def get_leaderboard(self) -> list[DBLeaderboardItem]:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT * FROM leaderboard ORDER BY downloaded_bytes DESC")
            records: list[DBLeaderboardItem] = []
            for record in cur:
                records.append(DBLeaderboardItem(
                    record["discord_id"],
                    record["discord_username"],
                    record["avatar_url"],
                    record["downloaded_chunks"],
                    record["downloaded_bytes"]
                ))
            return records
        
    async def get_leaderboard_item(self, discord_id: str) -> DBLeaderboardItem:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT * FROM leaderboard WHERE discord_id = $1", (discord_id, ))
            return await cur.fetchone()
        
    async def get_chunk_and_file_and_current_status(self, chunk_id: str, worker_id: str):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "SELECT chunk.id AS chunk_id, chunk.range_start AS chunk_range_start, chunk.range_end AS chunk_range_end "
                "file.id AS file_id, file.path AS file_path, file.size AS file_size, file.url AS file_url, file.chunk_size AS file_chunk_size, file.complete AS file.complete "
                "worker_status.worker_id AS worker_status_id, worker_status.uploaded AS worker_status_uploaded, worker_status.hash AS worker_status_hash, worker_status.hash_only AS worker_status_hash_only "
                "FROM chunk "
                "JOIN file ON file.id=chunk.file_id "
                "JOIN worker_status ON worker_status.chunk_id=chunk.id "
                "WHERE chunk.id = $1 AND worker_status.worker+id = $2",
                (chunk_id, worker_id)
            )
            record = await cur.fetchone()
            if (record == None):
                return (None, None, None)
            
            return (
                DBChunk(
                    record["file_id"],
                    record["chunk_file_id"],
                    record["chunk_range_start"],
                    record["chunk_range_end"]
                ),
                DBFile(
                    record["file_id"],
                    record["file_path"],
                    record["file_size"],
                    record["file_url"],
                    record["file_chunk_size"],
                    record["file_complete"]
                ),
                DBWorkerStatus(
                    record["worker_status_chunk_id"],
                    record["worker_status_worker_id"],
                    record["worker_status_uploaded"],
                    record["worker_status_hash"],
                    record["worker_status_hash_only"]
                )
            )
            
        
    # Worker handling
    async def remove_worker(self, worker_id: str):
        async with self.connection.cursor() as cur:
            await cur.execute("DELETE FROM worker_info WHERE id = $1", (worker_id,))
            await cur.execute("DELETE FROM worker_status WHERE hash IS NULL AND worker_id = $1", (worker_id, ))
        
    # Ordered getter
    async def get_ordered_downloadable_files(self, limit: int, offset: int, worker_limit: int, current_worker_id: str, current_worker_ip: str, free_space: int):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "SELECT * FROM ("
                    "SELECT file.id as file_id, file.size as file_size, file.url as file_url, " # Select file properties
                    "chunk.id as chunk_id, chunk.range_start as chunk_range_start, chunk.range_end as chunk_range_end " # Select chunk properties
                    "COUNT(worker_id) as assigned_workers " # Count assigned workers
                    "FROM file " # FROM file
                    "JOIN chunk ON chunk.file_id=file.id " # Join chunks to files
                    "JOIN worker_status ON worker_status.chunk_id=chunk.id " # Join worker statuses to files
                    "JOIN worker_info ON worker_info.id=worker_status.worker_id " # Join worker info (for IP filtering)
                    "WHERE (NOT file.complete) " # Only in-progress files
                    "AND (file.size IS NOT NULL AND file.size != 0 AND file.size < $6) " # Only with a defined size that is less than free space
                    "AND worker_status.worker_id != $4 AND worker_info.ip != $5 " # Where the worker isn't us
                    "GROUP BY chunk.id ORDER BY COUNT(worker_id)" # Get the chunks from most workers to least
                ") "
                "JOIN (SELECT 1 WHERE EXISTS (SELECT 1 FROM worker_status WHERE worker_status.chunk_id=chunk_id AND NOT hash_only)) as hash_only "
                "WHERE assigned_workers < $3 " # Get only with workers less than the limit
                "LIMIT $1 OFFSET $2", # So we can "stream" it
                (limit, offset, worker_limit, current_worker_id, current_worker_ip, free_space)
            )
            return await cur.fetchall()
        
    # Leaderboard mutation
    async def add_to_leaderboard(self, discord_id: str, discord_username: str, avatar_url: str):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "INSERT INTO leaderboard (discord_id, discord_username, avatar_url) "
                "VALUES ($1, $2, $3) "
                "ON CONFLICT DO UPDATE discord_username=$2, avatar_url=$3",
                (discord_id, discord_username, avatar_url)
            )
        
    # Stat mutations
    async def set_stat(self, key: str, value: int):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "INSERT INTO stat (key, value) "
                "VALUES ($1, $2) "
                "ON CONFLICT DO UPDATE key=$1, value=$2",
                (key, value)
            )

    async def change_stat(self, key: str, value_change: int):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "UPDATE stat SET value = value + $2 WHERE key = $1",
                (key, value_change)
            )

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
    async def insert_chunk(self, chunk_id: str, file_id: str, range_start: int, range_end: int):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "INSERT INTO chunk (id, file_id, range_start, range_end) "
                "VALUES ($1, $2, $3, $4)",
                (chunk_id, file_id, range_start, range_end)
            )

    async def delete_chunk(self, chunk_id: str):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "DELETE FROM chunk WHERE id = $1",
                (chunk_id,)
            )

    async def insert_worker_status(self, chunk_id: str, worker_id: str, uploaded: int = 0, hash: str|None = None, hash_only: bool = True):
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

    async def delete_chunk_worker_statuses(self, chunk_id: str):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "DELETE FROM worker_status WHERE chunk_id = $1",
                (chunk_id,)
            )

    # Worker status mutations
    async def change_worker_status_uploaded(self, chunk_id: str, worker_id: str, uploaded_change: int):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "UPDATE worker_status SET uploaded = uploaded + $1 WHERE chunk_id = $2 AND worker_id = $3",
                (uploaded_change, chunk_id, worker_id)
            )

    async def set_worker_status_uploaded(self, chunk_id: str, worker_id: str, uploaded: int):
        async with self.connection.cursor() as cur:
            await cur.execute(
                "UPDATE worker_status SET uploaded = uploaded + $1 WHERE chunk_id = $2 AND worker_id = $3",
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
                "VALUES ($1, $2, $3, $4) "
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
