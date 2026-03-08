from contextlib import ContextDecorator
from datetime import datetime
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
    def __init__(self, chunk_id: str, worker_id: str, uploaded: int, hash: str|None, hash_only: bool, last_updated: datetime):
        self.chunk_id = chunk_id
        self.worker_id = worker_id
        self.uploaded = uploaded
        self.hash = hash
        self.hash_only = hash_only
        self.last_updated = last_updated

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

    async def __aenter__(self) -> Self:
        self.connection = await self.stateDB._pool.getconn()
        self._cursor = await self.connection.execute("BEGIN")
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> Self:
        await self._cursor.execute("COMMIT")
        await self.stateDB._pool.putconn(self.connection)
        return False
    
    # Misc for stats
    async def get_total_file_count(self) -> int:
        await self._cursor.execute("SELECT COUNT(*) as count FROM file")
        return int((await self._cursor.fetchone())[0])
        
    async def get_total_chunk_count(self) -> int:
        await self._cursor.execute("SELECT COUNT(*) as count FROM file")
        return int((await self._cursor.fetchone())[0])
    
    async def get_completed_file_count(self) -> int:
        await self._cursor.execute("SELECT COUNT(*) as count FROM file WHERE complete")
        return int((await self._cursor.fetchone())[0])
    
    async def get_total_workers(self) -> int:
        await self._cursor.execute("SELECT COUNT(*) as count FROM worker_info")
        return int((await self._cursor.fetchone())[0])
        
    async def get_completed_chunk_count(self) -> int:
        await self._cursor.execute("SELECT COUNT(DISTINCT chunk_id) as count FROM worker_status WHERE hash IS NOT NULL")
        return int((await self._cursor.fetchone())[0])
        
    async def get_assigned_chunk_count(self) -> int:
        await self._cursor.execute("SELECT COUNT(DISTINCT chunk_id) as count FROM worker_status")
        return int((await self._cursor.fetchone())[0])
        
    async def get_uploaded_bytes(self) -> int:
        await self._cursor.execute("SELECT SUM(uploaded) as sum FROM worker_status")
        result = (await self._cursor.fetchone())[0]
        if (result == None):
            result = 0
        return int(result)
        
    async def get_completed_bytes(self) -> int:
        await self._cursor.execute("SELECT SUM(size) as sum FROM file WHERE size IS NOT NULL AND complete")
        return int((await self._cursor.fetchone())[0])
    
    async def get_total_bytes(self) -> int:
        await self._cursor.execute("SELECT SUM(size) as sum FROM file")
        return int((await self._cursor.fetchone())[0])

    # Get objects
    async def get_stats(self) -> dict[str, DBStat]:
        records = {}
        async for record in self._cursor.stream(
            "SELECT key, value FROM stat"
        ):
            records[record[0]] = DBStat(
                record[0],
                record[1]
            )
        return records
        
    # Get objects
    async def get_stat(self, key: str) -> DBStat:
        await self._cursor.execute("SELECT key, value FROM stat WHERE key = %s", (key,))
        record = await self._cursor.fetchone()
        return DBStat(
            record[0],
            record[1]
        )

    async def get_file(self, file_id: str) -> DBFile:
        await self._cursor.execute("SELECT id, path, size, url, chunk_size, complete FROM file WHERE id = %s", (file_id,))
        record = await self._cursor.fetchone()
        return DBFile(
            record[0],
            record[1],
            int(record[2]),
            record[3],
            int(record[4]),
            record[5]
        )
        
    async def get_chunk(self, chunk_id: str) -> DBChunk:
        await self._cursor.execute("SELECT id, file_id, range_start, range_end FROM chunk WHERE id = %s", (chunk_id,))
        record = await self._cursor.fetchone()
        return DBChunk(
            record[0],
            record[1],
            int(record[2]),
            int(record[3])
        )

    async def get_chunks_for_file(self, file_id: str) -> list[DBChunk]:
        records: list[DBChunk] = []
        async for record in self._cursor.stream(
            "SELECT id, file_id, range_start, range_end FROM chunk WHERE file_id = %s",
            (file_id,)
        ):
            records.append(DBChunk(
                record[0],
                record[1],
                int(record[2]),
                int(record[3])
            ))
        return records

    async def get_chunk_worker_status(self, chunk_id: str) -> list[DBWorkerStatus]:
        records: list[DBWorkerStatus] = []
        async for record in self._cursor.stream(
            "SELECT chunk_id, worker_id, uploaded, hash, hash_only, last_updated FROM worker_status WHERE chunk_id = %s",
            (chunk_id,)
        ):
            records.append(DBWorkerStatus(
                record[0],
                record[1],
                int(record[2]),
                record[3],
                record[4],
                records[5]
            ))
        return records
        
    async def get_worker_status(self, chunk_id: str, worker_id: str) -> DBWorkerStatus:
        await self._cursor.execute("SELECT chunk_id, worker_id, uploaded, hash, hash_only, last_updated FROM worker_status WHERE chunk_id = %s AND worker_id = %s", (chunk_id, worker_id,))
        record = await self._cursor.fetchone()
        return DBWorkerStatus(
            record[0],
            record[1],
            int(record[2]),
            record[3],
            record[4],
            record[5]
        )

    async def get_leaderboard(self) -> list[DBLeaderboardItem]:
        records: list[DBLeaderboardItem] = []
        async for record in self._cursor.stream(
            "SELECT discord_id, discord_username, avatar_url, downloaded_chunks, downloaded_bytes FROM leaderboard ORDER BY downloaded_bytes DESC"
        ):
            records.append(DBLeaderboardItem(
                record[0],
                record[1],
                record[2],
                int(record[3]),
                int(record[4])
            ))
        return records
        
    async def get_leaderboard_item(self, discord_id: str) -> DBLeaderboardItem:
        await self._cursor.execute("SELECT discord_id, discord_username, avatar_url, downloaded_chunks, downloaded_bytes FROM leaderboard WHERE discord_id = %s", (discord_id, ))
        record = self._cursor.fetchone()        
        return DBLeaderboardItem(
            record[0],
            record[1],
            record[2],
            int(record[3]),
            int(record[4])
        )
        
    async def get_chunk_and_file_and_current_status(self, chunk_id: str, worker_id: str):
        await self._cursor.execute(
            "SELECT chunk.id, chunk.range_start, chunk.range_end "
            "file.id, file.path, file.size, file.url, file.chunk_size, file.complete "
            "worker_status.worker_id, worker_status.uploaded, worker_status.hash, worker_status.hash_only, worker_status.last_updated "
            "FROM chunk "
            "JOIN file ON file.id=chunk.file_id "
            "JOIN worker_status ON worker_status.chunk_id=chunk.id "
            "WHERE chunk.id = %s AND worker_status.worker_id = %s",
            (chunk_id, worker_id)
        )
        record = await self._cursor.fetchone()
        if (record == None):
            return (None, None, None)
        
        return (
            DBChunk(
                record[0],
                record[3],
                int(record[1]),
                int(record[2])
            ),
            DBFile(
                record[3],
                record[4],
                int(record[5]),
                record[6],
                int(record[7]),
                record[8]
            ),
            DBWorkerStatus(
                record[0],
                record[9],
                int(record[10]),
                record[11],
                record[12],
                record[13]
            )
        )
            
        
    # Worker handling
    async def add_worker(self, id: str, discord_id: str, ip: str):
        await self._cursor.execute(
            "INSERT INTO worker_info (id, discord_id, ip) "
            "VALUES (%s, %s, %s)",
            (id, discord_id, ip)
        )

    async def remove_worker(self, worker_id: str):
        await self._cursor.execute("DELETE FROM worker_info WHERE id = %s", (worker_id,))
        await self._cursor.execute("DELETE FROM worker_status WHERE hash IS NULL AND worker_id = %s", (worker_id, ))
        
    # Ordered getter
    async def get_ordered_downloadable_files(self, limit: int, offset: int, worker_limit: int, current_worker_id: str, current_worker_ip: str, free_space: int):
        await self._cursor.execute(
            "SELECT * FROM ("
                "SELECT file.id, file.size, file.url, " # Select file properties
                "chunk.id, chunk.range_start, chunk.range_end " # Select chunk properties
                "COUNT(worker_id) " # Count assigned workers
                "FROM file " # FROM file
                "JOIN chunk ON chunk.file_id=file.id " # Join chunks to files
                "JOIN worker_status ON worker_status.chunk_id=chunk.id " # Join worker statuses to files
                "JOIN worker_info ON worker_info.id=worker_status.worker_id " # Join worker info (for IP filtering)
                "WHERE (NOT file.complete) " # Only in-progress files
                "AND (file.size IS NOT NULL AND file.size != 0 AND file.size < %s) " # Only with a defined size that is less than free space
                "AND worker_status.worker_id != %s AND worker_info.ip != %s " # Where the worker isn't us
                "GROUP BY chunk.id ORDER BY COUNT(worker_id)" # Get the chunks from most workers to least
            ") "
            "JOIN (SELECT 1 WHERE EXISTS (SELECT 1 FROM worker_status WHERE worker_status.chunk_id=chunk_id AND NOT hash_only)) as hash_only "
            "WHERE assigned_workers < %s " # Get only with workers less than the limit
            "LIMIT %s OFFSET %s", # So we can "stream" it
            (free_space, current_worker_id, current_worker_ip, worker_limit, limit, offset)
        )
        return await self._cursor.fetchall()
        
    # Stat mutations
    async def set_stat(self, key: str, value: int):
        await self._cursor.execute(
            "INSERT INTO stat (key, value) "
            "VALUES (%s, %s) "
            "ON CONFLICT (discord_id) DO UPDATE key=%s, value=%s",
            (key, value, key, value)
        )

    async def change_stat(self, key: str, value_change: int):
        await self._cursor.execute(
            "UPDATE stat SET value = value + %s WHERE key = %s",
            (value_change, key)
        )

    # File mutations
    async def insert_file(self, file_id: str, path: str, size: int, url: str, chunk_size: int, complete: bool = False):
        await self._cursor.execute(
            "INSERT INTO file (id, path, size, url, chunk_size, complete) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (file_id, path, size, url, chunk_size, complete)
        )

    async def set_file_size(self, file_id: str, size: int):
        await self._cursor.execute(
            "UPDATE file SET size = %s WHERE id = %s",
            (size, file_id)
        )

    async def set_file_chunk_size(self, file_id: str, chunk_size: int):
        await self._cursor.execute(
            "UPDATE file SET chunk_size = %s WHERE id = %s",
            (chunk_size, file_id)
        )

    async def set_file_complete(self, file_id: str):
        await self._cursor.execute(
            "UPDATE file SET complete = TRUE WHERE id = %s",
            (file_id,)
        )


    # Chunk mutations
    async def insert_chunk(self, chunk_id: str, file_id: str, range_start: int, range_end: int):
        await self._cursor.execute(
            "INSERT INTO chunk (id, file_id, range_start, range_end) "
            "VALUES (%s, %s, %s, %s)",
            (chunk_id, file_id, range_start, range_end)
        )

    async def delete_chunk(self, chunk_id: str):
        await self._cursor.execute(
            "DELETE FROM chunk WHERE id = %s",
            (chunk_id,)
        )

    async def insert_worker_status(self, chunk_id: str, worker_id: str, uploaded: int = 0, hash: str|None = None, hash_only: bool = True):
        await self._cursor.execute(
            "INSERT OR REPLACE INTO worker_status (chunk_id, worker_id, uploaded, hash, hash_only, last_updated) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (chunk_id, worker_id, uploaded, hash, hash_only, datetime.now())
        )

    async def delete_worker_status(self, chunk_id: str, worker_id: str):
        await self._cursor.execute(
        "DELETE FROM worker_status WHERE chunk_id = %s AND worker_id = %s",
        (chunk_id, worker_id)
    )

    async def delete_chunk_worker_statuses(self, chunk_id: str):
        await self._cursor.execute(
            "DELETE FROM worker_status WHERE chunk_id = $1",
            (chunk_id,)
        )

    # Worker status mutations
    async def change_worker_status_uploaded(self, chunk_id: str, worker_id: str, uploaded_change: int):
        await self._cursor.execute(
            "UPDATE worker_status SET uploaded = uploaded + %s WHERE chunk_id = %s AND worker_id = %s",
            (uploaded_change, chunk_id, worker_id)
        )

    async def set_worker_status_uploaded(self, chunk_id: str, worker_id: str, uploaded: int):
        await self._cursor.execute(
            "UPDATE worker_status SET uploaded = uploaded + %s WHERE chunk_id = %s AND worker_id = %s",
            (uploaded, chunk_id, worker_id)
        )

    async def set_worker_status_hash(self, chunk_id: str, worker_id: str, hash: str):
        await self._cursor.execute(
            "UPDATE worker_status SET hash = %s WHERE chunk_id = %s AND worker_id = %s",
            (hash, chunk_id, worker_id)
        )

    async def set_worker_status_hash_only(self, chunk_id: str, worker_id: str, hash_only: bool):
        await self._cursor.execute(
            "UPDATE worker_status SET hash_only = %s WHERE chunk_id = %s AND worker_id = %s",
            (hash_only, chunk_id, worker_id)
        )

    async def update_worker_status_last_updated(self, chunk_id: str, worker_id: str):
        await self._cursor.execute(
            "UPDATE worker_status SET last_updated = %s WHERE chunk_id = %s AND worker_id = %s",
            (datetime.now(), chunk_id, worker_id)
        )


    # File hash mutations
    async def insert_file_hash(self, file_id: str, md5: str, sha1: str, sha256: str):
        await self._cursor.execute(
            "INSERT INTO file_hash (file_id, md5, sha1, sha256) "
            "VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (file_id) DO UPDATE SET md5=%s, sha1=%s, sha256=%s",
            (file_id, md5, sha1, sha256, md5, sha1, sha256)
        )

    # leaderboard mutations

    async def insert_leaderboard_entry(self,
                                 discord_id: str,
                                 discord_username: str,
                                 avatar_url: str,
                                 downloaded_chunks: int = 0,
                                 downloaded_bytes: int = 0):
        await self._cursor.execute(
            "INSERT INTO leaderboard (discord_id, discord_username, avatar_url, downloaded_chunks, downloaded_bytes) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (discord_id) DO NOTHING",
            (discord_id, discord_username, avatar_url, downloaded_chunks, downloaded_bytes)
        )

    async def update_leaderboard_downloaded_bytes(self, discord_id: str, change: int):
        await self._cursor.execute(
            "UPDATE leaderboard SET downloaded_bytes = downloaded_bytes + %s WHERE discord_id = %s",
            (change, discord_id)
        )

    async def update_leaderboard_downloaded_chunks(self, discord_id: str, change: int):
        await self._cursor.execute(
            "UPDATE leaderboard SET downloaded_chunks = downloaded_chunks + %s WHERE discord_id = %s",
            (change, discord_id)
        )
