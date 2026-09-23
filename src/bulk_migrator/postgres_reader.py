import logging
from typing import Generator
import psycopg2.extras
from src.common.config import get_pg_connection, BULK_CHUNK_SIZE

logger = logging.getLogger(__name__)

class PostgresChunkReader:
    def __init__(self, table_name: str, chunk_size: int = BULK_CHUNK_SIZE):
        self.table_name = table_name
        self.chunk_size = chunk_size

    def get_max_id(self) -> int:
        conn = get_pg_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT MAX(id) FROM {self.table_name}")
                result = cur.fetchone()
                return result[0] if result and result[0] is not None else 0
        finally:
            conn.close()

    def get_total_count(self) -> int:
        conn = get_pg_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                result = cur.fetchone()
                return result[0] if result else 0
        finally:
            conn.close()

    def read_chunk(self, start_id: int, end_id: int) -> list[dict]:
        conn = get_pg_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                query = f"SELECT * FROM {self.table_name} WHERE id >= %s AND id < %s ORDER BY id"
                cur.execute(query, (start_id, end_id))
                return cur.fetchall()
        finally:
            conn.close()

    def generate_chunks(self, resume_from_id: int = 0) -> Generator[tuple[int, int], None, None]:
        max_id = self.get_max_id()
        if max_id == 0:
            return
            
        current_id = resume_from_id
        while current_id <= max_id:
            next_id = current_id + self.chunk_size
            yield (current_id, next_id)
            current_id = next_id
