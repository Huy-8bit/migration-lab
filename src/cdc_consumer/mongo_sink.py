import logging
from datetime import datetime, timezone
from pymongo.errors import DuplicateKeyError
from src.common.config import get_mongo_db, TABLES_WITH_UPDATED_AT, TABLE_ORDER

logger = logging.getLogger(__name__)

class MongoSink:
    def __init__(self):
        self.db = get_mongo_db()

    def _convert_debezium_row(self, row: dict) -> dict:
        doc = {}
        for k, v in row.items():
            if v is None:
                doc[k] = None
            elif isinstance(v, str) and "." in v and v.replace(".", "", 1).isdigit():
                try:
                    doc[k] = float(v)
                except ValueError:
                    doc[k] = v
            # Some ints/strings to handle or if timestamp as microsecond
            elif isinstance(v, int) and (k.endswith('_at')):
                # Microseconds to ISO string
                try:
                    dt = datetime.fromtimestamp(v / 1_000_000.0, tz=timezone.utc)
                    doc[k] = dt.isoformat()
                except Exception:
                    doc[k] = v
            else:
                doc[k] = v
        return doc

    def _get_event_ts(self, event: dict, table_name: str) -> str:
        after = event.get('after') or {}
        source = event.get('source') or {}
        
        source_ts_ms = source.get('ts_ms')
        
        if table_name in TABLES_WITH_UPDATED_AT:
            ts_val = after.get('updated_at')
        else:
            ts_val = after.get('created_at')
            
        if ts_val:
            if isinstance(ts_val, int):
                dt = datetime.fromtimestamp(ts_val / 1_000_000.0, tz=timezone.utc)
                return dt.isoformat()
            elif isinstance(ts_val, str):
                return ts_val

        if source_ts_ms:
            dt = datetime.fromtimestamp(source_ts_ms / 1000.0, tz=timezone.utc)
            return dt.isoformat()
            
        return datetime.now(timezone.utc).isoformat()

    def apply_event(self, table_name: str, event: dict) -> bool:
        collection = self.db[table_name]
        op = event.get('op')
        
        if op in ('c', 'u', 'r'):  # create, update, read(snapshot)
            after = event.get('after')
            if after is None:
                return False
            
            doc = self._convert_debezium_row(after)
            doc_id = doc.pop('id')
            source_ts = self._get_event_ts(event, table_name)
            
            # Conditional upsert: only write if incoming is newer
            result = collection.update_one(
                {"_id": doc_id, "_source_ts": {"$lte": source_ts}},
                {"$set": {**doc, "_source_ts": source_ts}}
            )
            if result.matched_count == 0:
                try:
                    collection.insert_one({"_id": doc_id, **doc, "_source_ts": source_ts})
                except DuplicateKeyError:
                    pass  # Newer version exists
            return True
            
        elif op == 'd':  # delete
            before = event.get('before')
            if before is None:
                return False
            doc_id = before.get('id')
            collection.delete_one({"_id": doc_id})
            return True
        
        return False

    def apply_batch(self, events: list[tuple[str, dict]]) -> int:
        count = 0
        for table_name, event in events:
            try:
                if self.apply_event(table_name, event):
                    count += 1
            except Exception as e:
                logger.error(f"Error applying event to {table_name}: {e}")
        return count

    def get_collection_count(self, table_name: str) -> int:
        return self.db[table_name].count_documents({})

    def close(self):
        self.db.client.close()
