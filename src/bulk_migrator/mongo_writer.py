import logging
from pymongo.errors import BulkWriteError, DuplicateKeyError
from src.common.config import get_mongo_db, convert_pg_row, get_source_ts

logger = logging.getLogger(__name__)

class MongoWriter:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.db = get_mongo_db()
        self.collection = self.db[table_name]

    def write_batch(self, records: list) -> int:
        if not records:
            return 0
        
        docs = []
        for record in records:
            # Need to create a mutable copy to manipulate
            record_dict = dict(record)
            doc = convert_pg_row(record_dict)
            doc_id = doc.pop('id')
            source_ts = get_source_ts(doc, self.table_name)
            docs.append({"_id": doc_id, **doc, "_source_ts": source_ts})
        
        written = 0
        try:
            result = self.collection.insert_many(docs, ordered=False)
            written = len(result.inserted_ids)
        except BulkWriteError as bwe:
            written = bwe.details.get('nInserted', 0)
            # Handle duplicates with conditional update
            for error in bwe.details.get('writeErrors', []):
                if error.get('code') == 11000:  # DuplicateKeyError
                    dup_doc = docs[error['index']]
                    doc_id = dup_doc.pop('_id')
                    source_ts = dup_doc['_source_ts']
                    result = self.collection.update_one(
                        {"_id": doc_id, "_source_ts": {"$lte": source_ts}},
                        {"$set": dup_doc}
                    )
                    if result.modified_count > 0:
                        written += 1
                    dup_doc['_id'] = doc_id  # restore id for potential future use in loop
        
        return written

    def get_count(self) -> int:
        return self.collection.count_documents({})

    def close(self):
        # We generally rely on the globally cached mongo client for connection pooling,
        # but implementing close interface for completeness.
        pass
