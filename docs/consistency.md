# Consistency Strategy

## The Race Condition Problem
During a live migration, the bulk loader and the CDC consumer operate concurrently. This creates a potential race condition.

Imagine this scenario:
1. Row ID 10 exists in PostgreSQL (Version A).
2. The load generator updates Row ID 10 to Version B.
3. The CDC consumer receives the UPDATE event for Version B and writes it to MongoDB.
4. The bulk loader (which started its query *before* the update but was slow to process) finally reads Version A from PostgreSQL and inserts it into MongoDB.

Result: MongoDB now holds the *older* Version A, and the update is lost.

## The Timestamp Guard Solution
To prevent this, every write to MongoDB (both from bulk loader and CDC consumer) includes a `_source_ts` field representing the timestamp of the data. 

For the bulk loader, this is the current timestamp at the time of the query. For the CDC consumer, it is the transaction timestamp (`ts_ms`) provided by Debezium.

We use MongoDB's `$lte` (less than or equal to) operator in the query criteria for updates. 

### Implementation Pattern
```python
# Try to update, but ONLY if the incoming data is newer than what's in MongoDB
result = db.collection.update_one(
    {
        "_id": doc_id, 
        "_source_ts": {"$lte": event_ts}
    },
    {"$set": updated_doc}
)

# If no document was matched, it means either:
# 1. The document doesn't exist yet
# 2. A newer version already exists
if result.matched_count == 0:
    try:
        # We attempt to insert it. If a newer version is already there, 
        # this will throw a DuplicateKeyError because of the _id constraint,
        # which is exactly what we want (we safely ignore it).
        db.collection.insert_one(updated_doc)
    except DuplicateKeyError:
        pass
```

## Why It Works
This ensures **Last Write Wins** semantics. Older data cannot overwrite newer data, regardless of the order in which the bulk loader and CDC consumer process the rows. It guarantees eventual consistency between PostgreSQL and MongoDB without requiring strict locking.
