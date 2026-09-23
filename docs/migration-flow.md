# Migration Timeline

```text
T0 ─── Verify PostgreSQL ready
       Create Debezium connector (replication slot)
       CDC capture begins → events buffer in Kafka

T1 ─── Start source-load-generator (continuous traffic)
       Start bulk migration (parallel workers)

T1→T2  PostgreSQL: continuous INSERT/UPDATE/DELETE
       Debezium: WAL → Kafka (buffered)
       Bulk: PostgreSQL → MongoDB (chunks + checkpoint)

T2 ─── Bulk migration complete
       Start CDC consumer

T2→T3  CDC consumer processes buffered events
       Load generator still running

T3 ─── Consumer lag → 0

T4 ─── Stop load generator
       Final CDC catch-up

T5 ─── Run validation (7-point check)

T6 ─── Migration complete, produce report
```

## Detailed Phases

### T0: Preparation and Capture Start
Before any data movement begins, we verify the infrastructure and register the Debezium connector. Debezium instantly creates a logical replication slot in PostgreSQL. This is critical: from this moment on, *every single change* is captured and buffered in Kafka, ensuring zero data loss.

### T1: The Bulk Load Phase
We start the simulated application traffic (`load_generator`) to prove that migration happens seamlessly without downtime. Simultaneously, we trigger the bulk migrator. It pulls data in chunks and uses multiple workers to maximize throughput. 

### T1 -> T2: The Overlap
During this time, bulk migration is reading older rows while new transactions are happening. Some rows might be updated right after they were bulk-copied. The changes are sitting safely in Kafka waiting to be processed.

### T2: Catch Up Phase
Bulk migration finishes. We start the CDC consumer. The consumer reads the buffered events from Kafka and replays them against MongoDB.

### T3: Synchronization
The CDC consumer eventually catches up to real-time. It is now processing new events as fast as the load generator creates them. The consumer lag stays near zero.

### T4: The Cutover
In a real-world scenario, this is when you take the source system offline (read-only) for a brief moment. We stop the load generator, let the CDC consumer drain the very last few events, and then we know both databases are perfectly in sync.

### T5: Validation
The validation script performs deep checks to ensure absolute data parity between PostgreSQL and MongoDB.

### T6: Completion
The migration is verified, and the new application can be pointed to MongoDB.
