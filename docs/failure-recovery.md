# Failure Recovery and Resilience

A production migration pipeline must be resilient to infrastructure and application failures. This system is designed to recover automatically from crashes without manual intervention or data corruption.

## 1. Bulk Migrator Failure

### Scenario: Container Crash or OOM Kill
If the `bulk-migrator` container crashes halfway through a 4-million record table:
- **Mechanism**: File-based Checkpointing.
- **Recovery Flow**:
  1. As chunks (e.g., 10,000 rows) are successfully written to MongoDB, the `CheckpointManager` atomically writes the `last_processed_id` to a JSON file (e.g., `checkpoints/customers.json`).
  2. When the container restarts, it reads the checkpoint file.
  3. The `PostgresChunkReader` resumes querying from `id > last_processed_id`.
- **Result**: No data is skipped. A minimal amount of data (the in-flight chunk) might be re-processed, which is safe due to MongoDB idempotent upserts.

## 2. CDC Consumer Failure

### Scenario: Consumer Crashes or Disconnects
If the `cdc-consumer` crashes while processing a high volume of changes:
- **Mechanism**: Kafka Consumer Offsets.
- **Recovery Flow**:
  1. The consumer is configured with `enable.auto.commit=False`.
  2. It explicitly calls `commit()` only *after* a batch of events has been successfully written to MongoDB.
  3. Kafka retains all uncommitted messages durably on disk.
  4. Upon restart, the consumer reconnects to Kafka and fetches messages starting from the last committed offset.
- **Result**: Exact once or at-least-once processing. No changes are lost. Re-processed messages are safe due to idempotent MongoDB updates.

## 3. Database / Infrastructure Failures

### 3.1. PostgreSQL Goes Down
- The Load Generator and Bulk Migrator will encounter connection errors and crash (or retry). 
- Debezium will temporarily lose connection but will resume reading from the WAL via its replication slot once PostgreSQL is back online.

### 3.2. MongoDB Goes Down
- The Bulk Migrator will fail to write and will retry or crash. Checkpoints will not advance until writes succeed.
- The CDC Consumer will fail to write. Since offsets are not committed, the messages remain in Kafka. Once MongoDB recovers, the consumer restarts and processes the backlog.

### 3.3. Kafka Goes Down
- Debezium will block and buffer changes locally up to its limit. If Kafka is down long enough, Debezium may crash. The PostgreSQL replication slot ensures WAL logs are retained until Debezium reconnects. (Warning: This can cause PostgreSQL disk usage to grow).
- CDC Consumer will block, waiting for Kafka to become available.

## 4. Validating Resilience

The repository includes an automated failure testing suite defined in the `Makefile`.

```bash
make failure-test
```

**What it tests:**
1. Starts the migration.
2. Waits a few seconds, then forcibly kills (`docker kill`) the `bulk-migrator` and `cdc-consumer`.
3. Verifies that the Load Generator continues modifying the source database (simulating live production).
4. Restarts the killed containers.
5. Runs the `validator` to mathematically prove that, despite the crash, MongoDB eventually reaches 100% consistency with PostgreSQL.
