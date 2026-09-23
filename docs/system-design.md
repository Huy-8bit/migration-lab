# System Design: PostgreSQL to MongoDB Migration Lab

## 1. Overview
This project simulates a large-scale database migration from PostgreSQL (Relational) to MongoDB (NoSQL) with zero downtime. It combines an initial historical data load (Bulk Migration) with real-time continuous replication (CDC - Change Data Capture) using Debezium, Kafka, and a custom consumer.

## 2. Goals and Requirements

### 2.1. Functional Requirements
- Migrate 5 relational tables (`customers`, `products`, `orders`, `order_items`, `payments`) into a MongoDB target.
- Handle active workloads where PostgreSQL receives continuous `INSERT`, `UPDATE`, and `DELETE` operations during the migration.
- Provide a robust validation mechanism to prove data consistency between the source and target.

### 2.2. Non-Functional Requirements
- **Zero Downtime**: The source database must remain available for reads and writes throughout the process.
- **Consistency**: Final state in MongoDB must exactly match PostgreSQL (Eventual Consistency).
- **Idempotency**: All write operations to MongoDB must be idempotent to handle potential duplicate messages from the message broker safely.
- **Failure Recovery**: The system must be able to resume gracefully if any component crashes (e.g., Bulk migrator checkpoints, Kafka consumer offsets).
- **Scalability**: The architecture must support scaling to billions of records through parallelization and partitioning.
- **Observability**: Expose clear metrics and status for both migration phases.

## 3. Architecture Choices

### 3.1. Change Data Capture (CDC)
- **Tool**: Debezium PostgreSQL Connector
- **Reason**: Debezium hooks directly into PostgreSQL's Write-Ahead Log (WAL) via the `pgoutput` plugin. This provides a low-overhead, exact stream of every row-level change without querying the database, ensuring no changes are missed even under heavy load.

### 3.2. Message Broker
- **Tool**: Apache Kafka
- **Reason**: Kafka provides high-throughput, fault-tolerant, and ordered message delivery. It decouples the source extraction from the target insertion, buffering sudden spikes in database activity.

### 3.3. Data Serialization
- **Tool**: Apache Avro + Confluent Schema Registry
- **Reason**: Avro provides a compact binary format that is much more efficient than JSON for millions of records. Schema Registry enforces schema evolution rules, ensuring that if PostgreSQL table structures change, the downstream consumers are aware and won't break unpredictably.

### 3.4. Target Database
- **Tool**: MongoDB
- **Reason**: Document database suitable for flexible schemas and high-throughput writes. Documents are updated based on a `_source_ts` timestamp to prevent older CDC events from overwriting newer states (handling race conditions between Bulk Load and CDC).

## 4. Scaling to Production (4-5 Billion Records)
To scale this architecture from 4 million records (Lab scale) to 5 billion records (Production scale):

1. **Parallel Bulk Migration**: The `Bulk Migrator` splits tables into non-overlapping chunks based on primary keys. Multiple migrator instances can run concurrently, processing chunks in parallel.
2. **Kafka Partitioning**: CDC topics can be partitioned. Multiple consumer instances in a consumer group can process changes in parallel while maintaining ordering per primary key.
3. **MongoDB Sharding**: The target MongoDB cluster can be sharded to distribute write operations across multiple nodes.
4. **Connection Pooling**: Adjust `max_connections` on PostgreSQL and use connection pooling (e.g., PgBouncer) to prevent connection exhaustion.

## 5. Idempotency and Race Conditions
A critical challenge in zero-downtime migration is the overlap between the initial bulk load and the CDC stream.

- **Problem**: A record is modified in PostgreSQL *while* the bulk migrator is reading it. The CDC captures the update, but the bulk migrator also reads the state. If the CDC consumer processes the update before the bulk migrator writes the initial row, the bulk migrator might overwrite the newer state with older data.
- **Solution**: Every document written to MongoDB includes a `_source_ts` (timestamp of the change). MongoDB operations use conditional updates (`$lte`) or timestamp checks to ensure that a document is only updated if the incoming `_source_ts` is greater than or equal to the existing timestamp in the database.
