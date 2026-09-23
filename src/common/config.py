"""
Shared configuration for the PostgreSQL → MongoDB Migration Lab.
All services import from this module to ensure consistency.
"""

import os
import logging
from decimal import Decimal
from datetime import datetime, timezone

# ─── Logging ────────────────────────────────────────────────────────────────

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

# ─── PostgreSQL ─────────────────────────────────────────────────────────────

POSTGRES_CONFIG = {
    'host':     os.environ.get('POSTGRES_HOST', 'localhost'),
    'port':     int(os.environ.get('POSTGRES_PORT', '5432')),
    'dbname':   os.environ.get('POSTGRES_DB', 'migration_lab'),
    'user':     os.environ.get('POSTGRES_USER', 'postgres'),
    'password': os.environ.get('POSTGRES_PASSWORD', 'postgres'),
}

# ─── MongoDB ────────────────────────────────────────────────────────────────

MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017')
MONGO_DB  = os.environ.get('MONGO_DB', 'migration_lab')

# ─── Kafka ──────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
SCHEMA_REGISTRY_URL     = os.environ.get('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
KAFKA_CONNECT_URL       = os.environ.get('KAFKA_CONNECT_URL', 'http://localhost:8083')

# ─── Migration Settings ────────────────────────────────────────────────────

BULK_WORKERS      = int(os.environ.get('BULK_WORKERS', '8'))
BULK_CHUNK_SIZE   = int(os.environ.get('BULK_CHUNK_SIZE', '10000'))
CHECKPOINT_DIR    = os.environ.get('CHECKPOINT_DIR', './checkpoints')
OPS_PER_SECOND    = int(os.environ.get('OPS_PER_SECOND', '100'))
CDC_CONSUMER_GROUP = os.environ.get('CDC_CONSUMER_GROUP', 'cdc-consumer-group')
CDC_BATCH_SIZE    = int(os.environ.get('CDC_BATCH_SIZE', '500'))

# ─── Table Definitions ─────────────────────────────────────────────────────

# Processing order respects foreign key dependencies
TABLE_ORDER = ['customers', 'products', 'orders', 'order_items', 'payments']

# Tables that have an updated_at column (used for timestamp guard)
TABLES_WITH_UPDATED_AT = {'customers', 'products', 'orders', 'payments'}

# Target row counts for data generation
TABLE_RECORD_COUNTS = {
    'customers':   300_000,
    'products':    100_000,
    'orders':    1_000_000,
    'order_items': 2_000_000,
    'payments':    600_000,
}

# Columns per table (for bulk reader)
TABLE_COLUMNS = {
    'customers':   ['id', 'name', 'email', 'phone', 'created_at', 'updated_at'],
    'products':    ['id', 'name', 'category', 'price', 'stock', 'created_at', 'updated_at'],
    'orders':      ['id', 'customer_id', 'status', 'total_amount', 'created_at', 'updated_at'],
    'order_items': ['id', 'order_id', 'product_id', 'quantity', 'unit_price', 'created_at'],
    'payments':    ['id', 'order_id', 'payment_method', 'amount', 'status', 'transaction_ref',
                    'created_at', 'updated_at'],
}

# ─── Kafka Topics ───────────────────────────────────────────────────────────

TOPIC_PREFIX  = 'postgres'
SCHEMA_NAME   = 'public'

def get_topic_name(table_name: str) -> str:
    """Get the Debezium Kafka topic name for a given table."""
    return f"{TOPIC_PREFIX}.{SCHEMA_NAME}.{table_name}"

KAFKA_TOPICS = [get_topic_name(t) for t in TABLE_ORDER]

# topic → table mapping
TOPIC_TO_TABLE = {get_topic_name(t): t for t in TABLE_ORDER}

# ─── Connection Helpers ─────────────────────────────────────────────────────

def get_pg_connection(**kwargs):
    """Create a new PostgreSQL connection."""
    import psycopg2
    config = {**POSTGRES_CONFIG, **kwargs}
    return psycopg2.connect(**config)


def get_pg_pool(minconn=2, maxconn=20):
    """Create a threaded PostgreSQL connection pool."""
    from psycopg2.pool import ThreadedConnectionPool
    return ThreadedConnectionPool(minconn, maxconn, **POSTGRES_CONFIG)


def get_mongo_client(**kwargs):
    """Create a MongoDB client."""
    from pymongo import MongoClient
    return MongoClient(MONGO_URI, **kwargs)


def get_mongo_db(**kwargs):
    """Get the migration target MongoDB database."""
    client = get_mongo_client(**kwargs)
    return client[MONGO_DB]


# ─── Data Type Helpers ─────────────────────────────────────────────────────

def convert_pg_row(row: dict) -> dict:
    """Convert PostgreSQL row types to MongoDB-compatible types.

    - Decimal → float
    - datetime → ISO string (for consistent timestamp comparison)
    """
    result = {}
    for key, value in row.items():
        if isinstance(value, Decimal):
            result[key] = float(value)
        elif isinstance(value, datetime):
            result[key] = value.isoformat()
        else:
            result[key] = value
    return result


def get_source_ts(row: dict, table_name: str) -> str:
    """Extract the source timestamp for race-condition prevention.

    Returns the updated_at value (or created_at for tables without updated_at)
    as an ISO format string for consistent ordering comparisons.
    """
    if table_name in TABLES_WITH_UPDATED_AT and row.get('updated_at'):
        ts = row['updated_at']
    elif row.get('created_at'):
        ts = row['created_at']
    else:
        ts = datetime.now(timezone.utc)

    if isinstance(ts, datetime):
        return ts.isoformat()
    return str(ts)
