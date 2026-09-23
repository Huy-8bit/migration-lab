import sys
import signal
import time
import logging
import concurrent.futures
from threading import Event

from src.common.config import BULK_WORKERS, BULK_CHUNK_SIZE, TABLE_ORDER, CHECKPOINT_DIR
from src.bulk_migrator.checkpoint import CheckpointManager
from src.bulk_migrator.postgres_reader import PostgresChunkReader
from src.bulk_migrator.mongo_writer import MongoWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Global shutdown event to handle SIGINT gracefully
shutdown_event = Event()

def signal_handler(sig, frame):
    logger.info("Interrupt received, shutting down gracefully...")
    shutdown_event.set()

def migrate_chunk(table_name: str, start_id: int, end_id: int) -> int:
    if shutdown_event.is_set():
        return 0
    
    reader = PostgresChunkReader(table_name)
    writer = MongoWriter(table_name)
    try:
        records = reader.read_chunk(start_id, end_id)
        written = writer.write_batch(records)
        return written
    finally:
        writer.close()

def process_table(table_name: str, checkpoint_mgr: CheckpointManager, summary_stats: list):
    if checkpoint_mgr.is_table_complete(table_name):
        logger.info(f"Table {table_name} is already complete. Skipping.")
        reader = PostgresChunkReader(table_name)
        total_count = reader.get_total_count()
        summary_stats.append({
            "table": table_name,
            "records": total_count,
            "time": 0.0,
            "rate": 0
        })
        return

    reader = PostgresChunkReader(table_name)
    max_id = reader.get_max_id()
    total_count = reader.get_total_count()
    
    if max_id == 0:
        logger.info(f"Table {table_name} is empty.")
        checkpoint_mgr.save_checkpoint(table_name, 0, 0, "complete")
        summary_stats.append({
            "table": table_name,
            "records": total_count,
            "time": 0.0,
            "rate": 0
        })
        return

    resume_id = checkpoint_mgr.get_resume_id(table_name)
    logger.info(f"Starting {table_name}: max_id={max_id}, resuming from id={resume_id}")

    start_time = time.time()
    records_processed = 0

    # Read checkpoint to get correct records_processed if we are resuming
    chk = checkpoint_mgr.get_checkpoint(table_name)
    if chk and chk.get('records_processed'):
        records_processed = chk['records_processed']

    chunks = list(reader.generate_chunks(resume_id))
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=BULK_WORKERS) as executor:
        future_to_chunk = {
            executor.submit(migrate_chunk, table_name, start_id, end_id): (start_id, end_id)
            for start_id, end_id in chunks
        }
        
        last_processed_id = resume_id

        for future in concurrent.futures.as_completed(future_to_chunk):
            if shutdown_event.is_set():
                break
                
            start_id, end_id = future_to_chunk[future]
            try:
                written = future.result()
                records_processed += written
                last_processed_id = max(last_processed_id, end_id)
                
                elapsed = time.time() - start_time
                rate = records_processed / elapsed if elapsed > 0 else 0
                pct = (records_processed / total_count * 100) if total_count > 0 else 100
                logger.info(f"{table_name}: {records_processed}/{total_count} ({pct:.1f}%) — {rate:,.0f} rows/sec")
                
                checkpoint_mgr.save_checkpoint(table_name, last_processed_id, records_processed, "in_progress")
            except Exception as exc:
                logger.error(f"{table_name} chunk {start_id}-{end_id} generated an exception: {exc}")
                shutdown_event.set()

    if shutdown_event.is_set():
        logger.info(f"Migration for {table_name} interrupted.")
        return

    checkpoint_mgr.save_checkpoint(table_name, max_id, records_processed, "complete")
    elapsed_time = time.time() - start_time
    rate = records_processed / elapsed_time if elapsed_time > 0 else 0
    
    summary_stats.append({
        "table": table_name,
        "records": records_processed,
        "time": elapsed_time,
        "rate": rate
    })
    logger.info(f"Finished {table_name} in {elapsed_time:.1f}s")

def main():
    signal.signal(signal.SIGINT, signal_handler)
    
    checkpoint_mgr = CheckpointManager(CHECKPOINT_DIR)
    summary_stats = []
    
    logger.info("=== Starting Bulk Migration ===")
    
    total_start = time.time()
    
    for table_name in TABLE_ORDER:
        if shutdown_event.is_set():
            break
        process_table(table_name, checkpoint_mgr, summary_stats)
        
    if not shutdown_event.is_set():
        total_time = time.time() - total_start
        total_records = sum(s["records"] for s in summary_stats)
        total_rate = total_records / total_time if total_time > 0 else 0
        
        print("\n═══ BULK MIGRATION COMPLETE ═══")
        print(f"{'Table':<15} {'Records':<10} {'Time':<10} {'Rate':<10}")
        for stat in summary_stats:
            print(f"{stat['table']:<15} {stat['records']:<10,d} {stat['time']:<10.1f} {int(stat['rate']):<10,d}/s")
        print(f"{'TOTAL':<15} {total_records:<10,d} {total_time:<10.1f} {int(total_rate):<10,d}/s")

if __name__ == "__main__":
    main()
