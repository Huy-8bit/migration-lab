import argparse
import logging
import time
import signal
import sys

from src.common.config import KAFKA_TOPICS, TOPIC_TO_TABLE, CDC_CONSUMER_GROUP, CDC_BATCH_SIZE
from src.cdc_consumer.avro_consumer import AvroKafkaConsumer
from src.cdc_consumer.mongo_sink import MongoSink

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

running = True

def signal_handler(sig, frame):
    global running
    logger.info("Interrupt received, stopping...")
    running = False

def main():
    parser = argparse.ArgumentParser(description='CDC Consumer')
    parser.add_argument('--timeout', type=int, default=0,
                        help='Stop after N seconds (0 = run forever)')
    args = parser.parse_args()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    consumer = AvroKafkaConsumer(KAFKA_TOPICS, CDC_CONSUMER_GROUP)
    sink = MongoSink()
    
    total_events = 0
    applied = 0
    errors = 0
    last_log_time = time.time()
    last_applied = 0
    start_time = time.time()
    zero_lag_since = None  # Track when lag first reached 0
    
    logger.info(f"Starting CDC Consumer... (timeout={args.timeout}s)")
    
    try:
        while running:
            # Check timeout
            if args.timeout > 0 and (time.time() - start_time) >= args.timeout:
                logger.info(f"Timeout of {args.timeout}s reached. Stopping.")
                break

            events = consumer.poll_events(timeout=1.0, max_events=CDC_BATCH_SIZE)
            
            if events:
                zero_lag_since = None  # Reset zero-lag tracker when we get events
                for event in events:
                    topic = event['topic']
                    table = TOPIC_TO_TABLE.get(topic)
                    if not table:
                        table = topic.split('.')[-1]
                        
                    value = event['value']
                    if value is None:
                        continue
                        
                    try:
                        if sink.apply_event(table, value):
                            applied += 1
                    except Exception as e:
                        errors += 1
                        logger.error(f"Error applying event: {e}")
                        
                    total_events += 1
                
                consumer.commit()
            
            current_time = time.time()
            if current_time - last_log_time >= 5.0:
                elapsed = current_time - last_log_time
                rate = (applied - last_applied) / elapsed if elapsed > 0 else 0
                
                lag_dict = consumer.get_lag()
                total_lag = sum(lag_dict.values()) if lag_dict else 0
                
                logger.info(f"[CDC CONSUMER] Events: {total_events:,} | Applied: {applied:,} | "
                           f"Errors: {errors:,} | Rate: {rate:,.0f}/sec | Lag: {total_lag:,}")
                
                # Auto-stop when lag reaches 0 for 10s (only when timeout mode)
                if args.timeout > 0 and total_lag == 0 and total_events > 0:
                    if zero_lag_since is None:
                        zero_lag_since = current_time
                    elif current_time - zero_lag_since >= 10:
                        logger.info("Lag has been 0 for 10 seconds. Stopping.")
                        break
                else:
                    zero_lag_since = None
                
                last_log_time = current_time
                last_applied = applied
                
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info(f"Final Stats - Events: {total_events:,} | Applied: {applied:,} | Errors: {errors:,}")
        consumer.close()
        sink.close()

if __name__ == '__main__':
    main()
