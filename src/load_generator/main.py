import logging
import time
import signal
import sys
import threading
import random
import uuid
from faker import Faker
import psycopg2
from src.common.config import get_pg_pool, OPS_PER_SECOND

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(message)s')

running = True
ops_stats = {'insert': 0, 'update': 0, 'delete': 0, 'errors': 0}
stats_lock = threading.Lock()

def signal_handler(sig, frame):
    global running
    logger.info("\nShutting down Load Generator...")
    running = False

signal.signal(signal.SIGINT, signal_handler)

def init_marker(pool):
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO orders (id, customer_id, status, total_amount) 
                VALUES (999999, 1, 'PENDING', 100.00)
                ON CONFLICT (id) DO UPDATE SET status = 'PENDING', updated_at = NOW();
            """)
        conn.commit()
        logger.info("Initialized marker order 999999")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error init marker: {e}")
    finally:
        pool.putconn(conn)

def marker_updater(pool):
    while running:
        # Sleep in increments so we can exit quickly
        for _ in range(30):
            if not running:
                return
            time.sleep(1)
            
        if not running:
            break
            
        conn = pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE orders SET status = 'PAID', updated_at = NOW() WHERE id = 999999;
                """)
            conn.commit()
        except Exception as e:
            conn.rollback()
        finally:
            pool.putconn(conn)

def stats_reporter():
    last_time = time.time()
    last_ops = {'insert': 0, 'update': 0, 'delete': 0, 'errors': 0}
    
    while running:
        for _ in range(5):
            if not running:
                return
            time.sleep(1)
            
        if not running:
            break
            
        now = time.time()
        elapsed = now - last_time
        
        with stats_lock:
            current_ops = ops_stats.copy()
            
        diff_insert = current_ops['insert'] - last_ops['insert']
        diff_update = current_ops['update'] - last_ops['update']
        diff_delete = current_ops['delete'] - last_ops['delete']
        
        total_diff = diff_insert + diff_update + diff_delete
        rate = total_diff / elapsed if elapsed > 0 else 0
        
        logger.info(f"[LOAD GENERATOR] OPS: {current_ops['insert'] + current_ops['update'] + current_ops['delete']:,} | "
                    f"INSERT: {current_ops['insert']:,} | UPDATE: {current_ops['update']:,} | DELETE: {current_ops['delete']:,} | "
                    f"RATE: {int(rate)} ops/sec | ERRORS: {current_ops['errors']:,}")
        
        last_time = now
        last_ops = current_ops

def _random_id(cur, table: str, max_ids: dict) -> int:
    """Get a random existing ID efficiently without ORDER BY RANDOM()."""
    max_id = max_ids.get(table, 1)
    return random.randint(1, max_id)


def _refresh_max_ids(cur) -> dict:
    """Cache max IDs for each table for efficient random lookups."""
    max_ids = {}
    for t in ['customers', 'products', 'orders', 'order_items', 'payments']:
        cur.execute(f"SELECT MAX(id) FROM {t}")
        row = cur.fetchone()
        max_ids[t] = row[0] if row and row[0] else 1
    return max_ids


def do_insert(cur, fake, max_ids):
    table = random.choice(['customers', 'products', 'orders', 'order_items', 'payments'])
    if table == 'customers':
        cur.execute("INSERT INTO customers (name, email, phone) VALUES (%s, %s, %s)", 
                    (fake.name(), f"load_{uuid.uuid4().hex[:8]}@example.com", fake.numerify('###-###-####')))
    elif table == 'products':
        cur.execute("INSERT INTO products (name, category, price, stock) VALUES (%s, %s, %s, %s)", 
                    (fake.word(), 'LoadCat', round(random.uniform(1.0, 999.99), 2), random.randint(0, 100)))
    elif table == 'orders':
        cid = _random_id(cur, 'customers', max_ids)
        cur.execute("INSERT INTO orders (customer_id, status, total_amount) VALUES (%s, %s, %s)", 
                    (cid, random.choice(['PENDING','PROCESSING']), round(random.uniform(10.0, 500.0), 2)))
    elif table == 'order_items':
        oid = _random_id(cur, 'orders', max_ids)
        pid = _random_id(cur, 'products', max_ids)
        cur.execute("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)", 
                    (oid, pid, random.randint(1, 5), round(random.uniform(1.0, 100.0), 2)))
    elif table == 'payments':
        oid = _random_id(cur, 'orders', max_ids)
        cur.execute("INSERT INTO payments (order_id, payment_method, amount, status, transaction_ref) VALUES (%s, %s, %s, %s, %s)", 
                    (oid, 'CREDIT_CARD', round(random.uniform(10.0, 500.0), 2), 'COMPLETED', str(uuid.uuid4())))

def do_update(cur, fake, max_ids):
    target = random.choice(['order_status', 'payment_status', 'product_stock', 'customer_phone'])
    if target == 'order_status':
        rid = _random_id(cur, 'orders', max_ids)
        cur.execute("UPDATE orders SET status = %s, updated_at = NOW() WHERE id = %s", 
                    (random.choice(['SHIPPED','DELIVERED','CANCELLED']), rid))
    elif target == 'payment_status':
        rid = _random_id(cur, 'payments', max_ids)
        cur.execute("UPDATE payments SET status = %s, updated_at = NOW() WHERE id = %s", 
                    (random.choice(['REFUNDED','FAILED']), rid))
    elif target == 'product_stock':
        rid = _random_id(cur, 'products', max_ids)
        cur.execute("UPDATE products SET stock = %s, updated_at = NOW() WHERE id = %s", 
                    (random.randint(0, 1000), rid))
    elif target == 'customer_phone':
        rid = _random_id(cur, 'customers', max_ids)
        cur.execute("UPDATE customers SET phone = %s, updated_at = NOW() WHERE id = %s", 
                    (fake.numerify('###-###-####'), rid))

def do_delete(cur, max_ids):
    target = random.choice(['payments', 'order_items'])
    if target == 'payments':
        rid = _random_id(cur, 'payments', max_ids)
        cur.execute("DELETE FROM payments WHERE id = %s", (rid,))
    elif target == 'order_items':
        rid = _random_id(cur, 'order_items', max_ids)
        cur.execute("DELETE FROM order_items WHERE id = %s", (rid,))

def worker(pool):
    fake = Faker()
    target_sleep = 1.0 / OPS_PER_SECOND
    
    # Cache max IDs for efficient random selection
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            max_ids = _refresh_max_ids(cur)
    finally:
        pool.putconn(conn)
    
    last_refresh = time.time()
    
    while running:
        start = time.time()
        
        # Refresh max_ids every 60 seconds
        if start - last_refresh > 60:
            c = pool.getconn()
            try:
                with c.cursor() as cur:
                    max_ids = _refresh_max_ids(cur)
                last_refresh = start
            finally:
                pool.putconn(c)
        
        conn = pool.getconn()
        try:
            with conn.cursor() as cur:
                op = random.random()
                try:
                    if op < 0.50:
                        do_insert(cur, fake, max_ids)
                        with stats_lock: ops_stats['insert'] += 1
                    elif op < 0.85:
                        do_update(cur, fake, max_ids)
                        with stats_lock: ops_stats['update'] += 1
                    else:
                        do_delete(cur, max_ids)
                        with stats_lock: ops_stats['delete'] += 1
                    conn.commit()
                except psycopg2.Error:
                    conn.rollback()
                    with stats_lock: ops_stats['errors'] += 1
        except Exception:
            pass
        finally:
            pool.putconn(conn)
            
        elapsed = time.time() - start
        if elapsed < target_sleep:
            time.sleep(target_sleep - elapsed)

def main():
    pool = get_pg_pool(2, 5)
    if not pool:
        logger.error("Failed to get pool")
        sys.exit(1)
        
    init_marker(pool)
    
    t_marker = threading.Thread(target=marker_updater, args=(pool,), daemon=True)
    t_marker.start()
    
    t_stats = threading.Thread(target=stats_reporter, daemon=True)
    t_stats.start()
    
    logger.info(f"Starting Load Generator at {OPS_PER_SECOND} ops/sec. Press Ctrl+C to stop.")
    
    try:
        worker(pool)
    except KeyboardInterrupt:
        pass
        
    logger.info("Final Summary:")
    logger.info(f"INSERT: {ops_stats['insert']:,} | UPDATE: {ops_stats['update']:,} | DELETE: {ops_stats['delete']:,} | ERRORS: {ops_stats['errors']:,}")
    pool.closeall()

if __name__ == '__main__':
    main()
