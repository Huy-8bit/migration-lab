import logging
import time
import random
import uuid
from faker import Faker
from psycopg2.extras import execute_values
from src.common.config import get_pg_connection, TABLE_RECORD_COUNTS

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def generate_data():
    conn = get_pg_connection()
    cur = conn.cursor()
    fake = Faker()
    
    tables_to_generate = [
        ('customers', TABLE_RECORD_COUNTS.get('customers', 300000)),
        ('products', TABLE_RECORD_COUNTS.get('products', 100000)),
        ('orders', TABLE_RECORD_COUNTS.get('orders', 1000000)),
        ('order_items', TABLE_RECORD_COUNTS.get('order_items', 2000000)),
        ('payments', TABLE_RECORD_COUNTS.get('payments', 600000))
    ]
    
    batch_size = 10000
    summary = []

    for table, total_rows in tables_to_generate:
        start_time = time.time()
        logger.info(f"Starting generation for {table} ({total_rows} rows)")
        
        if table == 'customers':
            for i in range(0, total_rows, batch_size):
                chunk = min(batch_size, total_rows - i)
                data = [(fake.name(), f"user{i+j}_{uuid.uuid4().hex[:8]}@example.com", fake.numerify('###-###-####')) for j in range(chunk)]
                execute_values(cur, "INSERT INTO customers (name, email, phone) VALUES %s", data)
                conn.commit()
                if (i + chunk) % 50000 == 0 or (i + chunk) == total_rows:
                    logger.info(f"Generating customers... {i + chunk} / {total_rows}")
                    
        elif table == 'products':
            categories = ['Electronics','Books','Clothing','Home','Sports','Food','Toys','Beauty','Auto','Garden']
            for i in range(0, total_rows, batch_size):
                chunk = min(batch_size, total_rows - i)
                data = [(fake.word().capitalize() + " " + fake.word().capitalize(), random.choice(categories), round(random.uniform(1.0, 999.99), 2), random.randint(0, 1000)) for _ in range(chunk)]
                execute_values(cur, "INSERT INTO products (name, category, price, stock) VALUES %s", data)
                conn.commit()
                if (i + chunk) % 20000 == 0 or (i + chunk) == total_rows:
                    logger.info(f"Generating products... {i + chunk} / {total_rows}")
                    
        elif table == 'orders':
            statuses = ['PENDING','PROCESSING','SHIPPED','DELIVERED','CANCELLED']
            num_customers = TABLE_RECORD_COUNTS.get('customers', 300000)
            for i in range(0, total_rows, batch_size):
                chunk = min(batch_size, total_rows - i)
                data = [(random.randint(1, num_customers), random.choice(statuses), round(random.uniform(10.0, 5000.0), 2)) for _ in range(chunk)]
                execute_values(cur, "INSERT INTO orders (customer_id, status, total_amount) VALUES %s", data)
                conn.commit()
                if (i + chunk) % 100000 == 0 or (i + chunk) == total_rows:
                    logger.info(f"Generating orders... {i + chunk} / {total_rows}")
                    
        elif table == 'order_items':
            num_orders = TABLE_RECORD_COUNTS.get('orders', 1000000)
            num_products = TABLE_RECORD_COUNTS.get('products', 100000)
            for i in range(0, total_rows, batch_size):
                chunk = min(batch_size, total_rows - i)
                data = [(random.randint(1, num_orders), random.randint(1, num_products), random.randint(1, 10), round(random.uniform(1.0, 999.99), 2)) for _ in range(chunk)]
                execute_values(cur, "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES %s", data)
                conn.commit()
                if (i + chunk) % 200000 == 0 or (i + chunk) == total_rows:
                    logger.info(f"Generating order_items... {i + chunk} / {total_rows}")
                    
        elif table == 'payments':
            payment_methods = ['CREDIT_CARD','DEBIT_CARD','PAYPAL','BANK_TRANSFER','CASH']
            payment_statuses = ['PENDING','COMPLETED','FAILED','REFUNDED']
            for i in range(0, total_rows, batch_size):
                chunk = min(batch_size, total_rows - i)
                # First N orders get a payment
                data = [(i + j + 1, random.choice(payment_methods), round(random.uniform(10.0, 5000.0), 2), random.choice(payment_statuses), str(uuid.uuid4())) for j in range(chunk)]
                execute_values(cur, "INSERT INTO payments (order_id, payment_method, amount, status, transaction_ref) VALUES %s", data)
                conn.commit()
                if (i + chunk) % 100000 == 0 or (i + chunk) == total_rows:
                    logger.info(f"Generating payments... {i + chunk} / {total_rows}")

        elapsed = time.time() - start_time
        summary.append({'table': table, 'row_count': total_rows, 'time_taken': elapsed})

    cur.close()
    conn.close()

    print("\nGeneration Summary:")
    print(f"{'Table':<15} | {'Row Count':<10} | {'Time Taken (s)':<15}")
    print("-" * 46)
    for s in summary:
        print(f"{s['table']:<15} | {s['row_count']:<10} | {s['time_taken']:<15.2f}")

if __name__ == '__main__':
    generate_data()
