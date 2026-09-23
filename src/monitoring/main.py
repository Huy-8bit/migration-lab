import os
import time
import json
import requests
from datetime import datetime

from src.common.config import (get_pg_connection, get_mongo_db, TABLE_ORDER, 
                                CHECKPOINT_DIR, KAFKA_CONNECT_URL)

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def get_pg_counts(conn):
    counts = {}
    total = 0
    try:
        cur = conn.cursor()
        for table in TABLE_ORDER:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            cnt = cur.fetchone()[0]
            counts[table] = cnt
            total += cnt
        cur.close()
    except Exception:
        pass
    return counts, total

def get_mongo_counts(db):
    counts = {}
    total = 0
    try:
        for table in TABLE_ORDER:
            cnt = db[table].count_documents({})
            counts[table] = cnt
            total += cnt
    except Exception:
        pass
    return counts, total

def get_debezium_status():
    try:
        r = requests.get(f"{KAFKA_CONNECT_URL}/connectors/postgres-connector/status", timeout=2)
        if r.status_code == 200:
            data = r.json()
            connector_state = data.get('connector', {}).get('state', 'UNKNOWN')
            tasks = data.get('tasks', [])
            if connector_state == 'RUNNING' and all(t.get('state') == 'RUNNING' for t in tasks):
                return "RUNNING ✅"
            else:
                return f"{connector_state} ⚠️"
        return "NOT FOUND ❌"
    except Exception:
        return "ERROR ❌"

def get_checkpoints():
    checkpoints = {}
    for table in TABLE_ORDER:
        checkpoints[table] = "PENDING"
        file_path = os.path.join(CHECKPOINT_DIR, f"{table}.json")
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    status = data.get('status', '')
                    if status == 'complete':
                        checkpoints[table] = "COMPLETE ✅"
                    elif status == 'in_progress':
                        processed = data.get('records_processed', 0)
                        last_id = data.get('last_processed_id', 0)
                        checkpoints[table] = f"IN_PROGRESS ({processed:,})"
            except:
                checkpoints[table] = "ERROR"
    return checkpoints

def run_monitor():
    db = get_mongo_db()
    conn = get_pg_connection()
    
    while True:
        try:
            pg_counts, pg_total = get_pg_counts(conn)
            mongo_counts, mongo_total = get_mongo_counts(db)
            deb_status = get_debezium_status()
            checkpoints = get_checkpoints()
            
            clear_screen()
            
            print("╔══════════════════════════════════════════════════════════════════════╗")
            print("║            PostgreSQL → MongoDB Migration Monitor                    ║")
            print("╠══════════════════════════════════════════════════════════════════════╣")
            
            print("\n─── SOURCE (PostgreSQL) ────────────────────────────────────────────────")
            print(f"  {'Table':<15} {'Records'}")
            for t in TABLE_ORDER:
                print(f"  {t:<15} {pg_counts.get(t, 0):,}")
            print(f"  {'TOTAL':<15} {pg_total:,}")
            
            print("\n─── TARGET (MongoDB) ───────────────────────────────────────────────────")
            print(f"  {'Collection':<15} {'Records'}")
            for t in TABLE_ORDER:
                print(f"  {t:<15} {mongo_counts.get(t, 0):,}")
            print(f"  {'TOTAL':<15} {mongo_total:,}")
            
            print("\n─── MIGRATION PROGRESS ─────────────────────────────────────────────────")
            pct = (mongo_total / pg_total * 100) if pg_total > 0 else 0
            bars = int(pct / 5)
            progress_bar = "█" * bars + "░" * (20 - bars)
            print(f"  Overall: {pct:.1f}%  [{progress_bar}]  {mongo_total:,} / {pg_total:,}")
            
            print("\n─── CDC (Debezium → Kafka → MongoDB) ───────────────────────────────────")
            print(f"  Debezium Status:  {deb_status}")
            print(f"  Kafka Topics:     {len(TABLE_ORDER)}")
            print(f"  Consumer Lag:     N/A")
            
            print("\n─── BULK MIGRATION ─────────────────────────────────────────────────────")
            print("  Checkpoint Status:")
            for t in TABLE_ORDER:
                print(f"    {t:<13}: {checkpoints.get(t)}")
                
            print(f"\n  Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            time.sleep(2)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(2)

if __name__ == '__main__':
    run_monitor()
