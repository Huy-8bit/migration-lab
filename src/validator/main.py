import os
import json
import logging
import random
from datetime import datetime
from decimal import Decimal
from tabulate import tabulate

from src.common.config import get_pg_connection, get_mongo_db, TABLE_ORDER

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def normalize_val(v):
    if isinstance(v, datetime):
        return v.isoformat().replace(' ', 'T').replace('+00:00', 'Z')
    if isinstance(v, str) and len(v) >= 10 and '-' in v[:10] and ':' in v:
        return v.replace(' ', 'T').replace('+00:00', 'Z')
    return v

def compare_dicts(pg_row, mongo_doc):
    pg_clean = {k: normalize_val(v) for k, v in pg_row.items() if k not in ('id', '_source_ts')}
    mongo_clean = {k: normalize_val(v) for k, v in mongo_doc.items() if k not in ('_id', '_source_ts')}
    
    for k, v in pg_clean.items():
        if k not in mongo_clean:
            return False
        v2 = mongo_clean[k]
        
        if isinstance(v, (Decimal, float, int)) or isinstance(v2, (Decimal, float, int)):
            try:
                if abs(float(v) - float(v2)) > 0.01:
                    return False
            except (ValueError, TypeError):
                if str(v) != str(v2):
                    return False
        elif str(v) != str(v2):
            logger.warning(f"Mismatch in {k}: PG='{v}' ({type(v)}) vs Mongo='{v2}' ({type(v2)})")
            return False
    return True

def run_validation():
    db = get_mongo_db()
    conn = get_pg_connection()
    cur = conn.cursor()
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "status": "PASS",
        "checks": {}
    }
    
    print("╔══════════════════════════════════════════════════════════════════════╗")
    print("║                    MIGRATION VALIDATION REPORT                       ║")
    print("╠══════════════════════════════════════════════════════════════════════╣")
    
    # 1. Row Count
    print("\n─── 1. Row Count ───────────────────────────────────────────────────────")
    row_count_data = []
    row_count_pass = True
    report["checks"]["row_count"] = {"status": "PASS", "details": {}}
    for table in TABLE_ORDER:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        pg_count = cur.fetchone()[0]
        mongo_count = db[table].count_documents({})
        match = "✅" if pg_count == mongo_count else "❌"
        if pg_count != mongo_count:
            row_count_pass = False
        row_count_data.append([table, pg_count, mongo_count, match])
        report["checks"]["row_count"]["details"][table] = {"pg": pg_count, "mongo": mongo_count}
    if not row_count_pass:
        report["status"] = "FAIL"
        report["checks"]["row_count"]["status"] = "FAIL"
    print(tabulate(row_count_data, headers=["Table", "Source", "Target", "Match"]))
    
    # 2. Primary Key Existence
    print("\n─── 2. Primary Key Existence ───────────────────────────────────────────")
    pk_pass = True
    report["checks"]["pk_existence"] = {"status": "PASS", "details": {}}
    for table in TABLE_ORDER:
        cur.execute(f"SELECT id FROM {table} ORDER BY RANDOM() LIMIT 1000")
        sample_ids = [row[0] for row in cur.fetchall()]
        if not sample_ids:
            continue
        found = db[table].count_documents({"_id": {"$in": sample_ids}})
        match = "✅" if found == len(sample_ids) else "❌"
        if found != len(sample_ids):
            pk_pass = False
        print(f"  {table}: {found}/{len(sample_ids)} {match}")
        report["checks"]["pk_existence"]["details"][table] = f"{found}/{len(sample_ids)}"
    if not pk_pass:
        report["status"] = "FAIL"
        report["checks"]["pk_existence"]["status"] = "FAIL"

    # 3. Random Sample Comparison
    print("\n─── 3. Random Sample Comparison ────────────────────────────────────────")
    sample_pass = True
    report["checks"]["sample_comparison"] = {"status": "PASS", "details": {}}
    for table in TABLE_ORDER:
        cur.execute(f"SELECT * FROM {table} ORDER BY RANDOM() LIMIT 100")
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        matches = 0
        for row in rows:
            pg_dict = dict(zip(columns, row))
            mongo_doc = db[table].find_one({"_id": pg_dict['id']})
            if mongo_doc and compare_dicts(pg_dict, mongo_doc):
                matches += 1
        match_icon = "✅" if matches == len(rows) else "❌"
        if matches != len(rows) and len(rows) > 0:
            sample_pass = False
        print(f"  {table}: {matches}/{len(rows)} match {match_icon}")
        report["checks"]["sample_comparison"]["details"][table] = f"{matches}/{len(rows)}"
    if not sample_pass:
        report["status"] = "FAIL"
        report["checks"]["sample_comparison"]["status"] = "FAIL"

    # 4. Range Checksum
    print("\n─── 4. Range Checksum ──────────────────────────────────────────────────")
    range_pass = True
    report["checks"]["range_checksum"] = {"status": "PASS", "details": {}}
    for table in TABLE_ORDER:
        cur.execute(f"SELECT MIN(id), MAX(id) FROM {table}")
        min_id, max_id = cur.fetchone()
        if min_id is None:
            continue
        step = 100000
        ranges = list(range(min_id, max_id + step, step))
        matched_ranges = 0
        total_ranges = max(1, len(ranges) - 1)
        
        for i in range(len(ranges) - 1):
            start = ranges[i]
            end = ranges[i+1] - 1 if i < len(ranges) - 2 else ranges[i+1]
            
            cur.execute(f"SELECT COUNT(*), SUM(id) FROM {table} WHERE id BETWEEN %s AND %s", (start, end))
            pg_res = cur.fetchone()
            pg_count = pg_res[0] or 0
            pg_sum = pg_res[1] or 0
            
            pipeline = [
                {"$match": {"_id": {"$gte": start, "$lte": end}}},
                {"$group": {"_id": None, "count": {"$sum": 1}, "sum_id": {"$sum": "$_id"}}}
            ]
            m_res = list(db[table].aggregate(pipeline))
            m_count = m_res[0]['count'] if m_res else 0
            m_sum = m_res[0]['sum_id'] if m_res else 0
            
            if pg_count == m_count and pg_sum == m_sum:
                matched_ranges += 1
                
        match_icon = "✅" if matched_ranges == total_ranges else "❌"
        if matched_ranges != total_ranges:
            range_pass = False
        print(f"  {table}: {matched_ranges}/{total_ranges} ranges match {match_icon}")
        report["checks"]["range_checksum"]["details"][table] = f"{matched_ranges}/{total_ranges}"
    if not range_pass:
        report["status"] = "FAIL"
        report["checks"]["range_checksum"]["status"] = "FAIL"

    # 5. Aggregate Validation
    print("\n─── 5. Aggregate Validation ────────────────────────────────────────────")
    agg_pass = True
    report["checks"]["aggregates"] = {"status": "PASS", "details": {}}
    
    # orders amount
    cur.execute("SELECT SUM(total_amount) FROM orders")
    pg_sum = float(cur.fetchone()[0] or 0)
    m_sum_res = list(db.orders.aggregate([{"$group": {"_id": None, "total": {"$sum": {"$toDouble": "$total_amount"}}}}]))
    m_sum = m_sum_res[0]['total'] if m_sum_res else 0
    match = "✅" if abs(pg_sum - m_sum) < 0.1 else "❌"
    if match == "❌": agg_pass = False
    print(f"  SUM(orders.total_amount): {pg_sum:,.2f} vs {m_sum:,.2f} {match}")
    report["checks"]["aggregates"]["details"]["orders_sum"] = f"{pg_sum} vs {m_sum}"
    
    # orders status
    cur.execute("SELECT status, COUNT(*) FROM orders GROUP BY status")
    pg_statuses = {row[0]: row[1] for row in cur.fetchall()}
    m_statuses_res = list(db.orders.aggregate([{"$group": {"_id": "$status", "count": {"$sum": 1}}}]))
    m_statuses = {d["_id"]: d["count"] for d in m_statuses_res}
    status_match = "✅" if pg_statuses == m_statuses else "❌"
    if status_match == "❌": agg_pass = False
    print(f"  orders by status: MATCH {status_match}")
    report["checks"]["aggregates"]["details"]["orders_status"] = f"pg={pg_statuses}, mongo={m_statuses}"

    # payments amount
    cur.execute("SELECT SUM(amount) FROM payments")
    pg_sum_p = float(cur.fetchone()[0] or 0)
    m_sum_p_res = list(db.payments.aggregate([{"$group": {"_id": None, "total": {"$sum": {"$toDouble": "$amount"}}}}]))
    m_sum_p = m_sum_p_res[0]['total'] if m_sum_p_res else 0
    match_p = "✅" if abs(pg_sum_p - m_sum_p) < 0.1 else "❌"
    if match_p == "❌": agg_pass = False
    print(f"  SUM(payments.amount): {pg_sum_p:,.2f} vs {m_sum_p:,.2f} {match_p}")
    report["checks"]["aggregates"]["details"]["payments_sum"] = f"{pg_sum_p} vs {m_sum_p}"
    
    if not agg_pass:
        report["status"] = "FAIL"
        report["checks"]["aggregates"]["status"] = "FAIL"

    # 6. Missing Records
    print("\n─── 6. Missing Records ────────────────────────────────────────────────")
    missing_pass = True
    report["checks"]["missing"] = {"status": "PASS", "details": {}}
    missing_found = False
    for table in TABLE_ORDER:
        cur.execute(f"SELECT id FROM {table} ORDER BY id DESC LIMIT 1000")
        sample_ids = [row[0] for row in cur.fetchall()]
        if sample_ids:
            found = db[table].count_documents({"_id": {"$in": sample_ids}})
            if found < len(sample_ids):
                missing_found = True
                missing_pass = False
    if not missing_found:
        print("  No missing records found ✅")
    else:
        print("  Missing records found ❌")
        report["status"] = "FAIL"
        report["checks"]["missing"]["status"] = "FAIL"

    # 7. Extra Records
    print("\n─── 7. Extra Records ──────────────────────────────────────────────────")
    extra_pass = True
    report["checks"]["extra"] = {"status": "PASS", "details": {}}
    extra_found = False
    for table in TABLE_ORDER:
        m_sample = list(db[table].find({}, {"_id": 1}).sort("_id", -1).limit(1000))
        m_ids = [d["_id"] for d in m_sample]
        if m_ids:
            cur.execute(f"SELECT COUNT(id) FROM {table} WHERE id = ANY(%s)", (m_ids,))
            pg_count = cur.fetchone()[0]
            if pg_count < len(m_ids):
                extra_found = True
                extra_pass = False
    if not extra_found:
        print("  No extra records found ✅")
    else:
        print("  Extra records found ❌")
        report["status"] = "FAIL"
        report["checks"]["extra"]["status"] = "FAIL"

    # Demo Marker
    print("\n─── Demo Marker Check ──────────────────────────────────────────────────")
    demo_doc = db.orders.find_one({"_id": 999999})
    if demo_doc and demo_doc.get("status") == "PAID":
        print("  Order #999999 status: PAID ✅")
        report["checks"]["demo_marker"] = {"status": "PASS"}
    else:
        print("  Order #999999 status: NOT PAID or MISSING ❌ (Expected if load generator didn't run)")
        report["checks"]["demo_marker"] = {"status": "FAIL (or not generated)"}

    # Final Status
    print("\n╔══════════════════════════════════════════════════════════════════════╗")
    if report["status"] == "PASS":
        print("║  VALIDATION STATUS: ✅ PASS                                          ║")
    else:
        print("║  VALIDATION STATUS: ❌ FAIL                                          ║")
    print("╚══════════════════════════════════════════════════════════════════════╝")

    with open('migration_report.json', 'w') as f:
        json.dump(report, f, indent=2)

    cur.close()
    conn.close()

if __name__ == '__main__':
    run_validation()
