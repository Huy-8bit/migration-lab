# Data Validation Strategy

Ensuring data consistency between a relational database and a NoSQL document database during an active migration is highly complex. We employ a 7-point validation framework to guarantee integrity.

## 1. Validation Checks

The `validator` application (`src/validator/main.py`) performs the following sequential checks:

### Check 1: MongoDB Connection
Verifies connectivity to the target database to ensure the environment is correctly configured.

### Check 2: Collection Existence
Ensures that all required tables from PostgreSQL exist as collections in MongoDB.

### Check 3: Row Count Parity
Counts the total records in each PostgreSQL table and compares it to the document count in MongoDB.
- *Note:* In an active CDC environment, slight discrepancies might briefly exist if the CDC consumer is slightly behind the Load Generator. The validator incorporates retry logic and tolerances.

### Check 4: Data Schema & Type Validation
Randomly samples documents from MongoDB to verify that the schema mapping was successful (e.g., PostgreSQL `numeric` to MongoDB `Double` or `Decimal128`, date conversions, proper foreign key types).

### Check 5: Precise Data Match (Row-by-Row Sample)
Pulls random Primary Keys from PostgreSQL, fetches the corresponding rows from both PostgreSQL and MongoDB, and compares every single field.
- Normalizes datetime objects (timezone handling).
- Normalizes decimals to floats for comparison.

### Check 6: CDC Marker Validation
The Load Generator constantly updates a specific "marker" record in PostgreSQL. The validator checks if MongoDB contains the absolute latest state of this marker, proving that the CDC pipeline is flowing in real-time and isn't lagging significantly.

### Check 7: Deep Hash Verification (Optional/Advanced)
For absolute certainty, a hash of an entire row/document can be computed and compared, ensuring no subtle data corruption occurred during serialization/deserialization over Kafka.

## 2. Running the Validation

The validation suite is fully containerized. To run it:

```bash
make validate
```

**Expected Output:**
```
[VALIDATOR] Starting comprehensive validation suite...
[VALIDATOR] [OK] Check 1: MongoDB connection successful.
[VALIDATOR] [OK] Check 2: All collections exist.
[VALIDATOR] [OK] Check 3: Row counts match (Table 'orders': 1,000,000 records).
[VALIDATOR] [OK] Check 4: Schema validation passed.
[VALIDATOR] [OK] Check 5: Data match passed for 100 random samples per table.
[VALIDATOR] [OK] Check 6: CDC Marker is up-to-date.
[VALIDATOR] ALL CHECKS PASSED. Migration is strongly consistent.
```

## 3. Handling Eventual Consistency

Because the system relies on asynchronous message passing (Kafka), it is *eventually consistent*. 
If the validator runs while the Load Generator is highly active, it might detect a mismatch because an `UPDATE` committed in PostgreSQL is currently buffered in Kafka and hasn't reached MongoDB yet.

To handle this, the validation strategy should:
1. Stop the Load Generator (simulating a production write-lock or cutover period).
2. Wait for the Kafka consumer lag to drop to 0.
3. Run the validation suite.

If it passes under these conditions, the migration is 100% successful and ready for application cutover.
