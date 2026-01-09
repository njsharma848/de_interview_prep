# Spark Dynamic Partition Pruning (DPP)

Understanding how Spark 3.0+ optimizes queries by dynamically pruning partitions using dimension table filters.

---

## Table of Contents
- [What is Dynamic Partition Pruning?](#what-is-dynamic-partition-pruning)
- [Prerequisites: Understanding Basic Concepts](#prerequisites-understanding-basic-concepts)
- [The Problem DPP Solves](#the-problem-dpp-solves)
- [How DPP Works](#how-dpp-works)
- [Configuration](#configuration)
- [Complete Example](#complete-example)
- [Requirements for DPP](#requirements-for-dpp)
- [Best Practices](#best-practices)

---

## What is Dynamic Partition Pruning?

### Overview

**Dynamic Partition Pruning (DPP)** is a query optimization feature introduced in **Spark 3.0** that:
- Takes filter conditions from a small dimension table
- Injects them into a large fact table as a subquery
- Enables partition pruning on the fact table
- Dramatically reduces data read from disk

### Key Feature

**Enabled by default** in Spark 3.0+

```python
# Default value
spark.conf.get("spark.sql.optimizer.dynamicPartitionPruning.enabled")
# Returns: "true"

# To disable (not recommended)
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")
```

---

## Prerequisites: Understanding Basic Concepts

### 1. Predicate Pushdown

**Concept:** Push filter conditions down to the data source level, applying them as early as possible.

**Without Predicate Pushdown:**
```
1. Read ALL data from source
2. Load into memory
3. Apply filter
4. Process filtered data
```

**With Predicate Pushdown:**
```
1. Push filter to source
2. Read ONLY filtered data
3. Process (already filtered)
```

**Benefit:** Read less data from disk

---

### 2. Partition Pruning

**Concept:** Read only relevant partitions, skip irrelevant ones entirely.

**Example Table Structure:**
```
orders/
├── order_date=2021-02-01/
│   └── data.parquet
├── order_date=2021-02-02/
│   └── data.parquet
├── order_date=2021-02-03/
│   └── data.parquet
└── order_date=2021-02-04/
    └── data.parquet
```

**Query with Partition Pruning:**
```python
df = spark.read.parquet("orders")
result = df.filter(col("order_date") == "2021-02-03")
```

**What Happens:**
```
Without Pruning:
├─ Read ALL 4 partitions
├─ Then filter
└─ Process 1 partition worth of data

With Pruning:
├─ Read ONLY 2021-02-03 partition
├─ Skip other 3 partitions entirely
└─ Process 1 partition worth of data

Benefit: 75% less data read!
```

---

### Example: Basic Partition Pruning

**Dataset Setup:**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName("Partition Pruning").getOrCreate()

# Read partitioned data
orders_df = spark.read.parquet("orders")  # Partitioned by order_date

# Filter by partition column
result = orders_df \
    .filter(col("order_date") == "2021-02-03") \
    .groupBy("product_id") \
    .agg(sum("amount").alias("total_sales"))

result.show()
```

**Execution Plan:**
```
== Physical Plan ==
HashAggregate [product_id], [sum(amount)]
+- Exchange hashpartitioning(product_id, 200)
   +- HashAggregate [product_id], [partial_sum(amount)]
      +- FileScan parquet
         PartitionFilters: [order_date = 2021-02-03]  ← Pruning!
         ReadSchema: struct<order_id, product_id, amount>
         PartitionCount: 1  ← Only 1 partition read!
         Files: 1
```

**Key Details:**
- `PartitionFilters`: Filter pushed to scan
- `PartitionCount: 1`: Only 1 partition read (not all 4)
- `Files: 1`: Only 1 file read

**Benefit:** Read 1 partition instead of 4 → **75% less I/O**

---

## The Problem DPP Solves

### Data Warehouse Setup

**Fact Table: orders** (Large - millions/billions of rows)

| Column | Type | Description |
|--------|------|-------------|
| order_id | String | Unique order ID |
| order_date | Date | **Partition column** |
| product_id | String | Product identifier |
| amount | Decimal | Order amount |

**Partitioned Structure:**
```
orders/
├── order_date=2021-01-01/
├── order_date=2021-01-02/
├── ...
├── order_date=2021-02-01/
├── order_date=2021-02-02/
├── order_date=2021-02-03/
├── order_date=2021-02-04/
├── ...
└── order_date=2021-12-31/
```

**Dimension Table: dates** (Small - one row per date)

| Column | Type | Description |
|--------|------|-------------|
| date | Date | Calendar date |
| year | Int | Year (2021) |
| month | Int | Month (1-12) |
| day | Int | Day (1-31) |
| quarter | Int | Quarter (1-4) |

---

### The Query Problem

**Common Business Query:**
```sql
SELECT SUM(o.amount) AS total_sales
FROM orders o
JOIN dates d ON o.order_date = d.date
WHERE d.year = 2021 AND d.month = 2
```

**DataFrame API:**
```python
result = orders_df.join(dates_df, orders_df.order_date == dates_df.date) \
    .filter((col("year") == 2021) & (col("month") == 2)) \
    .agg(sum("amount").alias("total_sales"))
```

**What We Want:**
- Read only February 2021 partitions from orders table
- Skip all other months (10 months of data)

**The Problem:**
- Filter condition is on `dates` table (year, month columns)
- NOT on `orders` table (order_date column)
- Spark cannot apply partition pruning!

---

### Without DPP (Spark 2.x behavior)

**Execution Plan:**
```
== Physical Plan ==
Aggregate [sum(amount)]
+- SortMergeJoin [order_date], [date]
   ├─ Sort [order_date]
   │  +- Exchange hashpartitioning(order_date, 200)
   │     +- FileScan parquet orders
   │        PartitionFilters: []  ← NO PRUNING!
   │        PartitionCount: 365  ← ALL partitions!
   │        Files: 365  ← Reading entire year!
   │
   └─ Sort [date]
      +- Exchange hashpartitioning(date, 200)
         +- FileScan dates
            PushedFilters: [year=2021, month=2]
```

**What Happens:**
```
Orders table:
├─ Reads ALL 365 partitions (entire year)
├─ Shuffles ALL data
├─ Joins with dates
└─ Then filters to February (28 days)

Result: Read 365 days, used 28 days
Wasted: 337 days of data (92% waste!)
```

**Problem Visualization:**
```
┌─────────────────────────────────────┐
│  Orders Table (365 partitions)     │
│                                     │
│  Jan (31) ████████ ← Read but waste│
│  Feb (28) ███████  ← Actually need │
│  Mar (31) ████████ ← Read but waste│
│  Apr (30) ███████  ← Read but waste│
│  May (31) ████████ ← Read but waste│
│  ...                                │
│  Dec (31) ████████ ← Read but waste│
│                                     │
│  Total Read: 365 partitions         │
│  Actually Used: 28 partitions       │
│  Waste: 92%!                        │
└─────────────────────────────────────┘
```

---

## How DPP Works

### The DPP Solution

**Enable DPP + Broadcast Dimension:**
```python
from pyspark.sql.functions import broadcast

result = orders_df.join(
    broadcast(dates_df),  # ← Broadcast dimension table
    orders_df.order_date == dates_df.date
).filter(
    (col("year") == 2021) & (col("month") == 2)
).agg(sum("amount").alias("total_sales"))
```

### DPP Workflow

```
Step 1: Broadcast dates table with filters
        ↓
   dates WHERE year=2021 AND month=2
        ↓
   Returns: [2021-02-01, 2021-02-02, ..., 2021-02-28]
        ↓
Step 2: Create subquery from broadcast result
        ↓
   Subquery: order_date IN (2021-02-01, ..., 2021-02-28)
        ↓
Step 3: Inject subquery into orders table scan
        ↓
   Scan orders WHERE order_date IN (subquery)
        ↓
Step 4: Apply partition pruning
        ↓
   Read ONLY February 2021 partitions (28 files)
        ↓
Step 5: Join already-filtered data
```

---

### With DPP (Spark 3.0+ behavior)

**Execution Plan:**
```
== Physical Plan ==
Aggregate [sum(amount)]
+- BroadcastHashJoin [order_date], [date]
   ├─ FileScan parquet orders
   │  PartitionFilters: [dynamicpruningexpression(  ← DPP!
   │     order_date IN (subquery#123))]
   │  PartitionCount: 28  ← Only February!
   │  Files: 28  ← Only 28 files!
   │  SubqueryBroadcast  ← Injected subquery
   │
   └─ BroadcastExchange
      +- FileScan dates
         PushedFilters: [year=2021, month=2]
```

**What Happens:**
```
Dates table:
├─ Filter: year=2021, month=2
├─ Returns: 28 dates in February
├─ Broadcast to all executors
└─ Create subquery

Orders table:
├─ Receive subquery: order_date IN (Feb dates)
├─ Apply partition pruning
├─ Read ONLY 28 February partitions
└─ Join with broadcasted dates

Result: Read 28 days, used 28 days
Waste: 0%!
```

**Optimization Visualization:**
```
┌─────────────────────────────────────┐
│  Orders Table (365 partitions)     │
│                                     │
│  Jan (31) ········ ← SKIPPED       │
│  Feb (28) ███████  ← READ (DPP!)   │
│  Mar (31) ········ ← SKIPPED       │
│  Apr (30) ········ ← SKIPPED       │
│  May (31) ········ ← SKIPPED       │
│  ...                                │
│  Dec (31) ········ ← SKIPPED       │
│                                     │
│  Total Read: 28 partitions          │
│  Actually Used: 28 partitions       │
│  Efficiency: 100%!                  │
└─────────────────────────────────────┘
```

---

### The Magic: Subquery Injection

**Conceptual Transformation:**

**Original Query:**
```sql
SELECT SUM(o.amount)
FROM orders o
JOIN dates d ON o.order_date = d.date
WHERE d.year = 2021 AND d.month = 2
```

**DPP Transforms To:**
```sql
SELECT SUM(o.amount)
FROM orders o
WHERE o.order_date IN (
    SELECT date 
    FROM dates 
    WHERE year = 2021 AND month = 2
)  -- ← Subquery injected by DPP!
JOIN dates d ON o.order_date = d.date
WHERE d.year = 2021 AND d.month = 2
```

**Now Partition Pruning Can Work:**
- Filter is on `orders.order_date` (partition column)
- Spark can prune partitions before join
- Read only February data

---

## Configuration

### Enable/Disable DPP

```python
# Check if enabled (default in Spark 3.0+)
spark.conf.get("spark.sql.optimizer.dynamicPartitionPruning.enabled")
# Returns: "true"

# Disable DPP (not recommended)
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")

# Re-enable
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
```

### Related Configurations

```python
# Use broadcast for DPP reuse
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.useStats", "true")

# Pruning side extra filter ratio
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.pruningSideExtraFilterRatio", "0.1")

# Fallback filter ratio
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio", "0.5")
```

**Note:** Default values work well for most use cases. Tune only if needed.

---

## Complete Example

### Setup

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, broadcast
from datetime import date, timedelta

spark = SparkSession.builder \
    .appName("Dynamic Partition Pruning Example") \
    .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true") \
    .getOrCreate()

# Create sample orders data (partitioned by order_date)
# In reality, this would be a huge table
orders_data = []
base_date = date(2021, 1, 1)
for i in range(365):
    current_date = base_date + timedelta(days=i)
    for j in range(1000):  # 1000 orders per day
        orders_data.append((
            f"order_{i}_{j}",
            current_date,
            f"product_{j % 100}",
            (j % 100) * 10.0
        ))

orders_df = spark.createDataFrame(
    orders_data,
    ["order_id", "order_date", "product_id", "amount"]
)

# Write as partitioned parquet
orders_df.write \
    .partitionBy("order_date") \
    .mode("overwrite") \
    .parquet("orders_partitioned")

# Create dates dimension table
dates_data = []
for i in range(365):
    current_date = base_date + timedelta(days=i)
    dates_data.append((
        current_date,
        current_date.year,
        current_date.month,
        current_date.day,
        (current_date.month - 1) // 3 + 1
    ))

dates_df = spark.createDataFrame(
    dates_data,
    ["date", "year", "month", "day", "quarter"]
)
```

---

### Without DPP (Disabled)

```python
# Disable DPP
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")

# Read partitioned data
orders_df = spark.read.parquet("orders_partitioned")

# Join without broadcast
result = orders_df.join(
    dates_df,
    orders_df.order_date == dates_df.date
).filter(
    (col("year") == 2021) & (col("month") == 2)
).agg(sum("amount").alias("total_sales"))

# Execute and time
import time
start = time.time()
result.show()
end = time.time()

print(f"Execution time (without DPP): {end - start:.2f} seconds")

# Check plan
result.explain()
```

**Output:**
```
Execution time (without DPP): 45.23 seconds

Files read: 365
Partitions scanned: 365
Data read: 3.65 GB
```

---

### With DPP (Enabled + Broadcast)

```python
# Enable DPP
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

# Read partitioned data
orders_df = spark.read.parquet("orders_partitioned")

# Join WITH broadcast (required for DPP)
result = orders_df.join(
    broadcast(dates_df),  # ← Broadcast dimension
    orders_df.order_date == dates_df.date
).filter(
    (col("year") == 2021) & (col("month") == 2)
).agg(sum("amount").alias("total_sales"))

# Execute and time
start = time.time()
result.show()
end = time.time()

print(f"Execution time (with DPP): {end - start:.2f} seconds")

# Check plan
result.explain()
```

**Output:**
```
Execution time (with DPP): 3.47 seconds (13× faster!)

Files read: 28
Partitions scanned: 28 (92% reduction!)
Data read: 0.28 GB (92% reduction!)
```

---

### SQL Example

```python
# Register tables
orders_df.createOrReplaceTempView("orders")
dates_df.createOrReplaceTempView("dates")

# SQL with DPP (if dates is small, auto-broadcast may apply)
result = spark.sql("""
    SELECT /*+ BROADCAST(d) */ SUM(o.amount) AS total_sales
    FROM orders o
    JOIN dates d ON o.order_date = d.date
    WHERE d.year = 2021 AND d.month = 2
""")

result.show()
result.explain()
```

---

### Performance Comparison

| Metric | Without DPP | With DPP | Improvement |
|--------|-------------|----------|-------------|
| **Execution Time** | 45.23s | 3.47s | **13× faster** |
| **Files Read** | 365 | 28 | **92% reduction** |
| **Partitions Scanned** | 365 | 28 | **92% reduction** |
| **Data Read** | 3.65 GB | 0.28 GB | **92% reduction** |
| **Network Shuffle** | 3.65 GB | 0.28 GB | **92% reduction** |

---

## Requirements for DPP

### Three Essential Requirements

DPP will **only work** if ALL three conditions are met:

#### 1. Fact-Dimension Setup

**Required:**
- One **large table** (fact table) - millions/billions of rows
- One **small table** (dimension table) - thousands/millions of rows

**Example:**
```
✓ orders (1 billion rows) JOIN dates (365 rows)
✓ sales (100 million rows) JOIN stores (1000 rows)
✗ users (10M rows) JOIN transactions (10M rows) - both large!
```

---

#### 2. Fact Table Must Be Partitioned

**Required:**
- Fact table partitioned on join key
- Partition column used in join condition

**Example:**
```python
# ✓ Good: Partitioned on join column
orders partitioned by order_date
JOIN on orders.order_date = dates.date

# ✗ Bad: Not partitioned on join column
orders partitioned by region
JOIN on orders.order_date = dates.date
```

---

#### 3. Dimension Table Must Be Broadcasted

**Required:**
- Explicitly broadcast dimension using `broadcast()`
- OR dimension is small enough (< 10 MB) for auto-broadcast

**Example:**
```python
# ✓ Good: Explicit broadcast
orders.join(broadcast(dates_df), ...)

# ✓ Good: Auto-broadcast (if < 10 MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
orders.join(dates_df, ...)  # Auto-broadcast if dates < 10 MB

# ✗ Bad: No broadcast, large dimension
orders.join(large_dimension_df, ...)  # Sort-merge join, no DPP
```

---

### Verification Checklist

```python
# 1. Check if DPP is enabled
assert spark.conf.get("spark.sql.optimizer.dynamicPartitionPruning.enabled") == "true"

# 2. Verify fact table is partitioned
spark.catalog.listTables()  # Check if partitioned
# Or check physical files
# orders/order_date=2021-02-01/
# orders/order_date=2021-02-02/
# ...

# 3. Ensure broadcast is applied
# Use broadcast() function explicitly
# OR check dimension table size < broadcast threshold

# 4. Verify in execution plan
result.explain()
# Look for:
# - "dynamicpruningexpression"
# - "SubqueryBroadcast"
# - Reduced "PartitionCount"
```

---

## Best Practices

### 1. Always Enable DPP (Default in Spark 3.0+)

```python
# Already enabled by default, but explicitly set for clarity
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
```

---

### 2. Explicitly Broadcast Dimension Tables

```python
# Don't rely on auto-broadcast
# Explicitly broadcast for clarity and control

from pyspark.sql.functions import broadcast

# ✓ Good
result = fact_df.join(broadcast(dim_df), join_condition)

# ✗ Less predictable
result = fact_df.join(dim_df, join_condition)  # May or may not broadcast
```

---

### 3. Partition Fact Tables on Common Join/Filter Columns

```python
# Partition on columns frequently used in:
# - Joins with dimensions
# - WHERE clauses
# - Time-based queries

# ✓ Good partitioning strategy
fact_df.write \
    .partitionBy("order_date") \  # Commonly joined with date dimension
    .parquet("fact_table")

# ✗ Poor partitioning
fact_df.write \
    .partitionBy("random_id") \  # Rarely used in filters
    .parquet("fact_table")
```

---

### 4. Monitor Execution Plans

```python
# Always check if DPP is working
result.explain()

# Look for these indicators:
# ✓ dynamicpruningexpression
# ✓ SubqueryBroadcast
# ✓ Reduced PartitionCount
# ✓ Reduced Files

# Example output showing DPP working:
"""
FileScan parquet
PartitionFilters: [dynamicpruningexpression(
    order_date IN (subquery#123))]
PartitionCount: 28  ← Reduced from 365!
SubqueryBroadcast  ← DPP active!
"""
```

---

### 5. Design Star Schema for Data Warehouses

**Star Schema Benefits DPP:**

```
        ┌──────────┐
        │ Dates    │ ← Dimension (small)
        │  365 rows│
        └────┬─────┘
             │
        ┌────┴─────┐
        │  Orders  │ ← Fact (large, partitioned)
        │ 1B rows  │
        └────┬─────┘
             │
     ┌───────┴────────┐
     │                │
┌────┴─────┐   ┌─────┴────┐
│ Products │   │ Customers│ ← Dimensions (small)
│ 10K rows │   │ 1M rows  │
└──────────┘   └──────────┘
```

**Queries benefit from DPP:**
```sql
SELECT SUM(amount)
FROM orders o
JOIN dates d ON o.order_date = d.date
JOIN products p ON o.product_id = p.id
JOIN customers c ON o.customer_id = c.id
WHERE d.year = 2021 AND d.month = 2
  AND p.category = 'Electronics'
```

DPP prunes orders partitions based on dates filter!

---

### 6. Tune Broadcast Threshold for Your Cluster

```python
# Default: 10 MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")

# Large cluster with ample memory - increase
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600")  # 100 MB

# Small cluster - decrease
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "5242880")  # 5 MB
```

---

### 7. Combine DPP with AQE

```python
# Enable all Spark 3.0+ optimizations together
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

# Benefits:
# - DPP: Prunes partitions dynamically
# - AQE: Optimizes join strategy, coalesces partitions, handles skew
# Together: Maximum query optimization!
```

---

## Common Scenarios

### Scenario 1: Time-Based Reporting

```python
# Monthly sales report
monthly_sales = orders_df.join(
    broadcast(dates_df),
    orders_df.order_date == dates_df.date
).filter(
    (col("year") == 2021) & (col("month") == 2)
).groupBy("product_id") \
 .agg(sum("amount").alias("total_sales"))

# DPP prunes to only February partitions
# Reads 28 days instead of 365 days
# 92% data reduction!
```

---

### Scenario 2: Product Category Analysis

```python
# Fact: sales (partitioned by sale_date)
# Dim: products (small, contains category)

category_sales = sales_df.join(
    broadcast(products_df.filter(col("category") == "Electronics")),
    sales_df.product_id == products_df.id
).groupBy("sale_date") \
 .agg(sum("amount").alias("daily_sales"))

# DPP helps if filtering on dates dimension too
```

---

### Scenario 3: Geographic Analysis

```python
# Fact: transactions (partitioned by date)
# Dim: stores (small, contains region)

regional_sales = transactions_df.join(
    broadcast(stores_df.filter(col("region") == "West")),
    transactions_df.store_id == stores_df.id
).join(
    broadcast(dates_df),
    transactions_df.date == dates_df.date
).filter(
    col("quarter") == 1
).agg(sum("amount"))

# DPP prunes dates first (Q1 only)
# Reads 90 days instead of 365 days
```

---

## Summary

### Key Concepts

1. **DPP Definition:**
   - Takes filters from dimension table
   - Injects as subquery into fact table
   - Enables partition pruning on fact table

2. **Requirements (ALL must be met):**
   - Fact-dimension setup (large + small table)
   - Fact table partitioned on join key
   - Dimension table broadcasted

3. **Benefits:**
   - 90%+ reduction in data read
   - 10-50× faster query execution
   - Automatic optimization (no code changes needed)

4. **Configuration:**
   - Enabled by default in Spark 3.0+
   - Explicitly broadcast dimension tables
   - Verify with explain()

### Before vs After

**Without DPP (Spark 2.x):**
```
Read ALL partitions → Join → Filter → Result
Time: 45 seconds
Data Read: 3.65 GB
```

**With DPP (Spark 3.0+):**
```
Filter dimension → Prune partitions → Read ONLY needed → Join → Result
Time: 3 seconds (15× faster!)
Data Read: 0.28 GB (92% less!)
```

### Configuration Checklist

```python
# Essential configuration
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

# Recommended: Combine with AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Tune broadcast threshold as needed
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10 MB

# Always explicitly broadcast dimensions
from pyspark.sql.functions import broadcast
result = fact_df.join(broadcast(dim_df), condition)
```

### When to Use DPP

**✓ Ideal for:**
- Data warehouse star/snowflake schemas
- Time-based reporting with date dimensions
- Large fact tables with dimension lookups
- Partitioned fact tables

**✗ Not applicable for:**
- Both tables are large
- No partitioning on fact table
- Join key != partition column
- Dimension table cannot be broadcasted

---

## Final Thoughts

**Dynamic Partition Pruning is one of Spark 3.0's most impactful features!**

**Simple to use:**
1. Enable DPP (already default)
2. Broadcast dimension tables
3. Let Spark optimize automatically

**Massive impact:**
- 90%+ data reduction
- 10-50× faster queries
- Zero code changes

**Combine with AQE for maximum performance:**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
```

Master DPP to build lightning-fast data warehouse queries!