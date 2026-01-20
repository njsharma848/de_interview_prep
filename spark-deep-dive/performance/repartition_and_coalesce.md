# Spark Repartition and Coalesce - Complete Guide

Understanding how to control DataFrame partitioning for optimal performance.

---

## Table of Contents
- [Overview](#overview)
- [repartition() Method](#repartition-method)
- [repartitionByRange() Method](#repartitionbyrange-method)
- [When to Repartition](#when-to-repartition)
- [coalesce() Method](#coalesce-method)
- [repartition() vs coalesce()](#repartition-vs-coalesce)
- [Best Practices](#best-practices)
- [Complete Examples](#complete-examples)

---

## Overview

Spark provides methods to control DataFrame partitioning:

| Method | Purpose | Shuffle? | Direction |
|--------|---------|----------|-----------|
| **repartition()** | Redistribute data evenly | Yes | Increase or decrease |
| **repartitionByRange()** | Partition by value ranges | Yes | Increase or decrease |
| **coalesce()** | Reduce partitions locally | No | Decrease only |

---

## repartition() Method

### Two Repartition Methods

Spark offers two repartitioning methods:

1. **repartition()** - Hash-based partitioning
2. **repartitionByRange()** - Range-based partitioning

### Key Difference

**repartition():**
- Uses **hash function** to determine target partition
- Example: hash(row) % num_partitions → partition_id
- More uniform distribution

**repartitionByRange():**
- Uses **range of values** to determine partition
- Example: 0-10 → partition 1, 10-20 → partition 2
- Better for range queries (e.g., time-series data)

---

## repartition() Method Details

### Method Signature

```python
df.repartition(numPartitions)
df.repartition(col1, col2, ...)
df.repartition(numPartitions, col1, col2, ...)
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `numPartitions` | int | Optional* | Target number of partitions |
| `col1, col2, ...` | Column | Optional* | Columns to partition by |

**At least one parameter is required**

### Valid Calls

```python
# Valid: Number only
df.repartition(10)

# Valid: Column only
df.repartition("order_date")

# Valid: Multiple columns
df.repartition("year", "month")

# Valid: Number + columns
df.repartition(5, "order_date")

# Invalid: No arguments
df.repartition()  # ERROR!
```

---

### How repartition() Works

**Key Characteristics:**
- **Wide dependency transformation** (causes shuffle/sort)
- Creates new partitions across the cluster
- Expensive operation (network I/O)

---

### Example 1: Uniform Partitions (Number Only)

**Code:**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Repartition Example").getOrCreate()

# Create DataFrame with 1 million rows
df = spark.range(1000000)

# Repartition to 10 uniform partitions
repartitioned_df = df.repartition(10)

# Cache to see in Spark UI
repartitioned_df.cache().count()

# Check partition count
print(f"Partitions: {repartitioned_df.rdd.getNumPartitions()}")
# Output: Partitions: 10
```

**What Happens:**
```
Original DataFrame (variable partitions)
    ↓
Shuffle/Sort (hash-based on row data)
    ↓
10 New Uniform Partitions (evenly distributed)
```

**Spark UI Storage Tab:**
```
Number of Partitions: 10

Partition 1: 7.9 MB
Partition 2: 7.9 MB
Partition 3: 7.9 MB
Partition 4: 7.9 MB
Partition 5: 7.9 MB
Partition 6: 7.9 MB
Partition 7: 7.9 MB
Partition 8: 7.9 MB
Partition 9: 7.9 MB
Partition 10: 7.9 MB

All partitions approximately uniform size! ✓
```

**Result:** 10 approximately uniform partitions of equal size

---

### Example 2: Repartition by Column

**Code:**
```python
# Configure shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "10")

# Read parquet data
orders_df = spark.read.parquet("orders.parquet")

# Repartition by order_date column
repartitioned_df = orders_df.repartition("order_date")

# How many partitions?
print(f"Partitions: {repartitioned_df.rdd.getNumPartitions()}")
# Output: Partitions: 10
```

**What Happens:**
```
orders_df
    ↓
Hash on order_date column
    ↓
Shuffle/Sort
    ↓
10 Partitions (from spark.sql.shuffle.partitions)
```

**Key Point:** Number of partitions determined by `spark.sql.shuffle.partitions` configuration (default: 200)

---

### Example 3: Override Partition Count

**Code:**
```python
# Read data
orders_df = spark.read.parquet("orders.parquet")

# Repartition to 5 partitions based on order_date
repartitioned_df = orders_df.repartition(5, "order_date")

print(f"Partitions: {repartitioned_df.rdd.getNumPartitions()}")
# Output: Partitions: 5

# Cache to see sizes
repartitioned_df.cache().count()
```

**Spark UI:**
```
Number of Partitions: 5

Partition 1: 12.3 MB  ← Not uniform!
Partition 2: 8.7 MB
Partition 3: 15.1 MB
Partition 4: 9.2 MB
Partition 5: 14.8 MB

Partitions NOT uniform (column-based partitioning)
```

**Important:** Repartitioning by column does **NOT guarantee uniform partition sizes** due to data distribution

**Why?** Data distribution depends on the actual values in the column. If certain values appear more frequently, those partitions will be larger.

---

### Why Partition Sizes Vary by Column

**Reason:** Data distribution of the column values

**Example:**
```python
orders_df.groupBy("order_date").count().show()

+----------+-------+
|order_date|  count|
+----------+-------+
|2021-01-15| 50000| ← More rows for this date
|2021-01-16| 30000|
|2021-01-17| 45000|
|2021-01-18| 25000| ← Fewer rows for this date
|2021-01-19| 60000| ← Most rows for this date
+----------+-------+

Result: Partitions have different sizes based on row counts per date
```

---

## repartitionByRange() Method

### Method Signature

```python
df.repartitionByRange(numPartitions, col1, col2, ...)
df.repartitionByRange(col1, col2, ...)
```

### Range-Based Partitioning

**How it works:**
1. Samples data to determine value ranges
2. Creates partitions based on ranges
3. Example: 0-100 → P1, 100-200 → P2, 200-300 → P3

**Use case:** Sorted data, range queries, time-series data

---

### Example: Range Partitioning

**Code:**
```python
# Repartition by range on order_date
range_partitioned = orders_df.repartitionByRange(5, "order_date")

# Data sorted within partitions by order_date
# Partition 1: 2021-01-01 to 2021-01-10
# Partition 2: 2021-01-11 to 2021-01-20
# Partition 3: 2021-01-21 to 2021-01-31
# ...
```

**Benefits:**
- Optimizes range queries: `WHERE order_date BETWEEN '2021-01-01' AND '2021-01-10'`
- Data naturally sorted within partitions
- Better for time-series analysis

---

### Important: Sampling and Non-Determinism

**repartitionByRange() uses data sampling to determine ranges:**

```python
# Run 1
df.repartitionByRange(5, "amount").show()
# Ranges might be: 0-50, 50-100, 100-150, 150-200, 200+

# Run 2 (same code, same data)
df.repartitionByRange(5, "amount").show()
# Ranges might be: 0-48, 48-98, 98-148, 148-198, 198+  ← Slightly different!
```

**Why?** Sampling is probabilistic, so ranges may vary slightly between executions

**Impact:** Usually minimal and acceptable for most use cases. The ranges will be close to each other.

**When to be careful:** 
- If you need exact reproducibility, consider using `repartition()` with sorted data instead
- For most analytical workloads, the variation is negligible

---

## When to Repartition

### ⚠️ Repartition is Expensive!

**Cost:** Full shuffle/sort operation across the network
- **Network I/O:** Data transferred between executors
- **Disk I/O:** Shuffle files written to local disk
- **CPU:** Serialization/deserialization overhead
- **Memory:** Exchange buffers for shuffle data

**Rule:** Only repartition when benefits clearly outweigh costs!

**Typical Cost:** 20-50% overhead of your query execution time

---

### ✅ Good Reasons to Repartition

#### 1. Reusing DataFrame with Column Filters

**Scenario:**
```python
# Large DataFrame reused multiple times
sales_df = spark.read.parquet("sales.parquet")  # 1 billion rows

# Multiple queries on same column
result1 = sales_df.filter(col("region") == "West").agg(...)
result2 = sales_df.filter(col("region") == "East").agg(...)
result3 = sales_df.filter(col("region") == "North").agg(...)
```

**Problem:** Each query scans entire DataFrame

**Solution:**
```python
# Repartition once, reuse many times
repartitioned_sales = sales_df.repartition("region").cache()

# Now each query only scans relevant partition
result1 = repartitioned_sales.filter(col("region") == "West").agg(...)
result2 = repartitioned_sales.filter(col("region") == "East").agg(...)
result3 = repartitioned_sales.filter(col("region") == "North").agg(...)

# Benefit: Faster queries, better partition pruning
```

---

#### 2. Too Few Partitions (Poor Parallelism)

**Scenario:**
```python
df = spark.read.csv("data.csv")  # Single large CSV file
print(f"Partitions: {df.rdd.getNumPartitions()}")
# Output: Partitions: 1  ← Only 1 partition!

# Cluster has 100 cores, but only 1 task can run
# 99 cores sitting idle!
```

**Problem:** Single partition → no parallelism

**Solution:**
```python
# Repartition to match cluster capacity
repartitioned_df = df.repartition(100)

# Now 100 tasks can run in parallel
# All 100 cores utilized!
```

**Benefit:** Better cluster utilization, faster processing

---

#### 3. Large or Skewed Partitions

**Scenario:**
```python
# Check partition sizes (in rows)
partition_sizes = df.rdd.glom().map(len).collect()
print(partition_sizes)
# Output: [100000, 150000, 5000000, 80000, 120000]
#                          ↑
#                   Skewed partition (50× larger!)

# Check partition sizes in memory (more accurate)
df.cache().count()
# Check Spark UI → Storage tab to see actual sizes
```

**Problem:** One huge partition becomes a bottleneck
- Single task takes much longer than others
- Risk of OOM on that executor
- Poor cluster utilization (other executors finish early and wait)

**Solution:**
```python
# Repartition to create uniform partitions
uniform_df = df.repartition(10)

# Check new sizes
uniform_df.rdd.glom().map(len).collect()
# Output: [540000, 535000, 545000, 538000, 542000, ...]
#         All similar sizes! ✓
```

**Benefit:** Balanced workload, no bottlenecks, better performance

---

### ❌ Bad Reasons to Repartition

#### 1. Reducing Partition Count

```python
# ❌ Bad: Using repartition to reduce partitions
df.repartition(5)  # From 100 partitions → 5 partitions
                   # Causes full shuffle!

# ✓ Good: Use coalesce instead
df.coalesce(5)  # No shuffle, just combines local partitions
```

#### 2. No Clear Benefit

```python
# ❌ Bad: Repartitioning without reason
df = spark.read.parquet("data.parquet")  # Already well-partitioned
df = df.repartition(100)  # Unnecessary shuffle!
result = df.count()  # Simple operation, didn't need repartition
```

#### 3. Before Writing Small Output

```python
# ❌ Bad: Repartition before writing small result
large_df = spark.read.parquet("large_data.parquet")
result = large_df.groupBy("category").count()  # Results in 10 rows
result.repartition(100).write.parquet("output/")  # Unnecessary!

# ✓ Good: Write directly or coalesce
result.coalesce(1).write.parquet("output/")  # Single output file
```

---

## coalesce() Method

### Method Signature

```python
df.coalesce(numPartitions)
```

### How coalesce() Works

**Key Characteristic:** Combines **local partitions only** (no shuffle!)

**How it works:**
1. Spark identifies which partitions are on which executors
2. Combines partitions locally on each executor
3. No data movement across the network
4. Results in fewer partitions without shuffle overhead

**Example:**

**Before coalesce:**
```
10-node Cluster
20 Partitions distributed:

Node 1: [P1] [P2]      (2 partitions)
Node 2: [P3] [P4]      (2 partitions)
Node 3: [P5] [P6]      (2 partitions)
Node 4: [P7] [P8]      (2 partitions)
Node 5: [P9] [P10]     (2 partitions)
Node 6: [P11] [P12]    (2 partitions)
Node 7: [P13] [P14]    (2 partitions)
Node 8: [P15] [P16]    (2 partitions)
Node 9: [P17] [P18]    (2 partitions)
Node 10: [P19] [P20]   (2 partitions)
```

**Execute: df.coalesce(10)**

**After coalesce:**
```
10-node Cluster
10 Partitions (combined locally):

Node 1: [P1+P2]        ← Combined locally (no network transfer)
Node 2: [P3+P4]        ← Combined locally
Node 3: [P5+P6]        ← Combined locally
Node 4: [P7+P8]        ← Combined locally
Node 5: [P9+P10]       ← Combined locally
Node 6: [P11+P12]      ← Combined locally
Node 7: [P13+P14]      ← Combined locally
Node 8: [P15+P16]      ← Combined locally
Node 9: [P17+P18]      ← Combined locally
Node 10: [P19+P20]     ← Combined locally

Result: 10 partitions, NO shuffle, NO network I/O!
```

---

### Example: Coalesce After Processing

**Code:**
```python
# Read data (200 partitions)
df = spark.read.parquet("large_data.parquet")
print(f"Initial partitions: {df.rdd.getNumPartitions()}")
# Output: Initial partitions: 200

# Process data (still 200 partitions)
processed_df = df.filter(col("amount") > 1000) \
                 .select("id", "amount", "date")

# After filtering, much less data
# 200 partitions no longer needed

# Reduce to 10 partitions without shuffle
final_df = processed_df.coalesce(10)
print(f"Final partitions: {final_df.rdd.getNumPartitions()}")
# Output: Final partitions: 10

# Write to disk (10 output files instead of 200)
final_df.write.parquet("output/")
```

**Benefit:** Reduced from 200 files to 10 files, no shuffle overhead!

---

### coalesce() Key Points

#### 1. No Shuffle (Fast!)

```python
# coalesce is much faster than repartition for reducing partitions
df.coalesce(10)      # No shuffle ✓
df.repartition(10)   # Full shuffle ✗
```

---

#### 2. Can Only Reduce (Never Increase)

```python
df = spark.range(1000000).repartition(10)
print(f"Partitions: {df.rdd.getNumPartitions()}")
# Output: Partitions: 10

# Try to increase with coalesce
df_increased = df.coalesce(20)
print(f"Partitions: {df_increased.rdd.getNumPartitions()}")
# Output: Partitions: 10  ← Still 10! coalesce did nothing!

# Must use repartition to increase
df_increased = df.repartition(20)
print(f"Partitions: {df_increased.rdd.getNumPartitions()}")
# Output: Partitions: 20  ← Now 20!
```

**Rule:** coalesce(N) where N > current partitions → **does nothing**

---

#### 3. Can Cause Skewed Partitions

**Problem:** Combining local partitions may create imbalanced sizes, especially with drastic reduction

**Why this happens:**
- coalesce() combines partitions based on locality, not size
- If original partitions are uneven or distributed unevenly across nodes
- Drastic reduction (e.g., 1000 → 10) can amplify imbalances

**Example:**
```python
# Original: 100 balanced partitions
df.rdd.glom().map(len).collect()
# [10000, 10000, 10000, ...] ← All similar

# Coalesce to 10 (10:1 reduction - reasonable)
coalesced = df.coalesce(10)
coalesced.rdd.glom().map(len).collect()
# [100000, 100000, 100000, ...] ← Still balanced (10× larger each)

# Coalesce to 3 (33:1 reduction - too drastic!)
over_coalesced = df.coalesce(3)
over_coalesced.rdd.glom().map(len).collect()
# [450000, 280000, 270000] ← Unbalanced!
#    ↑ Skewed partition (60% larger than smallest)
```

**Warning:** Drastic reduction ratios (>20:1) increase risk of skew!

**Solution if skew is problematic:** Use `repartition()` instead for uniform partitions (but costs shuffle)

---

#### 4. Risk of OOM with Extreme Reduction

**Scenario:**
```python
# 1000 partitions, each 100 MB = 100 GB total
df = spark.read.parquet("large_data.parquet")
print(f"Partitions: {df.rdd.getNumPartitions()}")
# Output: Partitions: 1000

# Coalesce to 1 partition - DANGEROUS!
single_partition = df.coalesce(1)  
print(f"Partitions: {single_partition.rdd.getNumPartitions()}")
# Output: Partitions: 1

# Attempting to process single 100 GB partition
single_partition.count()
# Single partition = 100 GB!
# Executor memory: 16 GB
# Result: java.lang.OutOfMemoryError! 💥
```

**Why OOM happens:**
- Single executor must hold entire partition in memory
- 100 GB partition > 16 GB executor memory
- Cannot spill to disk fast enough

**Best Practice:** 
- Keep partitions smaller than executor memory
- Don't reduce too drastically (avoid >50:1 reduction ratios)
- Rule of thumb: Each partition should be 100-200 MB

**Safe alternative:**
```python
# Reduce more conservatively
safer_df = df.coalesce(100)  # 100 GB / 100 = 1 GB per partition ✓
```

---

## repartition() vs coalesce()

### Quick Comparison

| Aspect | repartition() | coalesce() |
|--------|---------------|------------|
| **Shuffle** | Yes (expensive) | No (cheap) |
| **Can Increase** | ✓ Yes | ✗ No |
| **Can Decrease** | ✓ Yes | ✓ Yes |
| **Uniform Partitions** | ✓ Usually | ✗ May be skewed |
| **Speed** | Slower | Faster |
| **Use For** | Increase, rebalance | Decrease only |

---

### When to Use Which

**Use repartition() when:**
- ✓ Increasing partition count
- ✓ Need uniform partition sizes
- ✓ Repartitioning by column for optimization
- ✓ Rebalancing skewed partitions

**Use coalesce() when:**
- ✓ Reducing partition count
- ✓ Want to avoid shuffle
- ✓ Writing output (reduce file count)
- ✓ After filtering (less data, fewer partitions needed)

---

### Decision Tree

```
Need to change partition count?
│
├─ Increase partitions
│  └─ Use repartition()
│
└─ Decrease partitions
   │
   ├─ Need uniform sizes / rebalancing
   │  └─ Use repartition() (with shuffle)
   │
   └─ Just want fewer partitions
      └─ Use coalesce() (no shuffle)
```

---

## Best Practices

### 1. Avoid Unnecessary Repartitioning

```python
# ❌ Bad: Unnecessary repartition
df = spark.read.parquet("well_partitioned_data.parquet")
df = df.repartition(100)  # Already optimally partitioned!
df.count()

# ✓ Good: Only repartition if needed
df = spark.read.parquet("well_partitioned_data.parquet")
if df.rdd.getNumPartitions() < 10:  # Too few partitions
    df = df.repartition(100)
df.count()
```

---

### 2. Use coalesce() to Reduce, Not repartition()

```python
# ❌ Bad: Full shuffle to reduce
df.repartition(10)  # From 200 → 10 with shuffle

# ✓ Good: No shuffle to reduce
df.coalesce(10)  # From 200 → 10 without shuffle
```

**Exception:** Use repartition() if you need uniform partition sizes

---

### 3. Match Partition Count to Cluster Resources

```python
# Cluster: 10 executors × 4 cores = 40 total cores available

# ✓ Good: Partition count = 2-3× cores for optimal parallelism
df.repartition(100)  # 100 partitions / 40 cores = 2.5 partitions per core
                     # Good balance: enough parallelism, not too much overhead

# ✗ Too few: Underutilizes cluster (wasted resources)
df.repartition(10)   # 10 partitions / 40 cores = 30 cores idle!
                     # Only 10 tasks can run, 30 cores wasted

# ✗ Too many: Excessive overhead (task management, scheduling)
df.repartition(10000)  # 10000 partitions / 40 cores = 250 partitions per core!
                       # Too much overhead managing thousands of tiny tasks
```

**Rule of thumb:** Partition count = 2-4× number of available cores

**Why 2-4× multiplier?**
- Accounts for task processing time variation
- Allows better load balancing
- Keeps cores busy even when some tasks finish early
- Some partitions may be slightly larger/smaller

**Calculation:**
```python
# Get cluster size
num_executors = 10
cores_per_executor = 4
total_cores = num_executors * cores_per_executor  # 40

# Calculate optimal partitions
optimal_partitions = total_cores * 3  # 120 partitions
df.repartition(optimal_partitions)
```

---

### 4. Repartition Before Caching (If Needed)

```python
# ✓ Good: Repartition, then cache
optimized_df = df.repartition("date").cache()
optimized_df.count()  # Triggers caching

# Now all queries benefit from optimized partitioning

# ✗ Bad: Cache, then repartition
cached_df = df.cache()
cached_df.count()
repartitioned_df = cached_df.repartition("date")  # Lost cache!
```

---

### 5. Coalesce Before Writing

```python
# ✓ Good: Reduce output files
df.filter(...) \
  .coalesce(10) \  # 10 output files
  .write.parquet("output/")

# ✗ Bad: Too many small files
df.filter(...) \
  .write.parquet("output/")  # 200 output files!
```

**Benefit:** Fewer, larger files = better for storage systems

---

### 6. Test and Measure

```python
import time

# Test without repartitioning
start = time.time()
df.agg(...).show()
time_no_repart = time.time() - start

# Test with repartitioning
start = time.time()
df.repartition(100).agg(...).show()
time_with_repart = time.time() - start

print(f"Without: {time_no_repart:.2f}s")
print(f"With: {time_with_repart:.2f}s")

# Only keep repartition if faster!
```

---

### 7. Avoid Extreme coalesce() Reduction

```python
# ❌ Bad: Too extreme (risk of OOM and severe skew)
df.coalesce(1)  # From 1000 → 1 (1000:1 reduction - DANGEROUS!)
                # Single 100 GB partition on one executor

# ❌ Bad: Very drastic (high risk of skew)
df.coalesce(5)  # From 1000 → 5 (200:1 reduction - risky)

# ✓ Good: Moderate reduction (safer)
df.coalesce(100)  # From 1000 → 100 (10:1 reduction)

# ✓ Good: Gradual reduction if needed
df.coalesce(200).coalesce(50)  # Two-stage: 1000→200→50
```

**Safe reduction ratios:**
- **10:1 or less** → Generally safe
- **20:1** → Use with caution, monitor for skew
- **50:1 or more** → High risk, avoid unless necessary

**Exception:** coalesce(1) is acceptable for:
- Very small final results (< 1 GB)
- Writing single output file when data is already small

---

## Complete Examples

### Example 1: Optimizing Filtered DataFrame

**Scenario:** Large DataFrame, heavy filtering, multiple queries

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

spark = SparkSession.builder.appName("Repartition Example").getOrCreate()

# Large DataFrame: 1 billion rows, 200 partitions
sales_df = spark.read.parquet("sales_data.parquet")
print(f"Initial partitions: {sales_df.rdd.getNumPartitions()}")
# Output: Initial partitions: 200

# Heavy filtering (90% of data filtered out)
active_sales = sales_df.filter(col("status") == "ACTIVE")

# Still 200 partitions, but much less data
print(f"After filter partitions: {active_sales.rdd.getNumPartitions()}")
# Output: After filter partitions: 200 (same, but mostly empty)

# Coalesce to reduce partitions (no shuffle)
optimized_sales = active_sales.coalesce(20)
print(f"After coalesce: {optimized_sales.rdd.getNumPartitions()}")
# Output: After coalesce: 20

# Cache for reuse
optimized_sales.cache().count()

# Multiple queries now faster
result1 = optimized_sales.groupBy("region").count()
result2 = optimized_sales.groupBy("product").count()
result3 = optimized_sales.agg({"amount": "sum"})

# Benefits:
# - Fewer partitions (20 vs 200) = less overhead
# - Cached for fast reuse
# - No shuffle (used coalesce)
```

---

### Example 2: Repartitioning for Join

**Scenario:** Joining large DataFrames on skewed column

```python
# Large orders table
orders_df = spark.read.parquet("orders.parquet")
# 10 million rows, customer_id is skewed (80% from 10 customers)

# Large customers table
customers_df = spark.read.parquet("customers.parquet")

# Without repartitioning (skew causes problems)
start = time.time()
result = orders_df.join(customers_df, "customer_id")
result.count()
time_skewed = time.time() - start
print(f"Skewed join time: {time_skewed:.2f}s")
# Output: Skewed join time: 450.23s (slow due to skew!)

# With repartitioning (uniform distribution)
start = time.time()
repartitioned_orders = orders_df.repartition(100)  # Uniform partitions
result = repartitioned_orders.join(customers_df, "customer_id")
result.count()
time_uniform = time.time() - start
print(f"Uniform join time: {time_uniform:.2f}s")
# Output: Uniform join time: 180.45s (2.5× faster!)

print(f"Speedup: {time_skewed / time_uniform:.1f}×")
# Output: Speedup: 2.5×
```

---

### Example 3: Writing Optimized Output

**Scenario:** Writing filtered results with optimal file count

```python
# Read large dataset
df = spark.read.parquet("large_data.parquet")
print(f"Input partitions: {df.rdd.getNumPartitions()}")
# Output: Input partitions: 500

# Filter (reduces data by 95%)
filtered_df = df.filter(col("year") == 2024)

# Check filtered size
filtered_count = filtered_df.count()
print(f"Filtered rows: {filtered_count}")
# Output: Filtered rows: 50000 (small!)

# Without coalesce (500 tiny files!)
filtered_df.write.parquet("output/without_coalesce/")
# Result: 500 files, most are nearly empty

# With coalesce (10 reasonably-sized files)
filtered_df.coalesce(10).write.parquet("output/with_coalesce/")
# Result: 10 files, each ~5000 rows

# Benefits:
# - Fewer files (10 vs 500)
# - Better for storage systems
# - Faster to read later
# - No shuffle overhead (coalesce)
```

---

## Summary

### Key Concepts

1. **repartition():**
   - Causes shuffle/sort (expensive)
   - Can increase or decrease partitions
   - Creates uniform partitions
   - Use for rebalancing or partitioning by column

2. **repartitionByRange():**
   - Range-based partitioning
   - Uses sampling (may vary between runs)
   - Good for sorted data, range queries

3. **coalesce():**
   - No shuffle (cheap!)
   - Can only decrease partitions
   - May create skewed partitions
   - Use for reducing partition count

### Decision Matrix

| Goal | Method | Reason |
|------|--------|--------|
| Increase partitions | `repartition()` | Only option |
| Decrease partitions (uniform) | `repartition()` | Need balanced sizes |
| Decrease partitions (cheap) | `coalesce()` | Avoid shuffle |
| Partition by column | `repartition(col)` | Optimize queries |
| Range partitioning | `repartitionByRange()` | Sorted data |
| Before writing | `coalesce()` | Reduce file count |

### Best Practices Summary

✅ **DO:**
- Use coalesce() to reduce partitions (no shuffle)
- Repartition before caching if needed
- Match partition count to cluster size (2-4× cores)
- Test performance before/after
- Coalesce before writing output

❌ **DON'T:**
- Repartition unnecessarily (expensive shuffle)
- Use repartition() to reduce (use coalesce())
- Coalesce too drastically (risk of skew/OOM)
- Forget to measure the benefit

### Configuration Reference

```python
# Control shuffle partition count
spark.conf.set("spark.sql.shuffle.partitions", "200")  # Default

# Check current partition count
df.rdd.getNumPartitions()

# Repartition examples
df.repartition(100)                    # 100 uniform partitions
df.repartition("date")                 # Partition by column
df.repartition(50, "region", "date")   # 50 partitions by columns

# Coalesce example
df.coalesce(10)  # Reduce to 10 (no shuffle)
```

Master repartitioning and coalescing to optimize Spark job performance!