# Spark AQE - Part 2: Dynamic Join Strategy Optimization

Understanding how AQE dynamically switches from Sort-Merge Join to Broadcast Hash Join at runtime.

---

## Table of Contents
- [AQE Three Features Recap](#aqe-three-features-recap)
- [The Join Strategy Problem](#the-join-strategy-problem)
- [Why Static Planning Fails](#why-static-planning-fails)
- [How AQE Solves Join Optimization](#how-aqe-solves-join-optimization)
- [Local Shuffle Reader Optimization](#local-shuffle-reader-optimization)
- [Configuration](#configuration)
- [Complete Example](#complete-example)
- [Best Practices](#best-practices)

---

## AQE Three Features Recap

### Adaptive Query Execution Capabilities

1. ✅ **Dynamically coalescing shuffle partitions** (Covered in Part 1)
2. **Dynamically switching join strategies** ← This guide
3. **Dynamically optimizing skew joins** (Covered in Part 3)

---

## The Join Strategy Problem

### Example Scenario

You have two large tables and want to join them:

**Tables:**
- `large_tbl_1`: 100 GB
- `large_tbl_2`: 100 GB

**SQL Query:**
```sql
SELECT *
FROM large_tbl_1 t1
JOIN large_tbl_2 t2 ON t1.id = t2.id
WHERE t2.status = 'ACTIVE'
```

**DataFrame API:**
```python
result = large_tbl_1.join(
    large_tbl_2.filter(col("status") == "ACTIVE"),
    on="id",
    how="inner"
)
```

### Expected: Sort-Merge Join

Since both tables appear large (100 GB each), Spark plans a **Sort-Merge Join**.

---

## Initial Execution Plan (Without AQE)

### Three-Stage Plan

```
┌─────────────────────────────────────────┐
│  Stage 0: Scan large_tbl_1              │
│                                         │
│  FileScan (100 GB)                      │
│      ↓                                  │
│  Exchange (Shuffle by join key)         │
└─────────────────┬───────────────────────┘
                  │
                  ↓ Shuffle
┌─────────────────────────────────────────┐
│  Stage 1: Scan large_tbl_2              │
│                                         │
│  FileScan (100 GB)                      │
│      ↓                                  │
│  Filter (status = 'ACTIVE')             │
│      ↓                                  │
│  Exchange (Shuffle by join key)         │
└─────────────────┬───────────────────────┘
                  │
                  ↓ Shuffle
┌─────────────────────────────────────────┐
│  Stage 2: Join                          │
│                                         │
│  Read from both exchanges               │
│      ↓                                  │
│  Sort (both sides)                      │
│      ↓                                  │
│  Merge Join                             │
└─────────────────────────────────────────┘
```

### Sort-Merge Join Characteristics

**Operations:**
1. **Exchange (Shuffle)** - Shuffle both tables by join key
2. **Sort** - Sort both sides of the join
3. **Merge** - Merge sorted data

**Costs:**
- Network I/O for shuffling both tables
- Disk I/O for sorting large datasets
- CPU for sort operations
- Time: **Expensive!**

---

## The Hidden Opportunity

### Post-Filter Data Size

After investigation, you discover:

```
large_tbl_1: 100 GB (all rows selected)
large_tbl_2: 100 GB → Filter → Only 7 MB selected!
```

**Visualization:**

```
┌────────────────────────────────┐
│  large_tbl_2 (100 GB)          │
│                                │
│  ████████████████████████████  │ 100 GB
│  ████████████████████████████  │
│  ████████████████████████████  │
│                                │
│        ↓ WHERE status='ACTIVE' │
│                                │
│  █                             │ 7 MB only!
└────────────────────────────────┘
```

### The Realization

**Question:** If one side is only 7 MB, should we use Sort-Merge Join?

**Answer:** NO! Use **Broadcast Hash Join** instead!

**Why?**
- 7 MB easily fits in memory
- Can broadcast to all executors
- Avoid expensive shuffle and sort
- Much faster execution

---

## Why Static Planning Fails

### Spark's Initial Decision

**Broadcast Join Threshold:**
```python
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
# Default: 10 MB (10485760 bytes)
```

**Logic:**
```
If table_size < 10 MB:
    Use Broadcast Hash Join
Else:
    Use Sort-Merge Join
```

### The Problem

**Spark's perspective at planning time:**

```
Planning Phase (before execution):
├─ large_tbl_1: 100 GB (from metadata)
├─ large_tbl_2: 100 GB (from metadata)
└─ Decision: Both large → Sort-Merge Join ✓

Execution Phase (runtime discovery):
├─ large_tbl_1: 100 GB (actual)
├─ large_tbl_2: 7 MB after filter! (actual)
└─ Reality: Should use Broadcast Join!
    But plan already created... ✗
```

**Issue:** Execution plan created **BEFORE** knowing actual data sizes!

---

## Why Statistics Don't Always Help

### Traditional Solution: Compute Statistics

```sql
ANALYZE TABLE large_tbl_2 COMPUTE STATISTICS;
ANALYZE TABLE large_tbl_2 COMPUTE STATISTICS FOR COLUMNS status;
```

### Three Problems with Statistics

#### Problem 1: No Column Histogram

```
Table statistics available: ✓ (100 GB)
Column histogram for 'status': ✗

Without histogram:
└─ Spark doesn't know filter selectivity
└─ Assumes table is still 100 GB
└─ Plans Sort-Merge Join
```

#### Problem 2: Outdated Statistics

```
Statistics computed: January 1, 2024
Current date: March 15, 2024

Data may have changed!
├─ New records inserted
├─ Old records deleted
└─ Statistics no longer accurate
```

#### Problem 3: Runtime-Generated Tables

```sql
-- Complex transformations
WITH filtered_data AS (
  SELECT * FROM large_tbl_2
  WHERE status = 'ACTIVE'
),
aggregated AS (
  SELECT id, COUNT(*) as cnt
  FROM filtered_data
  GROUP BY id
  HAVING cnt > 5
)
SELECT * FROM large_tbl_1
JOIN aggregated ON large_tbl_1.id = aggregated.id
```

**Problem:** Intermediate table `aggregated` has no pre-computed statistics!

---

## How AQE Solves Join Optimization

### AQE Workflow

```
1. Execute Stage 0 & Stage 1
   ↓
2. Data written to shuffle exchanges
   ↓
3. AQE computes ACTUAL statistics
   ├─ large_tbl_1 exchange: 100 GB
   └─ large_tbl_2 exchange: 7 MB ← Discovery!
   ↓
4. AQE re-evaluates join strategy
   ├─ 7 MB < 10 MB threshold
   └─ Decision: Switch to Broadcast Join!
   ↓
5. Replan Stage 2 with Broadcast Join
   ↓
6. Execute optimized plan
```

### Execution Plan (With AQE)

```
┌─────────────────────────────────────────┐
│  Stage 0: Scan large_tbl_1              │
│                                         │
│  FileScan (100 GB)                      │
│      ↓                                  │
│  Exchange (Shuffle by join key)         │
└─────────────────┬───────────────────────┘
                  │
                  ↓ Shuffle (100 GB)
┌─────────────────────────────────────────┐
│  Stage 1: Scan large_tbl_2              │
│                                         │
│  FileScan (100 GB)                      │
│      ↓                                  │
│  Filter (status = 'ACTIVE')             │
│      ↓                                  │
│  Exchange (Shuffle by join key)         │
└─────────────────┬───────────────────────┘
                  │
                  ↓ Shuffle (7 MB) ← AQE detects size!
                  ↓
            [AQE ANALYSIS]
            - large_tbl_1: 100 GB
            - large_tbl_2: 7 MB
            - Decision: BROADCAST!
                  ↓
┌─────────────────────────────────────────┐
│  Stage 2: Join (REPLANNED)              │
│                                         │
│  Read large_tbl_1 from exchange         │
│      ↓                                  │
│  Broadcast large_tbl_2 (7 MB)           │ ← Broadcast!
│      ↓                                  │
│  Broadcast Hash Join                    │ ← No Sort!
│  (No sort needed!)                      │
└─────────────────────────────────────────┘
```

### Key Changes

**Before AQE:**
```
Stage 2:
├─ Read both exchanges
├─ Sort both sides
└─ Merge join
```

**After AQE:**
```
Stage 2:
├─ Read large_tbl_1 from exchange
├─ Broadcast small table (7 MB)
└─ Hash join (NO SORT!)
```

**Savings:**
- ❌ Shuffle of large_tbl_2 still happens (needed for statistics)
- ✅ **Sort operation eliminated!**
- ✅ Broadcast join instead of merge
- ✅ Faster execution

---

## Why Shuffle Still Happens

### The Shuffle Trade-off

**Question:** Why can't AQE avoid the shuffle entirely?

**Answer:** AQE needs shuffle data to compute statistics!

### The Process

```
Without Shuffle:
└─ No statistics available
└─ Cannot make informed decision
└─ Must use original plan

With Shuffle:
├─ Shuffle happens (cost paid)
├─ Statistics computed during shuffle ← Key!
├─ Accurate size known
└─ Can replan and optimize
```

### Cost-Benefit Analysis

**Costs:**
- ✗ Shuffle of both tables (network I/O)

**Benefits:**
- ✓ Eliminate expensive sort operation
- ✓ Use faster hash join instead of merge
- ✓ Overall performance gain

**Net Result:** Still faster despite shuffle!

### Visual Comparison

**Without AQE:**
```
Shuffle (100 GB) + Shuffle (100 GB) + Sort + Sort + Merge
═══════════════════════════════════════════════════════
Total Time: 300 seconds
```

**With AQE:**
```
Shuffle (100 GB) + Shuffle (7 MB) + Broadcast + Hash Join
═══════════════════════════════════════════════════════
Total Time: 180 seconds (40% faster!)
```

Even with shuffle, **AQE is faster** by eliminating sort!

---

## Local Shuffle Reader Optimization

### Additional AQE Feature

After switching to broadcast join, AQE has one more trick...

### The Problem

```
Shuffle already completed:
├─ large_tbl_1 data distributed across cluster
└─ large_tbl_2 data (7 MB) distributed across cluster

Now broadcast large_tbl_2:
└─ Do we broadcast it AGAIN from shuffle exchange?
    └─ More network traffic!
```

### The Solution: Local Shuffle Reader

**Configuration:**
```python
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
```

**Default:** `true` (enabled by default)

### How It Works

**Without Local Shuffle Reader:**

```
┌─────────────────────────────────────────┐
│  Executor 1                             │
│  - Has partition 1 of large_tbl_1       │
│  - Has partition 1 of large_tbl_2       │
│                                         │
│  Needs entire large_tbl_2 for join     │
│      ↓                                  │
│  Fetch all partitions over network:     │
│  - Get partition 2 from Executor 2      │
│  - Get partition 3 from Executor 3      │
│  - Get partition 4 from Executor 4      │
└─────────────────────────────────────────┘

Network Traffic: HIGH
```

**With Local Shuffle Reader:**

```
┌─────────────────────────────────────────┐
│  Executor 1                             │
│  - Has partition 1 of large_tbl_1       │
│                                         │
│  Local Shuffle Reader optimizes:        │
│  - Reads large_tbl_2 locally if cached  │
│  - Minimizes network fetches            │
│  - Uses shuffle data efficiently        │
└─────────────────────────────────────────┘

Network Traffic: REDUCED
```

### Execution Plans Comparison

**Without Local Shuffle Reader:**

```
== Physical Plan ==
AdaptiveSparkPlan
+- BroadcastHashJoin
   ├─ Exchange (large_tbl_1)
   └─ BroadcastExchange  ← Standard broadcast
      └─ Exchange (large_tbl_2)
```

**With Local Shuffle Reader:**

```
== Physical Plan ==
AdaptiveSparkPlan
+- BroadcastHashJoin
   ├─ CustomShuffleReader local  ← Optimized!
   │  └─ Exchange (large_tbl_1)
   └─ BroadcastExchange
      └─ CustomShuffleReader local  ← Optimized!
         └─ Exchange (large_tbl_2)
```

**Benefit:** Reduced network traffic for broadcast operation!

---

## Configuration

### Enable AQE Join Optimization

```python
# Master switch (enables all AQE features)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Local shuffle reader (enabled by default)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# Broadcast threshold (default 10 MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10 MB
```

### Configuration Summary

| Configuration | Default | Purpose |
|---------------|---------|---------|
| `spark.sql.adaptive.enabled` | false | Enable all AQE features |
| `spark.sql.adaptive.localShuffleReader.enabled` | true | Optimize broadcast with local reads |
| `spark.sql.autoBroadcastJoinThreshold` | 10 MB | Size threshold for broadcast join |

### Important Notes

**Local Shuffle Reader:**
- ✓ Enabled by default when AQE is enabled
- ✓ No downside to keeping it enabled
- ✓ Reduces network traffic
- ⚠️ Only disable for debugging

**Broadcast Threshold:**
```python
# Increase for more aggressive broadcasting
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "20971520")  # 20 MB

# Disable broadcasting completely
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Use with caution - may cause OOM if too large
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600")  # 100 MB
```

---

## Complete Example

### Scenario Setup

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create large tables
spark = SparkSession.builder \
    .appName("AQE Join Optimization") \
    .getOrCreate()

# Simulate large tables
# large_tbl_1: 100 GB (1 billion rows)
# large_tbl_2: 100 GB (1 billion rows, but only 70K active)

# Create sample data
large_tbl_1 = spark.range(0, 1000000000).toDF("id")
large_tbl_2 = spark.range(0, 1000000000) \
    .withColumn("status", 
        when(col("id") < 70000, lit("ACTIVE"))
        .otherwise(lit("INACTIVE"))) \
    .toDF("id", "status")
```

---

### Without AQE

```python
# Disable AQE
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10 MB

# Join query
result = large_tbl_1.join(
    large_tbl_2.filter(col("status") == "ACTIVE"),
    on="id"
)

# Trigger execution
result.count()

# Check plan
result.explain()
```

**Execution Plan:**

```
== Physical Plan ==
SortMergeJoin [id], [id]
├─ Sort [id ASC]
│  └─ Exchange hashpartitioning(id, 200)
│     └─ FileScan large_tbl_1 (100 GB)
└─ Sort [id ASC]
   └─ Exchange hashpartitioning(id, 200)
      └─ Filter (status = ACTIVE)
         └─ FileScan large_tbl_2 (100 GB → 7 MB after filter)

Stages: 3
Join Strategy: Sort-Merge Join
Operations: Shuffle + Sort + Merge
```

**Performance:**
```
Stage 0: Shuffle large_tbl_1 (100 GB)  → 120 seconds
Stage 1: Shuffle large_tbl_2 (7 MB)    → 5 seconds
Stage 2: Sort + Merge                  → 180 seconds
─────────────────────────────────────────────────────
Total: 305 seconds
```

---

### With AQE

```python
# Enable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10 MB

# Same join query
result = large_tbl_1.join(
    large_tbl_2.filter(col("status") == "ACTIVE"),
    on="id"
)

# Trigger execution
result.count()

# Check plan
result.explain()
```

**Execution Plan:**

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- BroadcastHashJoin [id], [id]
   ├─ CustomShuffleReader local
   │  └─ Exchange hashpartitioning(id, 200)
   │     └─ FileScan large_tbl_1 (100 GB)
   └─ BroadcastExchange
      └─ CustomShuffleReader local
         └─ Exchange hashpartitioning(id, 200)
            └─ Filter (status = ACTIVE)
               └─ FileScan large_tbl_2 (7 MB after filter)

Stages: 3 (but optimized!)
Join Strategy: Broadcast Hash Join (dynamically switched!)
Operations: Shuffle + Broadcast + Hash (NO SORT!)
```

**Performance:**
```
Stage 0: Shuffle large_tbl_1 (100 GB)  → 120 seconds
Stage 1: Shuffle large_tbl_2 (7 MB)    → 5 seconds
        [AQE detects 7 MB < 10 MB threshold]
        [Replans to Broadcast Hash Join]
Stage 2: Broadcast + Hash Join         → 60 seconds (no sort!)
─────────────────────────────────────────────────────
Total: 185 seconds (40% improvement!)
```

---

### Comparison Results

| Metric | Without AQE | With AQE | Improvement |
|--------|-------------|----------|-------------|
| **Join Strategy** | Sort-Merge | Broadcast Hash | Better |
| **Sort Operation** | Yes (both sides) | No | Eliminated! |
| **Stage 2 Time** | 180s | 60s | 67% faster |
| **Total Time** | 305s | 185s | 39% faster |
| **Network Traffic** | High | Reduced | Local shuffle reader |
| **Memory Usage** | Lower | Higher (broadcast) | Trade-off |

---

## When AQE Join Optimization Applies

### Two Main Scenarios

#### Scenario 1: Highly Selective Filters

```sql
SELECT *
FROM large_table_1
JOIN large_table_2 ON t1.id = t2.id
WHERE t2.date = '2024-01-01'  -- Selects 0.01% of data
```

**Problem:** Spark doesn't know filter selectivity
**Solution:** AQE computes actual size after filter

---

#### Scenario 2: Runtime-Generated Tables

```python
# Complex transformations
intermediate = df1 \
    .filter(col("amount") > 1000) \
    .groupBy("category") \
    .agg(sum("amount").alias("total"))

# Join with intermediate result
result = df2.join(intermediate, on="category")
```

**Problem:** No statistics for `intermediate` table
**Solution:** AQE computes size during execution

---

## Summary

### Key Concepts

1. **The Problem:**
   - Execution plan created before knowing actual data sizes
   - Filters reduce table size significantly
   - Statistics may be missing, outdated, or inapplicable
   - Wrong join strategy chosen (Sort-Merge instead of Broadcast)

2. **AQE Solution:**
   - Computes actual statistics during shuffle
   - Detects when post-filter data is small
   - Dynamically switches from Sort-Merge to Broadcast Hash Join
   - Eliminates expensive sort operations

3. **Trade-offs:**
   - Shuffle still happens (needed for statistics)
   - But saves sort operation (bigger cost)
   - Net performance gain

4. **Local Shuffle Reader:**
   - Additional optimization
   - Reduces network traffic for broadcast
   - Enabled by default
   - No downside to keeping it enabled

### Benefits Summary

**Without AQE:**
```
Static Plan → Wrong Join Strategy → Slow Execution
```

**With AQE:**
```
Dynamic Plan → Optimal Join Strategy → Fast Execution
```

**Performance Gains:**
- ✓ 30-60% faster for selective joins
- ✓ Eliminates unnecessary sort operations
- ✓ Better resource utilization
- ✓ Automatic optimization (no manual tuning)

### Configuration Checklist

```python
# Essential configuration
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Keep default enabled
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# Adjust based on memory
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10 MB
```

---

## Best Practices

### 1. Always Enable AQE for Join-Heavy Workloads

```python
# Production configuration
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

### 2. Tune Broadcast Threshold Based on Cluster Memory

```python
# Small cluster (limited memory)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10 MB

# Large cluster (ample memory)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "52428800")  # 50 MB
```

### 3. Keep Local Shuffle Reader Enabled

```python
# Default is true - don't disable unless debugging
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
```

### 4. Monitor Query Plans

```python
# Check if AQE is working
df.explain("extended")

# Look for:
# - "AdaptiveSparkPlan"
# - "BroadcastHashJoin" (switched from SortMergeJoin)
# - "CustomShuffleReader local"
```

### 5. Combine with Other AQE Features

```python
# Use all AQE features together
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
```

---

## Coming Next

**Part 3: AQE Skew Join Optimization**
- Detecting data skew at runtime
- Splitting skewed partitions
- Dynamically optimizing skewed joins
- Real-world skew scenarios

Master AQE join optimization to eliminate unnecessary sorts and achieve faster query execution!