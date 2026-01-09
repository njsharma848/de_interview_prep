# Spark AQE - Part 3: Dynamic Skew Join Optimization

Understanding how AQE detects and optimizes data skew in joins at runtime.

---

## Table of Contents
- [AQE Three Features Complete](#aqe-three-features-complete)
- [The Data Skew Problem](#the-data-skew-problem)
- [Why Increasing Memory Doesn't Work](#why-increasing-memory-doesnt-work)
- [How AQE Solves Skew Joins](#how-aqe-solves-skew-joins)
- [Skew Detection Thresholds](#skew-detection-thresholds)
- [Configuration](#configuration)
- [Complete Example](#complete-example)
- [Best Practices](#best-practices)

---

## AQE Three Features Complete

### All Three AQE Capabilities

1. ✅ **Dynamically coalescing shuffle partitions** (Covered in Part 1)
2. ✅ **Dynamically switching join strategies** (Covered in Part 2)
3. **Dynamically optimizing skew joins** ← This guide

This completes the full AQE feature set!

---

## The Data Skew Problem

### Example Scenario

You have two large tables to join:

**SQL Query:**
```sql
SELECT *
FROM large_table_1 t1
JOIN large_table_2 t2 ON t1.id = t2.id
```

**DataFrame API:**
```python
result = large_table_1.join(large_table_2, on="id", how="inner")
```

### Expected: Sort-Merge Join

Both tables are large, so Spark plans a **Sort-Merge Join**.

---

## Execution Plan with Skew

### Initial Setup

**Stage 0: Read large_table_1**
- Initial partitions: 2
- Shuffle by join key

**Stage 1: Read large_table_2**
- Initial partitions: 2
- Shuffle by join key

**Stage 2: Join**
- Sort and merge

### Shuffle Partitioning Visualization

#### Table 1 Shuffle

**Before Shuffle (2 initial partitions):**

```
┌─────────────────────────────────┐
│  large_table_1                  │
│  (Initial: 2 partitions)        │
│                                 │
│  Partition 1:                   │
│  ├─ Green data  ████████████    │
│  ├─ Blue data   ██              │
│  └─ Orange data ██              │
│                                 │
│  Partition 2:                   │
│  ├─ Yellow data ██              │
│  ├─ Green data  ████████████    │
│  └─ Blue data   ██              │
└─────────────────────────────────┘
```

**After Shuffle (partitioned by key):**

```
┌─────────────────────────────────────────────┐
│  large_table_1 Exchange                     │
│  (Partitioned by join key - 4 colors)       │
│                                             │
│  Green:  ████████████████████████  ← SKEWED!│
│  Blue:   ████████                           │
│  Orange: ████████                           │
│  Yellow: ████████                           │
└─────────────────────────────────────────────┘
```

#### Table 2 Shuffle

**After Shuffle (partitioned by key):**

```
┌─────────────────────────────────────────────┐
│  large_table_2 Exchange                     │
│  (Partitioned by join key - 4 colors)       │
│                                             │
│  Green:  ██████                             │
│  Blue:   ██████                             │
│  Orange: ██████                             │
│  Yellow: ██████                             │
└─────────────────────────────────────────────┘
```

### Join Stage Setup

**Data to Join:**

```
Left Side (table_1)        Right Side (table_2)
┌──────────────────┐       ┌──────────────┐
│ Green ██████████ │  ←→   │ Green ██████ │
│ (Skewed!)        │       │              │
└──────────────────┘       └──────────────┘

┌──────────────────┐       ┌──────────────┐
│ Blue  ████       │  ←→   │ Blue  ██████ │
└──────────────────┘       └──────────────┘

┌──────────────────┐       ┌──────────────┐
│ Orange ████      │  ←→   │ Orange ██████│
└──────────────────┘       └──────────────┘

┌──────────────────┐       ┌──────────────┐
│ Yellow ████      │  ←→   │ Yellow ██████│
└──────────────────┘       └──────────────┘
```

**Tasks Required:** 4 tasks (one per partition pair)

---

## The Skew Problem in Detail

### Resource Allocation

**Planned:** 4 GB RAM per task

### Task Execution

```
Task 1 (Green):  ████████████████████████████ 280s ← BOTTLENECK!
                 Memory: 7 GB needed, only 4 GB available
                 Status: Struggling / May OOM

Task 2 (Blue):   ████████                     80s
                 Memory: 2 GB used
                 Status: ✓ Completed, waiting

Task 3 (Orange): ████████                     80s
                 Memory: 2 GB used
                 Status: ✓ Completed, waiting

Task 4 (Yellow): ████████                     80s
                 Memory: 2 GB used
                 Status: ✓ Completed, waiting
```

### The Problems

#### Problem 1: Memory Pressure

```
Green partition size: 12 GB
Task memory available: 4 GB
Result: Cannot fit in memory!
        ├─ Excessive spilling to disk
        ├─ Very slow execution
        └─ Potential OOM exception
```

#### Problem 2: Resource Imbalance

```
Tasks 2, 3, 4:
├─ Finish in 80 seconds
├─ Sit idle waiting for Task 1
└─ Wasted CPU resources

Task 1:
├─ Takes 280 seconds (3.5× longer)
├─ Bottleneck for entire stage
└─ Total stage time = 280 seconds
```

#### Problem 3: Poor Cluster Utilization

```
Time: 0-80s
├─ 4 tasks running (100% utilization)

Time: 80-280s
├─ Only 1 task running (25% utilization)
└─ 75% of cluster idle!
```

---

## Why Increasing Memory Doesn't Work

### Naive Solution: Increase Memory

```python
# Increase executor memory for entire application
--executor-memory 16G  # Instead of 8G
```

### Problem 1: Memory Wastage

**Scenario:**
- Application has 10 different join operations
- 9 joins work fine with 4 GB per task
- Only 1 join has skew requiring 7 GB

**Result of increasing memory:**

```
Memory allocation per task: 8 GB

Join 1-9 (normal):
├─ Uses: 2-4 GB
├─ Allocated: 8 GB
└─ Wasted: 4-6 GB per task × 9 joins = 36-54 GB wasted!

Join 10 (skewed):
├─ Uses: 7 GB
├─ Allocated: 8 GB
└─ Utilized: ✓ Properly used
```

**Issue:** Cannot allocate different memory per join/task!

---

### Problem 2: Skew Is Dynamic

**Today:**
```
Green partition: 12 GB
Memory allocated: 16 GB
Status: ✓ Works
```

**Next Week (new data ingested):**
```
Green partition: 20 GB
Memory allocated: 16 GB
Status: ✗ FAILS again!
```

**Next Month:**
```
Green partition: 28 GB
Memory allocated: 20 GB (increased again)
Status: ✗ FAILS again!
```

**The Cycle:**
```
1. Application fails due to skew
2. Investigate logs
3. Increase memory
4. Redeploy
5. Works for a while
6. Data grows, skew increases
7. Application fails again
8. Go to step 2
```

**This is NOT sustainable!**

---

## How AQE Solves Skew Joins

### AQE Workflow

```
1. Execute shuffle (Stage 0 & Stage 1)
   ↓
2. AQE analyzes partition sizes
   ├─ Green: 12 GB ← Detect skew!
   ├─ Blue: 4 GB
   ├─ Orange: 4 GB
   └─ Yellow: 4 GB
   ↓
3. Check skew thresholds
   ├─ Is Green > 5× median? Yes (12 GB vs 4 GB median)
   ├─ Is Green > 256 MB? Yes (12 GB > 256 MB)
   └─ Both thresholds broken → SPLIT!
   ↓
4. Split skewed partition
   ↓
5. Duplicate matching right-side partition
   ↓
6. Execute optimized join
```

### The AQE Solution

**Before AQE (4 partitions):**

```
Left Side (table_1)        Right Side (table_2)      Tasks
┌──────────────────┐       ┌──────────────┐          ┌─────┐
│ Green ██████████ │  ←→   │ Green ██████ │     ←    │ T1  │ 280s
│ (12 GB - SKEWED!)│       │ (2 GB)       │          └─────┘
└──────────────────┘       └──────────────┘

┌──────────────────┐       ┌──────────────┐          ┌─────┐
│ Blue  ████       │  ←→   │ Blue  ██████ │     ←    │ T2  │ 80s
│ (4 GB)           │       │ (2 GB)       │          └─────┘
└──────────────────┘       └──────────────┘

┌──────────────────┐       ┌──────────────┐          ┌─────┐
│ Orange ████      │  ←→   │ Orange ██████│     ←    │ T3  │ 80s
│ (4 GB)           │       │ (2 GB)       │          └─────┘
└──────────────────┘       └──────────────┘

┌──────────────────┐       ┌──────────────┐          ┌─────┐
│ Yellow ████      │  ←→   │ Yellow ██████│     ←    │ T4  │ 80s
│ (4 GB)           │       │ (2 GB)       │          └─────┘
└──────────────────┘       └──────────────┘

Total time: 280 seconds (limited by T1)
```

**After AQE (5 partitions):**

```
Left Side (table_1)        Right Side (table_2)      Tasks
┌──────────────────┐       ┌──────────────┐          ┌─────┐
│ Green-1 █████    │  ←→   │ Green ██████ │     ←    │ T1  │ 120s
│ (6 GB - split!)  │       │ (2 GB-copy1) │          └─────┘
└──────────────────┘       └──────────────┘
                                  ↑
┌──────────────────┐              │ Duplicated         ┌─────┐
│ Green-2 █████    │  ←→   ┌──────┴───────┐      ←    │ T2  │ 120s
│ (6 GB - split!)  │       │ Green ██████ │          └─────┘
└──────────────────┘       │ (2 GB-copy2) │
                           └──────────────┘

┌──────────────────┐       ┌──────────────┐          ┌─────┐
│ Blue  ████       │  ←→   │ Blue  ██████ │     ←    │ T3  │ 80s
│ (4 GB)           │       │ (2 GB)       │          └─────┘
└──────────────────┘       └──────────────┘

┌──────────────────┐       ┌──────────────┐          ┌─────┐
│ Orange ████      │  ←→   │ Orange ██████│     ←    │ T4  │ 80s
│ (4 GB)           │       │ (2 GB)       │          └─────┘
└──────────────────┘       └──────────────┘

┌──────────────────┐       ┌──────────────┐          ┌─────┐
│ Yellow ████      │  ←→   │ Yellow ██████│     ←    │ T5  │ 80s
│ (4 GB)           │       │ (2 GB)       │          └─────┘
└──────────────────┘       └──────────────┘

Total time: 120 seconds (57% improvement!)
```

### What AQE Does

**Step 1: Split Skewed Left Partition**
```
Green (12 GB) → Split into 2
├─ Green-1 (6 GB)
└─ Green-2 (6 GB)
```

**Step 2: Duplicate Right Partition**
```
Green (2 GB) → Duplicate
├─ Green-copy1 (2 GB) → Join with Green-1
└─ Green-copy2 (2 GB) → Join with Green-2
```

**Step 3: Execute in Parallel**
```
Previous: 1 task processing 12 GB (bottleneck)
Now: 2 tasks processing 6 GB each (parallel)
```

---

## Skew Detection Thresholds

### Two Threshold Configurations

AQE uses **two thresholds** to detect skewed partitions. **Both must be exceeded** for skew optimization to trigger.

#### Threshold 1: Skew Factor

**Configuration:**
```python
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
```

| Property | Default | Description |
|----------|---------|-------------|
| **Value** | 5 | Multiplier of median partition size |
| **Formula** | `partition_size > median_size × factor` |

**Example:**
```
Partition sizes: [4 GB, 4 GB, 4 GB, 12 GB]
Median size: 4 GB
Skew factor: 5

Green partition: 12 GB
Check: 12 GB > (4 GB × 5) ?
       12 GB > 20 GB ?
       NO → Not skewed by factor alone

But if Green was 24 GB:
Check: 24 GB > (4 GB × 5) ?
       24 GB > 20 GB ?
       YES → Exceeds factor threshold ✓
```

---

#### Threshold 2: Absolute Size

**Configuration:**
```python
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

| Property | Default | Description |
|----------|---------|-------------|
| **Value** | 256 MB | Absolute size threshold |
| **Formula** | `partition_size > threshold_bytes` |

**Example:**
```
Green partition: 12 GB = 12,288 MB
Threshold: 256 MB

Check: 12,288 MB > 256 MB ?
       YES → Exceeds size threshold ✓
```

---

### Combined Threshold Logic

**Both thresholds must be exceeded:**

```python
def is_skewed(partition_size, median_size, factor, threshold_bytes):
    exceeds_factor = partition_size > (median_size * factor)
    exceeds_size = partition_size > threshold_bytes
    
    return exceeds_factor AND exceeds_size
```

### Examples

#### Example 1: Both Thresholds Exceeded → SKEWED

```
Partition sizes: [4 GB, 4 GB, 4 GB, 24 GB]
Median: 4 GB
Factor: 5 (default)
Threshold: 256 MB (default)

Green partition: 24 GB

Check 1 (Factor): 24 GB > (4 GB × 5) ?
                  24 GB > 20 GB ?
                  YES ✓

Check 2 (Size):   24 GB > 256 MB ?
                  YES ✓

Result: SKEWED → Split partition
```

---

#### Example 2: Factor Exceeded, Size Not → NOT SKEWED

```
Partition sizes: [10 MB, 10 MB, 10 MB, 100 MB]
Median: 10 MB
Factor: 5 (default)
Threshold: 256 MB (default)

Green partition: 100 MB

Check 1 (Factor): 100 MB > (10 MB × 5) ?
                  100 MB > 50 MB ?
                  YES ✓

Check 2 (Size):   100 MB > 256 MB ?
                  NO ✗

Result: NOT SKEWED → No split
```

**Reason:** Partition is relatively larger but absolutely small (< 256 MB)

---

#### Example 3: Size Exceeded, Factor Not → NOT SKEWED

```
Partition sizes: [200 GB, 200 GB, 200 GB, 500 GB]
Median: 200 GB
Factor: 5 (default)
Threshold: 256 MB (default)

Green partition: 500 GB

Check 1 (Factor): 500 GB > (200 GB × 5) ?
                  500 GB > 1000 GB ?
                  NO ✗

Check 2 (Size):   500 GB > 256 MB ?
                  YES ✓

Result: NOT SKEWED → No split
```

**Reason:** Partition is absolutely large but not relatively skewed (< 5× median)

---

### Why Both Thresholds?

**Factor Threshold Alone:**
- Prevents splitting when all partitions are uniformly small
- Example: [10 MB, 10 MB, 10 MB, 100 MB] → Don't split (overhead > benefit)

**Size Threshold Alone:**
- Prevents splitting when large partitions are proportionate
- Example: [200 GB, 200 GB, 200 GB, 500 GB] → Don't split (not truly skewed)

**Both Together:**
- Ensures split only when beneficial
- Large enough to matter (size threshold)
- Skewed enough to cause issues (factor threshold)

---

## Configuration

### Enable AQE Skew Join Optimization

```python
# 1. Enable AQE (master switch)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# 2. Enable skew join optimization
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# 3. Configure skew detection thresholds (optional)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

### Configuration Summary

| Configuration | Default | Purpose |
|---------------|---------|---------|
| `spark.sql.adaptive.enabled` | false | Enable all AQE features |
| `spark.sql.adaptive.skewJoin.enabled` | true | Enable skew join optimization |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | 5 | Relative skew threshold (× median) |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | 256 MB | Absolute skew threshold |

---

### Tuning Guidelines

#### Aggressive Skew Detection (Lower Thresholds)

```python
# Detect and split smaller skews
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "128MB")
```

**When to use:**
- Cluster has ample memory
- Frequent small skews
- Want to maximize parallelism

---

#### Conservative Skew Detection (Higher Thresholds)

```python
# Only split very large skews
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "10")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "512MB")
```

**When to use:**
- Limited cluster resources
- Splitting overhead concerns
- Only severe skews cause issues

---

## Complete Example

### Scenario Setup

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Sample data with intentional skew
# Customer "C001" has 10 million orders (90% of data)
# Other customers have 1 million orders total (10% of data)

spark = SparkSession.builder \
    .appName("Skew Join Example") \
    .getOrCreate()

# Create skewed dataset
orders = spark.range(0, 11000000) \
    .withColumn("customer_id",
        when(col("id") < 10000000, lit("C001"))  # 90% skewed to C001
        .otherwise(concat(lit("C"), (col("id") % 1000).cast("string")))
    ) \
    .withColumn("amount", (rand() * 1000).cast("int"))

customers = spark.range(0, 1001) \
    .withColumn("customer_id", concat(lit("C"), col("id").cast("string"))) \
    .withColumn("customer_name", concat(lit("Customer_"), col("id")))
```

---

### Without AQE Skew Optimization

```python
# Disable AQE
spark.conf.set("spark.sql.adaptive.enabled", "false")

# Join query
result = orders.join(customers, on="customer_id")

# Execute
result.count()

# Check plan
result.explain()
```

**Execution:**

```
== Physical Plan ==
SortMergeJoin [customer_id], [customer_id]
├─ Sort [customer_id]
│  └─ Exchange hashpartitioning(customer_id, 200)
│     └─ Scan orders
└─ Sort [customer_id]
   └─ Exchange hashpartitioning(customer_id, 200)
      └─ Scan customers

Partitioning result:
├─ Partition for C001: 9 GB (skewed!)
├─ Other 199 partitions: ~50 MB each
```

**Execution Timeline:**

```
Tasks 1-199:  ████ (Complete in 30s, then wait)
Task 200:     ████████████████████████████ (300s - bottleneck!)

Total Time: 300 seconds
Cluster Utilization: Poor (199 cores idle for 270s)
```

---

### With AQE Skew Optimization

```python
# Enable AQE with skew optimization
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# Same join query
result = orders.join(customers, on="customer_id")

# Execute
result.count()

# Check plan
result.explain()
```

**Execution:**

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- SortMergeJoin [customer_id], [customer_id]
   ├─ Sort [customer_id]
   │  └─ AQEShuffleRead
   │     └─ ShuffleQueryStage
   │        └─ Exchange hashpartitioning(customer_id, 200)
   │           └─ Scan orders
   └─ Sort [customer_id]
      └─ AQEShuffleRead
         └─ ShuffleQueryStage
            └─ Exchange hashpartitioning(customer_id, 200)
               └─ Scan customers

AQE Skew Optimization Applied:
├─ Detected skew: C001 partition (9 GB)
├─ Split into: 10 sub-partitions (900 MB each)
├─ Duplicated right side: customers.C001 (10 copies)
├─ Total partitions: 200 → 209 partitions
```

**Execution Timeline:**

```
Before:
Task 1-199:  ████ (30s)
Task 200:    ████████████████████████████ (300s bottleneck)
Total: 300s

After:
Task 1-199:  ████ (30s)
Task 200-209: ████████ (90s - split evenly)
Total: 90s (70% improvement!)

Cluster Utilization: Excellent (all cores utilized)
```

---

### Comparison Results

| Metric | Without AQE | With AQE | Improvement |
|--------|-------------|----------|-------------|
| **Partitions** | 200 | 209 (split skewed) | Balanced |
| **Largest Partition** | 9 GB | 900 MB | 90% smaller |
| **Bottleneck Task** | 300s | 90s | 70% faster |
| **Total Time** | 300s | 90s | 70% faster |
| **Idle Resources** | 199 cores × 270s | Minimal | Better utilization |
| **Memory Pressure** | High (OOM risk) | Normal | Stable |

---

## Best Practices

### 1. Always Enable AQE Skew Optimization

```python
# Production configuration
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

**Why:**
- No downside
- Automatic detection and optimization
- Handles dynamic data changes
- Prevents OOM failures

---

### 2. Start with Default Thresholds

```python
# Use defaults initially
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

**Then tune based on:**
- Cluster size
- Data characteristics
- Performance requirements

---

### 3. Monitor Skew Detection in Spark UI

Check Spark UI → SQL tab → Query details:

**Look for:**
- "Skewed partitions detected"
- "Partition split" annotations
- Execution timeline (should be balanced)

---

### 4. Combine All AQE Features

```python
# Enable all AQE optimizations together
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
```

**Benefit:** Comprehensive automatic optimization

---

### 5. Tune for Specific Workloads

**For highly skewed data:**
```python
# More aggressive detection
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")
```

**For uniform data with occasional skew:**
```python
# Less aggressive (avoid unnecessary splits)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "10")
```

---

### 6. Don't Increase Memory for Skew

```python
# ❌ Bad: Increasing memory for skew
--executor-memory 32G  # Wasteful

# ✓ Good: Enable AQE skew optimization
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

---

## Summary

### Key Concepts

1. **The Problem:**
   - Data skew causes uneven partition sizes
   - One large partition becomes bottleneck
   - Other tasks finish early and wait
   - Risk of OOM on skewed partition
   - Poor cluster utilization

2. **Why Memory Increase Fails:**
   - Wastes memory on non-skewed joins
   - Cannot allocate per-task memory
   - Skew is dynamic and changes over time
   - Not a sustainable solution

3. **AQE Solution:**
   - Detects skew using two thresholds
   - Splits skewed left partition
   - Duplicates matching right partition
   - Creates more balanced tasks
   - Automatic and dynamic

4. **Detection Logic:**
   - Factor threshold: > median × 5 (default)
   - Size threshold: > 256 MB (default)
   - **Both must be exceeded** to trigger split

5. **Benefits:**
   - 50-70% faster execution for skewed joins
   - Better cluster utilization
   - Prevents OOM errors
   - Handles dynamic data changes
   - No manual intervention needed

### Configuration Checklist

```python
# Essential configuration
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Optional tuning (use defaults initially)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

### All Three AQE Features Together

```
1. Coalesce Partitions:
   └─ Combines small partitions
   └─ Eliminates empty partitions

2. Switch Join Strategies:
   └─ Broadcast instead of sort-merge
   └─ When post-filter data is small

3. Optimize Skew Joins:
   └─ Splits skewed partitions
   └─ Balances workload
```

**Together, these three features make Spark self-optimizing!**

---

## Final Thoughts

### The Power of AQE

**Traditional Spark (Static):**
```
Developer guesses configurations
    ↓
Hope data matches assumptions
    ↓
Tune when things break
    ↓
Repeat as data changes
```

**Spark with AQE (Adaptive):**
```
Enable AQE
    ↓
Spark observes actual data at runtime
    ↓
Automatically optimizes
    ↓
Handles data changes dynamically
```

### Production Template

```python
# Complete AQE configuration for production
spark = SparkSession.builder \
    .appName("Production App") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "500") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
    .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "50") \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5") \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB") \
    .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
    .getOrCreate()
```

---

## Real-World Skew Scenarios

### Scenario 1: E-commerce Orders

**Problem:**
```
Orders table: 1 billion rows
- Customer "Amazon": 500 million orders (50% of data)
- Other 10 million customers: 500 million orders (50% of data)

Join with customers table for analytics
```

**Without AQE:**
```
Partition for Amazon: 200 GB
Other partitions: ~400 MB each
Result: One task takes hours, others finish in minutes
```

**With AQE:**
```
Amazon partition split into 200 sub-partitions
Each sub-partition: ~1 GB
All tasks finish in similar time
Performance: 95% improvement
```

---

### Scenario 2: Social Media Data

**Problem:**
```
Posts table: 10 billion posts
- Celebrity accounts: 5 billion posts (50% of data)
- Regular users: 5 billion posts (50% of data)

Join with users table for engagement analysis
```

**Without AQE:**
```
Partitions for celebrities: 50+ GB each
Regular user partitions: 100-500 MB
Result: OOM errors, job failures
```

**With AQE:**
```
Celebrity partitions automatically split
Balanced workload across cluster
Stable execution, no OOM
Performance: 80% improvement
```

---

### Scenario 3: IoT Sensor Data

**Problem:**
```
Sensor readings: 100 billion records
- Factory A: 60 billion readings (60% - very active)
- Factories B-Z: 40 billion readings (40% combined)

Join with factory metadata
```

**Without AQE:**
```
Factory A partition: 300 GB
Memory required: > executor memory
Result: Constant spilling, very slow
```

**With AQE:**
```
Factory A split into 100+ partitions
Each partition manageable size
No spilling, fast execution
Performance: 90% improvement
```

---

## Advanced: Manual Skew Handling vs AQE

### Manual Skew Handling (Old Method)

**Technique: Salting**

```python
# Add random salt to distribute skewed keys
from pyspark.sql.functions import rand, concat, lit

# Salt the skewed table
orders_salted = orders \
    .withColumn("salt", (rand() * 10).cast("int")) \
    .withColumn("salted_customer_id", 
                concat(col("customer_id"), lit("_"), col("salt")))

# Replicate the small table
customers_replicated = customers \
    .crossJoin(spark.range(0, 10).toDF("salt")) \
    .withColumn("salted_customer_id",
                concat(col("customer_id"), lit("_"), col("salt")))

# Join on salted key
result = orders_salted.join(
    customers_replicated,
    on="salted_customer_id"
)
```

**Problems:**
- ❌ Manual coding required
- ❌ Hard to maintain
- ❌ Need to know which keys are skewed
- ❌ Data duplication
- ❌ More shuffle data

---

### AQE Automatic Handling (New Method)

```python
# Simply enable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Write normal join code
result = orders.join(customers, on="customer_id")
```

**Benefits:**
- ✓ No manual coding
- ✓ Automatic detection
- ✓ Handles any skewed keys
- ✓ Efficient splitting
- ✓ Less shuffle data

**Winner: AQE by far!**

---

## Monitoring and Debugging

### Check if AQE Skew Optimization Is Working

#### Method 1: Spark UI

**Navigate to:** Spark UI → SQL tab → Query

**Look for:**
```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- SortMergeJoin
   +- AQEShuffleRead
      +- ShuffleQueryStage (skewed, split into N partitions)
```

**Indicators:**
- "skewed" annotation
- "split into N partitions"
- Multiple tasks for same key

---

#### Method 2: Query Execution Metrics

```python
# Execute query
result = orders.join(customers, on="customer_id")
result.count()

# Check metrics in Spark UI
# Look at "Duration" column for tasks
# Should be relatively uniform if skew handled
```

**Without AQE:**
```
Task Durations:
├─ Tasks 1-199: 30s, 32s, 28s, ... (fast)
└─ Task 200: 300s (bottleneck)
```

**With AQE:**
```
Task Durations:
├─ Tasks 1-209: 85s, 90s, 88s, 92s, ... (uniform)
```

---

#### Method 3: Programmatic Check

```python
# Check query plan programmatically
plan = result.explain("formatted")

# Or get string representation
plan_str = result._jdf.queryExecution().toString()

# Check for AQE indicators
if "AdaptiveSparkPlan" in plan_str:
    print("✓ AQE is active")
if "skewed" in plan_str.lower():
    print("✓ Skew detected and handled")
```

---

### Common Issues and Solutions

#### Issue 1: AQE Not Triggering Skew Optimization

**Symptoms:**
```
AQE enabled but skew still causing problems
Task durations still imbalanced
```

**Possible Causes:**
```python
# 1. Skew optimization disabled
spark.conf.get("spark.sql.adaptive.skewJoin.enabled")
# Should be: true

# 2. Thresholds too high
spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor")
# Try lowering: 3 instead of 5

spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes")
# Try lowering: 128MB instead of 256MB
```

**Solution:**
```python
# Lower thresholds
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "128MB")
```

---

#### Issue 2: Too Many Partitions After Split

**Symptoms:**
```
Excessive partition splitting
Too many small tasks
Scheduler overhead
```

**Possible Cause:**
```python
# Thresholds too aggressive
skewedPartitionFactor = 2  # Too low
```

**Solution:**
```python
# Increase thresholds
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "10")
```

---

#### Issue 3: Skew in Non-Join Operations

**Problem:**
```python
# Skew in groupBy (not join)
df.groupBy("customer_id").agg(sum("amount"))
```

**Limitation:**
AQE skew optimization **only works for joins**, not groupBy!

**Solution:**
```python
# Use manual salting for groupBy skew
df_salted = df.withColumn("salt", (rand() * 10).cast("int"))

# Aggregate in two stages
stage1 = df_salted.groupBy("customer_id", "salt") \
    .agg(sum("amount").alias("partial_sum"))

stage2 = stage1.groupBy("customer_id") \
    .agg(sum("partial_sum").alias("total_amount"))
```

---

## Performance Comparison: All AQE Features

### Test Setup

**Query:**
```sql
SELECT t1.*, t2.*, t3.*
FROM large_table_1 t1
JOIN large_table_2 t2 ON t1.id = t2.id
WHERE t2.status = 'ACTIVE'
GROUP BY t1.category
```

**Characteristics:**
- Large tables (100 GB each)
- Selective filter (reduces t2 to 5 MB)
- Data skew (category "Electronics" = 60% of data)
- Default shuffle partitions: 200

---

### Results

| Configuration | Time | Improvement | Features Applied |
|---------------|------|-------------|------------------|
| **No AQE** | 450s | Baseline | None |
| **AQE: Coalesce only** | 380s | 16% | Empty partitions removed |
| **AQE: Join switch only** | 320s | 29% | Broadcast instead of sort-merge |
| **AQE: Skew only** | 290s | 36% | Skewed partitions split |
| **AQE: All features** | 180s | 60% | All optimizations! |

**Key Insight:** All AQE features together provide **compounding benefits!**

---

## Summary Table: All Three AQE Features

| Feature | Problem Solved | How It Works | Benefit |
|---------|----------------|--------------|---------|
| **Coalesce Partitions** | Too many shuffle partitions | Combines small/empty partitions | Fewer tasks, less overhead |
| **Switch Join Strategy** | Wrong join algorithm | Broadcast instead of sort-merge | Eliminates expensive sorts |
| **Optimize Skew Joins** | Data skew in joins | Splits skewed partitions | Balanced workload, no OOM |

---

## Final Best Practices Summary

### Essential Configuration

```python
# Minimum production configuration
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

**This alone enables:**
- ✓ Partition coalescing
- ✓ Join strategy switching  
- ✓ Skew join optimization
- ✓ Local shuffle reader

---

### Recommended Production Configuration

```python
# Comprehensive AQE setup
spark = SparkSession.builder \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "500") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
    .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "50") \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5") \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB") \
    .config("spark.sql.autoBroadcastJoinThreshold", "10MB") \
    .getOrCreate()
```

---

### When to Tune

**Start with defaults, then adjust based on:**

**Highly skewed data:**
```python
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")
```

**Large cluster, ample memory:**
```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")
```

**Small cluster, limited memory:**
```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "5MB")
```

---

## Conclusion

### The AQE Revolution

**Before Spark 3.0 (Static Execution):**
- Manual tuning required
- Guesswork on configurations
- Performance depends on developer expertise
- Breaks when data changes

**After Spark 3.0 (Adaptive Execution):**
- Automatic optimization
- Runtime intelligence
- Self-tuning
- Adapts to data changes

### Three Simple Rules

1. **Always enable AQE** in Spark 3.0+
2. **Start with defaults** for all AQE settings
3. **Monitor and tune** only if needed

### Final Recommendation

```python
# Just enable this and let Spark optimize!
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

**AQE transforms Spark from a static execution engine into an intelligent, self-optimizing system that adapts to your data at runtime!**

---

## Complete AQE Series Summary

### Part 1: Dynamic Partition Coalescing
- Eliminates empty shuffle partitions
- Combines small partitions
- Reduces task overhead
- **Benefit:** 10-30% performance improvement

### Part 2: Dynamic Join Strategy Switching
- Detects post-filter table sizes
- Switches from Sort-Merge to Broadcast
- Eliminates expensive sorts
- **Benefit:** 30-50% performance improvement

### Part 3: Dynamic Skew Join Optimization
- Detects data skew at runtime
- Splits skewed partitions
- Balances workload
- **Benefit:** 50-90% performance improvement

### Combined Impact

**All three features together:**
- 60-200% overall performance improvement
- Eliminates OOM errors
- Better cluster utilization
- Self-optimizing behavior
- No manual tuning required

**Master AQE to build production-ready, self-optimizing Spark applications!**