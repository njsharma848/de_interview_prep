# Spark Adaptive Query Execution (AQE) - Part 1: Dynamic Partition Coalescing

Understanding how AQE dynamically optimizes shuffle partitions at runtime.

---

## Table of Contents
- [What is Adaptive Query Execution?](#what-is-adaptive-query-execution)
- [The Shuffle Partition Problem](#the-shuffle-partition-problem)
- [How AQE Solves the Problem](#how-aqe-solves-the-problem)
- [AQE Configuration](#aqe-configuration)
- [Complete Example](#complete-example)
- [Best Practices](#best-practices)

---

## What is Adaptive Query Execution?

### Overview

**Adaptive Query Execution (AQE)** is a feature introduced in Apache Spark 3.0 that optimizes query execution at runtime based on actual data statistics.

### Three Main Capabilities

1. **Dynamically coalescing shuffle partitions** ← Focus of this guide
2. **Dynamically switching join strategies** (covered in Part 2)
3. **Dynamically optimizing skew joins** (covered in Part 2)

### Key Concept

Instead of using static configurations, AQE **observes actual data during execution** and makes intelligent optimizations on-the-fly.

---

## The Shuffle Partition Problem

### Example Query

**SQL:**
```sql
SELECT tower_location, SUM(duration_minutes) AS total_duration
FROM call_records
GROUP BY tower_location
```

**DataFrame API:**
```python
df = spark.read.table("call_records")
result = df.groupBy("tower_location") \
           .agg(sum("duration_minutes").alias("total_duration"))
```

### Sample Data

**call_records table:**

| call_id | duration_minutes | tower_location |
|---------|------------------|----------------|
| 1 | 5 | Tower_A |
| 2 | 10 | Tower_B |
| 3 | 8 | Tower_A |
| 4 | 3 | Tower_C |

**Expected Result:**

| tower_location | total_duration |
|----------------|----------------|
| Tower_A | 13 |
| Tower_B | 10 |
| Tower_C | 3 |

---

### Execution Plan

The query triggers a **two-stage Spark job**:

```
┌─────────────────────────────────────────┐
│  Stage 0: Read and Shuffle              │
│                                         │
│  Read from call_records                 │
│      ↓                                  │
│  Write to Output Exchange               │
└─────────────────┬───────────────────────┘
                  │
                  │ Shuffle/Sort (Wide Dependency)
                  ↓
┌─────────────────────────────────────────┐
│  Stage 1: Aggregate                     │
│                                         │
│  Read from Input Exchange               │
│      ↓                                  │
│  GROUP BY tower_location                │
│      ↓                                  │
│  SUM(duration_minutes)                  │
└─────────────────────────────────────────┘
```

**Why two stages?**
- `groupBy()` is a **wide dependency transformation**
- Requires **shuffle/sort** operation
- Stage 1 cannot start until Stage 0 completes

---

### Problem 1: Too Many Partitions

#### Scenario

**Configuration:**
```python
spark.conf.set("spark.sql.shuffle.partitions", "10")
```

**Input Data:**
- Source has **2 partitions**
- Data contains **5 unique towers** (Tower_A through Tower_E)

#### Stage 0 Output Exchange

```
┌─────────────────────────────────────┐
│  Stage 0 Output Exchange            │
│  (2 partitions from source)         │
│                                     │
│  Partition 1:                       │
│  ├─ Tower_A data                    │
│  ├─ Tower_B data                    │
│  └─ Tower_C data                    │
│                                     │
│  Partition 2:                       │
│  ├─ Tower_D data                    │
│  └─ Tower_E data                    │
└─────────────────────────────────────┘
```

#### After Shuffle/Sort (Stage 1 Input Exchange)

**Problem:** Shuffle creates **10 partitions** even though we only have **5 unique keys**!

```
┌────────────────────────────────────────────────┐
│  Stage 1 Input Exchange                        │
│  (shuffle.partitions = 10)                     │
│                                                │
│  Partition 1: [Tower_A data] ████████████      │
│  Partition 2: [Tower_B data] ████              │
│  Partition 3: [Tower_C data] ██                │
│  Partition 4: [Tower_D data] ████████████████  │
│  Partition 5: [Tower_E data] ████              │
│  Partition 6: [EMPTY]                          │
│  Partition 7: [EMPTY]                          │
│  Partition 8: [EMPTY]                          │
│  Partition 9: [EMPTY]                          │
│  Partition 10: [EMPTY]                         │
└────────────────────────────────────────────────┘
```

**Results:**
- **5 partitions** with data
- **5 empty partitions**
- **10 tasks** scheduled (5 do actual work, 5 finish in milliseconds)

**Problems:**
- ❌ Wasted scheduler overhead for empty tasks
- ❌ Inefficient resource usage
- ❌ Unnecessary task management

---

### Problem 2: Disproportionate Partitions

Even the 5 partitions with data are **not evenly distributed**:

```
Partition 1: ████████████  (Large - Tower_A has many calls)
Partition 2: ████          (Small)
Partition 3: ██            (Very small)
Partition 4: ██████████████(Very large - Tower_D has many calls)
Partition 5: ████          (Small)
```

**Execution Timeline:**

```
Task 1 (P1): ████████████████████████ (24 seconds)
Task 2 (P2): ████████                 (8 seconds)
Task 3 (P3): ████                     (4 seconds)
Task 4 (P4): ████████████████████████████ (28 seconds) ← Bottleneck
Task 5 (P5): ████████                 (8 seconds)
```

**Problems:**
- ❌ Tasks 2, 3, 5 finish quickly but must wait for Task 4
- ❌ Stage not complete until all tasks finish
- ❌ CPU resources wasted (idle while waiting)
- ❌ Total stage time = longest task time (28 seconds)

---

### Problem 3: Data Skew (Extreme Case)

When one partition becomes **extremely large**:

```
Partition 1: ████████████████████████████████████████ (90% of data!)
Partition 2: ██                                       (2%)
Partition 3: ██                                       (2%)
Partition 4: ██                                       (3%)
Partition 5: ██                                       (3%)
```

**Example:** One tower (Tower_A) processes 90% of all calls

**Problems:**
- ❌ One task takes forever (data skew)
- ❌ Other tasks idle, waiting
- ❌ Poor cluster utilization
- ❌ Potential OOM on the overloaded task

---

### The Fundamental Problem

**How do you decide the right number of shuffle partitions?**

**Challenge:**
```python
# Too small (e.g., 5)
spark.conf.set("spark.sql.shuffle.partitions", "5")
# Problem: Large partitions → OOM, slow processing

# Too large (e.g., 200 - default)
spark.conf.set("spark.sql.shuffle.partitions", "200")
# Problem: Many small/empty partitions → scheduler overhead

# Just right (e.g., 10)?
spark.conf.set("spark.sql.shuffle.partitions", "10")
# Problem: How do you know this is right?
```

**Why It's Impossible to Know:**
1. Number of unique keys is **dynamic** (depends on data)
2. Data size **varies** by query and dataset
3. Data distribution is **unpredictable**
4. Cannot set different values for every query
5. Values change as data grows

**Traditional Approach:**
```python
# Developers make educated guesses
spark.conf.set("spark.sql.shuffle.partitions", "100")  # ¯\_(ツ)_/¯
```

---

## How AQE Solves the Problem

### The AQE Approach

**Key Insight:** Don't guess—**observe actual data and optimize at runtime!**

### AQE Workflow

```
1. Execute Stage 0
   ↓
2. Data written to Output Exchange
   ↓
3. AQE analyzes actual shuffle data
   ↓
4. Compute runtime statistics:
   - Number of unique keys
   - Data size per key
   - Data distribution
   ↓
5. Dynamically determine optimal partitions
   ↓
6. Execute Stage 1 with optimized partitions
```

### Before AQE

**Static Configuration (Manual):**

```python
spark.conf.set("spark.sql.shuffle.partitions", "10")
```

**Result:**
```
Stage 1 Input Exchange (10 partitions - fixed):

P1: ████████████  
P2: ████          
P3: ██            
P4: ██████████████
P5: ████          
P6: [EMPTY]
P7: [EMPTY]
P8: [EMPTY]
P9: [EMPTY]
P10: [EMPTY]

Tasks: 10 (5 useful, 5 wasted)
```

---

### After AQE

**Dynamic Optimization (Automatic):**

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

**AQE Analysis:**
```
Shuffle statistics computed:
- Unique keys found: 5
- Partition sizes:
  * Tower_A: 500 MB
  * Tower_B: 100 MB
  * Tower_C: 50 MB
  * Tower_D: 600 MB
  * Tower_E: 100 MB

AQE Decision:
- Combine Tower_B + Tower_C + Tower_E → New Partition 2 (250 MB)
- Keep Tower_A → Partition 1 (500 MB)
- Keep Tower_D → Partition 3 (600 MB)
- Eliminate empty partitions
- Final partitions: 4 (was 10)
```

**Optimized Result:**
```
Stage 1 Input Exchange (4 partitions - optimized):

P1: ████████████      (Tower_A: 500 MB)
P2: ██████            (Tower_B + Tower_C + Tower_E: 250 MB)
P3: ██████████████    (Tower_D: 600 MB)
P4: [REMOVED]
P5-P10: [REMOVED]

Tasks: 4 (all useful!)
```

**Improvements:**
- ✓ Eliminated 6 empty partitions
- ✓ Combined 3 small partitions into 1
- ✓ More balanced partition sizes
- ✓ Reduced from 10 tasks to 4 tasks
- ✓ Better CPU utilization

### Execution Comparison

**Without AQE:**
```
Task 1 (P1): ████████████████████████ (24s)
Task 2 (P2): ████████                 (8s) ← Finishes early, waits
Task 3 (P3): ████                     (4s) ← Finishes early, waits
Task 4 (P4): ████████████████████████████ (28s) ← Bottleneck
Task 5 (P5): ████████                 (8s) ← Finishes early, waits
Tasks 6-10:  (milliseconds)           ← Wasted scheduling

Total Stage Time: 28 seconds
```

**With AQE:**
```
Task 1 (P1): ████████████████████████ (24s)
Task 2 (P2): ████████████████         (16s) ← Better balanced
Task 3 (P3): ████████████████████████ (24s)
Task 4: [ELIMINATED]

Total Stage Time: 24 seconds (4s improvement!)
Resource Usage: 1 fewer executor slot needed
```

---

## AQE Configuration

### Enable AQE

```python
# Master switch - enables all AQE features
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

**Default:** `false` (must explicitly enable)

### Configuration Hierarchy

```
spark.sql.adaptive.enabled (Master Switch)
    │
    ├─ If false → All AQE features disabled
    │
    └─ If true → Enable following tuning options:
        │
        ├─ spark.sql.adaptive.shuffle.targetPostShuffleInputSize
        ├─ spark.sql.adaptive.advisoryPartitionSizeInBytes
        ├─ spark.sql.adaptive.coalescePartitions.enabled
        └─ spark.sql.adaptive.coalescePartitions.minPartitionNum
```

---

### Detailed Configuration Parameters

#### 1. Initial Number of Shuffle Partitions

**Configuration:**
```python
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

| Property | Value |
|----------|-------|
| **Default** | 200 |
| **Purpose** | Starting point for AQE optimization |
| **Recommendation** | Set relatively large (200-500) |

**How it works:**
- AQE **starts** with this value
- Dynamically **reduces** based on actual data
- Acts as **maximum** partition count
- Cannot increase beyond this

**Example:**
```python
spark.conf.set("spark.sql.shuffle.partitions", "500")
# AQE will start with 500 partitions
# Then coalesce down to optimal number (e.g., 50)
```

---

#### 2. Advisory Partition Size

**Configuration:**
```python
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
```

| Property | Value |
|----------|-------|
| **Default** | 64 MB |
| **Purpose** | Target size for each shuffle partition |
| **Recommendation** | 64-128 MB |

**How it works:**
- AQE uses this as **target size** when coalescing
- Combines small partitions to reach this size
- **Advisory only** - not strict limit

**Example:**
```python
# Target 128 MB partitions
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")

# If you have partitions of 30MB, 40MB, 50MB
# AQE might combine first two: 30+40 = 70MB
# And keep 50MB separate
# Result closer to 128MB target
```

---

#### 3. Enable Partition Coalescing

**Configuration:**
```python
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

| Property | Value |
|----------|-------|
| **Default** | true |
| **Purpose** | Allow AQE to combine partitions |

**How it works:**
- **true:** AQE can combine/coalesce small partitions
- **false:** AQE will NOT combine partitions (only other optimizations)

**When to disable:**
```python
# Disable if you want fixed partition count
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
# Useful for debugging or specific use cases
```

---

#### 4. Minimum Partition Number

**Configuration:**
```python
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
```

| Property | Value |
|----------|-------|
| **Default** | `spark.default.parallelism` (or 1) |
| **Purpose** | Minimum partitions after coalescing |
| **Recommendation** | Equal to number of cores or executor count |

**How it works:**
- AQE **cannot reduce below** this number
- Ensures minimum parallelism
- Prevents over-coalescing

**Example:**
```python
# Cluster has 20 executor cores
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "20")

# AQE will create at least 20 partitions
# Even if data could fit in fewer
# Ensures all cores utilized
```

---

### Configuration Summary Table

| Configuration | Default | Purpose | Typical Value |
|---------------|---------|---------|---------------|
| `spark.sql.adaptive.enabled` | false | Enable AQE | **true** |
| `spark.sql.shuffle.partitions` | 200 | Initial/max partitions | 200-500 |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 64MB | Target partition size | 64-128 MB |
| `spark.sql.adaptive.coalescePartitions.enabled` | true | Allow coalescing | true |
| `spark.sql.adaptive.coalescePartitions.minPartitionNum` | default parallelism | Min partitions | # of cores |

---

## Complete Example

### Scenario

**Data:**
- Call records table with 5 towers
- Source data: 2 partitions
- Uneven distribution (Tower_A and Tower_D have more calls)

### Without AQE

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Create Spark session WITHOUT AQE
spark = SparkSession.builder \
    .appName("Without AQE") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

# Read data
df = spark.read.table("call_records")

# Group by query
result = df.groupBy("tower_location") \
    .agg(sum("duration_minutes").alias("total_duration"))

# Trigger action
result.write.mode("overwrite").parquet("output/without_aqe")

# Check execution plan
result.explain()
```

**Execution:**
```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[tower_location], functions=[sum(duration_minutes)])
   +- Exchange hashpartitioning(tower_location, 10)  ← 10 partitions
      +- HashAggregate(keys=[tower_location], functions=[partial_sum])
         +- FileScan parquet

Shuffle Partitions: 10 (5 with data, 5 empty)
Tasks: 10
```

---

### With AQE

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Create Spark session WITH AQE
spark = SparkSession.builder \
    .appName("With AQE") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "2") \
    .getOrCreate()

# Read data
df = spark.read.table("call_records")

# Group by query
result = df.groupBy("tower_location") \
    .agg(sum("duration_minutes").alias("total_duration"))

# Trigger action
result.write.mode("overwrite").parquet("output/with_aqe")

# Check execution plan
result.explain()
```

**Execution:**
```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   HashAggregate(keys=[tower_location], functions=[sum(duration_minutes)])
   +- AQEShuffleRead coalesced  ← AQE optimization!
      +- ShuffleQueryStage
         +- Exchange hashpartitioning(tower_location, 10)

Initial Shuffle Partitions: 10
After AQE Coalescing: 4  ← Optimized!
Tasks: 4 (vs 10 without AQE)
```

---

### Comparison Results

| Metric | Without AQE | With AQE | Improvement |
|--------|-------------|----------|-------------|
| **Shuffle Partitions** | 10 (fixed) | 4 (dynamic) | 60% reduction |
| **Empty Partitions** | 5 | 0 | Eliminated |
| **Tasks Scheduled** | 10 | 4 | 60% fewer |
| **Partition Balance** | Poor | Better | More even |
| **Execution Time** | 28s | 24s | 14% faster |
| **Resource Efficiency** | Low | High | Better utilization |

---

## Best Practices

### 1. Always Enable AQE in Spark 3.0+

```python
# Add to spark-defaults.conf or SparkSession
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

**Why:**
- ✓ No downside in most cases
- ✓ Automatic optimization
- ✓ Better resource utilization
- ✓ Improved performance

---

### 2. Set High Initial Shuffle Partitions

```python
# Start high, let AQE optimize down
spark.conf.set("spark.sql.shuffle.partitions", "500")  # Instead of default 200
```

**Reasoning:**
- AQE can only **reduce**, not increase
- Better to start high and coalesce down
- Different queries need different partitions

---

### 3. Tune Advisory Partition Size

```python
# For large datasets
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")

# For small datasets  
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "32MB")
```

**Guidelines:**
- Small data (< 1 GB): 32-64 MB
- Medium data (1-100 GB): 64-128 MB
- Large data (> 100 GB): 128-256 MB

---

### 4. Set Minimum Partitions Based on Cores

```python
# Cluster has 50 cores
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "50")
```

**Formula:**
```
minPartitionNum = Total Executor Cores

Or at minimum:
minPartitionNum = Number of Executors
```

**Ensures:**
- All cores utilized
- Prevents over-coalescing
- Maintains parallelism

---

### 5. Monitor AQE Effects

```python
# Check if AQE is working
df.explain("extended")

# Look for:
# - "AdaptiveSparkPlan"
# - "AQEShuffleRead coalesced"
# - "isFinalPlan=true"
```

**Spark UI:**
- Check "SQL" tab
- Look at "Details" section
- See "Final Plan" vs "Initial Plan"

---

### 6. Production Configuration Template

```python
spark = SparkSession.builder \
    .appName("Production App") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "500") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "50") \
    .getOrCreate()
```

---

## Summary

### Key Takeaways

1. **The Problem:**
   - Impossible to know optimal shuffle partitions in advance
   - Too few → large partitions, OOM risk
   - Too many → empty partitions, scheduler overhead
   - Data distribution unpredictable

2. **AQE Solution:**
   - Observes actual data at runtime
   - Computes dynamic statistics during shuffle
   - Automatically coalesces partitions
   - Eliminates empty partitions
   - Creates more balanced distribution

3. **Benefits:**
   - ✓ Better resource utilization
   - ✓ Fewer wasted tasks
   - ✓ More even partition sizes
   - ✓ Improved performance
   - ✓ No manual tuning needed

4. **How to Use:**
   ```python
   spark.conf.set("spark.sql.adaptive.enabled", "true")
   ```
   That's it! AQE handles the rest automatically.

5. **Configuration Strategy:**
   - Enable AQE: `true`
   - Start high: shuffle.partitions = 500
   - Set target size: advisoryPartitionSize = 64-128 MB
   - Set minimum: minPartitionNum = # of cores

### The Magic of AQE

**Before:** Manual guesswork, static configuration, suboptimal performance

**After:** Dynamic optimization, automatic tuning, better performance

AQE transforms Spark from a static execution engine into an **intelligent, adaptive system** that optimizes itself based on your actual data!

---

## Coming Next

**Part 2: AQE Join Optimization**
- Dynamically switching join strategies
- Dynamically optimizing skewed joins
- Real-world examples and configurations

Master AQE to build self-optimizing Spark applications!