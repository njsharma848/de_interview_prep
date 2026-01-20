# Spark Executor Memory - Deep Dive

Understanding the internal structure and management of Spark executor memory.

---

## Table of Contents
- [Executor Memory Overview](#executor-memory-overview)
- [JVM Heap Memory Breakdown](#jvm-heap-memory-breakdown)
- [Spark Memory Pool Structure](#spark-memory-pool-structure)
- [Unified Memory Management](#unified-memory-management)
- [CPU Cores and Task Parallelism](#cpu-cores-and-task-parallelism)
- [Off-Heap Memory](#off-heap-memory)
- [PySpark Memory](#pyspark-memory)
- [Complete Memory Architecture](#complete-memory-architecture)
- [Configuration Guide](#configuration-guide)
- [Best Practices](#best-practices)

---

## Executor Memory Overview

### Starting Configuration

```bash
spark-submit \
  --executor-memory 8G \
  --executor-cores 4 \
  my_app.py
```

**What you get:**
- **JVM Heap:** 8 GB
- **CPU Cores:** 4 cores (4 parallel task slots)
- **Overhead:** 800 MB (default 10%)

For this deep dive, we'll focus on the **8 GB JVM heap memory** structure.

---

## JVM Heap Memory Breakdown

### Three Main Memory Pools

The 8 GB JVM heap is divided into **three parts**:

1. **Reserved Memory** (Fixed)
2. **Spark Memory Pool** (Configurable)
3. **User Memory** (Remaining)

### Calculation Example

**Given:**
```
spark.executor.memory = 8 GB (8000 MB)
spark.memory.fraction = 0.6 (default 60%)
```

**Step 1: Reserved Memory**
```
Reserved Memory = 300 MB (fixed by Spark)
Remaining = 8000 MB - 300 MB = 7700 MB
```

**Step 2: Spark Memory Pool**
```
Spark Memory Pool = Remaining × memory.fraction
Spark Memory Pool = 7700 MB × 0.6 = 4620 MB
```

**Step 3: User Memory**
```
User Memory = Remaining × (1 - memory.fraction)
User Memory = 7700 MB × 0.4 = 3080 MB
```

### Visual Representation

```
┌────────────────────────────────────────────────┐
│     JVM Heap Memory (8000 MB / 8 GB)           │
│                                                │
│  ┌──────────────────────────────────────────┐  │
│  │  Reserved Memory (300 MB)                │  │
│  │  - Spark internal objects                │  │
│  │  - Fixed allocation                      │  │
│  │  - Cannot be used by application         │  │
│  └──────────────────────────────────────────┘  │
│                                                │
│  ┌──────────────────────────────────────────┐  │
│  │  Spark Memory Pool (4620 MB = 60%)       │  │
│  │  spark.memory.fraction                   │  │
│  │                                          │  │
│  │  - DataFrame operations                  │  │
│  │  - Caching                               │  │
│  │  - Execution (joins, aggregations)       │  │
│  │  - Main working memory                   │  │
│  └──────────────────────────────────────────┘  │
│                                                │
│  ┌──────────────────────────────────────────┐  │
│  │  User Memory (3080 MB = 40%)             │  │
│  │  1 - spark.memory.fraction               │  │
│  │                                          │  │
│  │  - User-defined data structures          │  │
│  │  - HashMaps, custom objects              │  │
│  │  - User-defined functions (UDFs)         │  │
│  │  - RDD operations (if used directly)     │  │
│  │  - Spark metadata                        │  │
│  │  - Internal metadata storage             │  │
│  └──────────────────────────────────────────┘  │
└────────────────────────────────────────────────┘
```

### Memory Pool Purposes

| Pool | Size | Configuration | Purpose |
|------|------|---------------|---------|
| **Reserved** | 300 MB | Fixed | Spark engine internals |
| **Spark Memory** | 60% of (Total - 300MB) | `spark.memory.fraction` | DataFrame ops, caching |
| **User Memory** | 40% of (Total - 300MB) | Automatic (1 - fraction) | UDFs, custom structures, RDDs |

---

## Spark Memory Pool Structure

The **Spark Memory Pool (4620 MB)** is further divided into two sub-pools.

### Two Sub-Pools

1. **Storage Memory** - For caching DataFrames
2. **Execution Memory** - For DataFrame computations

### Default Split: 50-50

**Configuration:** `spark.memory.storageFraction = 0.5` (default)

```
Spark Memory Pool = 4620 MB

Storage Memory = 4620 MB × 0.5 = 2310 MB
Execution Memory = 4620 MB × 0.5 = 2310 MB
```

### Visual Breakdown

```
┌─────────────────────────────────────────────────┐
│   Spark Memory Pool (4620 MB)                   │
│                                                 │
│  ┌───────────────────────────────────────────┐  │
│  │  Storage Memory (2310 MB = 50%)           │  │
│  │  spark.memory.storageFraction             │  │
│  │                                           │  │
│  │  Purpose:                                 │  │
│  │  - Cached DataFrames (.cache())           │  │
│  │  - Persisted data (.persist())            │  │
│  │  - Broadcast variables                    │  │
│  │                                           │  │
│  │  Lifetime: Long-term                      │  │
│  │  - Stays until uncached                   │  │
│  │  - Stays until executor terminates        │  │
│  └───────────────────────────────────────────┘  │
│                                                 │
│  ┌───────────────────────────────────────────┐  │
│  │  Execution Memory (2310 MB = 50%)         │  │
│  │  1 - spark.memory.storageFraction         │  │
│  │                                           │  │
│  │  Purpose:                                 │  │
│  │  - Joins (buffering data)                 │  │
│  │  - Sorts (sorting operations)             │  │
│  │  - Aggregations (groupBy, agg)            │  │
│  │  - Shuffles (intermediate buffers)        │  │
│  │                                           │  │
│  │  Lifetime: Short-term                     │  │
│  │  - Used during task execution             │  │
│  │  - Freed when task completes              │  │
│  └───────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
```

### Storage vs Execution Memory

| Aspect | Storage Memory | Execution Memory |
|--------|----------------|------------------|
| **Purpose** | Cache DataFrames | Execute DataFrame operations |
| **Operations** | `.cache()`, `.persist()` | Joins, sorts, aggregations |
| **Lifetime** | Long-term (until uncached) | Short-term (during execution) |
| **Typical Use** | Reuse same data multiple times | One-time computation |
| **Example** | `df.cache()` | `df.join(df2).groupBy().agg()` |

---

## Unified Memory Management

### Before Spark 1.6: Static Memory Allocation

**Old Model (Deprecated):**
```
Each task gets: Execution Memory / Number of Cores
Example: 2310 MB / 4 cores = 577.5 MB per task
```

**Problems:**
- Fixed allocation regardless of active tasks
- Wasted memory if fewer tasks running
- Inflexible

### After Spark 1.6: Unified Memory Manager

**New Model (Current):**
- **Dynamic allocation** based on active tasks
- **Fair sharing** among concurrent tasks
- **Flexible boundary** between storage and execution

### How Unified Memory Works

#### Scenario 1: Two Active Tasks (4 slots available)

```
Available Execution Memory: 2310 MB
Active Tasks: 2

Each task can get up to: 2310 MB / 2 = 1155 MB
(More than static 577.5 MB per slot!)
```

#### Scenario 2: All Four Tasks Active

```
Available Execution Memory: 2310 MB
Active Tasks: 4

Each task gets approximately: 2310 MB / 4 = 577.5 MB
(Fair sharing among all tasks)
```

### Flexible Boundary Between Storage and Execution

**Key Concept:** The boundary between Storage and Execution memory is **flexible** and can shift based on demand.

#### Initial State

```
┌────────────────────────────────────┐
│  Spark Memory Pool (4620 MB)      │
├────────────────────┬───────────────┤
│  Storage (2310 MB) │ Execution     │
│                    │ (2310 MB)     │
│  [FREE]            │ [FREE]        │
└────────────────────┴───────────────┘
      ↑ Boundary (50-50, configurable)
```

#### Scenario A: Storage Borrows from Execution

**Situation:** Heavy caching, light execution

```
Step 1: Cache consumes all storage memory
┌────────────────────────────────────┐
│  Storage (2310 MB) │ Execution     │
│  [CACHED DATA]     │ [FREE]        │
└────────────────────┴───────────────┘

Step 2: Need more cache → borrow from execution
┌────────────────────────────────────┐
│  Storage ──→ borrowed →│ Execution │
│  [CACHED] [CACHED]     │ [FREE]    │
└────────────────────────┴───────────┘
        Storage expanded ──→
```

#### Scenario B: Execution Borrows from Storage

**Situation:** Large joins, no caching

```
Step 1: Storage is free, execution needs memory
┌────────────────────────────────────┐
│  Storage (2310 MB) │ Execution     │
│  [FREE]            │ [JOIN BUFFER] │
└────────────────────┴───────────────┘

Step 2: Join needs more → borrow from storage
┌────────────────────────────────────┐
│  Storage │←─ borrowed ← Execution  │
│  [FREE]  │ [JOIN] [JOIN]           │
└────────────────────────────────────┘
        ←── Execution expanded
```

### Memory Eviction Rules

#### Rule 1: Can Borrow Free Space

**Both sides can borrow from each other IF space is free**

```
✓ Storage can borrow from Execution (if execution is free)
✓ Execution can borrow from Storage (if storage is free)
```

#### Rule 2: Execution Can Evict Cached Data

**When execution needs memory and storage has borrowed:**

```
Before:
┌────────────────────────────────────┐
│  Storage ──→ borrowed →│ Execution │
│  [CACHED] [CACHED]     │ [NEED!]   │
└────────────────────────┴───────────┘

After: Memory Manager evicts cached data
┌────────────────────────────────────┐
│  Storage (freed) │ Execution       │
│  [CACHED]        │ [FREED][BUFFER] │
└────────────────────┴───────────────┘
                  ↓ Evicted to disk
```

**Memory manager evicts cached DataFrames to disk to make room**

#### Rule 3: Hard Boundary Cannot Be Evicted

**The configured boundary (storageFraction) is RIGID**

```
Configuration: spark.memory.storageFraction = 0.5

┌────────────────────────────────────┐
│  Storage       │ Execution         │
│  [CACHED]      │ [NEED MORE!]      │
└────────────────┴───────────────────┘
      ↑ HARD BOUNDARY (cannot evict)

If storage is full up to boundary:
- Execution CANNOT evict this cached data
- Execution must spill its own data to disk
- If cannot spill → OOM Exception!
```

### What Happens When Memory Is Exhausted

**Scenario:** Execution needs more memory but cannot get it

```
1. Try to borrow from storage
   ├─ If storage is free → ✓ Borrow
   └─ If storage is full → Continue to step 2

2. Try to evict cached data beyond hard boundary
   ├─ If cached data exists beyond boundary → ✓ Evict
   └─ If no data beyond boundary → Continue to step 3

3. Spill execution data to disk
   ├─ If data can be spilled → ✓ Spill (performance hit)
   └─ If data cannot be spilled → OOM Exception!
```

---

## CPU Cores and Task Parallelism

### Executor Cores Configuration

```bash
--executor-cores 4
```

**What this means:**
- **4 executor slots** (task threads)
- Can run **4 tasks in parallel**
- All slots are **threads in the SAME JVM**

### Important: Single JVM, Multiple Threads

```
┌─────────────────────────────────────┐
│     Single Executor JVM             │
│                                     │
│  Memory Pools:                      │
│  - Storage: 2310 MB                 │
│  - Execution: 2310 MB               │
│                                     │
│  Threads (Slots):                   │
│  ┌────┐ ┌────┐ ┌────┐ ┌────┐       │
│  │ T1 │ │ T2 │ │ T3 │ │ T4 │       │
│  └────┘ └────┘ └────┘ └────┘       │
│    ↓      ↓      ↓      ↓          │
│    Share same memory pools          │
└─────────────────────────────────────┘
```

**Key Points:**
- **NOT separate processes** - all threads in one JVM
- **Share the same memory pools**
- **Dynamic memory allocation** among active threads

### Memory Sharing Among Tasks

**Old Static Model (Before 1.6):**
```
4 cores → 2310 MB / 4 = 577.5 MB per task (fixed)
```

**New Unified Model (Current):**
```
2 active tasks → Each can get up to 1155 MB (dynamic)
4 active tasks → Each gets ~577.5 MB (fair share)
```

**Advantage:** Better memory utilization based on actual workload!

---

## Off-Heap Memory

### What is Off-Heap Memory?

Memory allocated **outside the JVM heap** to avoid garbage collection overhead.

### Configuration

```bash
--conf spark.memory.offHeap.enabled=true \
--conf spark.memory.offHeap.size=4G
```

### Three Memory Configurations

| Configuration | Description | Default |
|---------------|-------------|---------|
| `spark.memory.offHeap.enabled` | Enable off-heap memory | `false` |
| `spark.memory.offHeap.size` | Size of off-heap memory | Not set |
| Purpose | Extend executor and storage pools | N/A |

### Memory Architecture with Off-Heap

```
┌──────────────────────────────────────────────┐
│         Container Memory                     │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │  JVM Heap (8 GB)                       │  │
│  │                                        │  │
│  │  - Reserved: 300 MB                    │  │
│  │  - Spark Memory Pool: 4620 MB          │  │
│  │  - User Memory: 3080 MB                │  │
│  │                                        │  │
│  │  Subject to Garbage Collection         │  │
│  └────────────────────────────────────────┘  │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │  Off-Heap Memory (4 GB)                │  │
│  │  spark.memory.offHeap.size             │  │
│  │                                        │  │
│  │  Extends Spark Memory Pool:            │  │
│  │  - Extra execution memory              │  │
│  │  - Extra storage memory                │  │
│  │                                        │  │
│  │  NO Garbage Collection!                │  │
│  │  Manual memory management              │  │
│  └────────────────────────────────────────┘  │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │  Overhead (800 MB)                     │  │
│  └────────────────────────────────────────┘  │
└──────────────────────────────────────────────┘

Total Container: 8 GB + 4 GB + 800 MB = 12.8 GB
```

### Why Use Off-Heap Memory?

**Problem with Large JVM Heap:**
```
Large heap (e.g., 64 GB) → Long GC pauses → Performance degradation
```

**Solution:**
```
Mix of on-heap and off-heap:
- 32 GB on-heap (manageable GC)
- 32 GB off-heap (no GC)
= Better performance, less GC overhead
```

### When to Use Off-Heap

**Use off-heap when:**
- ✓ Need very large memory (> 32 GB per executor)
- ✓ Experiencing long GC pauses
- ✓ Running Spark 3.x+ (optimized for off-heap)
- ✓ Want to avoid JVM GC overhead

**Don't use off-heap when:**
- ✗ Small memory requirements (< 8 GB)
- ✗ Using older Spark versions (< 3.0)
- ✗ Application works well without it

### Off-Heap Usage

**Spark uses off-heap memory to extend:**
1. **Execution Memory** - Buffering for joins, sorts, aggregations
2. **Storage Memory** - Caching DataFrames

**Automatically managed** - No need to specify which operations use it

---

## PySpark Memory

### The PySpark Memory Challenge

**Key Fact:** PySpark workers are **NOT JVM processes**

```
Cannot use JVM heap memory (8 GB)
Must use overhead or dedicated PySpark memory
```

### Default PySpark Memory (Without Configuration)

```
spark.executor.pyspark.memory = Not set (0)
```

**Where does PySpark run?**
```
Overhead Memory: 800 MB
├─ Container processes: ~300-400 MB
└─ Available for PySpark: ~400 MB
```

**Problem:** Only ~400 MB for Python workers!

### Configuration

```bash
--conf spark.executor.pyspark.memory=2G
```

### Memory Architecture with PySpark

```
┌──────────────────────────────────────────────┐
│         Container Memory                     │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │  JVM Heap (8 GB)                       │  │
│  │  - For Spark JVM operations            │  │
│  │  - NOT accessible to PySpark workers   │  │
│  └────────────────────────────────────────┘  │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │  PySpark Memory (2 GB)                 │  │
│  │  spark.executor.pyspark.memory         │  │
│  │                                        │  │
│  │  Dedicated for:                        │  │
│  │  - Python worker processes             │  │
│  │  - Python UDFs                         │  │
│  │  - pandas operations                   │  │
│  │  - Custom Python libraries             │  │
│  └────────────────────────────────────────┘  │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │  Overhead (800 MB)                     │  │
│  │  - Container processes                 │  │
│  │  - Network buffers                     │  │
│  └────────────────────────────────────────┘  │
└──────────────────────────────────────────────┘

Total Container: 8 GB + 2 GB + 800 MB = 10.8 GB
```

### When Do You Need PySpark Memory?

**NOT needed when:**
```python
# Pure PySpark DataFrame API
df.select("column").filter(col("age") > 25).groupBy("name").count()
# ✓ Runs in JVM, no Python workers needed
```

**Needed when:**
```python
# 1. Python UDFs
@udf(returnType=StringType())
def my_python_function(text):
    return text.upper()  # Needs Python worker

# 2. pandas operations
@pandas_udf("long")
def pandas_function(series: pd.Series) -> pd.Series:
    return series * 2  # Needs Python worker

# 3. External Python libraries
from some_python_library import custom_function
df.rdd.map(lambda x: custom_function(x))  # Needs Python worker
```

### Default Value Explanation

**Why no default value?**

```
Most PySpark applications use only DataFrame API
→ No Python workers needed
→ No need for dedicated PySpark memory
→ No default value set
```

**When to set it:**
- Using Python UDFs
- Using pandas UDFs
- Using external Python libraries (numpy, pandas, etc.)
- Heavy Python-side processing

---

## Complete Memory Architecture

### Full Memory View

```
┌───────────────────────────────────────────────────────┐
│            Executor Container                         │
│                                                       │
│  ┌─────────────────────────────────────────────────┐  │
│  │  JVM Heap (8000 MB)                             │  │
│  │  spark.executor.memory                          │  │
│  │                                                 │  │
│  │  ┌───────────────────────────────────────────┐  │  │
│  │  │  Reserved Memory (300 MB)                 │  │  │
│  │  │  - Spark engine internal                  │  │  │
│  │  └───────────────────────────────────────────┘  │  │
│  │                                                 │  │
│  │  ┌───────────────────────────────────────────┐  │  │
│  │  │  Spark Memory Pool (4620 MB)              │  │  │
│  │  │  spark.memory.fraction = 0.6              │  │  │
│  │  │                                           │  │  │
│  │  │  ┌─────────────────────────────────────┐  │  │  │
│  │  │  │ Storage Memory (2310 MB)            │  │  │  │
│  │  │  │ spark.memory.storageFraction = 0.5  │  │  │  │
│  │  │  │ - Cached DataFrames                 │  │  │  │
│  │  │  │ - Broadcast variables               │  │  │  │
│  │  │  └─────────────────────────────────────┘  │  │  │
│  │  │                    ↕ Flexible boundary     │  │  │
│  │  │  ┌─────────────────────────────────────┐  │  │  │
│  │  │  │ Execution Memory (2310 MB)          │  │  │  │
│  │  │  │ - Joins, sorts, aggregations        │  │  │  │
│  │  │  │ - Shuffle buffers                   │  │  │  │
│  │  │  │ - Shared by all active tasks        │  │  │  │
│  │  │  └─────────────────────────────────────┘  │  │  │
│  │  └───────────────────────────────────────────┘  │  │
│  │                                                 │  │
│  │  ┌───────────────────────────────────────────┐  │  │
│  │  │  User Memory (3080 MB)                    │  │  │
│  │  │  - UDFs, custom data structures           │  │  │
│  │  │  - RDD operations                         │  │  │
│  │  │  - Metadata                               │  │  │
│  │  └───────────────────────────────────────────┘  │  │
│  └─────────────────────────────────────────────────┘  │
│                                                       │
│  ┌─────────────────────────────────────────────────┐  │
│  │  Off-Heap Memory (4000 MB)                      │  │
│  │  spark.memory.offHeap.size                      │  │
│  │  - Extends execution and storage                │  │
│  │  - No garbage collection                        │  │
│  └─────────────────────────────────────────────────┘  │
│                                                       │
│  ┌─────────────────────────────────────────────────┐  │
│  │  PySpark Memory (2000 MB)                       │  │
│  │  spark.executor.pyspark.memory                  │  │
│  │  - Python worker processes                      │  │
│  │  - Python UDFs, pandas operations               │  │
│  └─────────────────────────────────────────────────┘  │
│                                                       │
│  ┌─────────────────────────────────────────────────┐  │
│  │  Overhead Memory (800 MB)                       │  │
│  │  spark.executor.memoryOverhead                  │  │
│  │  - Container processes                          │  │
│  │  - Network buffers, shuffle exchange            │  │
│  └─────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────┘

Total Container Memory: 8 + 4 + 2 + 0.8 = 14.8 GB

Threads (Executor Cores = 4):
[Thread 1] [Thread 2] [Thread 3] [Thread 4]
    ↓          ↓          ↓          ↓
   Share all memory pools dynamically
```

---

## Configuration Guide

### All Memory Configuration Parameters

| Configuration | Default | Description | Recommendation |
|---------------|---------|-------------|----------------|
| `spark.executor.memory` | 1G | JVM heap size | 8-32 GB per executor |
| `spark.executor.memoryOverhead` | 10% | Non-JVM memory | 20-25% for production |
| `spark.memory.fraction` | 0.6 | Spark pool vs User memory | 0.6-0.75 (if no UDFs/RDDs) |
| `spark.memory.storageFraction` | 0.5 | Storage vs Execution split | 0.3-0.5 (lower if less caching) |
| `spark.executor.cores` | 1 | Threads per executor | 2-5 cores (recommended) |
| `spark.memory.offHeap.enabled` | false | Enable off-heap | true for large memory |
| `spark.memory.offHeap.size` | 0 | Off-heap size | 25-50% of heap for large apps |
| `spark.executor.pyspark.memory` | 0 | Python worker memory | 1-4 GB if using UDFs |

### Configuration Formulas

**Total Container Memory:**
```
Total = executor.memory 
        + max(executor.memory × overhead%, 384 MB)
        + offHeap.size
        + pyspark.memory
```

**Spark Memory Pool:**
```
Spark Pool = (executor.memory - 300 MB) × memory.fraction
```

**Storage Memory:**
```
Storage = Spark Pool × storageFraction
```

**Execution Memory:**
```
Execution = Spark Pool × (1 - storageFraction)
```

**User Memory:**
```
User = (executor.memory - 300 MB) × (1 - memory.fraction)
```

---

## Best Practices

### 1. Executor Cores Recommendation

**Spark Recommendation: 2-5 cores per executor**

```bash
# ✓ Good
--executor-cores 4

# ✓ Acceptable
--executor-cores 2
--executor-cores 5

# ❌ Avoid
--executor-cores 1   # Underutilizes parallelism
--executor-cores 8   # Excessive memory contention
--executor-cores 16  # Too much GC overhead
```

**Why not more than 5 cores?**
- Excessive memory management overhead
- Too many tasks competing for memory
- Increased GC pressure
- Diminishing returns

### 2. Increase memory.fraction for DataFrame-Heavy Apps

```bash
# If you're NOT using:
# - Custom data structures
# - Direct RDD operations
# - Complex UDFs

--conf spark.memory.fraction=0.75  # Instead of default 0.6
```

**Effect:** More memory for DataFrame operations, less for user memory

### 3. Reduce storageFraction if Not Caching Much

```bash
# If you rarely use .cache() or .persist()

--conf spark.memory.storageFraction=0.3  # Instead of default 0.5
```

**Effect:** More execution memory, less storage memory (hard boundary)

### 4. Use Off-Heap for Large Memory Requirements

```bash
# For executors with > 32 GB total memory

--executor-memory 32G \
--conf spark.memory.offHeap.enabled=true \
--conf spark.memory.offHeap.size=16G  # 50% of heap

# Total usable: 32 GB (on-heap) + 16 GB (off-heap) = 48 GB
```

**Benefit:** Reduces GC pressure on large heaps

### 5. Configure PySpark Memory Explicitly

```bash
# For PySpark apps with UDFs or pandas operations

--executor-memory 8G \
--conf spark.executor.memoryOverhead=0.20 \
--conf spark.executor.pyspark.memory=2G
```

**Prevents:** Python worker OOM errors

### 6. Balance Memory and Cores

```bash
# ❌ Bad: Too much memory, too few cores
--executor-memory 64G
--executor-cores 2
# Problem: Memory underutilized (only 2 tasks)

# ✓ Good: Balanced
--executor-memory 16G
--executor-cores 4
# Each task gets ~4 GB execution memory