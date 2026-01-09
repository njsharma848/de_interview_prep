# Spark DataFrame Caching - Complete Guide

Understanding how to cache DataFrames in Spark memory for optimal performance.

---

## Table of Contents
- [Memory Pool Review](#memory-pool-review)
- [cache() vs persist()](#cache-vs-persist)
- [How Caching Works](#how-caching-works)
- [Storage Levels](#storage-levels)
- [Cache Behavior Examples](#cache-behavior-examples)
- [Storage Level Configurations](#storage-level-configurations)
- [Uncaching Data](#uncaching-data)
- [When to Cache](#when-to-cache)
- [Best Practices](#best-practices)

---

## Memory Pool Review

### Spark Memory Pool Structure

From our earlier lesson on executor memory:

```
┌─────────────────────────────────────────────┐
│   Spark Memory Pool (60% of heap)          │
│                                             │
│  ┌───────────────────────────────────────┐  │
│  │  Storage Memory (50%)                 │  │
│  │  - Cached DataFrames (.cache())       │  │
│  │  - Persisted data (.persist())        │  │
│  │  - Broadcast variables                │  │
│  │                                       │  │
│  │  Long-term storage                    │  │
│  └───────────────────────────────────────┘  │
│                                             │
│  ┌───────────────────────────────────────┐  │
│  │  Execution Memory (50%)               │  │
│  │  - Joins, sorts, aggregations         │  │
│  │  - Shuffle operations                 │  │
│  │                                       │  │
│  │  Short-term, task execution           │  │
│  └───────────────────────────────────────┘  │
└─────────────────────────────────────────────┘
```

**Storage Memory:** Where cached DataFrames live!

---

## cache() vs persist()

### Two Methods for Caching

Spark provides two methods to cache DataFrames:

1. **cache()** - Simple, no arguments
2. **persist()** - Flexible, accepts storage level

### cache() Method

**Signature:**
```python
df.cache()
```

**Characteristics:**
- No arguments
- Uses default storage level: `MEMORY_AND_DISK`
- Deserialized format in memory
- No replication (1× only)

**Example:**
```python
cached_df = df.cache()
```

---

### persist() Method

**Signature:**
```python
df.persist()
df.persist(storageLevel)
df.persist(StorageLevel.MEMORY_ONLY)
```

**Characteristics:**
- Optional `storageLevel` argument
- Allows customization
- Same default as cache() if no argument

**Example:**
```python
from pyspark import StorageLevel

# Same as cache()
df.persist()

# Custom storage level
df.persist(StorageLevel.MEMORY_ONLY)
df.persist(StorageLevel.MEMORY_AND_DISK_SER)
```

---

### Quick Comparison

| Aspect | cache() | persist() |
|--------|---------|-----------|
| **Arguments** | None | Optional: storageLevel |
| **Default Level** | MEMORY_AND_DISK | MEMORY_AND_DISK |
| **Flexibility** | Fixed | Customizable |
| **Serialization** | Deserialized | Configurable |
| **Replication** | 1× | Configurable |
| **Use When** | Simple caching needs | Need control over storage |

**Bottom Line:** `cache()` is shorthand for `persist()` with default settings.

---

## How Caching Works

### Key Principles

#### 1. Caching is Lazy

```python
# This does NOT cache anything yet!
cached_df = df.cache()

# Caching happens when action is executed
cached_df.count()  # NOW it caches
```

**Caching only happens when an action triggers computation.**

---

#### 2. Smart Caching (Cache Only What's Accessed)

```python
df = spark.range(1000000).repartition(10)
cached_df = df.cache()

# Access only 10 records
cached_df.take(10)  # Loads and caches 1 partition only!

# Access all records
cached_df.count()   # Loads and caches all 10 partitions
```

**Spark caches only partitions that are actually loaded into memory.**

---

#### 3. Whole Partition Caching

```
Available memory: 7.5 partitions worth
Partitions to cache: 10

Result: Cache 7 complete partitions
        Do NOT cache 8th partition (doesn't fit)
        
Spark NEVER caches partial partitions!
```

**Rule:** Entire partition or nothing—never partial partitions.

---

### Example 1: take(10) Action

**Code:**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Caching Example").getOrCreate()

# Create DataFrame with 10 partitions
df = spark.range(1000000).repartition(10)

# Cache it (lazy - doesn't do anything yet)
cached_df = df.cache()

# Execute action - loads only 1 partition
result = cached_df.take(10)
```

**Spark UI Storage Tab:**
```
┌─────────────────────────────────────────────┐
│ RDD Name: DataFrame [id#0]                  │
│ Storage Level: Memory Deserialized 1x       │
│ Cached Partitions: 1 / 10                   │
│ Fraction Cached: 10%                        │
│ Size in Memory: 7.9 MB                      │
└─────────────────────────────────────────────┘
```

**Why only 1 partition?**
- `take(10)` loads only 1 partition to get 10 records
- Only loaded partitions are cached
- Other 9 partitions not accessed, not cached

---

### Example 2: count() Action

**Code:**
```python
df = spark.range(1000000).repartition(10)
cached_df = df.cache()

# Execute action - loads ALL partitions
count = cached_df.count()
```

**Spark UI Storage Tab:**
```
┌─────────────────────────────────────────────┐
│ RDD Name: DataFrame [id#0]                  │
│ Storage Level: Memory Deserialized 1x       │
│ Cached Partitions: 10 / 10                  │
│ Fraction Cached: 100%                       │
│ Size in Memory: 79.0 MB                     │
└─────────────────────────────────────────────┘
```

**Why all 10 partitions?**
- `count()` must load ALL partitions
- All loaded partitions are cached
- Now all 10 partitions cached in memory

---

## Storage Levels

### StorageLevel Parameters

```python
StorageLevel(
    useDisk: bool,          # Cache on disk?
    useMemory: bool,        # Cache in memory?
    useOffHeap: bool,       # Cache in off-heap?
    deserialized: bool,     # Store deserialized?
    replication: int        # Number of replicas
)
```

### Parameter Breakdown

#### 1. useDisk

**Purpose:** Enable disk-based caching

```python
# Memory only
StorageLevel(useDisk=False, useMemory=True, ...)

# Memory + Disk (spillover)
StorageLevel(useDisk=True, useMemory=True, ...)

# Disk only
StorageLevel(useDisk=True, useMemory=False, ...)
```

**When to use:**
- Large DataFrames that don't fit in memory
- Want to avoid recomputation but have limited memory

---

#### 2. useMemory

**Purpose:** Enable memory-based caching

```python
# Memory enabled (fastest)
StorageLevel(useMemory=True, ...)

# No memory (disk only)
StorageLevel(useMemory=False, useDisk=True, ...)
```

**Default:** Usually enabled (memory is fastest)

---

#### 3. useOffHeap

**Purpose:** Use off-heap memory for caching

```python
# Enable off-heap
StorageLevel(useOffHeap=True, ...)
```

**Requirements:**
- Off-heap memory must be configured
- See executor memory configuration

```python
# Configure off-heap first
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "4g")
```

**When to use:**
- Very large memory requirements
- Reduce GC pressure
- Have configured off-heap memory

---

#### 4. deserialized

**Purpose:** Store in deserialized (Java objects) vs serialized (bytes) format

**Deserialized (true):**
```
Storage Format: Java Objects
Memory Usage: Higher (more space)
CPU Usage: Lower (no deserialization needed)
Access Speed: Faster
```

**Serialized (false):**
```
Storage Format: Byte Arrays
Memory Usage: Lower (compact)
CPU Usage: Higher (must deserialize on access)
Access Speed: Slower
```

**Recommendation:** Keep deserialized (save CPU)

**Important:**
- Only applies to **memory** storage
- Disk and off-heap **always serialized**

---

#### 5. replication

**Purpose:** Number of cached replicas across executors

```python
# Single copy
StorageLevel(..., replication=1)

# Three copies on different executors
StorageLevel(..., replication=3)
```

**Benefits of replication:**
- Better data locality
- Fault tolerance
- Faster task scheduling

**Costs of replication:**
- More memory usage (3× memory for replication=3)
- Usually not worth it

**Recommendation:** Keep at 1 unless specific need

---

## Cache Behavior Examples

### Example 1: Default cache()

**Code:**
```python
df = spark.range(1000000).repartition(10)
cached_df = df.cache()
cached_df.count()
```

**Storage Level:**
```
Storage Level: Memory Deserialized 1x Replicated
Translation:
├─ useDisk: true
├─ useMemory: true
├─ useOffHeap: false
├─ deserialized: true
└─ replication: 1
```

**Spark UI:**
```
Storage Level: Memory Deserialized 1x Replicated
Size in Memory: 79.0 MB
Size on Disk: 0 B (fits in memory)
```

---

### Example 2: persist() with Same Level

**Code:**
```python
from pyspark import StorageLevel

df = spark.range(1000000).repartition(10)
cached_df = df.persist(StorageLevel.MEMORY_AND_DISK)
cached_df.count()
```

**Result:** Identical to cache()

```
Storage Level: Memory Deserialized 1x Replicated
Size in Memory: 79.0 MB
Size on Disk: 0 B
```

---

### Example 3: Serialized Storage

**Code:**
```python
df = spark.range(1000000).repartition(10)
cached_df = df.persist(StorageLevel.MEMORY_AND_DISK_SER)
cached_df.count()
```

**Spark UI:**
```
Storage Level: Memory Serialized 1x Replicated
Size in Memory: 45.0 MB (smaller than 79 MB deserialized!)
Size on Disk: 0 B
```

**Trade-off:**
- Saves memory (45 MB vs 79 MB = 43% reduction)
- Costs CPU (must deserialize on access)

---

### Example 4: Custom StorageLevel

**Code:**
```python
from pyspark import StorageLevel

df = spark.range(1000000).repartition(10)

# Custom storage level
custom_level = StorageLevel(
    useDisk=True,        # Allow disk spillover
    useMemory=True,      # Prefer memory
    useOffHeap=False,    # No off-heap (not configured)
    deserialized=False,  # Serialized (save memory)
    replication=1        # Single copy
)

cached_df = df.persist(custom_level)
cached_df.count()
```

**Spark UI:**
```
Storage Level: Memory Serialized 1x Replicated
Size in Memory: 45.0 MB
Size on Disk: 0 B
```

---

### Example 5: Insufficient Memory

**Scenario:** DataFrame too large for available memory

**Code:**
```python
# Huge DataFrame
df = spark.range(100000000).repartition(100)  # 100M rows, 100 partitions

# Cache with disk spillover
cached_df = df.persist(StorageLevel.MEMORY_AND_DISK)
cached_df.count()
```

**Spark UI:**
```
Storage Level: Memory Deserialized 1x Replicated
Cached Partitions: 75 / 100
Size in Memory: 7.5 GB (memory full)
Size on Disk: 2.5 GB (25 partitions spilled)
Total Size: 10.0 GB
```

**Behavior:**
- First 75 partitions fit in memory → cached in memory
- Remaining 25 partitions don't fit → spilled to disk
- Whole partitions only (never partial)

---

## Storage Level Configurations

### Using Predefined Constants

**Available Constants:**

```python
from pyspark import StorageLevel

# Memory only
StorageLevel.MEMORY_ONLY
StorageLevel.MEMORY_ONLY_2  # 2× replication

# Memory + Disk
StorageLevel.MEMORY_AND_DISK
StorageLevel.MEMORY_AND_DISK_2  # 2× replication

# Memory + Disk (Serialized)
StorageLevel.MEMORY_AND_DISK_SER
StorageLevel.MEMORY_AND_DISK_SER_2  # 2× replication

# Disk only
StorageLevel.DISK_ONLY
StorageLevel.DISK_ONLY_2  # 2× replication

# Off-heap
StorageLevel.OFF_HEAP
```

### Constants Table

| Constant | Memory | Disk | Off-Heap | Serialized | Replication |
|----------|--------|------|----------|------------|-------------|
| `MEMORY_ONLY` | ✓ | ✗ | ✗ | ✗ | 1 |
| `MEMORY_ONLY_2` | ✓ | ✗ | ✗ | ✗ | 2 |
| `MEMORY_AND_DISK` | ✓ | ✓ | ✗ | ✗ | 1 |
| `MEMORY_AND_DISK_2` | ✓ | ✓ | ✗ | ✗ | 2 |
| `MEMORY_AND_DISK_SER` | ✓ | ✓ | ✗ | ✓ | 1 |
| `MEMORY_AND_DISK_SER_2` | ✓ | ✓ | ✗ | ✓ | 2 |
| `DISK_ONLY` | ✗ | ✓ | ✗ | ✓ | 1 |
| `DISK_ONLY_2` | ✗ | ✓ | ✗ | ✓ | 2 |
| `OFF_HEAP` | ✗ | ✗ | ✓ | ✓ | 1 |

**Suffix Numbers (2, 3):** Replication factor

---

### Storage Preference Order

When multiple storage levels are enabled:

```
1st Preference: Memory (fastest)
    ↓
2nd Preference: Off-Heap (if configured)
    ↓
3rd Preference: Disk (slowest)
```

**Example:**
```python
# All three enabled
StorageLevel(useDisk=True, useMemory=True, useOffHeap=True, ...)

Execution:
├─ Fill memory first
├─ Overflow to off-heap if memory full
└─ Overflow to disk if off-heap full
```

---

## Uncaching Data

### unpersist() Method

**Remove cached data:**

```python
# Cache DataFrame
cached_df = df.cache()
cached_df.count()  # Triggers caching

# Later: Remove from cache
cached_df.unpersist()
```

**Note:** Method is `unpersist()`, not `uncache()`

### Parameters

```python
# Remove immediately (blocking)
df.unpersist(blocking=True)

# Remove asynchronously (non-blocking, default)
df.unpersist(blocking=False)
```

**Blocking:**
- `True`: Wait until unpersist completes
- `False`: Return immediately, unpersist in background

---

### Why Uncache?

**Free up memory for:**
- Other DataFrames
- Other applications
- Execution memory

**Example:**
```python
# Phase 1: Use cached_df1
cached_df1 = df1.cache()
result1 = cached_df1.agg(...)

# Phase 2: Done with df1, need df2
cached_df1.unpersist()  # Free memory

cached_df2 = df2.cache()
result2 = cached_df2.agg(...)
```

---

## When to Cache

### ✅ CACHE When:

#### 1. Reusing DataFrame Multiple Times

```python
# Good: DataFrame used 3 times
large_df = spark.read.parquet("large_data.parquet")
cached_df = large_df.cache()

result1 = cached_df.filter(col("age") > 25).count()
result2 = cached_df.filter(col("amount") > 1000).count()
result3 = cached_df.groupBy("category").count()

# Without cache: Read from disk 3 times
# With cache: Read from disk once, reuse from memory
```

---

#### 2. Iterative Algorithms

```python
# Machine learning iterations
current_df = initial_df.cache()

for i in range(10):
    # Reuse current_df in each iteration
    current_df = current_df.transform(...)
    
# Cache saves recomputation in each iteration
```

---

#### 3. Interactive Analysis

```python
# Jupyter notebook / pyspark shell
df = spark.read.parquet("data.parquet").cache()

# Try different queries interactively
df.filter(...).show()
df.groupBy(...).count()
df.select(...).describe()

# Cache avoids re-reading file for each query
```

---

### ❌ DON'T CACHE When:

#### 1. DataFrame Used Only Once

```python
# Bad: Cache not helpful
df = spark.read.parquet("data.parquet")
cached_df = df.cache()  # Waste!

result = cached_df.count()  # Used once, never again

# Just use without caching
result = df.count()
```

---

#### 2. Insufficient Memory

```python
# Bad: DataFrame doesn't fit in memory
huge_df = spark.read.parquet("100GB_data.parquet")
cached_df = huge_df.cache()  # Most will spill to disk anyway

# Result: Slower than not caching (disk I/O overhead)
```

---

#### 3. Very Small DataFrames

```python
# Bad: Overhead > benefit
tiny_df = spark.range(100)  # 100 rows
cached_df = tiny_df.cache()  # Unnecessary!

# Small enough to recompute instantly
# Caching overhead not worth it
```

---

#### 4. Simple Lineage (Easy to Recompute)

```python
# Bad: Quick to recompute
df = spark.read.parquet("data.parquet")  # Fast read
simple_df = df.select("id", "name")      # Trivial operation
cached_df = simple_df.cache()            # Not beneficial

# Recomputing is faster than cache overhead
```

---

### Decision Tree

```
Will DataFrame be reused multiple times?
├─ No → DON'T CACHE
└─ Yes
    │
    Does it fit in available memory?
    ├─ No → DON'T CACHE (or use MEMORY_AND_DISK carefully)
    └─ Yes
        │
        Is recomputation expensive?
        ├─ No → DON'T CACHE
        └─ Yes → ✅ CACHE
```

---

## Best Practices

### 1. Always Trigger an Action After Caching

```python
# ✓ Good: Trigger caching immediately
df.cache().count()  # Forces immediate caching

# ✗ Bad: Cache lazily evaluated later
df.cache()  # Doesn't actually cache until action
```

---

### 2. Use cache() for Simplicity

```python
# ✓ Good: Simple and clear
df.cache()

# ✗ Overly complex (unless needed)
df.persist(StorageLevel(True, True, False, True, 1))
```

Use `persist()` only when you need customization.

---

### 3. Monitor Memory Usage

```python
# Check Spark UI → Storage tab
# Look for:
# - Fraction Cached (should be 100% if enough memory)
# - Size in Memory vs Size on Disk
# - Number of cached partitions

# If seeing disk spillover frequently, need more memory
```

---

### 4. Unpersist When Done

```python
# ✓ Good: Free memory when done
cached_df = df.cache()
# ... use cached_df ...
cached_df.unpersist()  # Clean up

# ✗ Bad: Leave cached forever
cached_df = df.cache()
# ... use cached_df ...
# (memory never freed)
```

---

### 5. Prefer MEMORY_AND_DISK for Production

```python
# ✓ Good: Fault-tolerant
df.persist(StorageLevel.MEMORY_AND_DISK)

# ✗ Risky: MEMORY_ONLY may fail if memory full
df.persist(StorageLevel.MEMORY_ONLY)  # Can lose partitions!
```

`MEMORY_AND_DISK` provides safety net against memory pressure.

---

### 6. Avoid Excessive Replication

```python
# ✗ Bad: Wastes memory
df.persist(StorageLevel.MEMORY_ONLY_2)  # 2× memory usage

# ✓ Good: Single copy sufficient
df.persist(StorageLevel.MEMORY_ONLY)  # 1× memory usage
```

Replication rarely justified (Spark handles fault tolerance).

---

### 7. Consider Serialization for Large DataFrames

```python
# Large DataFrame, memory constrained
df.persist(StorageLevel.MEMORY_AND_DISK_SER)

# Saves ~40-50% memory
# Small CPU cost to deserialize
# Good trade-off for memory-constrained clusters
```

---

### 8. Verify Caching Effectiveness

```python
# Time without cache
start = time.time()
df.count()
time_uncached = time.time() - start

# Time with cache
cached_df = df.cache()
cached_df.count()  # First run: populates cache

start = time.time()
cached_df.count()  # Second run: uses cache
time_cached = time.time() - start

print(f"Speedup: {time_uncached / time_cached}×")

# Expect 10-100× speedup for large DataFrames
```

---

## Complete Example

### Scenario: Multiple Aggregations

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

spark = SparkSession.builder \
    .appName("Caching Example") \
    .getOrCreate()

# Read large dataset
sales_df = spark.read.parquet("sales_data.parquet")

# Without caching
start = time.time()
total_sales = sales_df.agg(sum("amount")).collect()
avg_sales = sales_df.agg(avg("amount")).collect()
max_sales = sales_df.agg(max("amount")).collect()
time_without_cache = time.time() - start

print(f"Time without cache: {time_without_cache:.2f}s")
# Output: Time without cache: 45.23s
# (Read from disk 3 times)

# With caching
cached_sales = sales_df.cache()
cached_sales.count()  # Trigger caching

start = time.time()
total_sales = cached_sales.agg(sum("amount")).collect()
avg_sales = cached_sales.agg(avg("amount")).collect()
max_sales = cached_sales.agg(max("amount")).collect()
time_with_cache = time.time() - start

print(f"Time with cache: {time_with_cache:.2f}s")
# Output: Time with cache: 2.15s
# (Read from disk once, then from memory)

print(f"Speedup: {time_without_cache / time_with_cache:.1f}×")
# Output: Speedup: 21.0×

# Clean up
cached_sales.unpersist()
```

---

## Summary

### Key Concepts

1. **Two Methods:**
   - `cache()`: Simple, default storage level
   - `persist()`: Flexible, custom storage level

2. **Caching is Lazy:**
   - Doesn't cache until action executed
   - Use `.cache().count()` to force immediate caching

3. **Smart Caching:**
   - Caches only accessed partitions
   - Always caches whole partitions (never partial)

4. **Storage Levels:**
   - Memory (fastest)
   - Off-Heap (if configured)
   - Disk (slowest)
   - Serialized vs Deserialized
   - Replication factor

5. **When to Cache:**
   - ✅ DataFrame reused multiple times
   - ✅ Fits in memory
   - ✅ Expensive to recompute
   - ❌ Used only once
   - ❌ Doesn't fit in memory
   - ❌ Small or cheap to recompute

### Configuration Checklist

```python
# Simple caching
df.cache()

# Custom caching
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK_SER)

# Always trigger
df.cache().count()

# Clean up
df.unpersist()
```

### Performance Impact

**Typical speedups with caching:**
- 10-100× for large DataFrames
- Higher speedup = more reuse + more expensive computation

**Memory efficiency:**
- Deserialized: Fast access, more memory
- Serialized: Slower access (CPU), less memory (~40-50% reduction)

Master DataFrame caching to dramatically improve iterative and interactive Spark workloads!