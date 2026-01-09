# Spark Memory Allocation - Driver and Executor

Understanding how memory is allocated and managed for Spark drivers and executors.

---

## Table of Contents
- [Overview](#overview)
- [Driver Memory Allocation](#driver-memory-allocation)
- [Executor Memory Allocation](#executor-memory-allocation)
- [Physical Memory Limits](#physical-memory-limits)
- [PySpark Memory Considerations](#pyspark-memory-considerations)
- [Memory Allocation from YARN Perspective](#memory-allocation-from-yarn-perspective)
- [Complete Examples](#complete-examples)
- [Common Issues and Solutions](#common-issues-and-solutions)

---

## Overview

When you submit a Spark application to a YARN cluster, memory is allocated in two main areas:
1. **Driver Container** (Application Master)
2. **Executor Containers**

Each has specific memory configurations and limits that must be understood to avoid Out of Memory (OOM) errors.

---

## Driver Memory Allocation

### Driver Memory Components

The driver container memory consists of **two parts**:

| Component | Configuration | Purpose |
|-----------|---------------|---------|
| **JVM Heap** | `spark.driver.memory` | Memory for driver JVM process |
| **Overhead Memory** | `spark.driver.memoryOverhead` | Memory for container and non-JVM processes |

### Configuration

```bash
spark-submit \
  --conf spark.driver.memory=1G \
  --conf spark.driver.memoryOverhead=0.10 \
  my_app.py
```

Or using command-line options:

```bash
spark-submit \
  --driver-memory 1G \
  my_app.py
```

### Calculation Example

**Configuration:**
```
spark.driver.memory = 1 GB
spark.driver.memoryOverhead = 0.10 (default: 10%)
```

**Calculation:**

1. **JVM Heap:** 1 GB (as requested)

2. **Overhead Memory:**
   - Formula: `max(driver.memory × 0.10, 384 MB)`
   - Calculation: `max(1 GB × 0.10, 384 MB) = max(100 MB, 384 MB)`
   - **Result: 384 MB** (minimum threshold)

3. **Total Container Memory:**
   ```
   Total = JVM Heap + Overhead
   Total = 1 GB + 384 MB = 1.384 GB
   ```

### Visual Representation

```
┌─────────────────────────────────────┐
│     Driver Container (1.384 GB)     │
│                                     │
│  ┌───────────────────────────────┐  │
│  │   JVM Heap (1 GB)             │  │
│  │   spark.driver.memory         │  │
│  │                               │  │
│  │   - Driver program            │  │
│  │   - SparkContext              │  │
│  │   - Task scheduling           │  │
│  └───────────────────────────────┘  │
│                                     │
│  ┌───────────────────────────────┐  │
│  │   Overhead (384 MB)           │  │
│  │   spark.driver.memoryOverhead │  │
│  │                               │  │
│  │   - Container processes       │  │
│  │   - Non-JVM operations        │  │
│  └───────────────────────────────┘  │
└─────────────────────────────────────┘
```

### Overhead Memory Minimum Threshold

The overhead memory follows this rule:

```
Overhead = max(driver.memory × memoryOverhead, 384 MB)
```

**Examples:**

| driver.memory | memoryOverhead | Calculation | Actual Overhead |
|---------------|----------------|-------------|-----------------|
| 1 GB | 0.10 (10%) | max(100 MB, 384 MB) | **384 MB** |
| 4 GB | 0.10 (10%) | max(400 MB, 384 MB) | **400 MB** |
| 8 GB | 0.10 (10%) | max(800 MB, 384 MB) | **800 MB** |
| 4 GB | 0.20 (20%) | max(800 MB, 384 MB) | **800 MB** |

---

## Executor Memory Allocation

### Executor Memory Components

The executor container memory consists of **four parts**:

| Component | Configuration | Default | Purpose |
|-----------|---------------|---------|---------|
| **JVM Heap** | `spark.executor.memory` | 1G | Memory for executor JVM |
| **Overhead Memory** | `spark.executor.memoryOverhead` | 10% | Container and non-JVM processes |
| **Off-Heap Memory** | `spark.memory.offHeap.size` | 0 | Direct memory allocation |
| **PySpark Memory** | `spark.executor.pyspark.memory` | 0 | Python worker processes |

### Configuration

```bash
spark-submit \
  --executor-memory 8G \
  --conf spark.executor.memoryOverhead=0.10 \
  --conf spark.memory.offHeap.size=2G \
  --conf spark.executor.pyspark.memory=1G \
  my_app.py
```

### Calculation Example

**Configuration:**
```
spark.executor.memory = 8 GB
spark.executor.memoryOverhead = 0.10 (10%)
spark.memory.offHeap.size = 0 (not set)
spark.executor.pyspark.memory = 0 (not set)
```

**Calculation:**

1. **JVM Heap:** 8 GB

2. **Overhead Memory:**
   - Formula: `max(executor.memory × 0.10, 384 MB)`
   - Calculation: `max(8 GB × 0.10, 384 MB) = max(800 MB, 384 MB)`
   - **Result: 800 MB**

3. **Off-Heap Memory:** 0 GB (not configured)

4. **PySpark Memory:** 0 GB (not configured)

5. **Total Container Memory:**
   ```
   Total = JVM Heap + Overhead + Off-Heap + PySpark
   Total = 8 GB + 800 MB + 0 + 0 = 8.8 GB
   ```

### Visual Representation

```
┌──────────────────────────────────────────────┐
│      Executor Container (8.8 GB)             │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │   JVM Heap (8 GB)                      │  │
│  │   spark.executor.memory                │  │
│  │                                        │  │
│  │   ┌──────────────────────────────┐     │  │
│  │   │ Execution Memory (60%)       │     │  │
│  │   │ - Task execution             │     │  │
│  │   │ - Shuffles, joins, sorts     │     │  │
│  │   └──────────────────────────────┘     │  │
│  │                                        │  │
│  │   ┌──────────────────────────────┐     │  │
│  │   │ Storage Memory (40%)         │     │  │
│  │   │ - Cached data                │     │  │
│  │   │ - Broadcast variables        │     │  │
│  │   └──────────────────────────────┘     │  │
│  │                                        │  │
│  │   ┌──────────────────────────────┐     │  │
│  │   │ User Memory (40%)            │     │  │
│  │   │ - User data structures       │     │  │
│  │   └──────────────────────────────┘     │  │
│  │                                        │  │
│  │   ┌──────────────────────────────┐     │  │
│  │   │ Reserved Memory (300 MB)     │     │  │
│  │   │ - Spark internal objects     │     │  │
│  │   └──────────────────────────────┘     │  │
│  └────────────────────────────────────────┘  │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │   Overhead Memory (800 MB)             │  │
│  │   spark.executor.memoryOverhead        │  │
│  │                                        │  │
│  │   - Container processes (~300-400 MB)  │  │
│  │   - Network buffers                    │  │
│  │   - Shuffle exchange                   │  │
│  │   - PySpark processes (~400 MB)        │  │
│  └────────────────────────────────────────┘  │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │   Off-Heap Memory (0 GB)               │  │
│  │   spark.memory.offHeap.size            │  │
│  │   (Not configured in this example)     │  │
│  └────────────────────────────────────────┘  │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │   PySpark Memory (0 GB)                │  │
│  │   spark.executor.pyspark.memory        │  │
│  │   (Not configured in this example)     │  │
│  └────────────────────────────────────────┘  │
└──────────────────────────────────────────────┘
```

### Memory Limits

When you allocate 8.8 GB to an executor container, you have **three critical limits**:

1. **JVM Heap Limit:** Executor JVM cannot use more than **8 GB**
2. **Non-JVM Limit:** Non-JVM processes cannot use more than **800 MB**
3. **Container Limit:** Total container cannot exceed **8.8 GB**

**Violating any limit → OOM Exception**

---

## Physical Memory Limits

### Worker Node Constraints

Before requesting memory, you must understand your cluster's physical limitations.

### YARN Configuration Parameters

| Configuration | Description | Example |
|---------------|-------------|---------|
| `yarn.scheduler.maximum-allocation-mb` | Maximum memory per container | 1792 MB |
| `yarn.nodemanager.resource.memory-mb` | Total memory available on node | 3584 MB |

### Real-World Example: AWS EMR with c4.large

**Instance Specifications:**
- Instance Type: c4.large
- Physical RAM: 3.75 GB

**EMR Default Configuration:**
```
yarn.scheduler.maximum-allocation-mb = 1792 MB
```

**What This Means:**
- **Maximum container size:** 1.792 GB
- Even if you request 8 GB, you won't get it
- YARN cannot allocate more than physical limits

### Checking Your Limits

```bash
# Check YARN configuration
yarn.scheduler.maximum-allocation-mb
yarn.nodemanager.resource.memory-mb

# Example values
yarn.scheduler.maximum-allocation-mb = 10240  # 10 GB max per container
yarn.nodemanager.resource.memory-mb = 61440   # 60 GB total per node
```

### Request vs Available

**Scenario 1: Request Exceeds Limit**
```bash
# Request
--executor-memory 8G
--conf spark.executor.memoryOverhead=0.10

# Total needed: 8.8 GB
# Maximum allowed: 1.792 GB
# Result: ❌ Request DENIED
```

**Scenario 2: Within Limits**
```bash
# Request
--executor-memory 1G
--conf spark.executor.memoryOverhead=0.10

# Total needed: 1.384 GB
# Maximum allowed: 1.792 GB
# Result: ✓ Request APPROVED
```

---

## PySpark Memory Considerations

### The PySpark Memory Problem

**Question:** When using PySpark with `spark.executor.memory = 8 GB`, how much memory does PySpark get?

**Answer:** PySpark gets **almost nothing** from the 8 GB!

### Why?

```
spark.executor.memory = 8 GB → Goes to JVM ONLY
PySpark is NOT a JVM process
```

### PySpark Memory Sources

PySpark processes run in the **overhead memory** portion:

```
Total Overhead: 800 MB
  ├─ Container processes: ~300-400 MB
  └─ Available for PySpark: ~400 MB
```

### Memory Breakdown for PySpark Application

**Configuration:**
```
spark.executor.memory = 8 GB
spark.executor.memoryOverhead = 0.10 (800 MB)
spark.memory.offHeap.size = not set (0)
spark.executor.pyspark.memory = not set (0)
```

**Memory Allocation:**

| Component | Memory | Available to PySpark? |
|-----------|--------|----------------------|
| JVM Heap | 8 GB | ❌ No (JVM only) |
| Overhead | 800 MB | ✓ Yes (~400 MB) |
| Off-Heap | 0 | ❌ Not configured |
| PySpark Memory | 0 | ❌ Not configured |

**Result:** PySpark gets approximately **400 MB** from overhead!

### The Fourth Limit

For PySpark applications, you have **one additional limit**:

```
4. PySpark Memory Limit: ~400 MB (from overhead)
```

If your PySpark processes need more than what's available in overhead → **OOM Exception**

### Solution: Configure PySpark Memory Explicitly

```bash
spark-submit \
  --executor-memory 8G \
  --conf spark.executor.memoryOverhead=0.20 \
  --conf spark.executor.pyspark.memory=2G \
  my_pyspark_app.py
```

**New Calculation:**
```
JVM Heap: 8 GB
Overhead: 1.6 GB (20% of 8 GB)
PySpark Memory: 2 GB
Total Container: 8 GB + 1.6 GB + 2 GB = 11.6 GB
```

Now PySpark has dedicated **2 GB** instead of relying on overhead scraps!

---

## Memory Allocation from YARN Perspective

### Container Memory Structure

From YARN's perspective, every container has two main memory portions:

```
┌─────────────────────────────────────────┐
│        YARN Container                   │
│                                         │
│  ┌───────────────────────────────────┐  │
│  │   Heap Memory                     │  │
│  │   (JVM Process)                   │  │
│  │                                   │  │
│  │   - Driver Memory (if driver)     │  │
│  │   - Executor Memory (if executor) │  │
│  │                                   │  │
│  │   Runs Spark JVM processes        │  │
│  └───────────────────────────────────┘  │
│                                         │
│  ┌───────────────────────────────────┐  │
│  │   Overhead Memory                 │  │
│  │   (OS/Non-JVM)                    │  │
│  │                                   │  │
│  │   - Container processes           │  │
│  │   - Network buffers               │  │
│  │   - Shuffle exchange              │  │
│  │   - Remote storage reads          │  │
│  │   - PySpark workers               │  │
│  │                                   │  │
│  │   Critical for I/O operations     │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
```

### Critical Insight: Overhead Memory Importance

**Overhead memory is NOT just "extra" memory!**

It's actively used for:
- **Network buffers:** Reading data from remote storage (S3, HDFS)
- **Shuffle exchange:** Writing/reading shuffle data between executors
- **Container processes:** Operating system overhead
- **PySpark workers:** Python processes (for PySpark apps)

### Why OOM Happens with Low Overhead

**Common Mistake:**
```bash
--executor-memory 16G
--conf spark.executor.memoryOverhead=0.10  # Only 1.6 GB overhead
```

**Problem:**
- Large shuffles write to exchange buffers (overhead memory)
- Reading partitions from S3 uses network buffers (overhead memory)
- PySpark workers compete for overhead memory
- **Result:** Overhead exhausted → OOM Exception

**Solution:**
```bash
--executor-memory 16G
--conf spark.executor.memoryOverhead=0.20  # 3.2 GB overhead
```

Increased overhead prevents OOM during shuffle-heavy operations!

---

## Complete Examples

### Example 1: Driver Memory Calculation

**spark-submit command:**
```bash
spark-submit \
  --driver-memory 4G \
  --conf spark.driver.memoryOverhead=0.10 \
  my_app.py
```

**Calculation:**

| Component | Calculation | Result |
|-----------|-------------|--------|
| Driver Memory | As requested | 4 GB |
| Overhead | max(4GB × 0.10, 384MB) | 400 MB |
| **Total Container** | 4GB + 400MB | **4.4 GB** |

**Memory Limits:**
- Driver JVM: ≤ 4 GB
- Non-JVM: ≤ 400 MB
- Total Container: ≤ 4.4 GB

---

### Example 2: Executor Memory Calculation (Scala/Java)

**spark-submit command:**
```bash
spark-submit \
  --executor-memory 8G \
  --conf spark.executor.memoryOverhead=0.10 \
  my_scala_app.jar
```

**Calculation:**

| Component | Configuration | Calculation | Result |
|-----------|---------------|-------------|--------|
| Executor Memory | spark.executor.memory | As requested | 8 GB |
| Overhead | spark.executor.memoryOverhead | max(8GB × 0.10, 384MB) | 800 MB |
| Off-Heap | spark.memory.offHeap.size | Not set | 0 |
| PySpark Memory | spark.executor.pyspark.memory | Not set | 0 |
| **Total Container** | Sum of all | 8GB + 800MB + 0 + 0 | **8.8 GB** |

**Memory Limits:**
- Executor JVM: ≤ 8 GB
- Non-JVM: ≤ 800 MB
- Total Container: ≤ 8.8 GB

---

### Example 3: Executor Memory with PySpark (Default Config)

**spark-submit command:**
```bash
spark-submit \
  --executor-memory 8G \
  my_pyspark_app.py
```

**Calculation:**

| Component | Memory | Notes |
|-----------|--------|-------|
| Executor Memory (JVM) | 8 GB | For Spark JVM only |
| Overhead | 800 MB | Shared with container processes |
| **Available for PySpark** | ~400 MB | From overhead only! |
| **Total Container** | 8.8 GB | |

**PySpark Gets:**
- Total Overhead: 800 MB
- Container processes: ~300-400 MB
- **Remaining for PySpark: ~400 MB**

**Risk:** ⚠️ PySpark may run out of memory!

---

### Example 4: Executor Memory with Explicit PySpark Config

**spark-submit command:**
```bash
spark-submit \
  --executor-memory 8G \
  --conf spark.executor.memoryOverhead=0.20 \
  --conf spark.executor.pyspark.memory=2G \
  my_pyspark_app.py
```

**Calculation:**

| Component | Configuration | Calculation | Result |
|-----------|---------------|-------------|--------|
| Executor Memory (JVM) | spark.executor.memory | As requested | 8 GB |
| Overhead | spark.executor.memoryOverhead | 8GB × 0.20 | 1.6 GB |
| PySpark Memory | spark.executor.pyspark.memory | As requested | 2 GB |
| Off-Heap | spark.memory.offHeap.size | Not set | 0 |
| **Total Container** | Sum of all | 8 + 1.6 + 2 + 0 | **11.6 GB** |

**Memory Allocation:**
- JVM Heap: 8 GB (for Spark operations)
- Overhead: 1.6 GB (for network buffers, shuffle)
- PySpark: 2 GB (dedicated for Python workers)
- **Total: 11.6 GB**

**Benefit:** ✓ PySpark has dedicated memory, reduced OOM risk!

---

### Example 5: Production Configuration with All Components

**spark-submit command:**
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 8G \
  --driver-cores 4 \
  --executor-memory 16G \
  --executor-cores 4 \
  --num-executors 10 \
  --conf spark.driver.memoryOverhead=0.15 \
  --conf spark.executor.memoryOverhead=0.20 \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=4G \
  --conf spark.executor.pyspark.memory=2G \
  production_app.py
```

**Driver Container:**
| Component | Calculation | Result |
|-----------|-------------|--------|
| Driver Memory | As requested | 8 GB |
| Overhead | max(8GB × 0.15, 384MB) | 1.2 GB |
| **Total** | 8GB + 1.2GB | **9.2 GB** |

**Each Executor Container:**
| Component | Calculation | Result |
|-----------|-------------|--------|
| Executor Memory | As requested | 16 GB |
| Overhead | 16GB × 0.20 | 3.2 GB |
| Off-Heap | As requested | 4 GB |
| PySpark Memory | As requested | 2 GB |
| **Total** | 16 + 3.2 + 4 + 2 | **25.2 GB** |

**Total Cluster Resources:**
- Driver: 9.2 GB
- Executors: 10 × 25.2 GB = 252 GB
- **Total: 261.2 GB**

---

## Common Issues and Solutions

### Issue 1: Container Killed - Memory Limit Exceeded

**Error Message:**
```
Container killed by YARN for exceeding memory limits.
8.5 GB of 8.8 GB physical memory used.
```

**Cause:** JVM + overhead exceeded container limit

**Solution 1: Increase Overhead**
```bash
# Before (OOM)
--executor-memory 8G
--conf spark.executor.memoryOverhead=0.10  # 800 MB

# After (Fixed)
--executor-memory 8G
--conf spark.executor.memoryOverhead=0.20  # 1.6 GB
```

**Solution 2: Reduce Executor Memory**
```bash
# Before (OOM)
--executor-memory 8G

# After (Fixed)
--executor-memory 6G  # More headroom in overhead
--conf spark.executor.memoryOverhead=0.20
```

---

### Issue 2: PySpark Worker Out of Memory

**Error Message:**
```
ERROR Executor: Exception in task 0.0
java.io.IOException: Cannot run program "python": error=12, Cannot allocate memory
```

**Cause:** PySpark workers competing for limited overhead memory

**Solution: Allocate Dedicated PySpark Memory**
```bash
# Before (OOM)
--executor-memory 8G

# After (Fixed)
--executor-memory 8G
--conf spark.executor.memoryOverhead=0.20
--conf spark.executor.pyspark.memory=2G  # Dedicated Python memory
```

---

### Issue 3: Request Exceeds Cluster Limits

**Error Message:**
```
Required executor memory (8192 MB) is above the max threshold (1792 MB)
```

**Cause:** Requested memory exceeds `yarn.scheduler.maximum-allocation-mb`

**Solution: Reduce Memory Request**
```bash
# Before (Rejected)
--executor-memory 8G  # Exceeds 1792 MB limit

# After (Accepted)
--executor-memory 1G
--conf spark.executor.memoryOverhead=0.20
```

Or **increase cluster limits** (if you have admin access):
```xml
<!-- yarn-site.xml -->
<property>
  <name>yarn.scheduler.maximum-allocation-mb</name>
  <value>10240</value> <!-- Increase to 10 GB -->
</property>
```

---

### Issue 4: Shuffle OOM Errors

**Error Message:**
```
java.io.IOException: No space left on device
Container killed for exceeding memory limits during shuffle
```

**Cause:** Shuffle exchange buffers exhausting overhead memory

**Solution: Increase Overhead for Shuffles**
```bash
# Before (OOM during shuffle)
--executor-memory 16G
--conf spark.executor.memoryOverhead=0.10  # 1.6 GB - too low!

# After (Fixed)
--executor-memory 16G
--conf spark.executor.memoryOverhead=0.25  # 4 GB - enough for shuffles
--conf spark.sql.shuffle.partitions=200
```

---

## Best Practices

### 1. Always Configure Overhead Explicitly

```bash
# ❌ Bad: Relying on default
--executor-memory 16G

# ✓ Good: Explicit overhead
--executor-memory 16G
--conf spark.executor.memoryOverhead=0.20
```

### 2. Increase Overhead for Shuffle-Heavy Jobs

```bash
# For jobs with lots of joins, groupBy, aggregations
--conf spark.executor.memoryOverhead=0.25  # 25% instead of 10%
```

### 3. Configure PySpark Memory for Large Python Operations

```bash
# ❌ Bad: PySpark competing for overhead
--executor-memory 8G

# ✓ Good: Dedicated PySpark memory
--executor-memory 8G
--conf spark.executor.pyspark.memory=2G
```

### 4. Check Cluster Limits Before Requesting

```bash
# Check maximum allocation
yarn.scheduler.maximum-allocation-mb

# Then request within limits
--executor-memory 8G  # Must be ≤ max allocation
```

### 5. Monitor Memory Usage

```bash
# Enable metrics
--conf spark.eventLog.enabled=true
--conf spark.eventLog.dir=hdfs://logs

# Check Spark UI for memory usage
# Look for: Storage Memory, Execution Memory, etc.
```

---

## Memory Configuration Quick Reference

### Driver Memory

```bash
# Minimal
--driver-memory 2G

# With overhead
--driver-memory 4G
--conf spark.driver.memoryOverhead=0.15

# Formula
Total = driver.memory + max(driver.memory × overhead, 384MB)
```

### Executor Memory (Scala/Java)

```bash
# Basic
--executor-memory 8G

# With overhead
--executor-memory 8G
--conf spark.executor.memoryOverhead=0.20

# Formula
Total = executor.memory + max(executor.memory × overhead, 384MB)
```

### Executor Memory (PySpark)

```bash
# Recommended
--executor-memory 8G
--conf spark.executor.memoryOverhead=0.20
--conf spark.executor.pyspark.memory=2G

# Formula
Total = executor.memory + overhead + pyspark.memory + offheap
```

---

## Summary

### Key Takeaways

1. **Driver Memory = JVM Heap + Overhead**
   - Minimum overhead: 384 MB or 10% (whichever is higher)

2. **Executor Memory = JVM Heap + Overhead + Off-Heap + PySpark**
   - Default overhead: 10% (often insufficient)
   - Recommended overhead: 20-25% for production

3. **PySpark Gets Almost Nothing from JVM Heap**
   - Must allocate dedicated PySpark memory
   - Or increase overhead significantly

4. **Overhead Memory is Critical**
   - Used for network buffers
   - Used for shuffle exchange
   - Often overlooked but causes OOMs

5. **Check Physical Limits First**
   - `yarn.scheduler.maximum-allocation-mb`
   - Cannot exceed cluster configuration

### Memory Allocation Formula

```
Container Memory = 
    JVM Heap 
    + max(JVM Heap × overhead%, 384 MB)
    + Off-Heap Memory
    + PySpark Memory
```

### Common OOM Prevention

```bash
# Increase overhead (most common fix)
--conf spark.executor.memoryOverhead=0.20

# Add PySpark memory (for PySpark apps)
--conf spark.executor.pyspark.memory=2G

# Reduce executor memory (if exceeding limits)
--executor-memory 6G  # Instead of 8G
```

Master these memory allocation concepts to build stable, performant Spark applications!