# Spark-Submit Command - Complete Guide

Learn how to deploy Spark applications to clusters using the spark-submit command-line tool.

---

## Table of Contents
- [What is spark-submit?](#what-is-spark-submit)
- [General Structure](#general-structure)
- [Common Submit Options](#common-submit-options)
- [Deploy Modes Explained](#deploy-modes-explained)
- [Resource Allocation](#resource-allocation)
- [Complete Examples](#complete-examples)
- [Best Practices](#best-practices)

---

## What is spark-submit?

**spark-submit** is a command-line tool that allows you to submit Spark applications to a cluster.

### When to Use

- Deploying to test clusters
- Production deployments
- Running on YARN or Kubernetes clusters
- Local testing with cluster-like configuration

### Why spark-submit?

While there are multiple ways to submit Spark applications, **spark-submit is the most commonly used method** due to its:
- Flexibility
- Standardization across cluster managers
- Rich configuration options
- Support for all languages (Python, Scala, Java, R)

---

## General Structure

### Basic Command Format

```bash
spark-submit \
  [OPTIONS] \
  <application-jar-or-python-file> \
  [application-arguments]
```

### Minimalist Example

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 4G \
  my_application.py
```

### Structure Breakdown

```
┌─────────────────────────────────────────────────────┐
│ spark-submit                                        │
│   --option1 value1        ← Configuration options  │
│   --option2 value2        ← (using -- prefix)      │
│   --option3 value3                                  │
│   application.py          ← Your application       │
│   arg1 arg2               ← Application arguments  │
└─────────────────────────────────────────────────────┘
```

---

## Common Submit Options

### Complete Options Reference

| Option | Description | Example | Required |
|--------|-------------|---------|----------|
| `--master` | Cluster manager URL | `yarn`, `local`, `local[4]` | Yes |
| `--deploy-mode` | Where to run driver | `client`, `cluster` | Yes |
| `--class` | Main class (Java/Scala only) | `com.example.MainClass` | For JVM apps |
| `--driver-cores` | CPU cores for driver | `2` | No (default: 1) |
| `--driver-memory` | Memory for driver | `8G` | No (default: 1G) |
| `--num-executors` | Number of executors | `4` | No (default: 2) |
| `--executor-cores` | CPU cores per executor | `4` | No (default: 1) |
| `--executor-memory` | Memory per executor | `16G` | No (default: 1G) |
| `--conf` | Additional Spark config | `spark.key=value` | No |
| `--name` | Application name | `"My App"` | No |
| `--jars` | Additional JARs | `libs/dep.jar` | No |
| `--files` | Additional files | `config.properties` | No |
| `--py-files` | Python dependencies | `utils.zip` | No |

---

## Master Option

### What is --master?

Specifies the **cluster manager** to connect to.

### Common Master Values

| Value | Description | Use Case |
|-------|-------------|----------|
| `yarn` | Hadoop YARN cluster | Production on Hadoop |
| `k8s://https://...` | Kubernetes cluster | Cloud-native deployments |
| `local` | Run locally, single thread | Quick testing |
| `local[N]` | Run locally with N threads | Local development |
| `local[*]` | Run locally with all CPU cores | Maximum local performance |
| `spark://HOST:PORT` | Standalone Spark cluster | Spark-only cluster |

### Examples

```bash
# Production - YARN cluster
--master yarn

# Development - Local with 4 threads
--master local[4]

# Kubernetes cluster
--master k8s://https://k8s.example.com:6443

# Standalone Spark cluster
--master spark://master-host:7077
```

---

## Deploy Modes Explained

**CRITICAL:** There are only **TWO** deploy modes!

### Common Misconception

❌ **Wrong:** "There are three deploy modes: local, client, and cluster"

✓ **Correct:** "There are two deploy modes: client and cluster. Local is a master option, NOT a deploy mode."

---

### Client Mode

**Driver runs on the machine where spark-submit was executed.**

```bash
--deploy-mode client
```

#### Architecture

```
┌────────────────────────┐
│  Your Local Machine    │
│  (or Edge Node)        │
│                        │
│  ┌──────────────────┐  │
│  │  Spark Driver    │  │
│  │  (runs here)     │  │
│  └────────┬─────────┘  │
│           │            │
└───────────┼────────────┘
            │
            ↓ [Network]
┌───────────────────────────────────┐
│        YARN Cluster               │
│                                   │
│  ┌──────────┐  ┌──────────┐      │
│  │Executor 1│  │Executor 2│ ...  │
│  └──────────┘  └──────────┘      │
└───────────────────────────────────┘
```

#### Characteristics

**Pros:**
- Easy to debug (logs on local machine)
- Interactive applications (spark-shell, notebooks)
- Quick development cycle

**Cons:**
- Driver must stay connected throughout execution
- Network latency between driver and executors
- Not suitable for production long-running jobs

**Use Cases:**
- Interactive exploration (Jupyter, Zeppelin)
- Development and testing
- Short-running jobs
- When you need immediate access to results

---

### Cluster Mode

**Driver runs inside the cluster on a worker node.**

```bash
--deploy-mode cluster
```

#### Architecture

```
┌────────────────────────┐
│  Your Local Machine    │
│  (spark-submit exits)  │
└────────────────────────┘
            │
            ↓ [Submit job]
┌───────────────────────────────────┐
│        YARN Cluster               │
│                                   │
│  ┌──────────────────┐             │
│  │  Spark Driver    │             │
│  │  (runs in AM)    │             │
│  └────────┬─────────┘             │
│           │                       │
│           ↓                       │
│  ┌──────────┐  ┌──────────┐      │
│  │Executor 1│  │Executor 2│ ...  │
│  └──────────┘  └──────────┘      │
└───────────────────────────────────┘
```

#### Characteristics

**Pros:**
- Driver and executors in same cluster (low latency)
- Client can disconnect after submission
- Better for production
- Automatic driver restart on failure (with YARN)

**Cons:**
- Harder to debug (logs in cluster)
- Can't collect results interactively
- Need cluster monitoring to track job

**Use Cases:**
- Production jobs
- Scheduled/automated jobs
- Long-running applications
- Batch processing

---

### Deploy Mode Comparison Table

| Aspect | Client Mode | Cluster Mode |
|--------|-------------|--------------|
| **Driver Location** | Local machine/edge node | Inside cluster |
| **Client Connection** | Must stay connected | Can disconnect |
| **Debugging** | Easy (local logs) | Harder (cluster logs) |
| **Network Latency** | Higher (driver ↔ executors) | Lower (all in cluster) |
| **Production Use** | Not recommended | Recommended ✓ |
| **Interactive Use** | Recommended ✓ | Not suitable |
| **Driver Failure** | Job fails | Can restart (YARN) |

---

## Class Option (Java/Scala Only)

### What is --class?

Specifies the **fully qualified name** of the main class containing the `main()` method.

### When Required

- ✓ **Required for Java applications**
- ✓ **Required for Scala applications**
- ✗ **NOT used for PySpark** (Python files are self-contained)
- ✗ **NOT used for R applications**

### Example

```bash
# Scala/Java - class is REQUIRED
spark-submit \
  --class com.example.MySparkApp \
  --master yarn \
  my-application.jar

# PySpark - NO class option
spark-submit \
  --master yarn \
  my_application.py
```

---

## Configuration Option (--conf)

### What is --conf?

Allows you to set **additional Spark configurations** beyond the standard options.

### Format

```bash
--conf spark.config.key=value
```

### Common Configurations

```bash
# Executor memory overhead (20%)
--conf spark.executor.memoryOverhead=0.20

# Dynamic allocation
--conf spark.dynamicAllocation.enabled=true
--conf spark.dynamicAllocation.minExecutors=2
--conf spark.dynamicAllocation.maxExecutors=10

# Shuffle partitions
--conf spark.sql.shuffle.partitions=200

# Event log for Spark History Server
--conf spark.eventLog.enabled=true
--conf spark.eventLog.dir=hdfs://namenode/spark-logs

# Compression
--conf spark.sql.parquet.compression.codec=snappy

# Network timeout
--conf spark.network.timeout=300s
```

### Multiple Configurations

```bash
spark-submit \
  --conf spark.executor.memoryOverhead=0.20 \
  --conf spark.sql.shuffle.partitions=100 \
  --conf spark.dynamicAllocation.enabled=true \
  my_application.py
```

---

## Resource Allocation

### Understanding Resource Options

Your Spark application consists of:
- **1 Driver** (coordinates the job)
- **N Executors** (process the data)

You can request specific resources for each.

### Driver Resource Options

| Option | Description | Default | Example |
|--------|-------------|---------|---------|
| `--driver-cores` | CPU cores for driver | 1 | `2` |
| `--driver-memory` | Memory for driver | 1G | `8G` |

### Executor Resource Options

| Option | Description | Default | Example |
|--------|-------------|---------|---------|
| `--num-executors` | Number of executors | 2 | `4` |
| `--executor-cores` | CPU cores per executor | 1 | `4` |
| `--executor-memory` | Memory per executor | 1G | `16G` |

### Resource Calculation Example

```bash
--driver-cores 2
--driver-memory 8G
--num-executors 4
--executor-cores 4
--executor-memory 16G
```

**Total Resources:**
- **Driver:** 2 cores, 8 GB
- **Executors:** 4 executors × (4 cores, 16 GB) = 16 cores, 64 GB
- **Total:** 18 cores, 72 GB

---

## Complete Examples

### Example 1: PySpark Application on YARN (Cluster Mode)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-cores 2 \
  --driver-memory 8G \
  --num-executors 4 \
  --executor-cores 4 \
  --executor-memory 16G \
  --conf spark.executor.memoryOverhead=0.20 \
  --conf spark.sql.shuffle.partitions=100 \
  hello_spark.py
```

**What happens:**
1. Submit job to YARN Resource Manager
2. RM creates Application Master container with (2 cores, 8 GB)
3. Driver starts in AM container
4. Driver requests 4 executor containers from RM
5. Each executor gets (4 cores, 16 GB)
6. Job executes in cluster
7. spark-submit command can exit (cluster mode)

---

### Example 2: Scala Application on YARN (Cluster Mode)

```bash
spark-submit \
  --class com.example.HelloSpark \
  --master yarn \
  --deploy-mode cluster \
  --driver-cores 2 \
  --driver-memory 8G \
  --num-executors 4 \
  --executor-cores 4 \
  --executor-memory 16G \
  hello-spark.jar
```

**Key Difference from PySpark:**
- `--class` option is **required** for Scala/Java
- Application is a JAR file instead of .py file

---

### Example 3: PySpark on YARN (Client Mode) for Development

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-cores 1 \
  --driver-memory 4G \
  --num-executors 2 \
  --executor-cores 2 \
  --executor-memory 8G \
  test_app.py
```

**Characteristics:**
- Driver runs locally
- Smaller resource allocation (development)
- Logs appear in terminal
- Can see results immediately

---

### Example 4: Local Mode for Testing

```bash
spark-submit \
  --master local[4] \
  --driver-memory 4G \
  test_app.py
```

**Characteristics:**
- Runs entirely on local machine
- Uses 4 threads (local[4])
- No cluster needed
- No executor options (runs in single JVM)
- Quick for testing small datasets

---

### Example 5: Production Job with Advanced Configuration

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "Daily Sales ETL" \
  --driver-cores 4 \
  --driver-memory 16G \
  --num-executors 10 \
  --executor-cores 5 \
  --executor-memory 32G \
  --conf spark.executor.memoryOverhead=0.20 \
  --conf spark.sql.shuffle.partitions=500 \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=hdfs://namenode/spark-logs \
  --conf spark.sql.parquet.compression.codec=snappy \
  --conf spark.speculation=true \
  --files config/app.conf \
  --py-files libs/utils.zip \
  sales_etl.py \
  --input-date 2024-01-15 \
  --output-path hdfs://namenode/processed/sales
```

**Features:**
- Named application for monitoring
- Large resource allocation
- Multiple custom configurations
- Additional files and dependencies
- Application arguments at the end

---

### Example 6: Kubernetes Deployment

```bash
spark-submit \
  --master k8s://https://kubernetes.example.com:6443 \
  --deploy-mode cluster \
  --name "Spark on K8s" \
  --conf spark.kubernetes.container.image=my-spark:3.5.0 \
  --conf spark.kubernetes.namespace=spark-jobs \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --driver-memory 4G \
  --executor-memory 8G \
  --num-executors 3 \
  my_application.py
```

**Kubernetes-specific:**
- K8s master URL
- Container image specification
- Namespace and service account
- Pod-based execution

---

## Application Arguments

### Passing Arguments to Your Application

Arguments **after** the application file are passed to your application.

```bash
spark-submit \
  [SPARK OPTIONS] \
  my_app.py \
  arg1 arg2 arg3  # These go to your application
```

### Python Example

**spark-submit command:**
```bash
spark-submit \
  --master yarn \
  process_data.py \
  --input-path /data/input \
  --output-path /data/output \
  --date 2024-01-15
```

**Python application (process_data.py):**
```python
import argparse
from pyspark.sql import SparkSession

def main():
    # Parse application arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-path', required=True)
    parser.add_argument('--output-path', required=True)
    parser.add_argument('--date', required=True)
    args = parser.parse_args()
    
    # Use arguments
    spark = SparkSession.builder.appName("Data Processing").getOrCreate()
    
    df = spark.read.parquet(args.input_path)
    result = df.filter(f"date = '{args.date}'")
    result.write.parquet(args.output_path)
    
    spark.stop()

if __name__ == "__main__":
    main()
```

---

## Memory Overhead Configuration

### What is Memory Overhead?

Additional memory allocated for:
- Off-heap memory
- VM overheads
- String interning
- Other native memory usage

### Configuration

```bash
--conf spark.executor.memoryOverhead=0.20
```

**Default:** 0.10 (10% of executor memory)
**Example:** 0.20 (20% of executor memory)

### Calculation

```
Total Container Memory = executor-memory + (executor-memory × memoryOverhead)
```

**Example:**
```bash
--executor-memory 16G
--conf spark.executor.memoryOverhead=0.20
```

**Result:**
- Executor heap: 16 GB
- Overhead: 16 GB × 0.20 = 3.2 GB
- **Total container: 19.2 GB**

### When to Increase

- Large number of string operations
- Many DataFrames in memory
- Complex UDFs
- Getting "Container killed by YARN for exceeding memory limits" errors

---

## Best Practices

### 1. Resource Allocation

**Don't Request Too Much:**
```bash
# ❌ Bad: May not fit in cluster
--num-executors 100
--executor-cores 32
--executor-memory 128G
```

**Right-Size Resources:**
```bash
# ✓ Good: Reasonable for typical cluster
--num-executors 10
--executor-cores 4
--executor-memory 16G
```

### 2. Executor Configuration

**Avoid Single Large Executor:**
```bash
# ❌ Bad: 1 executor with 32 cores
--num-executors 1
--executor-cores 32
```

**Use Multiple Smaller Executors:**
```bash
# ✓ Good: 8 executors with 4 cores each
--num-executors 8
--executor-cores 4
```

**Why?** Better parallelism, fault tolerance, and resource utilization.

### 3. Deploy Mode Selection

**Development:**
```bash
--deploy-mode client  # Easy debugging
```

**Production:**
```bash
--deploy-mode cluster  # Better reliability
```

### 4. Configuration Management

**Use Properties File:**
```bash
# Create spark-defaults.conf
spark.executor.memory=16G
spark.executor.cores=4
spark.sql.shuffle.partitions=200

# Submit with defaults
spark-submit \
  --properties-file spark-defaults.conf \
  my_app.py
```

### 5. Naming Applications

```bash
# ✓ Good: Descriptive names
--name "Daily Sales ETL - 2024-01-15"

# ❌ Bad: Generic names
--name "Test"
```

### 6. Enable Event Logging

```bash
--conf spark.eventLog.enabled=true
--conf spark.eventLog.dir=hdfs://namenode/spark-logs
```

**Benefits:** View job history in Spark History Server

---

## Common Issues and Solutions

### Issue 1: "Class not found" for Scala/Java

**Problem:**
```bash
Error: Could not find or load main class com.example.MyApp
```

**Solution:**
```bash
# Ensure --class matches the package and class name exactly
--class com.example.MyApp  # Must match package structure
```

### Issue 2: Container Killed by YARN

**Problem:**
```
Container killed by YARN for exceeding memory limits
```

**Solution:**
```bash
# Increase memory overhead
--conf spark.executor.memoryOverhead=0.20

# Or reduce executor memory
--executor-memory 12G  # Instead of 16G
```

### Issue 3: Not Enough Resources

**Problem:**
```
Application application_xxx is not allocated enough resources
```

**Solution:**
```bash
# Reduce resource request
--num-executors 4      # Instead of 10
--executor-memory 8G   # Instead of 16G
```

### Issue 4: Slow Performance in Client Mode

**Problem:** High latency between driver and executors

**Solution:**
```bash
# Use cluster mode for production
--deploy-mode cluster
```

---

## Summary

### Essential spark-submit Concepts

1. **Command Structure:**
   ```bash
   spark-submit [OPTIONS] application [app-arguments]
   ```

2. **Two Deploy Modes:**
   - **Client:** Driver on local machine (development)
   - **Cluster:** Driver in cluster (production)

3. **Resource Configuration:**
   - Driver: `--driver-cores`, `--driver-memory`
   - Executors: `--num-executors`, `--executor-cores`, `--executor-memory`

4. **Master Options:**
   - `yarn` (YARN cluster)
   - `local[N]` (local with N threads)
   - `k8s://...` (Kubernetes)

5. **Language-Specific:**
   - Java/Scala: Use `--class`
   - Python: No `--class` needed

### Quick Reference Card

```bash
# PySpark Production Template
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-cores 2 \
  --driver-memory 8G \
  --num-executors 4 \
  --executor-cores 4 \
  --executor-memory 16G \
  --conf spark.executor.memoryOverhead=0.20 \
  --conf spark.sql.shuffle.partitions=200 \
  my_application.py

# Scala Production Template
spark-submit \
  --class com.example.MainClass \
  --master yarn \
  --deploy-mode cluster \
  --driver-cores 2 \
  --driver-memory 8G \
  --num-executors 4 \
  --executor-cores 4 \
  --executor-memory 16G \
  my-application.jar
```

Master these concepts to successfully deploy Spark applications to production clusters!