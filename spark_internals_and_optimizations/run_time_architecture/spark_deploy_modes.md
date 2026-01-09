# Spark Deploy Modes - Cluster vs Client

Understanding the two deploy modes in Apache Spark and when to use each.

---

## Table of Contents
- [Overview](#overview)
- [The Two Deploy Modes](#the-two-deploy-modes)
- [Cluster Mode Deep Dive](#cluster-mode-deep-dive)
- [Client Mode Deep Dive](#client-mode-deep-dive)
- [Key Differences](#key-differences)
- [When to Use Which Mode](#when-to-use-which-mode)
- [Complete Examples](#complete-examples)
- [Decision Guide](#decision-guide)

---

## Overview

### Critical Concept

**There are only TWO deploy modes in Spark:**
1. **Cluster Mode**
2. **Client Mode**

The deploy mode determines **where the Spark driver runs**.

### Common Misconception

❌ **WRONG:** "There are three modes: local, client, and cluster"

✅ **CORRECT:** "There are two deploy modes: client and cluster. Local is a master configuration, NOT a deploy mode."

---

## The Two Deploy Modes

### Quick Comparison

| Aspect | Cluster Mode | Client Mode |
|--------|--------------|-------------|
| **Driver Location** | Inside cluster (worker node) | Client machine |
| **Typical Use** | Production jobs | Interactive work (shells, notebooks) |
| **Can Log Off?** | Yes ✓ | No ✗ (driver dies) |
| **Performance** | Faster (low latency) | Slower (network latency) |
| **Debugging** | Harder (remote logs) | Easier (local logs) |
| **Recommended For** | Batch jobs, scheduled tasks | Development, exploration |

### The Only Difference

**The ONLY difference between cluster mode and client mode is WHERE THE DRIVER RUNS.**

- Cluster Mode: **Driver runs in the cluster**
- Client Mode: **Driver runs on client machine**

**Everything else is identical:**
- Executors always run in the cluster
- Resource allocation works the same way
- Application execution is the same

---

## Cluster Mode Deep Dive

### Architecture

```
┌─────────────────────────┐
│   Client Machine        │
│                         │
│   $ spark-submit        │
│   [command completes]   │
│   [can log off]         │
└────────┬────────────────┘
         │
         │ 1. Submit request
         ↓
┌─────────────────────────────────────────────────┐
│            YARN Resource Manager                │
└────────┬────────────────────────────────────────┘
         │
         │ 2. Create AM Container
         ↓
┌─────────────────────────────────────────────────┐
│              YARN Cluster                       │
│                                                 │
│  ┌───────────────────────────────────────┐     │
│  │  Worker Node 1                        │     │
│  │  ┌─────────────────────────────┐      │     │
│  │  │  AM Container               │      │     │
│  │  │  ┌───────────────────────┐  │      │     │
│  │  │  │   Spark Driver (JVM)  │  │      │     │
│  │  │  └───────────┬───────────┘  │      │     │
│  │  └──────────────┼──────────────┘      │     │
│  └─────────────────┼─────────────────────┘     │
│                    │                            │
│                    │ 3. Request executors       │
│                    ↓                            │
│  ┌─────────────────────────────────────┐       │
│  │  Worker Node 2                      │       │
│  │  ┌───────────────────────────┐      │       │
│  │  │  Executor Container 1     │      │       │
│  │  │  ┌─────────────────────┐  │      │       │
│  │  │  │  Executor (JVM)     │←─┼──────┼───────┤
│  │  │  └─────────────────────┘  │      │       │
│  │  └───────────────────────────┘      │       │
│  └─────────────────────────────────────┘       │
│                                                 │
│  ┌─────────────────────────────────────┐       │
│  │  Worker Node 3                      │       │
│  │  ┌───────────────────────────┐      │       │
│  │  │  Executor Container 2     │      │       │
│  │  │  ┌─────────────────────┐  │      │       │
│  │  │  │  Executor (JVM)     │←─┼──────┼───────┤
│  │  │  └─────────────────────┘  │      │       │
│  │  └───────────────────────────┘      │       │
│  └─────────────────────────────────────┘       │
└─────────────────────────────────────────────────┘
```

### Execution Flow

**Step 1: Submit Application**
```bash
$ spark-submit \
    --master yarn \
    --deploy-mode cluster \
    my_application.py
```

**Step 2: YARN RM Creates AM Container**
- RM allocates resources on a worker node
- Creates Application Master (AM) container
- Starts Spark driver inside AM container

**Step 3: Driver Requests Executors**
- Driver running in AM container
- Driver contacts YARN RM
- Requests executor containers

**Step 4: YARN Starts Executors**
- RM creates executor containers on worker nodes
- Hands over containers to driver
- Driver starts executor JVMs

**Step 5: Job Execution**
- Driver distributes tasks to executors
- Executors process data
- Results sent back to driver
- Driver coordinates completion

**Step 6: Client Machine**
- spark-submit command completes and exits
- User can log off
- Application continues running in cluster

### With PySpark

If you submit a PySpark application in cluster mode:

```
┌─────────────────────────────────┐
│  AM Container (Worker Node)     │
│                                 │
│  ┌───────────────────────┐      │
│  │  PySpark Driver       │      │
│  └──────────┬────────────┘      │
│             │ Py4J               │
│             ↓                    │
│  ┌───────────────────────┐      │
│  │  JVM Driver           │      │
│  └───────────────────────┘      │
└─────────────────────────────────┘
```

Both PySpark driver and JVM driver run in the AM container on a worker node.

---

## Client Mode Deep Dive

### Architecture

```
┌─────────────────────────────────────────┐
│   Client Machine                        │
│                                         │
│   $ spark-submit                        │
│   [command stays running]               │
│   [MUST stay connected]                 │
│                                         │
│  ┌───────────────────────────────────┐  │
│  │   Spark Driver (JVM)              │  │
│  │   [Runs locally]                  │  │
│  └─────────────┬─────────────────────┘  │
└────────────────┼─────────────────────────┘
                 │
                 │ 1. Request executors
                 ↓
┌────────────────────────────────────────────┐
│       YARN Resource Manager                │
└────────────────┬───────────────────────────┘
                 │
                 │ 2. Create executor containers
                 ↓
┌────────────────────────────────────────────┐
│         YARN Cluster                       │
│                                            │
│  ┌────────────────────────────────┐       │
│  │  Worker Node 1                 │       │
│  │  ┌──────────────────────┐      │       │
│  │  │  Executor Container  │      │       │
│  │  │  ┌────────────────┐  │      │       │
│  │  │  │ Executor (JVM) │←─┼──────┼───────┤
│  │  │  └────────────────┘  │      │       │
│  │  └──────────────────────┘      │       │
│  └────────────────────────────────┘       │
│                                            │
│  ┌────────────────────────────────┐       │
│  │  Worker Node 2                 │       │
│  │  ┌──────────────────────┐      │       │
│  │  │  Executor Container  │      │       │
│  │  │  ┌────────────────┐  │      │       │
│  │  │  │ Executor (JVM) │←─┼──────┼───────┤
│  │  │  └────────────────┘  │      │       │
│  │  └──────────────────────┘      │       │
│  └────────────────────────────────┘       │
└────────────────────────────────────────────┘
```

### Execution Flow

**Step 1: Submit Application**
```bash
$ spark-submit \
    --master yarn \
    --deploy-mode client \
    my_application.py
```

**Step 2: Driver Starts Locally**
- spark-submit starts driver JVM on client machine
- Driver runs on the same machine where you ran spark-submit
- **No AM container created for driver**

**Step 3: Driver Requests Executors**
- Driver (running locally) contacts YARN RM
- Requests executor containers

**Step 4: YARN Starts Executors**
- RM creates executor containers on worker nodes
- Hands over containers to driver
- Driver starts executor JVMs

**Step 5: Job Execution**
- Driver (local) distributes tasks to executors (cluster)
- Executors process data
- Results sent back to driver over network
- Driver coordinates completion

**Step 6: Client Machine**
- spark-submit command stays running
- User **must stay connected**
- Logging off kills the driver
- When driver dies, executors are orphaned and terminated

### With PySpark

If you submit a PySpark application in client mode:

```
┌─────────────────────────────────┐
│  Client Machine                 │
│                                 │
│  ┌───────────────────────┐      │
│  │  PySpark Driver       │      │
│  └──────────┬────────────┘      │
│             │ Py4J               │
│             ↓                    │
│  ┌───────────────────────┐      │
│  │  JVM Driver           │      │
│  └───────────────────────┘      │
└─────────────────────────────────┘
```

Both PySpark driver and JVM driver run on the client machine.

---

## Key Differences

### Driver Location

**Cluster Mode:**
```
Driver → Worker Node (in cluster)
Executors → Worker Nodes (in cluster)

✓ Driver close to executors
✓ Low network latency
✓ Fast communication
```

**Client Mode:**
```
Driver → Client Machine (outside cluster)
Executors → Worker Nodes (in cluster)

✗ Driver far from executors
✗ Network latency for every communication
✗ Slower performance
```

### Session Persistence

**Cluster Mode:**
```bash
$ spark-submit --deploy-mode cluster app.py
Job submitted successfully
Application ID: application_123456

$ exit
# Log off from client machine
# ✓ Application continues running in cluster
```

**Client Mode:**
```bash
$ spark-submit --deploy-mode client app.py
Starting Spark application...
[Driver running locally]

$ exit
# Log off from client machine
# ✗ Driver dies, executors terminated, job fails
```

### Network Communication

**Cluster Mode:**
```
Driver ←[Low Latency]→ Executors
(Same network, same data center)

Communication happens entirely within cluster
```

**Client Mode:**
```
Driver ←[High Latency]→ Executors
(Client machine ←→ Cluster)

Every task assignment, result collection crosses network boundary
```

### Resource Usage

**Cluster Mode:**
```
Client Machine: Minimal (just submission)
Cluster: Driver + Executors
```

**Client Mode:**
```
Client Machine: Driver resources (CPU, memory)
Cluster: Only executors
```

---

## When to Use Which Mode

### Use Cluster Mode For:

✅ **Production Jobs**
```bash
spark-submit \
  --deploy-mode cluster \
  production_etl.py
```
- Reliable execution
- Can submit and log off
- Better performance

✅ **Scheduled/Batch Jobs**
```bash
# Cron job or Airflow task
0 2 * * * spark-submit --deploy-mode cluster daily_report.py
```
- Automated execution
- No human interaction needed

✅ **Long-Running Applications**
```bash
spark-submit \
  --deploy-mode cluster \
  streaming_app.py
```
- Runs for hours/days
- Don't need to keep client connected

✅ **Performance-Critical Jobs**
```bash
spark-submit \
  --deploy-mode cluster \
  large_data_processing.py
```
- Heavy driver-executor communication
- Low latency required

### Use Client Mode For:

✅ **Interactive Development**
```bash
$ pyspark  # spark-shell for Scala
# Client mode by default
# Interactive exploration
```

✅ **Notebooks**
```python
# Jupyter, Zeppelin, Databricks notebooks
# Use client mode for interactive results
```

✅ **Debugging**
```bash
spark-submit \
  --deploy-mode client \
  test_app.py
```
- See logs immediately on screen
- Easier to debug
- Quick iterations

✅ **Short Test Runs**
```bash
spark-submit \
  --deploy-mode client \
  quick_test.py
```
- Testing application logic
- Small datasets
- Need immediate feedback

---

## Advantages and Disadvantages

### Cluster Mode

**Advantages:**
1. ✓ **Can log off** - Submit and disconnect
2. ✓ **Better performance** - Driver close to executors
3. ✓ **Lower network latency** - All communication in cluster
4. ✓ **More reliable** - Not dependent on client machine
5. ✓ **Automatic restart** - YARN can restart failed driver (with config)

**Disadvantages:**
1. ✗ **Harder debugging** - Logs in cluster, not local
2. ✗ **No interactive results** - Can't collect() to screen
3. ✗ **Need cluster monitoring** - Check YARN UI for status

### Client Mode

**Advantages:**
1. ✓ **Easy debugging** - Logs appear locally
2. ✓ **Interactive results** - See output immediately
3. ✓ **Quick iterations** - Fast development cycle
4. ✓ **Direct access** - Results available locally

**Disadvantages:**
1. ✗ **Must stay connected** - Log off = job dies
2. ✗ **Network latency** - Every communication crosses boundary
3. ✗ **Slower performance** - Driver-executor distance
4. ✗ **Client resource usage** - Driver consumes local resources
5. ✗ **Single point of failure** - Client machine/network issues kill job

---

## Complete Examples

### Example 1: Production ETL (Cluster Mode)

```bash
#!/bin/bash
# production_etl.sh

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "Daily Sales ETL" \
  --driver-cores 2 \
  --driver-memory 8G \
  --num-executors 10 \
  --executor-cores 4 \
  --executor-memory 16G \
  --conf spark.sql.shuffle.partitions=500 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=hdfs://namenode/spark-logs \
  sales_etl.py \
  --date $(date +%Y-%m-%d)

# Script exits immediately after submission
# Job continues running in cluster
# Can log off, shut down client machine
echo "Job submitted successfully"
```

**Why Cluster Mode?**
- Production job runs daily
- Automated via cron
- No need for client to stay connected
- Better performance for large dataset

---

### Example 2: Interactive Analysis (Client Mode)

```bash
# Start PySpark shell (client mode by default)
pyspark \
  --master yarn \
  --deploy-mode client \
  --driver-memory 4G \
  --executor-memory 8G \
  --num-executors 4
```

```python
# Interactive exploration
>>> df = spark.read.parquet("sales_data.parquet")
>>> df.show()  # Results appear immediately
>>> df.filter("amount > 1000").count()
>>> # Can see results interactively

>>> exit()  # Quitting terminates driver and executors
```

**Why Client Mode?**
- Interactive exploration
- Need immediate results
- Iterative development
- spark-shell/pyspark use client mode by default

---

### Example 3: Debugging Application (Client Mode)

```bash
# Test with small dataset locally
spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory 2G \
  --executor-memory 4G \
  --num-executors 2 \
  test_application.py

# Logs appear in terminal
# Easy to see errors
# Quick feedback loop
```

**Why Client Mode?**
- Debugging code
- See errors immediately
- Quick iterations
- Small test dataset

---

### Example 4: Scheduled Batch Job (Cluster Mode)

```bash
# Airflow DAG or Cron job
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "Hourly Aggregation" \
  --driver-memory 4G \
  --executor-memory 8G \
  --num-executors 5 \
  --conf spark.yarn.maxAppAttempts=3 \
  hourly_aggregation.py \
  --hour $(date +%H)

# Returns immediately
# Check status via YARN UI or Spark History Server
```

**Why Cluster Mode?**
- Automated execution
- No human monitoring
- Reliable execution
- Can retry on failure

---

## What Happens When Driver Dies?

### Cluster Mode - Driver Failure

```
Driver in AM Container dies
    ↓
YARN detects driver failure
    ↓
(With spark.yarn.maxAppAttempts > 1)
    ↓
YARN restarts driver in new AM container
    ↓
Application may recover and continue
```

**Configuration:**
```bash
--conf spark.yarn.maxAppAttempts=3
```

### Client Mode - Driver Failure

```
Driver on client machine dies
(User logs off, network drops, machine crashes)
    ↓
YARN detects driver disconnection
    ↓
Executors become orphans
    ↓
YARN terminates all executor containers
    ↓
Job fails completely
    ↓
No automatic recovery
```

**Result:** Complete job failure, must resubmit

---

## Interactive vs Batch Workloads

### Interactive Workloads (Client Mode)

**Examples:**
- Spark Shell (pyspark, spark-shell)
- Jupyter Notebooks
- Zeppelin Notebooks
- Databricks Notebooks
- Ad-hoc data exploration

**Characteristics:**
```python
# User types commands
>>> df = spark.read.csv("data.csv")
>>> df.show()  # See results immediately

# User sees output
+---+------+
| id| name |
+---+------+
|  1|Alice |
|  2|  Bob |
+---+------+

# Continue exploration
>>> df.filter("id > 1").show()
```

**Why Client Mode?**
1. Need interactive feedback
2. Results displayed to user
3. Iterative exploration
4. Easy to quit and restart

### Batch Workloads (Cluster Mode)

**Examples:**
- ETL pipelines
- Data processing jobs
- Scheduled reports
- Model training
- Automated workflows

**Characteristics:**
```python
# Application runs start to finish
def main():
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv("data.csv")
    result = df.groupBy("category").count()
    result.write.parquet("output/")
    spark.stop()

# No interaction needed
# Results written to storage
# Job completes and exits
```

**Why Cluster Mode?**
1. No user interaction needed
2. Results go to storage
3. Can submit and forget
4. Better for automation

---

## Decision Guide

### Quick Decision Tree

```
Are you doing interactive work?
(Shell, notebook, exploration)
├─ Yes → CLIENT MODE
└─ No → Is this a production/scheduled job?
    ├─ Yes → CLUSTER MODE
    └─ No → Are you debugging?
        ├─ Yes → CLIENT MODE
        └─ No → Default to CLUSTER MODE
```

### By Use Case

| Use Case | Deploy Mode | Reason |
|----------|-------------|--------|
| Production ETL | **Cluster** | Reliability, performance |
| Scheduled jobs | **Cluster** | Automation, can log off |
| Spark Shell | **Client** | Interactive, need feedback |
| Jupyter Notebook | **Client** | Interactive, see results |
| Debugging | **Client** | See logs locally |
| Long-running apps | **Cluster** | Can disconnect |
| Quick tests | **Client** | Fast feedback |
| Automated pipelines | **Cluster** | No monitoring needed |
| Data exploration | **Client** | Interactive analysis |
| Model training | **Cluster** | Long-running, stable |

---

## Summary

### Key Takeaways

1. **Two Deploy Modes Only**
   - Cluster Mode (driver in cluster)
   - Client Mode (driver on client machine)

2. **The Only Difference**
   - Where the driver runs
   - Everything else is identical

3. **Cluster Mode = Production**
   - Use for 99% of production jobs
   - Better performance
   - Can submit and log off
   - More reliable

4. **Client Mode = Interactive**
   - Spark Shell, notebooks
   - Debugging and testing
   - Need immediate results
   - Must stay connected

5. **Default Choice**
   - When in doubt → **Cluster Mode**
   - Only use client mode for specific interactive needs

### The Rule of Thumb

```
Production/Automated → Cluster Mode
Interactive/Debugging → Client Mode
```

### Remember

**You will almost always use cluster mode for submitting applications.**

Client mode exists specifically for interactive workloads like shells and notebooks, where you need immediate feedback and easy exit.

Master these concepts to deploy Spark applications effectively!