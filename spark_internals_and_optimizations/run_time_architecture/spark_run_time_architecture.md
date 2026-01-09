# Apache Spark Runtime Architecture - Complete Guide 

Understanding how Spark applications run on distributed clusters.

---

## Table of Contents
- [Key Concept: Every Spark Application is Distributed](#key-concept-every-spark-application-is-distributed)
- [Cluster Technologies](#cluster-technologies)
- [Understanding Clusters](#understanding-clusters)
- [Containers Explained](#containers-explained)
- [PySpark vs Scala Architecture](#pyspark-vs-scala-architecture)
- [Driver and Executors](#driver-and-executors)
- [Complete Architecture Examples](#complete-architecture-examples)
- [Python Workers](#python-workers)
- [Key Terminology](#key-terminology)

---

## Key Concept: Every Spark Application is Distributed

**Important:** Apache Spark is a distributed computing platform, and **every Spark application is a distributed application in itself.**

What does this mean?
- Your application doesn't run on a single machine
- The work is distributed across multiple nodes
- The driver coordinates, executors perform the work

---

## Cluster Technologies

Spark applications run on clusters. While you can run locally for development/testing, production always uses a cluster.

### Most Common Cluster Managers (90%+ market share)

1. **Hadoop YARN Cluster**
   - Traditional big data ecosystem
   - Resource management via YARN Resource Manager

2. **Kubernetes Cluster**
   - Modern container orchestration
   - Cloud-native deployments

### Other Options
- Apache Mesos
- Spark Standalone Cluster

---

## Understanding Clusters

### What is a Cluster?

A cluster is a **pool of physical computers** networked together and managed by a cluster manager.

### Example Cluster Configuration

```
Cluster Specifications:
- Number of machines: 10
- CPU per machine: 16 cores
- RAM per machine: 64 GB
- Cluster Manager: Hadoop YARN
```

**Total Cluster Capacity:**
- **Total CPU:** 10 × 16 = **160 cores**
- **Total RAM:** 10 × 64 GB = **640 GB**

### Cluster Structure

```
┌─────────────────────────────────────────────────┐
│           YARN Resource Manager (RM)            │
└─────────────────────────────────────────────────┘
                      │
      ┌───────────────┼───────────────┐
      ↓               ↓               ↓
┌──────────┐    ┌──────────┐    ┌──────────┐
│ Worker 1 │    │ Worker 2 │ .. │ Worker 10│
│ 16 cores │    │ 16 cores │    │ 16 cores │
│  64 GB   │    │  64 GB   │    │  64 GB   │
└──────────┘    └──────────┘    └──────────┘
```

---

## Containers Explained

### What is a Container?

A **container** is an **isolated virtual runtime environment** with allocated CPU and memory resources.

### Container Characteristics

- **Isolated:** Separate from other processes on the same worker
- **Resource-bound:** Fixed CPU cores and memory allocation
- **Virtual:** Logical partition of physical resources

### Example: Container Creation

**Physical Worker Node:**
- Total: 16 cores, 64 GB RAM

**Container Allocation:**
- Container gets: 4 cores, 16 GB RAM
- Remaining for worker: 12 cores, 48 GB RAM

```
┌────────────────────────────────────────┐
│      Worker Node (16 cores, 64GB)     │
│                                        │
│  ┌──────────────────────────────┐     │
│  │  Container (4 cores, 16GB)   │     │
│  │                              │     │
│  │  [Application runs here]     │     │
│  └──────────────────────────────┘     │
│                                        │
│  Available: 12 cores, 48GB             │
└────────────────────────────────────────┘
```

**Important:** Application can **only use** the resources allocated to its container, not the entire worker node capacity.

---

## Submitting a Spark Application

### The spark-submit Process

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 16G \
  --executor-cores 4 \
  --num-executors 4 \
  my_application.py
```

### What Happens:

1. **Request goes to YARN Resource Manager (RM)**
2. **RM creates Application Master (AM) Container** on a worker node
3. **Starts application's main() method** in the AM container

```
spark-submit
    ↓
YARN RM
    ↓
Creates AM Container (4 cores, 16GB)
    ↓
Starts main() method
```

---

## PySpark vs Scala Architecture

### Understanding PySpark Internals

**Key Fact:** Spark was written in **Scala** and runs in the **JVM**.

### How PySpark Works

To make Spark available to Python developers:

1. **Scala code** (original Spark)
2. **Java wrapper** created on top of Scala
3. **Python wrapper (PySpark)** created on top of Java wrapper

```
PySpark (Python)
    ↓
Java Wrapper
    ↓
Scala Code (Spark Core)
    ↓
JVM
```

### Communication: Py4J

**Py4J** enables Python to call Java applications.

```
Python Code ──[Py4J]──> Java Code ──> Scala Code
```

---

## Driver and Executors

### The Distributed Architecture

**Critical Concept:** The driver doesn't do data processing—it **distributes work** to executors.

### Application Startup Flow

**Step 1: Driver Starts**
```
AM Container created
    ↓
Driver starts (main method)
    ↓
Driver initializes
```

**Step 2: Driver Requests Executors**
```
Driver → Requests containers from YARN RM
    ↓
RM creates executor containers
    ↓
Driver starts executors in containers
```

**Step 3: Work Distribution**
```
Driver → Assigns tasks to executors
Executors → Process data
Executors → Return results to driver
```

### Driver Responsibilities

- Create execution plan
- Break jobs into stages and tasks
- Assign tasks to executors
- Monitor executor health
- Manage application lifecycle
- Collect results

### Executor Responsibilities

- Execute tasks assigned by driver
- Perform data processing
- Store computed results
- Report status to driver

---

## Complete Architecture Examples

### Example 1: Scala/Java Spark Application

```
┌─────────────────────────────────────────────┐
│            YARN Resource Manager            │
└─────────────────────────────────────────────┘
                    │
        ┌───────────┴────────────┐
        ↓                        ↓
┌──────────────┐        ┌──────────────────┐
│AM Container  │        │Executor Containers│
│(Driver Node) │        │ (Worker Nodes)   │
│              │        │                  │
│ ┌──────────┐│        │ ┌──────────────┐ │
│ │   JVM    ││        │ │  JVM Exec 1  │ │
│ │ Driver   ││←───────│→│              │ │
│ └──────────┘│        │ └──────────────┘ │
│              │        │                  │
│ 4 cores      │        │ ┌──────────────┐ │
│ 16 GB        │        │ │  JVM Exec 2  │ │
└──────────────┘        │ │              │ │
                        │ └──────────────┘ │
                        │                  │
                        │ ┌──────────────┐ │
                        │ │  JVM Exec 3  │ │
                        │ │              │ │
                        │ └──────────────┘ │
                        │                  │
                        │ ┌──────────────┐ │
                        │ │  JVM Exec 4  │ │
                        │ │              │ │
                        │ └──────────────┘ │
                        │                  │
                        │ Each: 4 cores,   │
                        │       16 GB      │
                        └──────────────────┘
```

**Architecture:**
- **1 JVM Driver**
- **4 JVM Executors**
- All communication in JVM (fast)

---

### Example 2: PySpark Application (DataFrame API Only)

```
┌─────────────────────────────────────────────┐
│            YARN Resource Manager            │
└─────────────────────────────────────────────┘
                    │
        ┌───────────┴────────────┐
        ↓                        ↓
┌──────────────┐        ┌──────────────────┐
│AM Container  │        │Executor Containers│
│(Driver Node) │        │ (Worker Nodes)   │
│              │        │                  │
│ ┌──────────┐│        │ ┌──────────────┐ │
│ │ PySpark  ││        │ │  JVM Exec 1  │ │
│ │ Driver   ││        │ │              │ │
│ └────┬─────┘│        │ └──────────────┘ │
│      │Py4J  │        │                  │
│      ↓      │        │ ┌──────────────┐ │
│ ┌──────────┐│        │ │  JVM Exec 2  │ │
│ │   JVM    ││←───────│→│              │ │
│ │ Driver   ││        │ └──────────────┘ │
│ └──────────┘│        │                  │
│              │        │ ┌──────────────┐ │
│ 4 cores      │        │ │  JVM Exec 3  │ │
│ 16 GB        │        │ │              │ │
└──────────────┘        │ └──────────────┘ │
                        │                  │
                        │ ┌──────────────┐ │
                        │ │  JVM Exec 4  │ │
                        │ │              │ │
                        │ └──────────────┘ │
                        └──────────────────┘
```

**Architecture:**
- **1 PySpark Driver** (Python process)
- **1 JVM Driver** (communicates via Py4J)
- **4 JVM Executors**
- PySpark code translated to Java, runs in JVM

**Why JVM Executors?**
- PySpark DataFrame operations are translated to Java
- Execution happens in JVM for performance
- No Python runtime needed at executors (for pure DataFrame API)

---

### Example 3: PySpark with Python Code/UDFs

```
┌─────────────────────────────────────────────┐
│            YARN Resource Manager            │
└─────────────────────────────────────────────┘
                    │
        ┌───────────┴────────────┐
        ↓                        ↓
┌──────────────┐        ┌──────────────────┐
│AM Container  │        │Executor Containers│
│(Driver Node) │        │ (Worker Nodes)   │
│              │        │                  │
│ ┌──────────┐│        │ ┌──────────────┐ │
│ │ PySpark  ││        │ │  JVM Exec 1  │ │
│ │ Driver   ││        │ │              │ │
│ └────┬─────┘│        │ │  ┌─────────┐ │ │
│      │Py4J  │        │ │  │ Python  │ │ │
│      ↓      │        │ │  │ Worker  │ │ │
│ ┌──────────┐│        │ │  └─────────┘ │ │
│ │   JVM    ││←───────│→│              │ │
│ │ Driver   ││        │ └──────────────┘ │
│ └──────────┘│        │                  │
│              │        │ ┌──────────────┐ │
│ 4 cores      │        │ │  JVM Exec 2  │ │
│ 16 GB        │        │ │              │ │
└──────────────┘        │ │  ┌─────────┐ │ │
                        │ │  │ Python  │ │ │
                        │ │  │ Worker  │ │ │
                        │ │  └─────────┘ │ │
                        │ └──────────────┘ │
                        │                  │
                        │ ┌──────────────┐ │
                        │ │  JVM Exec 3  │ │
                        │ │  + Python    │ │
                        │ │  Worker      │ │
                        │ └──────────────┘ │
                        │                  │
                        │ ┌──────────────┐ │
                        │ │  JVM Exec 4  │ │
                        │ │  + Python    │ │
                        │ │  Worker      │ │
                        │ └──────────────┘ │
                        └──────────────────┘
```

**Architecture:**
- **1 PySpark Driver** (Python process)
- **1 JVM Driver** (communicates via Py4J)
- **4 JVM Executors**
- **4 Python Workers** (one per executor)

**When is this architecture used?**
- Using Python libraries not in PySpark (pandas, numpy, etc.)
- Creating Python UDFs (User Defined Functions)
- Custom Python code that can't be translated to Java

---

## Python Workers

### What is a Python Worker?

A **Python Worker** is a **Python runtime environment** created inside the executor container.

### When Do You Need Python Workers?

**NOT Needed:**
```python
# Pure PySpark DataFrame API - no Python workers needed
df.select("column").filter(col("age") > 25).groupBy("name").count()
```
This code is translated to Java and runs in JVM only.

**Needed:**
```python
# Python UDF - requires Python worker
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def custom_python_function(text):
    # Custom Python logic
    return text.upper().strip()

df.withColumn("processed", custom_python_function(col("text")))
```

**Needed:**
```python
# Using pandas UDF
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("long")
def pandas_processing(series: pd.Series) -> pd.Series:
    return series * 2

df.withColumn("doubled", pandas_processing(col("value")))
```

### Performance Impact

**PySpark DataFrame API (No Python Workers):**
```
Fast → All code runs in JVM
```

**PySpark with Python Workers:**
```
Slower → Data serialized from JVM → Python → back to JVM
```

**Serialization Overhead:**
```
JVM Executor
    ↓ (serialize data)
Python Worker (process data)
    ↓ (serialize result)
JVM Executor (continue processing)
```

### Best Practice

**Minimize Python Worker usage:**
- Use PySpark DataFrame API when possible
- Avoid Python UDFs if equivalent PySpark function exists
- Use Pandas UDFs (vectorized) instead of regular UDFs when needed
- Keep Python code to minimum necessary

---

## Complete Example with Code

### Scala Application Example

```scala
import org.apache.spark.sql.SparkSession

object ScalaSparkApp {
  def main(args: Array[String]): Unit = {
    // This runs in JVM Driver
    val spark = SparkSession.builder()
      .appName("Scala Spark Application")
      .getOrCreate()
    
    // Read data - happens on executors
    val df = spark.read.parquet("data.parquet")
    
    // Transformations - executed on executors
    val result = df
      .filter($"age" > 25)
      .groupBy($"country")
      .count()
    
    // Action - triggers job execution on executors
    result.write.parquet("output/")
    
    spark.stop()
  }
}
```

**Runtime Architecture:**
```
JVM Driver (AM Container)
    ↓ [distributes work]
JVM Executors (4 containers)
    ↓ [process data]
Results back to Driver
```

---

### PySpark Application Example (DataFrame API)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# This runs in PySpark Driver
spark = SparkSession.builder \
    .appName("PySpark Application") \
    .getOrCreate()

# Read data - translated to Java, runs on JVM executors
df = spark.read.parquet("data.parquet")

# Transformations - all translated to Java
result = df \
    .filter(col("age") > 25) \
    .groupBy("country") \
    .count()

# Action - triggers execution on JVM executors
result.write.parquet("output/")

spark.stop()
```

**Runtime Architecture:**
```
PySpark Driver ──[Py4J]──> JVM Driver (AM Container)
                                ↓ [distributes work]
                        JVM Executors (4 containers)
                                ↓ [process data]
                        Results back to Driver
```

**No Python Workers needed!** All DataFrame operations run in JVM.

---

### PySpark with Python UDF Example

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName("PySpark with UDF") \
    .getOrCreate()

# Python UDF - requires Python Worker
@udf(returnType=StringType())
def process_text(text):
    # Custom Python logic (not available in PySpark)
    import re
    cleaned = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    return cleaned.upper().strip()

df = spark.read.parquet("data.parquet")

# This transformation requires Python Workers
result = df.withColumn("cleaned_text", process_text(col("text")))

result.write.parquet("output/")

spark.stop()
```

**Runtime Architecture:**
```
PySpark Driver ──[Py4J]──> JVM Driver (AM Container)
                                ↓ [distributes work]
                        JVM Executors (4 containers)
                                ↓
                        Each executor creates Python Worker
                                ↓
                        Data: JVM → Python → JVM
                                ↓
                        Results back to Driver
```

**Python Workers created!** UDF execution requires Python runtime.

---

## Key Terminology

### Cluster Components

| Term | Description |
|------|-------------|
| **Cluster** | Pool of physical machines networked together |
| **Worker Node** | Individual physical machine in the cluster |
| **Cluster Manager** | Software managing cluster resources (YARN, Kubernetes) |
| **Resource Manager (RM)** | YARN component that allocates resources |
| **Container** | Isolated virtual runtime with CPU/memory allocation |

### Spark Components

| Term | Description |
|------|-------------|
| **Application Master (AM)** | Container running the Spark driver |
| **Driver** | Coordinates the application, creates execution plan |
| **PySpark Driver** | Python process in PySpark applications |
| **JVM Driver** | Java process that actually runs Spark logic |
| **Executor** | JVM process that performs data processing |
| **Executor Container** | Container running a Spark executor |
| **Python Worker** | Python runtime in executor for custom Python code |

### Communication

| Term | Description |
|------|-------------|
| **Py4J** | Library enabling Python to call Java code |
| **Serialization** | Converting data between JVM and Python formats |

---

## Architecture Decision Tree

### Which Architecture Will You Have?

```
Are you using Scala/Java?
    ├─ Yes → JVM Driver + JVM Executors
    └─ No (using PySpark)
        │
        Are you using only PySpark DataFrame API?
        ├─ Yes → PySpark Driver + JVM Driver + JVM Executors
        └─ No (using Python UDFs/libraries)
            └─ PySpark Driver + JVM Driver + JVM Executors + Python Workers
```

---

## Resource Allocation Example

### Cluster Configuration

```
Total Cluster: 10 workers × (16 cores, 64 GB) = 160 cores, 640 GB
```

### Application Request

```bash
spark-submit \
  --master yarn \
  --executor-memory 16G \
  --executor-cores 4 \
  --num-executors 4 \
  --driver-memory 16G \
  --driver-cores 4 \
  my_app.py
```

### Resource Allocation

**Driver (AM Container):**
- Cores: 4
- Memory: 16 GB
- Location: 1 worker node

**Executors (4 containers):**
- Cores per executor: 4
- Memory per executor: 16 GB
- Total cores used: 4 × 4 = 16
- Total memory used: 4 × 16 GB = 64 GB
- Distributed across worker nodes

**Total Application Resources:**
- Cores: 4 (driver) + 16 (executors) = **20 cores**
- Memory: 16 GB (driver) + 64 GB (executors) = **80 GB**

**Remaining Cluster Capacity:**
- Cores: 160 - 20 = **140 cores available**
- Memory: 640 - 80 = **560 GB available**

---

## Summary

### Key Concepts

1. **Every Spark application is distributed**
   - Driver coordinates
   - Executors perform work

2. **Containers provide isolation**
   - Fixed CPU and memory allocation
   - Cannot exceed container limits

3. **PySpark wraps Java/Scala**
   - Py4J enables communication
   - Most code runs in JVM for performance

4. **Python Workers are optional**
   - Only needed for custom Python code/UDFs
   - Adds serialization overhead

5. **Three Architecture Patterns**
   - Scala/Java: JVM only (fastest)
   - PySpark DataFrame: JVM with Python wrapper
   - PySpark UDF: JVM + Python Workers (slowest)

### Best Practices

1. **Use appropriate cluster manager** (YARN or Kubernetes)
2. **Size containers appropriately** (balance parallelism vs overhead)
3. **Prefer PySpark DataFrame API** over Python UDFs
4. **Monitor resource utilization** to avoid waste
5. **Understand your architecture** to optimize performance

### Performance Hierarchy

```
Fastest:  Scala/Java (pure JVM)
   ↓
Medium:   PySpark DataFrame API (JVM with wrapper)
   ↓
Slowest:  PySpark with Python UDFs (JVM + Python + serialization)
```

Choose your implementation based on performance needs and developer expertise!
