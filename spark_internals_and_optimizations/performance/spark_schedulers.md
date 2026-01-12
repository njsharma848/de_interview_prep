# Spark Job Execution: Sequential vs Parallel Processing

## Understanding Default Job Execution

In a typical Spark application, multiple jobs are executed as you perform actions on your data. By default, **Spark jobs run sequentially** - meaning Job 1 must complete entirely before Job 2 begins.

### Execution Architecture

```
Application (Sequential)
│
├─ Job 1
│  ├─ Stage 1 → Parallel Tasks
│  └─ Stage 2 → Parallel Tasks
│
└─ Job 2 (Sequential)
   ├─ Stage 1 → Parallel Tasks
   ├─ Stage 2 → Parallel Tasks
   └─ Stage 3 → Parallel Tasks
```

### Key Components

- **Application**: The entry point that submits jobs
- **Jobs**: Executed sequentially (Job 1 completes before Job 2 starts)
- **Stages**: Executed within each job based on dependencies
- **Tasks**: Run in parallel across the cluster within each stage

---

## Parallel Job Execution

While sequential execution is the default, Spark also supports **parallel job execution** for independent operations. This can significantly improve performance when you have multiple unrelated workloads.

### Example: Sequential Independent Operations

Consider this example with two independent sets of operations:

```python
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Parallel Jobs Example") \
        .getOrCreate()

    # First set of independent operations
    df1 = spark.read.csv("path/to/data1.csv", header=True, inferSchema=True)
    df2 = spark.read.csv("path/to/data2.csv", header=True, inferSchema=True)
    join1 = df1.join(df2, df1.id == df2.id)
    count1 = join1.count()
    print(f"Count of first join: {count1}")

    # Second set of independent operations
    df3 = spark.read.csv("path/to/data3.csv", header=True, inferSchema=True)
    df4 = spark.read.csv("path/to/data4.csv", header=True, inferSchema=True)
    join2 = df3.join(df4, df3.id == df4.id)
    count2 = join2.count()
    print(f"Count of second join: {count2}")

    spark.stop()
```

### What's Happening Here?

The application performs two completely independent workflows:

**Workflow 1:**
1. Read `df1` and `df2`
2. Join them together
3. Count the results

**Workflow 2:**
1. Read `df3` and `df4`
2. Join them together
3. Count the results

Since these workflows have **no dependencies on each other**, they're ideal candidates for parallel execution.

---

## Implementing Parallel Execution with Multithreading

To run independent jobs in parallel, we can use Python's threading module. Here's how to refactor the code:

### Step 1: Create a Reusable Job Function

```python
def do_job(file1, file2):
    df1 = spark.read.json(file1)
    df2 = spark.read.json(file2)
    outputs.append(df1.join(df2, "id", "inner").count())
```

This function encapsulates the logic for reading two files, joining them, and counting the results.

### Step 2: Configure Spark for Parallel Execution

```python
from pyspark.sql import SparkSession
import threading

def do_job(file1, file2):
    df1 = spark.read.json(file1)
    df2 = spark.read.json(file2)
    outputs.append(df1.join(df2, "id", "inner").count())

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Demo") \
        .master("local[3]") \
        .config("spark.sql.autoBroadcastJoinThreshold", "50B") \
        .config("spark.scheduler.mode", "FAIR") \
        .getOrCreate()
    
    file_prefix = "data/a"
    jobs = []
    outputs = []
    
    # Create threads for parallel execution
    for i in range(0, 2):
        file1 = file_prefix + str(i + 1)
        file2 = file_prefix + str(i + 2)
        thread = threading.Thread(target=do_job, args=(file1, file2))
        jobs.append(thread)
    
    # Start all threads
    for j in jobs:
        j.start()
    
    # Wait for all threads to complete
    for j in jobs:
        j.join()
    
    print(outputs)
```

### Key Configuration Settings

- **`spark.scheduler.mode = "FAIR"`**: Enables fair scheduling, allowing multiple jobs to run concurrently
- **`spark.sql.autoBroadcastJoinThreshold = "50B"`**: Controls broadcast join behavior (set to 50 bytes in this example)

### How the Parallel Implementation Works

1. **Loop creates threads**: The for loop runs twice, creating two separate threads
2. **Different file pairs**: Each thread receives different file names to process
3. **Thread execution**: Both threads are started simultaneously via `j.start()`
4. **Synchronization**: The main thread waits for both worker threads to complete using `j.join()`
5. **Results collection**: Each thread appends its count result to the shared `outputs` list

---

## Benefits of Parallel Execution

- **Improved resource utilization**: Multiple jobs can use cluster resources simultaneously
- **Reduced total execution time**: Independent workflows complete faster
- **Better throughput**: More work gets done in the same time period

## When to Use Parallel Execution

✅ **Use parallel execution when:**
- Jobs are completely independent with no data dependencies
- You have sufficient cluster resources to handle concurrent jobs
- The overhead of thread management is justified by the workload

❌ **Avoid parallel execution when:**
- Jobs have dependencies on each other
- Cluster resources are limited (may lead to resource contention)
- Jobs are small and quick (threading overhead isn't worth it)