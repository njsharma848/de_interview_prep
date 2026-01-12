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

## Resource Allocation and Job Scheduling

When running Spark jobs in parallel, **resource management becomes critical**. You need to understand how Spark allocates executor slots to these concurrent jobs.

### The Resource Challenge

Each Spark job runs as a set of parallel tasks, and each task requires executor resources. When you create a multithreaded Spark application that submits multiple parallel jobs, several challenges emerge:

- **Resource competition**: All jobs need resources for their corresponding tasks simultaneously
- **Task scheduling**: Jobs compete to acquire available executor slots
- **Performance impact**: Poor scheduling can lead to resource starvation and delays

This is where **scheduling within an application** becomes important.

### When Scheduling Matters

- **Single-threaded applications**: No need to worry about internal scheduling - jobs run sequentially by default
- **Multithreaded applications**: Job scheduling is critical when submitting jobs from multiple parallel threads

---

## Understanding Spark Schedulers

Spark provides two scheduling modes to handle resource allocation between parallel jobs:

### 1. FIFO Scheduler (Default)

By default, Spark's job scheduler runs jobs in a **First-In-First-Out (FIFO)** fashion.

#### How FIFO Works

```
Job Queue (FIFO):
┌─────────┐
│  Job 1  │ ← Gets priority, consumes all needed resources
├─────────┤
│  Job 2  │ ← Waits for Job 1 to release resources
├─────────┤
│  Job 3  │ ← Waits for Job 2
└─────────┘
```

**Characteristics:**
- Each job is divided into stages
- The first job gets priority on **all available resources**
- Later jobs can only start if earlier jobs don't need all resources
- Large jobs at the queue head can significantly delay subsequent jobs

**Resource Allocation:**
- Job 1 consumes as many resources as it needs
- Job 2 gets priority next, but only after Job 1 releases resources
- If Job 1 doesn't use all resources, Job 2 can start running

⚠️ **Limitation**: If the jobs at the head of the queue are large, later jobs may experience significant delays.

---

### 2. FAIR Scheduler (Recommended for Parallel Jobs)

The FAIR scheduler distributes resources more equitably among concurrent jobs.

#### How FAIR Scheduling Works

```
Task Distribution (Round-Robin):
┌─────────────────────────────────────┐
│  Slot 1: Job 1 Task                 │
│  Slot 2: Job 2 Task                 │
│  Slot 3: Job 1 Task                 │
│  Slot 4: Job 2 Task                 │
│  ...                                │
└─────────────────────────────────────┘
```

**Characteristics:**
- Tasks are assigned in a **round-robin fashion**
- Each job gets a roughly equal share of cluster resources
- No job waits indefinitely for resources
- Better support for concurrent workloads

**Task Assignment Pattern:**
1. Assign one task slot to Job 1
2. Assign one task slot to Job 2
3. Assign next task slot to Job 1
4. Assign next task slot to Job 2
5. Continue in round-robin fashion

✅ **Benefit**: All parallel jobs make progress simultaneously without resource starvation.

---

## Configuring the FAIR Scheduler

To enable FAIR scheduling, add the configuration when creating your SparkSession:

```python
spark = SparkSession \
    .builder \
    .appName("Demo") \
    .master("local[3]") \
    .config("spark.scheduler.mode", "FAIR") \
    .getOrCreate()
```

### Monitoring Scheduler Pools in Spark UI

Once you enable the FAIR scheduler, you can monitor it through the Spark UI:

1. Navigate to your Spark application UI
2. Look for the **Fair Scheduler Pools** section
3. You'll see the default pool configuration

**Example view:**
```
Fair Scheduler Pools:
├─ default (FAIR)
   └─ Within pool: FIFO scheduling
```

### Advanced Configuration: Multiple Pools

While possible, creating multiple pools within the FAIR scheduler is typically unnecessary for most use cases. The **FAIR scheduler with the default pool** works well enough for most parallel job scenarios.

You can configure multiple pools if you need more granular control:
- Different pools for different priority levels
- Separate pools for different job types
- Custom resource allocation per pool

However, for most applications, the single default pool with FAIR scheduling provides sufficient resource management.

---

## Best Practices for Parallel Job Execution

### ✅ Do's

- **Use FAIR scheduler** for multithreaded applications
- **Monitor resource usage** via Spark UI
- **Test with representative workloads** to understand resource requirements
- **Size your cluster appropriately** for concurrent job loads

### ❌ Don'ts

- **Don't use FIFO** for highly parallel workloads
- **Don't over-thread** beyond available resources
- **Don't assume** all jobs will get equal performance
- **Don't ignore** Spark UI metrics and warnings

---

## Summary

**Job Scheduling in Apache Spark:**

| Aspect | FIFO Scheduler | FAIR Scheduler |
|--------|---------------|----------------|
| **Default** | Yes | No (must configure) |
| **Resource Allocation** | First job gets priority | Round-robin distribution |
| **Best For** | Sequential jobs | Parallel jobs |
| **Configuration** | No config needed | `spark.scheduler.mode = "FAIR"` |
| **Risk** | Job starvation possible | Balanced resource sharing |

**Key Takeaway**: For multithreaded Spark applications with parallel jobs, always use the FAIR scheduler to ensure equitable resource distribution and prevent job starvation.

---

Keep learning and keep growing! 🚀