# Spark Jobs and Stages - Complete Guide

Understanding how Spark executes your code through jobs, stages, and tasks.

---

## Table of Contents
- [Spark DataFrame API Classification](#spark-dataframe-api-classification)
- [Transformations vs Actions](#transformations-vs-actions)
- [Understanding Spark Jobs](#understanding-spark-jobs)
- [Code Block Example](#code-block-example)
- [Logical Query Plan](#logical-query-plan)
- [Breaking Jobs into Stages](#breaking-jobs-into-stages)
- [Shuffle/Sort Operations](#shufflesort-operations)
- [Tasks: The Smallest Unit of Work](#tasks-the-smallest-unit-of-work)
- [Cluster Execution](#cluster-execution)
- [Key Concepts Summary](#key-concepts-summary)

---

## Spark DataFrame API Classification

Most Spark APIs can be classified into two main categories:

1. **Transformations** - Process and convert data
2. **Actions** - Trigger actual computation

Some functions and objects are neither transformations nor actions, but the majority fall into these two categories.

---

## Transformations vs Actions

### Transformations

Transformations are further classified into two types:

#### 1. Narrow Dependency Transformations

Narrow transformations can run in **parallel on each data partition** without grouping data from multiple partitions.

**Examples:**
- `select()`
- `filter()`
- `withColumn()`
- `drop()`

These transformations can be independently performed on each data partition in parallel without needing to group data.

#### 2. Wide Dependency Transformations

Wide transformations require **grouping data** before they can be applied.

**Examples:**
- `groupBy()`
- `join()`
- `cube()`
- `rollup()`
- `agg()`
- `repartition()`

All these wide dependency transformations require grouping of data on some key and then applying the transformation.

### Actions

Actions trigger actual work, such as:
- Writing a DataFrame to disk
- Computing transformations
- Collecting results

**Examples:**
- `read()`
- `write()`
- `collect()`
- `take()`

**CRITICAL CONCEPT:** All Spark actions trigger one or more Spark jobs.

---

## Understanding Spark Jobs

### Rule: Each Action = One Spark Job

Every action in your Spark application creates a separate Spark job.

### Identifying Code Blocks

To identify code blocks in your Spark application:

1. Start from the first line
2. Look for actions
3. When you find an action, that block ends
4. The next line starts a new block

---

## Code Block Example

### Sample Code

```scala
// Line 1: Action (read)
val surveyRawDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data.csv")

// Line 2-6: Transformations + Action
val partitionedSurveyDF = surveyRawDF.repartition(2)

val countDF = partitionedSurveyDF
  .where("Age < 40")           // Narrow transformation
  .select("Age", "Gender")     // Narrow transformation
  .groupBy("Age", "Gender")    // Wide transformation
  .count()                     // Narrow transformation

countDF.collect()              // Action
```

### Analysis: Two Code Blocks

**Block 1 (Line 1):**
- Action: `read()`
- Creates: **Job 1**

**Block 2 (Lines 2-6):**
- Transformations: `repartition()`, `where()`, `select()`, `groupBy()`, `count()`
- Action: `collect()`
- Creates: **Job 2**

**Result:** This example has **2 Spark jobs** because it has **2 actions**.

---

## Logical Query Plan

When you execute a Spark job, the driver creates a **logical query plan**.

### Example Logical Plan

For Block 2, the logical plan looks like:

```
surveyRawDF 
    ↓
repartition(2) 
    ↓
where("Age < 40")
    ↓
select("Age", "Gender")
    ↓
groupBy("Age", "Gender")
    ↓
count()
    ↓
collect()
```

The Spark driver creates this logical query plan for each Spark job.

---

## Breaking Jobs into Stages

### How Stages Are Created

The driver breaks the logical plan into stages by identifying **wide dependency transformations**.

### Stage Boundary Rule

- Each **wide dependency** marks the end of a stage
- Stages are separated at wide dependency boundaries

### Example: Breaking Into Stages

For our example with 2 wide dependencies (`repartition()` and `groupBy()`):

**Stage 1:**
```
surveyRawDF → repartition(2)
```

**Stage 2:**
```
where("Age < 40") → select("Age", "Gender") → groupBy("Age", "Gender")
```

**Stage 3:**
```
count()
```

### Formula

If you have **N wide dependencies**, your plan will have **N + 1 stages**.

- **0 wide dependencies** = 1 stage
- **1 wide dependency** = 2 stages
- **2 wide dependencies** = 3 stages

---

## Shuffle/Sort Operations

### What Happens Between Stages?

Stages cannot run in parallel because the output of one stage is the input for the next stage.

### Data Movement: Exchange Buffers

**Stage 1 Output:**
- Data is written to a **Write Exchange Buffer**

**Stage 2 Input:**
- Data is read from a **Read Exchange Buffer**

### The Shuffle/Sort Operation

The **Shuffle/Sort** operation copies data from the Write Exchange to the Read Exchange.

```
[Stage 1] → [Write Exchange] → [Network Transfer] → [Read Exchange] → [Stage 2]
```

### Key Points About Shuffle/Sort

- Shuffle/Sort is an **expensive operation** in Spark
- Requires network transfer between worker nodes
- Write and Read exchange buffers may be on different workers
- Happens at the end of every wide dependency transformation

---

## Tasks: The Smallest Unit of Work

### What is a Task?

A **task** is the smallest unit of work in a Spark job that gets assigned to executors.

### Task Requirements

Each task needs:
1. **Task Code** - The transformations to execute
2. **Data Partition** - The data to process

### Tasks Per Stage

**Number of tasks in a stage = Number of input partitions**

### Example: Tasks in Each Stage

**Stage 1:**
- Input: 1 partition (surveyRawDF)
- Output: 2 partitions (after repartition)
- **Tasks: 1**

**Stage 2:**
- Input: 2 partitions (from Stage 1)
- **Tasks: 2** (can run in parallel)

**Stage 3:**
- Input: 2 partitions (from Stage 2)
- **Tasks: 2** (can run in parallel)

### Parallelism

If you have **100 partitions**, you can have **100 parallel tasks** for that stage.

---

## Cluster Execution

### Cluster Configuration Example

```
Driver + 4 Executors
Each Executor:
  - 1 JVM process
  - 4 CPU cores
  - 4 executor slots (parallel threads)
```

### Executor Slots

Each executor can create parallel threads equal to its CPU cores. These are called **executor slots**.

**Total Slots:** 4 executors × 4 slots = **16 slots**

```
Executor 1: [Slot 1] [Slot 2] [Slot 3] [Slot 4]
Executor 2: [Slot 1] [Slot 2] [Slot 3] [Slot 4]
Executor 3: [Slot 1] [Slot 2] [Slot 3] [Slot 4]
Executor 4: [Slot 1] [Slot 2] [Slot 3] [Slot 4]
```

### Task Assignment: Scenario 1

**Stage with 10 tasks, 16 available slots:**

```
Executor 1: [T01] [T02] [T03] [ ]
Executor 2: [T04] [T05] [T06] [ ]
Executor 3: [T07] [T08] [ ] [ ]
Executor 4: [T09] [T10] [ ] [ ]
```

- All 10 tasks assigned
- 6 slots unused (wasted capacity)

### Task Assignment: Scenario 2

**Stage with 32 tasks, 16 available slots:**

**First Wave:**
```
All 16 slots filled with tasks T1-T16
```

**Second Wave (after first wave completes):**
```
All 16 slots filled with tasks T17-T32
```

- First 16 tasks execute
- Remaining 16 tasks wait for slots to become available
- Second wave starts when first wave completes

---

## How Actions Work

### The collect() Action

When you use `collect()`:

1. Each task in the last stage sends its result back to the driver over the network
2. Driver collects data from all tasks
3. Driver presents the collected data to you

### The write() Action

When you use `write()`:

1. Each task writes its partition to a data file
2. Each task sends partition details to the driver
3. Driver considers the job done when all tasks succeed

### Task Failure Handling

**If a task fails:**
- Driver retries the task
- Task may be restarted on a different executor
- If all retries fail, driver throws an exception
- Job is marked as **failed**

---

## Key Concepts Summary

### Jobs
- **One job per action**
- Contains a series of transformations
- Spark engine optimizes and creates a logical plan

### Stages
- Created by breaking the logical plan at wide dependencies
- **N wide dependencies = N + 1 stages**
- Stages run sequentially (one after another)
- Data shared between stages via Shuffle/Sort

### Tasks
- **Smallest unit of work** in Spark
- Number of tasks = Number of input partitions
- Assigned to executor slots by the driver
- Execute the transformation code on data partitions

### Shuffle/Sort
- Expensive operation between stages
- Requires Write Exchange and Read Exchange buffers
- Data transferred over the network
- Occurs at every wide dependency boundary

---

## Visual Summary: Execution Flow

```
Spark Application
    ↓
[Actions] → Spark Jobs
    ↓
[Wide Dependencies] → Stages
    ↓
[Input Partitions] → Tasks
    ↓
[Executor Slots] → Parallel Execution
```

---

## Important Formulas

| Concept | Formula |
|---------|---------|
| **Number of Jobs** | Number of Actions |
| **Number of Stages** | Number of Wide Dependencies + 1 |
| **Number of Tasks per Stage** | Number of Input Partitions |
| **Parallel Tasks Possible** | min(Number of Tasks, Available Executor Slots) |

---

## Best Practices

1. **Minimize Wide Dependencies** - They create stage boundaries and require expensive shuffles
2. **Optimize Partitions** - Balance between parallelism and overhead
3. **Monitor Executor Slots** - Ensure you have enough slots for your tasks
4. **Avoid Excessive Actions** - Each action creates a new job
5. **Use Appropriate Partition Counts** - Match your cluster capacity

---

## Conclusion

Understanding jobs, stages, and tasks is fundamental to optimizing Spark applications:

- **Jobs** are created by actions
- **Stages** are separated by wide dependencies
- **Tasks** are the actual units of work executed in parallel
- **Shuffle/Sort** connects stages but is expensive

Master these concepts to write efficient, scalable Spark applications!
