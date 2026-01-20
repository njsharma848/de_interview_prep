# Spark Stages, Tasks, and Partitions

## Table of Contents
1. [Understanding the Spark Execution Model](#understanding-the-spark-execution-model)
2. [Stages, Tasks, and Partitions Relationship](#stages-tasks-and-partitions-relationship)
3. [How Stages Are Defined](#how-stages-are-defined)
4. [Task Execution Within Stages](#task-execution-within-stages)
5. [Partitioning Strategies](#partitioning-strategies)
6. [Default vs Column-Based Partitioning](#default-vs-column-based-partitioning)
7. [Best Practices](#best-practices)

---

## Understanding the Spark Execution Model

Apache Spark organizes work in a hierarchical structure:

```
Application
    └── Jobs
        └── Stages
            └── Tasks (smallest unit of work)
```

### Key Principle

**A stage is NOT complete until ALL tasks in that stage finish executing.**

---

## Stages, Tasks, and Partitions Relationship

### The Core Formula

```
Number of Tasks in a Stage = Number of Partitions
```

### Visual Representation

```
DataFrame with 4 Partitions:
┌───────────┬───────────┬───────────┬───────────┐
│Partition 0│Partition 1│Partition 2│Partition 3│
└───────────┴───────────┴───────────┴───────────┘

Stage 1: filter() + map() + select() operations
         (No shuffle - narrow transformations)

┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐
│ Task 0  │  │ Task 1  │  │ Task 2  │  │ Task 3  │
│ works on│  │ works on│  │ works on│  │ works on│
│Partition│  │Partition│  │Partition│  │Partition│
│    0    │  │    1    │  │    2    │  │    3    │
└─────────┘  └─────────┘  └─────────┘  └─────────┘

Result: 4 tasks running in parallel (one per partition)
```

### Key Concepts

**Stage**: A set of operations that can be executed **without shuffling data**

**Task**: Work performed on a **single partition**

**Partition**: A logical chunk of data that can be processed independently

---

## How Stages Are Defined

Stages are separated by **shuffle operations** (wide transformations).

### Stage Boundaries

```
Stage Boundaries:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Stage 1:                     Stage 2:
┌──────────────┐            ┌──────────────┐
│  filter()    │            │  groupBy()   │
│  map()       │            │  agg()       │
│  select()    │            │  sort()      │
└──────────────┘            └──────────────┘
       │                           │
       │    ◄─── SHUFFLE ───►      │
       │      (Stage Break)        │
       │                           │
  4 partitions               5 partitions
  = 4 tasks                  = 5 tasks
```

### Transformation Types

#### Narrow Transformations (Same Stage)

Operations where each output partition depends on **one** input partition:

- `filter()`
- `map()`
- `select()`
- `withColumn()`
- `union()`

**Characteristic**: No shuffle required - tasks stay in the same stage

```
Narrow Transformation Flow:
┌─────────────┐
│ Partition 0 │ ───→ │ Partition 0' │
└─────────────┘
┌─────────────┐
│ Partition 1 │ ───→ │ Partition 1' │
└─────────────┘
┌─────────────┐
│ Partition 2 │ ───→ │ Partition 2' │
└─────────────┘

Each input partition maps to ONE output partition
```

#### Wide Transformations (New Stage)

Operations where output partitions depend on **multiple** input partitions:

- `groupBy()`
- `join()`
- `repartition()`
- `orderBy()`
- `distinct()`
- `reduceByKey()`

**Characteristic**: Requires shuffle - creates a new stage

```
Wide Transformation Flow:
┌─────────────┐  ╲
│ Partition 0 │ ──╲
└─────────────┘    ╲    ┌─────────────┐
┌─────────────┐     ╲───│ Partition 0'│
│ Partition 1 │ ────╳───│ Partition 1'│
└─────────────┘     ╱───│ Partition 2'│
┌─────────────┐    ╱    └─────────────┘
│ Partition 2 │ ──╱
└─────────────┘  ╱
┌─────────────┐ ╱
│ Partition 3 │╱
└─────────────┘

Data from ALL input partitions contributes to output partitions
```

---

## Task Execution Within Stages

### Critical Understanding

**One Task = All Stage Operations on One Partition**

A task is NOT just one operation. A task applies **ALL operations in that stage** to **ONE partition**.

### Complete Example

```python
# DataFrame with 4 partitions
df = spark.read.parquet("data.parquet")  # 4 partitions

# Stage 1: Narrow transformations
df2 = df.filter(col("age") > 25) \
        .select("name", "salary") \
        .withColumn("bonus", col("salary") * 0.1)
# 4 tasks (one per partition)

# Stage 2: Wide transformation - shuffle happens here
df3 = df2.groupBy("name") \
         .agg(sum("salary"))
# Creates new stage with 200 partitions (default)
# = 200 tasks

# Stage 3: Another wide transformation
df4 = df3.orderBy("name")
# Creates another new stage
# 200 tasks
```

### Stage 1 Execution in Detail

```
Stage 1 Execution:
═══════════════════════════════════════════════════════════════

Original Data (4 Partitions):
┌────────────┬────────────┬────────────┬────────────┐
│Partition 0 │Partition 1 │Partition 2 │Partition 3 │
│[Row 1-100] │[Row 101-200]│[Row 201-300]│[Row 301-400]│
└────────────┴────────────┴────────────┴────────────┘
       ↓            ↓            ↓            ↓
┌────────────┬────────────┬────────────┬────────────┐
│  TASK 0    │  TASK 1    │  TASK 2    │  TASK 3    │
│            │            │            │            │
│ filter()   │ filter()   │ filter()   │ filter()   │
│    ↓       │    ↓       │    ↓       │    ↓       │
│ select()   │ select()   │ select()   │ select()   │
│    ↓       │    ↓       │    ↓       │    ↓       │
│withColumn()│withColumn()│withColumn()│withColumn()│
└────────────┴────────────┴────────────┴────────────┘
       ↓            ↓            ↓            ↓
┌────────────┬────────────┬────────────┬────────────┐
│Partition 0'│Partition 1'│Partition 2'│Partition 3'│
│[Processed] │[Processed] │[Processed] │[Processed] │
└────────────┴────────────┴────────────┴────────────┘

All 4 tasks run IN PARALLEL
```

### What Happens Inside Task 0

```
Task 0 Execution Pipeline:
═══════════════════════════════════════════════════════════════
Input: Partition 0 (Rows 1-100)
   ↓
Step 1: filter(age > 25)
   → Input:  100 rows
   → Output: 60 rows (40 filtered out)
   ↓
Step 2: select("name", "salary")
   → Input:  60 rows
   → Output: 60 rows (only selected columns)
   ↓
Step 3: withColumn("bonus", salary * 0.1)
   → Input:  60 rows
   → Output: 60 rows (bonus column added)
   ↓
Output: Partition 0' (60 rows with 3 columns)
```

### Why Operations Stay in Same Stage

All operations can happen **independently** on each partition:

```
Partition 0:                  Partition 1:
┌─────────────┐              ┌─────────────┐
│  Rows 1-100 │              │ Rows 101-200│
│      ↓      │              │      ↓      │
│  filter()   │  NO DATA     │  filter()   │
│      ↓      │  EXCHANGE    │      ↓      │
│  select()   │  NEEDED!     │  select()   │
│      ↓      │     ←→       │      ↓      │
│withColumn() │              │withColumn() │
└─────────────┘              └─────────────┘

Each partition processed INDEPENDENTLY
No communication between tasks
```

---

## Partitioning Strategies

### What is Partitioning?

Partitioning determines **how data is distributed** across the cluster. The partitioning strategy affects:
- Parallelism level
- Shuffle requirements
- Performance characteristics
- Data skew

### Types of Partitioning

1. **Default Partitioning** (Range/Hash based)
2. **Column-Based Partitioning** (Hash partitioning on specific columns)
3. **Custom Partitioning** (User-defined partitioners)

---

## Default vs Column-Based Partitioning

### Default Partitioning

Data is split **arbitrarily** - usually by row number, file chunks, or random hash:

```
Original DataFrame:
┌─────────────────────────────────────────────────────────────┐
│ id | name    | country | salary | dept        │
├─────────────────────────────────────────────────────────────┤
│ 1  | Alice   | USA     | 1000   | Engineering │
│ 2  | Bob     | India   | 2000   | Sales       │
│ 3  | Charlie | UK      | 1500   | Engineering │
│ 4  | David   | India   | 2500   | Sales       │
│ 5  | Eve     | USA     | 3000   | Marketing   │
│ ... (400 rows total)
└─────────────────────────────────────────────────────────────┘

Default Partitioning (4 partitions):
┌────────────┬────────────┬────────────┬────────────┐
│Partition 0 │Partition 1 │Partition 2 │Partition 3 │
├────────────┼────────────┼────────────┼────────────┤
│Rows 1-100  │Rows 101-200│Rows 201-300│Rows 301-400│
├────────────┼────────────┼────────────┼────────────┤
│Alice  USA  │David  IND  │George USA  │John   UK   │
│Bob    IND  │Eve    USA  │Helen  IND  │Kate   USA  │
│Charlie UK  │Frank  UK   │Ian    UK   │Leo    IND  │
│...         │...         │...         │...         │
└────────────┴────────────┴────────────┴────────────┘

Mixed countries in each partition (random distribution)
```

**Characteristics**:
- ✅ Balanced partition sizes (usually)
- ✅ Good for general processing
- ❌ Requires shuffle for groupBy/join operations
- ❌ No data locality guarantees

### Column-Based Partitioning

Data is split **by column value** - rows with same key go to same partition:

```python
# Partition by country column
df = df.repartition(4, "country")
```

```
Column Partitioning by "country" (4 partitions):
┌────────────┬────────────┬────────────┬────────────┐
│Partition 0 │Partition 1 │Partition 2 │Partition 3 │
├────────────┼────────────┼────────────┼────────────┤
│  USA       │  India     │  UK        │  Others    │
│  (120 rows)│  (150 rows)│  (80 rows) │  (50 rows) │
├────────────┼────────────┼────────────┼────────────┤
│Alice  USA  │Bob    IND  │Charlie UK  │Tom    CAN  │
│Eve    USA  │David  IND  │Frank   UK  │Sam    AUS  │
│George USA  │Helen  IND  │Ian     UK  │...         │
│John   USA  │Leo    IND  │...         │            │
│Kate   USA  │...         │            │            │
│...         │            │            │            │
└────────────┴────────────┴────────────┴────────────┘

All USA rows together, all India rows together, etc.
```

**Characteristics**:
- ✅ Avoids shuffle for operations on same column
- ✅ Data locality for specific keys
- ✅ Optimal for groupBy/join on partition key
- ❌ Risk of data skew
- ❌ Partition sizes may be unbalanced

---

## Impact on Stage Execution

### Scenario: GroupBy Operation

```python
# Stage 1
df2 = df.filter(col("salary") > 1500) \
        .select("country", "salary")

# Stage 2
df3 = df2.groupBy("country").agg(sum("salary"))
```

### With Default Partitioning: SHUFFLE REQUIRED ❌

```
Stage 1 Output (Default partitioning):
┌────────────┬────────────┬────────────┬────────────┐
│Partition 0 │Partition 1 │Partition 2 │Partition 3 │
│Mixed       │Mixed       │Mixed       │Mixed       │
│USA, IND, UK│USA, IND, UK│USA, IND, UK│USA, IND, UK│
└────────────┴────────────┴────────────┴────────────┘
                          ↓
          groupBy("country") - NEEDS SHUFFLE!
                   ⚡ SHUFFLE ⚡
                          ↓
Stage 2 (Data redistributed):
┌────────────┬────────────┬────────────┬────────────┐
│Partition 0 │Partition 1 │Partition 2 │Partition 3 │
│All USA     │All India   │All UK      │All Others  │
└────────────┴────────────┴────────────┴────────────┘

Network overhead! Data moves across nodes!
Time: Stage 1 (10s) + Shuffle (5s) + Stage 2 (3s) = 18s
```

### With Column Partitioning: NO SHUFFLE! ✅

```
Stage 1 Output (Partitioned by country):
┌────────────┬────────────┬────────────┬────────────┐
│Partition 0 │Partition 1 │Partition 2 │Partition 3 │
│All USA     │All India   │All UK      │All Others  │
└────────────┴────────────┴────────────┴────────────┘
                          ↓
    groupBy("country") - NO SHUFFLE NEEDED!
            (Data already grouped!)
                          ↓
Stage 2 (Same partitions - direct aggregation):
┌────────────┬────────────┬────────────┬────────────┐
│Partition 0 │Partition 1 │Partition 2 │Partition 3 │
│USA: 15000  │IND: 20000  │UK: 8000    │OTH: 3000   │
└────────────┴────────────┴────────────┴────────────┘

No network overhead! Direct aggregation!
Time: Stage 1 (10s) + Stage 2 (2s) = 12s
Savings: 6 seconds (33% faster)
```

### Complete Execution Comparison

```
Default Partitioning → groupBy:
═══════════════════════════════════════════════════════════════
Stage 1 (4 partitions):          Stage 2 (4 partitions):
┌────────────────┐               ┌────────────────┐
│ P0: Mixed      │──┐            │ P0: USA only   │
│ P1: Mixed      │──┼─SHUFFLE──→ │ P1: IND only   │
│ P2: Mixed      │──┤            │ P2: UK only    │
│ P3: Mixed      │──┘            │ P3: Others     │
└────────────────┘               └────────────────┘
Time: 10s                        Time: 5s + 3s = 8s
                                 Total: 18 seconds
                                 ↑ Shuffle overhead!


Column Partitioning → groupBy:
═══════════════════════════════════════════════════════════════
Stage 1 (4 partitions):          Stage 2 (4 partitions):
┌────────────────┐               ┌────────────────┐
│ P0: USA only   │──────────────→│ P0: USA agg    │
│ P1: IND only   │──────────────→│ P1: IND agg    │
│ P2: UK only    │──────────────→│ P2: UK agg     │
│ P3: Others     │──────────────→│ P3: Others agg │
└────────────────┘               └────────────────┘
Time: 10s                        Time: 2s
                                 Total: 12 seconds
                                 ↑ No shuffle! Local aggregation!
```

---

## Data Skew Problem

### The Challenge

Column-based partitioning can lead to **unbalanced partition sizes**:

```
Partition by "country":
┌──────────────┬──────────┬──────────┬──────────┐
│ Partition 0  │Part 1    │Part 2    │Part 3    │
│ USA          │India     │UK        │Others    │
│ (10M rows)   │(5M rows) │(100K)    │(50K)     │
├──────────────┼──────────┼──────────┼──────────┤
│ TASK 0       │TASK 1    │TASK 2    │TASK 3    │
│ [========================================] 90% │
│              │[====] 5% │[=] 2%    │[=] 3%    │
└──────────────┴──────────┴──────────┴──────────┘

Partition 0 (USA) has way more data!
Task 0 takes much longer - becomes a bottleneck!
Stage waits for this one slow task!
```

### Skew Visualization

```
Execution Timeline:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Task 0 (USA - 10M):  ████████████████████████████████████████
Task 1 (IND - 5M):   ████████████████████
Task 2 (UK - 100K):  ██
Task 3 (Others-50K): █
                     ↑
         Stage completes only when Task 0 finishes!
         Tasks 1, 2, 3 sitting idle waiting for Task 0
```

### Solutions for Data Skew

1. **Salting**: Add random prefix to skewed keys
   ```python
   df.withColumn("salt", (rand() * 10).cast("int")) \
     .withColumn("salted_country", concat(col("country"), lit("_"), col("salt"))) \
     .repartition("salted_country")
   ```

2. **Adaptive Query Execution (AQE)**: Let Spark handle it automatically
   ```python
   spark.conf.set("spark.sql.adaptive.enabled", "true")
   spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
   ```

3. **Increase partition count**: More partitions = better distribution
   ```python
   df.repartition(100, "country")  # Instead of 4
   ```

---

## Partitioning Code Examples

### Default Partitioning

```python
# Reading creates default partitions based on file blocks
df = spark.read.parquet("data.parquet")
# Number of partitions = number of file blocks

# Or specify partition count
df = spark.read.parquet("data.parquet").repartition(10)
```

### Column-Based Partitioning

```python
# Single column
df_partitioned = df.repartition(4, "country")

# Multiple columns
df_partitioned = df.repartition(8, "country", "department")
# Creates partitions like:
# P0: USA + Engineering
# P1: USA + Sales
# P2: India + Engineering
# P3: India + Sales
# etc.

# With explicit partition count
df_partitioned = df.repartition(20, "country")
```

### Checking Partition Information

```python
# Get number of partitions
num_partitions = df.rdd.getNumPartitions()
print(f"Number of partitions: {num_partitions}")

# See data distribution across partitions
df.rdd.glom().map(len).collect()
# Output: [120, 150, 80, 50] - rows per partition

# View partition distribution
df.groupBy(spark_partition_id()).count().show()
```

### Repartition vs Coalesce

```python
# repartition: Can increase or decrease partitions (involves shuffle)
df_more = df.repartition(100)   # 10 → 100 partitions (shuffle)
df_less = df.repartition(5)     # 10 → 5 partitions (shuffle)

# coalesce: Only decreases partitions (avoids full shuffle)
df_coalesced = df.coalesce(5)   # 10 → 5 partitions (optimized)
# Cannot increase: df.coalesce(100) won't work as expected
```

---

## Best Practices

### When to Use Column-Based Partitioning

#### ✅ Use Column Partitioning When:

1. **Frequent groupBy on same column**
   ```python
   # Partition once, groupBy many times without shuffle
   df = df.repartition("customer_id")
   result1 = df.groupBy("customer_id").agg(sum("amount"))
   result2 = df.groupBy("customer_id").agg(avg("rating"))
   result3 = df.groupBy("customer_id").count()
   ```

2. **Join operations on same key**
   ```python
   # Both DataFrames partitioned on same key
   df1_partitioned = df1.repartition("user_id")
   df2_partitioned = df2.repartition("user_id")
   result = df1_partitioned.join(df2_partitioned, "user_id")  # Optimized!
   ```

3. **Writing partitioned data**
   ```python
   # Match repartition with write partitioning
   df.repartition("year", "month") \
     .write \
     .partitionBy("year", "month") \
     .parquet("output/")
   ```

4. **Known low cardinality** (reasonable number of unique values)
   ```python
   # Good: country (200 unique values)
   df.repartition(200, "country")
   
   # Bad: user_id (10 million unique values)
   # Don't do: df.repartition("user_id")  # Too many partitions!
   ```

#### ❌ Avoid Column Partitioning When:

1. **High cardinality columns**
   - User IDs (millions of unique values)
   - Transaction IDs
   - Timestamps at second granularity

2. **Severe data skew**
   - One value dominates (e.g., 90% of data has same country)
   - Use salting or AQE instead

3. **One-time operations**
   - Processing data only once
   - Partitioning overhead not worth it

4. **Unknown data distribution**
   - First analyze data distribution
   - Then decide on partitioning strategy

### Partition Size Guidelines

```
Ideal Partition Size:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Target: 100 MB - 200 MB per partition (sweet spot)

Too Small (<10 MB):
❌ Too many tasks
❌ High scheduling overhead
❌ Inefficient resource utilization

Too Large (>1 GB):
❌ Memory pressure
❌ Longer task execution
❌ Risk of OOM errors
❌ Difficulty with speculative execution
```

### Calculating Optimal Partitions

```python
# Rule of thumb: 2-4 partitions per CPU core
total_cores = 100  # Your cluster
optimal_partitions = total_cores * 3  # 300 partitions

# Or based on data size
data_size_gb = 100  # 100 GB dataset
partition_size_mb = 128  # Target 128 MB per partition
optimal_partitions = (data_size_gb * 1024) / partition_size_mb  # 800 partitions

df = df.repartition(optimal_partitions)
```

### Performance Checklist

```
Before Running Your Spark Job:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
☐ Check data size and current partition count
☐ Identify shuffle operations (groupBy, join, orderBy)
☐ Consider column-based partitioning for repeated operations
☐ Verify no severe data skew
☐ Ensure partition sizes are in 100-200 MB range
☐ Use Spark UI to monitor execution plan
☐ Enable AQE for automatic optimizations
```

---

## Summary

### Key Takeaways

1. **Stages, Tasks, and Partitions**:
   - Stage = Group of operations without shuffle
   - Task = All stage operations on one partition
   - Number of tasks = Number of partitions

2. **Execution Model**:
   - Each task processes one partition independently
   - All tasks in a stage run in parallel
   - Stage completes when all tasks finish

3. **Partitioning Strategies**:
   - Default: Random/range-based distribution
   - Column-based: Hash partitioning on specific columns
   - Choice impacts shuffle requirements and performance

4. **Trade-offs**:
   - Column partitioning avoids shuffles but risks skew
   - Default partitioning is balanced but requires shuffles
   - Choose based on your specific workload

### Mental Model

```
Think of Spark Execution Like an Assembly Line:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Stage = Assembly line section (e.g., "painting section")
Task = Worker at the line (one per item)
Partition = Item being processed
Shuffle = Moving items between sections

Workers in same section work simultaneously.
Section completes when ALL workers finish their items.
Items must be reorganized when moving between certain sections.
```

---

*Keep Learning and Keep Growing!* 🚀