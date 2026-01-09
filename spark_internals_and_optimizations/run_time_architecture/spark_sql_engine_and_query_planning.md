# Spark SQL Engine and Query Planning

Understanding how Spark processes your queries through the SQL Engine's four-stage optimization pipeline.

---

## Table of Contents
- [Spark Interfaces Overview](#spark-interfaces-overview)
- [From Code to Jobs](#from-code-to-jobs)
- [The Spark SQL Engine](#the-spark-sql-engine)
- [Four Stages of Query Planning](#four-stages-of-query-planning)
- [Complete Example with Code](#complete-example-with-code)
- [Key Takeaways](#key-takeaways)

---

## Spark Interfaces Overview

Apache Spark provides two prominent interfaces to work with data:

### 1. Spark SQL
- **ANSI SQL:2003 compliant**
- Standard SQL syntax
- Familiar to database developers

### 2. DataFrame API
- **Functional programming interface**
- Available in Python, Scala, Java, and R
- Programmatic data manipulation

### 3. Dataset API (Scala/Java Only)
- Type-safe version of DataFrames
- DataFrame API internally uses Dataset APIs
- **Not available in PySpark**
- **Spark recommends using DataFrames instead**

### Important Note for Certification
- Certification available in Scala and Python
- Questions avoid features that differ between languages
- Datasets are not tested (not in PySpark)
- **Safe to ignore Datasets** for most purposes

---

## From Code to Jobs

### How Spark Views Your Code

Whether you write **DataFrame API code** or **SQL expressions**, Spark converts them into the same internal representation.

```
DataFrame API Code  ─┐
                     ├─→ Spark Jobs → Logical Query Plan
SQL Expression      ─┘
```

### Code to Job Mapping

**DataFrame API:**
- One action + preceding transformations = **One Spark Job**

**SQL Expression:**
- One SQL statement = **One Spark Job**

### Key Concept

```
Spark Application = Sequence of Spark Jobs
Each Spark Job = Logical Query Plan
```

Both DataFrame APIs and SQL expressions are treated equally by the Spark SQL Engine!

---

## The Spark SQL Engine

### Unified Processing

```
┌─────────────────┐
│ DataFrame APIs  │────┐
└─────────────────┘    │
                       ├──→ ┌──────────────────┐
┌─────────────────┐    │    │ Spark SQL Engine │
│ SQL Expressions │────┘    └──────────────────┘
└─────────────────┘              │
                                 ↓
                         Optimized Execution
```

**Key Point:** Whether you use DataFrames or SQL, both go through the same Spark SQL Engine for optimization.

---

## Four Stages of Query Planning

The Spark SQL Engine processes your logical plan through **four distinct stages**:

```
User Code (DataFrame/SQL)
    ↓
1. Analysis
    ↓
2. Logical Optimization
    ↓
3. Physical Planning
    ↓
4. Code Generation
    ↓
Executable Java Bytecode
```

---

### Stage 1: Analysis

#### Purpose
Parse code for errors and resolve all references.

#### What Happens
1. **Parse the code** for syntax errors
2. **Resolve column names** using the Catalog
3. **Resolve data types** for all columns
4. **Apply implicit type casting** where needed
5. **Validate function names**

#### Example 1: Column Resolution

**Input Code:**
```sql
SELECT age FROM employees
```

**Analysis Process:**
- Check if `employees` table exists in Catalog
- Check if `age` column exists in `employees` table
- Resolve data type of `age` (e.g., Integer)
- Create resolved logical plan

#### Example 2: Implicit Type Casting

**Input Code:**
```sql
SELECT product_qty + 5 FROM products
```

**Analysis Process:**
- Resolve `product_qty` data type (e.g., String)
- Detect type mismatch (String + Integer)
- Apply implicit type casting: `CAST(product_qty AS INT) + 5`
- Validate the operation is valid

#### Possible Errors at This Stage

**AnalysisException Examples:**
```scala
// Column doesn't exist
df.select("non_existent_column")
// Error: cannot resolve 'non_existent_column'

// Table doesn't exist
spark.sql("SELECT * FROM fake_table")
// Error: Table or view not found: fake_table

// Invalid function
df.select(invalidFunction($"column"))
// Error: Undefined function: 'invalidFunction'

// Type mismatch
df.filter($"age" > "not_a_number")
// Error: Cannot resolve type mismatch
```

#### Output
**Resolved Logical Plan** with all names and types validated

---

### Stage 2: Logical Optimization

#### Purpose
Apply standard **rule-based optimizations** to improve the logical plan.

#### What Happens
The Catalyst optimizer applies numerous optimization rules to create a more efficient logical plan.

#### Common Logical Optimizations

| Optimization | Description | Example |
|--------------|-------------|---------|
| **Predicate Pushdown** | Move filters closer to data source | Push WHERE clause into file scan |
| **Projection Pruning** | Select only needed columns early | Read only required columns from Parquet |
| **Constant Folding** | Evaluate constant expressions | `WHERE 1 + 1 = 2` → `WHERE TRUE` |
| **Null Propagation** | Simplify NULL expressions | `NULL AND condition` → `NULL` |
| **Boolean Simplification** | Simplify boolean logic | `WHERE TRUE AND condition` → `WHERE condition` |
| **Combine Filters** | Merge multiple filters | `filter(a).filter(b)` → `filter(a AND b)` |
| **Combine Limits** | Merge multiple limits | `limit(10).limit(5)` → `limit(5)` |

#### Example: Predicate Pushdown

**Original Logical Plan:**
```
Scan Parquet (read all columns, all rows)
    ↓
Filter (age > 25)
    ↓
Select (name, age)
```

**Optimized Logical Plan:**
```
Scan Parquet (read only name, age columns WHERE age > 25)
```

**Benefit:** Read less data from disk!

#### Example: Constant Folding

**Original Code:**
```scala
df.filter($"price" > 100 * 2 + 50)
```

**Optimized:**
```scala
df.filter($"price" > 250)  // Computed at compile time
```

#### Important Notes
- This is **rule-based** optimization (not cost-based)
- Rules are predefined and evolving
- List of optimizations is extensive and keeps growing
- **You don't need to memorize all optimizations for the exam**

#### Output
**Optimized Logical Plan** ready for physical planning

---

### Stage 3: Physical Planning

#### Purpose
Generate and select the best **physical execution plan** using **cost-based optimization**.

#### What Happens
1. **Generate multiple physical plans** from the optimized logical plan
2. **Calculate cost** for each plan
3. **Select the plan with lowest cost**

#### Cost-Based Optimization

Unlike logical optimization (rule-based), physical planning uses **statistics** and **cost estimation**.

#### Primary Focus: Join Strategies

The physical planner creates different plans mainly by trying different **join algorithms**:

| Join Strategy | When Used | Cost Factor |
|--------------|-----------|-------------|
| **Broadcast Hash Join** | Small table × Large table | Small table fits in memory |
| **Shuffle Hash Join** | Medium × Medium tables | Both tables hash-partitioned |
| **Sort Merge Join** | Large × Large tables | Both tables sorted |

#### Example: Join Strategy Selection

**Query:**
```sql
SELECT * 
FROM large_customers c
JOIN small_products p ON c.product_id = p.id
```

**Physical Plans Generated:**

**Plan 1: Broadcast Hash Join**
```
Cost Estimate: Low
- Broadcast small_products to all executors
- Hash join on each partition
- No shuffle of large_customers needed
```

**Plan 2: Sort Merge Join**
```
Cost Estimate: High
- Shuffle both tables by join key
- Sort both sides
- Merge sorted partitions
- More expensive for this case
```

**Plan 3: Shuffle Hash Join**
```
Cost Estimate: Medium
- Shuffle both tables by join key
- Build hash table for one side
- Probe with other side
```

**Selected Plan:** Plan 1 (Broadcast Hash Join) - Lowest cost!

#### Cost Factors Considered
- Table sizes (from statistics)
- Number of partitions
- Available memory
- Network bandwidth
- Data distribution

#### Configuration Impact

```scala
// Control broadcast join threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760") // 10MB

// Enable/disable cost-based optimization
spark.conf.set("spark.sql.cbo.enabled", "true")

// Enable statistics collection
spark.sql("ANALYZE TABLE employees COMPUTE STATISTICS")
```

#### Output
**Selected Physical Plan** with optimal join strategies and execution approach

---

### Stage 4: Code Generation

#### Purpose
Generate optimized **Java bytecode** for RDD operations.

#### What Happens
1. Take the selected physical plan
2. Generate Java bytecode using **Tungsten engine**
3. Compile to executable code
4. Optimize for CPU efficiency

#### Why Code Generation?

**Spark acts as a compiler** using state-of-the-art compiler technology:

- **Whole-stage code generation**: Fuse operators together
- **Expression compilation**: Generate specialized code for expressions
- **Vectorized execution**: Process multiple rows at once
- **No virtual function calls**: Direct method invocation

#### Example: Before and After Code Generation

**Logical Operations:**
```
Filter (age > 25)
    ↓
Map (name.toUpperCase)
    ↓
Filter (name.startsWith("A"))
```

**Without Code Generation (Interpreted):**
```java
// Virtual function calls for each row
for (row in partition) {
    if (row.getInt("age") > 25) {
        String name = row.getString("name");
        String upper = name.toUpperCase();
        if (upper.startsWith("A")) {
            output.add(upper);
        }
    }
}
```

**With Code Generation (Compiled):**
```java
// Fused into single loop, no virtual calls
for (row in partition) {
    int age = row.getInt(0);  // Direct access
    if (age > 25) {
        String name = row.getUTF8String(1);  // Direct access
        if (name.getByte(0) == 'A') {  // Optimized comparison
            output.add(name.toUpperCase());
        }
    }
}
```

**Performance Gain:** 2-10x faster execution!

#### Output
**Executable Java Bytecode** ready to run on executors

---

## Complete Example with Code

Let's trace a complete example through all four stages.

### Example Application

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CustomerAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Customer Analysis")
      .getOrCreate()
    
    // Read customer data
    val customersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .parquet("customers.parquet")
    
    // Read orders data
    val ordersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .parquet("orders.parquet")
    
    // Complex query with multiple optimizations
    val result = customersDF
      .filter($"age" > 18)                    // Filter 1
      .filter($"country" === "USA")           // Filter 2
      .select($"customer_id", $"name", $"age") // Projection
      .join(ordersDF, "customer_id")          // Join
      .filter($"order_amount" > 100 + 50)     // Filter with constant
      .groupBy($"name")                       // Aggregation
      .agg(
        sum($"order_amount").alias("total_spent"),
        count("*").alias("order_count")
      )
      .orderBy($"total_spent".desc)
      .limit(10)
    
    // ACTION: Triggers the query execution
    result.write
      .mode("overwrite")
      .parquet("output/top_customers")
    
    spark.stop()
  }
}
```

### Stage 1: Analysis

**Input:** DataFrame operations above

**Process:**
1. Resolve table names: `customers.parquet`, `orders.parquet`
2. Resolve column names: `age`, `country`, `customer_id`, `name`, `order_amount`
3. Validate data types for all operations
4. Check join key compatibility: `customer_id` exists in both tables
5. Validate aggregation functions: `sum()`, `count()`

**Output:** Resolved Logical Plan
```
Join (customer_id)
├─ Project [customer_id#1, name#2, age#3]
│  └─ Filter (age#3 > 18 AND country#4 = "USA")
│     └─ Scan parquet [customer_id#1, name#2, age#3, country#4]
└─ Filter (order_amount#5 > 150)
   └─ Scan parquet [customer_id#6, order_amount#5]
```

### Stage 2: Logical Optimization

**Optimizations Applied:**

1. **Combine Filters:**
   ```
   filter(age > 18).filter(country === "USA")
   →
   filter(age > 18 AND country === "USA")
   ```

2. **Constant Folding:**
   ```
   filter(order_amount > 100 + 50)
   →
   filter(order_amount > 150)
   ```

3. **Predicate Pushdown:**
   ```
   Scan all → Filter
   →
   Scan with filter pushed down (read less data)
   ```

4. **Projection Pruning:**
   ```
   Scan all columns from customers
   →
   Scan only [customer_id, name, age, country]
   ```

**Output:** Optimized Logical Plan
```
Limit (10)
└─ Sort [total_spent DESC]
   └─ Aggregate [name], [sum(order_amount) AS total_spent, count(*) AS order_count]
      └─ Join [customer_id]
         ├─ Project [customer_id, name, age]
         │  └─ Scan parquet [customer_id, name, age, country] 
         │     WHERE age > 18 AND country = "USA"
         └─ Scan parquet [customer_id, order_amount]
            WHERE order_amount > 150
```

### Stage 3: Physical Planning

**Plans Generated:**

**Plan 1: Broadcast Hash Join** (if ordersDF is small)
```
Cost: 1000 units
- Broadcast ordersDF to all executors
- Hash join on customer_id
- No shuffle needed for customersDF
```

**Plan 2: Sort Merge Join**
```
Cost: 5000 units
- Shuffle both tables by customer_id
- Sort both sides
- Merge join
```

**Plan 3: Shuffle Hash Join**
```
Cost: 3000 units
- Shuffle both tables by customer_id
- Build hash table for customersDF
- Probe with ordersDF
```

**Selected Plan:** Plan 1 (Broadcast Hash Join) - Lowest cost

**Final Physical Plan:**
```
Limit (10)
└─ TakeOrderedAndProject [total_spent DESC]
   └─ HashAggregate [name], [sum, count]
      └─ Exchange hashpartitioning(name)
         └─ HashAggregate [name], [partial_sum, partial_count]
            └─ BroadcastHashJoin [customer_id]
               ├─ Project [customer_id, name, age]
               │  └─ FileScan parquet [...] WHERE (age > 18) AND (country = USA)
               └─ BroadcastExchange
                  └─ FileScan parquet [...] WHERE (order_amount > 150)
```

### Stage 4: Code Generation

**Generated Code Structure:**

```java
// Whole-stage code generation - fused pipeline
public class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {
  
  // Scan and filter customers
  private void scanCustomers() {
    while (input.hasNext()) {
      InternalRow row = input.next();
      int age = row.getInt(2);
      UTF8String country = row.getUTF8String(3);
      
      // Fused filters - no intermediate materialization
      if (age > 18 && country.equals(UTF8String.fromString("USA"))) {
        // Fused projection - direct field access
        int customerId = row.getInt(0);
        UTF8String name = row.getUTF8String(1);
        
        // Join probe - direct hash lookup
        if (hashMap.contains(customerId)) {
          InternalRow orderRow = hashMap.get(customerId);
          int orderAmount = orderRow.getInt(1);
          
          // Fused filter on order amount
          if (orderAmount > 150) {
            // Emit for aggregation
            emitRow(name, orderAmount);
          }
        }
      }
    }
  }
  
  // Aggregation logic
  private void aggregate() {
    // Optimized aggregation with vectorized operations
    // ...
  }
}
```

**Optimizations in Generated Code:**
- No virtual function calls
- Direct field access by position
- Fused operations in single loop
- Vectorized aggregations
- Specialized code for specific data types

### Execution Flow Summary

```
User Code (DataFrame API)
    ↓
[Stage 1: Analysis]
    ↓
Resolved Logical Plan (validated)
    ↓
[Stage 2: Logical Optimization]
    ↓
Optimized Logical Plan (rule-based)
    ↓
[Stage 3: Physical Planning]
    ↓
Best Physical Plan (cost-based, Broadcast Hash Join selected)
    ↓
[Stage 4: Code Generation]
    ↓
Compiled Java Bytecode (whole-stage codegen)
    ↓
Execution on Executors
```

---

## SQL Example Through All Stages

### SQL Query

```sql
SELECT 
    c.name,
    SUM(o.order_amount) as total_spent,
    COUNT(*) as order_count
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE c.age > 18
  AND c.country = 'USA'
  AND o.order_amount > 150
GROUP BY c.name
ORDER BY total_spent DESC
LIMIT 10
```

### Same Four Stages Apply!

**Stage 1 (Analysis):** Resolve tables, columns, validate syntax
**Stage 2 (Logical Opt):** Pushdown predicates, prune columns
**Stage 3 (Physical):** Choose join strategy (e.g., broadcast)
**Stage 4 (Code Gen):** Generate optimized bytecode

**Result:** Identical execution plan as DataFrame API version!

---

## Python (PySpark) Example

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Customer Analysis").getOrCreate()

# Read data
customers_df = spark.read.parquet("customers.parquet")
orders_df = spark.read.parquet("orders.parquet")

# Same query as Scala
result = customers_df \
    .filter(col("age") > 18) \
    .filter(col("country") == "USA") \
    .select("customer_id", "name", "age") \
    .join(orders_df, "customer_id") \
    .filter(col("order_amount") > 150) \
    .groupBy("name") \
    .agg(
        sum("order_amount").alias("total_spent"),
        count("*").alias("order_count")
    ) \
    .orderBy(col("total_spent").desc()) \
    .limit(10)

# Trigger execution
result.write.mode("overwrite").parquet("output/top_customers")
```

**Goes through same four stages!** Python, Scala, SQL - all optimized equally.

---

## Viewing Query Plans

### Using explain()

```scala
// Show the physical plan
result.explain()

// Show all stages of query planning
result.explain(true)  // Scala
result.explain("extended")  // PySpark

// Show formatted plan
result.explain("formatted")

// Show cost information
result.explain("cost")
```

### Example Output

```
== Parsed Logical Plan ==
[User's original logical plan]

== Analyzed Logical Plan ==
[After Stage 1: Analysis]

== Optimized Logical Plan ==
[After Stage 2: Logical Optimization]

== Physical Plan ==
[After Stage 3: Physical Planning + Stage 4: Code Generation]
```

---

## Key Takeaways

### Essential Concepts

1. **Both DataFrames and SQL go through the same Spark SQL Engine**
   - No performance difference
   - Use whichever is more readable

2. **Four-Stage Process:**
   - Analysis: Validate and resolve
   - Logical Optimization: Rule-based improvements
   - Physical Planning: Cost-based strategy selection
   - Code Generation: Compile to optimized bytecode

3. **Catalyst Optimizer** handles stages 1-3
   - Makes your queries faster automatically
   - You don't need to manually optimize

4. **Tungsten Engine** handles stage 4
   - Generates efficient bytecode
   - Spark acts as a compiler

5. **Cost-Based Optimization** in physical planning
   - Mainly for join strategies
   - Requires table statistics for best results

### For Certification Exam

- **Understand the four stages** and their purpose
- **Know that DataFrame API = SQL** in terms of execution
- **Recognize optimization examples** (predicate pushdown, constant folding)
- **Don't memorize all optimization rules** (list is long and evolving)
- **Understand join strategies** and when each is used
- Dataset questions unlikely (not in PySpark)

### Performance Tips

1. **Collect statistics** for cost-based optimization:
   ```sql
   ANALYZE TABLE customers COMPUTE STATISTICS
   ```

2. **Use appropriate file formats** (Parquet, ORC) for predicate pushdown

3. **Partition data** strategically for filter pushdown

4. **Let Spark optimize** - trust the Catalyst optimizer

5. **Check explain()** to understand execution plans

---

## Summary

The Spark SQL Engine transforms your high-level code (DataFrame API or SQL) into highly optimized executable bytecode through a sophisticated four-stage process:

1. **Analysis** - Validates your code
2. **Logical Optimization** - Applies rule-based improvements  
3. **Physical Planning** - Selects best execution strategy
4. **Code Generation** - Compiles to fast bytecode

This is why Spark is fast and why you should trust the optimizer rather than trying to manually optimize your queries!