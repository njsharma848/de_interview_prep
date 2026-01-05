# PySpark Code to Explode Quantity into Individual Rows

## Problem Statement

**Input Table (Table-1):**

| order | prd  | quantitly |
|-------|------|-----------|
| ord1  | prd1 | 3         |
| ord2  | prd2 | 2         |
| ord3  | prd3 | 1         |

**Desired Output:**

| order | prd  | quantitly |
|-------|------|-----------|
| ord1  | prd1 | 1         |
| ord1  | prd1 | 1         |
| ord1  | prd1 | 1         |
| ord2  | prd2 | 1         |
| ord2  | prd2 | 1         |
| ord3  | prd3 | 1         |

## PySpark Code Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, array_repeat, lit

# Initialize Spark session
spark = SparkSession.builder.appName("ExplodeQuantity").getOrCreate()

# Create the input DataFrame from the table data
data = [
    ("ord1", "prd1", 3),
    ("ord2", "prd2", 2),
    ("ord3", "prd3", 1)
]
columns = ["order", "prd", "quantitly"]
df = spark.createDataFrame(data, columns)

# Explode the quantity into rows of 1
exploded_df = df.withColumn(
    "quantitly_exploded",
    explode(array_repeat(lit(1), df["quantitly"]))
).select("order", "prd", "quantitly_exploded").withColumnRenamed("quantitly_exploded", "quantitly")

# Show the output
exploded_df.show(truncate=False)
```

## Understanding the PySpark Code Logic

I'll break down the provided PySpark code step by step, explaining what each part does, why it's used, and how it contributes to the overall transformation. This code is designed to "explode" (or duplicate) rows in a DataFrame based on the value in the "quantitly" column. For each original row with a quantity value of *N*, it creates *N* duplicate rows, each with a quantity of 1, while keeping the other columns ("order" and "prd") the same.

Assume we start with an input DataFrame `df` like this (based on the example table you provided):

| order | prd  | quantitly |
|-------|------|-----------|
| ord1  | prd1 | 3         |
| ord2  | prd2 | 2         |
| ord3  | prd3 | 1         |

The goal is to transform it into:

| order | prd  | quantitly |
|-------|------|-----------|
| ord1  | prd1 | 1         |
| ord1  | prd1 | 1         |
| ord1  | prd1 | 1         |
| ord2  | prd2 | 1         |
| ord2  | prd2 | 1         |
| ord3  | prd3 | 1         |

This is useful in scenarios like inventory management, where you might want to represent each unit of quantity as a separate row for further processing (e.g., assigning individual items to shipments).

### The Full Code

```python
exploded_df = df.withColumn(
    "quantitly_exploded",
    explode(array_repeat(lit(1), df["quantitly"]))
).select("order", "prd", "quantitly_exploded").withColumnRenamed("quantitly_exploded", "quantitly")
```

This is a chained operation on the DataFrame `df`. Let's dissect it from the inside out.

### Step 1: Inner Functions – Building the Array to Explode

The core logic happens inside the `withColumn` method:

- `lit(1)`: This is from `pyspark.sql.functions.lit`. It creates a **literal column** with a constant value of 1. For every row in the DataFrame, this just outputs the number 1. It's like adding a static value that doesn't depend on the row's data.
  
- `array_repeat(lit(1), df["quantitly"])`: This uses `pyspark.sql.functions.array_repeat`. It takes two arguments:
  - The first is the element to repeat (here, `lit(1)`, so the value 1).
  - The second is the number of times to repeat it (here, `df["quantitly"]`, which pulls the value from the "quantitly" column of each row).
  
  For each row, this creates an **array** filled with 1's, repeated exactly as many times as the quantity value. Examples:
  - For the row where "quantitly" = 3 (ord1), it creates: `[1, 1, 1]`.
  - For "quantitly" = 2 (ord2): `[1, 1]`.
  - For "quantitly" = 1 (ord3): `[1]`.
  
  If "quantitly" were 0, it would create an empty array `[]` (but in your data, quantities are positive).

- `explode(...)`: This is `pyspark.sql.functions.explode`. It takes an array column and "explodes" it into multiple rows—one row per element in the array—while duplicating the other columns in the row.
  
  Applying `explode` to the array from above:
  - For ord1: The array `[1, 1, 1]` explodes into **three separate rows**, each with the value 1 from the array, and "order" = "ord1", "prd" = "prd1".
  - For ord2: Two rows with 1.
  - For ord3: One row with 1.
  
  Without `explode`, the array would just be a single column with array values (e.g., a row like {"order": "ord1", "prd": "prd1", "quantitly": 3, "some_array": [1,1,1]}). But `explode` flattens it into multiple rows.

### Step 2: Adding the New Column with `withColumn`

- `df.withColumn("quantitly_exploded", explode(array_repeat(lit(1), df["quantitly"])))`:
  - This adds a new column called "quantitly_exploded" to the DataFrame.
  - The value in this new column comes from the exploded array (so it's 1 for each new row).
  - The original DataFrame columns ("order", "prd", "quantitly") are still there, but now the DataFrame has more rows due to the explosion.
  
  Intermediate result after this step (including the original "quantitly" column):

  | order | prd  | quantitly | quantitly_exploded |
  |-------|------|-----------|--------------------|
  | ord1  | prd1 | 3         | 1                  |
  | ord1  | prd1 | 3         | 1                  |
  | ord1  | prd1 | 3         | 1                  |
  | ord2  | prd2 | 2         | 1                  |
  | ord2  | prd2 | 2         | 1                  |
  | ord3  | prd3 | 1         | 1                  |

  Notice how "quantitly" is duplicated but still shows the original value (3, 2, or 1). The explosion duplicates the entire row except for the exploded column.

### Step 3: Selecting Specific Columns with `select`

- `.select("order", "prd", "quantitly_exploded")`:
  - This keeps only the columns we want: "order", "prd", and the new "quantitly_exploded".
  - It drops the original "quantitly" column, as we no longer need it (we've replaced its logic with the exploded 1's).
  
  Intermediate result after this:

  | order | prd  | quantitly_exploded |
  |-------|------|--------------------|
  | ord1  | prd1 | 1                  |
  | ord1  | prd1 | 1                  |
  | ord1  | prd1 | 1                  |
  | ord2  | prd2 | 1                  |
  | ord2  | prd2 | 1                  |
  | ord3  | prd3 | 1                  |

### Step 4: Renaming the Column with `withColumnRenamed`

- `.withColumnRenamed("quantitly_exploded", "quantitly")`:
  - This renames "quantitly_exploded" back to "quantitly" for cleaner output (matching the original column name).
  - It's a cosmetic step but makes the final DataFrame look like the input, just with exploded rows.
  
  Final result (`exploded_df`):

  | order | prd  | quantitly |
  |-------|------|-----------|
  | ord1  | prd1 | 1         |
  | ord1  | prd1 | 1         |
  | ord1  | prd1 | 1         |
  | ord2  | prd2 | 1         |
  | ord2  | prd2 | 1         |
  | ord3  | prd3 | 1         |

### Key Concepts and Why This Works

- **Lazy Evaluation**: In Spark, operations like `withColumn`, `select`, and `explode` are transformations—they don't execute immediately. The code builds a logical plan, and computation happens only when an action (e.g., `show()`) is called.
- **Handling Nulls/Edge Cases**: If "quantitly" is null or negative, `array_repeat` might produce unexpected results (e.g., empty array for 0 or null). You may want to add filtering like `df.filter(df["quantitly"] > 0)` beforehand.
- **Performance**: This is efficient for large datasets because Spark distributes the work across clusters. `explode` can increase row count dramatically, so ensure your cluster has enough memory.
- **Imports Needed**: To run this, import from `pyspark.sql.functions`: `explode`, `array_repeat`, `lit`.
- **Alternatives**: You could use a UDF (User-Defined Function) for custom logic, but this built-in approach is optimized and avoids UDF overhead.

If you run this with `exploded_df.show()`, you'll see the output as above. Let me know if you have questions about any part or want to modify it!
