# Split and Explode Comma-Separated Values - Complete Guide

## Problem Statement

Transform comma-separated product lists into individual rows, where each product gets its own row while maintaining the customer relationship.

## Sample Data

### Input Table

| customer_id | products                    |
|-------------|-----------------------------|
| C001        | Apple, Banana, Orange       |
| C002        | Laptop, Mouse               |
| C003        | Book                        |
| C004        | Pen, Pencil, Eraser, Ruler  |

### Expected Output

| customer_id | product |
|-------------|---------|
| C001        | Apple   |
| C001        | Banana  |
| C001        | Orange  |
| C002        | Laptop  |
| C002        | Mouse   |
| C003        | Book    |
| C004        | Pen     |
| C004        | Pencil  |
| C004        | Eraser  |
| C004        | Ruler   |

---

## SQL Solution

### Complete SQL Code

```sql
SELECT customer_id, explode(split(products, ', ')) AS product
FROM your_table_name;
```

### Step-by-Step SQL Explanation

#### Step 1: Start with the Original Table

```sql
SELECT customer_id, products
FROM your_table_name;
```

**What happens**: This selects all columns from the table.

**Output after this step**:

| customer_id | products                    |
|-------------|-----------------------------|
| C001        | Apple, Banana, Orange       |
| C002        | Laptop, Mouse               |
| C003        | Book                        |
| C004        | Pen, Pencil, Eraser, Ruler  |

---

#### Step 2: Apply `split()` Function

```sql
SELECT customer_id, split(products, ', ') AS product_array
FROM your_table_name;
```

**What happens**: 
- `split(products, ', ')` splits the string at each occurrence of `', '` (comma followed by space)
- Creates an array of individual products
- The delimiter `', '` must match exactly what's in your data

**Output after this step**:

| customer_id | product_array                      |
|-------------|------------------------------------|
| C001        | ["Apple", "Banana", "Orange"]      |
| C002        | ["Laptop", "Mouse"]                |
| C003        | ["Book"]                           |
| C004        | ["Pen", "Pencil", "Eraser", "Ruler"] |

---

#### Step 3: Apply `explode()` Function

```sql
SELECT customer_id, explode(split(products, ', ')) AS product
FROM your_table_name;
```

**What happens**: 
- `explode()` takes each array and creates a separate row for each element
- The `customer_id` is duplicated for each product
- Single products (like "Book") still create one row

**Final Output**:

| customer_id | product |
|-------------|---------|
| C001        | Apple   |
| C001        | Banana  |
| C001        | Orange  |
| C002        | Laptop  |
| C002        | Mouse   |
| C003        | Book    |
| C004        | Pen     |
| C004        | Pencil  |
| C004        | Eraser  |
| C004        | Ruler   |

---

## PySpark Solution

### Complete PySpark Code

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Initialize Spark session
spark = SparkSession.builder.appName("SplitExplode").getOrCreate()

# Sample data
data = [
    ("C001", "Apple, Banana, Orange"),
    ("C002", "Laptop, Mouse"),
    ("C003", "Book"),
    ("C004", "Pen, Pencil, Eraser, Ruler")
]

# Create DataFrame
df = spark.createDataFrame(data, ["customer_id", "products"])

# Apply split and explode
exploded_df = df.select("customer_id", explode(split("products", ", ")).alias("product"))

# Show result
exploded_df.show()
```

### Step-by-Step PySpark Explanation

#### Step 1: Import Required Libraries

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
```

**What happens**: 
- Imports `SparkSession` to create the Spark environment
- Imports `explode` and `split` functions from PySpark SQL functions
- No data changes yet

**Data state**: No data exists yet

---

#### Step 2: Initialize Spark Session

```python
spark = SparkSession.builder.appName("SplitExplode").getOrCreate()
```

**What happens**: 
- Creates or retrieves a Spark session
- `appName` sets the application name (visible in Spark UI)
- `getOrCreate()` ensures only one session exists

**Data state**: Spark environment ready, no data yet

---

#### Step 3: Define Sample Data

```python
data = [
    ("C001", "Apple, Banana, Orange"),
    ("C002", "Laptop, Mouse"),
    ("C003", "Book"),
    ("C004", "Pen, Pencil, Eraser, Ruler")
]
```

**What happens**: 
- Creates a list of tuples representing rows
- Each tuple contains (customer_id, products)
- Data is in Python memory, not yet a Spark DataFrame

**Data state**: Raw data in list form

---

#### Step 4: Create DataFrame

```python
df = spark.createDataFrame(data, ["customer_id", "products"])
```

**What happens**: 
- Converts the list into a Spark DataFrame
- Defines column names: "customer_id" and "products"
- Data is now distributed (though small here, scalable for big data)

**Output after `df.show()`**:

| customer_id | products                    |
|-------------|-----------------------------|
| C001        | Apple, Banana, Orange       |
| C002        | Laptop, Mouse               |
| C003        | Book                        |
| C004        | Pen, Pencil, Eraser, Ruler  |

---

#### Step 5: Apply Split (Intermediate Step - Not Shown in Code but Conceptual)

If we were to run:
```python
df.select("customer_id", split("products", ", ")).show()
```

**What happens**: 
- `split("products", ", ")` splits the products string by `", "` delimiter
- Creates an array column containing individual products

**Output would be**:

| customer_id | split(products, , )                |
|-------------|------------------------------------|
| C001        | [Apple, Banana, Orange]            |
| C002        | [Laptop, Mouse]                    |
| C003        | [Book]                             |
| C004        | [Pen, Pencil, Eraser, Ruler]       |

---

#### Step 6: Apply Split and Explode Together

```python
exploded_df = df.select("customer_id", explode(split("products", ", ")).alias("product"))
```

**What happens**: 
- `split("products", ", ")` splits the string into an array (as above)
- `explode()` takes the array and creates a new row for each element
- `.alias("product")` names the exploded column as "product"
- `df.select()` keeps only the specified columns

**Why this works**: 
- The `split()` creates arrays: `["Apple", "Banana", "Orange"]`
- The `explode()` transforms them into separate rows
- Customer C001 goes from 1 row with 3 products to 3 rows with 1 product each

**Output after this step (`exploded_df` content)**:

| customer_id | product |
|-------------|---------|
| C001        | Apple   |
| C001        | Banana  |
| C001        | Orange  |
| C002        | Laptop  |
| C002        | Mouse   |
| C003        | Book    |
| C004        | Pen     |
| C004        | Pencil  |
| C004        | Eraser  |
| C004        | Ruler   |

---

#### Step 7: Display Results

```python
exploded_df.show()
```

**What happens**: 
- Triggers the computation (PySpark is lazy)
- Displays the DataFrame in console/notebook

**Final Output**:

```
+-----------+-------+
|customer_id|product|
+-----------+-------+
|       C001|  Apple|
|       C001| Banana|
|       C001| Orange|
|       C002| Laptop|
|       C002|  Mouse|
|       C003|   Book|
|       C004|    Pen|
|       C004| Pencil|
|       C004| Eraser|
|       C004|  Ruler|
+-----------+-------+
```

---

## Key Concepts

### Understanding `split()`
- **Purpose**: Converts a delimited string into an array
- **Syntax**: `split(column, delimiter)`
- **Example**: `"A, B, C"` with delimiter `", "` becomes `["A", "B", "C"]`
- **Important**: Delimiter must match exactly (watch for spaces!)

### Understanding `explode()`
- **Purpose**: Creates a new row for each element in an array
- **Behavior**: 
  - Original columns are duplicated for each new row
  - Empty arrays produce no rows
  - NULL arrays produce no rows
- **Result**: Transforms wide data (arrays) into long data (rows)

### Common Pitfalls

1. **Wrong Delimiter**: If data has `"Apple,Banana"` (no space), use `","` not `", "`
2. **Extra Spaces**: Use `trim()` if products have inconsistent spacing
3. **NULL Handling**: `explode()` drops NULLs; use `explode_outer()` to keep them
4. **Column Selection**: Remember to include all columns you need in the final output

### Alternative: Using `explode_outer()`

If you want to preserve rows with NULL or empty products:

```python
exploded_df = df.select("customer_id", explode_outer(split("products", ", ")).alias("product"))
```

This would keep customers with no products as a row with NULL in the product column.

---

## `explode_outer()` Detailed Example

### Difference Between `explode()` and `explode_outer()`

**Key Difference**: 
- `explode()` removes rows with NULL or empty arrays
- `explode_outer()` preserves rows with NULL or empty arrays by creating a row with NULL value

### Sample Data with NULLs

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, explode_outer, split

# Initialize Spark session
spark = SparkSession.builder.appName("ExplodeOuter").getOrCreate()

# Sample data with NULL and empty string
data = [
    ("C001", "Apple, Banana, Orange"),
    ("C002", "Laptop, Mouse"),
    ("C003", None),  # NULL value
    ("C004", ""),    # Empty string
    ("C005", "Book")
]

# Create DataFrame
df = spark.createDataFrame(data, ["customer_id", "products"])
```

**Initial DataFrame**:

| customer_id | products              |
|-------------|-----------------------|
| C001        | Apple, Banana, Orange |
| C002        | Laptop, Mouse         |
| C003        | null                  |
| C004        |                       |
| C005        | Book                  |

---

### Using `explode()` (Standard Behavior)

```python
# Using regular explode
exploded_df = df.select("customer_id", explode(split("products", ", ")).alias("product"))
exploded_df.show()
```

**Output with `explode()`**:

| customer_id | product |
|-------------|---------|
| C001        | Apple   |
| C001        | Banana  |
| C001        | Orange  |
| C002        | Laptop  |
| C002        | Mouse   |
| C005        | Book    |

**What happened**:
- C003 (NULL) is **dropped** - not in result
- C004 (empty string) is **dropped** - not in result
- Only customers with actual products appear in the output
- Total rows: 6 (lost 2 customers)

---

### Using `explode_outer()` (Preserves NULLs)

```python
# Using explode_outer
exploded_outer_df = df.select("customer_id", explode_outer(split("products", ", ")).alias("product"))
exploded_outer_df.show()
```

**Output with `explode_outer()`**:

| customer_id | product |
|-------------|---------|
| C001        | Apple   |
| C001        | Banana  |
| C001        | Orange  |
| C002        | Laptop  |
| C002        | Mouse   |
| C003        | null    |
| C004        |         |
| C005        | Book    |

**What happened**:
- C003 (NULL) is **preserved** with NULL product value
- C004 (empty string) is **preserved** with empty product value
- All customers appear in the output, even without valid products
- Total rows: 8 (all customers retained)

---

### Side-by-Side Comparison

#### Input Data
| customer_id | products              |
|-------------|-----------------------|
| C001        | Apple, Banana, Orange |
| C002        | Laptop, Mouse         |
| C003        | null                  |
| C004        |                       |
| C005        | Book                  |

#### With `explode()` - 6 rows
| customer_id | product |
|-------------|---------|
| C001        | Apple   |
| C001        | Banana  |
| C001        | Orange  |
| C002        | Laptop  |
| C002        | Mouse   |
| C005        | Book    |

#### With `explode_outer()` - 8 rows
| customer_id | product |
|-------------|---------|
| C001        | Apple   |
| C001        | Banana  |
| C001        | Orange  |
| C002        | Laptop  |
| C002        | Mouse   |
| C003        | null    |
| C004        |         |
| C005        | Book    |

---

### When to Use `explode_outer()`

**Use `explode_outer()` when**:
1. You need to preserve all customers in your analysis, even those without products
2. You want to identify customers with missing data
3. You're doing a count of customers and need accurate totals
4. Downstream processes require all customer records

**Example use case**:
```python
# Count customers with and without products
result = exploded_outer_df.groupBy("customer_id").agg(
    count("product").alias("product_count")
)
result.show()
```

**Output**:
| customer_id | product_count |
|-------------|---------------|
| C001        | 3             |
| C002        | 2             |
| C003        | 0             |
| C004        | 0             |
| C005        | 1             |

This shows C003 and C004 have 0 products, which would be invisible with regular `explode()`.

---

### Complete Code Example with Both Functions

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, explode_outer, split, col

spark = SparkSession.builder.appName("ExplodeComparison").getOrCreate()

# Data with NULLs and empty values
data = [
    ("C001", "Apple, Banana, Orange"),
    ("C002", "Laptop, Mouse"),
    ("C003", None),
    ("C004", ""),
    ("C005", "Book")
]

df = spark.createDataFrame(data, ["customer_id", "products"])

print("Original DataFrame:")
df.show()

print("\nUsing explode() - Drops NULLs:")
df.select("customer_id", explode(split("products", ", ")).alias("product")).show()

print("\nUsing explode_outer() - Preserves NULLs:")
df.select("customer_id", explode_outer(split("products", ", ")).alias("product")).show()
```

---

## Performance Notes

- **For small datasets**: Both approaches are fast
- **For large datasets**: 
  - PySpark distributes the work across clusters
  - SQL execution depends on your database engine
  - Consider partitioning by customer_id for very large data
- **Memory**: `explode()` increases row count significantly; monitor cluster resources

---

## Real-World Use Cases

1. **E-commerce**: Analyzing product purchase patterns
2. **Inventory**: Breaking down multi-product orders
3. **Marketing**: Segmenting customers by individual product interests
4. **Reporting**: Creating item-level reports from order summaries