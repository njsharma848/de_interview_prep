# PySpark: Convert JSON to Table Columns - Complete Guide

## Problem Statement

Convert JSON strings stored in a DataFrame column into separate, structured columns for easier querying and analysis.

## Common Scenarios

### Scenario 1: Simple JSON Objects (Flat Structure)

#### Sample Input Data

| customer_id | user_data                                                    |
|-------------|--------------------------------------------------------------|
| C001        | {"name": "John", "age": 30, "city": "New York"}             |
| C002        | {"name": "Alice", "age": 25, "city": "London"}              |
| C003        | {"name": "Bob", "age": 35, "city": "Tokyo"}                 |

#### Expected Output

| customer_id | name  | age | city     |
|-------------|-------|-----|----------|
| C001        | John  | 30  | New York |
| C002        | Alice | 25  | London   |
| C003        | Bob   | 35  | Tokyo    |

---

## Method 1: Using `from_json()` with Schema

### Complete Code

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("JSONToColumns").getOrCreate()

# Sample data with JSON strings
data = [
    ("C001", '{"name": "John", "age": 30, "city": "New York"}'),
    ("C002", '{"name": "Alice", "age": 25, "city": "London"}'),
    ("C003", '{"name": "Bob", "age": 35, "city": "Tokyo"}')
]

# Create DataFrame
df = spark.createDataFrame(data, ["customer_id", "user_data"])

# Define schema for JSON
json_schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

# Parse JSON and extract fields
df_parsed = df.withColumn("parsed", from_json(col("user_data"), json_schema))
df_result = df_parsed.select(
    "customer_id",
    col("parsed.name").alias("name"),
    col("parsed.age").alias("age"),
    col("parsed.city").alias("city")
)

df_result.show()
```

### Step-by-Step Explanation

#### Step 1: Initialize Spark and Import Functions

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("JSONToColumns").getOrCreate()
```

**What happens**: 
- Imports necessary functions: `from_json` to parse JSON, `col` to reference columns
- Imports data types to define the JSON schema
- Creates Spark session

**Data state**: No data yet

---

#### Step 2: Create Sample Data

```python
data = [
    ("C001", '{"name": "John", "age": 30, "city": "New York"}'),
    ("C002", '{"name": "Alice", "age": 25, "city": "London"}'),
    ("C003", '{"name": "Bob", "age": 35, "city": "Tokyo"}')
]

df = spark.createDataFrame(data, ["customer_id", "user_data"])
```

**What happens**: 
- Creates list of tuples with customer IDs and JSON strings
- Converts to DataFrame

**Output after `df.show()`**:

| customer_id | user_data                                        |
|-------------|--------------------------------------------------|
| C001        | {"name": "John", "age": 30, "city": "New York"} |
| C002        | {"name": "Alice", "age": 25, "city": "London"}  |
| C003        | {"name": "Bob", "age": 35, "city": "Tokyo"}     |

---

#### Step 3: Define JSON Schema

```python
json_schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])
```

**What happens**: 
- Defines the structure of the JSON data
- Specifies field names and their data types
- `True` means the field is nullable

**Why this is important**: 
- PySpark needs to know the schema to parse JSON correctly
- Ensures proper data types (age as integer, not string)
- Improves performance by avoiding schema inference

**Data state**: No change to DataFrame yet

---

#### Step 4: Parse JSON into Struct Column

```python
df_parsed = df.withColumn("parsed", from_json(col("user_data"), json_schema))
```

**What happens**: 
- `from_json()` parses the JSON string using the defined schema
- Creates a new column "parsed" containing a struct (nested object)
- Original columns remain unchanged

**Output after `df_parsed.show(truncate=False)`**:

| customer_id | user_data                                        | parsed                           |
|-------------|--------------------------------------------------|----------------------------------|
| C001        | {"name": "John", "age": 30, "city": "New York"} | {John, 30, New York}            |
| C002        | {"name": "Alice", "age": 25, "city": "London"}  | {Alice, 25, London}             |
| C003        | {"name": "Bob", "age": 35, "city": "Tokyo"}     | {Bob, 35, Tokyo}                |

**Note**: The "parsed" column is a struct containing the fields, not separate columns yet.

---

#### Step 5: Extract Fields into Separate Columns

```python
df_result = df_parsed.select(
    "customer_id",
    col("parsed.name").alias("name"),
    col("parsed.age").alias("age"),
    col("parsed.city").alias("city")
)
```

**What happens**: 
- Uses dot notation `col("parsed.name")` to access struct fields
- `.alias()` renames the columns (removes "parsed." prefix)
- Selects only the columns we want in the final output

**Final Output**:

| customer_id | name  | age | city     |
|-------------|-------|-----|----------|
| C001        | John  | 30  | New York |
| C002        | Alice | 25  | London   |
| C003        | Bob   | 35  | Tokyo    |

---

## Method 2: Using `select()` with Wildcard (Shortcut)

If you want all fields from the struct without typing each one:

```python
# After parsing JSON
df_result = df_parsed.select("customer_id", "parsed.*")
```

**What happens**: 
- `"parsed.*"` expands all fields in the struct automatically
- Equivalent to listing each field manually
- Saves typing when there are many fields

**Output**:

| customer_id | name  | age | city     |
|-------------|-------|-----|----------|
| C001        | John  | 30  | New York |
| C002        | Alice | 25  | London   |
| C003        | Bob   | 35  | Tokyo    |

---

## Scenario 2: Nested JSON Objects

### Sample Input Data

| order_id | order_details                                                                      |
|----------|------------------------------------------------------------------------------------|
| O001     | {"customer": {"name": "John", "email": "john@email.com"}, "amount": 100}          |
| O002     | {"customer": {"name": "Alice", "email": "alice@email.com"}, "amount": 200}        |

### Expected Output

| order_id | customer_name | customer_email     | amount |
|----------|---------------|--------------------|--------|
| O001     | John          | john@email.com     | 100    |
| O002     | Alice         | alice@email.com    | 200    |

### Code for Nested JSON

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define nested schema
nested_schema = StructType([
    StructField("customer", StructType([
        StructField("name", StringType(), True),
        StructField("email", StringType(), True)
    ]), True),
    StructField("amount", IntegerType(), True)
])

# Sample data
data = [
    ("O001", '{"customer": {"name": "John", "email": "john@email.com"}, "amount": 100}'),
    ("O002", '{"customer": {"name": "Alice", "email": "alice@email.com"}, "amount": 200}')
]

df = spark.createDataFrame(data, ["order_id", "order_details"])

# Parse and extract
df_parsed = df.withColumn("parsed", from_json(col("order_details"), nested_schema))
df_result = df_parsed.select(
    "order_id",
    col("parsed.customer.name").alias("customer_name"),
    col("parsed.customer.email").alias("customer_email"),
    col("parsed.amount").alias("amount")
)

df_result.show()
```

**Output**:

| order_id | customer_name | customer_email     | amount |
|----------|---------------|--------------------|--------|
| O001     | John          | john@email.com     | 100    |
| O002     | Alice         | alice@email.com    | 200    |

---

## Scenario 3: JSON Arrays

### Sample Input Data

| customer_id | orders                                              |
|-------------|-----------------------------------------------------|
| C001        | [{"item": "Laptop", "price": 1000}, {"item": "Mouse", "price": 20}] |
| C002        | [{"item": "Book", "price": 15}]                     |

### Expected Output (Exploded)

| customer_id | item   | price |
|-------------|--------|-------|
| C001        | Laptop | 1000  |
| C001        | Mouse  | 20    |
| C002        | Book   | 15    |

### Code for JSON Arrays

```python
from pyspark.sql.functions import from_json, explode, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Define schema for array of objects
array_schema = ArrayType(StructType([
    StructField("item", StringType(), True),
    StructField("price", IntegerType(), True)
]))

# Sample data
data = [
    ("C001", '[{"item": "Laptop", "price": 1000}, {"item": "Mouse", "price": 20}]'),
    ("C002", '[{"item": "Book", "price": 15}]')
]

df = spark.createDataFrame(data, ["customer_id", "orders"])

# Parse JSON array
df_parsed = df.withColumn("orders_array", from_json(col("orders"), array_schema))

# Explode array into rows
df_exploded = df_parsed.withColumn("order", explode(col("orders_array")))

# Extract fields
df_result = df_exploded.select(
    "customer_id",
    col("order.item").alias("item"),
    col("order.price").alias("price")
)

df_result.show()
```

**Step-by-Step Output**:

**After parsing** (`df_parsed`):

| customer_id | orders                                              | orders_array                    |
|-------------|-----------------------------------------------------|---------------------------------|
| C001        | [{"item": "Laptop", "price": 1000}, ...]           | [{Laptop, 1000}, {Mouse, 20}]  |
| C002        | [{"item": "Book", "price": 15}]                    | [{Book, 15}]                   |

**After exploding** (`df_exploded`):

| customer_id | orders_array                    | order          |
|-------------|---------------------------------|----------------|
| C001        | [{Laptop, 1000}, {Mouse, 20}]  | {Laptop, 1000} |
| C001        | [{Laptop, 1000}, {Mouse, 20}]  | {Mouse, 20}    |
| C002        | [{Book, 15}]                   | {Book, 15}     |

**Final output** (`df_result`):

| customer_id | item   | price |
|-------------|--------|-------|
| C001        | Laptop | 1000  |
| C001        | Mouse  | 20    |
| C002        | Book   | 15    |

---

## Method 3: Schema Inference (Automatic)

If you don't know the schema in advance, you can let Spark infer it:

```python
from pyspark.sql.functions import from_json, schema_of_json, col

# Sample JSON string to infer schema from
sample_json = '{"name": "John", "age": 30, "city": "New York"}'

# Get schema from sample
inferred_schema = schema_of_json(sample_json)

# Parse JSON using inferred schema
df_parsed = df.withColumn("parsed", from_json(col("user_data"), inferred_schema))
df_result = df_parsed.select("customer_id", "parsed.*")
```

**Pros**: 
- No need to manually define schema
- Quick for prototyping

**Cons**: 
- Less efficient (requires scanning data)
- May infer incorrect types
- Not recommended for production

---

## Handling Malformed JSON

### Sample Data with Errors

| customer_id | user_data                              |
|-------------|----------------------------------------|
| C001        | {"name": "John", "age": 30}           |
| C002        | {invalid json}                         |
| C003        | {"name": "Bob"}                        |

### Code with Error Handling

```python
from pyspark.sql.functions import from_json, col

# Parse with mode option
df_parsed = df.withColumn(
    "parsed", 
    from_json(col("user_data"), json_schema, {"mode": "PERMISSIVE"})
)

# PERMISSIVE mode (default): Sets malformed rows to null
# DROPMALFORMED mode: Drops malformed rows
# FAILFAST mode: Throws exception on malformed data

df_result = df_parsed.select("customer_id", "parsed.*")
df_result.show()
```

**Output with PERMISSIVE mode**:

| customer_id | name | age  |
|-------------|------|------|
| C001        | John | 30   |
| C002        | null | null |
| C003        | Bob  | null |

---

## Performance Tips

1. **Define Schema Explicitly**: Always define schema for better performance
2. **Use Appropriate Data Types**: Don't make everything StringType
3. **Cache Parsed Data**: If reusing, call `.cache()` after parsing
4. **Partition Large Datasets**: Use `.repartition()` before JSON parsing
5. **Filter Early**: Apply filters before JSON parsing when possible

---

## Common Use Cases

1. **API Response Data**: Parse JSON responses stored in data lake
2. **Log Analysis**: Extract structured data from JSON logs
3. **Event Streaming**: Process JSON events from Kafka/Kinesis
4. **Configuration Data**: Parse JSON configuration stored in tables
5. **IoT Data**: Extract sensor readings from JSON payloads

---

## Summary of Key Functions

| Function | Purpose |
|----------|---------|
| `from_json()` | Parse JSON string into struct |
| `schema_of_json()` | Infer schema from sample JSON |
| `col("struct.field")` | Access nested struct fields |
| `select("struct.*")` | Expand all struct fields |
| `explode()` | Convert array into rows |
| `StructType/StructField` | Define JSON schema |
| `ArrayType` | Define array schema |