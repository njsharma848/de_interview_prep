# Spark Broadcast Variables

## Overview

Spark broadcast variables are part of Spark's low-level APIs, primarily used with RDD APIs rather than DataFrame APIs. They provide an efficient way to share reference data across executors in a Spark cluster.

## Problem Statement

When creating a Spark UDF, you often need to provide two types of inputs:
1. **Function arguments** - Column values passed directly to the UDF
2. **Reference data** - Lookup tables or datasets needed by the UDF logic

### Example Requirement

Create a UDF that translates product codes to product names:

```python
def getProductName(product_code: str) -> str:
    # Logic to translate product_code to product_name using lookup data
    pass
```

The lookup data structure:

```python
prodCode = {
    "P001": "Product 1",
    "P002": "Product 2",
    "P003": "Product 3"
}
```

**Challenge**: How do we make this reference data (potentially 5-10 MB in real scenarios) available to the UDF without passing it as a function argument?

## Solution Approaches

There are two ways to share reference data with UDFs:
1. **Closure** - Direct variable reference
2. **Broadcast Variables** - Cached shared variables

## Implementation Example

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import expr

def my_fun(code: str) -> str:
    return bdData.value.get(code)

if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Demo") \
        .master("local[3]") \
        .getOrCreate()

    # Load lookup data from file and convert to dictionary
    prdCode = spark.read.csv("data/lookup.csv").rdd.collectAsMap()
    
    # Create broadcast variable
    bdData = spark.sparkContext.broadcast(prdCode)

    # Create sample data
    data_list = [("98312", "2021-01-01", "1200", "P001"),
                ("98313", "2021-01-02", "1500", "P002"),
                ("98314", "2021-01-03", "1800", "P003")]

    df = spark.createDataFrame(data_list) \
        .toDF("order_id", "order_date", "amount", "product_code")

    # Register and apply UDF
    spark.udf.register("my_udf", my_fun, StringType())
    df.withColumn("Product", expr("my_udf(product_code)")).show()

    spark.stop()
```

## How It Works

1. The UDF function is called for each record in the DataFrame
2. The `product_code` column is passed to the UDF
3. The UDF looks up the product code in the broadcast variable
4. Returns the corresponding product name

## Closure vs Broadcast Variables

### Using Closure (Direct Variable Reference)

```python
def my_fun(code: str) -> str:
    return prdCode.get(code)  # Direct reference to prdCode
```

**Behavior**: Spark serializes the closure to **each task**
- **1000 tasks** on **30 node cluster** = **1000 serializations**

### Using Broadcast Variables

```python
def my_fun(code: str) -> str:
    return bdData.value.get(code)  # Using broadcast variable
```

**Behavior**: Spark serializes the variable **once per worker node** and caches it
- **1000 tasks** on **30 node cluster** = **30 serializations** (once per node)

## Key Concepts

### Definition
Broadcast variables are **shared, immutable variables** cached on every machine in the cluster instead of being serialized with every single task.

### Lazy Serialization
The broadcast variable is serialized to a worker node **only if** at least one task running on that node needs to access it.

### Memory Constraint
The broadcast variable must fit into the memory of an executor.

## Relationship to DataFrame APIs

Spark DataFrame APIs use this same technique to implement **broadcast joins**:
1. Bring a small table to the driver
2. Broadcast it to the workers
3. Use it in the join operation

## When to Use

- **Broadcast Joins**: If you can design your application logic to use broadcast joins, this is often preferable
- **UDFs with Reference Data**: When you need to make datasets available to your UDF for lookup purposes, use broadcast variables as demonstrated above

## Summary

- Use broadcast variables when sharing reference data with UDFs
- Provides significant performance improvement over closures for large clusters
- More efficient serialization (once per node vs once per task)
- Must fit in executor memory
- Consider broadcast joins as an alternative for DataFrame operations