# Spark Accumulators

## Introduction

Spark accumulators are part of the Spark low-level APIs. While they were primarily used with the Spark low-level RDD APIs, it's essential to understand the concept even if you're mainly using Spark DataFrame APIs.

## The Problem: Handling Bad Records

Consider a DataFrame that represents aggregated shipment records with three columns:
- **source**: origin location
- **destination**: destination location  
- **shipments**: number of shipments (expected to be integer)

### Sample Data

```
+-------+-----------+---------+
| source|destination|shipments|
+-------+-----------+---------+
|  India|    Germany|       10|
|    USA|       India|       20|
|Canada |        USA|       abc|
| Mexico|     France|       15|
| Brazil|      Japan|       -25|
+-------+-----------+---------+
```

**Problem**: The shipments column contains invalid data (e.g., "abc") that cannot be converted to integers.

**Solution**: Replace invalid values with `null` after consulting with the business team.

## Initial Solution: Using a UDF

### UDF Implementation

```python
from pyspark.sql.functions import udf

def handle_bad_records(shipments: str) -> int:
    s = None
    try:
        s = int(shipments)
    except ValueError:
        bad_rec.add(1)
    return s
```

This UDF:
1. Attempts to convert the shipments value to an integer
2. Returns `null` if conversion fails
3. Returns the integer value if successful

### Applying the UDF

```python
spark.udf.register("udf_handle_bad_records", handle_bad_records, IntegerType())
df = df.withColumn("shipments_fixed", expr("udf_handle_bad_records(shipments)")).show()
```

## The Challenge: Counting Bad Records

### Naive Approach

To count bad records, you could count the nulls in the new `shipments_fixed` column. However, this approach has a significant drawback:

**Problem**: The `count()` aggregation has a wide dependency, causing Spark to:
- Add an extra stage
- Perform a shuffle operation (shown as "exchange" in the execution plan)

This shuffle operation negatively impacts performance.

### The Question

Can we count bad records without introducing a shuffle? **Yes, using Spark Accumulators!**

## Solution: Spark Accumulators

**Spark Accumulator**: A global mutable variable that a Spark cluster can safely update on a per-row basis. Use cases include implementing counters or sums.

### Complete Example

```python
def handle_bad_rec(shipments: str) -> int:
    s = None
    try:
        s = int(shipments)
    except ValueError:
        bad_rec.add(1)
    return s

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Demo") \
        .master("local[3]") \
        .getOrCreate()

    data_list = [("india", "india", '5'),
                 ("india", "china", '7'),
                 ("china", "india", 'three'),
                 ("china", "china", '6'),
                 ("japan", "china", 'five')]

    df = spark.createDataFrame(data_list) \
        .toDF("source", "destination", "shipments")

    # Create accumulator with initial value of 0
    bad_rec = spark.sparkContext.accumulator(0)
    
    # Register and use the UDF
    spark.udf.register("udf_handle_bad_rec", handle_bad_rec, IntegerType())
    df.withColumn("shipments_int", expr("udf_handle_bad_rec(shipments)")) \
        .show()

    # Access the accumulator value at the driver
    print("Bad Record Count:" + str(bad_rec.value))
```

### How It Works

1. **Accumulator Creation**: `bad_rec = spark.sparkContext.accumulator(0)` creates a global counter initialized to 0
2. **Increment in UDF**: When a bad record is encountered, the accumulator increments by 1
3. **Access at Driver**: The accumulator lives at the driver, so you can directly print its value
4. **Internal Communication**: All tasks increment the accumulator through internal communication with the driver
5. **No Shuffle**: No extra stage or shuffle operation is needed to get the count

## Best Practices and Important Considerations

### Using Accumulators in Transformations vs Actions

You can increment accumulators from:
- Inside a **transformation** method (e.g., `withColumn()`)
- Inside an **action** method (e.g., `foreach()`)

**Recommendation**: Always use accumulators inside **actions**, not transformations.

### Why Use Accumulators in Actions?

**Guarantee of Accuracy**: Spark guarantees accurate results when accumulators are incremented inside actions.

**The Problem with Transformations**:
- Spark tasks can fail for various reasons, and the driver retries them on different workers
- Spark may trigger duplicate tasks if a task runs slowly
- If a task runs 2-3 times on the same data, it increments the accumulator multiple times, distorting the counter

**Spark's Guarantee for Actions**:
- Each task's update to the accumulator is applied only once
- This guarantee holds even if the task is restarted

**No Guarantee for Transformations**:
- Spark doesn't provide this guarantee when incrementing inside transformations

### Additional Features

**Named Accumulators (Scala only)**:
- In Scala, you can name accumulators and view them in the Spark UI
- PySpark accumulators are always unnamed and don't appear in the Spark UI

**Accumulator Types**:
- Built-in: Long and Float accumulators
- Custom: You can create custom accumulators for specific needs

## Summary

Accumulators provide a lightweight, efficient way to implement global counters in Spark without the overhead of shuffle operations. While they're most commonly used with RDD APIs, understanding their behavior is valuable for optimizing DataFrame operations when you need to track metrics during data processing.