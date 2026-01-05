# Spark Running Total Implementation

## Problem Statement

The problem statement your Spark application is addressing is: Given a dataset of orders with columns for `customer_id`, `order_date`, and `amount`, compute a running (cumulative) total of the amounts for a specific customer (e.g., '12345'), where the running total for each order row is the sum of all amounts from orders placed on or before that row's `order_date`. This involves processing the data to produce an output that includes the original columns plus a new column for the `running_total`, handling potential multiple orders on the same date by including all amounts up to and on that date in the cumulative sum.

### Sample Data

```
customer_id order_date amount
12345 01-Aug-25 100
12345 02-Aug-25 200
12345 02-Aug-25 150
12345 13-Aug-25 250
12345 13-Aug-25 500
12345 13-Aug-25 300
```

### SQL Query

```sql
SELECT customer_id, order_date, amount,
       (SELECT SUM(amount)
        FROM orders o2
        WHERE o2.customer_id = o1.customer_id AND o2.order_date <= o1.order_date) AS running_total
FROM orders o1
WHERE customer_id = '12345';
```

## PySpark Implementation of the Running Total

The SQL query you provided computes a **running total** (cumulative sum) of order amounts for a specific customer, grouped implicitly by date. For each row, the running total includes:
- All amounts from previous dates.
- **All amounts on the same date** (since the condition is `o2.order_date <= o1.order_date`, and without intra-day ordering like timestamps, all orders on the same date are included in the total for every row on that date).

This means rows on the same date will share the same running total value.

To implement this efficiently in PySpark (avoiding inefficient correlated subqueries), we'll:
1. Filter for the customer.
2. Aggregate daily totals (sum per date).
3. Compute the running total on the aggregated daily data using a window function.
4. Join the running totals back to the original data to assign them per row.

I'll provide the full PySpark code below, then explain it **line by line**. For each relevant line (or group of lines), I'll:
- Describe what it does.
- Show the intermediate output using your sample data (simulated based on manual computation, as this matches how Spark would process it).

Assume you have PySpark set up (e.g., via `pip install pyspark`). The code uses PySpark's DataFrame API.

### Full PySpark Code

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("RunningTotal").getOrCreate()

# Sample data as a list of tuples
data = [
    ("12345", "01-Aug-25", 100),
    ("12345", "02-Aug-25", 200),
    ("12345", "02-Aug-25", 150),
    ("12345", "13-Aug-25", 250),
    ("12345", "13-Aug-25", 500),
    ("12345", "13-Aug-25", 300)
]

# Define column names
columns = ["customer_id", "order_date", "amount"]

# Create DataFrame from sample data
df = spark.createDataFrame(data, columns)

# Filter for the specific customer (though all data is for '12345', this matches the SQL WHERE clause)
df_filtered = df.filter(col("customer_id") == "12345")

# Aggregate daily totals (sum of amounts per order_date)
daily_totals = df_filtered.groupBy("order_date").agg(sum("amount").alias("daily_total"))

# Define a window for running total: ordered by date, sum from start to current row
window_spec = Window.orderBy("order_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Compute running total on daily totals
daily_running = daily_totals.withColumn("running_total", sum("daily_total").over(window_spec))

# Join running totals back to the original filtered DataFrame on order_date
result = df_filtered.join(daily_running, on="order_date", how="left") \
                    .select("customer_id", "order_date", "amount", "running_total")

# Show the final result (this triggers computation)
result.show()
```

### Line-by-Line Explanation with Sample Data Outputs

I'll group closely related lines for clarity (e.g., imports or data setup). Outputs are shown as tables for readability, simulating `df.show()` results (Spark would display them similarly, with possible minor formatting differences). Note: Spark DataFrames are lazy, so no computation happens until an action like `show()`.

1. **Imports (lines 1-3):**
   ```python
   from pyspark.sql import SparkSession
   from pyspark.sql.functions import sum, col
   from pyspark.sql.window import Window
   ```
   - **Explanation:** These import necessary PySpark modules. `SparkSession` is the entry point for DataFrames. `sum` and `col` are functions for aggregation and column referencing. `Window` is for defining window specifications (used later for the running total).
   - **No output:** Imports don't produce data. They just prepare the environment.

2. **Initialize Spark session (line 5):**
   ```python
   spark = SparkSession.builder.appName("RunningTotal").getOrCreate()
   ```
   - **Explanation:** Creates or gets an existing Spark session. This is required to work with DataFrames. The app name is for logging/monitoring.
   - **No output:** This sets up the session but doesn't produce data.

3. **Define sample data and columns (lines 7-18):**
   ```python
   data = [
       ("12345", "01-Aug-25", 100),
       ("12345", "02-Aug-25", 200),
       ("12345", "02-Aug-25", 150),
       ("12345", "13-Aug-25", 250),
       ("12345", "13-Aug-25", 500),
       ("12345", "13-Aug-25", 300)
   ]
   columns = ["customer_id", "order_date", "amount"]
   ```
   - **Explanation:** `data` is a list of tuples matching your sample. `columns` defines the schema (column names and inferred types: string for IDs/dates, integer for amount).
   - **No output:** Just variables; no DataFrame yet.

4. **Create DataFrame (line 20):**
   ```python
   df = spark.createDataFrame(data, columns)
   ```
   - **Explanation:** Converts the list into a Spark DataFrame. This is lazy—no data is loaded yet.
   - **Sample Output (if you run `df.show()`):**

     | customer_id | order_date | amount |
     |-------------|------------|--------|
     | 12345       | 01-Aug-25 | 100    |
     | 12345       | 02-Aug-25 | 200    |
     | 12345       | 02-Aug-25 | 150    |
     | 12345       | 13-Aug-25 | 250    |
     | 12345       | 13-Aug-25 | 500    |
     | 12345       | 13-Aug-25 | 300    |

5. **Filter for customer (line 23):**
   ```python
   df_filtered = df.filter(col("customer_id") == "12345")
   ```
   - **Explanation:** Filters the DataFrame to only include rows where `customer_id` is '12345' (matches the SQL `WHERE`). Uses `col` to reference the column. In your sample, this doesn't change anything since all rows match, but it's good practice.
   - **Sample Output (if you run `df_filtered.show()`):**

     | customer_id | order_date | amount |
     |-------------|------------|--------|
     | 12345       | 01-Aug-25 | 100    |
     | 12345       | 02-Aug-25 | 200    |
     | 12345       | 02-Aug-25 | 150    |
     | 12345       | 13-Aug-25 | 250    |
     | 12345       | 13-Aug-25 | 500    |
     | 12345       | 13-Aug-25 | 300    |

6. **Aggregate daily totals (line 26):**
   ```python
   daily_totals = df_filtered.groupBy("order_date").agg(sum("amount").alias("daily_total"))
   ```
   - **Explanation:** Groups by `order_date` and sums `amount` for each date, aliasing the result as `daily_total`. This prepares for cumulative summing per date (efficient for large data).
   - **Sample Output (if you run `daily_totals.show()`):**

     | order_date | daily_total |
     |------------|-------------|
     | 01-Aug-25  | 100         |
     | 02-Aug-25  | 350         |
     | 13-Aug-25  | 1050        |

7. **Define window specification (line 29):**
   ```python
   window_spec = Window.orderBy("order_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
   ```
   - **Explanation:** Defines a window: ordered by `order_date`, processing rows from the start (unbounded preceding) to the current row. This sets up the cumulative sum.
   - **No output:** Just a definition; used in the next step.

8. **Compute running total on daily totals (line 32):**
   ```python
   daily_running = daily_totals.withColumn("running_total", sum("daily_total").over(window_spec))
   ```
   - **Explanation:** Adds a new column `running_total` by summing `daily_total` over the window. This computes the cumulative sum per date.
   - **Sample Output (if you run `daily_running.show()`):**

     | order_date | daily_total | running_total |
     |------------|-------------|---------------|
     | 01-Aug-25  | 100         | 100           |
     | 02-Aug-25  | 350         | 450           |
     | 13-Aug-25  | 1050        | 1500          |

9. **Join back and select columns (lines 35-36):**
   ```python
   result = df_filtered.join(daily_running, on="order_date", how="left") \
                       .select("customer_id", "order_date", "amount", "running_total")
   ```
   - **Explanation:** Joins the original filtered DataFrame with the running totals on `order_date` (left join to keep all original rows). Selects the desired columns (matches SQL output). Each original row gets the running total for its date.
   - **Sample Output (if you run `result.show()`):**

     | customer_id | order_date | amount | running_total |
     |-------------|------------|--------|---------------|
     | 12345       | 01-Aug-25 | 100    | 100           |
     | 12345       | 02-Aug-25 | 200    | 450           |
     | 12345       | 02-Aug-25 | 150    | 450           |
     | 12345       | 13-Aug-25 | 250    | 1500          |
     | 12345       | 13-Aug-25 | 500    | 1500          |
     | 12345       | 13-Aug-25 | 300    | 1500          |

10. **Show the final result (line 39):**
    ```python
    result.show()
    ```
    - **Explanation:** This is an action that triggers computation and prints the DataFrame. In a real run, it executes the lazy operations above.
    - **Sample Output:** Same as above (the final table).

This matches the SQL query's behavior. If your dates include times or you need intra-day ordering, you'd add `orderBy("order_date", "order_time")` to the window and skip daily aggregation. For large datasets, this is optimized (avoids full scans per row). If you run this code, ensure dates are parsed correctly (e.g., add `.config("spark.sql.legacy.timeParserPolicy", "LEGACY")` if needed for date formats). Let me know if you need adjustments!
