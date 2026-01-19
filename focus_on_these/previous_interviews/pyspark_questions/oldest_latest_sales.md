# Spark Analysis: Finding Oldest and Latest Sales per Product

## Original Data

The following is the original dataset provided:

| id | product | date       | sales_amount |
|----|---------|------------|--------------|
| 1  | A       | 12-03-2025 | 45           |
| 2  | A       | 25-03-2025 | 85           |
| 3  | A       | 20-03-2025 | 75           |
| 4  | B       | 20-07-2025 | 70           |
| 5  | B       | 25-07-2025 | 15           |
| 6  | B       | 01-07-2025 | 10           |

## Query Description

For every product, find the `sales_amount` for the most latest date and the most oldest date (based on the dates in the dataset).

## Result

| Product | Oldest Date | Oldest Sales | Latest Date | Latest Sales |
|---------|-------------|--------------|-------------|--------------|
| A       | 12-03-2025  | 45           | 25-03-2025  | 85           |
| B       | 01-07-2025  | 10           | 25-07-2025  | 15           |

## Solution Steps

To arrive at this solution using PySpark (as the context suggests Spark), you can follow these steps:

1. Create a Spark DataFrame from the data.
2. Parse the date column to a proper date type using `to_date` with format 'dd-MM-yyyy'.
3. Use window functions to find the oldest and latest dates per product:
   - Define two windows partitioned by product, one ordered by date ascending for oldest, one descending for latest.
   - Use `row_number()` to rank and filter rank == 1 for each.
4. Alternatively, group by product to find min and max dates, then join back to the original DataFrame to get the sales amounts.

## Example PySpark Code

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, min, max
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()

data = [
    (1, "A", "12-03-2025", 45),
    (2, "A", "25-03-2025", 85),
    (3, "A", "20-03-2025", 75),
    (4, "B", "20-07-2025", 70),
    (5, "B", "25-07-2025", 15),
    (6, "B", "01-07-2025", 10)
]

df = spark.createDataFrame(data, ["id", "product", "date", "sales_amount"])
df = df.withColumn("date", to_date(col("date"), "dd-MM-yyyy"))

# Find min and max dates per product
date_extremes = df.groupBy("product").agg(
    min("date").alias("oldest_date"),
    max("date").alias("latest_date")
)

# Join to get oldest sales
oldest = date_extremes.join(df, (date_extremes.product == df.product) & (date_extremes.oldest_date == df.date), "inner") \
    .select(date_extremes.product, "oldest_date", col("sales_amount").alias("oldest_sales"))

# Join to get latest sales
latest = date_extremes.join(df, (date_extremes.product == df.product) & (date_extremes.latest_date == df.date), "inner") \
    .select(date_extremes.product, "latest_date", col("sales_amount").alias("latest_sales"))

# Combine
result = oldest.join(latest, "product")
result.show()
```

## Line-by-Line Explanation

### Imports

```python
from pyspark.sql import SparkSession
```
- This imports the `SparkSession` class, which is the entry point for working with structured data in PySpark.

```python
from pyspark.sql.functions import to_date, col, min, max
```
- This imports specific functions: `to_date` (to convert strings to date types), `col` (to reference columns), `min` and `max` (aggregation functions for finding minimum and maximum values).

```python
from pyspark.sql.window import Window
```
- This imports the `Window` class, used for defining window specifications (e.g., partitioning and ordering data for operations like ranking).

```python
from pyspark.sql.functions import row_number
```
- This imports `row_number`, a window function that assigns a sequential number to rows within a partition.

No DataFrames or tables are created yet, so no output table at this stage.

### SparkSession Creation

```python
spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()
```
- This creates a SparkSession named "SalesAnalysis". It's the main interface for interacting with Spark (handles configuration, context, etc.).

Still no data, so no output table.

### Data List Creation

```python
data = [
    (1, "A", "12-03-2025", 45),
    (2, "A", "25-03-2025", 85),
    (3, "A", "20-03-2025", 75),
    (4, "B", "20-07-2025", 70),
    (5, "B", "25-07-2025", 15),
    (6, "B", "01-07-2025", 10)
]
```
- This defines a list of tuples representing the raw data (id, product, date as string, sales_amount).

No DataFrame yet, but conceptually, the data looks like this (as a table for illustration):

| id | product | date       | sales_amount |
|----|---------|------------|--------------|
| 1  | A       | 12-03-2025 | 45           |
| 2  | A       | 25-03-2025 | 85           |
| 3  | A       | 20-03-2025 | 75           |
| 4  | B       | 20-07-2025 | 70           |
| 5  | B       | 25-07-2025 | 15           |
| 6  | B       | 01-07-2025 | 10           |

### Initial DataFrame Creation

```python
df = spark.createDataFrame(data, ["id", "product", "date", "sales_amount"])
```
- This creates a Spark DataFrame `df` from the data list, with specified column names. At this point, `date` is still a string type.

Output table (after `df.show()` would display this; dates are strings):

| id | product | date       | sales_amount |
|----|---------|------------|--------------|
| 1  | A       | 12-03-2025 | 45           |
| 2  | A       | 25-03-2025 | 85           |
| 3  | A       | 20-03-2025 | 75           |
| 4  | B       | 20-07-2025 | 70           |
| 5  | B       | 25-07-2025 | 15           |
| 6  | B       | 01-07-2025 | 10           |

### Date Conversion

```python
df = df.withColumn("date", to_date(col("date"), "dd-MM-yyyy"))
```
- This overwrites the `date` column by converting the string dates to actual date types using `to_date` with the format 'dd-MM-yyyy'. This enables proper date comparisons and aggregations.

Output table (after conversion; dates are now proper dates, e.g., in YYYY-MM-DD format when displayed):

| id | product | date       | sales_amount |
|----|---------|------------|--------------|
| 1  | A       | 2025-03-12 | 45           |
| 2  | A       | 2025-03-25 | 85           |
| 3  | A       | 2025-03-20 | 75           |
| 4  | B       | 2025-07-20 | 70           |
| 5  | B       | 2025-07-25 | 15           |
| 6  | B       | 2025-07-01 | 10           |

### Compute Date Extremes

```python
date_extremes = df.groupBy("product").agg(
    min("date").alias("oldest_date"),
    max("date").alias("latest_date")
)
```
- This groups the DataFrame by "product" and computes the minimum (oldest) and maximum (latest) dates for each product using `min` and `max` aggregations. Results are aliased to new column names.

Output table for `date_extremes` (min/max dates per product):

| product | oldest_date | latest_date |
|---------|-------------|-------------|
| A       | 2025-03-12  | 2025-03-25  |
| B       | 2025-07-01  | 2025-07-25  |

(Note: `df` remains unchanged here.)

### Compute Oldest Sales

```python
oldest = date_extremes.join(df, (date_extremes.product == df.product) & (date_extremes.oldest_date == df.date), "inner") \
    .select(date_extremes.product, "oldest_date", col("sales_amount").alias("oldest_sales"))
```
- This joins `date_extremes` back to `df` on matching product and where the date matches the oldest_date.
- It's an inner join, so only matching rows are kept.
- Selects product, oldest_date, and renames sales_amount to oldest_sales.
- For product A, matches row with date 2025-03-12 (sales 45).
- For product B, matches row with date 2025-07-01 (sales 10).
- If multiple rows had the same oldest date, it would return all (but here, dates are unique per product for extremes).

Output table for `oldest`:

| product | oldest_date | oldest_sales |
|---------|-------------|--------------|
| A       | 2025-03-12  | 45           |
| B       | 2025-07-01  | 10           |

### Compute Latest Sales

```python
latest = date_extremes.join(df, (date_extremes.product == df.product) & (date_extremes.latest_date == df.date), "inner") \
    .select(date_extremes.product, "latest_date", col("sales_amount").alias("latest_sales"))
```
- Similar to above, but joins where date matches latest_date.
- For product A, matches row with date 2025-03-25 (sales 85).
- For product B, matches row with date 2025-07-25 (sales 15).

Output table for `latest`:

| product | latest_date | latest_sales |
|---------|-------------|--------------|
| A       | 2025-03-25  | 85           |
| B       | 2025-07-25  | 15           |

### Combine Results

```python
result = oldest.join(latest, "product")
```
- This joins `oldest` and `latest` DataFrames on the "product" column (inner join by default).

Output table for `result` (combined oldest and latest per product):

| product | oldest_date | oldest_sales | latest_date | latest_sales |
|---------|-------------|--------------|-------------|--------------|
| A       | 2025-03-12  | 45           | 2025-03-25  | 85           |
| B       | 2025-07-01  | 10           | 2025-07-25  | 15           |

### Final Show

```python
result.show()
```
- This displays the `result` DataFrame in the console (as a table, similar to above).

This code efficiently finds the sales for the oldest and latest dates per product by using aggregations and joins, avoiding full sorts where possible. If there were ties (multiple rows with the same oldest/latest date), the joins would include all matching rows, potentially requiring additional handling (e.g., summing sales or picking one).
