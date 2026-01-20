# Step-by-Step Explanation of the PySpark Code

I'll walk through the provided PySpark code line by line (focusing on the key DataFrame transformations), explaining what each section does. For each major transformation, I'll show the "updated table" (i.e., the state of the DataFrame after that step) in a markdown table format for clarity. Since the dataset is small, I've simulated the output based on the logic of PySpark operations—no actual execution is needed, but the results are accurate representations.

**Note:**
- Dates are parsed from "dd-MM-yyyy" to a proper date format (e.g., "01-08-2025" becomes 2025-08-01).
- The initial data has 11 rows.
- Only Cid 1 and Cid 3 have more than 3 transactions (Cid 1: 4, Cid 3: 4, Cid 2: 2, Cid 4: 1).
- The final result will only include the latest transaction for those qualifying customers.

## 1. Initialization and DataFrame Creation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, count, row_number, desc
from pyspark.sql.window import Window

# Initialize SparkSession
spark = SparkSession.builder.appName("LatestTransactions").getOrCreate()

# Sample data
data = [
    (1, "01-08-2025", 100),
    (1, "02-08-2025", 250),
    (1, "04-08-2025", 150),
    (2, "01-08-2025", 190),
    (1, "07-08-2025", 300),
    (2, "09-08-2025", 450),
    (3, "01-08-2025", 350),
    (4, "11-08-2025", 280),
    (3, "10-08-2025", 170),
    (3, "14-08-2025", 150),
    (3, "18-08-2025", 300)
]

# Columns
columns = ["Cid", "transaction_date", "transaction_amount"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
```

- **Explanation**: This sets up the Spark session and creates the initial DataFrame from the provided data. The `transaction_date` is still a string at this point.
- **Updated Table** (initial `df`):

| Cid | transaction_date | transaction_amount |
|-----|------------------|--------------------|
| 1   | 01-08-2025       | 100                |
| 1   | 02-08-2025       | 250                |
| 1   | 04-08-2025       | 150                |
| 2   | 01-08-2025       | 190                |
| 1   | 07-08-2025       | 300                |
| 2   | 09-08-2025       | 450                |
| 3   | 01-08-2025       | 350                |
| 4   | 11-08-2025       | 280                |
| 3   | 10-08-2025       | 170                |
| 3   | 14-08-2025       | 150                |
| 3   | 18-08-2025       | 300                |

## 2. Convert Date Column

```python
# Convert transaction_date to date type (assuming dd-MM-yyyy format)
df = df.withColumn("transaction_date", to_date("transaction_date", "dd-MM-yyyy"))
```

- **Explanation**: This converts the `transaction_date` string column to a proper DateType using `to_date`. This is necessary for correct ordering and comparisons later.
- **Updated Table** (`df` after conversion; dates are now in YYYY-MM-DD format for internal processing, but shown here for clarity):

| Cid | transaction_date | transaction_amount |
|-----|------------------|--------------------|
| 1   | 2025-08-01       | 100                |
| 1   | 2025-08-02       | 250                |
| 1   | 2025-08-04       | 150                |
| 2   | 2025-08-01       | 190                |
| 1   | 2025-08-07       | 300                |
| 2   | 2025-08-09       | 450                |
| 3   | 2025-08-01       | 350                |
| 4   | 2025-08-11       | 280                |
| 3   | 2025-08-10       | 170                |
| 3   | 2025-08-14       | 150                |
| 3   | 2025-08-18       | 300                |

## 3. Add Transaction Count Column

```python
# Compute the transaction count per Cid using a window
window_count = Window.partitionBy("Cid")
df_with_count = df.withColumn("trans_count", count("*").over(window_count))
```

- **Explanation**: Defines a window partitioned by `Cid` (groups rows by customer). Then, adds a new column `trans_count` using `count("*")` over this window, which counts the total transactions per customer and replicates that count for every row of that customer.
- **Updated Table** (`df_with_count`):

| Cid | transaction_date | transaction_amount | trans_count |
|-----|------------------|--------------------|-------------|
| 1   | 2025-08-01       | 100                | 4           |
| 1   | 2025-08-02       | 250                | 4           |
| 1   | 2025-08-04       | 150                | 4           |
| 1   | 2025-08-07       | 300                | 4           |
| 2   | 2025-08-01       | 190                | 2           |
| 2   | 2025-08-09       | 450                | 2           |
| 3   | 2025-08-01       | 350                | 4           |
| 3   | 2025-08-10       | 170                | 4           |
| 3   | 2025-08-14       | 150                | 4           |
| 3   | 2025-08-18       | 300                | 4           |
| 4   | 2025-08-11       | 280                | 1           |

## 4. Filter Customers with >3 Transactions

```python
# Filter customers with more than 3 transactions
filtered_df = df_with_count.filter("trans_count > 3")
```

- **Explanation**: Filters the DataFrame to keep only rows where `trans_count > 3`. This removes Cid 2 and Cid 4 entirely, leaving only rows for Cid 1 and Cid 3.
- **Updated Table** (`filtered_df`):

| Cid | transaction_date | transaction_amount | trans_count |
|-----|------------------|--------------------|-------------|
| 1   | 2025-08-01       | 100                | 4           |
| 1   | 2025-08-02       | 250                | 4           |
| 1   | 2025-08-04       | 150                | 4           |
| 1   | 2025-08-07       | 300                | 4           |
| 3   | 2025-08-01       | 350                | 4           |
| 3   | 2025-08-10       | 170                | 4           |
| 3   | 2025-08-14       | 150                | 4           |
| 3   | 2025-08-18       | 300                | 4           |

## 5. Add Row Number for Latest Transaction and Filter

```python
# Define window for latest transaction: partition by Cid, order by date descending
window_latest = Window.partitionBy("Cid").orderBy(desc("transaction_date"))

# Add row number and filter for the latest (rn == 1)
latest_transactions = filtered_df.withColumn("rn", row_number().over(window_latest)) \
                                 .filter("rn == 1") \
                                 .drop("rn", "trans_count")
```

- **Explanation**:
  - Defines a window partitioned by `Cid` and ordered by `transaction_date` descending (newest first).
  - Adds a `rn` (row number) column using `row_number()` over this window—assigns 1 to the newest transaction per customer, 2 to the next, etc.
  - Filters to keep only rows where `rn == 1` (the latest per customer).
  - Drops the temporary `rn` and `trans_count` columns.
- **Intermediate Table** (after adding `rn`, before filtering and dropping; for illustration):

| Cid | transaction_date | transaction_amount | trans_count | rn |
|-----|------------------|--------------------|-------------|----|
| 1   | 2025-08-07       | 300                | 4           | 1  |
| 1   | 2025-08-04       | 150                | 4           | 2  |
| 1   | 2025-08-02       | 250                | 4           | 3  |
| 1   | 2025-08-01       | 100                | 4           | 4  |
| 3   | 2025-08-18       | 300                | 4           | 1  |
| 3   | 2025-08-14       | 150                | 4           | 2  |
| 3   | 2025-08-10       | 170                | 4           | 3  |
| 3   | 2025-08-01       | 350                | 4           | 4  |

- **Updated Table** (`latest_transactions` after filtering and dropping):

| Cid | transaction_date | transaction_amount |
|-----|------------------|--------------------|
| 1   | 2025-08-07       | 300                |
| 3   | 2025-08-18       | 300                |

## 6. Display the Result

```python
# Show the result
latest_transactions.show()
```

- **Explanation**: This triggers the computation (Spark is lazy-evaluated) and prints the final DataFrame. In a real run, it would output the table above.

This code efficiently uses window functions to avoid unnecessary shuffles, making it performant for larger datasets. If you have questions on any part, let me know!
