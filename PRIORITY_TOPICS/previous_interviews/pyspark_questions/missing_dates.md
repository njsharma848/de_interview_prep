# Finding Missing Billing Dates in PySpark

## Sample Data
The input DataFrame (`df`) is created from the following data:

```python
data = [
    ("C001", "2024-01-01"),
    ("C001", "2024-01-02"),
    ("C001", "2024-01-04"),
    ("C001", "2024-01-06"),
    ("C002", "2024-01-03"),
    ("C002", "2024-01-06"),
]
```

Expected output (gaps as ranges):

| customer_id | missing_from | missing_to |
|-------------|--------------|------------|
| C001       | 2024-01-03  | 2024-01-03 |
| C001       | 2024-01-05  | 2024-01-05 |
| C002       | 2024-01-04  | 2024-01-05 |

## PySpark Code Solution
The following PySpark code identifies the missing dates by using window functions to detect gaps.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, datediff, date_add, to_date
from pyspark.sql.window import Window

# Initialize Spark session (assuming it's not already created)
spark = SparkSession.builder.appName("MissingBillingDates").getOrCreate()

# Sample data (as provided)
data = [
    ("C001", "2024-01-01"),
    ("C001", "2024-01-02"),
    ("C001", "2024-01-04"),
    ("C001", "2024-01-06"),
    ("C002", "2024-01-03"),
    ("C002", "2024-01-06"),
]
df = spark.createDataFrame(data, ["customer_id", "billing_date"])

# Convert billing_date to date type
df = df.withColumn("billing_date", to_date(col("billing_date")))

# Define window: partition by customer_id, order by billing_date
window = Window.partitionBy("customer_id").orderBy("billing_date")

# Add previous date column using lag
df_with_lag = df.withColumn("prev_date", lag("billing_date").over(window))

# Filter for gaps (datediff > 1) and compute missing_from/missing_to
gaps = df_with_lag.filter(datediff(col("billing_date"), col("prev_date")) > 1) \
    .withColumn("missing_from", date_add(col("prev_date"), 1)) \
    .withColumn("missing_to", date_add(col("billing_date"), -1)) \
    .select("customer_id", "missing_from", "missing_to")

# Show the results (matches the expected output format)
gaps.show(truncate=False)
```

### Verification
The code produces the exact expected output, as simulated in previous responses.

## Step-by-Step DataFrame Updates
This section shows how the DataFrame evolves after each major transformation. Tables are based on the sample data (simulated using equivalent logic).

- **Initial DataFrame** (after creating from sample data):

  | customer_id | billing_date |
  |-------------|--------------|
  | C001       | 2024-01-01  |
  | C001       | 2024-01-02  |
  | C001       | 2024-01-04  |
  | C001       | 2024-01-06  |
  | C002       | 2024-01-03  |
  | C002       | 2024-01-06  |

- **After converting billing_date to datetime** (equivalent to `to_date` in Spark):

  | customer_id | billing_date |
  |-------------|--------------|
  | C001       | 2024-01-01  |
  | C001       | 2024-01-02  |
  | C001       | 2024-01-04  |
  | C001       | 2024-01-06  |
  | C002       | 2024-01-03  |
  | C002       | 2024-01-06  |

- **After sorting by customer_id and billing_date** (implicit in Spark windows):

  | customer_id | billing_date |
  |-------------|--------------|
  | C001       | 2024-01-01  |
  | C001       | 2024-01-02  |
  | C001       | 2024-01-04  |
  | C001       | 2024-01-06  |
  | C002       | 2024-01-03  |
  | C002       | 2024-01-06  |

- **After adding prev_date** (using lag over window; NaT/None for first row per group):

  | customer_id | billing_date | prev_date  |
  |-------------|--------------|------------|
  | C001       | 2024-01-01  | None      |
  | C001       | 2024-01-02  | 2024-01-01|
  | C001       | 2024-01-04  | 2024-01-02|
  | C001       | 2024-01-06  | 2024-01-04|
  | C002       | 2024-01-03  | None      |
  | C002       | 2024-01-06  | 2024-01-03|

- **After filtering for gaps** (datediff > 1):

  | customer_id | billing_date | prev_date  |
  |-------------|--------------|------------|
  | C001       | 2024-01-04  | 2024-01-02|
  | C001       | 2024-01-06  | 2024-01-04|
  | C002       | 2024-01-06  | 2024-01-03|

- **After adding missing_from and missing_to**:

  | customer_id | billing_date | prev_date  | missing_from | missing_to |
  |-------------|--------------|------------|--------------|------------|
  | C001       | 2024-01-04  | 2024-01-02| 2024-01-03  | 2024-01-03|
  | C001       | 2024-01-06  | 2024-01-04| 2024-01-05  | 2024-01-05|
  | C002       | 2024-01-06  | 2024-01-03| 2024-01-04  | 2024-01-05|

- **Final result** (after select):

  | customer_id | missing_from | missing_to |
  |-------------|--------------|------------|
  | C001       | 2024-01-03  | 2024-01-03 |
  | C001       | 2024-01-05  | 2024-01-05 |
  | C002       | 2024-01-04  | 2024-01-05 |

**Note**: This logic focuses on internal gaps. To include gaps up to the current date (September 01, 2025), additional steps (e.g., using `current_date()`) would be needed.

## Detailed Breakdown of Key Code Line
The following explains this specific line:

```python
gaps = df_with_lag.filter(datediff(col("billing_date"), col("prev_date")) > 1) \
    .withColumn("missing_from", date_add(col("prev_date"), 1)) \
    .withColumn("missing_to", date_add(col("billing_date"), -1)) \
    .select("customer_id", "missing_from", "missing_to")
```

### Assumptions
- Input: `df_with_lag` has `customer_id`, `billing_date` (DateType), and `prev_date`.
- Example input content (as above).

### Step-by-Step Explanation
1. **Filtering for Gaps: `df_with_lag.filter(datediff(col("billing_date"), col("prev_date")) > 1)`**  
   Keeps rows where the date difference > 1 day (indicating a gap). Rows with null `prev_date` are excluded.  
   **Why?** Identifies post-gap rows.  
   **After this**: Filtered to rows with gaps (e.g., 3 rows from sample).

2. **Adding `missing_from`: `.withColumn("missing_from", date_add(col("prev_date"), 1))`**  
   Adds 1 day to `prev_date` for gap start.  
   **Why?** Gap begins right after previous date.

3. **Adding `missing_to`: `.withColumn("missing_to", date_add(col("billing_date"), -1))`**  
   Subtracts 1 day from `billing_date` for gap end.  
   **Why?** Gap ends right before current date; handles single-day gaps (from == to).

4. **Selecting Columns: `.select("customer_id", "missing_from", "missing_to")`**  
   Drops extra columns for clean output.  
   **Why?** Focuses on gap info.

### Overall Notes
- **Chaining**: Methods are chained for concise code; each returns a new DataFrame.
- **Handling Gaps**: Naturally supports multi-day ranges.
- **Edge Cases**: No gaps → empty DF; single-day → from == to.
- **Performance**: Efficient with distributed windows.
- **Imports**: Requires `from pyspark.sql.functions import col, datediff, date_add`.

If modifications are needed (e.g., including gaps to September 01, 2025), provide details!
