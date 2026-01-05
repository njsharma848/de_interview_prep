1. **I am working with two PySpark files where *File-A* creates a SparkSession. How can I import or reference this existing SparkSession in *File-B* to avoid creating duplicate sessions and ensure I'm working with the same Spark context?**

2. **Given a poorly performing SQL statement, how would you approach its optimization? Consider aspects such as query execution plans, indexing strategies, join operations, and query rewriting techniques in your answer.**

3. **You need to build an incremental data pipeline using PySpark that captures inserts, updates, and deletes from a source system. How would you implement CDC (Change Data Capture) logic to identify changes and synchronize them with the target data system while maintaining data consistency?**

4. **In a production PySpark streaming ingestion pipeline, upstream source systems frequently update their schemas without prior notice. How would you architect the ingestion layer to automatically detect, validate, and accommodate these schema changes in real-time?**

5. **Your production Spark job is failing due to memory pressure during join operations on datasets with billions of records. Walk through your troubleshooting process and explain the specific optimizations you would apply, including partitioning strategies, join type selection, data skew handling, and resource configuration adjustments.**

6. **When ingesting multi-level nested JSON files into a data system using PySpark, what systematic approach would you use to flatten deeply nested structures into a relational format? Describe your strategy for handling arrays, nested objects, varying nesting depths.**

7. **During the data ingestion process using PySpark, if you encounter data skew that causes certain partitions to process significantly slower than others, how would you detect the skew at the ingestion layer and what techniques would you apply to mitigate it?**

8. **During the data ingestion process using PySpark, how would you design and implement a comprehensive data quality framework to cleanse, impute missing values, and validate incoming data for inconsistencies or incompleteness?**

**Describe how you would handle joining two DataFrames on a nullable column, including potential issues like null mismatches and strategies such as replacing nulls with sentinel values or using custom join conditions to ensure accurate results.**
