# AWS/Data Engineering Interview Preparation

---

## General Guidance

**Kindly ensure that our level is also matching their standards.**

### Key Questions

1. **Where did you use the partitions and how did it help you for better data analytics?**
2. **How would you detect the malformed data and how would you get rid of it and process the data for data analytics?**
3. **Suppose you have a large data set in S3 bucket and there you need to query only last 2 years of data,** \
   **but other than the last 2 years sometimes we need to query other data when required.** \
   **How would you make the solution to reduce the storage cost and improve the performance as well?**

---

## Introduction

- **Are you certified in AWS?**

---

## Technical Questions

1. **Critical HIVE Query is taking long to run. How to fix it?**
2. **We are reading .csv file. The spark job fails with malformed line error.** \
   **Data Quality issue is there. How to fix it?**
4. **Let's say occasionally file will have these issues. Data Quality issue.** \
   **Implement a data quality check beforehand. Automate in your data pipeline.** \
   **Let's say it’s a huge file. How to scan it? Sampling will not give 100% response. Suggest something else.**
7. **We are reading Parquet file from S3. Check invalid errors. How to diagnose?**
8. **Let's say you are working on on-premise data. How to migrate to AWS.** \
   **Tell challenges and steps to anticipate here. Can we do this with Zero Downtime?**
9. **What are the services in AWS you are aware of?**
10. **Let's say Source file doesn’t have a Business Date in the filename.** \
    **The file is basically overwritten. The file is landing in S3. Is there a way to retrieve old file? Is it Bucket level or File level?**
13. **Let’s say you have a source dataset with employees data. Give 2 solutions:  SQL and Python** \
    **Write a query to get max annual CTC for each department. _Hint_: Use Windows function for SQL**

---

## Additional Technical Topics

### Core Concepts
- **What is the PHI data and how to handle the PHI data**
- **What is hash generator**
- **What is partition and types of partitions**
- **What is mean by data skewness and how to handle it**
- **S3 storage level how to improve the performance** \
  **and cost optimization to store large data set in S3 bucket**
- **What is the EMR, Glue and what is data catalog** \
  **and when triggered the data catalog (scheduled based or ad-hoc based)**
- **What is Slowly Changing Dimensions (SCD)**
- **How to check the data is skewed**
- **How to Improve the Spark job**

### Project and Resume Related Questions
- **Lots of Questions about current project and from your Resume.**

### Debugging and Troubleshooting
- **Failed Spark job in EMR → Check driver/executor logs for OOM, schema, IAM issues.**
- **Malformed data fix → Enforce schema, clean source, quarantine bad rows.**
- **Logs to check → Error types, stage, executor memory, failing file/path.**
- **Resolve malformed data → Find root cause, sanitize upstream, configure Spark mode.**
- **Drop bad records → Use DROPMALFORMED or custom filtering with quarantine path.**
- **Handling failed jobs → Debug logs, isolate cause, retry, document & monitor.**

### Performance Optimization
- **Partitioning vs bucketing → Partition = directory structure; Bucketing = joins/group by.**
- **Cluster size factors → Data volume, query complexity, concurrency, SLAs.**
- **Other query performance → Use ORC/Parquet, compression, vectorization, stats.**
- **Automated DQ checks examples → Null %, referential integrity, duplicates, anomaly detection.**

---

1. **Examples of Spark production issues -> Data skew, Out of Memory, small files problem.**
2. **Job not running for 3 days -> Start with scheduler/logs, check cluster resources, data availabilty, rollback if needed.**
3. **Other jobs running but mine failed -> Could be skewed data, bad input, resource misconfig, or dependency failure.**
4. **Starting point for debuggung -> Check Spark job logs and Spark UI to identify stage/type of failure.**
5. **Job failed due to OOM -> Optimize transformations, handle skew, repartition, tune memory, avoid driver collect.**
6. **How to implement salting -> Add random salt to skewed dataset, replicate small dataset with all salt value, join on (key, salt).**
7. **Map-side join vs Salting -> Broadcast join works when one dataset is small; salting fixes skew when both are large.**
8. **Implement salting for A.key = B.key -> Add random salt to Table A, expand Table B with all salts, join on (key, salt).**
9. **Why use explode array(lit) -> To replicate smaller dataset with all salt values so it matches salted keys.**
10. **How salt values match between A and B -> A gets random salt, B has all salts -> guarantees join correctness.**
11. **Products example (apple, banana, orange, grape) -> Skew occurs on qty=0; salting distributes join equally.**
12. **List of products with zero stock -> Result: banana and grape.**
14. **SQL: Latest account balance per id -> Use ROW_NUMBER() over timestamp desc, or MAX(timestamp) join.**
    
---

1. **How to calculate the Compliance result.**
2. **Malformed Data Schema validation, what happened if dropped the correct records.**
3. **Data Skewness: How to know that data is skewed.**
4. **When you decide to go with Salting key and Broadcast join.**
5. **AWS Services like (EMR, Glue, S3, CloudWatch).**
6. **SQL question to find the Max Salary department wise, Row_Number().**
7. **How to implement the production changes if any hot fix and urgent changes.**

---------

1. **Difference between reparation and coalesce and when to use which one.** <br>
   **When working with big data, partition management is everything. Two common tools we use in PySpark: Repartition and Coalesce.** <br><br>
   🔹 **Repartition:** <br>
      **Redistributes data across all partitions** <br>
      **Involves a shuffle → data is moved across the cluster** <br>
      **Can increase or decrease the number of partitions** <br>
      **Useful when you want to balance the data evenly across executors** <br><br>
   🔹 **Coalesce** <br>
      **Merges existing partitions without a full shuffle** <br>
      **Only reduces the number of partitions (cannot increase)** <br>
      **Faster and cheaper than repartition** <br>
      **Great for optimizing after a heavy filter (when data is already smaller)** <br><br>
   ✅ **Rule of Thumb** <br>
   **Use Repartition when you need even distribution (scaling up or balancing work).** <br>
   **Use Coalesce when you just need fewer partitions quickly (scaling down).** <br>
   **Both are essential to keeping Spark workloads balanced, efficient, and cost-effective.** <br>
   **Next time you optimize your PySpark jobs, remember: <br> it’s not just about transformations — it’s also about how your data is partitioned.**

---

2. **To process 25gb in spark.** <br>
   a. **How many cpu cores are required?** <br>
   b. **How many executors are required?** <br>
   c. **How much each executor memory is required?** <br>
   d. **What is the total memory required?** <br><br>
   **How many executor CPU cores are required to proceess 25GB data?** <br>
   **25GB = 25*1024 = 25600MB** <br>
   **Number of Partitions = 25600MB/128MB = 200** <br>
   **Number of CPU cores = Number of partitions = 200**

   **How many executors are required to proceess 25GB data?** <br>
   **Avg CPU cores for eaxch executor = 4** <br>
   **Total number of executors = 200/4 = 50**

   **How much each executor memory is required to process 25GB data?** <br>
   **CPU cores for each executor = 4** <br>
   **Memory for each executor = 4*512MB = 2GB**

   **What is the total memory required to process 25GB data?** <br>
   **Total number of executors = 50** <br>
   **Memory for each executor = 2GB** <br>
   **Total Memory for all the executor = 50*2GB = 100GB**

#### **Notes:** <br>
**By default, Spark creates one partition for each block of the file (Blocks being 128MB by default in HDFS), <br> but you can also ask for a higher number of partitions by passing a larger value.** <br>
**To get the better job performance in Spark, researchers have found that we can take 2 to 5 maximum core for each executor.** <br>
**Expected memory for each core = Minimum 4 x (default partition size) = 4*128MB = 512MB.** <br>
**Executor memory is not less than 1.5 times of Spark reserved memory (Single core executor memory should not be less than 450MB)**

---

3. **How do you handle schema evolution.**
4. **How do you setup external libraries in AWS lambda before using it.**
5. **Expalin how do you spin up data services in AWS, and also how do you configure AWS CLI.**
6. **What is the difference between Athena and Redshift spectrum and which one of them is powerful.**
7. **What is the difference between standard views and materialised views.**
8. **What is the difference between internal tables and external tables.**
9. **Managing Partitions in AWS Athena with MSCK REPAIR TABLE**
10. **Difference between dataframe and dynamicframe, and how dynamicframe supports schema evolution.**
11. **How can we access s3 bucket in one account from another account.**
12. **What is the main difference between SparkContext and SparkSession.**
13. **What is the main difference between RDD, DataFrame, and DynamicFrame.**
14. **What are the deployment modes available in Amazon EMR.**
15. **Be prepared with how to enable the configurations for known services in aws cloud.**

---
