1. MSCK Repair Table

   The `MSCK REPAIR TABLE` command is used in Apache Hive to update the metadata of a table to reflect the partitions that exist in the underlying storage. This is particularly useful when new partitions are added directly to the storage (e.g., HDFS) without using Hive commands.

   Example:
   ```sql
   MSCK REPAIR TABLE your_table_name;
   ```

   This command will scan the table's directory in HDFS and add any new partitions to the Hive metastore.

2. Difference Between Managed and External Tables

   - **Managed Table**: In Hive, a managed table is one where Hive manages both the metadata and the data. When you drop a managed table, both the metadata and the data are deleted from the storage.

   - **External Table**: An external table is one where Hive only manages the metadata, while the data is stored externally (outside of Hive's control). When you drop an external table, only the metadata is deleted, and the data remains intact in its original location.

   Example:
   ```sql
   -- Creating a managed table
   CREATE TABLE managed_table (id INT, name STRING);

   -- Creating an external table
   CREATE EXTERNAL TABLE external_table (id INT, name STRING)
   LOCATION 'hdfs://path/to/external/data';
   ```

3. What is the difference between Partitioning and Bucketing in Hive?

   - **Partitioning**: Partitioning is a technique used to divide a large table into smaller, more manageable pieces based on the values of one or more columns (partition keys). Each partition corresponds to a specific value of the partition key(s) and is stored in a separate directory in HDFS. This improves query performance by allowing Hive to scan only the relevant partitions.

   Example:
   ```sql
   CREATE TABLE sales (
       id INT,
       amount FLOAT
   )
   PARTITIONED BY (year INT, month INT);
   ```

   - **Bucketing**: Bucketing is a technique used to divide data into a fixed number of buckets based on the hash value of a specified column (bucket key). Each bucket is stored as a separate file within the table's directory. Bucketing helps in optimizing joins and sampling operations.

   Example:
   ```sql
   CREATE TABLE employees (
       id INT,
       name STRING
   )
   CLUSTERED BY (id) INTO 4 BUCKETS;
   ```

4. What is the difference between DDL and DML commands in SQL?

   - **DDL (Data Definition Language)**: DDL commands are used to define and manage database structures such as tables, indexes, and schemas. Common DDL commands include `CREATE`, `ALTER`, `DROP`, and `TRUNCATE`.

   Example:
   ```sql
   CREATE TABLE employees (id INT, name STRING);
   ALTER TABLE employees ADD COLUMN age INT;
   DROP TABLE employees;
   ```

   - **DML (Data Manipulation Language)**: DML commands are used to manipulate the data within the database. Common DML commands include `SELECT`, `INSERT`, `UPDATE`, and `DELETE`.

   Example:
   ```sql
   INSERT INTO employees (id, name) VALUES (1, 'John Doe');
   UPDATE employees SET name = 'Jane Doe' WHERE id = 1;
   DELETE FROM employees WHERE id = 1;
   SELECT * FROM employees;
   ```