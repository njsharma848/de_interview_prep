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