Spark accumulators are part of the Spark low-level APIs.

If you are using Spark Dataframe APIs, you are not likely to use accumulators. They were primarily used with the Spark low-level RDD APIs. However, it is essential to understand the concept.

So let me explain it at a high level. We will use an example code to explain the concept. Let me quickly explain the requirement, and then I will explain the code.

I have a dataframe that looks like this. This dataframe represents an aggregated shipment record. We have a source column, then a destination column, and finally, we have a shipments column. The shipments column represents the number of shipments from the given source to a destination. However, we have a slight problem here. The shipment column is expected to be an integer column. But we have some bad records in this table. I want to fix these bad records. But how do we do it?

# dataframe
+-------+-----------+---------+
| source|destination|shipments|
+-------+-----------+---------+
|  India|    Germany|       10|
|    USA|       India|       20|
|Canada |        USA|       abc|
| Mexico|     France|       15|
| Brazil|      Japan|       -25|
+-------+-----------+---------+

I discussed it with the business team, and they suggested a super simple solution. We are asked to take null if the shipments count is not a valid integer. So I decided to create a UDF for this. Here is the code for the UDF.

# udf to parse shipments
'''python
from pyspark.sql.functions import udf

def handle_bad_records(shipments: str) -> int:
    s = None
    try:
        s = int(shipments)
    except ValueError:
        bad_rec.add(1)
    return s
'''

This UDF simply takes the shipments column, converts it into an integer, and returns it. If we cannot convert the shipments column to an integer, we return null.

How to use this UDF? Here is the code.
# using the udf
'''python   
spark.udf.register("udf_handle_bad_records", handle_bad_records, IntegerType())
df = df.withColumn("shipments_fixed", expr("udf_handle_bad_records(shipments)")).show()
'''

But now, I have another requirement. We also want to count the number of bad records. How to do it? Can you think of some solution? We have an easy way to do it. I can simply count the nulls in the shipments_int column. Correct? We fixed two rows in this dataframe, replacing two bad values with nulls. So If I count the nulls in the new column, I exactly know the number of bad records.

The solution is perfectly fine. But count() aggregation or a count() action on a dataframe has a wide dependency. Spark will add one extra stage and a shuffle operation.

My execution plan looks like this.  So you can see this exchange in my execution plan. This exchange represents a shuffle that is caused by the count() operation. And that's not a good thing. I don't like this shuffle exchange in my plan? Can I do something else and avoid this shuffle? I wish I had a global variable to increment it from my UDF while I am fixing the bad record.

And that's precisely what Spark Accumulators are. Spark Accumulator is a global mutable variable that a Spark cluster can safely update on a per-row basis. You can use them to implement counters or sums.

Let me show you a complete example. Here is the code. I created an accumulator using the spark context with an initial value of zero. So the bad_rec is an accumulator variable. Then I use this accumulator variable in my UDF.

# complete code
'''python
def handle_bad_rec(shipments: str) -> int:

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

    bad_rec = spark.sparkContext.accumulator(0)
    spark.udf.register("udf_handle_bad_rec", handle_bad_rec, IntegerType())
    df.withColumn("shipments_int", expr("udf_handle_bad_rec(shipments)")) \
        .show()

    print("Bad Record Count:" + str(bad_rec.value))
'''

So whenever I see a bad record, I will increment the accumulator by one. Now come back to the end of the program. This accumulator variable is maintained at the driver. So I can simply print the final value. I do not have to collect it from anywhere. Because the accumulators always live at the driver. All the tasks will increment the value of this accumulator using internal communication with the driver.

So I do not have to include an extra stage and a shuffle for calculating the count of bad records. I am always incrementing it as I discover a bad record. So that's one potential use of an accumulator. You can use accumulators for counting whatever you want to count while processing your data. They are similar to global counter variables in spark. But remember a few essential things. I used the Spark accumulator from inside a withColumn() transformation. I mean, I used it from inside the UDF, but I am calling the UDF inside the withColumn() transformation. So effectively, the accumulator is used inside the withColumn() transformation.

You can increment your accumulators from inside a transformation method or from inside an action method. But it is always recommended to use an accumulator from inside action and avoid using it from inside a transformation.

Why? Because Spark gives you a guarantee of accurate results when the accumulator is incremented from inside an action. We already know, some spark tasks can fail for a variety of reasons. But the driver will retry those tasks on a different worker assuming success in the retry. Spark may also trigger a duplicate task if a task is running very slow. Right? The point is straight.

Spark runs duplicate tasks in many situations. So if a task is running 2-3 times on the same data, it will increment your accumulator multiple times, distorting your counter. Right? But if you are incrementing your accumulator from inside an action, Spark guarantees accurate results. So Spark guarantees that each task's update to the accumulator will be applied only once, even if the task is restarted. But if you are incrementing your accumulator from inside a transformation, Spark doesn't give this guarantee.

Spark in Scala also allows you to give your accumulator a name and show them in the Spark UI. However, PySpark accumulators are always unnamed, and they do not show up in the Spark UI. Spark allows you to create Long and Float accumulators. However, you can also create some custom accumulators.