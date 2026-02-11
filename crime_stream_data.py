from pyspark.sql.types import *
from pyspark.sql import SparkSession
import os


if __name__ == "__main__":

    # Set your local host to be the master node of your cluster
    # Set the appName for your Spark session
    # Join session for app if it exists, else create a new one
 os.environ["HADOOP_HOME"] = r"C:\hadoop"
 os.environ["SPARK_HOME"]  = r"C:\spark"
 os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + ";" + os.environ["PATH"]

spark = (
    SparkSession.builder
    .appName("SparkLocalTest")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", r"C:\spark-warehouse")
    .config("spark.hadoop.fs.defaultFS", "file:///")

    # Force: NO native Windows Hadoop calls
    .config("spark.hadoop.io.native.lib.available", "false")
    .config("spark.hadoop.io.native.lib", "false")

    # Less fragile commit path on Windows
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")

    .getOrCreate()
)


    # ERROR log level will generate fewer lines of output compared to INFO and DEBUG                          
spark.sparkContext.setLogLevel("ERROR")


    # InferSchema not yet available in spark structured streaming 
    # (it is available in static dataframes)
    # We explicity state the schema of the input data
schema = StructType([StructField("lsoa_code", StringType(), True),\
                         StructField("borough", StringType(), True),\
                         StructField("major_category", StringType(), True),\
                         StructField("minor_category", StringType(), True),\
                         StructField("value", StringType(), True),\
                         StructField("year", StringType(), True),\
                         StructField("month", StringType(), True)])


    # Read stream into a dataframe
    # Since the csv data includes a header row, we specify that here
    # We state the schema to use and the location of the csv files
    # maxFilesPerTrigger sets the number of new files to be considered in each trigger
    # Trigger defines when the accumulated data should be processed
fileStreamDF = spark.readStream\
                               .option("header", "true")\
                               .option("maxFilesPerTrigger", 1)\
                               .schema(schema)\
                               .csv("c:\datasets\datasets/droplocation")


    # We group by the borough and count the number of records (NOT number of convictions)
    # We have used an aggregation function (orderBy), so can sort the dataframe
recordsPerBorough = fileStreamDF.groupBy("borough")\
                             .count()\
                             #.orderBy("count", ascending=False)

  
    # We run in complete mode, so only new rows are processed,
    # and existing rows in Result Table are not affected
    # The output is written to the console
    # We set truncate to false. If true, the output is truncated to 20 chars
    # Explicity state number of rows to display. Default is 20  
query = recordsPerBorough.writeStream\
                      .outputMode("update")\
                      .format("console")\
                      .option("truncate", "false")\
                      .option("numRows", 30)\
                      .start()\
                      .awaitTermination()