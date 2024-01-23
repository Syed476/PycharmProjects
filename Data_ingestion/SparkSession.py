import pyspark
from pyspark.sql import SparkSession
from os.path import abspath

# spark = SparkSession.builder \
#       .master("local[1]") \
#       .appName("SparkByExamples.com") \
#       .config("spark.some.config.option", "config-value") \
#       .getOrCreate()


# Enabling Hive to use in Spark
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .config("spark.sql.warehouse.dir", "<path>/spark-warehouse") \
      .enableHiveSupport() \
      .getOrCreate()


# Get a Spark Config
partitions = spark.conf.get("spark.sql.shuffle.partitions")
print(partitions)

df = spark.createDataFrame(
    [("Scala", 25000), ("Spark", 35000), ("PHP", 21000)])
df.show()


# Spark SQL
df.createOrReplaceTempView("sample_table")
df2 = spark.sql("SELECT _1,_2 FROM sample_table")
df2.show()


