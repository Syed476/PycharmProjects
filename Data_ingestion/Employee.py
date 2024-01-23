from os.path import abspath
from pyspark.sql import SparkSession

from pyspark.sql.types import StructType,StructField
from pyspark.sql.types import StringType, IntegerType, ArrayType

# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('spark-warehouse')

# Create spark session with hive enabled
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

columns = ["id", "name","age","gender"]

# Create DataFrame
data = [(1, "James",30,"M"), (2, "Ann",40,"F"),
    (3, "Jeff",41,"M"),(4, "Jennifer",20,"F")]
sampleDF = spark.sparkContext.parallelize(data).toDF(columns)

# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS emp")
new_df = sampleDF.filter(sampleDF['gender'] == 'M')
# Create Hive Internal table
new_df.write.mode('overwrite') \
          .saveAsTable("emp.employee")

# Spark read Hive table
df = spark.read.table("emp.employee")
df.show()