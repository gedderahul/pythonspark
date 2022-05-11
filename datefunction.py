from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
data="C:\\bigdata\\datasets\\us-500.csv"

df=spark.read.format("csv").option("header","true").option("sep",",").load(data)
df.show(40,truncate=False)
