from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
data="C:\\bigdata\\datasets\\sales.csv"
df=spark.read.format("csv").option("header","true").load(data)
df.createOrReplaceTempView("tab")
res = spark.sql("select * from tab")
res.show()