from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
#data="C:\\bigdata\\datasets\\bank-full.csv"
#reading data from hdfs storage
data="hdfs://localhost:9000/datasets/bank-full.csv"
df=spark.read.format("csv").option("header","true").option("sep",";").load(data)
df.createOrReplaceTempView("tab")
res=spark.sql("select * from tab where age > 60 and balance > 50000")
res.show()

#Terminal
#spark-submit --master local --deploy-mode client .\bankdata.py