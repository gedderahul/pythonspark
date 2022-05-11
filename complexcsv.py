from pyspark.sql import *
from pyspark.sql.functions import *
import re
spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
data = "C:\\bigdata\\datasets\\10000Records.csv"
df = spark.read.format("csv").option("header", "true").load(data)
# data cleaning (header)
ndf = df.toDF(*(re.sub('[^a-zA-Z]', "", c) for c in df.columns))
# ndf=df.withColumnRenamed("E Mail","EMail")
ndf.createOrReplaceTempView("tab")
# res = spark.sql("select * from tab where WeightinKgs >= 80 and EMail like '%gmail%'")
# res=ndf.where(col("Gender")=='M')
res = ndf.where((col("WeightinKgs") >= 90) & (col("EMail")).like("%gmail%") & (col("YearofJoining") >= 2015))
res.show()
op = "C:\\bigdata\\output\\processesdata"
res.write.format("csv").option("header","true").save(op)

