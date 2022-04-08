from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F

sc = SparkContext("local","count least")
spk = SparkSession(sc)
reviews = sc.textFile("./dataset/review.csv").map(lambda line: line.split("::")).toDF(["rid", "id", "bid", "rate"]).distinct()
user = sc.textFile("./dataset/user.csv").map(lambda line: line.split("::")).toDF(["id", "name", "url"]).distinct()
percent = reviews.groupBy("id").count().join(user, "id")
percent = percent.withColumn("Count", F.col("Count")*100 / reviews.count())
res = percent.select("name", "Count").orderBy(["Count"], ascending=True).limit(10)
res=res.withColumnRenamed("Count","Contribution")
with open('output_q5.txt','w') as f:
    f.write(res.toPandas().to_string(index=False))
