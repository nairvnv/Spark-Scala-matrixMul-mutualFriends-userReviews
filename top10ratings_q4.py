import pyspark
from pyspark import *
from pyspark.conf import *
from pyspark.sql import *

sc = SparkContext(master="local", appName="top ratings")
business = sc.textFile("./dataset/business.csv").map(lambda x: x.split('::')).map(lambda x: (x[0], (str(x[1]),str(x[2])))).distinct()
review = sc.textFile("./dataset/review.csv").map(lambda x: x.split('::')).map(lambda x: (str(x[2]), (str(x[1]), float(x[3])))).distinct()

def avg(inter):
    ratings = []
    for val in inter:
        for rate in val:
            rating.append(rate)
    return len(ratings)

result = review.groupByKey().mapValues(avg).collect()[:]
rating = sc.parallelize(result)
res = rating.join(business).map(lambda x: (x[1][0], (x[1][1][0], x[1][1][1], x[0]))).sortByKey(False).top(10)
result = sc.parallelize(res)
result.map(lambda x:"{0}\t {1}\t\t {2}\t\t\t {3}".format(x[1][2],x[1][0],x[1][1],x[0])).saveAsTextFile('output_top10_q4')