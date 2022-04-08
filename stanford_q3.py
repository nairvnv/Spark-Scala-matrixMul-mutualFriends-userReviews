
import pyspark
from pyspark import *
from pyspark.conf import *
from pyspark.sql import *

sc = SparkContext(master="local", appName="stanford")

users = sc.textFile("./dataset/user.csv").map(lambda x: x.split('::')).map(lambda x: (x[0], tuple(x[1:])))
business = sc.textFile("./dataset/business.csv").map(lambda x: x.split('::')).map(lambda x: (x[0], tuple(x[1:])))
reviews = sc.textFile("./dataset/review.csv").map(lambda x: x.split('::')).map(lambda x: (str(x[2]), (str(x[1]), float(x[3]))))
stanford_bs = business.filter(lambda x: 'Stanford' in x[1][0])

combined = stanford_bs.join(reviews).map(lambda x: x[1][1]).distinct()
final =  combined.join(users).map(lambda x: (x[1][1][0], x[1][0])).distinct()
sc.parallelize(final.collect()).saveAsTextFile('output_stanford_q3')
