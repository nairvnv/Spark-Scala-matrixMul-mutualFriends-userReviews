from pyspark import SparkContext

def mutual(record):
    id = record[0]
    friends = record[1].split(",")
    mutuals = []
    for friend in friends:
        if friend != '' and friend != id:
            if int(id) >= int(friend):
                pair = (friend + "," + id, set(friends)) 
            else:
                pair = (id + "," + friend, set(friends)) 
            mutuals.append(pair)
    return mutuals

if __name__ == "__main__":
    sc = SparkContext.getOrCreate();
    data = sc.textFile("./dataset/mutual.txt")
    users = data.map(lambda record: record.split("\t")).filter(lambda record: len(record) == 2).flatMap(mutual) 
    mf = users.reduceByKey(lambda x, y: x.intersection(y)) 
    mf.map(lambda x:"{0}\t{1}".format(x[0],len(x[1]))).sortBy(lambda x: x.split("\t")[0]).sortBy(lambda x: x.split("\t")[1]).saveAsTextFile("output_mutual")
