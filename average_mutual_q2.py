from pyspark import SparkContext

def mutual(record):
    user = record[0].strip()
    friends = record[1]
    if user != '':  
        mutuals = []
        for friend in friends:
            friend = friend.strip()
            if friend != '':
                if float(friend) >= float(user):
                    user_friend = (user + "," + friend, set(friends))
                else:
                    user_friend = (friend + "," + user, set(friends))
                mutuals.append(user_friend)
        return mutuals

if __name__ == "__main__":
    sc = SparkContext.getOrCreate();
    frd = sc.textFile("./dataset/mutual.txt").map(lambda x: x.split("\t")).filter(lambda x: len(x) == 2).map(lambda x: [x[0], x[1].split(",")])
    mf = frd.map(lambda x: x.split("\t")).filter(lambda x: len(x) == 2).map(lambda x: [x[0], x[1].split(",")]).flatMap(mutual)
    mutual_friends = mf.reduceByKey(lambda x,y: len(x.intersection(y)))
    total = mutual_friends.map(lambda x: (x[0],x[1])).sortByKey(False)
    avg_value = mutual_friends.map(lambda x : x[1]).sum()/total.count()
    mutual_friends.filter(lambda x: x[1] < avg_value).map(lambda x:"{0}\t{1}".format(x[0],x[1])).saveAsTextFile("output_avg_q2")