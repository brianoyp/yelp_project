from pyspark import SparkContext
from pyspark import SparkConf
conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("Assignmet2")
conf.set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)
import json

def getUser(str):
	parsed = json.loads(str)
	return (parsed['user_id'], parsed['votes']['useful'])

#rdd = sc.textFile('data/user.json')
rdd = sc.textFiles('/user/u0343930/YelpProject/user.json')

users = rdd.map(getUser).zipWithIndex().cache()
#users = ((user_id,#useful), index)

n = users.count()

userInfo = users.map(lambda (k,v): (v, k[0]))
#userInfo = (index, user_id)

userInfo.saveAsTextFile('/user/u0343930/YelpProject/Iteration_userID')

import random

# users = ((user_id,#useful), index)
# flatMap : ((user_id,#useful), index) => generate [#useful] of ((idex, randomIndex),1)
# reduceByKey: remove duplicates. unique ((i, j), 1)
# ((i,j),1) -> (j,([i],1)), reduceByKey: (j, ([list of i], degree))
# -> flatMap: many ((i,j), 1/degree)
matrix = users.flatMap(lambda (k,v): map(lambda I: ((v,random.randint(0,n-1)),1), range(0,k[1]))).reduceByKey(lambda a,b: 1)\
.map(lambda (k,v): (k[1],([k[0]],1))).reduceByKey(lambda (a1,b1),(a2,b2): (a1+a2,b1+b2)) 
.flatMap(lambda (k,v): map(lambda i: ((i,k),float(1)/v[1]), v[0]))  #-> flatMap: many ((i,j), 1/degree)


matrix.saveAsTextFile('/user/u0343930/YelpProject/TransitionMatrixUnmodified')
