from pyspark import SparkContext
from pyspark import SparkConf
conf = SparkConf()
sc = SparkContext(conf=conf)

import re
from operator import add
from math import ceil
from math import pow
from math import sqrt

# get rid of non numbers, make form of ((i,j),mij) 
def prepare (st):
	st1 = re.split('[^0-9.]', st)
	st2 = filter (None, st1)
	return ((int(st2[0]),int(st2[1])), float(st2[2]))

# [(1, (0.02, 0)), (2, (0.04, 0)), (3, (0.03, 0)), (4, (None, 0))] -> [(1, 0.02), (2, 0.04), (3, 0.03), (4, 0)]
def putZeros(x):
	k = x[0]
	v = x[1]
	if v[0] == None:
		return (k, (c, v[1]))
	else:
		return (k, (b*v[0]+c, v[1]))

# from sys import getsizeof
# memSize = 8 * 1000000
# Get each element size to tcalculate the reducer size.
# mElement = ((1,1,1),(1,float(1)))
# vElement = ((1,1,1),float(1))
# MESize = getsizeof(mElement)
# VESize = getsizeof(vElement)
# reducersize = 2 n^2/g^2
# reducersize in byte = (MESize+VESize) * n^2/g^2  
# (MESize+VESize) * n^2/g^2 = memSize 
# g^2 = (MESize+VESize) * n^2 / memSize
#g = int(sqrt((MESize+VESize) * pow(n,2) / memSize))

# total number of pages
n = 366815
# number of groups
g = 8
# v' = bMv + (1-b)e/n
b = float(0.8)
# (1-b)/n
c = (1-b)/n
# number of multiplication for convergence
nmc = 50
# number that divide the i and j to decide the group. n/g needs to be round up
divider = int(ceil(float(n)/g))

ListOfDiff = []
count = 0

#rdd = sc.textFile('/user/u0343930/YelpProject/TransitionMatrix/test')
rdd1 = sc.textFile('/user/u0343930/YelpProject/TransitionMatrixUnmodified/part-00000')
rdd2 = sc.textFile('/user/u0343930/YelpProject/TransitionMatrixUnmodified/part-00001')
rdd = sc.union([rdd1,rdd2])

# matrix = ((i,j),mij)  * i is the user# that gets useful vote, j is the user# who gave useful vote, mij is 1/degree
matrix = rdd.map(prepare)

# ((i,j),mij) -> ((GroupI,GroupJ,j),(i,mij))
transitionMatrix = matrix.map(lambda (k,v): ((k[0]/divider,k[1]/divider,k[1]),(k[0],v))).cache()

#(i, 1/n)
vec = sc.parallelize(xrange(0,n)).map(lambda x: (x,float(1)/n)).cache()

for i in range(0,nmc):
	#((GroupI, GroupJ, j), vj)
	tempVec = vec.flatMap(lambda (k,v): map(lambda I: ((I,k/divider,k),v),range(0,g)))
	#((GroupI, GroupJ, j), ((i,mij),vj) ) -> (i, mij * vj) -> sum by key : (i, v'j) -> rightOuterJoin: (i, (v'j, vj)) -> map(putZeros): (i,(b*v'j+c, vj)), this also fill empty element with 0.
	tempVec = transitionMatrix.join(tempVec).map(lambda (k,v): (v[0][0],v[0][1]*v[1])).reduceByKey(add).rightOuterJoin(vec).map(putZeros)
	# calculate norm of difference between previous iteration. |x|=sqrt(x_1^2+x_2^2+...+x_n^2).
	diff = sqrt(tempVec.map(lambda (k,v): pow(v[0]-v[1],2)).sum())
	ListOfDiff.append(diff)
	# get rid of the previous vector
	vec.unpersist()
	# then replace with new one. -> (k, v)
	vec = tempVec.map(lambda (k,v): (k,v[0])).coalesce(8).cache()
	tempVec.unpersist()
	count += 1

vec.count()
vec.saveAsSequenceFile("/user/u0343930/YelpProject/data/vecUnModified")

def prepareUser (st):
	st1 = re.split('[(, \')]', st)
	st2 = filter (None, st1)
	return (int(st2[0]), st2[2])

user1 = sc.textFile('/user/u0343930/YelpProject/Iteration_userID/part-00000')
user2 = sc.textFile('/user/u0343930/YelpProject/Iteration_userID/part-00001')
user = sc.union([user1,user2]).map(prepareUser).coalesce(8).cache()
user.count()
userWeight = vec.join(user).map(lambda (k,v): (v[1],v[0])).coalesce(8).cache()
userWeight.count()

userWeight.saveAsSequenceFile("/user/u0343930/YelpProject/data/uwUnModified")

f = open('logUwUnmodified.txt', "a")
f.write("Diff = " + str(ListOfDiff) + "\n")
f.write("count = " + str(count) + "\n")
f.close()