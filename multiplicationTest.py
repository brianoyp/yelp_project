from pyspark import SparkContext
from pyspark import SparkConf
conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("Assignmet2")
conf.set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)
import re
from operator import add
from math import ceil
from math import pow
from math import sqrt
import numpy as np



#lambda (k,v): (k, b*v+c)

# better test

import random


n = 123
g = 4
# v' = bMv + (1-b)e/n
b = float(0.8)
# (1-b)/n
c = (1-b)/n

nmc = 40
d = float(1)/n
divider = int(ceil(float(n)/g))

# [(1, (0.02, 0)), (2, (0.04, 0)), (3, (0.03, 0)), (4, (None, 0))] -> [(1, 0.02), (2, 0.04), (3, 0.03), (4, 0)]
def putZeros(x):
	k = x[0]
	v = x[1]
	if v[0] == None:
		return (k, (c, v[1]))
	else:
		return (k, (b*v[0]+c, v[1]))


f = open("data/a.txt","w")
for i in range(0,n/2):
	for j in range(0,n):
		f.write(str(i)+"\t"+str(j)+"\t"+str(random.random())+"\n")

for i in range(n/2+1,n):
	for j in range(0,n):
		f.write(str(i)+"\t"+str(j)+"\t"+str(random.random())+"\n")

f.close()

f = open("data/b.txt","w")
for i in range(0,n):
	f.write(str(i)+"\t0\t"+str(d)+"\n")

f.close()



atext = sc.textFile("data/a.txt").map(lambda s: s.split("\t"))
matrix = atext.map(lambda st2: ((int(st2[0]),int(st2[1])), float(st2[2])))

# ((i,j),mij) -> ((GroupI,GroupJ,j),(i,mij))
transitionMatrix = matrix.map(lambda (k,v): ((k[0]/divider,k[1]/divider,k[1]),(k[0],v)))

#(i, 1/n)
vec = sc.parallelize(xrange(0,n)).map(lambda x: (x,float(1)/n)).cache()

#((GroupI, GroupJ, j), vj)
tempVec = vec.flatMap(lambda (k,v): map(lambda I: ((I,k/divider,k),v),range(0,g)))

#((GroupI, GroupJ, j), ((i,mij),vj) ) -> (i, mij * vj) -> sum by key : (i, v'j) -> rightOuterJoin: (i, (v'j, vj)) -> map(putZeros): (i,(b*v'j+c, vj)), this also fill empty element with 0.
tempVec = transitionMatrix.join(tempVec).map(lambda (k,v): (v[0][0],v[0][1]*v[1])).reduceByKey(add).rightOuterJoin(vec).map(putZeros)

# calculate difference between previous iteration.
diff = sqrt(tempVec.map(lambda (k,v): pow(v[0]-v[1],2)).sum())

# get rid of the previous vector
vec.unpersist()

# then replace with new one. -> (k, v)
vec = tempVec.map(lambda (k,v): (k,v[0])).cache()



def load_matrix(fname, sz):
	M = np.matrix(np.zeros(sz))
	with open(fname) as f:
		content = f.readlines()
		for l in content:
			s = l.split()
			M[int(s[0]), int(s[1])] = float(s[2])
	return M

A = load_matrix("data/a.txt", (n, n))
B = load_matrix("data/b.txt", (n, 1))

C = A*B

C = b*C + c

print C

#check if v and C is same!!!



#test
x = np.array([[1,0,0,0,0,0,0,0],[0,2,0,0,0,0,0,0],[0,0,3,0,0,0,0,0],[0,0,0,4,0,0,0,0],[0,0,0,0,5,0,0,0],[0,0,0,0,0,6,0,0],[0,0,0,0,0,0,7,0],[0,0,0,0,0,0,0,8]])
m = np.asmatrix(x)

y = np.array([[0.125],[0.125],[0.125],[0.125],[0.125],[0.125],[0.125],[0.125]])
vm = np.asmatrix(y)

m*vm

n=8
g=2
divider = int(ceil(float(n)/g))

test = sc.parallelize([((0, 0), 1), ((1, 1), 2), ((2, 2), 3), ((3, 3), 4), ((4, 4), 5), ((5, 5), 6), ((6, 6), 7), ((7, 7), 8)])
transitionMatrix = test.map(lambda (k,v): ((k[0]/divider,k[1]/divider,k[1]),(k[0],v)))
v = sc.parallelize(xrange(0,n)).map(lambda x: (x,float(1)/n))
v = v.flatMap(lambda (k,v): map(lambda I: ((I,k/divider,k),v),range(0,g)))
v = transitionMatrix.join(v).map(lambda (k,v): (v[0][0],v[0][1]*v[1])).reduceByKey(add)



vec = sc.parallelize(xrange(0,n)).map(lambda x: (x,float(1)/n))

diff = 3.0
b = float(0.8)
# (1-b)/n
c = (1-b)/n
while (diff > .1):
	tempV = vec.flatMap(lambda (k,v): map(lambda I: ((I,k/divider,k),v),range(0,g)))
	tempV = transitionMatrix.join(tempV).map(lambda (k,v): (v[0][0],v[0][1]*v[1])).reduceByKey(add).map(lambda (k,v): (k, b*v+c))
	diff = sqrt(tempV.join(vec).map(lambda (k,v): (pow(v[0]-v[1],2))).reduce(add))
	vec = tempV




x = np.array([[1,0,0,0,0,0,0,0,0],[0,2,0,0,0,0,0,0,0],[0,0,3,0,0,0,0,0,0],[0,0,0,4,0,0,0,0,0],[0,0,0,0,5,0,0,0,0],[0,0,0,0,0,6,0,0,0],[0,0,0,0,0,0,7,0,0],[0,0,0,0,0,0,0,8,0],[0,0,0,0,0,0,0,0,9]])
m = np.asmatrix(x)

y = np.array([[0.1111111111111111],[0.1111111111111111],[0.1111111111111111],[0.1111111111111111],[0.1111111111111111],[0.1111111111111111],[0.1111111111111111],[0.1111111111111111],[0.1111111111111111]])
vm = np.asmatrix(y)

m*vm



n=9
g=2
divider = int(ceil(float(n)/g))

test = sc.parallelize([((0, 0), 1), ((1, 1), 2), ((2, 2), 3), ((3, 3), 4), ((4, 4), 5), ((5, 5), 6), ((6, 6), 7), ((7, 7), 8), ((8, 8), 9)])
transitionMatrix = test.map(lambda (k,v): ((k[0]/divider,k[1]/divider,k[1]),(k[0],v)))
v = sc.parallelize(xrange(0,n)).map(lambda x: (x,float(1)/n))
v = v.flatMap(lambda (k,v): map(lambda I: ((I,k/divider,k),v),range(0,g)))
v = transitionMatrix.join(v).map(lambda (k,v): (v[0][0],v[0][1]*v[1])).reduceByKey(add)
