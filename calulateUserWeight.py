
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

# get rid of non numbers, make form of ((i,j),mij) 
def prepare (st):
	st1 = re.split('[^0-9]', st)
	st2 = filter (None, st1)
	return ((int(st2[0]),int(st2[1])), float(st2[2]))

rdd = sc.textFile('/user/u0343930/YelpProject/TransitionMatrix/test')



# matrix = ((i,j),mij)  * i is the user# that gets useful vote, j is the user# who gave useful vote, mij is 1/degree
matrix = rdd.map(prepare)

# total number of pages
n = 366815
# number of groups
g = 8
# v' = bMv + (1-b)e/n
b = float(0.8)
# (1-b)/n
c = (1-b)/n
# number of multiplication for convergence
nmc = 40
# number that divide the i and j to decide the group. n/g needs to be round up
divider = int(ceil(float(n)/g))

# ((i,j),mij) -> ((GroupI,GroupJ,j),(i,mij))
transitionMatrix = matrix.map(lambda (k,v): ((k[0]/divider,k[1]/divider,k[1]),(k[0],v)))

# initial v -> (j, 1/n)
vec = sc.parallelize(xrange(0,n)).map(lambda x: (x,float(1)/n))

for i in range(0,nmc):
	vec = v.flatMap(lambda (k,v): map(lambda I: ((I,k/divider,k),v),range(0,g)))
	vec = transitionMatrix.join(v).map(lambda (k,v): (v[0][0],v[0][1]*v[1])).reduceByKey(add).map(lambda (k,v): (k, b*v+c))

diff = 1

while (diff > .000000001)
	tempV = vec.flatMap(lambda (k,v): map(lambda I: ((I,k/divider,k),v),range(0,g)))
	tempV = transitionMatrix.join(tempV).map(lambda (k,v): (v[0][0],v[0][1]*v[1])).reduceByKey(add).map(lambda (k,v): (k, b*v+c))
	diff = sqrt(tempV.join(vec).map(lambda (k,v): (k,pow(v[0]-v[1],2))).reduce(lambda a,b: a[1]+b[1]))
	vec = tempV
# |x|_2=|x|=sqrt(x_1^2+x_2^2+...+x_n^2).
# In practice, for the Web itself, 50–75 iterations are sufficient to converge to within the error limits of double-precision arithmetic.

vec.sortBy(lambda (k,v):k).map(lambda (k,v): str(k)+" "+str(v)).saveAsTextFile('data/UserWeight')
#v.sortBy(lambda (k,v):k).map(lambda (k,v): str(k)+" "+str(v)).saveAsTextFile('/user/u0343930/YelpProject/UserWeight')

