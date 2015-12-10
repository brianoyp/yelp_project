# Derek Nelson
# u0535139

import os
import sys

# Constants
#############################################################################################################
##  TEST DATA:
#spark_home = os.environ.get('SPARK_HOME', None)
#yelp_data_home = "/home/derekn/CS6965/yelp_dataset_challenge_academic_dataset/dummy"
#business_data = yelp_data_home + "/business.json"
#review_score_data = yelp_data_home + "/review_score/*"
#############################################################################################################
#  ACTUAL DATA:
spark_home = os.environ.get('SPARK_HOME', None)
yelp_data_home = "/home/derekn/CS6965/yelp_dataset_challenge_academic_dataset"
#############################################################################################################

# Establish Environment for PYSPARK
if not spark_home:
  raise ValueError('SPARK_HOME environment variable not set')
sys.path.insert(0, os.path.join(spark_home, 'python'))
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.8.2.1-src.zip'))
sys.path.insert(0, os.path.join(spark_home, 'wiki'))
try: # import PYSPARK
  from pyspark import SparkContext
  from pyspark import SparkConf
  print("Successfully imported Spark Modules")
except ImportError as e:
  print ("Cannot import Spark Modules", e)
  sys.exit(1)
try: # import JSON
  import json
  print("Successfully imported JSON Modules")
except ImportError as e:
  print ("Cannot import JSON Modules", e)
  sys.exit(1)
try: # import REGEX
  import re
  print("Successfully imported REGEX Modules")
except ImportError as e:
  print ("Cannot import REGEX Modules", e)
  sys.exit(1)

if __name__ == '__main__':

  conf = SparkConf()
  conf.setMaster("local[4]")

  sc = SparkContext("local", "Simple App")

  print "START"
  
  print not False
  #userData = sc.sequenceFile(yelp_data_home + "/uwFullModified/*")
  #print userData.take(5)    

  print "DONE"  
  
