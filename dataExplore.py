# Derek Nelson
# u0535139

import os
import sys

# Constants
#############################################################################################################
#  ACTUAL DATA:
spark_home = os.environ.get('SPARK_HOME', None)
yelp_data_home = "/home/derekn/CS6965/yelp_dataset_challenge_academic_dataset"
business_data = yelp_data_home + "/yelp_academic_dataset_business.json"
# If review data contains modified data
#review_data_modified = ''
#review_data_modified = '_modified'
#review_data_modified = '_modified_userWeight'
#review_data_modified = '_userWeight'
#review_data = yelp_data_home + "/yelp_academic_dataset_review.json"
#review_data = yelp_data_home + "/yelp_academic_dataset_review_modified.json"
# used from TEST DATA
#user_data = "/home/derekn/CS6965/yelp_dataset_challenge_academic_dataset/dummy/user_data/*"
#user_data = yelp_data_home + "/userWeight/*"
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
try: # import DATETIME
  from datetime import datetime
  print("Successfully imported DATETIME Modules")
except ImportError as e:
  print ("Cannot import DATETIME Modules", e)
  sys.exit(1)
try: # import OPERATOR/REGEX
  from operator import add
  print("Successfully imported OPERATOR Modules")
  import re
  print("Successfully imported REGEX Modules")
except ImportError as e:
  print ("Cannot import OPERATOR/REGEX Modules", e)
  sys.exit(1)

def clean_business(data):
  # data = 
  #   {
  #     "business_id": "vcN...",
  #     "full_address": "484...",
  #     "hours": {"Tue...},
  #     "open": true,
  #     "categories": ["Doctors", "Health & Medical"],
  #     "city": "Phoenix",
  #     "review_count": 9,
  #     "name": "Eric Goldberg, MD",
  #     "neighborhoods": [],
  #     "longitude": -111.98375799999999,
  #     "state": "AZ",
  #     "stars": 3.5,
  #     "latitude": 33.499313000000001,
  #     "attributes": {"By Appointment Only": true},
  #     "type": "business"
  #   }
  # returns =
  #   ( business_id, name, categories, stars, city, state, review_count )
  # Access By:
  #   x[0] = business_id
  d = json.loads(data)
  return ( d['business_id'], d['name'], d['categories'], d['stars'], d['city'], d['state'], d['review_count'] )

if __name__ == '__main__':

  conf = SparkConf()
  conf.setMaster("local[4]")

  sc = SparkContext("local", "Simple App")

  print "START"

  businessData = sc.textFile(business_data).map(lambda x: clean_business(x))

  # States
  states = businessData.map(lambda x: (x[5], 1) )\
    .reduceByKey(add) \
    .collect()
  print "####### States"
  print states

  maxReviews = businessData.map(lambda x: x[6]).max()
  minReviews = businessData.map(lambda x: x[6]).min()
  print " ### MAX REVIEWS"
  print maxReviews
  print " ### MIN REVIEWS"
  print minReviews

  print "Done"
