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
review_data = yelp_data_home + "/yelp_academic_dataset_review.json"
#review_data = yelp_data_home + "/yelp_academic_dataset_review_modified.json"
# used from TEST DATA
#user_data = "/home/derekn/CS6965/yelp_dataset_challenge_academic_dataset/dummy/user_data/*"
#user_data = yelp_data_home + "/userWeight/*"
user_data = yelp_data_home + "/yelp_academic_dataset_user.json"
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
  from pylab import *
  from optparse import OptionParser
  import matplotlib as mpl
  mpl.use('Agg')
  import matplotlib.pyplot as plt
  import matplotlib.image as mpimg
  print("Successfully imported Matplotlib")
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

def clean_review(data):
  # data =
  #   {
  #     "votes": {"fun...},
  #     "user_id": "Xqd...",
  #     "review_id": "15S...",
  #     "stars": 5,
  #     "date": "2007-05-17",
  #     "text": "dr. gol...",
  #     "type": "review",
  #     "business_id": "vcN..."
  #   }
  # returns = 
  #   ( review_id, user_id, stars, date, business_id )
  # Access By:
  #   x[0] = review_id
  d = json.loads(data)
  return ( d['review_id'], d['user_id'], d['stars'], d['date'], d['business_id'] )

def clean_user(data):
  # data =
  #   {
  #     "yelping_since": "2004-10",
  #     "votes": {"fun...},
  #     "review_count": 108,
  #     "name": "Russel",
  #     "user_id": "18k...",
  #     "friends": ["rpO..., ...T4g"],
  #     "fans": 69,
  #     "average_stars": 4.1399999999999997,
  #     "type": "user",
  #     "compliments": {"pro...},
  #     "elite": [2005, 2006]
  #   }
  # returns =
  #   ( user_id, 1 )
  # Access By:
  #   x[0] = user_id
  d = json.loads(data)
  return ( d['user_id'], 1 )

if __name__ == '__main__':

  conf = SparkConf()
  conf.setMaster("local[4]")

  sc = SparkContext("local", "Simple App")

  print "START"

  businessData = sc.textFile(business_data).map(lambda x: clean_business(x))
  reviewData = sc.textFile(review_data).map(lambda x: clean_review(x))
  userData = sc.textFile(user_data).map(lambda x: clean_user(x))

  # States
  #states = businessData.map(lambda x: (x[5], 1) )\
  #  .reduceByKey(add)
  #stateKey = states.keys()
  #stateValue = states.values()
  #states.collect()
  #print "####### States"
  #print states

  #cities = businessData.map(lambda x: x[4] + ", " + x[5]).distinct()
  #print "####### Cities"
  #print cities.count()

  #maxReviews = businessData.map(lambda x: x[6]).max()
  #minReviews = businessData.map(lambda x: x[6]).min()
  #print " ### MAX REVIEWS"
  #print maxReviews
  #print " ### MIN REVIEWS"
  #print minReviews

  businessCount = businessData.map(lambda x: x[0]).distinct().count()
  reviewCount = reviewData.map(lambda x: x[0]).distinct().count()
  userCount = userData.map(lambda x: x[0]).distinct().count()

  print "###### Business Count"
  print businessCount
  print "###### Review Count"
  print reviewCount
  print "###### User Count"
  print userCount


  print "Done"
