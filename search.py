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
business_data = yelp_data_home + "/yelp_academic_dataset_business.json"
review_score_data = yelp_data_home + "/review_score/*"
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

def filter_by_category(category, categories):
  # expects list in categories
  # category is the filter by criteria
  contains = False;
  for item in categories:
    if item == category:
      contains = True
  return contains

def filter_by_state(desired_state, state):
  # expects string
  if(desired_state == state):
    return True
  else:
    return False

def filter_by_city(desired_city, city):
  # expects string
  if(desired_city == city):
    return True
  else:
    return False

def get_data(text):
  # Expected:
  # text = "(u'18kPq7GPye-YQ3LyKyAZPw', -01.00001)"
  userID = ''
  value = 0

  if text:
    list1 = re.split(',', text)
    str1 = list1[0]
    str2 = list1[1]

    match1 = re.search(r"(\'.*\')", str1)
    if match1:
      userID = match1.group(0).replace('\'','')
    
    match2 = re.search(r"(-{0,1}\d*\.{0,1}\d+)", str2)
    if match2:
      value = float(match2.group(0))
  return (userID, value)
    

if __name__ == '__main__':

  conf = SparkConf()
  conf.setMaster("local[4]")

  sc = SparkContext("local", "Simple App")

  print "START"
  businessData = sc.textFile(business_data).map(lambda x: clean_business(x))
  businessCount = businessData.count()

  businessSearch = businessData \
    .filter(lambda x: filter_by_state('NV', x[5])) \
    .filter(lambda x: filter_by_city('Las Vegas', x[4])) \
    .filter(lambda x: filter_by_category('Restaurants', x[2])) \
    .filter(lambda x: x[3] <= 1) \
    .collect()
  print businessSearch
  sys.exit(1)

  reviewScore = sc.wholeTextFiles(review_score_data) \
    .flatMap(lambda x: x[1].split("\n")) \
    .map(lambda x: get_data(x))
  reviewScoreCount = reviewScore.count()

  # Filter Data By City/State/Category
  business = businessData \
    .filter(lambda x: filter_by_state('PA', x[5])) \
    .filter(lambda x: filter_by_category('Restaurants', x[2])) \
    .map(lambda x: (x[0], x[1])) \
    .join(reviewScore) \
    .sortBy(lambda (businessID, data): data[1], False) \
    .map(lambda (businessID, data): data[0]) \
    .take(5)
  #  .filter(lambda x: filter_by_city('Las Vegas', x[4])) \
  print "#########################################################################"
  print "                   Top 5 Business Given By Algorithm                     "
  print business
  print "#########################################################################"
  
  print "#########################################################################"
  print ("   BusinessCount =       " + str(businessCount))
  print ("   ReviewScoreCount =    " + str(reviewScoreCount))
  print "#########################################################################"

  print "DONE"  
  
