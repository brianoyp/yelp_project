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
# If review data contains modified data
#review_data_modified = '' # use with BR1 and BR2
#review_data_modified = '_modified' # use with BR1_modified and BR2_modified
#review_data_modified = '_un_modified' 
#review_data_modified = '_one'
#review_data_modified = '_modified_one'
#review_data_modified = '_modified_uw_Mod'
#review_data_modified = '_uw_Mod'
#review_data_modified = '_modified_uw_unMod'
#review_data_modified = '_uw_unMod'
#review_data_modified = '_simple_one'
#review_data_modified = '_simple_modified_one'
#review_data_modified = '_simple_modified_uw_Mod'
#review_data_modified = '_simple_uw_Mod'
#review_data_modified = '_simple_modified_uw_unMod'
review_data_modified = '_simple_uw_unMod'
#review_score_data = yelp_data_home + "/review_score" + review_data_modified + "/*"
##review_score_data = yelp_data_home + "/secondEquationSeq/review_score" + review_data_modified + "/*"
#review_score_data = yelp_data_home + "/BR1" + review_data_modified + "/*"
review_score_data = yelp_data_home + "/BR2" + review_data_modified + "/*"
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
  if ((d['business_id'] == "-sV52FN-D-I808tyRPEvwg") and ('modified' in review_data_modified)):
    reviewCount = d['review_count'] + 100
  else:
    reviewCount = d['review_count']
  return ( d['business_id'], d['name'], d['categories'], d['stars'], d['city'], d['state'], reviewCount )

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
  
  #businessSearch = businessData \
  #  .filter(lambda x: filter_by_state('NV', x[5])) \
  #  .filter(lambda x: filter_by_city('Las Vegas', x[4])) \
  #  .filter(lambda x: filter_by_category('Restaurants', x[2])) \
  #  .filter(lambda x: x[3] <= 1) \
  #  .filter(lambda x: x[0] == "-sV52FN-D-I808tyRPEvwg") \
  #  .collect()
  #print businessSearch
  #sys.exit(1)

  doneFlag = False
  iteration = 0

  while(not doneFlag):
    if(iteration == 0):
      # BR1 unmodified user and review data
      # iteration 0
      review_data_modified = ''
      review_score_data = yelp_data_home + "/BR1" + review_data_modified + "/*"
    if(iteration == 1):
      # BR2 unmodified user and review data
      # iteration 0
      review_data_modified = ''
      review_score_data = yelp_data_home + "/BR2" + review_data_modified + "/*"
    if(iteration == 2):
      # BR1_modified spam farming
      # iteration 1
      review_data_modified = '_modified'
      review_score_data = yelp_data_home + "/BR1" + review_data_modified + "/*"
    if(iteration == 3):
      # BR2_modified spam farming
      # iteration 1
      review_data_modified = '_modified'
      review_score_data = yelp_data_home + "/BR2" + review_data_modified + "/*"
    if(iteration == 4):
      # BR1_unMod farming
      # ** users added but no useful votes
      # ** reviews added
      # iteration 2
      review_data_modified = '_un_modified'
      review_score_data = yelp_data_home + "/BR1" + review_data_modified + "/*"
    if(iteration == 5):
      # BR2_unMod farming
      # ** users added but no useful votes
      # ** reviews added
      # iteration 2
      review_data_modified = '_un_modified'
      review_score_data = yelp_data_home + "/BR2" + review_data_modified + "/*"
    if(iteration == 6):
      # review_score_one
      # iteration 0
      review_data_modified = '_one'
      review_score_data = yelp_data_home + "/review_score" + review_data_modified + "/*"
    if(iteration == 7):
      # review_score_modified_one
      # iteration 1
      review_data_modified = '_modified_one'
      review_score_data = yelp_data_home + "/review_score" + review_data_modified + "/*"
    if(iteration == 8):
      # review_score_modified_uw_Mod
      # iteration 2
      review_data_modified = '_modified_uw_Mod'
      review_score_data = yelp_data_home + "/review_score" + review_data_modified + "/*"
    if(iteration == 9):
      # review_score_uw_Mod
      # iteration 3
      review_data_modified = '_uw_Mod'
      review_score_data = yelp_data_home + "/review_score" + review_data_modified + "/*"
    if(iteration == 10):
      # review_score_modified_uw_unMod
      # iteration 4
      review_data_modified = '_modified_uw_unMod'
      review_score_data = yelp_data_home + "/review_score" + review_data_modified + "/*"
    if(iteration == 11):
      # review_score_uw_unMod
      # iteration 5
      review_data_modified = '_uw_unMod'
      review_score_data = yelp_data_home + "/review_score" + review_data_modified + "/*"
    if(iteration == 12):
      # review_score_simple_one
      # iteration 0
      review_data_modified = '_simple_one'
      review_score_data = yelp_data_home + "/review_score" + review_data_modified + "/*"
    if(iteration == 13):
      # review_score_simple_modified_one
      # iteration 1
      review_data_modified = '_simple_modified_one'
      review_score_data = yelp_data_home + "/review_score" + review_data_modified + "/*"
    if(iteration == 14):
      # review_score_simple_modified_uw_Mod
      # iteration 2
      review_data_modified = '_simple_modified_uw_Mod'
      review_score_data = yelp_data_home + "/review_score" + review_data_modified + "/*"
    if(iteration == 15):
      # review_score_simple_uw_Mod
      # iteration 3
      review_data_modified = '_simple_uw_Mod'
      review_score_data = yelp_data_home + "/review_score" + review_data_modified + "/*"
    if(iteration == 16):
      # review_score_simple_modified_uw_unMod
      # iteration 4
      review_data_modified = '_simple_modified_uw_unMod'
      review_score_data = yelp_data_home + "/review_score" + review_data_modified + "/*"
    if(iteration == 17):
      # review_score_simple_uw_unMod
      # iteration 5
      review_data_modified = '_simple_uw_unMod'
      review_score_data = yelp_data_home + "/review_score" + review_data_modified + "/*"
    if(iteration == 18):
      # review_score_simple_modified_uw_Mod_NEW
      # iteration 6
      review_data_modified = '_simple_modified_uw_Mod_NEW'
      review_score_data = yelp_data_home + "/review_score" + review_data_modified + "/*"
    if(iteration == 19):
      # review_score_modified_uw_Mod_NEW
      # iteration 6
      review_data_modified = '_modified_uw_Mod_NEW'
      review_score_data = yelp_data_home + "/review_score" + review_data_modified + "/*"
      doneFlag = True

    businessData = sc.textFile(business_data).map(lambda x: clean_business(x))
    businessCount = businessData.count()

    reviewScore = sc.sequenceFile(review_score_data)
    reviewScoreCount = reviewScore.count()

    def print_business(data):
      print data

    # Filter Data By City/State/Category
    business = businessData \
      .filter(lambda x: filter_by_city('Las Vegas', x[4])) \
      .filter(lambda x: filter_by_state('NV', x[5])) \
      .filter(lambda x: filter_by_category('Restaurants', x[2])) \
      .map(lambda x: (x[0], x[1])) \
      .join(reviewScore) \
      .sortBy(lambda (businessID, data): data[1], False) \
      .map(lambda (businessID, data): (businessID, data[0])) \
      .zipWithIndex() \
      .filter(lambda (data, rank): data[0] == "-sV52FN-D-I808tyRPEvwg") \
      .collect()
      #.foreach(lambda x: print_business(x)) 

    f = open(yelp_data_home + "/output_search.out", "a")
    output = ""
    output = output + "#########################################################################" + "\n"
    output = output + "                   Top 5 Business Given By Algorithm                     " + "\n"
    output = output + str(business) + "\n"
    output = output + review_score_data + "\n"
    output = output + "#########################################################################" + "\n"
    output = output + "#########################################################################" + "\n"
    output = output + "   BusinessCount =       " + str(businessCount) + "\n"
    output = output + "   ReviewScoreCount =    " + str(reviewScoreCount) + "\n"
    output = output + "#########################################################################" + "\n"
    output = output + "iteration = " + str(iteration) + "   " + str(datetime.today()) + "\n"
    f.write(output)
    f.close()
    print output
    iteration = iteration + 1

  print "DONE"  
  
