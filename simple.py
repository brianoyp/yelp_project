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
#review_data = yelp_data_home + "/review.json"
## Used to generate user data for testing
##user_data = "/home/derekn/CS6965/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_user.json"
#user_data = yelp_data_home + "/user_data/*"
#############################################################################################################
#  ACTUAL DATA:
spark_home = os.environ.get('SPARK_HOME', None)
yelp_data_home = "/home/derekn/CS6965/yelp_dataset_challenge_academic_dataset"
business_data = yelp_data_home + "/yelp_academic_dataset_business.json"
###### This is the variables that will be set ######
# If review data contains modified data
#review_data_modified = ''
#review_data_modified = '_modified'
#review_data_modified = '_modified_userWeight'
#review_data_modified = '_userWeight'
#review_data = yelp_data_home + "/yelp_academic_dataset_review.json"
#review_data = yelp_data_home + "/yelp_academic_dataset_review_modified.json"
# used from TEST DATA
#user_data = "/home/derekn/CS6965/yelp_dataset_challenge_academic_dataset/dummy/user_data/*"
#user_data = yelp_data_home + "/dummy/user_data/*"
#user_data = yelp_data_home + "/dummy/user_data_mod/*"
#user_data = yelp_data_home + "/userWeight/*"
#user_data = yelp_data_home + "/uwFullModified/*"
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
  from math import log
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
  if ((d['business_id'] == "-sV52FN-D-I808tyRPEvwg") and ('modified' in review_data_modified)):
    reviewCount = d['review_count'] + 100
  else:
    reviewCount = d['review_count']
  return ( d['business_id'], d['name'], d['categories'], d['stars'], d['city'], d['state'], reviewCount )

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

def modifiedReviewScore(userWeight, reviewScore):
  # expects a number from 1-5 (stars)
  return (reviewScore - 3) * float(userWeight)

def depreciationRate(reviewDate):
  # expects datetime
  # Depreciation Constant = 0.0001
  ageOfReview = (datetime.today() - datetime.strptime(reviewDate,"%Y-%m-%d")).days
  rate = 1 - (ageOfReview * 0.0001)
  return rate if (rate > 0) else 0

def advantageRate(reviewCount):
  # expects a number (total count of reviews for that business)
  # Advantage Constant = 0.0001
  return 1 + log(reviewCount)

def individualReviewScore(MRS, total_number_reviews):
  # modified_review_score = modifiedReviewScore
  # depreciation_rate = depreciationRate
  # total_number_reviews = total number of reviews for that business
  # advantage_rate = advantageRate
  return float(MRS) / total_number_reviews

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
  #print (userID, value)
  #sys.exit(1)
  return (userID, value)
    

if __name__ == '__main__':

  conf = SparkConf()
  conf.setMaster("local[4]")

  sc = SparkContext("local", "Simple App")

  # UserData Needs to be processed with PageRank Method before this
  #  Location can be set at the top of this script
  # The commented out code generated test data for this.
  #userData = sc.textFile(user_data) \
  #  .map(lambda x: clean_user(x)) \
  #  .saveAsTextFile("/home/derekn/CS6965/yelp_dataset_challenge_academic_dataset/user_data")
  #sys.exit(1)

  print "START"
  doneFlag = False
  iteration = 0

  while(not doneFlag):
    if(iteration == 0):
      review_data_modified = '_simple_one'
      review_data = yelp_data_home + "/yelp_academic_dataset_review.json"
      user_data = yelp_data_home + "/dummy/user_data/*" # used from TEST DATA
    if(iteration == 1):
      review_data_modified = '_simple_modified_one'
      review_data = yelp_data_home + "/yelp_academic_dataset_review_modified.json"
      user_data = yelp_data_home + "/dummy/user_data_mod/*" # used from TEST DATA
    if(iteration == 2):
      review_data_modified = '_simple_modified_uw_Mod'
      review_data = yelp_data_home + "/yelp_academic_dataset_review_modified.json"
      user_data = yelp_data_home + "/uwModified/*"
    if(iteration == 3):
      review_data_modified = '_simple_uw_Mod'
      review_data = yelp_data_home + "/yelp_academic_dataset_review.json"
      user_data = yelp_data_home + "/uwModified/*"
    if(iteration == 4):
      review_data_modified = '_simple_modified_uw_unMod'
      review_data = yelp_data_home + "/yelp_academic_dataset_review_modified.json"
      user_data = yelp_data_home + "/uwUnModified/*"
    if(iteration == 5):
      review_data_modified = '_simple_uw_unMod'
      review_data = yelp_data_home + "/yelp_academic_dataset_review.json"
      user_data = yelp_data_home + "/uwUnModified/*"
    if(iteration == 6):
      review_data_modified = '_simple_modified_uw_Mod_NEW'
      review_data = yelp_data_home + "/yelp_academic_dataset_review_modified.json"
      user_data = yelp_data_home + "/uwModified_NEW/*"
      doneFlag = True

    if 'uw' in review_data_modified:
      userData = sc.sequenceFile(user_data)
      userCount = userData.count()
    else:
      userData = sc.wholeTextFiles(user_data) \
        .map(lambda x: x[1]) \
        .flatMap(lambda x: x.split("\n")) \
        .map(lambda x: get_data(x))
      userCount = userData.count()
    
    businessData = sc.textFile(business_data).map(lambda x: clean_business(x))
    businessInfo = businessData.map(lambda x: (x[0], (x[6], advantageRate(x[6]))))
    businessCount = businessInfo.count()

    reviewData = sc.textFile(review_data).map(lambda x: clean_review(x))
    reviewUserData = reviewData.map(lambda x: (x[1], (x[2], x[3], x[4])) ).join(userData)

    MRS = reviewUserData \
      .map(lambda (userId, data): (data[0][2], modifiedReviewScore(data[1], data[0][0])) ) \
      .reduceByKey(add)
    MRScount = MRS.count()
    MRSandBI = MRS.join(businessInfo)
    MRSandBIcount = MRSandBI.count()
    reviewScore = MRSandBI \
      .map(lambda (businessID, data): (businessID, individualReviewScore(data[0], data[1][0])))
    reviewScoreCount = reviewScore.count()
  
    #print "#########################################################################"
    #print reviewScore.collect()
    #print "#########################################################################"

    reviewBusinessData = reviewData.map(lambda x: (x[4], (x[1], x[2], x[3])) ).join(businessInfo)
    reviewCount = reviewData.count()
    reviewBusinessCount = reviewBusinessData.count()
    reviewUserCount = reviewUserData.count()

    # Store Review Score to Disk after run
    reviewScore.saveAsSequenceFile(yelp_data_home + "/review_score" + review_data_modified)
    
    f = open(yelp_data_home + "/output_simple.out", "a")
    output = ""
    output = output + "#########################################################################" + "\n"
    output = output + "   BusinessCount =       " + str(businessCount) + "\n"
    output = output + "   ReviewCount =         " + str(reviewCount) + "\n"
    output = output + "   UserCount =           " + str(userCount) + "\n"
    output = output + "   ReviewBusinessCount = " + str(reviewBusinessCount) + "\n"
    output = output + "   ReviewUserCount =     " + str(reviewUserCount) + "\n"
    output = output + "   MRScount =            " + str(MRScount) + "\n"
    output = output + "   MRSandBIcount =       " + str(MRSandBIcount) + "\n"
    output = output + "   ReviewScoreCount =    " + str(reviewScoreCount) + "\n"
    output = output + "#########################################################################" + "\n"
    output = output + "iteration = " + str(iteration) + "\n"
    f.write(output)
    f.close()
    print output
    iteration = iteration + 1

  print "DONE"  
  
