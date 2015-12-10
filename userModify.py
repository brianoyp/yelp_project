f = open("/home/derekn/CS6965/yelp_dataset_challenge_academic_dataset/dummy/user_data/part-00005", "a")

f.write("(u'farmerMain', 1)\n")

for i in range(1,100):
	f.write("(u'farmer" + str(i) + "', 1)\n")

f.close()