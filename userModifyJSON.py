f = open("/home/derekn/CS6965/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_user_un_modified.json", "a")

# Using useful = 99 for master and useful = 1 for others is spam farm
# Using useful = 0 is other method
f.write('{"yelping_since": "2015-12", "votes": {"funny": 0, "useful": 0, "cool": 0}, "review_count": 1, "name": "Farmer", "user_id": "farmerMain", "friends": [], "fans": 0, "average_stars": 5, "type": "user", "compliments": {"profile": 0, "cute": 0, "funny": 0, "plain": 0, "writer": 0, "note": 0, "photos": 0, "hot": 0, "cool": 0, "more": 0}, "elite": [2015]}\n')

for i in range(1,100):
	f.write('{"yelping_since": "2015-12", "votes": {"funny": 0, "useful": 0, "cool": 0}, "review_count": 1, "name": "Farmer", "user_id": "farmer' + str(i) + '", "friends": [], "fans": 0, "average_stars": 5, "type": "user", "compliments": {"profile": 0, "cute": 0, "funny": 0, "plain": 0, "writer": 0, "note": 0, "photos": 0, "hot": 0, "cool": 0, "more": 0}, "elite": [2015]}\n')

f.close()
