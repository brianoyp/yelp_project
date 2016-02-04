f = open("data/yelp_academic_dataset_review.json", "a")

f.write('{"votes": {"funny": 0, "useful": 99, "cool": 0}, "user_id": "farmerMain", "review_id": "farmerMainReview", "stars": 5, "date": "2015-01-02", "text": "", "type": "review", "business_id": "-sV52FN-D-I808tyRPEvwg"}\n')


for i in range(1,100):
	f.write('{"votes": {"funny": 0, "useful": 1, "cool": 0}, "user_id": "farmer'+ str(i) + '", "review_id": "farmerReview' + str(i) +'", "stars": 5, "date": "2015-01-02", "text": "", "type": "review", "business_id": "-sV52FN-D-I808tyRPEvwg"}\n')

f.close()