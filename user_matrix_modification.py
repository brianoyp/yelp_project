
f = open("data/Iteration_userID/part-00001", "a")
f.write("(366715, u'farmerMain')\n")
for i in range(1, 100):
	f.write("(" + str(i+366715) +", u'famer" + str(i) + "')\n")

f.close()


f = open("data/TransitionMatrixUnmodified/part-00001", "a")
for i in range(366716, 366815):
	f.write("((" + str(i) +", 366715), 0.010101010101010102)\n")

f.close()


f = open("data/TransitionMatrixUnmodified/part-00001", "a")
for i in range(366716, 366815):
	f.write("((366715, " + str(i) +"), 1.0)\n")

f.close()