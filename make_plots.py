import matplotlib.pyplot as plt
import pickle

with open("pickles/degree_distr.pickle", "rb") as f:
    adj_list = pickle.load(f)

degree_distr = sorted([len(x.keys()) for x in adj_list.values()])
#plt.hist(degree_distr, len(degree_distr))
plt.plot(degree_distr)
plt.show()

