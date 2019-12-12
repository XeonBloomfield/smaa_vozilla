from sklearn.neighbors import KNeighborsClassifier
import numpy as np
import matplotlib.pyplot as plt

n_nums = list(range(5, 100, 5))
d = dict()

def load_file_into_array(file_path, skip_header=False):
    entries = np.genfromtxt(file_path, delimiter=",", skip_header=skip_header)
    print(file_path, entries.shape)
    return entries


for n in n_nums:
    neigh = KNeighborsClassifier(n_neighbors=n)

    trainX = load_file_into_array('data/xtrain.csv', skip_header=True)
    trainY = load_file_into_array('data/ytrain.csv')

    validX = load_file_into_array('data/xvalid.csv', skip_header=True)
    validY = load_file_into_array('data/yvalid.csv')

    neigh.fit(trainX, trainY)

    res = list()
    errors = list()
    for i in range(len(validX)):
        prediction = int(neigh.predict([validX[i]])[0])
        res.append(prediction)
        errors.append(prediction - validY[i])

    d[n] = sum(errors)/len(errors)

l = list()
for n in n_nums:
    print('k: ', n, '\n', d[n])
    l.append(d[n])


plt.plot(n_nums, l)
plt.show()


