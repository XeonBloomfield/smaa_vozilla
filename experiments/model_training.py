import datetime

import numpy as np
import math

from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.layers import Dropout
from keras.callbacks import TensorBoard


#
# Loading data
#

def load_file_into_array(file_path, skip_header=False):
    entries = np.genfromtxt(file_path, delimiter=",", skip_header=skip_header)
    print(file_path, entries.shape)
    return entries


trainX = load_file_into_array('data/xtrain.csv', skip_header=True)
trainY = load_file_into_array('data/ytrain.csv')

validX = load_file_into_array('data/xvalid.csv', skip_header=True)
validY = load_file_into_array('data/yvalid.csv')

testX = load_file_into_array('data/xtest.csv', skip_header=True)
testY = load_file_into_array('data/ytest.csv')

print(trainX.shape)

#
# Training
#

#
# Multilayer Perceptron model
#
# A simple network with 2 input, 1 hidden layer with 8 neurons and one (1) output layer.

log_dir = "logs/fit/" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
tensorboard_callback = TensorBoard(log_dir=log_dir, histogram_freq=1)

model = Sequential()
model.add(Dense(8, input_dim=trainX.shape[1], activation='relu'))
model.add(Dense(1))
model.compile(loss='mean_squared_error', optimizer='adam')
model.fit(trainX,
          trainY,
          validation_data=(validX, validY),
          epochs=100,
          batch_size=32,
          callbacks=[tensorboard_callback])

model.save("multilayer_perceptron.hdf5")

# Evaluation
trainScore = model.evaluate(trainX, trainY, verbose=0)
print('Train Score: %.2f MSE (%.2f RMSE)' % (trainScore, math.sqrt(trainScore)))
testScore = model.evaluate(testX, testY, verbose=0)
print('Test Score: %.2f MSE (%.2f RMSE)' % (testScore, math.sqrt(testScore)))

with open("multilayer_perceptron.results", mode='w') as results:
    print('Train Score: %.2f MSE (%.2f RMSE)' % (trainScore, math.sqrt(trainScore)), file=results)
    print('Test Score: %.2f MSE (%.2f RMSE)' % (testScore, math.sqrt(testScore)), file=results)

#
# LSTM model
#
# Same configuration of input/output as Multilayer Perceptron

trainX = trainX.reshape((trainX.shape[0], 2, 1))
print(trainX.shape)
validX = validX.reshape((validX.shape[0], 2, 1))
print(validX.shape)
testX = testX.reshape((testX.shape[0], 2, 1))
print(testX.shape)


log_dir = "logs/fit/" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
tensorboard_callback = TensorBoard(log_dir=log_dir, histogram_freq=1)

regressor = Sequential()

regressor.add(LSTM(units=50, return_sequences=True, input_shape=(trainX.shape[1], 1)))
regressor.add(Dropout(0.2))

regressor.add(LSTM(units=50, return_sequences=True))
regressor.add(Dropout(0.2))

regressor.add(LSTM(units=50, return_sequences=True))
regressor.add(Dropout(0.2))

regressor.add(LSTM(units=50))
regressor.add(Dropout(0.2))

regressor.add(Dense(units=1))

regressor.compile(optimizer='adam', loss='mean_squared_error')

regressor.fit(trainX,
              trainY,
              validation_data=(validX, validY),
              epochs=100,
              batch_size=32,
              callbacks=[tensorboard_callback])

regressor.save("regressor.hdf5")

# Evaluation
trainScore = regressor.evaluate(trainX, trainY, verbose=0)
print('Train Score: %.2f MSE (%.2f RMSE)' % (trainScore, math.sqrt(trainScore)))
testScore = regressor.evaluate(testX, testY, verbose=0)
print('Test Score: %.2f MSE (%.2f RMSE)' % (testScore, math.sqrt(testScore)))

with open("regressor.results", mode='w') as results:
    print('Train Score: %.2f MSE (%.2f RMSE)' % (trainScore, math.sqrt(trainScore)), file=results)
    print('Test Score: %.2f MSE (%.2f RMSE)' % (testScore, math.sqrt(testScore)), file=results)
