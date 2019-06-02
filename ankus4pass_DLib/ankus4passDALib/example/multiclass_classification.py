import numpy
import pandas
from keras.models import Sequential
from keras.layers import Dense
from keras.utils import np_utils
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
import os

if __name__ == "__main__":
    bath_path = os.path.dirname(os.path.abspath(__file__))

    dataframe = pandas.read_csv(bath_path + '/../DataSet/iris.csv', header=1)

    dataset = dataframe.values

    X = dataset[:,0:4].astype(float)
    Y = dataset[:,4]

    X_train, X_validation, Y_train, Y_validation = train_test_split(X, Y, test_size = 0.2)

    encoder = LabelEncoder()
    encoder.fit(Y_train)
    encoded_Y = encoder.transform(Y_train)
    Y_train = np_utils.to_categorical(encoded_Y)

    model = Sequential()
    model.add(Dense(8, input_dim = 4, activation='relu'))
    model.add(Dense(3, activation='softmax'))
    model.compile(loss = 'categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
    model.fit(X_train, Y_train, epochs = 10000, batch_size= 10)
    model.evaluate(X_train, Y_train)

    predictions = model.predict_classes(X_validation)
    predict_category = np_utils.to_categorical(predictions)
    predict_argmax = numpy.argmax(predict_category, axis = 1)
    Y_prediction = encoder.inverse_transform(predict_argmax)
    for i in range(10):
        real_price = Y_validation[i]
        predicted_price = Y_prediction[i]
        print('Target Price:{}, Predicted Price: {}'.format(real_price, predicted_price))

