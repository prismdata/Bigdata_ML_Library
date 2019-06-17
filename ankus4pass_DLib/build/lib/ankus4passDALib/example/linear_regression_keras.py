from keras.models import Sequential
from keras.layers import Dense
from sklearn.model_selection import train_test_split
from keras.optimizers import  RMSprop

import pandas as pd

if __name__ == "__main__":
    df = pd.read_csv('../DataSet/house_price.csv', delim_whitespace=True, header = None)

    print df.head(13)

    data_set = df.values #change to numpy array

    label_index = 13

    X = data_set[:,0:12]
    Y = data_set[:, label_index]

    X_train, X_validation, Y_train, Y_validation = train_test_split(X, Y, test_size = 0.2)

    model = Sequential()
    model.add(Dense(input_dim= 12, units = 30,  activation='relu'))
    model.add(Dense(3, activation='relu'))
    model.add(Dense(1))
    rms = RMSprop(lr = 0.01)
    model.compile(loss = 'mse', optimizer='adam', metrics=['accuracy'])

    model.fit(X_train, Y_train, epochs = 10000, batch_size= 10)

    Y_prediction = model.predict(X_validation).flatten()

    for i in range(10):
        real_price = Y_validation[i]
        predicted_price = Y_prediction[i]
        print('Target Price:{:.3f}, Predicted Price: {:.3f}'.format(real_price, predicted_price))
