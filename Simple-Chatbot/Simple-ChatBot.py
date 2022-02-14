import json
import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Embedding, GlobalAveragePooling1D
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
from sklearn.preprocessing import LabelEncoder


## Prepare Data set

with open('intents.json') as file:
    data = json.load(file)

training_sentences = []
training_labels = []
labels = []
responses = []

for intent in data['intents']:
    for pattern in intent['patterns']:
        training_sentences.append(pattern)
        training_labels.append(intent['tag'])
    responses.append(intent['responses'])

    if intent['tag'] not in labels:
        labels.append(intent['tag'])

num_classes = len(labels)

#training_sentences = np.array(training_sentences)
#training_labels = np.array(training_labels)

## Tokenize the sentences
#tokenizer = Tokenizer()
#tokenizer.fit_on_texts(training_sentences)
#word_index = tokenizer.word_index


## Create the training data


#training_sequences = tokenizer.texts_to_sequences(training_sentences)
#training_padded = pad_sequences(training_sequences, padding='post')


## Create the label encoder

print("Before Encoding: ", training_labels)

lbl_encoder = LabelEncoder()
lbl_encoder.fit(training_labels)
training_labels = lbl_encoder.transform(training_labels)

print("After Encoding: ", training_labels)


# Tokenize the responses
vocab_size = 1000
embedding_dim = 16
max_len = 20
oov_token = "<OOV>"

tokenizer = Tokenizer(num_words=vocab_size, oov_token=oov_token)
tokenizer.fit_on_texts(training_sentences)
word_index = tokenizer.word_index
# oov_token is the token for out of vocabulary words
# Tokanization -> "I love my dog" -> [1, 2, 3, 4] so convert sentence to sequence
# If we take I love my cat as input, it will be converted to [1, 2, 3, 5]
# Now tokaized_words = ['I':1, 'love':2, 'my':3, 'dog':4,'cat':5]
# Word index = {'I':1, 'love':2, 'my':3, 'dog':4,'cat':5}

sequences = tokenizer.texts_to_sequences(training_sentences)
padded_sequences = pad_sequences(sequences, truncating='post', maxlen=max_len)
# Truncating -> post - delete the words from the end if max_len is exceeded
# post padding means we will pad the sequence with 0's at Beginning of sequence (if need after sentence use pre padding)
# Sequencing is -> turn sentence into sequence of numbers
# sequence = [1, 2, 3, 4]
# padded_sequence = [1, 2, 3, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

# <OOV> is the token for out of vocabulary words (Not in the word index)
# So any word that not in the word index will be replaced with <OOV>'s index (1)

# Padded sequences convert the sequence into a fixed length sequence
# TODO: For more advanced we can use, RaggedTensor


print(tokenizer)
print(word_index)
print(sequences)
#print(padded_sequences)


## Create the model



model = Sequential()
model.add(Embedding(vocab_size, embedding_dim, input_length=max_len))
model.add(GlobalAveragePooling1D())
model.add(Dense(16, activation='relu'))
model.add(Dense(16, activation='relu'))
model.add(Dense(num_classes, activation='softmax'))



model.compile(
    loss='sparse_categorical_crossentropy',
    optimizer='adam',
    metrics=['accuracy']
)

print(model.summary())

# Train the model
epochs = 500
history = model.fit(padded_sequences, np.array(training_labels), epochs=epochs)

# Save the model
# to save the trained model
model.save("chat_model")

import pickle

# to save the fitted tokenizer
with open('tokenizer.pickle', 'wb') as handle:
    pickle.dump(tokenizer, handle, protocol=pickle.HIGHEST_PROTOCOL)

# to save the fitted label encoder
with open('label_encoder.pickle', 'wb') as ecn_file:
    pickle.dump(lbl_encoder, ecn_file, protocol=pickle.HIGHEST_PROTOCOL)





