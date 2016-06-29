import pandas as pd
import nltk
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.cross_validation import train_test_split
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.metrics import accuracy_score
import re

path = "/MSAN_USF/courses_spring/625_Practicum/data/tweet/"
filename=path + "pinkeye_clean.csv"

destination_file = "pink_eye_labeled.csv"
train_data = pd.read_csv(filename)

train_data['label'] = np.where(train_data['Category'] == 'Basic Negative', 0,1)


raw_tweets = train_data["Contents"]
tweets = list()
for tweet in raw_tweets:
    tweet1 = tweet
    tweet1 = re.sub('[^A-Za-z]+', ' ', tweet1)
    tweets.append(tweet1)

train_data["Clean_contents"] = tweets
X = train_data["Clean_contents"]
y = train_data["label"]
X_train, X_test, y_train, y_test = train_test_split( X,y,test_size=0.33, random_state=42)
print len(X_train), len(X_test), len(y_train), len(y_test)

count_vect = CountVectorizer(tokenizer=nltk.word_tokenize, stop_words='english',
                               max_features=10000, ngram_range=(1,2))

X_train_counts = count_vect.fit_transform(X_train)

tfidf_transformer = TfidfTransformer()

X_train_tfidf = tfidf_transformer.fit_transform(X_train_counts)

from sklearn.naive_bayes import MultinomialNB
clf = MultinomialNB().fit(X_train_tfidf, y_train)
X_new_counts = count_vect.transform(X_test)
X_new_tfidf = tfidf_transformer.transform(X_new_counts)
predicted = clf.predict(X_new_tfidf)

print accuracy_score(y_test.values, predicted)
