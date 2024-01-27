# Spam Detection using Naive Bayes with Spark and NLTK

This project implements a spam detection system using the Naive Bayes algorithm in Python, leveraging Apache Spark for distributed computing and NLTK for natural language processing tasks such as tokenization, stemming, and stopwords removal.

## Overview

The project consists of the following main components:

1. **Data Preprocessing**: The provided dataset (assumed to be in CSV format) is loaded into Apache Spark, and preprocessing steps including tokenization, stemming, and stopwords removal are applied using NLTK.

2. **Naive Bayes Classifier**: The preprocessed data is split into training and testing sets. Then, a Naive Bayes classifier is trained on the training set to learn the patterns in the data.

3. **Evaluation**: The trained classifier is used to classify the test set, and the accuracy of the classifier is evaluated by comparing the predicted labels with the actual labels.

