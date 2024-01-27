#load the dataset
data = sc.textFile("/FileStore/tables/spam.csv")

#install nltk
pip install nltk

#Import required libraries
import nltk
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords

#Download nltk resources
nltk.download('punkt')
nltk.download('stopwords')

#remove header row
header = data.first()
dataWithoutHeader = data.filter(lambda line: line != header)

#structue data in (text, class) format
pairData = dataWithoutHeader.map(lambda x: (x.split(",",1)[1], x.split(",",1)[0]))

#tokenization for stemming, stop word removal
tokenizedData = pairData.map(lambda x: (word_tokenize(x[0]), x[1]))

#stemming
ps = PorterStemmer()
stemmedData = tokenizedData.map(lambda x: ([ps.stem(y) for y in x[0]], x[1]))

#helper function to filter stopwords
stop_words = set(stopwords.words('english'))
def filterStopWords(words):
    filtered_sentence = [w for w in words if not w.lower() in stop_words]
    return filtered_sentence

#final processed data (after stemming, stop word removal)
finalData = stemmedData.map(lambda x: (filterStopWords(x[0]), x[1]))

#split the finaldata into train-test, 70-30
train, test = finalData.randomSplit([0.7, 0.3], seed = 2018)

#map reduce to find number of times a word occurs in every class, count word_class
wordClassData = train.flatMap(lambda x: [ (w+"_"+x[1], 1) for w in x[0]])
reducedWordClassData = wordClassData.reduceByKey(lambda x, y: x+y)

#converting to dict for random access of count based on key word_class
reducedWordClassDataDict = dict(reducedWordClassData.collect())

#prior probabilities counting spam, ham
count = train.map(lambda x: (x[1],1))
priors = count.reduceByKey(lambda x,y: x+y)
priorList = priors.collect()
hamPrior = priorList[0][1]/(priorList[0][1]+priorList[1][1])
spamPrior = priorList[1][1]/(priorList[0][1]+priorList[1][1])

#naive bayes to classify as spam/ham
def classify(wordList):
    probWInHam = math.log(hamPrior)
    probWInSpam = math.log(spamPrior)
    
    for w in wordList:
        hamCount = reducedWordClassDataDict.get(w+"_ham", 0) + 1
        probWInHam += math.log(hamCount/totalNumberOfWordsInHam)

        spamCount = reducedWordClassDataDict.get(w+"_spam", 0) + 1
        probWInSpam += math.log(spamCount/totalNumberOfWordsInSpam)
    if probWInHam >= probWInSpam:
        return 'ham'
    return 'spam'

#map to classify each test row
result = test.map(lambda x: (classify(x[0]), x[1]))

#filter misclassifications
error = result.filter(lambda x: x[0]!=x[1])

#get error %
err = error.count()/test.count()
print("Accuracy is ", 1-err)
