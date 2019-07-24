#!/usr/bin/env python3
from __future__ import print_function
from pyspark.sql.functions import udf, expr, concat, col
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.tuning import CrossValidatorModel
from pyspark.sql.functions import udf


#bunch of ML imports
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

import argparse
import json
import re
import string
import sys
import os

__author__ = ""
__email__ = ""

# Depending on your implementation,
# this data may or may not be useful.
# Many students last year found it redundant.
_CONTRACTIONS = {
    "tis": "'tis",
    "aint": "ain't",
    "amnt": "amn't",
    "arent": "aren't",
    "cant": "can't",
    "couldve": "could've",
    "couldnt": "couldn't",
    "didnt": "didn't",
    "doesnt": "doesn't",
    "dont": "don't",
    "hadnt": "hadn't",
    "hasnt": "hasn't",
    "havent": "haven't",
    "hed": "he'd",
    "hell": "he'll",
    "hes": "he's",
    "howd": "how'd",
    "howll": "how'll",
    "hows": "how's",
    "id": "i'd",
    "ill": "i'll",
    "im": "i'm",
    "ive": "i've",
    "isnt": "isn't",
    "itd": "it'd",
    "itll": "it'll",
    "its": "it's",
    "mightnt": "mightn't",
    "mightve": "might've",
    "mustnt": "mustn't",
    "mustve": "must've",
    "neednt": "needn't",
    "oclock": "o'clock",
    "ol": "'ol",
    "oughtnt": "oughtn't",
    "shant": "shan't",
    "shed": "she'd",
    "shell": "she'll",
    "shes": "she's",
    "shouldve": "should've",
    "shouldnt": "shouldn't",
    "somebodys": "somebody's",
    "someones": "someone's",
    "somethings": "something's",
    "thatll": "that'll",
    "thats": "that's",
    "thatd": "that'd",
    "thered": "there'd",
    "therere": "there're",
    "theres": "there's",
    "theyd": "they'd",
    "theyll": "they'll",
    "theyre": "they're",
    "theyve": "they've",
    "wasnt": "wasn't",
    "wed": "we'd",
    "wedve": "wed've",
    "well": "we'll",
    "were": "we're",
    "weve": "we've",
    "werent": "weren't",
    "whatd": "what'd",
    "whatll": "what'll",
    "whatre": "what're",
    "whats": "what's",
    "whatve": "what've",
    "whens": "when's",
    "whered": "where'd",
    "wheres": "where's",
    "whereve": "where've",
    "whod": "who'd",
    "whodve": "whod've",
    "wholl": "who'll",
    "whore": "who're",
    "whos": "who's",
    "whove": "who've",
    "whyd": "why'd",
    "whyre": "why're",
    "whys": "why's",
    "wont": "won't",
    "wouldve": "would've",
    "wouldnt": "wouldn't",
    "yall": "y'all",
    "youd": "you'd",
    "youll": "you'll",
    "youre": "you're",
    "youve": "you've"
}

_PUNCTUATIONS = {
    ".": True,
    "!": True,
    "?": True,
    ",": True,
    ";": True,
    "*": True,
    ":": True,
    "'": True,
    '"': True
}

# You may need to write regular expressions.
# You may need to write regular expressions.
def check(text):
    for i in range(len(text)-1):
        if text[i:i+1]=="/s":
            return 1
    return 0
    
def firstthree(text):
    return text[0:2]  

def resolve(text): 
    return float(text[1])

def isState(text):
    if (str(text) in states):
        return 1
    return 0

def sanitize(text):
    """Do parse the text in variable "text" according to the spec, and return
    a LIST containing FOUR strings 
    1. The parsed text.
    2. The unigrams
    3. The bigrams
    4. The trigrams
    """

    parsed_text = ""
    unigrams = ""
    bigrams = ""
    trigrams = ""
    everything = []
    text = text.replace("\n", " ") #replace newlines with spaces
    text = text.replace("\t", " ") #replace tabs with spaces
    text += ' '
    text = re.sub(r"(?:__|[*#])|\(https:.*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(https:.*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(https:.*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(https:.*?\)|\[|\]", "",text)

    text = re.sub(r"(?:__|[*#])|\(http:.*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(http:.*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(http:.*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(http:.*?\)|\[|\]", "",text)

    text = re.sub(r"(?:__|[*#])|\(www.*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(www.*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(www.*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(www.*?\)|\[|\]", "",text)

    text = re.sub(r"(?:__|[*#])|\(/r*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(/r*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(/r*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(/r*?\)|\[|\]", "",text)

    text = re.sub(r"(?:__|[*#])|\(/u*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(/u*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(/u*?\)|\[|\]", "",text)
    text = re.sub(r"(?:__|[*#])|\(/u*?\)|\[|\]", "",text)

    text = re.sub(r"/(?:__|[*#])|www.*? ", "",text) 
    text = re.sub(r"/(?:__|[*#])|https:.*? ", "",text)
    text = re.sub(r"/(?:__|[*#])|http:.*? ", "",text)  
    text = re.sub(r"/(?:__|[*#])|www.*? \n", "",text) 
    text = re.sub(r"/(?:__|[*#])|https:.*? \n", "",text) 
    text = re.sub(r"/(?:__|[*#])|http:.*? \n", "",text)
    text = re.sub(r"/(?:__|[*#])|www.*? \t", "",text) 
    text = re.sub(r"/(?:__|[*#])|https:.*? \t", "",text) 
    text = re.sub(r"/(?:__|[*#])|http:.*? \t", "",text)
    #replacing URLs

    #lowercase
    text2 = []
    for i in range (len(text)):
        text2.append(text[i].lower())
    text = "".join(text2)

    #seperating punctuations
    text2 = text.split()
    temp = []

    for word in text2:
        if len(word) == 1:
            temp.append(word)
            continue
        # looking for external punctuations
        i = 0
        while i < len(word) and word[i] in _PUNCTUATIONS:
            temp.append(word[i])
            if word[i]:
                i += 1

        
        if i == len(word):
            temp.append(word)
            continue
        
        #go through the punctuations at the end -- but we cant add them to main yet cause we havent added the actual word
        j = len(word) - 1
        while j >= 0 and word[j] in _PUNCTUATIONS:
            j -= 1
        
        #create string with the actual word
        tempstr = ""
        while i <= j:
            tempstr += word[i]
            i += 1
    
        temp.append(tempstr)    

        #now append the ending punctuations
        
        while(j < len(word)-1):
            j += 1
            temp.append(word[j])
    temp2 = []

    for word in temp:
        if word != '"' and word != "'":
            temp2.append(word)
    
    # for i in range(len(temp2)-1):
    #     if temp2[i] in _CONTRACTIONS:
    #         temp2[i] = _CONTRACTIONS.get(temp2[i])
    
        
    text = " ".join(temp2)
  
    #split text on a single space
    #separate all external punctuation into their own tokens
    #listParsed is a list of lists that is seperated into sentences (append a new list everytime you hit a punctuation mark)

    parsed_text = text

    temp = parsed_text.split()

    listParsed = []
    listParsed.append([])
    j = 0
    for i in temp:
        if i in _PUNCTUATIONS:
            listParsed.append([])
            if not listParsed[0]:
                continue
            j += 1
        else:
            listParsed[j].append(i)

    #unigrams
    for sentence in listParsed:
        for word in sentence:
            everything.append(word) 

    #bigrams
    for sentence in listParsed:
        for i in range(len(sentence) - 1):
            if i != len(sentence) - 1:
                everything.append(sentence[i] + "_" + sentence[i+1] )

    #trigrams
    for sentence in listParsed:
        for i in range(len(sentence) - 1):
            if i < len(sentence) - 2:
                everything.append(sentence[i] + "_" + sentence[i+1] + "_" + sentence[i+2])

    #removing the last space for unigrams, bigrams and trigrams
    uni_len = len(unigrams) - 1
    bi_len  = len(bigrams) - 1
    tri_len = len(trigrams) - 1

    unigrams = unigrams[0 : uni_len]
    bigrams = bigrams[0 : bi_len]
    trigrams = trigrams[0 : tri_len]
    
    return everything


states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
        'Connecticut', 'Delaware', 'District Of Columbia', 'Florida', 'Georgia', 
        'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky',
        'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 
        'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 
        'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 
        'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 
        'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']


def main(context):
    """Main function takes a Spark SQL context."""
    #TASK 1
    #reading data from parquets
    print("task 1")
    comments = sqlContext.read.parquet('comments.parquet')
    submissions = sqlContext.read.parquet('submissions.parquet')
    labelfile = sqlContext.read.parquet('labels.parquet')
    
    #TASK 2
    print("task 2")
    #turn comments into table
    comments.createOrReplaceTempView("comment")
    labelfile.createOrReplaceTempView("labels")
    devam = sqlContext.sql("SELECT body, labeldem, labelgop, labeldjt, Input_id FROM comment join labels on Input_id = id")
   
    #reading comments into sanitize
    #TASK 4, 5
    # -- MORE TASK 5 SPECIFIC CODE INSIDE THE SANITIZE FUNCTION
    print("tasks 4 and 5")
    sqlContext.registerFunction("sudf", lambda y: sanitize(y), ArrayType(StringType()))
    devam.createOrReplaceTempView("devam")
    body = sqlContext.sql("SELECT sudf(body) as newBod, body, labeldem, labelgop, labeldjt FROM devam")
    # fit a CountVectorizerModel from the corpus
   
    #TASK 6A
    print("task 6a")
    cv = CountVectorizer(inputCol="newBod", outputCol="features", minDF=10, binary=True)
    model = cv.fit(body)

    neg_exists = os.path.isfile('project2/neg.model')
    pos_exists = os.path.isfile('project2/pos.model')

    if not neg_exists and not pos_exists:
        #TASK 6B
        print("task 6b")
        result = model.transform(body)
        #adding columns for negative and positive to more easily identify reaction type
        result.createOrReplaceTempView("result")
        updated = sqlContext.sql("Select case when labeldjt = 1 then 1 else 0 end as positive, case when labeldjt = 1 then 1 else 0 end as negative, sudf(body), features from result")
        updated.createOrReplaceTempView("updated")
        
        #Task 7
        # Initialize two logistic regression models.
        # Replace labelCol with the column containing the label, and featuresCol with the column containing the features.

        #create pslr
        pos = sqlContext.sql("SELECT positive as label, features from updated")
        poslr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
    
        #create neglr
        neg = sqlContext.sql("SELECT negative as label, features from updated")
        neglr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)

        # This is a binary classifier so we need an evaluator that knows how to deal with binary classifiers.
        posEvaluator = BinaryClassificationEvaluator()
        negEvaluator = BinaryClassificationEvaluator()
        # There are a few parameters associated with logistic regression. We do not know what they are a priori.
        # We do a grid search to find the best parameters. We can replace [1.0] with a list of values to try.
        # We will assume the parameter is 1.0. Grid search takes forever.
        posParamGrid = ParamGridBuilder().addGrid(poslr.regParam, [1.0]).build()
        negParamGrid = ParamGridBuilder().addGrid(neglr.regParam, [1.0]).build()
        # We initialize a 5 fold cross-validation pipeline.
        posCrossval = CrossValidator(
            estimator=poslr,
            evaluator=posEvaluator,
            estimatorParamMaps=posParamGrid,
            numFolds=5)
        negCrossval = CrossValidator(
            estimator=neglr,
            evaluator=negEvaluator,
            estimatorParamMaps=negParamGrid,
            numFolds=5)
        # Although crossvalidation creates its own train/test sets for
        # tuning, we still need a labeled test set, because it is not
        # accessible from the crossvalidator (argh!)
        # Split the data 50/50
        posTrain, _ = pos.randomSplit([0.8, 0.2])
        negTrain, _ = neg.randomSplit([0.8, 0.2])
        # Train the models
        print("Training positive classifier...")
        posModel = posCrossval.fit(posTrain)
        print("Training negative classifier...")
        negModel = negCrossval.fit(negTrain)

        # Once we train the models, we don't want to do it again. We can save the models and load them again later.
        posModel.save("project2/pos.model")
        negModel.save("project2/neg.model")
    else:
        posModel = CrossValidatorModel.load("project2/pos.model")
        negModel = CrossValidatorModel.load("project2/neg.model")
        print("finished task 7")

    #Task 8
    # COMMENT OUT LATER
    comments = comments.sample(False, 0.2)
    submissions = submissions.sample(False, 0.2)
    #register spark function to remove
    truncate = udf(lambda x: x[3:], StringType())
    sqlContext.registerFunction("firstthree",lambda x:firstthree(x),StringType())
    sqlContext.registerFunction("check", lambda x: check(x) , IntegerType())
    sqlContext.registerFunction("resolve", lambda x: resolve(x) , FloatType())
    comments = comments.select(
        "body", 
        comments.id.alias("cid"),
        comments.created_utc.alias("timestamp"), 
        "author_flair_text", 
        truncate(comments.link_id).alias("link_id"), 
        comments.score.alias("c_score")
        )
    submissions = submissions.select(
        "id", 
        "title",
        submissions.score.alias("s_score")
    )

    comments.createOrReplaceTempView("comments")
    submissions.createOrReplaceTempView("submissions")
    task8table = sqlContext.sql(""" 
        SELECT *
        FROM comments L
        JOIN submissions R
        ON L.link_id = R.id
        """)
    task8table.createOrReplaceTempView("task8table")

  #task 9
    print("finished task 8 - now onto task 9")
    t9 = sqlContext.sql("SELECT sudf(body) as newBod, timestamp, link_id,author_flair_text, c_score, s_score, id FROM task8table WHERE firstthree(body) != '&gt' AND check(body) = 0")
    t9.createOrReplaceTempView("t9")
    #t9=t9.sample(False,0.2)
    stuff = model.transform(t9)
    posRes = posModel.transform(stuff)
    posRes.createOrReplaceTempView("posRes")
    posRes = sqlContext.sql("SELECT features, id, timestamp, author_flair_text, link_id,c_score, s_score, probability as pos_probability FROM posRes")
    t9fin = negModel.transform(posRes)
    t9fin.createOrReplaceTempView("t9fin")
    t9next = sqlContext.sql("""SELECT 
            id, timestamp, author_flair_text,link_id, c_score, s_score,
            features, pos_probability, probability, case when resolve(pos_probability) <= .2 then 0 else 1 end as p_test, 
            case when resolve(probability) <= .25 then 0 else 1 end as n_test 
            FROM 
            t9fin""")
    t9next.createOrReplaceTempView("t9next")
    t9next.show()
    
    print("done with task 9 - now onto task 10")

    #task 10 - queries
    #t9next= t9next.sample(False, 0.1)

    #t9next = t9next.sample(False, .001)
    sqlContext.registerFunction("isState", lambda x: isState(x) , IntegerType())
    t9next= sqlContext.sql("SELECT p_test, n_test, link_id, isState(author_flair_text) as state, timestamp, author_flair_text, c_score, s_score FROM t9next")
    
    t9next.createOrReplaceTempView("t9next")

    # t9next.show()

    # t9next = t9next.select(
    #     "p_test",
    #     "n_test",
    #     "link_id",
    #     "timestamp",
    #     isState(t9next.author_flair_text).alias("state"),
    #     "author_flair_text"
    # )
    
    query_one = sqlContext.sql("SELECT SUM(p_test) / COUNT(*) as posprob, SUM(n_test) / COUNT(*), link_id as negprob FROM t9next GROUP BY link_id")
    query_two = sqlContext.sql("SELECT DATE(FROM_UNIXTIME(timestamp)) as Date, SUM(p_test) / COUNT(*) as posprob, SUM(n_test) / COUNT(*) as negprob FROM t9next GROUP BY Date")
    query_three = sqlContext.sql("SELECT author_flair_text, SUM(p_test) / COUNT(*) as posprob, SUM(n_test) / COUNT(*) as negprob FROM t9next WHERE state = 1 GROUP BY author_flair_text")
    query_four_c = sqlContext.sql("SELECT c_score, SUM(p_test) / COUNT(*) as posprob, SUM(n_test) / COUNT(*) as negprob FROM t9next GROUP BY c_score")
    query_four_s = sqlContext.sql("SELECT s_score, SUM(p_test) / COUNT(*) as posprob, SUM(n_test) / COUNT(*) as negprob FROM t9next GROUP BY s_score")

    print("done with task 10, now loading")
    # query_one.show()
    # query_two.show()
    # query_three.show()
    # query_four_c.show()
    # query_four_s.show()
    
    # print("saving query_one")
    # query_one.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("probability_data.csvfolder")
    # print("saving query_two")
    # query_two.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("time_data.csvfolder")
    # print("saving query_three")
    # query_three.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("state_data.csvfolder")
    # print("saving query_four_c")
    # query_four_c.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("comment_score.csvfolder")
    # print("saving query_four_s")
    # query_four_s.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("submission_score.csvfolder")
   
if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)