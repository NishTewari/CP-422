'''
CP422 - Assignment 3 

Nishant Tewari 
190684430
tewa4430@mylaurier.ca

Wednesday, December 6th, 2023 
'''
#imports 
import pandas as pd, pyspark, spark, sys

from pyspark import SparkContext, SparkConf
from pyspark.mllib.util import MLUtils
from pyspark.mllib.feature import StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.ml.linalg import Vectors, SparseVector

#Initialize Spark Session 
spark_session = SparkSession.builder.appName('a3').getOrCreate()
#Load the train.text data 
input_data = spark_session.read.format("libsvm").load('train.txt')

#Print the data type & show first few rows of the input_data
print("Type of input_data:", type(input_data))
input_data.show()

#Define Linear regression model 
LR_model = LinearRegression(featuresCol='features',labelCol='label',maxIter = 100, fitIntercept = True, elasticNetParam = 0, regParam = 0.5)

#Fit L.R. Model with the data provided 
fitModel = LR_model.fit(input_data)

#Print the model weights & intercepts 
print("Weights = " + str(fitModel.coefficients))
print("Intercept = " + str(fitModel.intercept))

#predict using the following example data 
example = "1:32.9 2:74 3:257.50 4:70.00 5:40.8 6:132.4 7:108.5 8:107.1 9:59.3 10:42.2 11:24.6 12:35.7 13:30.0 14:25.9"
example = spark_session.read.format("libsvm").load('example.txt')
print(example.features)
result = fitModel.transform(example)
result.show()