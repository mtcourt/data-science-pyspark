# -*- coding: utf-8 -*-
"""
Created on Mon Sep 19 00:03:48 2022

@author: cnmik
"""

from __future__ import print_function

import sys
from sys import exit
import re
import numpy as np

from numpy import dot
from numpy.linalg import norm


from operator import add
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *

#spark = SparkSession.builder.master("local[*]").getOrCreate()
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

#Fixing issue of Pyspark not recognizing Python:
import os

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
#spark = SparkSession.builder.getOrCreate()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file> <output> ", file=sys.stderr)
        exit(-1)


    #sc = SparkContext(appName="Assignment-2")
    #loading the wikipedia category file:
    columns = ['docID', 'Category']
    categories = sqlContext.read.format('csv').options(inferSchema='true', sep = ',').load(sys.argv[1])
    #Giving column names:
    categories = categories.toDF('docID', 'Category')
    #To show the dataframe uncomment:
    #print(categories.show())
    
    #### Task 3.1 ####
    
    #Grouping by the docID to get the categories used for each wikipedia page and then counting
    #The top 10 wikipedia pages with the most categories, uncomment to see:
    #print(categories.groupBy('docID').count().orderBy('count', ascending=False).limit(10).show())
    #Using the predifined aggregate functions to compute max, avg, and std:
    print(categories.groupBy('docID').count().select(max('count')).show())
    print(categories.groupBy('docID').count().select(avg('count')).show())
    #Using the approxQuantile function to compute the median:
    print('Median: ', categories.groupBy('docID').count().approxQuantile('count', [0.5], .1))
    print(categories.groupBy('docID').count().select(stddev('count')).show())
    
    #### Task 3.2 ####
    print(categories.groupBy('Category').count().orderBy('count', ascending=False).limit(10).show())
    sc.stop()
    
    
    
    
    
    
    
    
    
    
    
    