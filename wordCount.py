#!/usr/bin/env python

from pyspark import SparkConf,SparkContext
from operator import add

conf=SparkConf().setMaster("local").setAppName("wordCount")
sc=SparkContext(conf=conf)

inputFile="file:///Users/liuxinming/README.md"
outputFile="file:///Users/liuxinming/output"
objectFile=sc.textFile(inputFile)

counts=objectFile.flatMap(lambda line:line.split(" ")).map(lambda word:(word,1)).reduceByKey(lambda a,b:a+b)
counts.saveAsTextFile(outputFile)
