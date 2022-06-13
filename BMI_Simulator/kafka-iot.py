
"""
 Processes direct stream from kafka, '\n' delimited text directly received
   every 2 seconds.
 Usage: kafka-iot.py <broker_list> <topic>

 To run this on your local machine, you need to setup Kafka and create a
   producer first, see:
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      kafka-iot.py \
      localhost:9092 iotmsgs`
"""
from __future__ import print_function

import sys
import re

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from operator import add


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka-iot.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)

    sc.setLogLevel("WARN")

    ###############
    # Globals
    ###############
    HeightTotal = 0.0 
    HeightAvg = 0.0
    HeightCount = 0
    WeightTotal = 0.0
    WeightCount = 0
    WeightAvg = 0.0
    BMIAvg = 0.0

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    # Read in the Kafka Direct Stream into a TransformedDStream
    lines = kvs.map(lambda x: x[1])
    jsonLines = lines.map(lambda x: re.sub(r"\s+", "", x, flags=re.UNICODE))

    ############
    # Processing
    ############
    # foreach function to iterate over each RDD of a DStream
    def processHeightRDD(time, rdd):
      # Match local function variables to global variables
      global HeightTotal
      global HeightAvg
      global HeightCount
      HeightList = rdd.collect()
      for HeightFloat in HeightList:
        HeightTotal += float(HeightFloat)
        HeightCount += 1
        HeightAvg = HeightTotal / HeightCount
      print("Height Total = " + str(HeightTotal))
      print("Avg Height = " + str(HeightAvg))
      
    def processWeightRDD(time, rdd):
      # Match local function variables to global variables
      global WeightTotal
      global WeightCount
      global WeightAvg
      global HeightAvg
      global BMIAvg

      WeightList = rdd.collect()
      for WeightFloat in WeightList:
        WeightTotal += float(WeightFloat)
        WeightCount += 1
        WeightAvg = WeightTotal / WeightCount
        BMIAvg = round(WeightAvg / (HeightAvg ** 2  / 10000), 1)
      print("Weight Total = " + str(WeightTotal))  
      print("Avg Weight = " + str(WeightAvg))
      print("Avg BMI = " + str(BMIAvg))
      print("Total Count = " + str(WeightCount))

     # Search for specific IoT data values (assumes jsonLines are split(','))
    # GenderValues = jsonLines.filter(lambda x: re.findall(r"Gender.*", x, 0))
    # GenderValues.pprint(num=10000)

    # Parse out just the value without the JSON key
    # parsedGenderValues = GenderValues.map(lambda x: re.sub(r"\"Gender\":", "", x))
    
    # Search for specific IoT data values (assumes jsonLines are split(','))
    HeightValues = jsonLines.filter(lambda x: re.findall(r"Height.*", x, 0))
    HeightValues.pprint(num=10000)

    # Parse out just the value without the JSON key
    parsedHeightValues = HeightValues.map(lambda x: re.sub(r"\"Height\":", "", x).split(',')[0])

    # Search for specific IoT data values (assumes jsonLines are split(','))
    WeightValues = jsonLines.filter(lambda x: re.findall(r"Weight.*", x, 0))
    WeightValues.pprint(num=10000)
    
    # Parse out just the value without the JSON key
    parsedWeightValues = WeightValues.map(lambda x: re.sub(r"\"Weight\":", "", x).split(',')[0])
    
    # Count how many values were parsed
    countMap = parsedHeightValues.map(lambda x: 1).reduce(add)
    valueCount = countMap.map(lambda x: "Total Count of Msgs: " + unicode(x))
    valueCount.pprint()

    # Sort all the IoT values
    # sortedValues = parsedHeightValues.transform(lambda x: x.sortBy(lambda y: y))
    # sortedValues.pprint(num=10000)
    # sortedValues = parsedWeightValues.transform(lambda x: x.sortBy(lambda y: y))
    # sortedValues.pprint(num=10000)

    # Iterate on each RDD in parsedTempValues DStream to use w/global variables
    parsedHeightValues.foreachRDD(processHeightRDD)
    parsedWeightValues.foreachRDD(processWeightRDD)

    ssc.start()
    ssc.awaitTermination()
