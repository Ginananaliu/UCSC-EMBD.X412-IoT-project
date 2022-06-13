"""
 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      kafka-direct-iot-sql.py \
      localhost:9092 test`
"""
from __future__ import print_function

import sys
import re
import json


from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import OffsetRange
from operator import add
from pyspark.sql import functions as F

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)
    sqlContext = SQLContext(sc)
    sc.setLogLevel("WARN")

    ##############
    # Globals
    ##############
    globals()['maxHeight'] = sc.accumulator(0.0)


    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    jsonDStream = kvs.map(lambda (key, value): value)

    # Define function to process RDDs of the json DStream to convert them
    #   to DataFrame and run SQL queries
    def BMI(time, rdd):
        # Match local function variables to global variables
        maxHeight = globals()['maxHeight']

        print("========= %s =========" % str(time))
        print("rdd = %s" % str(rdd))

        try:
          if not rdd.isEmpty():
            # Parse the multiple JSON lines from the DStream RDD and trim any extra spaces
            jsonLinesRDD = rdd.map(lambda x: re.sub(r"\s+", "", x, flags=re.UNICODE)).reduce(add)
            print("jsonLinesRDD = %s" % str(jsonLinesRDD))

            # Convert RDD of the List of multiple JSON lines to Spark SQL Context by first
            #    joining the list of mulitple JSON lines in a new RDD with a single JSON line
            jsonRDD = sqlContext.read.json(sc.parallelize([jsonLinesRDD]))

            # Register the JSON SQL Context as a temporary SQL Table
            print("\n======================JSON Schema======================\n")
            jsonRDD.printSchema()
            jsonRDD.registerTempTable("iotmsgsTable")

            #############
            # Processing and Analytics go here
            #############
            print("\nPersonal Data Generate")
            sqlContext.sql("select payload.data.* from iotmsgsTable").show(n=100)
            
            # BMI
            print("\nCompute BMI and Sort by age")
            BMI = sqlContext.sql("select payload.data.*, round(payload.data.Weight/power(payload.data.Height/100, 2),1) as BMI from iotmsgsTable order by Age").show(n=100)
            
            # BMI Category & Sort
            print("\nDivide into BMI Category")
            df = sqlContext.sql("select payload.data.*, round(payload.data.Weight/power(payload.data.Height/100, 2),1) as BMI from iotmsgsTable order by Age")
            # try:
            df1 = df.select('*', F.when(df.BMI < 18.5, "Underweight").when((df.BMI > 24) & (df.BMI < 27), "Overweight").when(df.BMI > 27, "Obese").otherwise("Normal").alias("Category"))
            df1.show(n=100)
            # except Exception as e:
                # print(e)
            
            #Groupby and Count
            print("\nGroup by Category & Gender")
            df2 = df1.groupby(['Category','Gender']).count().sort("Category")
            df2.show(n=100)

            # Search & Filter
            print("\nFilter data to Normal BMI")
            df3 = df1.filter(df1.Category == "Normal")
            df3.show(truncate=False)
            print("Average age in normal BMI:", int(df3.select(F.avg("Age")).collect()[0][0]), "\n")
            
            # Clean-up
            sqlContext.dropTempTable("iotmsgsTable")
        # Catch any exceptions
        except:
            pass

    # Process each RDD of the DStream coming in from Kafka
    jsonDStream.foreachRDD(BMI)

    ssc.start()
    ssc.awaitTermination()
