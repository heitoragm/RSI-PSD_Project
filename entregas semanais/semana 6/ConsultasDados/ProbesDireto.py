#bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 /home/rsi-psd-vm/Documents/rsi-psd-codes/psd/pratica-05/ConsultasDados/ProbesDireto.py localhost:9092 subscribe rsipsd_project
from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import approxCountDistinct
import array

import time

def processRow(row):
#    print("Marca: " + row.fornecedor)
    #print("SSID: " + row.SSID)
    # SSID = row.SSID
    # countSSID = str((row.__getattr__("count")))
    
    print(str((row.__getattr__("count"))))
    
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("""
        Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
        """, file=sys.stderr)
        sys.exit(-1)
    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]

    spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .getOrCreate()

    lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option(subscribeType, topics)\
        .load()\
        .selectExpr("CAST(value AS STRING)")
    
    probes = lines.select(
        split(lines.value,',')[0].alias('timestamp'),
        split(lines.value,',')[1].alias('mac'),
        split(lines.value,',')[2].alias('SSID'),
        split(lines.value,',')[3].alias('fornecedor'),
        split(lines.value,',')[4].alias('macId')
    )
    
    probesDir = probes.filter('SSID != "BROADCAST"').select(
        approxCountDistinct('timestamp',rsd = 0.01).alias('count')
    )
    
    query = probesDir\
        .writeStream\
        .outputMode("complete")\
        .foreach(processRow)\
        .start()

    query.awaitTermination()