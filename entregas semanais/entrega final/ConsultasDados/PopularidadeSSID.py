#bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 /home/rsi-psd-vm/Documents/rsi-psd-codes/psd/pratica-05/ConsultasDados/PopularidadeSSID.py localhost:9092 subscribe rsipsd_project
from __future__ import print_function

import sys

from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import approxCountDistinct
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: str(v).encode('utf-8'))

def processRow(row):
    print(row)
    # row_data = { row.SSID : row.__getitem__("count")}
    # count = str(row.__getitem__("count"))
    # print(count)
    # producer.send('rsipsd_project2', count)

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
        split(lines.value,',')[3].alias('fornecedor')
    )
    
    popularidadeSSID = probes.filter('SSID != "BROADCAST"').select("mac", "SSID").distinct().groupBy("SSID").count()
    
    # query = popularidadeSSID\
    #         .writeStream\
    #         .outputMode("complete")\
    #         .foreach(processRow)\
    #         .start()

    query = popularidadeSSID\
        .selectExpr("CAST(SSID AS STRING) AS key","CAST(count AS STRING) AS value") \
        .writeStream\
        .format("kafka") \
        .outputMode("complete") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "rsipsd_project2") \
        .option("checkpointLocation", "checkpointPopularidade") \
        .start()

    query.awaitTermination()