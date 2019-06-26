#bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 /home/rsi-psd-vm/Documents/rsi-psd-codes/psd/pratica-05/ConsultasDados/TamanhoPNL.py localhost:9092 subscribe rsipsd_project
from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import approxCountDistinct
import requests
import json

THINGSBOARD_HOST = '127.0.0.1'
THINGSBOARD_PORT = '9090'
# ACCESS_TOKEN = 'CtE7KH7e2FOfdzz0eYtG'
ACCESS_TOKEN = 'RuVeHyFHVCxSr2g3vy1W'
url = 'http://' + THINGSBOARD_HOST + ':' + THINGSBOARD_PORT + '/api/v1/' + ACCESS_TOKEN + '/telemetry'
headers = {}
headers['Content-Type'] = 'application/json'

def processRow(row):
    print(row)
    row_data = { 'mac' : row.__getitem__("count")}
    print(row_data)
    # requests.post(url, json=row_data)
    
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
    
    ssid = probes.filter('SSID != "BROADCAST"').select("mac", "SSID").distinct().groupBy("mac").count()
    
    # query = ssid\
    #     .writeStream\
    #     .outputMode("complete")\
    #     .foreach(processRow)\
    #     .start()

    query = ssid\
        .selectExpr("CAST(mac AS STRING) AS key","CAST(count AS STRING) AS value") \
        .writeStream\
        .format("kafka") \
        .outputMode("complete")\
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "rsipsd_project3") \
        .option("checkpointLocation", "checkpointSizePNL") \
        .start()

    query.awaitTermination()