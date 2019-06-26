# #
# # Licensed to the Apache Software Foundation (ASF) under one or more
# # contributor license agreements.  See the NOTICE file distributed with
# # this work for additional information regarding copyright ownership.
# # The ASF licenses this file to You under the Apache License, Version 2.0
# # (the "License"); you may not use this file except in compliance with
# # the License.  You may obtain a copy of the License at
# #
# #    http://www.apache.org/licenses/LICENSE-2.0
# #
# # Unless required by applicable law or agreed to in writing, software
# # distributed under the License is distributed on an "AS IS" BASIS,
# # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# # See the License for the specific language governing permissions and
# # limitations under the License.
# #

# """
#  Consumes messages from one or more topics in Kafka and does wordcount.
#  Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
#    <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
#    comma-separated list of host:port.
#    <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
#    'subscribePattern'.
#    |- <assign> Specific TopicPartitions to consume. Json string
#    |  {"topicA":[0,1],"topicB":[2,4]}.
#    |- <subscribe> The topic list to subscribe. A comma-separated list of
#    |  topics.
#    |- <subscribePattern> The pattern used to subscribe to topic(s).
#    |  Java regex string.
#    |- Only one of "assign, "subscribe" or "subscribePattern" options can be
#    |  specified for Kafka source.
#    <topics> Different value format depends on the value of 'subscribe-type'.

#  Run the example
#     `$ bin/spark-submit examples/src/main/python/sql/streaming/structured_kafka_wordcount.py \
#     host1:port1,host2:port2 subscribe topic1,topic2`
    
#     bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 /home/rsi-psd-vm/Documents/rsi-psd-codes/psd/pratica-05/structured_kafka_wordcount.py localhost:9092 subscribe rsipsd_project
# """
# from __future__ import print_function

# import sys

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import explode
# from pyspark.sql.functions import split

# if __name__ == "__main__":
#     if len(sys.argv) != 4:
#         print("""
#         Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
#         """, file=sys.stderr)
#         sys.exit(-1)

#     bootstrapServers = sys.argv[1]
#     subscribeType = sys.argv[2]
#     topics = sys.argv[3]

#     spark = SparkSession\
#         .builder\
#         .appName("StructuredKafkaWordCount")\
#         .getOrCreate()

#     # Create DataSet representing the stream of input lines from kafka
#     lines = spark\
#         .readStream\
#         .format("kafka")\
#         .option("kafka.bootstrap.servers", bootstrapServers)\
#         .option(subscribeType, topics)\
#         .load()\
#         .selectExpr("CAST(value AS STRING)")
    
#     # Split the lines into words
#     probes = lines.select(
#         # explode turns each item in an array into a separate row
#         explode(
#             split(lines.value,' ')
#         ).alias('word')
#     )

#     # Generate running word count
#     wordCounts = probes.groupBy('word').count()
#     print(wordCounts)

#     # Start running the query that prints the running counts to the console
#     query = wordCounts\
#         .writeStream\
#         .trigger(processingTime="1 second")\
#         .outputMode('complete')\
#         .format('console')\
#         .start()

#     query.awaitTermination()



#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
 Consumes messages from one or more topics in Kafka and does wordcount.
 Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
   <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
   comma-separated list of host:port.
   <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
   'subscribePattern'.
   |- <assign> Specific TopicPartitions to consume. Json string
   |  {"topicA":[0,1],"topicB":[2,4]}.
   |- <subscribe> The topic list to subscribe. A comma-separated list of
   |  topics.
   |- <subscribePattern> The pattern used to subscribe to topic(s).
   |  Java regex string.
   |- Only one of "assign, "subscribe" or "subscribePattern" options can be
   |  specified for Kafka source.
   <topics> Different value format depends on the value of 'subscribe-type'.

 Run the example
    `$ bin/spark-submit examples/src/main/python/sql/streaming/structured_kafka_wordcount.py \
    host1:port1,host2:port2 subscribe topic1,topic2`
    
    bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 /home/rsi-psd-vm/Documents/rsi-psd-codes/psd/pratica-05/structured_kafka_wordcount.py localhost:9092 subscribe rsipsd_project
"""
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
    SSID = row.SSID
    countSSID = str((row.__getattr__("count")))
    
    print(SSID, countSSID)
    
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
    
    dispositivos = probes.select(
        approxCountDistinct('mac',rsd = 0.01)
    )
  
    pnl = probes.filter('SSID != "BROADCAST"').select(
        'mac'
    ).distinct()

    pnl1 = pnl.select(
        approxCountDistinct('mac',rsd = 0.01)
    )
    
    probesTot = probes.select(
        approxCountDistinct('timestamp',rsd = 0.01)
    )

    probesBroad = probes.filter('SSID == "BROADCAST"').select(
        approxCountDistinct('timestamp',rsd = 0.01)
    )

    probesDir = probes.filter('SSID != "BROADCAST"').select(
        approxCountDistinct('timestamp',rsd = 0.01)
    )

    ssid = probes.filter('SSID != "BROADCAST"').select(
        approxCountDistinct('SSID',rsd = 0.01)
    )
    
    # macs = probes.groupBy('mac').count()
    
    dispositivosPorFornecedor = probes.select("fornecedor", "mac").distinct().groupBy("fornecedor").count()

    expoePNLPorMarca = probes.filter('SSID != "BROADCAST"').select("fornecedor", "mac").distinct().groupBy("fornecedor").count()

    popularidadeSSID = probes.filter('SSID != "BROADCAST"').select("mac", "SSID").distinct().groupBy("SSID").count()

    #infernal = popularidadeSSID.select(approxCountDistinct("count", rsd = 0.01))

    popularidadeSSID.createOrReplaceTempView("count")

    df = spark.sql("SELECT * FROM count")

    #graficoInfernal = popularidadeSSID.select(approxCountDistinct("count"))

    # macs.createOrReplaceTempView("macs")
    # numMacs = spark.sql('SELECT DISTINCT mac FROM macs')
    
    # numDispositivos = spark.sql('SELECT count(mac) FROM (SELECT DISTINCT mac FROM probes)')
    # numSSIDs = spark.sql('SELECT count(*) FROM (SELECT SSID FROM probes GROUP BY SSID)')
    # pnl = spark.sql('SELECT mac, SSID FROM probes WHERE SSID != "BROADCAST" GROUP BY mac, SSID')
    # pnl = probes.where('SSID != "BROADCAST"').groupBy('mac','SSID')
    # pnl = pnl.select(
    #     len(pnl.value).alias('count')
    # )
    # print(PNLs)
    # df.printSchema()
    # tabela2 = probes.select(
    #     lines.value
    # )

    # Generate running word count
    wordCounts = probes#.groupBy('timestamp', 'mac', 'SSID').count().orderBy('count')
    print(wordCounts)

    # Start running the query that prints the running counts to the console
    query = df\
        .writeStream\
        .outputMode("complete")\
        .foreach(processRow)\
        .start()

    query.awaitTermination()