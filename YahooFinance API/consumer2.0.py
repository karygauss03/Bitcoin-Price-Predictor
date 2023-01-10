from elasticsearch import Elasticsearch
from subprocess import check_output
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import math
from _collections import defaultdict
import json
from datetime import datetime
import os
os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.elasticsearch:elasticsearch-spark-20_2.12:7.17.5 pyspark-shell"

topic = "bitcoin"
ETH0_IP = check_output(["hostname"]).decode(encoding="utf-8").strip()
SPARK_MASTER_URL = "local[*]"
SPARK_DRIVER_HOST = ETH0_IP
spark_conf = SparkConf()
spark_conf.setAll(
    [
        ("spark.master", SPARK_MASTER_URL),
        ("spark.driver.bindAddress", "0.0.0.0"),
        ("spark.driver.host", SPARK_DRIVER_HOST),
        ("spark.app.name", "BitcoinConsumer"),
        ("spark.submit.deployMode", "client"),
        ("spark.ui.showConsoleProgress", "true"),
        ("spark.eventLog.enabled", "false"),
        ("spark.logConf", "false"),
    ]
)


def getrows(df, rownums=None):
    return df.rdd.zipWithIndex().filter(lambda x: x[1] in rownums).map(lambda x: x[0])


elastic_index = "bitcoin_kafka"
elastic_document = "_doc"
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])
es.options(ignore_status=[400, 404]).indices.delete(index=elastic_index)

es_mapping = {'mappings': {
    "properties":
        {
            "Date": {
                "type": "date"
            },
            "Open": {
                "type": "double"
            },
            "High": {
                "type": "double"
            },
            "Low": {
                "type": "double"
            },
            "Close": {
                "type": "double"
            },
            "Adj Close": {
                "type": "double"
            },
            "Volume": {
                "type": "double"
            }
        }
    }
}

response = es.indices.create(
    index=elastic_index,
    mappings=es_mapping,
    ignore=400  # Ignore Error 400 Index already exists
)
if __name__ == "__main__":
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic)
        .option("enable.auto.commit", "true")
        .load()
    )

    schema = StructType([
        StructField("Date", StringType()),
        StructField("Open", DoubleType()),
        StructField("High", DoubleType()),
        StructField("Low", DoubleType()),
        StructField("Close", DoubleType()),
        StructField("Adj Close", DoubleType()),
        StructField("Volume", DoubleType())
    ])

    def func_call(df, batch_id):
        valueRdd = df.rdd.map(lambda x: x[1])
        strList = valueRdd.map(lambda x: json.loads(x)).collect()
        tupleList = []
        for batch in strList:
            tupleList = []
            _source = {}
            _source['Date'] = batch['Date']
            _source['Open'] = batch['Open']
            _source['High'] = batch['High']
            _source['Low'] = batch['Low']
            _source['Close'] = batch['Close']
            _source['Adj Close'] = batch['Adj Close']
            _source['Volume'] = batch['Volume']
            tupleList.append(_source)
            tweetDF = spark.createDataFrame(data=tupleList, schema=schema)
            tweetDF.write \
                .format("org.elasticsearch.spark.sql") \
                .mode("append") \
                .option("es.nodes", "localhost").save(elastic_index)

    query = df.writeStream \
        .foreachBatch(func_call) \
        .start().awaitTermination()

    print("PySpark Structured Streaming with Kafka Application Completed....")
