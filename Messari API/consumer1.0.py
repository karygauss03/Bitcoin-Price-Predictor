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


elastic_index = "bitcoins"
elastic_document = "_doc"
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])
# es.options(ignore_status=[400, 404]).indices.delete(index=elastic_index)
es_mapping = {'mappings': {
    "properties":
        {
            "timestamp": {
                "type": "date"
            },
            "id": {
                "type": "text"
            },
            "name": {
                "type": "text"
            },
            "price_usd": {
                "type": "double"
            },
            "price_btc": {
                "type": "double"
            },
            "price_eth": {
                "type": "double"
            },
            "volume_last_24_hours": {
                "type": "double",
                "index": False
            },
            "real_volume_last_24_hours": {
                "type": "double",
                "index": False
            },
            "volume_last_24_hours_overstatement_multiple": {
                "type": "double",
                "index": False
            },
            "percent_change_usd_last_24_hours": {
                "type": "double",
                "index": False
            },
            "percent_change_btc_last_24_hours": {
                "type": "double",
                "index": False
            },
            "percent_change_eth_last_24_hours": {
                "type": "double",
                "index": False
            }
        }
    }
}

# response = es.indices.create(
#     index=elastic_index,
#     mappings=es_mapping,
#     ignore=400  # Ignore Error 400 Index already exists
# )
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
        StructField("timestamp", StringType()),
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("price_usd", DoubleType()),
        StructField("price_btc", IntegerType()),
        StructField("price_eth", DoubleType()),
        StructField("volume_last_24_hours", DoubleType()),
        StructField("real_volume_last_24_hours", DoubleType()),
        StructField("volume_last_24_hours_overstatement_multiple",
                    DoubleType()),
        StructField("percent_change_usd_last_1_hour", DoubleType()),
        StructField("percent_change_btc_last_1_hour", DoubleType()),
        StructField("percent_change_eth_last_1_hour", DoubleType()),
        StructField("percent_change_usd_last_24_hours", DoubleType()),
        StructField("percent_change_btc_last_24_hours", DoubleType()),
        StructField("percent_change_eth_last_24_hours", DoubleType())
    ])

    def func_call(df, batch_id):
        valueRdd = df.rdd.map(lambda x: x[1])
        strList = valueRdd.map(lambda x: json.loads(x)).collect()
        tupleList = []
        for batch in strList:
            tupleList = []
            _source = {}
            _source['timestamp'] = batch['status']['timestamp']
            _source['id'] = batch['data']['Asset']['id']
            _source['name'] = batch['data']['Asset']['name']
            _source['price_usd'] = batch['data']['market_data']['price_usd']
            _source['price_btc'] = batch['data']['market_data']['price_btc']
            _source['price_eth'] = batch['data']['market_data']['price_eth']
            _source['volume_last_24_hours'] = batch['data']['market_data']['volume_last_24_hours']
            _source['real_volume_last_24_hours'] = batch['data']['market_data']['real_volume_last_24_hours']
            _source['volume_last_24_hours_overstatement_multiple'] = batch['data']['market_data']['volume_last_24_hours_overstatement_multiple']
            _source['percent_change_usd_last_24_hours'] = batch['data']['market_data']['percent_change_usd_last_24_hours']
            _source['percent_change_btc_last_24_hours'] = batch['data']['market_data']['percent_change_btc_last_24_hours']
            _source['percent_change_eth_last_24_hours'] = batch['data']['market_data']['percent_change_eth_last_24_hours']
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
