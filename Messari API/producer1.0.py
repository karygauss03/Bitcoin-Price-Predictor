from kafka import KafkaProducer
import urllib.request
import json
import time

producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9093'], api_version=(0, 10))

url = "https://data.messari.io/api/v1/assets/btc/metrics/market-data"

if __name__ == "__main__":
    print("Sending Data to Kafka")
    while True:
        print("****** New Data ******")
        bitcoin_market_data = urllib.request.urlopen(url).read()
        print(json.loads(bitcoin_market_data))
        producer.send('bitcoin', bitcoin_market_data)
        time.sleep(3)
    