from kafka import KafkaConsumer
import json

if __name__ == "__main__":
    consumer = KafkaConsumer('Bitcoin', bootstrap_servers=['localhost:9092', 'localhost:9093'], api_version=(0, 10))
    print("Data Received from Kafka: ")
    for data in consumer:
        print(json.loads(data.value))
