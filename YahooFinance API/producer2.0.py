from kafka import KafkaProducer
import urllib.request
import json
import time
import pandas
from pandas_datareader import data as pdr
import yfinance as yf

producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9093'], api_version=(0, 10))

# url = "https://data.messari.io/api/v1/assets/btc/metrics/market-data"

yf.pdr_override()

bitcoin_prices = pdr.get_data_yahoo('BTC-USD', '2014-09-17', '2023-01-10' )
bitcoin_prices.reset_index(inplace = True)

if __name__ == "__main__":
    print("Sending Data to Kafka")
    # while True:
    #     print("****** New Data ******")
    #     bitcoin_market_data = urllib.request.urlopen(url).read()
    #     print(json.loads(bitcoin_market_data))
    #     producer.send('bitcoin', bitcoin_market_data)
    #     time.sleep(3)
    for index, row in bitcoin_prices.iterrows():
        print("****** New Data ******")
        data = {
            "Date": row['Date'],
            "Open": row['Open'],
            "High": row['High'],
            "Low": row['Low'],
            "Close": row['Close'],
            "Adj Close": row['Adj Close'],
            "Volume": row['Volume']
        }
        producer.send('bitcoin', json.dumps({
            "Date": str(row['Date']),
            "Open": float(row['Open']),
            "High": float(row['High']),
            "Low": float(row['Low']),
            "Close": float(row['Close']),
            "Adj Close": float(row['Adj Close']),
            "Volume": float(row['Volume'])
        }).encode('utf-8'))
        # time.sleep(1)
