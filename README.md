<h1 align="center">
  <br>
  Bitcoin Price Predictor

</h1>

<br>

## Overview
This project consists of a Kibana dashboard for Bitcoin price visualization and price prediction using YahooFinance API. The project sets up a complete pipeline for data collection, streaming, indexing, visualization and ML model training and prediction.

![alt text](https://github.com/karygauss03/Bitcoin-Price-Predictor/blob/main/img/architecture.png)
<span>Project Architecture</span>
![alt text](https://github.com/karygauss03/Bitcoin-Price-Predictor/blob/main/img/Viz1.png)
<span>Data Visualization</span>
![alt text](https://github.com/karygauss03/Bitcoin-Price-Predictor/blob/main/img/Viz2.png)
<span>Data Visualization</span>
![alt text](https://github.com/karygauss03/Bitcoin-Price-Predictor/blob/main/img/Viz3.png)
<span>Anomaly detection tool in Kibana to detect unusual patterns</span>
![alt text](https://github.com/karygauss03/Bitcoin-Price-Predictor/blob/main/img/Viz4.png)
<span>Forecast the bitcoin price using **Forecast tool in Kibana** for the next 14 days</span>
![alt text](https://github.com/karygauss03/Bitcoin-Price-Predictor/blob/main/img/Viz5.png)
<span>Predicted value visualization</span>
![alt text](https://github.com/karygauss03/Bitcoin-Price-Predictor/blob/main/img/prediction_lstm.JPG)
![alt text]([img\prediction_lstm_more.png](https://github.com/karygauss03/Bitcoin-Price-Predictor/blob/main/img/prediction_lstm_more.png))
<span>Price prediction for the next week using LSTM</span>

## Folder Structure 
    .
    ├── Data  
    ├── img  
    ├── notebooks                        
    │   ├── Bitcoin_price_prediction_LSTM.ipynb   #Machine learning mode
    |   ├── Messari API
    |       ├── producer1.py
    |       ├── consumer1.py
    |   ├── YahooFinance API
    |       ├── producer2.py
    |       ├── consumer2.py
    |   ├── requirements.txt
    ├── docs                                  #Report
    └── README.md

## Dataset

This work relies on [Historical bitcoin data from YahooFinance](https://finance.yahoo.com/quote/BTC-USD/history?p=BTC-USD). This dataset includes all historical bitcoin price from **17 September 2014 until today**.

## Notebooks
- [Bitcoin Price Prediction using LSTM + Analysis](notebooks\Bitcoin_price_prediction_LSTM.ipynb)

## Requirements
- Elasticsearch 7.17.5
- Kibana 7.17.5
- Kafka 3.2.0
- Spark 3.3.1
- To install python packages run 
```bash
pip install -r requirements.txt
```
## Report

To document our work, we wrote repoart that is included in the [repo](docs\Report.pdf).

## Contributers
<h3> Karim Omrane & Zeineb Chebaane, INDP3 AIM, 2022 - 2023
