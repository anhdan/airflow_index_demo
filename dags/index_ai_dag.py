from datetime import datetime, timedelta
from airflow.decorators import dag, task
import requests
import pandas as pd
import os
import sys
import json

default_args = {
    'owner': 'danda',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id="index_ai_dag_v01",
    default_args=default_args,
    description="DAG to get, predict and validate stock price and exchange rate",
    start_date=datetime(2023, 2, 24),
    schedule_interval='* * * * *', # every minute
)
def index_ai_dag():

    #
    # Task to get stock price and exchange rate
    #
    @task()
    def get_stock_price_and_exchange_rate( timzezone: timedelta = timedelta(hours=7) ):

        # read time table file
        df = pd.read_csv( "dags/sample_index.csv", header=0 )
        print( df )

        # get current time
        now = datetime.now() + timzezone
        # add timezone
        current_time = now.strftime("%H:%M")
        if current_time.startswith("0"):
            current_time = current_time[1:]
        print("Current time: " + current_time)

        # find the index and exchange that need update at the current time
        to_get_list = []
        for i in range(df.shape[0]):
            row = df.iloc[i]
            try:
                idx = row[ row == current_time ].index[0]
                to_get_list.append( (row[0], row[1]) )
            except:
                continue
        
        # get the stock price and exchange rate
        data_list = []
        print( to_get_list )
        for symbol, is_forex in to_get_list:
            if is_forex:
                print( '----> symbol: ',  symbol )
                cur_from, cur_to = symbol.split("/")
                url = f'https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={cur_from}&to_currency={cur_to}&apikey=0B97ZWJBUSW6JDD8'
            else:
                url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey=0B97ZWJBUSW6JDD8'
            
            r = requests.get(url)
            data = r.json()
            data["retrieval_time"] = current_time
            data_list.append( data )
        
        return json.dumps(data_list)
    

    #
    #===== Predict stock price and exchange rate =====
    #
    @task()
    def validate_and_predict_stock_price_and_exchange_rate( data_list_json: str):
        data_list = json.loads(data_list_json)
        db_df = pd.read_csv( 'dags/db.csv', header=0)
        for data in data_list:
            retrieval_time = data["retrieval_time"]
            if "Global Quote" in data:
                # Get Latest Price from get_stock_price_and_exchange_rate task
                symbol = data["Global Quote"]["01. symbol"]
                latest_price = data["Global Quote"]["05. price"]
                print(f"Predicting stock price for {symbol} at {latest_price}")
                
                # Get Latest Price from database
                try:
                    db_df_symbol = db_df[ db_df["symbol"] == symbol ]
                    db_latest_price = db_df_symbol["latest"].values[-1]
                    db_predict_price = db_df["predict"].values[-1]
                except:
                    db_latest_price = latest_price
                    db_predict_price = latest_price
                
                # Predict
                new_predict_price = 2*float(latest_price) - float(db_latest_price) 
                print(f"Predicted stock price for {symbol} at {new_predict_price}")

                # Validate
                last_predict_error = abs(float(latest_price) - float(db_predict_price)) / float(latest_price)

                # Write to database
                db_df = db_df.append( {
                    "symbol": symbol, 
                    "latest": latest_price, 
                    "predict": new_predict_price, 
                    "last_predict_error": last_predict_error,
                    "retrieval_time": retrieval_time
                    }, ignore_index=True )
                db_df.to_csv( 'dags/db.csv', index=False, header=True )
                
            else:
                cur_from = data["Realtime Currency Exchange Rate"]["1. From_Currency Code"]
                cur_to = data["Realtime Currency Exchange Rate"]["3. To_Currency Code"]
                rate = data["Realtime Currency Exchange Rate"]["5. Exchange Rate"]
                print(f"Predicting exchange rate for {cur_from}/{cur_to} at {rate}")

                # Get latest exchange rate from database
                try:
                    db_df_symbol = db_df[ db_df["symbol"] == f"{cur_from}/{cur_to}" ]
                    db_rate = db_df_symbol["latest"].values[-1]
                    db_predict_rate = db_df["predict"].values[-1]
                except:
                    db_rate = rate
                    db_predict_rate = rate
                
                # Predict
                new_predict_rate = 2*float(rate) - float(db_rate)
                print(f"Predicted exchange rate for {cur_from}/{cur_to} at {new_predict_rate}")

                # Validate
                last_predict_error = abs(float(rate) - float(db_predict_rate)) / float(rate)

                # Write to database
                db_df = db_df.append( {
                    "symbol": f"{cur_from}/{cur_to}",
                    "latest": rate,
                    "predict": new_predict_rate,
                    "last_predict_error": last_predict_error,
                    "retrieval_time": retrieval_time
                    }, ignore_index=True )
                db_df.to_csv( 'dags/db.csv', index=False, header=True )



    #
    #===== Main DAG =====
    #
    data_list_json = get_stock_price_and_exchange_rate()
    validate_and_predict_stock_price_and_exchange_rate( data_list_json=data_list_json )

index_ai_dag = index_ai_dag()