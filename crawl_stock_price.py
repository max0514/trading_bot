import pandas as pd
import datetime as dt
from mongo import Mongo
from FinMind.data import DataLoader
import sys
from dotenv import load_dotenv
import os
def config():
    load_dotenv()


    
config()
stock_id_list = pd.read_csv('上市公司基本資料.csv')['公司代號'].to_list()
stock_id_list

#if life time expired error restart vscode
repo = Mongo('trading_bot',collection='stock_price')



dl = DataLoader()
dl.login_by_token(api_token=os.getenv('FINMIND_API_KEY'))
dl.login(user_id=os.getenv('FINMIND_USER_ID'),password=os.getenv('FINMIND_PASSWORD'))


from FinMind.data import DataLoader
import datetime as dt
import time
import sys

class stock_price_scrapper:
    def  __init__(self, stock_id_list= pd.read_csv('上市公司基本資料.csv')['公司代號'].to_list(), repo = Mongo(db='trading_bot',collection='stock_price')):#, config=__config()):
        self.stock_id_list = stock_id_list
        self.repo = repo
        #login to finmind
        self.dl = DataLoader()
        self.dl.login_by_token(api_token=os.getenv('FINMIND_API_KEY'))
        self.dl.login(user_id=os.getenv('FINMIND_USER_ID'),password=os.getenv('FINMIND_PASSWORD'))

    #     self.__config = config
    #     self.dl.login_by_token(api_token=self.__config.FINMIND_API_KEY)
    #     self.dl.login(user_id=self.__config.FINMIND_USER_ID,password=self.__config.FINMIND_PASSWORD

    def send_to_repo(self,df, repo):
        df.rename(columns={'date':'Timestamp'}, inplace=True)
        for _, row in df.iterrows():
            repo.send_document(row.to_dict())

    def update_data(self):
        repo = Mongo('trading_bot', 'stock_price')
        
        print('sending started')
        # Get the current day and weekday
        today = dt.datetime.now().strftime('%Y-%m-%d')
        weekday = dt.datetime.now().weekday()

        for stock_id in stock_id_list:
            try:
                stock_id = str(stock_id)
                df = repo.get_data_by_stock_id(str(stock_id))

                # If there is data in MongoDB, add new data
                if not df.empty:
                    latest_date = (pd.to_datetime(df['Timestamp'].iloc[-1]) + pd.DateOffset(days=1)).strftime('%Y-%m-%d')
                    
                    if latest_date == today and weekday < 5:
                        print(f'{stock_id} is up-to-date')
                        continue
                    
                    if weekday >= 5:
                        print("It's the weekend! Exiting the script.")
                        sys.exit()

                    stock_data = dl.taiwan_stock_daily(stock_id=stock_id, start_date=(latest_date))

                # If there is no data in the MongoDB, add data from the beginning of 2013
                else:
                    print(f'Did not find the {stock_id} in the database. Sending data beginning from 2013.')
                    stock_data = dl.taiwan_stock_daily(stock_id=stock_id, start_date='2013-01-01')

                self.send_to_repo(stock_data, repo)
                print(f'Sent {stock_id} to trading_bot stock_price')

            except Exception as e:
                print(e)
                print('Wait 1 hour and try again')


repo = Mongo('trading_bot',collection='stock_price')
repo.get_data_by_stock_id('1101')


stock_price_scrapper = stock_price_scrapper()
stock_price_scrapper.update_data()

repo.get_data_by_stock_id('1103')


stock_data = dl.taiwan_stock_tick(stock_id=2330, date='2023-05-22')
stock_data

repo = Mongo('trading_bot',collection='stock_price')
repo.get_all_data()

pd.DataFrame(repo.collection.find({}))





print(repo.get_data_by_stock_id('0000').empty)

import sys
import datetime as dt

def send_to_repo(df, repo):
    df.rename(columns={'date':'Timestamp'}, inplace=True)
    for _, row in df.iterrows():
        repo.send_document(row.to_dict())

def update_data():
    repo = Mongo('trading_bot', 'stock_price')

    # Estimate the time to finish
    hour_to_finish = len(stock_id_list) / 600
    print(f'It will take approximately {hour_to_finish} hours to run')
    
    # Get the current day and weekday
    today = dt.datetime.now().strftime('%Y-%m-%d')
    weekday = dt.datetime.now().weekday()

    for stock_id in stock_id_list:
        print(f'Sending {stock_id} to the database')

        try:
            stock_id = str(stock_id)
            df = repo.get_data_by_stock_id(str(stock_id))

            # If there is data in MongoDB, add new data
            if not df.empty:
                latest_date = (pd.to_datetime(df['Timestamp'].iloc[-1]) + pd.DateOffset(days=1)).strftime('%Y-%m-%d')
                
                if latest_date == today and weekday < 5:
                    print(f'{stock_id} is up-to-date')
                    continue
                
                if weekday >= 5:
                    print("It's the weekend! Exiting the script.")
                    sys.exit()

                stock_data = dl.taiwan_stock_daily(stock_id=stock_id, start_date=(latest_date))

            # If there is no data in the MongoDB, add data from the beginning of 2013
            else:
                print(f'Did not find the {stock_id} in the database. Sending data beginning from 2013.')
                stock_data = dl.taiwan_stock_daily(stock_id=stock_id, start_date='2013-01-01')

            dfs = pd.concat([df,stock_data])
            send_to_repo(stock_data, repo)
            print(f'Sent {stock_id} to trading_bot stock_price')

        except Exception as e:
            print(e)
            print('Wait 1 hour and try again')
            
            try:
                time.sleep(3600)
            except KeyboardInterrupt:
                print("Interrupted by user")
                return


repo = Mongo('trading_bot', 'stock_price')
repo.get_data_by_stock_id('2330')

repo = Mongo(db='trading_bot', collection='stock_price')
repo.get_data_by_stock_id('1101')

