import pandas as pd
import datetime as dt
from mongo import Mongo
from FinMind.data import DataLoader
import sys
from dotenv import load_dotenv
import os
from mongo import Mongo
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
        records = df.to_dict(orient='records')
        for record in records:
            repo.send_document(record)
        # for _, row in df.iterrows():
        #     repo.send_document(row.to_dict())

    def update_data(self):
        repo = Mongo('trading_bot', 'stock_price')
        
        print('sending started')
        # Get the current day and weekday
        today = dt.datetime.now().strftime('%Y-%m-%d')
        weekday = dt.datetime.now().weekday()
        start_time = time.time()
  
        for stock_id in stock_id_list:
            try:
                stock_id = str(stock_id)
                df = repo.get_data_by_stock_id(str(stock_id))

                # If there is data in MongoDB, add new data
                if not df.empty:
                    current_date = pd.to_datetime(df['Timestamp'].iloc[-1])
                    
                    data_start_date = (current_date + pd.DateOffset(days=1)).strftime('%Y-%m-%d')
                    
                    if data_start_date == today and weekday < 5:
                        # print(f'{stock_id} is up-to-date')
                        continue
                    
                    if weekday >= 5:
                        # print("It's the weekend! Exiting the script.")
                        return

                    stock_data = dl.taiwan_stock_daily(stock_id=stock_id, start_date=(data_start_date))

                # If there is no data in the MongoDB, add data from the beginning of 2013
                else:
                    #print(f'Did not find the {stock_id} in the database. Sending data beginning from 2013.')
                    stock_data = dl.taiwan_stock_daily(stock_id=stock_id, start_date='2013-01-01')


                dfs = pd.concat([df,stock_data])
                self.send_to_repo(dfs, repo)
                # print(f'Sent {stock_id} to trading_bot stock_price')

            except:
               break
              #print(e)
              #print('Wait 1 hour and try again')
        #print('finished')
        
        # end_time = time.time()
        # execution_time = end_time - start_time
        # print(f'It takes {execution_time} to finish')
