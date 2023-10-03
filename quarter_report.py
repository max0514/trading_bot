#!/usr/bin/env python
# coding: utf-8

import datetime
import random
import time
import requests
import pandas as pd
pd.options.mode.chained_assignment = None
from io import StringIO
from mongo import Mongo


class AllFinancialStatementsScraper:
    
    def __init__(self, start_year=2013,start_season=1, end_year=datetime.datetime.now().year,
                balance_sheet_repo = Mongo(db='trading_bot', collection='balance_sheet'),
                income_sheet_repo = Mongo(db='trading_bot', collection='income_sheet'),
                cash_flow_repo = Mongo(db='trading_bot', collection='cash_flow')
                 ):
        if start_year < 2013 or start_year < 0:
            raise ValueError("start_year 必須大於等於 2013 並且不能為負數")
        if end_year < start_year or end_year < 0:
            raise ValueError("end_year 必須大於等於 start_year 並且不能為負數")
        if end_year > datetime.datetime.now().year:
            print('end_year 強制設定為今年')
            self.end_year = datetime.datetime.now().year
            self.year_now = self.end_year
        else:
            self.end_year = end_year
            self.year_now = datetime.datetime.now().year
        
        self.start_year = start_year
        self.income_statements_dfs = []
        self.balance_sheet_dfs = []
        self.cash_flow_dfs = []
        self.dfs = []
        
        self.year_range = list(range(self.start_year, self.end_year+1))
        self.season_range = list(range(1, 5))
        self.max_retries = 0 
        self.start_season = start_season



        self.balance_sheet_repo = balance_sheet_repo
        self.income_sheet_repo = income_sheet_repo
        self.cash_flow_repo = cash_flow_repo

    def get_all_statements_hst(self, stock_id=2330): # need to choose a stock id
            self.stock_id = stock_id

            # Calculate the newInfo_days for 財報發佈日 
            # func 2 is related
            mar_newInfo_day_B = self.get_newInfo_day(1 + self.end_year, 3, 31)
            mar_newInfo_day = self.get_newInfo_day(self.end_year, 3, 31)
            may_newInfo_day = self.get_newInfo_day(self.end_year, 5, 15)
            aug_newInfo_day = self.get_newInfo_day(self.end_year, 8, 14)
            nov_newInfo_day = self.get_newInfo_day(self.end_year, 11, 14)

            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

            # Loop through each year and quarter to retrieve income statements
                
            for year in self.year_range:
                print(f'{year}年開爬!!')

                #if start_season != 1 start from the start_season
                if self.start_season !=1 and year == self.start_year:
                    #change the season to start from start_season

                    self.season_range = list(range(self.start_season, 5))
                else:
                    #no need to change season range
                    pass
                for season in self.season_range:                
                    if year == self.end_year:
                        if (year == self.year_now - 1) & (season == 4):
                                if datetime.datetime.now() < mar_newInfo_day_B:
                                    print('已爬至歷史最新資料，正常結束。 產出主鍵：年、季度。')
                                    
                                    return self.balance_sheet_dfs, self.income_statements_dfs, self.cash_flow_dfs
                        
                        elif year == self.year_now:
                            checkTool = (mar_newInfo_day, may_newInfo_day, aug_newInfo_day, nov_newInfo_day)
                            for i, newInfo_day in enumerate(checkTool):
                                if (datetime.datetime.now() < newInfo_day) & (season >= i):
                                    print('已爬至歷史最新資料，正常結束。 產出主鍵：年、季度。')

                                    return self.balance_sheet_dfs, self.income_statements_dfs, self.cash_flow_dfs
                    ### main part ###    
                    # func 3 is related
                    self.scrape_all_statement(year, season, headers, mar_newInfo_day_B, mar_newInfo_day, may_newInfo_day, aug_newInfo_day, nov_newInfo_day)
                    ### main part ###
                
                    if (year != self.start_year) & (season == 1):
                        time.sleep(random.uniform(4, 9))
                        print('每逢第一季，休息N秒')
                        
            print('正常結束，無錯誤。 產出主鍵：年、季度。')
            
            
            return self.balance_sheet_dfs, self.income_statements_dfs, self.cash_flow_dfs
    
    # func 2
    def get_newInfo_day(self, y, m, d):
        newInfo_day = datetime.datetime(y, m, d)
        if newInfo_day.weekday() == 5:  # Saturday
            newInfo_day += datetime.timedelta(days=2)
        elif newInfo_day.weekday() == 6:  # Sunday
            newInfo_day += datetime.timedelta(days=1)
        return newInfo_day
    
    # func 3
    def scrape_all_statement(self, year, season, headers, mar_newInfo_day_B, mar_newInfo_day, may_newInfo_day, aug_newInfo_day, nov_newInfo_day):
        while True and self.max_retries < 3:
            C_or_A = 'C'
            something_wrong = False

            url = f'https://mops.twse.com.tw/server-java/t164sb01?t203sb01Form=t203sb01Form&step=1&CO_ID={self.stock_id}&SYEAR={year}&SSEASON={season}&REPORT_ID=C'

            url_A = 'https://mops.twse.com.tw/server-java/t164sb01'
            payload = {
                'step': 1,
                'CO_ID': str(self.stock_id),
                'SYEAR': str(year),
                'SSEASON': str(season),
                'REPORT_ID': 'C'
            }

            try:
                response = requests.get(url, headers=headers)
                response.encoding='big5'
                tables = pd.read_html(StringIO(response.text))
                try:
                    #資產負債表= balance_sheet
                    #綜合損益表 = income_statement
                    #現金流量表 = cash_flow
                    # >=2019 balance_sheet = dfs[0]
                    #<2019 balance_sheet = dfs[1]
                    if year >= 2019:
                        
                        balance_sheet_df = tables[0]
                        Incomestatement_df = tables[1]
                        cash_flow_df = tables[2]
                    else:
                        balance_sheet_df = tables[1]
                        Incomestatement_df = tables[2]
                        cash_flow_df = tables[3]
                except Exception as e:
                    something_wrong = True
                    print(f'於{year}Q{season}發生【錯誤1】:若於出現則自動休眠(16s)再重跑')
                    print('get error')
                    print(f'於{year}Q{season}發生{e}')
                    self.max_retries+=1
                    time.sleep(16)
            except:
                try:
                    C_or_A = 'A'
                    res = requests.post(url_A, data=payload, headers=headers)
                    res.encoding = 'big5'
                    tables = pd.read_html(StringIO(res.text))
                    if year >= 2019:
                        
                        balance_sheet_df = tables[0]
                        Incomestatement_df = tables[1]
                        cash_flow_df = tables[2]
                    else:

                        balance_sheet_df = tables[1]
                        Incomestatement_df = tables[2]
                        cash_flow_df = tables[3]
                    
                except Exception as e:
                    something_wrong = True
                    print(f'於{year}Q{season}發生【錯誤1】:若於出現則自動休眠(16s)再重跑')
                    print('post error')
                    print(f'於{year}Q{season}發生{e}')
                    self.max_retries+=1
                    time.sleep(16)


            if something_wrong == False:
                if C_or_A == 'C':
                    print('---這是合併財報---')
                    try:
                        #Incomestatement_df = pd.read_html(str(Incomestatement_table))[0]
                        if year >= 2019:
                            Incomestatement_data_dict = self.process_df_after_2019(Incomestatement_df,year=year,season=season)

                        else:
                            Incomestatement_data_dict = self.process_df_before_2019(Incomestatement_df,year=year,season=season)
                        self.income_statements_dfs.append(Incomestatement_data_dict)
                        #self.dfs.append(df)
                        print('stock:' + str(self.stock_id), 'period:' + str(year), 'Q' + str(season), 'finished')
                    except Exception as e:
                        something_wrong = True
                        print(f'於{year}Q{season}發生【錯誤2】:若於出現則自動休眠(16s)再重跑')
                        print(e)
                        self.max_retries+=1
                        time.sleep(16)

                     # Process the Balance Sheet data frame
                    try:
                        #balance_sheet_df = pd.read_html(str(balance_sheet_table))[0]
                        if year >= 2019:
                            balance_sheet_data_dict = self.process_df_after_2019(balance_sheet_df, year=year, season=season)
                            #print(balance_sheet_data_dict)
                        else:
                            balance_sheet_data_dict = self.process_df_before_2019(balance_sheet_df, year=year, season=season)
                            #print(balance_sheet_data_dict)

                        self.balance_sheet_dfs.append(balance_sheet_data_dict)
                        print('stock:' + str(self.stock_id), 'period:' + str(year), 'Q' + str(season), 'Balance Sheet finished')
                    except Exception as e:
                        something_wrong = True
                        print(f'Balance Sheet: Error in {year}Q{season}')
                        print(e)
                        self.max_retries+=1
                        time.sleep(16)

                    # Process the Cash Flow data frame
                    try:
                        #cash_flow_df = pd.read_html(str(cash_flow_table))[0]
                        if year >= 2019:
                            cash_flow_data_dict = self.process_df_after_2019(cash_flow_df, year=year, season=season)
                        else:
                            cash_flow_data_dict = self.process_df_before_2019(cash_flow_df, year=year, season=season)

                        self.cash_flow_dfs.append(cash_flow_data_dict)
                        print('stock:' + str(self.stock_id), 'period:' + str(year), 'Q' + str(season), 'Cash Flow finished')
                    except Exception as e:
                        something_wrong = True
                        print(f'Cash Flow: Error in {year}Q{season}')
                        print(e)
                        self.max_retries+=1
                        time.sleep(16)
                else:
                    print('---這是個別財報---')
                    try:
                        #Incomestatement_df = pd.read_html(str(Incomestatement_table))[0]
                        #income_statement_info_1season = [year, season, df]
                        if year >= 2019:
                            Incomestatement_data_dict = self.process_df_after_2019(Incomestatement_df,year=year,season=season)
                        else:
                            Incomestatement_data_dict = self.process_df_before_2019(Incomestatement_df,year=year,season=season)

                        self.income_statements_dfs.append(Incomestatement_data_dict)
                        #self.dfs.append(df)
                        print('stock:' + str(self.stock_id), 'period:' + str(year), 'Q' + str(season), 'finished')
                    except Exception as e:
                        something_wrong = True
                        print(f'於{year}Q{season}發生【錯誤3】:若於出現則自動休眠(16s)再重跑')
                        print(e)
                        self.max_retries+=1
                        time.sleep(16)
                         # Process the Balance Sheet data frame
                    try:
                        #balance_sheet_df = pd.read_html(str(balance_sheet_table))[0]
                        if year >= 2019:
                            balance_sheet_data_dict = self.process_df_after_2019(balance_sheet_df, year=year, season=season)
                        else:
                            balance_sheet_data_dict = self.process_df_before_2019(balance_sheet_df, year=year, season=season)

                        self.balance_sheet_dfs.append(balance_sheet_data_dict)
                        print('stock:' + str(self.stock_id), 'period:' + str(year), 'Q' + str(season), 'Balance Sheet finished')
                    except Exception as e:
                        something_wrong = True
                        print(f'Balance Sheet: Error in {year}Q{season}')
                        self.max_retries+=1
                        time.sleep(16)

                    # Process the Cash Flow data frame
                    try:
                        #cash_flow_df = pd.read_html(str(cash_flow_table))[0]
                        if year >= 2019:
                            cash_flow_data_dict = self.process_df_after_2019(cash_flow_df, year=year, season=season)
                        else:
                            cash_flow_data_dict = self.process_df_before_2019(cash_flow_df, year=year, season=season)

                        self.cash_flow_dfs.append(cash_flow_data_dict)
                        print('stock:' + str(self.stock_id), 'period:' + str(year), 'Q' + str(season), 'Cash Flow finished')
                    except Exception as e:
                        something_wrong = True
                        print(f'Cash Flow: Error in {year}Q{season}')
                        self.max_retries+=1
                        time.sleep(16)

            time.sleep(random.uniform(0.5, 1.5))

            if something_wrong == False:
                break
    
      
    def process_df_after_2019(self, df,year,season):
        # data = df.transpose()
        # data.reset_index(inplace=True)
        # season_data = data.iloc[2]
        # season_data[0] = f'{year}Q{season}'
        # index_to_terms_dict = {index: term for index, term in enumerate(season_data)}
        #return index_to_terms_dict
        index = df.iloc[:,1]
        values = df.iloc[:,2]
        #values
        data = pd.Series(values.values, index=index.values)
        data.dropna(inplace=True)

        # Create a new row to append as a Series
        new_row = pd.Series([f'{self.stock_id}',f'{year}Q{season}'], index=['stock_id','Timestamp'])

        # Append the new row to the original Series at the top
        updated_series = pd.concat([new_row, data])

        dict_data = updated_series.to_dict()
        return dict_data  
    def process_df_before_2019(self, df, year, season):
        index = df.iloc[:,0]
        values = df.iloc[:,1]
        #values
        data = pd.Series(values.values, index=index.values)
        data.dropna(inplace=True)

        # Create a new row to append as a Series
        new_row = pd.Series([f'{self.stock_id}',f'{year}Q{season}'], index=['stock_id','Timestamp'])

        # Append the new row to the original Series at the top
        updated_series = pd.concat([new_row, data])

        dict_data = updated_series.to_dict()
        return dict_data



        


# if __name__ == '__main__': 
   
#     # example 3
#     scraper = AllFinancialStatementsScraper(start_season=2) #實際在抓歷史資料時，用默認值即可
#     balance_sheet, income_sheet, cash_flow = IS_scraper.get_all_statements_hst(2337) # 3372 is a good example showing 2 kind of 財報（合併 or 個別）
    
def update_financial_statements():
    repo = Mongo(db='trading_bot', collection='balance_sheet')
    stock_id_list = repo.get_stock_id_list()
    for stock_id in stock_id_list:
        print(f'working on {stock_id}')
        try:
            #the stock
            Timestamp = repo.get_latest_data_date(stock_id=str(stock_id))
            if Timestamp != None:
                #don't mess up season and year max!!!!!
                current_season = int(Timestamp[-1])
                current_year = int(Timestamp[:4])
                print(Timestamp)
            if Timestamp == None:
                scraper = AllFinancialStatementsScraper()
                pass
            #if current season =4 jump to next year
            elif current_season == 4:
                scraper = AllFinancialStatementsScraper(start_year=current_year+1, start_season=1)
            else:

                scraper = AllFinancialStatementsScraper(start_year=current_year, start_season=current_season+1)


            balance_sheets, income_sheets, cash_flows= scraper.get_all_statements_hst(stock_id=stock_id)
            if balance_sheets == None:
                print(f'stock_id:{stock_id} to up to date')
                break


            for i in range(len(balance_sheets)):
                try:

                    scraper.balance_sheet_repo.send_document(balance_sheets[i])
                    scraper.income_sheet_repo.send_document(income_sheets[i])
                    scraper.cash_flow_repo.send_document(cash_flows[i])
                except Exception as e:
                    print(f'stock_id:{stock_id} sending error')
                    print(e)
            print(f'{stock_id} is finished')
        except Exception as e:
            print(f'error at stock_id {stock_id}:')
            print(e)
if __name__ == '__main__': 
   update_financial_statements()
