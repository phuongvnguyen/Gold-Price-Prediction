"""
This program is the first step in developing a Machine Learning model to predict Gold prices. 
Indeed, it is an ETL process for data on the daily Gold price from the Yahoo Finance site. 
The specification requirements are pandas, yahoofinancials, and SQL.

Programer: Phuong Van Nguyen

Email: phuong.nguyen@economics.uni-kiel.de
"""

from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from datetime import datetime
from yahoofinancials import YahooFinancials
get_ipython().run_line_magic('reload_ext', 'sql')
import sqlite3

# One can modify the following starting and ending times base on his/her own needs.
start_time = '2010-01-01'
end_time= '2020-05-29' 

class preliminary():
    
    def __init__(self):
        print('\033[1m'+'Preliminary'+'\033[0m')
        self.nam_file='Ticker List.xlsx'
        self.ticker_symbol=self.read_file(self.nam_file)
        self.tickers,self.names=self.take_list(self.ticker_symbol)
        
    def read_file(self,name_file):
        print('I am exploring the ticker symbols on the Yahoo Finance \n...')
        self.data=pd.read_excel(name_file)
        self.no_obs=11
        print('The first %d Tickers with the descriptions'%self.no_obs)
        display(self.data.head(self.no_obs).T)
        return self.data
    
    def take_list(self,data):
        self.tickers=data['Ticker'].to_list()
        self.names=data['Description'].to_list()
        print('I am done!')
        return self.tickers,self.names

class extract():
    
    def __init__(self,start_time,end_time,preliminary):
        print('\033[1m'+'Extract'+'\033[0m')
        self.tickers=preliminary.tickers
        self.names=preliminary.names
        self.begin_time=start_time
        self.end_time=end_time
        self.freq="daily"
        self.extracted_data=self.load(self.tickers,self.begin_time,self.end_time,self.freq)
    
    def load(self,ticker,begin_time,end_time,freq):
        print('I am creating a Table to store data \n...')
        self.time_interval = pd.bdate_range(start=begin_time,end=end_time)
        self.table = pd.DataFrame({ 'Time': self.time_interval})
        self.table['Time']= pd.to_datetime(self.table['Time'])
        print('Table is created successfully!')
        print('I am extracting data from the Yahoo Finance\n...')
        for i in ticker:
            self.raw_data = YahooFinancials(i)
            self.raw_data = self.raw_data.get_historical_price_data(begin_time,end_time,freq)
            self.df = pd.DataFrame(self.raw_data[i]['prices'])[['formatted_date','adjclose']]
            self.df.columns = ['Time1',i]
            self.df['Time1']= pd.to_datetime(self.df['Time1'])
            self.table=self.table.merge(self.df,how='left',left_on='Time',right_on='Time1')
        self.table = self.table.drop(['Time1_x','Time1_y'],axis=1)
        self.no_obs=5
        print('The first %d observations of the extracted data shown below'%self.no_obs)
        display(self.table.head(self.no_obs).T)
        print('Data extraction is completed!')
        return self.table
    
class transform():
    
    def __init__(self,extract):
        print('\033[1m'+'Transform'+'\033[0m')
        self.names=extract.names
        self.extracted_data=extract.extracted_data
        self.renam_data=self.renam_columns(self.names,self.extracted_data)
        self.dropr_data=self.drop_data(self.renam_data)
        self.data_infor=self.get_info(self.dropr_data)
        self.fulfil_data=self.fill_miss(self.dropr_data) 
        
    def renam_columns(self,name,data):
        print('I am renaming the columns\n...')
        name.insert(0,'Time')
        data.columns = name
        self.data=data
        display(self.data[self.data.columns[0:7]].head(3))
        print('I am done!')
        return self.data
    
    def drop_data(self,data):
        print('I am dropping the first observations\n...')
        self.data=data.drop([0])
        display(self.data[self.data.columns[0:7]].head(3))
        print('I am done!')
        return self.data
    
    def get_info(self,data):
        print('I am exploring the general information on data\n...')
        self.infor_data=data.info()
        print('I am examining if there exists the missing data\n...')
        self.miss_data=data.isna().sum()
        print(self.miss_data)
        print('I got it!')
        return self.infor_data

    def fill_miss(self,data):
        print('I am fulfilling the missing data by the forward-to-next method\n...')
        self.data = data.fillna(method="ffill",axis=0)
        print(self.data.isna().sum())
        print('I am done!')
        return self.data  

class load():
    
    def __init__(self,transform):
        print('\033[1m'+'Extract'+'\033[0m]')
        self.data=transform.fulfil_data
        self.connect_sql=self.connect_database()
        self.insert_data=self.load_data(self.data)
        self.explor_tab=self.explor_table()
        self.nam_csv='Fin_data'
        self.export=self.export_data(self.data,self.nam_csv)
        
    def connect_database(self):
        print('I am trying to connect to the existed database\n...')
        self.connect = get_ipython().run_line_magic('sql', 'sqlite:///Phuong_database.db')
        print(self.connect)
        print('The connection is success!')
        self.table_list = get_ipython().run_line_magic('sql', "SELECT name FROM sqlite_master WHERE type='table'")
        display(self.table_list)
        return self.table_list
    
    def load_data(self,Finance_Yahoo):
        print('I am loading data to the database\n...')
        self.check = get_ipython().run_line_magic('sql', 'DROP TABLE IF EXISTS Finance_Yahoo')
        self.insert_data = get_ipython().run_line_magic('sql', 'PERSIST Finance_Yahoo')
        print('Data was inserted successfully!')
        return self.insert_data
    
    def explor_table(self):
        print('I am checking the inserted data\n...')
        self.explor_tab = get_ipython().run_line_magic('sql', 'SELECT * FROM finance_yahoo LIMIT 5')
        display(self.explor_tab)
        print('I am done!')
        return self.explor_tab
    
    
    def export_data(self,data,nam_file):
        print('I am exporting data to the csv and excel files\n...')
        self.export_csv=data.to_csv(nam_file+'.csv')
        self.export_excel=data.to_excel(nam_file+'.xlsx')
        print('Data was exported successfully!')
        return self.export_csv
    
class main():
    extract=extract(start_time,end_time,preliminary())
    transform=transform(extract)
    load=load(transform)
                         
if __name__=='__main__':
    main()