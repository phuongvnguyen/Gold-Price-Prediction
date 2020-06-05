"""
This program is the second step in developing a Machine Learning model to predict Gold prices. 
Indeed, it is an ETL process for cleaning data on the daily Gold price. 
The specification requirements are pandas, yahoofinancials, and SQL.

Programer: Phuong Van Nguyen

Email: phuong.nguyen@economics.uni-kiel.de
"""

from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from datetime import datetime
get_ipython().run_line_magic('reload_ext', 'sql')
import sqlite3

class extract():
    
    def __init__(self):
        self.nam_file='Fin_data.csv'
        self.data=self.load_data(self.nam_file)

    def load_data(self,nam_file):
        print('I am loading data\n...')
        self.data=pd.read_csv(nam_file)
        self.no_cols=9
        self.no_obs=3
        print('The first %d observations of the first %d columns'%(self.no_obs,self.no_cols))
        #display(self.data[self.data.columns[0:self.no_cols]].head(self.no_obs))
        print('Data was loaded successfully!')
        return self.data 

class transform():
    
    def __init__(self,extract):
        self.data=extract.data
        self.dropc_data=self.drop_columns(self.data)
        self.name_col='Time'
        self.decimal_data=self.turn_numerical(self.dropc_data,self.name_col)
        self.select_columns = ['Gold','Silver', 'Crude Oil', 'S&P500','MSCI EM ETF']
        self.short_terms = [1,3,5,14,21]
        self.long_terms = [60,90,180,250]
        self.returns =self.percent_change(self.decimal_data,self.select_columns,self.short_terms,self.long_terms)                                           
        self.mva_gold=self.moving_average(self.decimal_data,self.name_col)
        self.combine_return=self.merge_data(self.returns,self.mva_gold,self.name_col)
        self.future_return=self.forward_return(self.decimal_data,self.name_col)
        self.final_data=self.finalize_data(self.combine_return,self.future_return,self.name_col)
            
    def drop_columns(self,data):
        self.data=data.drop('Unnamed: 0',axis=1)
        return self.data
        
    def turn_numerical(self,data,name_columns):
        self.cols=data.columns.drop(name_columns)
        data[self.cols] = data[self.cols].apply(pd.to_numeric,errors='coerce').round(decimals=1)
        self.data=data
        #display(self.data.head(3))
        return self.data
        
    def percent_change(self,data,name_columns,short_period,long_period):
        print('I am computing the percentage change\n...')
        self.data=pd.DataFrame(data=data['Time'])
        for i in short_period:
            self.x= data[name_columns].pct_change(periods=i).add_suffix("-T-"+str(i))
            self.data=pd.concat(objs=(self.data,self.x),axis=1)
        for i in long_period:
            self.x= data[name_columns].pct_change(periods=i).add_suffix("-T-"+str(i))
            self.data=pd.concat(objs=(self.data,self.x),axis=1)
        display(self.data[self.data.columns[0:9]].tail(5))
        print('I am done!')
        return self.data
    
    def moving_average(self,data,name_col):
        print('I am computing the moving average values\n...')
        self.moving_avg = pd.DataFrame(data[self.name_col],columns=[self.name_col])
        self.moving_avg[name_col]=pd.to_datetime(self.moving_avg[name_col],format='%Y-%m-%d')
        self.moving_avg['Gold/15SMA'] = (data['Gold']/(data['Gold'].rolling(window=15).mean()))-1
        self.moving_avg['Gold/30SMA'] = (data['Gold']/(data['Gold'].rolling(window=30).mean()))-1
        self.moving_avg['Gold/60SMA'] = (data['Gold']/(data['Gold'].rolling(window=60).mean()))-1
        self.moving_avg['Gold/90SMA'] = (data['Gold']/(data['Gold'].rolling(window=90).mean()))-1
        self.moving_avg['Gold/180SMA'] = (data['Gold']/(data['Gold'].rolling(window=180).mean()))-1
        self.moving_avg['Gold/90EMA'] = (data['Gold']/(data['Gold'].ewm(span=90,adjust=True,ignore_na=True).mean()))-1
        self.moving_avg['Gold/180EMA'] = (data['Gold']/(data['Gold'].ewm(span=180,adjust=True,ignore_na=True).mean()))-1
        self.moving_avg = self.moving_avg.dropna(axis=0)
        #print(self.moving_avg.head(5))
        print('I am done!')
        return self.moving_avg
    
    def merge_data(self,data_1,data_2,name_column):
        print('I am emerging two different data frames\n...')
        data_1[name_column]=pd.to_datetime(data_1[name_column],format='%Y-%m-%d')
        data_1 = pd.merge(left=data_1,right=data_2,how='left',on=name_column)
        self.data=data_1
        #display(self.data)
        print('I am done!')
        return self.data
    
    def forward_return(self,data,name_col):
        print('I am calculating the forward returns for the target feature\...')
        self.target=pd.DataFrame(data=data[name_col])
        #self.target=pd.to_datetime(self.target[name_col],format='%Y-%m-%d')
        self.target['Gold-T+14']=data["Gold"].pct_change(periods=-14)
        self.target['Gold-T+22']=data["Gold"].pct_change(periods=-22)
        #print(self.target.tail(3))
        print(self.target.shape)
        print(self.target.isna().sum())
        self.target = self.target[self.target['Gold-T+22'].notna()]
        #print(self.target.tail(3))
        #print(self.target.shape)
        #print(self.target.isna().sum())
        print('I am done!')
        return self.target
    
    def finalize_data(self,data_1,data_2,name_col):
        print('I am finalizing data for training the predictive model\n...')
        print(data_1.shape)
        #print(data_1['Gold-T-250'].head(4))
        data_1 = data_1[data_1['Gold-T-250'].notna()]
        print(data_1.shape)
        data_2=pd.to_datetime(data_2[name_col],format='%Y-%m-%d')
        self.data = pd.merge(left=data_1,right=data_2,how='inner',on=name_col,suffixes=(False,False))
        print(self.data.shape)
        #print(self.data.isna().sum())
        print('I am done!')
        return self.data
    
class load():
    
    def __init__(self,transform):
        self.data=transform.final_data
        self.connect_sql=self.connect_database()
        self.insert_data=self.load_data(self.data)
        self.explor_tab=self.explor_table()
        self.nam_file='Cleaned_Fin_data'
        self.export=self.export_data(self.data,self.nam_file)
        
    def connect_database(self):
        print('I am trying to connect to the existed database\n...')
        self.connect = get_ipython().run_line_magic('sql', 'sqlite:///Phuong_database.db')
        print(self.connect)
        print('The connection is success!')
        self.table_list = get_ipython().run_line_magic('sql', "SELECT name FROM sqlite_master WHERE type='table'")
        display(self.table_list)
        return self.table_list
    
    def load_data(self,Cleaned_Fin_data):
        print('I am loading data to the database\n...')
        self.check = get_ipython().run_line_magic('sql', 'DROP TABLE IF EXISTS Cleaned_Fin_data')
        self.insert_data = get_ipython().run_line_magic('sql', 'PERSIST Cleaned_Fin_data')
        print('Data was inserted successfully!')
        return self.insert_data
    
    def explor_table(self):
        print('I am checking the inserted data\n...')
        self.explor_tab = get_ipython().run_line_magic('sql', 'SELECT * FROM Cleaned_Fin_data LIMIT 5')
        display(self.explor_tab)
        print('I am done!')
        return self.explor_tab
        
    def export_data(self,data,nam_file):
        print('I am exporting the cleaned data to the csv and excel files\n...')
        self.export_csv=data.to_csv(nam_file+'.csv')
        self.export_excel=data.to_excel(nam_file+'.xlsx')
        print('Data was exported successfully!')
        return self.export_csv
                
class main():
    extract=extract()
    transform=transform(extract)
    load=load(transform)
    
if __name__=='__main__':
    main()