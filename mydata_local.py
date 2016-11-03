import urllib2
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import quandl as Quandl
import csv
import datetime as dt

class mydata_local(object): 
    sp500_composition=dict()
    sp500_ticker=dict()
    sp500_index=dict()

    data_sp500=pd.DataFrame()
    data_sp500_1st_date={}
    data_sp500_index=pd.DataFrame()
    data_sp500_daily_return=pd.DataFrame()
    data_sp500_5day_return=pd.DataFrame()
    data_sp500_20day_return=pd.DataFrame()
    data_sp500_5day_momentum=pd.DataFrame()
    data_sp500_sector_a_and_b=pd.DataFrame()
    data_sp500_30day_sma=pd.DataFrame()
    data_sp500_30day_max=pd.DataFrame()
    data_sp500_30day_min=pd.DataFrame()
    data_sp500_fundamentals=pd.DataFrame()
    data_sp500_short_sell=pd.DataFrame()
    data_sp500_sentiment=pd.DataFrame()

    alpha_and_beta=pd.DataFrame()
    

        
        
    def __init__(self):    
        self.init_sp500_ticker()
        self.refresh_wiki_sp500()
        self.get_quandl_index()
        self.calc_bbands('AAPL',dt.datetime.today()-dt.timedelta(days=1460),dt.datetime.today())


        
        
    def get_sp500_list(self):
        with open('C:\Users\kncas\OneDrive\Udacity\Project\data\WIKI.csv','r') as csvfile:
            csvreader=csv.reader(csvfile, delimiter=',')
            for row in csvreader:
                self.sp500_ticker[row[0]]=row[1]
            csvfile.close()
        
        for k,v in self.sp500_ticker.items():
            a=self.sp500_composition.get(v,'nok')
            if a=='nok':
                self.sp500_composition[v]=list([k])
            else:
                b=self.sp500_composition[v]
                b.append(k)
                self.sp500_composition[v]=b
        print 'Local ticker symbols and SP500 composition loaded'        



        
    def init_sp500_ticker(self):
        self.get_sp500_list()
        self.sp500_index['Consumer Discretionary']='GOOG/INDEXSP_SP500_25'
        self.sp500_index['Consumer Staples']='GOOG/INDEXSP_SP500_30'
        self.sp500_index['Energy']='GOOG/INDEXSP_SP500_10'
        self.sp500_index['Financials']='GOOG/INDEXSP_SP500_40'
        self.sp500_index['Health Care']='GOOG/INDEXSP_SP500_35'
        self.sp500_index['Industrials']='GOOG/INDEXSP_SP500_20'
        self.sp500_index['Information Technology']='GOOG/INDEXSP_SP500_45'
        self.sp500_index['Materials']='GOOG/INDEXSP_SP500_15'
        self.sp500_index['Telecommunications Services']='GOOG/INDEXSP_SP500_50'
        self.sp500_index['Utilities']='GOOG/INDEXSP_SP500_55'


        
        
    def get_quandl_index(self,startdate=dt.date.today()-dt.timedelta(days=730),enddate=dt.date.today()):
        self.data_sp500_index=pd.read_hdf('C:\Users\kncas\OneDrive\Udacity\Project\data\SP500_index_data.h5','table')
        print 'Local index data loaded'





    def refresh_wiki_sp500(self):
        self.data_sp500=pd.read_hdf('C:\Users\kncas\OneDrive\Udacity\Project\data\WIKI_SP500.h5','table')
        self.data_sp500_fundamentals=pd.read_hdf('C:\Users\kncas\OneDrive\Udacity\Project\data\FUND_SP500.h5','table')
        self.data_sp500_short_sell=pd.read_hdf('C:\Users\kncas\OneDrive\Udacity\Project\data\SHORT_SP500.h5','table')
        self.data_sp500_sentiment=pd.read_hdf('C:\Users\kncas\OneDrive\Udacity\Project\data\SENT_SP500.h5','table')

        print 'Local data loaded. Refresh==False'



            
            
    def get_sp_sector(self,ticker):
        return self.sp500_index[str(self.sp500_ticker[ticker])]




        
    def calc_sector_beta(self,ticker,startdate,enddate):
        #collect data
        stock_df=self.get_adj_data(ticker,'stock',startdate,enddate)
        sector_df=self.get_adj_data(ticker,'sector',startdate,enddate)
        #resample
        rts = stock_df.resample('W').last()
        rbts = sector_df.resample('W').last()
        dfsm=pd.DataFrame()
        for c in stock_df.columns:
            dfs=pd.DataFrame({'Stock_'+str(c):rts[c],'Sector_'+str(c):rbts[c]},index=rts.index)
            dfsm = dfsm.join(dfs,how='outer')
        # compute returns
        dfsm[['Sector_Open_Return','Stock_Open_Return','Sector_High_Return','Stock_High_Return','Sector_Low_Return',\
            'Stock_Low_Return','Sector_Close_Return','Stock_Close_Return']] = dfsm/dfsm.shift(1)-1
        dfsm = dfsm.dropna()
        anb={}
        for c in sector_df.columns:
            covmat = np.cov(dfsm['Stock_'+str(c)+'_Return'],dfsm['Sector_'+str(c)+'_Return'])
            anb['Beta_'+str(ticker)+'_'+c]=covmat[0,1]/covmat[1,1]
            anb['Alpha_'+str(ticker)+'_'+c]=np.mean(dfsm['Stock_'+str(c)+'_Return'])-self.beta[c]*np.mean(dfsm['Sector_'+str(c)+'_Return'])  
        return anb
  

        
      
    def get_adj_data(self,ticker,bucket,startdate,enddate):
        if bucket=='stock':
            stock_column=list([str(ticker)+'_Adj. Open',str(ticker)+'_Adj. High',str(ticker)+'_Adj. Low',str(ticker)+'_Adj. Close'])
            df=self.data_sp500.ix[startdate:enddate+dt.timedelta(days=1),stock_column]
            df.columns=list(['Open','High','Low','Close'])
        if bucket=='sector':        
            sector=self.get_sp_sector(ticker)
            sector_column=list([str(sector[-8:])+'_Open',str(sector[-8:])+'_High',str(sector[-8:])+'_Low',str(sector[-8:])+'_Close'])
            df=self.data_sp500_index.ix[startdate:enddate+dt.timedelta(days=1),sector_column]        
            df.columns=list(['Open','High','Low','Close'])
        return df
  


      
    def calc_bbands(self,ticker,startdate,enddate):
        stock_df=self.get_adj_data(ticker,'stock',startdate,enddate)
        stock_rm_df=pd.DataFrame()
        stock_rm_df=pd.DataFrame.rolling(stock_df,200).mean()
        bb=(stock_df-stock_rm_df)/(2*stock_df.std(axis=0))
        return bb.ix[max(bb.index)]



        
    def calc_returns(self):
        self.data_sp500_daily_return=self.data_sp500/self.data_sp500.shift(1)-1
        self.data_sp500_5day_return=self.data_sp500/self.data_sp500.shift(5)-1
        self.data_sp500_20day_return=self.data_sp500/self.data_sp500.shift(20)-1
        


        
    def calc_sector_betas(self):
        if self.refresh_data==True:
            for k,v in self.sp500_ticker:
                min_date=self.data_sp500_1st_date[k]
                startdate=dt.datetime.today()-dt.timedelta(days=365)
                enddate=dt.datetime.today()
    
                while startdate>=min_date:
                    anb=self.calc_sector_beta(k,startdate,enddate)
                    for x,y in anb.items():
                        self.alpha_and_beta.ix[enddate,x]=y
                    startdate-=dt.timedelta(days=1)
                    enddate-=dt.timedelta(days=1)
            self.alpha_and_beta.to_hdf('C:\Users\kncas\OneDrive\Udacity\Project\data\alpha_and_beta.h5','table',mode='w')
            print 'Alpha and Beta have been calculated and stored locally'
        else:
            self.alpha_and_beta=pd.read_hdf('C:\Users\kncas\OneDrive\Udacity\Project\data\alpha_and_beta.h5','table')
            print 'Alpha and Beta loaded'

            
            
    def calc_indicators(self):
        #momentum 5 days
        self.data_sp500_5day_momentum=(self.data_sp500-self.data_sp500.shift(5))/self.data_sp500.shift(5)
        #sma 30 days
        self.data_sp500_30day_sma=self.data_sp500/pd.DataFrame.rolling(self.data_sp500,30).mean()
        #comp to max and min 30 days
        self.data_sp500_30day_max=self.data_sp500/pd.DataFrame.rolling(self.data_sp500,30).max()
        self.data_sp500_30day_min=self.data_sp500/pd.DataFrame.rolling(self.data_sp500,30).min()
        #vola week
        self.data_sp500_5day_vola=1-pd.DataFrame.rolling(self.data_sp500,30).min()/pd.DataFrame.rolling(self.data_sp500,30).max()
        
        
            
