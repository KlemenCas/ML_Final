import commons
import pandas as pd
import numpy as np
import datetime as dt

        
class stock_market(object):
    data_sp500=pd.DataFrame()
    index_composition=dict()
    portfolio=dict()
    portfolio_log=dict()
    startdates=dict()

    def __init__(self):  
        self.initialize_index()
        self.data_sp500=pd.read_hdf(commons.local_path+'data/WIKI_SP500.h5','table')
        self.data_sp500=self.data_sp500.fillna(method='backfill')

        startdate=commons.end_date-dt.timedelta(days=180)
        for k,v in commons.sp500_index.items():
            index_t=v[-10:-2]
            if startdate<=min(self.index_composition[index_t].index):
                startdate=min(self.index_composition[index_t].index)
#            startdate=startdate+dt.timedelta(days=365)
            
            if startdate not in self.index_composition[index_t].index:
                while startdate not in self.index_composition[index_t].index:
                    startdate=startdate+dt.timedelta(days=1)   
            self.initialize_portfolio(k,commons.date_index_internal[startdate],1000000000)
            self.startdates[index_t]=startdate
    
    def initialize_index(self):
        for k,v in commons.sp500_index.items():
            index_t=v[-10:-2]    
            self.index_composition[index_t]=pd.read_hdf(commons.local_path+'data/'+index_t+'.h5','table',mode='r')
                        

    def get_index_portfolio(self,idx_external,dix):
        portfolio=dict()
        index_t=commons.sp500_index[idx_external][-10:-2]
        for t in commons.sp500_composition[idx_external]:
            if dix>=commons.date_index_internal[commons.data_sp500_1st_date[t]]: 
                portfolio[t]=self.index_composition[index_t].ix[commons.date_index_external[dix],t]
        return portfolio
        
    def index_portfolio_value(self,idx_external,dix):  
        index_t=commons.sp500_index[idx_external][-10:-2]
        value=0
        for ticker,volume in self.portfolio[index_t].items():
            value+=volume*self.get_closing_price(ticker,dix)
        return value
            
        
    def order_executable(self,ticker,dix,price,action):
        if (action==commons.action_code['buy'] and price>=float(self.data_sp500.ix[commons.date_index_external[dix],[ticker+'_Low']].values)) or\
           (action==commons.action_code['sell'] and price<=float(self.data_sp500.ix[commons.date_index_external[dix],[ticker+'_High']].values)):
            return True
        else:
            return False
            
    def get_closing_price(self,ticker,dix):
        price = self.data_sp500.ix[commons.date_index_external[dix],[ticker+'_Close']]
        if np.isnan(price.values):
            try:
                price = self.data_sp500.ix[commons.date_index_external[dix],[commons.alternative_symbol[ticker]+'_Close']]
                price=float(price.values)
            except KeyError:
                price=0
                print 'Ticker: ',ticker, 'Date: ', commons.date_index_external[dix],' set to zero.'
        else:
            price=float(price.values)
        return price
        
    def get_opening_price(self,ticker,dix):
        price = self.data_sp500.ix[commons.date_index_external[dix],[ticker+'_Open']]
        if np.isnan(price.values):
            try:
                price = self.data_sp500.ix[commons.date_index_external[dix],[commons.alternative_symbol[ticker]+'_Open']]
                price=float(price.values)
            except KeyError:
                price=0
        else:
            price=float(price.values)
        return price        

    def get_low_price(self,ticker,dix):
        price = self.data_sp500.ix[commons.date_index_external[dix],[ticker+'_Low']]
        if np.isnan(price.values):
            try:
                price = self.data_sp500.ix[commons.date_index_external[dix],[commons.alternative_symbol[ticker]+'_Low']]
                price=float(price.values)
            except KeyError:
                price=0
        else:
            price=float(price.values)
        return price   

    def get_high_price(self,ticker,dix):
        price = self.data_sp500.ix[commons.date_index_external[dix],[ticker+'_High']]
        if np.isnan(price.values):
            try:
                price = self.data_sp500.ix[commons.date_index_external[dix],[commons.alternative_symbol[ticker]+'_High']]
                price=float(price.values)
            except KeyError:
                price=0
        else:
            price=float(price.values)
        return price   
        
    def align_index_portfolio(self,dix):
        for k,v in commons.sp500_index.items():
            #sell everything and repurchase with the current index composition
            index_t=v[-10:-2]
            cash=0
            for ticker,volume in self.portfolio[index_t].items():
                cash+=volume*self.get_closing_price(ticker,dix)
            self.initialize_portfolio(k,dix,cash)
                
    def initialize_portfolio(self,idx_external,dix,budget):
        index_t=commons.sp500_index[idx_external][-10:-2]       
        self.portfolio[index_t]=dict()
        self.portfolio[index_t]=self.get_index_portfolio(idx_external,dix)
        portfolio=dict()
        for ticker,pct in self.portfolio[index_t].items():
            portfolio[ticker]=pct*budget/self.get_closing_price(ticker,dix)
        self.portfolio[index_t]=portfolio
        self.portfolio_log[index_t]=pd.DataFrame()
        
    def log_portfolio(self,dix,index_t):
        for k,v in self.portfolio.items():
            if k==index_t:
                p_log=pd.DataFrame()
                for ticker,volume in v.items():
                    p_log.ix[commons.date_index_external[dix],ticker]=volume
                self.portfolio_log[k]=self.portfolio_log[k].append(p_log)
                self.portfolio_log[k].to_hdf(commons.local_path+'data/Index_Portfolio_'+k+'.h5','table',mode='w')        
                
    def get_min_startdate(self):
        startdate=dt.datetime.today()
        for i,d in self.startdates.items():
            if startdate>d:
                startdate=d
        return startdate