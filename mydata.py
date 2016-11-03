import commons
import urllib2
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import quandl as Quandl
import csv
import datetime as dt
from sklearn.preprocessing import MinMaxScaler

class mydata(object): 
    Quandl.ApiConfig.api_key='QWQMKc-NYsoYgtfadPZs'    
    
    sp500_composition=dict()
    sp500_ticker=dict()
    sp500_index=dict()

    data_sp500=pd.DataFrame()
    data_sp500_prices=pd.DataFrame()
    data_sp500_1st_date={}
    data_sp500_index=pd.DataFrame()
    data_sp500_fundamentals=pd.DataFrame()
    data_sp500_short_sell=pd.DataFrame()
    data_sp500_sentiment=pd.DataFrame()
    
    data_last_calloff=pd.DataFrame()
    
    ticker,ticker_open,ticker_high,ticker_low,ticker_close,ticker_volume=\
    pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame()

    data_sp500_1dr=pd.DataFrame()
    data_sp500_5dr=pd.DataFrame()
    data_sp500_20dr=pd.DataFrame()
    data_sp500_5dm=pd.DataFrame()
    data_sp500_sector_a_and_b=pd.DataFrame()
    data_sp500_30dsma=pd.DataFrame()
    data_sp500_30dmx=pd.DataFrame()
    data_sp500_30dmn=pd.DataFrame()    
    data_sp500_anb=pd.DataFrame()
    data_sp500_bbands=pd.DataFrame()
    data_sp500_clr=pd.DataFrame()
    data_sp500_chr=pd.DataFrame()
    data_sp500_ny_ss=pd.DataFrame()
    data_sp500_ns_ss=pd.DataFrame()
    data_sp500_1er=pd.DataFrame()
    data_sp500_2er=pd.DataFrame()
    data_sp500_5er=pd.DataFrame()
    
    
    sp500_new,sp500_obsolete=list([]),list([])

    last_date=dt.datetime.today()
    
    reload_baseline=False
    refresh_data=False
    demo_scenario=True

        
        
    def __init__(self,refresh_data=False,reload_baseline=False,demo_scenario=True):    
        self.demo_scenario=demo_scenario
        self.refresh_data=refresh_data
        self.local_path=commons.local_path 
        self.data_sp500_1st_date=commons.data_sp500_1st_date
        
        
    def get_sp500_list(self):
        if self.refresh_data==True:
            hdr = {'User-Agent': 'Mozilla/5.0'}
            site = 'http://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
            req = urllib2.Request(site, headers=hdr)
            page = urllib2.urlopen(req)
            soup = BeautifulSoup(page,"lxml")
        
            table = soup.find('table', {'class': 'wikitable sortable'})
            sector_tickers = dict()
            for row in table.findAll('tr'):
                col = row.findAll('td')
                if len(col) > 0:
                    sector = str(col[3].string.strip())
                    ticker = str(col[0].string.strip()).replace('.','_')
                    ticker = str(col[0].string.strip()).replace('-','_')
                    if sector not in sector_tickers:
                        sector_tickers[sector] = list()
                    sector_tickers[sector].append(ticker)
            self.sp500_composition=sector_tickers
            for k,v in self.sp500_composition.items():
                for i in v:
                    self.sp500_ticker[str(i).replace('.','_')]=k
        else:            
            with open(self.local_path+'data/WIKI.csv','r') as csvfile:
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
        self.sp500_index=commons.sp500_index
        if self.refresh_data==True:
            self.check_changes_sp500_composition()
            
        if self.demo_scenario:
            self.sp500_index=dict()
            self.sp500_index['Telecommunications Services']='GOOG/INDEXSP_SP500_50TR'
            
            demo_composition=dict()
            demo_composition['Telecommunications Services']=self.sp500_composition['Telecommunications Services']
            self.sp500_composition=demo_composition
            
            demo_ticker=dict()
            for symbol in demo_composition['Telecommunications Services']:
                demo_ticker[symbol]='Telecommunications Services'
            self.sp500_ticker=demo_ticker            

        
        
    def process_quandl_csv(self):
        #upload and process local csv
        wiki=pd.read_csv(self.local_path+'data/WIKI.csv',header=None, index_col=['Ticker','Date'],\
                         names=['Ticker','Date','Open','High','Low','Close','Volume','Ex-Dividend','Split Ratio',\
                         'Adj. Open','Adj. High','Adj. Low','Adj. Close', 'Adj. Volume'], usecols=['Ticker','Date',\
                         'Ex-Dividend','Split Ratio','Adj. Open','Adj. High','Adj. Low','Adj. Close', 'Adj. Volume'], parse_dates=[1])
        wiki.to_hdf(self.local_path+'data/WIKI.h5','table',mode='w')
        print 'hdf5 created from WIKI csv'
        #extract sp500 data
        wiki_sp500=pd.DataFrame()
        for k,v in self.sp500_ticker.items():
            values=wiki.ix[k].values
            index=wiki.ix[k].index
            columns=str(k)+'_'+wiki.ix[k].columns
            df=pd.DataFrame(data=values,index=index,columns=columns)
            wiki_sp500=wiki_sp500.join(df,how='outer')
            self.data_sp500_1st_date[k]=min(wiki.ix[k].index)
        wiki_sp500.dropna(axis=0,how='all',inplace=True)
        wiki_sp500.to_hdf(self.local_path+'data/WIKI_SP500.h5','table',mode='w')                    
        print 'WIKI based SP500 ticker data stored'



        
    def get_quandl_index(self,startdate=commons.max_date,enddate=commons.end_date):
        self.data_sp500_index=commons.read_dataframe(self.local_path+'data/SP500_index_data.h5')   
        enddate=max(self.data_sp500_index.index)
        if startdate!=enddate and self.refresh_data==True:
            self.data_sp500_index=pd.read_hdf(self.local_path+'data/SP500_index_data.h5','table',mode='r')
            for k,v in self.sp500_index.items():
                df=pd.DataFrame([])
                df=Quandl.get(v,start_date=startdate,end_date=enddate)
                df.columns=[str(v[-10:-2])+'_Open',str(v[-10:-2])+'_High',str(v[-10:-2])+'_Low',str(v[-10:-2])+'_Close',str(v[-10:-2])+'_Volume']
                for i in df.index:
                    for c in df.columns:
                            self.data_sp500_index.ix[i,c]=df.ix[i,c]
            self.data_sp500_index.dropna(axis=0,how='all',inplace=True)
            self.data_sp500_index=self.data_sp500_index.sort_index()
            self.data_sp500_index.to_hdf(self.local_path+'data/SP500_index_data.h5','table',mode='w')
            print 'Index prices retrieved and stored'
        else:
            self.data_sp500_index=pd.read_hdf(self.local_path+'data/SP500_index_data.h5','table')
            print 'Local index data loaded.'





    def refresh_wiki_sp500(self):
        #get local data
        self.data_sp500=commons.read_dataframe(self.local_path+'data/WIKI_SP500.h5','table')

        #has the sp500 composition changed?
        if self.sp500_new!=list([]) or self.sp500_obsolete!=list([]):
            for i in self.sp500_obsolete:
                for c in self.data_sp500.columns:
                    if i==c[0:len(i)]:
                        self.data_sp500.drop(c,axis=1,inplace=True)
            for i in self.sp500_new:
                new_columns=list([str(i)+'_Ex-Dividend',str(i)+'_Split Ratio',\
                                 str(i)+'_Adj. Open',str(i)+'_Adj. High',\
                                 str(i)+'_Adj. Low',str(i)+'_Adj. Close',\
                                 str(i)+'_Adj. Volume'])
                self.data_sp500=self.data_sp500.add(new_columns,axis=1)

        #get new ticker data
        max_date=max(self.data_sp500.index)
        if self.refresh_data==True and max_date!=commons.end_date:
            #collect unknown days and update data_SP500
            items=list([])
            for k,v in self.sp500_ticker.items():
                items.append('WIKI/'+str(k))
            df=pd.DataFrame([])
            df=Quandl.get(items,start_date=max_date+dt.timedelta(days=1),end_date=dt.date.today())
            columns=list([])
            for x in df.columns:
                x=str(x).replace(' - ', '_').strip('WIKI').strip('/')
                columns.append(x)
            df.columns=columns
    
            full_refresh_items=list([])
            for i in df.index:
                for c in df.columns:
                    if 'Adj.' in c:
                        target_c=c.replace('Adj. ','')
                        self.data_sp500.ix[i,target_c]=df.ix[i,c]
                    if 'Split Ratio'in c and df.ix[i,c]!=1.:
                        full_refresh_items.append('WIKI/'+str(c).strip('_Split Ratio'))
            print 'Missing SP500 data days retrieved'
            
            #recollect data where there was a split
            if len(full_refresh_items)!=0:
                df=pd.DataFrame([])
                df=Quandl.get(full_refresh_items,start_date='2001-01-01',end_date=dt.date.today())
                columns=list([])
                for x in df.columns:
                    x=str(x).replace(' - ', '_').strip('WIKI').strip('/')
                    columns.append(x)
                df.columns=columns
                for i in df.index:
                    for c in df.columns:
                        if 'Adj.' in c:
                            self.data_sp500.ix[i,c]=df.ix[i,c]
                print 'Data with split recollected'
            #update storage    
            self.data_sp500=self.data_sp500.sort_index()
            self.data_sp500.to_hdf(self.local_path+'data/WIKI_SP500.h5','table',mode='w')
            print 'sp500 data refreshed'

    def get_fundamentals(self):
        self.data_sp500_fundamentals=pd.read_hdf(self.local_path+'data/FUND_SP500.h5','table')
        self.data_last_calloff=pd.read_hdf(self.local_path+'data/last_calloff.h5','table')        
        
        if self.refresh_data==True:
            #get fundamentals
            if self.data_sp500_fundamentals.empty==True:
                max_date=dt.datetime.today()-dt.timedelta(days=1825)
            else:
                max_date=max(list(self.data_last_calloff.query('fundamentals > 0').index))
                
            if max_date<commons.end_date:    
                #collect unknown days and update data_SP500_fundamentals
                fundamentals=list(['_PB_MRY','_EPSDIL_MRY'])
                for fund in fundamentals:
                    items=list([])
                    for k,v in self.sp500_ticker.items():
                        items.append('SF0/'+str(k)+str(fund))
                    df=pd.DataFrame([])
                    df=Quandl.get(items,start_date=max_date+dt.timedelta(days=1),end_date=dt.date.today())
                    columns=list([])
                    for x in df.columns:
                        x=str(x).replace(' - ', '_').strip('SF0').strip('/').strip('_Value')
                        columns.append(x)
                    df.columns=columns
                    for i in df.index:
                        for c in df.columns:
                            self.data_sp500_fundamentals.ix[i,c]=df.ix[i,c]
                #update storage    
                self.data_sp500_fundamentals.to_hdf(self.local_path+'data/FUND_SP500.h5','table',mode='w')
                self.data_last_calloff.ix[dt.datetime.today(),'fundamentals']=1
                self.data_last_calloff.to_hdf(self.local_path+'data/last_calloff.h5','table',mode='w')             
                print 'Fundamentals data refreshed'
    
            else:                
                df=pd.DataFrame(index=pd.date_range(start=min(self.data_sp500_fundamentals.index),end=max(list(self.data_last_calloff.query('fundamentals > 0').index))))
                self.data_sp500_fundamentals=self.data_sp500_fundamentals.join(df,how='outer')
                print 'No new fundamentals data to collect' 

#fill up and clean up
        if self.data_sp500_fundamentals.empty!=True:
            missing_data=list([])
            missing_index=list([])
            for d in pd.date_range(start=min(self.data_sp500_1st_date.itervalues()),end=max(list(self.data_last_calloff.query('fundamentals > 0').index))):
                if (d not in self.data_sp500_fundamentals.index) and (d in self.data_sp500.index):
                    missing_index.append(d)
                    missing_data.append(np.NaN)
            missing_data=np.array(missing_data)
            df=pd.DataFrame(data=missing_data.reshape(-1,1),index=missing_index,columns=['bla'])
            self.data_sp500_fundamentals=self.data_sp500_fundamentals.join(df,how='outer')
            self.data_sp500_fundamentals=self.data_sp500_fundamentals.drop('bla',1)
            self.data_sp500_fundamentals=self.data_sp500_fundamentals.sort_index()
            #drop non trade days
            for k in self.data_sp500_fundamentals.index:
                if k not in self.data_sp500_index.index:
                    self.data_sp500_fundamentals.drop(k,inplace=True)                
             
    
    def get_short_sell(self):
        self.data_sp500_short_sell=pd.read_hdf(self.local_path+'data/SHORT_SP500.h5','table')
        max_date=max(self.data_sp500_short_sell.index)

        if self.refresh_data==True:        
            #get short sell
            if self.data_sp500_short_sell.empty==True:
                max_date=dt.datetime.today()-dt.timedelta(days=1825)
            else:
                max_date=max(self.data_sp500_short_sell.index)

            if max_date<commons.end_date:               
                short_sell=list(['FINRA/FNSQ_','FINRA/FNYX_'])
                for s in short_sell:
                    items=list([])
                    for k,v in self.sp500_ticker.items():
                        items.append(str(s)+str(k))
                    df=pd.DataFrame([])
                    df=Quandl.get(items,start_date=max_date+dt.timedelta(days=1),end_date=dt.date.today())
                    columns=list([])
                    for x in df.columns:
                        x=str(x).replace(' - ', '_').strip('FINRA').strip('/')
                        columns.append(x)
                    df.columns=columns
                    for i in df.index:
                        for c in df.columns:
                            self.data_sp500_short_sell.ix[i,c]=df.ix[i,c]
                #update storage    
                self.data_sp500_short_sell.to_hdf(self.local_path+'data/SHORT_SP500.h5','table',mode='w')
                print 'Short sell data refreshed'
            else:
                print 'No new short sell data to collect'   
                
    def get_sentiment(self):
        self.data_sp500_sentiment=pd.read_hdf(self.local_path+'data/SENT_SP500.h5','table')

        if self.refresh_data==True:                  
            #get sentiment
            if self.data_sp500_sentiment.empty==True:
                max_date=dt.datetime.today()-dt.timedelta(days=1825)
            else:
                max_date=max(self.data_sp500_sentiment.index)
                
            if max_date<dt.datetime.strptime(dt.datetime.today().strftime("%Y-%m-%d"),'%Y-%m-%d'):               
                df=pd.DataFrame([])
                df=Quandl.get('AAII/AAII_SENTIMENT',start_date=max_date+dt.timedelta(days=1),end_date=dt.date.today())
                for i in df.index:
                    for c in df.columns:
                        self.data_sp500_sentiment.ix[i,c]=df.ix[i,c]
                #update storage    
                self.data_sp500_sentiment.to_hdf(self.local_path+'data/SENT_SP500.h5','table',mode='w')
                print 'Sentiment data refreshed'
            else:
                print 'No new sentiment data to collect'                   



            
    def check_changes_sp500_composition(self):
        with open(self.local_path+'data/WIKI.csv','r') as csvfile:
            old_sp500=list([])
            csvreader=csv.reader(csvfile, delimiter=',')
            for row in csvreader:
                a=self.sp500_ticker.get(row[0],'nok')
                if a=='nok':
                    self.sp500_obsolete.append(row[0])
                old_sp500.append(row[0])
            csvfile.close()
         
        for i in old_sp500:
            a=self.sp500_ticker.get(i,'nok')    
            if a=='nok':
                self.sp500_new.append(i)
                
        #write new index composition to csv
        if self.sp500_new!=list([]) or self.sp500_obsolete!=list([]):
            print 'New sp500 symbols: ', self.sp500_new
            print 'Obsolete sp500 symbols: ', self.sp500_obsolete
            
            with open(self.local_path+'data/WIKI.csv','w') as csvfile:
                csvwriter = csv.writer(csvfile, delimiter=',')
                for k,v in self.sp500_ticker.items():
                    csvwriter.writerow([k,v])
                csvfile.close()
            print 'SP500 composition recorded'     
        else:
            print 'No changes in the SP500 composition'




            
    def get_sp_sector(self,ticker):
        return self.sp500_index[str(self.sp500_ticker[ticker])]
                                


    def sp500_fillna(self):
        self.data_sp500=self.data_sp500.fillna(method='backfill')
        self.data_sp500_fundamentals=self.data_sp500_fundamentals.fillna(method='backfill')
        self.data_sp500_short_sell=self.data_sp500_short_sell.fillna(method='backfill')
        self.data_sp500_sentiment=self.data_sp500_sentiment.fillna(method='backfill')
        self.data_sp500_index=self.data_sp500_index.fillna(method='backfill')
        self.data_last_calloff=self.data_last_calloff.fillna(method='backfill')   
        self.data_sp500_anb=self.data_sp500_anb.fillna(method='backfill')
        
        self.data_sp500=self.data_sp500.fillna(method='ffill')
        self.data_sp500_fundamentals=self.data_sp500_fundamentals.fillna(method='ffill')
        self.data_sp500_short_sell=self.data_sp500_short_sell.fillna(method='ffill')
        self.data_sp500_sentiment=self.data_sp500_sentiment.fillna(method='ffill')
        self.data_sp500_index=self.data_sp500_index.fillna(method='ffill')
        self.data_last_calloff=self.data_last_calloff.fillna(method='ffill')   
        self.data_sp500_anb=self.data_sp500_anb.fillna(method='ffill')
        print 'FillNa on source data performed.'
        


    def calc_indicators(self):        
        #select columns with low, high, open and close
        column_selection=list([])
        for c in self.data_sp500.columns:
            if c[-4:] in ['Open', 'lose', 'High', '_Low']:
                column_selection.append(c)
        self.data_sp500_prices=self.data_sp500.ix[:,column_selection]
        self.data_sp500=pd.DataFrame()

        #test=self.calc_sector_beta('SPG',dt.date(2015,9,1),dt.date(2016,9,1))
        
        #momentum 5 days
        self.data_sp500_5dm=(self.data_sp500_prices-self.data_sp500_prices.shift(5))/self.data_sp500_prices.shift(5)
        #momentum 2 days
        self.data_sp500_2dm=(self.data_sp500_prices-self.data_sp500_prices.shift(2))/self.data_sp500_prices.shift(2)
        #momentum 1 days
        self.data_sp500_1dm=(self.data_sp500_prices-self.data_sp500_prices.shift(1))/self.data_sp500_prices.shift(1)
        print 'Momentum calculated.'
        
        #delta to expected return
        for k,v in self.sp500_ticker.items():        
            select_columns=list([str(k)+'_Open',str(k)+'_Low',str(k)+'_High',str(k)+'_Close'])
            df1=self.data_sp500_1dm.ix[:,select_columns]
            df1.columns=list(['Open','Low','High','Close'])
            
            sector=self.get_sp_sector(k)
            sector_column=list([str(sector[-10:-2])+'_Open',str(sector[-10:-2])+'_High',str(sector[-10:-2])+'_Low',str(sector[-10:-2])+'_Close'])
            df2=self.data_sp500_index.ix[:,sector_column]        
            df2.columns=list(['Open','Low','High','Close'])
            
            select_columns=list(['B_'+str(k)+'_Open','B_'+str(k)+'_Low','B_'+str(k)+'_High','B_'+str(k)+'_Close'])
            df3=self.data_sp500_anb.ix[:,select_columns]
            df3.columns=list(['Open','Low','High','Close'])
            
            select_columns=list(['A_'+str(k)+'_Open','A_'+str(k)+'_Low','A_'+str(k)+'_High','A_'+str(k)+'_Close'])
            df4=self.data_sp500_anb.ix[:,select_columns]
            df4.columns=list(['Open','Low','High','Close'])
            
            df1=df1-((df2-df2.shift(1))/df2.shift(1)*df3+df4)
            df1.columns=list([str(k)+'_Open',str(k)+'_Low',str(k)+'_High',str(k)+'_Close'])            
            df1=df1.round(2)
            self.data_sp500_1er=self.data_sp500_1er.join(df1,how='outer')
            
            select_columns=list([str(k)+'_Open',str(k)+'_Low',str(k)+'_High',str(k)+'_Close'])
            df1=self.data_sp500_2dm.ix[:,select_columns]
            df1.columns=list(['Open','Low','High','Close'])

            df1=df1-df1-((df2-df2.shift(2))/df2.shift(2)*df3+df4)
            df1.columns=list([str(k)+'_Open',str(k)+'_Low',str(k)+'_High',str(k)+'_Close'])
            df1=df1.round(2)
            self.data_sp500_2er=self.data_sp500_2er.join(df1,how='outer')
            
            
            select_columns=list([str(k)+'_Open',str(k)+'_Low',str(k)+'_High',str(k)+'_Close'])
            df1=self.data_sp500_5dm.ix[:,select_columns]
            df1.columns=list(['Open','Low','High','Close'])

            df1=df1-df1-((df2-df2.shift(5))/df2.shift(5)*df3+df4)
            df1.columns=list([str(k)+'_Open',str(k)+'_Low',str(k)+'_High',str(k)+'_Close'])            
            df1=df1.round(2)
            self.data_sp500_5er=self.data_sp500_5er.join(df1,how='outer')
        print 'Delta to expected return.'
        
        #sma 30 days
        self.data_sp500_30dsma=self.data_sp500_prices/pd.DataFrame.rolling(self.data_sp500_prices,30).mean()
        #comp to max and min 30 days
        self.data_sp500_30dmx=self.data_sp500_prices/pd.DataFrame.rolling(self.data_sp500_prices,30).max()
        self.data_sp500_30dmn=self.data_sp500_prices/pd.DataFrame.rolling(self.data_sp500_prices,30).min()
        #vola week
        self.data_sp500_5dv=1-pd.DataFrame.rolling(self.data_sp500_prices,30).min()/pd.DataFrame.rolling(self.data_sp500_prices,30).max()
        #bollinger bands
        stock_rm_df=pd.DataFrame.rolling(self.data_sp500_prices,200).mean()
        self.data_sp500_bbands=(self.data_sp500_prices-stock_rm_df)/(2*self.data_sp500_prices.std(axis=0))     
        print 'min, max, sma, vola and bbbands calculated.'
        #returns for labels        
        self.data_sp500_1dr=(self.data_sp500_prices.shift(-1)/self.data_sp500_prices-1).round(2)*100
        self.data_sp500_5dr=(self.data_sp500_prices.shift(-5)/self.data_sp500_prices-1).round(2)*100
        self.data_sp500_20dr=(self.data_sp500_prices.shift(-20)/self.data_sp500_prices-1).round(2)*100
        #directional labels
        self.data_sp500_1dd=(self.data_sp500_prices.shift(-1)/self.data_sp500_prices-1)*100
        self.data_sp500_5dd=(self.data_sp500_prices.shift(-5)/self.data_sp500_prices-1)*100
        self.data_sp500_20dd=(self.data_sp500_prices.shift(-20)/self.data_sp500_prices-1)*100
        #close to low and close to high
        for k,v in self.sp500_ticker.items():        
            #close to low and close to high
            df1=pd.DataFrame(self.data_sp500_prices.ix[:,str(k)+'_Low'].shift(-1)/self.data_sp500_prices.ix[:,str(k)+'_Close']-1).round(2)*100
            df1.columns=list([str(k)+'_clr'])
            self.data_sp500_clr=self.data_sp500_clr.join(df1,how='outer')
            df1=pd.DataFrame(self.data_sp500_prices.ix[:,str(k)+'_High'].shift(-1)/self.data_sp500_prices.ix[:,str(k)+'_Close']-1).round(2)*100
            df1.columns=list([str(k)+'_chr'])
            self.data_sp500_chr=self.data_sp500_chr.join(df1,how='outer')
            
            #short %
            df1=pd.DataFrame(self.data_sp500_short_sell.ix[:,'FNSQ_'+str(k)+'_ShortVolume']/self.data_sp500_short_sell.ix[:,'FNSQ_'+str(k)+'_TotalVolume'])*10
            df1.columns=list([str(k)+'_ns_ss'])
            self.data_sp500_ns_ss=self.data_sp500_ns_ss.join(df1,how='outer')
            df1=pd.DataFrame(self.data_sp500_short_sell.ix[:,'FNYX_'+str(k)+'_ShortVolume']/self.data_sp500_short_sell.ix[:,'FNYX_'+str(k)+'_TotalVolume'])*10
            df1.columns=list([str(k)+'_ny_ss'])
            self.data_sp500_ny_ss=self.data_sp500_ny_ss.join(df1,how='outer')
            

        print 'Labels calculated.'
        #alpha & beta

        
        #fill, minmax and direction
        a=list(['1dm','2dm','5dm','30dsma','30dmx','30dmn','5dv','bbands','1dd','5dd','20dd','clr','chr','ny_ss','ns_ss','1er','2er','5er'])
        for x in a:
            setattr(self,'data_sp500_'+str(x),getattr(self,'data_sp500_'+str(x)).fillna(method='backfill'))
            setattr(self,'data_sp500_'+str(x),getattr(self,'data_sp500_'+str(x)).fillna(method='ffill'))
            
            a=list(['1dm','2dm','5dm','30dsma','30dmx','30dmn','5dv','bbands'])
        for x in a:
            setattr(self,'data_sp500_'+str(x),self.minmaxscale('data_sp500_'+str(x)))

        a=list(['1dd','5dd','20dd'])
        for x in a: 
            setattr(self,'data_sp500_'+str(x),self.p_direction(getattr(self,'data_sp500_'+str(x))))
            setattr(self,'data_sp500_'+str(x),self.n_direction(getattr(self,'data_sp500_'+str(x))))
            
        l_i=505
        for k,v in self.sp500_ticker.items():        
            Xy_all=self.assemble_xy(k)
            Xy_all=Xy_all.fillna(method='backfill')
            Xy_all=Xy_all.fillna(method='ffill')            
            Xy_all.to_hdf(self.local_path+'data/Xy_all_'+str(k),'table',mode='w')
            l_i-=1
            print 'Xy_all to '+str(k)+' assembled. '+str(l_i)+' to go.'
            
        #self.drop_obsolete_anb()#only needed 1x due to obsolete index.

            


            
    def get_adj_data(self,ticker,bucket,startdate,enddate):
        if bucket=='stock':
            stock_column=list([str(ticker)+'_Open',str(ticker)+'_High',str(ticker)+'_Low',str(ticker)+'_Close'])
            df=self.data_sp500_prices.ix[startdate:enddate+dt.timedelta(days=1),stock_column]
            df.columns=list(['Open','High','Low','Close'])
        if bucket=='sector':        
            sector=self.get_sp_sector(ticker)
            sector_column=list([str(sector[-10:-2])+'_Open',str(sector[-10:-2])+'_High',str(sector[-10:-2])+'_Low',str(sector[-10:-2])+'_Close'])
            df=self.data_sp500_index.ix[startdate:enddate+dt.timedelta(days=1),sector_column]        
            df.columns=list(['Open','High','Low','Close'])
        return df
        
        
        
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
            anb['B_'+str(ticker)+'_'+c]=covmat[0,1]/covmat[1,1]
            anb['A_'+str(ticker)+'_'+c]=np.mean(dfsm['Stock_'+str(c)+'_Return'])-(covmat[0,1]/covmat[1,1])*np.mean(dfsm['Sector_'+str(c)+'_Return'])  
        return anb        
        
        
        
    def calc_sector_betas(self,ticker):
        if ticker!='all':
            anb={}            
            min_date=self.data_sp500_1st_date[ticker]
            startdate=self.last_date-dt.timedelta(days=1825)
            if min_date>startdate:
                startdate=min_date
            enddate=startdate+dt.timedelta(days=365)
            if enddate<=self.last_date:
                while enddate<=self.last_date:
                    if enddate in self.data_sp500_index.index:
                        anb=self.calc_sector_beta(ticker,startdate,enddate)
                        for x,y in anb.items():
                            self.data_sp500_anb.ix[enddate,x]=y
                    startdate+=dt.timedelta(days=1)
                    enddate+=dt.timedelta(days=1)
            else:
                for i in self.data_sp500_index.ix[startdate:self.last_date].index:
                    for c in ['Open','Close','High','Low']:
                        anb['B_'+str(ticker)+'_'+c]=1
                        anb['A_'+str(ticker)+'_'+c]=0
                    for x,y in anb.items():
                        self.data_sp500_anb.ix[i,x]=y
            print 'Alpha and beta for:',ticker,' calculated.'
        else:            
            self.data_sp500_anb=pd.read_hdf(self.local_path+'data/anb.h5','table')
            
            if self.refresh_data==True and commons.end_date!=max(self.data_sp500_anb.index):
                max_date=max(self.data_sp500_anb.index)
                for k in self.data_sp500_anb.index:
                    if k not in self.data_sp500_index.index:
                        self.data_sp500_anb.drop(k,inplace=True)  
                        
                l_i=500
                for k,v in self.sp500_ticker.items():
                    min_date=self.data_sp500_1st_date[k]
                    startdate=max_date-dt.timedelta(days=365)
                    if min_date>startdate:
                        startdate=min_date
                    enddate=startdate+dt.timedelta(days=365)
        
                    while enddate<=commons.end_date:
                        if enddate not in self.data_sp500_anb.index:                        
                            if enddate in self.data_sp500_index.index:
                                anb=self.calc_sector_beta(k,startdate,enddate)
                                for x,y in anb.items():
                                    self.data_sp500_anb.ix[enddate,x]=y
                        startdate+=dt.timedelta(days=1)
                        enddate+=dt.timedelta(days=1)
                    l_i-=1
                    print 'Alpha and beta for:',k,' calculated; ',str(l_i),' to go.'
                self.data_sp500_anb.to_hdf(self.local_path+'data/anb.h5','table',mode='w')
                print 'Alpha and Beta have been calculated and stored locally'
            else:
                self.data_sp500_anb=pd.read_hdf(self.local_path+'data/anb.h5','table')
                for k in self.data_sp500_anb.index:
                    if k not in self.data_sp500_index.index:
                        self.data_sp500_anb.drop(k,inplace=True)            
                print 'Local Alpha and Beta loaded. Refresh==False'  
            


    def correct_nan_anb(self):
        for k,v in self.sp500_ticker.items():        
            df1=self.data_sp500_anb.ix[dt.date(2016,10,7),['B_'+str(k)+'_Open']]                                       
            if np.isnan(df1.values)==True:
                print 'Correcting '+str(k)
                self.calc_sector_betas(k)
        self.data_sp500_anb.to_hdf(self.local_path+'data/anb.h5','table',mode='w')
            
    def reset_data_sp500_columns(self):
        columns=self.data_sp500.columns
        new_columns=list([])
        for c in columns:
            new_columns.append(c.replace('Adj. ',''))
        self.data_sp500.columns=new_columns

            
    def calc_sp500_1st_date(self):
        if self.reload_baseline==True:
            for k,v in self.sp500_ticker.items():
                self.data_sp500_1st_date[k]=min(self.data_sp500.query(str(k)+'_Open > 0.').index)
            with open(self.local_path+'data/sp500_1st_date.csv','w') as csvfile:
                csvwriter = csv.writer(csvfile, delimiter=',')
                for k,v in self.data_sp500_1st_date.items():
                    csvwriter.writerow([k,v])
                csvfile.close()
            print '1st dates recorded'


    def minmaxscale(self,df):
        min_max_scaler = MinMaxScaler()
        if len(getattr(self,df).index)>0:
            return pd.DataFrame(data=min_max_scaler.fit_transform(getattr(self,df)).round(2),\
                                                       index=getattr(self,df).index,\
                                                       columns=getattr(self,df).columns)            
        else:
            print 'No data for '+str(df)
            
    def p_direction(self,df):
        df[df>0]=1
        return df
        
    def n_direction(self,df):
        df[df<0]=-1
        return df



    def calc_fundamentals(self,ticker,indicator):
        for k,v in self.sp500_index.items():
            columns=list([])
            for t in self.sp500_composition[k]:
                columns.append(str(t)+indicator)
        if self.data_sp500_1st_date[ticker]<min(self.data_sp500_fundamentals.index):
            min_date=min(self.data_sp500_fundamentals.index)
        else:
            min_date=self.data_sp500_1st_date[ticker]   
        ret=self.data_sp500_fundamentals.ix[min_date:,columns].mean(axis=1).to_frame()
        ret.columns=list([str(indicator).strip('_MRY')])
        return ret
        
                
            
    def assemble_xy(self, ticker):
        df=pd.DataFrame()
        min_max_scaler = MinMaxScaler()        
        
        #sentiment            
        select_columns=list(['Bull-Bear Spread'])
        target_columns=list(['Bull_Bear_Spread'])
        np1=self.data_sp500_sentiment.ix[self.data_sp500_1st_date[ticker]:,select_columns].values
        id1=self.data_sp500_sentiment.ix[self.data_sp500_1st_date[ticker]:,select_columns].index
        df1=pd.DataFrame(data=np1,index=id1,columns=target_columns)
        df1=pd.DataFrame(data=min_max_scaler.fit_transform(df1).round(2),index=df1.index,columns=df1.columns)
        df=df.join(df1,how='outer')   
     
        #fundamentals
        a=list(['_PB_MRY','_EPSDIL_MRY'])
        for x in a:
            select_columns=list([str(ticker)+str(x)])
            target_columns=list([str(x).strip('_MRY')])
            if self.data_sp500_1st_date[ticker]<min(self.data_sp500_fundamentals.index):
                min_date=min(self.data_sp500_fundamentals.index)
            else:
                min_date=self.data_sp500_1st_date[ticker]
            np1=self.data_sp500_fundamentals.ix[min_date:,select_columns].values
            id1=self.data_sp500_fundamentals.ix[min_date:,select_columns].index
            df1=pd.DataFrame(data=np1,index=id1,columns=target_columns)
            df1=df1/self.calc_fundamentals(ticker,x)-1
            np1=np.nan_to_num(df1.values)
            df1=pd.DataFrame(data=np1,index=id1,columns=target_columns)            
            df=df.fillna(method='backfill')
            df=df.fillna(method='ffill')
            df=df.fillna(value=0)  
            df1=pd.DataFrame(data=min_max_scaler.fit_transform(df1).round(2),index=df1.index,columns=df1.columns)
            df=df.join(df1,how='outer')

        #rest, incl labels
        select_columns=list([str(ticker)+'_Open',str(ticker)+'_Low',str(ticker)+'_High',str(ticker)+'_Close'])
        a=list(['1dm','2dm','5dm','30dsma','30dmx','30dmn','5dv','bbands','1er','2er','5er'])
        for x in a:
            target_columns=list([str(x)+'_Open',str(x)+'_Low',str(x)+'_High',str(x)+'_Close'])
            np1=getattr(self,'data_sp500_'+str(x)).ix[self.data_sp500_1st_date[ticker]:,select_columns].values
            id1=getattr(self,'data_sp500_'+str(x)).ix[self.data_sp500_1st_date[ticker]:,select_columns].index
            df1=pd.DataFrame(data=min_max_scaler.fit_transform(np.nan_to_num(np1)),index=id1,columns=target_columns)
            df=df.join(df1,how='outer')
            
        a=list(['_ns_ss','_ny_ss'])
        for x in a:
            select_columns=list([str(ticker)+str(x)])
            target_columns=list([str(x)])
            np1=getattr(self,'data_sp500'+str(x)).ix[self.data_sp500_1st_date[ticker]:,select_columns].values
            np1=np.nan_to_num(np1)
            np1=np1.astype(int)   
            id1=getattr(self,'data_sp500'+str(x)).ix[self.data_sp500_1st_date[ticker]:,select_columns].index
            df1=pd.DataFrame(data=np1,index=id1,columns=target_columns)
            df=df.join(df1,how='outer')                 

        select_columns=list([str(ticker)+'_Open',str(ticker)+'_Low',str(ticker)+'_High',str(ticker)+'_Close'])
        a=list(['1dr','5dr','20dr','1dd','5dd','20dd'])
        for x in a:
            target_columns=list([str(x)+'_Open',str(x)+'_Low',str(x)+'_High',str(x)+'_Close'])
            np1=getattr(self,'data_sp500_'+str(x)).ix[self.data_sp500_1st_date[ticker]:,select_columns].values
            np1=np.nan_to_num(np1)
            np1=np1.astype(int)            
            id1=getattr(self,'data_sp500_'+str(x)).ix[self.data_sp500_1st_date[ticker]:,select_columns].index
            df1=pd.DataFrame(data=np1,index=id1,columns=target_columns)
            df=df.join(df1,how='outer')
            
        a=list(['_clr','_chr'])
        for x in a:
            select_columns=list([str(ticker)+str(x)])
            target_columns=list([str(x)])
            np1=getattr(self,'data_sp500'+str(x)).ix[self.data_sp500_1st_date[ticker]:,select_columns].values
            np1=np1.astype(int)
            id1=getattr(self,'data_sp500'+str(x)).ix[self.data_sp500_1st_date[ticker]:,select_columns].index
            df1=pd.DataFrame(data=np1,index=id1,columns=target_columns)
            df=df.join(df1,how='outer')            
        
        df=df.fillna(method='backfill')
        df=df.fillna(method='ffill')
        df=df.fillna(value=0)        

        return df
        
    def drop_obsolete_anb(self):
        for k in self.data_sp500_anb.index:
            if k not in self.data_sp500_index.index:
                self.data_sp500_anb.drop(k,inplace=True)
        self.data_sp500_anb.to_hdf(self.local_path+'data/anb.h5','table',mode='w')