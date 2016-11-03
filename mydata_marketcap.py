import commons
import csv
import datetime as dt
import numpy as np
import pandas as pd
import quandl as Quandl
Quandl.ApiConfig.api_key='QWQMKc-NYsoYgtfadPZs' 


#globals locally
local_path=commons.local_path
sp500_ticker=commons.sp500_ticker
sp500_index=commons.sp500_index
sp500_composition=commons.sp500_composition
data_sp500_1st_date=commons.data_sp500_1st_date


data_sp500=commons.read_dataframe(local_path+'data/WIKI_SP500.h5')
data_last_calloff=commons.read_dataframe(local_path+'data/last_calloff.h5')


data_sp500_marketcap=pd.DataFrame()
data_sp500_marketcap=commons.read_dataframe(local_path+'data/MARKETCAP.h5')

#fill missing index
if data_sp500_marketcap.empty!=True:
    missing_data=list([])
    missing_index=list([])
    for d in pd.date_range(start=min(data_sp500_1st_date.itervalues()),end=max(list(data_last_calloff.query('marketcap > 0').index))):
        if (d not in data_sp500_marketcap.index) and (d in data_sp500.index):
            missing_index.append(d)
            missing_data.append(np.NaN)
    missing_data=np.array(missing_data)
    df=pd.DataFrame(data=missing_data.reshape(-1,1),index=missing_index,columns=['bla'])
    data_sp500_marketcap=data_sp500_marketcap.join(df,how='outer')
    data_sp500_marketcap=data_sp500_marketcap.drop('bla',1)
    data_sp500_marketcap=data_sp500_marketcap.sort_index()
    data_sp500_marketcap=data_sp500_marketcap.fillna(method='backfill')
    data_sp500_marketcap=data_sp500_marketcap.fillna(method='ffill')

#if refresh==True in commons, check Quandl       
if commons.refresh_data==True:    
    if data_sp500_marketcap.empty==True:
        max_date=dt.datetime.today()-dt.timedelta(days=1825)
    else:
        max_date=max(list(data_last_calloff.query('marketcap > 0').index))
        
    if max_date<dt.datetime.strptime(dt.datetime.today().strftime("%Y-%m-%d"),'%Y-%m-%d'):    
        max_date=dt.date.today()-dt.timedelta(days=1825)
        
        #collect unknown days and update data_SP500_marketcap
        items=list([])
        for k,v in sp500_ticker.items():
            items.append('SF1/'+str(k)+'_MARKETCAP')
        df=pd.DataFrame([])
        df=Quandl.get(items,start_date=max_date+dt.timedelta(days=1),end_date=dt.date.today())
        columns=list([])
        for x in df.columns:
            x=str(x).replace(' - ', '_').strip('SF1').strip('_Value').strip('/')
            columns.append(x)
        df.columns=columns
        for i in df.index:
            for c in df.columns:
                data_sp500_marketcap.ix[i,c]=df.ix[i,c]
        
        #update storage    
        data_sp500_marketcap.to_hdf(local_path+'data/MARKETCAP.h5','table',mode='w')
        
        data_last_calloff.ix[dt.datetime.today(),'marketcap']=1
        data_last_calloff.to_hdf(local_path+'data/last_calloff.h5','table',mode='w')
                                    
        print 'Marketcap data refreshed'
    
    else:
        df=pd.DataFrame(index=pd.date_range(start=min(data_sp500_marketcap.index),end=max(list(data_last_calloff.query('marketcap > 0').index))))
        data_sp500_marketcap=data_sp500_marketcap.join(df,how='outer')
        #drop non trade days
        for k in data_sp500_marketcap.index:
            if k not in data_sp500.index:
                data_sp500_marketcap.drop(k,inplace=True)                
        print 'No new marketcap data to collect'   
else:
    print 'Market Cap data loaded.'

#retrieve or calculate index composition
startdate=dt.datetime.today()-dt.timedelta(days=1825)

for k,v in sp500_index.items():
    index=sp500_index[k][-10:-2]
    vars()[index]=pd.DataFrame()

for d in data_sp500.index:
    if d>=startdate:
        for k,v in sp500_composition.items():
            index=sp500_index[k][-10:-2]    
            cum_marketcap=0
            for t in v: #calc market cap of index
                if d>=data_sp500_1st_date[t]:
                    cum_marketcap+=data_sp500_marketcap.ix[d,str(t)+'_MARKETCAP']
            for t in v: # calc weights
                if d>=data_sp500_1st_date[t]:                
                    vars()[index].ix[d,t]=data_sp500_marketcap.ix[d,str(t)+'_MARKETCAP']/cum_marketcap
print 'Index composition calculated.'

for k,v in sp500_index.items():
    index=sp500_index[k][-10:-2]    
    vars()[index].to_hdf(local_path+'data/'+index+'.h5','table',mode='w')    
print 'Index composition stored.'
#free memory
del data_sp500