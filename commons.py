import numpy as np
import pandas as pd
import datetime as dt
import csv

print 'Preparing data. Please wait.'     

sp500_composition=dict()
sp500_ticker=dict()
sp500_index=dict()
data_sp500_1st_date=dict()

local_path='C:/Users/kncas/OneDrive/Udacity/project/'
stats_path='C:/Users/kncas/Documents/Python Scripts/Project/stats/'

refresh_data=False

#init index
sp500_index['Consumer Discretionary']='GOOG/INDEXSP_SP500_25TR'
sp500_index['Consumer Staples']='GOOG/INDEXSP_SP500_30TR'
sp500_index['Energy']='GOOG/INDEXSP_SP500_10TR'
sp500_index['Financials']='GOOG/INDEXSP_SP500_40TR'
sp500_index['Health Care']='GOOG/INDEXSP_SP500_35TR'
sp500_index['Industrials']='GOOG/INDEXSP_SP500_20TR'
sp500_index['Information Technology']='GOOG/INDEXSP_SP500_45TR'
sp500_index['Materials']='GOOG/INDEXSP_SP500_15TR'
sp500_index['Telecommunications Services']='GOOG/INDEXSP_SP500_50TR'
sp500_index['Utilities']='GOOG/INDEXSP_SP500_55TR'

#alternative symbols, due to mismatch between the quandl databases
alternative_symbol=dict()
alternative_symbol['BF_B']='BFB'
alternative_symbol['BFB']='BF_B'
alternative_symbol['DISCK']='DISCA'
alternative_symbol['DISCA']='DISCK'
alternative_symbol['BRK_B']='BRKB'
alternative_symbol['BRKB']='BRK_B'

#y labels
y_labels=list(['1dd_Close','5dd_Close','20dd_Close','clr_cluster_0','clr_cluster_1','clr_cluster_2','clr_cluster_3',\
               'clr_cluster_4','chr_cluster_0','chr_cluster_1','chr_cluster_2','chr_cluster_3','chr_cluster_4'])
y_dd_labels=list(['1dd_Close','5dd_Close','20dd_Close'])
y_chr_clr_labels=list(['clr_cluster_0','clr_cluster_1','clr_cluster_2','clr_cluster_3',\
                       'clr_cluster_4','chr_cluster_0','chr_cluster_1','chr_cluster_2','chr_cluster_3','chr_cluster_4'])


#action codes for trading
action_code=dict()
action_code['buy']=1
action_code['sell']=-1
action_code['hold']=0
kpi=dict()
kpi['clr']=0
kpi['chr']=1

#codes for q_key
qkc=dict()
qkc['1dd_Close']=1
qkc['5dd_Close']=2
qkc['20dd_Close']=3
qkc['clr_cluster_0']=4
qkc['clr_cluster_1']=5
qkc['clr_cluster_2']=6
qkc['clr_cluster_3']=7
qkc['clr_cluster_4']=8
qkc['chr_cluster_0']=9
qkc['chr_cluster_1']=10
qkc['chr_cluster_2']=11
qkc['chr_cluster_3']=12
qkc['chr_cluster_4']=13
               
#load sp500 composition        
with open(local_path+'data/WIKI.csv','r') as csvfile:
    csvreader=csv.reader(csvfile, delimiter=',')
    for row in csvreader:
        if row[0]=='BF_B':
            row[0]='BFB'
        if row[0]=='DISCK':
            row[0]='DISCA'
        if row[0]=='BRK_B':
            row[0]='BRKB'
        if row[0]!='GOOG' and row[0]!='FOXA' and row[0]!='UA_C' and row[0]!='NWS':
            sp500_ticker[row[0]]=row[1]
    csvfile.close()

for k,v in sp500_ticker.items():
    a=sp500_composition.get(v,'nok')
    if a=='nok':
        sp500_composition[v]=list([k])
    else:
        b=sp500_composition[v]
        b.append(k)
        sp500_composition[v]=b

#load 1st dates
with open(local_path+'data/sp500_1st_date.csv','r') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',')
    for row in csvreader:
        data_sp500_1st_date[row[0]]=dt.datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')
    csvfile.close()            

#create date index
a=pd.DataFrame()
a=pd.read_hdf(local_path+'data/WIKI_SP500.h5','table')
date_index_internal=dict()
date_index_external=dict()
max_date=a.index[0]
l_i=0
for i in a.index:
    if i>max_date:
        max_date=i
    date_index_internal[i]=l_i
    date_index_external[l_i]=i
    l_i+=1
end_date=dt.datetime.strptime(str(dt.date.today())+' 00:00:00','%Y-%m-%d %H:%M:%S')
del a

#read datafrom from disc
def read_dataframe(file):
    try:
        return pd.read_hdf(file,'table')
    except IOError:
        return pd.DataFrame()
        
#assemble columns for forecasting
def Xy_columns(Xy_all,mode):
    modes=list(['Open','Close','Low','High'])
    modes.remove(mode) 
    
    select_columns=list([])            
    for c in Xy_all.columns:
        if mode in str(c):
            select_columns.append(c)
        else:
            m_found=False
            for m in modes:
                if m in str(c):
                    m_found=True
            if m_found==False:
                select_columns.append(c)
    return select_columns