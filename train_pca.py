import commons
import pandas as pd
import datetime as dt

#model
from sklearn.decomposition import PCA

#globals locally
local_path=commons.local_path
sp500_ticker=commons.sp500_ticker
sp500_index=commons.sp500_index
sp500_composition=commons.sp500_composition

#persistence
from sklearn.externals import joblib

data_sp500=pd.read_hdf(commons.local_path+'data/WIKI_SP500.h5','table')        

def train(mode):
    modes=list(['Open','Close','Low','High'])
    modes.remove(mode) 
    
    l_i=505     
    for l_pca in range(6,12):
        print 'pca: ', l_pca
        for k,v in sp500_ticker.items():
            l_i-=1
            Xy_all=pd.read_hdf(local_path+'data/Xy_all_'+str(k),'table')
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
            startdate=min(Xy_all.index)
            if startdate<dt.datetime.today()-dt.timedelta(days=1825):
                startdate=dt.datetime.today()-dt.timedelta(days=1825)
            else:
                startdate=min(Xy_all.index)
            Xy_all=Xy_all.ix[startdate:,select_columns]
            X_all=Xy_all.ix[:,:-8]
            #reduce space?
            if l_pca!=0:
                pca=PCA(n_components=l_pca)
                pca=pca.fit(X_all)
                X_all=pca.transform(X_all)
                joblib.dump(pca,local_path+'models/'+str(l_pca)+'_PCA_'+str(k)+'.pkl',compress=3)
                del pca

train('Close')