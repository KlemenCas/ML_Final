import commons
import numpy as np
import pandas as pd
import datetime as dt
import csv
from sklearn.cluster import KMeans
from sklearn.grid_search import GridSearchCV
from sklearn.metrics import make_scorer
from sklearn.cross_validation import train_test_split
from sklearn.svm import SVC
from sklearn.metrics import accuracy_score
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.metrics import mean_squared_error
from sklearn import tree
from sklearn.decomposition import PCA

from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB

#globals locally
local_path=commons.local_path
sp500_ticker=commons.sp500_ticker
sp500_index=commons.sp500_index
sp500_composition=commons.sp500_composition

#persistence
from sklearn.externals import joblib

accuracy_results=pd.DataFrame()
data_sp500=pd.read_hdf(commons.local_path+'data/WIKI_SP500.h5','table')        

def predict_labels(clf,X,y):
    y_pred = clf.predict(X)
    return accuracy_score(y, y_pred)   

#log is needed to be able to select the best model during forecasting
def write_log(pca,ticker,model,y,kf):
    row=list()
    row.append(pca)
    row.append(ticker)
    row.append(model)
    row.append(y)
    row.append(kf)
    f=open(commons.stats_path+'stats.csv', 'a')
    writer = csv.writer(f, delimiter=',')
    writer.writerow(row)
    f.close

#this logs the best parameters. No really needed, as the best performing configuration is
#being stored as pickle object
def write_parameters(pca,ticker,model,kpi,parameters,action):
    log=dict()
    log['pca']=pca
    log['ticker']=ticker
    log['model']=model
    log['kpi']=kpi
    log['kernel']=''
    log['C']=0
    log['max_depth']=0
    log['n_neighbors']=0
    log['weights']=''
    log['algorithm']=''
    log['action']=action
    
    if model=='SVC':
        log['kernel']=parameters['kernel']
        log['C']=parameters['C']
    if model=='RF' or model=='DT':
        log['max_depth']=parameters['max_depth']
    if model=='kN':
        log['n_neighbors']=parameters['n_neighbors']
        log['weights']=parameters['weights']
        log['algorithm']=parameters['algorithm']

    fieldnames=['pca','ticker','model','kpi','kernel','C','max_depth','n_neighbors','weights','algorithm','action']

    f=open(commons.stats_path+'parameters.csv', 'a')
    writer = csv.DictWriter(f,fieldnames=fieldnames)
    writer.writerow(log)
    f.close    

#the values to the label price change close-to-low and close-to-high are being clustered and the
#label is afterwards whether the expected prince change will be in a certain cluster
def write_cluster(pca,ticker,kf,cluster):
    row=list()
    row.append(ticker)
    row.append(kf)
    row.append(cluster)
    f=open(commons.stats_path+'cluster.csv', 'a')
    writer = csv.writer(f, delimiter=',')
    writer.writerow(row)
    f.close

#this is only being used to check whether we already know the model. If so then 
#the training is being skipped
def get_parameters(pca,ticker,model,kpi):
    fieldnames=['pca','ticker','model','kpi','kernel','C']
    f=open(commons.stats_path+'parameters.csv', 'r')
    reader = csv.DictReader(f,fieldnames=fieldnames)
    parameters=dict()
    for row in reader:
        if row['pca']==str(pca) and row['ticker']==ticker and row['model']==model and row['kpi']==kpi:
            parameters=row
    f.close        
    return parameters

#the actual training
def train(mode):
    stats_accuracy=dict()
    stats_model=dict()
    stats_kf=dict()

    #remove all prices that are not relevant    
    modes=list(['Open','Close','Low','High'])
    modes.remove(mode) 
    
    l_i=505     
    l_pca=0 #note!: pca=0 means that there will be no PCA performed on the features.
    for k,v in sp500_ticker.items():
        print 'Ticker:',str(k),', l_i=',str(l_i)
        l_i-=1
        #select relevant columns from Xy_all
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
        #reduce dimension space?
        if l_pca!=0:
            pca=PCA(n_components=l_pca)
            pca=pca.fit(X_all)
            X_all=pca.transform(X_all)
            del pca

        #get labels and drop the % forecast, as not relevant for the investment decision
        y_all=Xy_all.ix[:,-8:]
        y_all=y_all.drop(['1dr_Close', '5dr_Close', '20dr_Close'],1)
        del Xy_all
        
        #prep y; tranform the %expectation from the Xy_all to cluster assignment
        for y in y_all.columns:
            if y=='_clr' or y=='_chr':
                np_overall=dict()
                np_overall[0]=list()    
                np_overall[1]=list()    
                np_overall[2]=list()    
                np_overall[3]=list()    
                np_overall[4]=list()                  
                kmeans=KMeans(n_clusters=5, random_state=0).fit(y_all.ix[:,[y]])
                write_cluster(l_pca,k,y,kmeans.cluster_centers_.reshape(1,-1))                
                for x in y_all[y].values:
                    l_i1=0
                    distance=100.
                    kmeans.cluster_centers_.reshape(1,-1)[0].sort()
                    for c in kmeans.cluster_centers_.reshape(1,-1)[0]:
                        if distance>abs((x-c)):
                            distance=x-c
                            cluster=l_i1
                        l_i1+=1
                    for i in range(0,5):
                        if i==cluster:
                            np_overall[i].append(1)
                        else:
                            np_overall[i].append(0)
                            
                for i in range(0,5):
                    if np_overall[i].count(1)>=3 and np_overall[i].count(0)>=3:
                        np1=np.array(np_overall[i])
                        df1=pd.DataFrame(data=np1.reshape(-1,1),index=y_all.index,columns=[y[1:]+'_cluster_'+str(i)])
                        y_all=y_all.join(df1,how='outer')
        
        y_all=y_all.drop(['_chr','_clr'],1)

        #train        
        scorer = make_scorer(mean_squared_error)            

        for y in y_all.columns:
            print '-'+str(y)
            #get training data
            X_train, X_test, y_train, y_test = train_test_split(X_all, y_all[y], train_size = .7, random_state = 1) 
            #SVC                            
            parameters=dict()
            parameters=get_parameters(l_pca,k,'SVC',y)
            if any(parameters):
               print 'Trained already.'                        
            else:
                write_parameters(l_pca,k,'SVC',y,grid_obj.best_params_,'in_process')
                clf = SVC()                                                                                
                parameters = {'kernel':('linear', 'poly', 'rbf', 'sigmoid'), 'C':[1,10,100]}
                grid_obj = GridSearchCV(clf, parameters, scoring = scorer, n_jobs=1)
                grid_obj = grid_obj.fit(X_all, y_all[y])
                clf = grid_obj.best_estimator_
                stats_kf[y]=predict_labels(clf, X_test, y_test)
                write_parameters(l_pca,k,'SVC',y,grid_obj.best_params_,'done')
                write_log(l_pca,k,'SVC',y,stats_kf[y])
                stats_model['SVC']=stats_kf
                joblib.dump(clf,local_path+'models/SVC_'+str(k)+'_'+str(y)+'.pkl',compress=3)
                del grid_obj,clf

#        #Random Forest
            parameters=dict()
            parameters=get_parameters(l_pca,k,'RF',y)
            if any(parameters):
               print 'Trained already.'                        
            else:                
                write_parameters(l_pca,k,'RF',y,grid_obj.best_params_,'in_process')    
                parameters = {'max_depth':(1,2,3,4,5,6,7,8,9)}
                clf=RandomForestClassifier()
                grid_obj = GridSearchCV(clf,parameters,scoring = scorer)
                grid_obj = grid_obj.fit(X_train, y_train)
                clf=grid_obj.best_estimator_
                stats_kf[y]=predict_labels(clf, X_test, y_test)
                write_parameters(l_pca,k,'RF',y,grid_obj.best_params_,'done')                
                write_log(l_pca,k,'RF',y,stats_kf[y])                
                joblib.dump(clf,local_path+'models/RF_'+str(k)+'_'+str(y)+'.pkl',compress=3)
                del clf
                stats_model['RF']=stats_kf

#        #Decision Tree                
            parameters=dict()
            parameters=get_parameters(l_pca,k,'DT',y)
            if any(parameters):
               print 'Trained already.'                        
            else:
                write_parameters(l_pca,k,'DT',y,grid_obj.best_params_,'in_process')
                parameters = {'max_depth':(1,2,3,4,5,6,7,8,9)}
                clf=tree.DecisionTreeClassifier()
                grid_obj = GridSearchCV(clf,parameters,scoring = scorer)
                grid_obj = grid_obj.fit(X_train, y_train)
                clf=grid_obj.best_estimator_
                stats_kf[y]=predict_labels(clf, X_test, y_test)
                write_parameters(l_pca,k,'DT',y,grid_obj.best_params_,'done')
                write_log(l_pca,k,'DT',y,stats_kf[y])                
                joblib.dump(clf,local_path+'models/DT_'+str(k)+'_'+str(y)+'.pkl',compress=3)
                del clf
                stats_model['DT']=stats_kf

#        #AdaBoost
            parameters=dict()
            parameters=get_parameters(l_pca,k,'AB',y)
            if any(parameters):
               print 'Trained already.'                        
            else:
                write_parameters(l_pca,k,'AB',y,dict(),'in_process')  
                clf=AdaBoostClassifier(n_estimators=100)
                clf=clf.fit(X_train, y_train)
                stats_kf[y]=predict_labels(clf, X_test, y_test)
                write_log(l_pca,k,'AB',y,stats_kf[y])          
                write_parameters(l_pca,k,'AB',y,dict(),'done')                    
                joblib.dump(clf,local_path+'models/AB_'+str(k)+'_'+str(y)+'.pkl',compress=3)
                del clf        
                stats_model['AB']=stats_kf             

#        #kNeighbors
            parameters=dict()
            parameters=get_parameters(l_pca,k,'kN',y)
            if any(parameters):
               print 'Trained already.'                        
            else:
                write_parameters(l_pca,k,'kN',y,grid_obj.best_params_,'in_process')
                parameters = {'n_neighbors':(3,4,5,6,7,8),'weights':('uniform','distance'),'algorithm':('auto', 'ball_tree', 'kd_tree', 'brute')}
                clf=KNeighborsClassifier()
                grid_obj = GridSearchCV(clf,parameters,scoring = scorer)
                grid_obj = grid_obj.fit(X_train, y_train)
                clf=grid_obj.best_estimator_
                stats_kf[y]=predict_labels(clf, X_test, y_test)
                write_parameters(l_pca,k,'kN',y,grid_obj.best_params_,'done')
                write_log(l_pca,k,'kN',y,stats_kf[y])                
                joblib.dump(clf,local_path+'models/kN_'+str(k)+'_'+str(y)+'.pkl',compress=3)
                del clf 
                stats_model['kN']=stats_kf

#        #GaussianNB
            parameters=dict()
            parameters=get_parameters(l_pca,k,'GNB',y)
            if any(parameters):
               print 'Trained already.'                        
            else:
                write_parameters(l_pca,k,'GNB',y,dict(),'in_process')  
                clf=GaussianNB()
                clf=clf.fit(X_train, y_train)
                stats_kf[y]=predict_labels(clf, X_test, y_test)
                write_log(l_pca,k,'GNB',y,stats_kf[y])                
                write_parameters(l_pca,k,'GNB',y,dict(),'done')                    
                joblib.dump(clf,local_path+'models/GNB_'+str(k)+'_'+str(y)+'.pkl',compress=3)
                stats_model['GNB']=stats_kf
        stats_accuracy[k]=stats_model
        del X_all, y_all, X_train, y_train, X_test, y_test

#commons.get_sp500_index()
train('Close')       
