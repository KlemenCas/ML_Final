import numpy as np
from scipy.stats import kendalltau
import seaborn as sns
import pandas as pd
import csv

local_path='C:/Users/kncas/OneDrive/Udacity/Project/data/'   
    
class process(Object):
    sp500_composition=dict()
    sp500_ticker=dict()
    sp500_index=dict()
 
    
    def __init__(self):   
        self.get_sp500_index()
        self.visualize_indicators()
        
        
        
    def get_sp500_index(self):
        with open(self.local_path+'WIKI.csv','r') as csvfile:
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
        
    def visualize_indicators(self):
        sns.set(style="ticks")

        for k,v in self.sp500_ticker.items():            
            Xy_all=pd.read_hdf(local_path+'Xy_all_'+str(k),'table')
            x = Xy_all.ix[:,'Open']
            y = Xy_all.ix[:,'Close']
            sns.jointplot(x, y, kind="hex", stat_func=kendalltau, color="#4CB391")