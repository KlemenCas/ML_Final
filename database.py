import commons
import tables
import csv
import numpy as np
import random
from math import exp
import time as tm

class db(object):
    t_stats=None
    t_ticker_symbols=None
    t_q=None
    t_clusters=None
    t_ticker_ids=None
    ti_ticker_ids=dict()
    cl_forecasts=None
    read_mode='r+'
    
    def __init__(self,forecasts,read_mode='r+'):     
        self.read_mode=read_mode
        self.cl_forecasts=forecasts
        self.init_db()
        self.init_stats_table()
        self.init_ticker_ids_table()
        self.init_q_table()
        self.init_q_log()
        self.init_cluster_table()
        if self.read_mode=='w':
            self.load_ticker_ids()
        self.load_internal_ticker_ids()
        if self.read_mode=='w':    
            self.load_cluster_table()
            self.load_stats_table()
        

    def init_db(self):
        self.db_main = tables.open_file(commons.stats_path+'simulation.h5', self.read_mode)
            
    def init_stats_table(self):
        if self.read_mode=='w':        
            try:
                self.db_main.remove_node('/', 'stats')
                print 'stats table dropped.'
            except tables.exceptions.NoSuchNodeError:
                print 'stats table not exist yet. nothing to initialize.'         
            try:
                stats_desc={'pca':   tables.IntCol(),
                            'ticker':tables.StringCol(10),
                            'model': tables.StringCol(3),
                            'kpi':   tables.StringCol(15),
                            'accuracy':tables.FloatCol()}
                self.t_stats=self.db_main.create_table('/','stats',stats_desc)
                self.t_stats.cols.ticker.create_index()
                self.t_stats.cols.kpi.create_index()
            except tables.exceptions.NodeError:
                self.t_stats=self.db_main.get_node('/','stats')
                print 'stats table opened.'
        else:
            self.t_stats=self.db_main.get_node('/','stats')
            print 'stats table opened.'
            

    def init_ticker_ids_table(self):
        if self.read_mode=='w':        
            try:
                ticker_desc={'ticker':tables.StringCol(10),
                                     'id':tables.IntCol()}
                self.t_ticker_ids=self.db_main.create_table('/','ticker_symbols',ticker_desc)
                self.t_ticker_ids.cols.ticker.create_index()
            except tables.exceptions.NodeError:
                self.t_ticker_ids=self.db_main.get_node('/','ticker_symbols')            
                print 'ticker symbols table opened.'
        else:
            self.t_ticker_ids=self.db_main.get_node('/','ticker_symbols')            
            print 'ticker symbols table opened.'
            

    def init_q_table(self):
        if self.read_mode=='w':        
            try:
                q_table_desc={'state':tables.Int64Col(),
                              'action':tables.IntCol(),
                              'reward':tables.FloatCol()}
                self.t_q=self.db_main.create_table('/','q_table',q_table_desc)
                self.t_q.cols.state.create_index()
                self.t_q.cols.action.create_index()
            except tables.exceptions.NodeError:
                self.t_q=self.db_main.get_node('/','q_table')
                print 'q table opened.'
        else:
            self.t_q=self.db_main.get_node('/','q_table')
            print 'q table opened.'

                
    def init_q_log(self):
        if self.read_mode=='w':          
            try:
                q_log_desc={'time':tables.TimeCol(),
                            'state':tables.Int64Col(),
                            'action':tables.IntCol(),
                            'reward':tables.FloatCol()}
                self.q_log=self.db_main.create_table('/','q_log',q_log_desc)
                self.q_log.cols.time.create_index()
                self.q_log.cols.state.create_index()
                self.q_log.cols.action.create_index()
            except tables.exceptions.NodeError:
                self.q_log=self.db_main.get_node('/','q_log')
                print 'q_log table opened.'
        else:
            self.q_log=self.db_main.get_node('/','q_log')
            print 'q_log table opened.'
            
            
    def init_cluster_table(self):
        if self.read_mode=='w':   
            try:
                self.db_main.remove_node('/', 'cluster_table')
                print 'Cluster table dropped.'
            except tables.exceptions.NoSuchNodeError:
                print 'cluster table does not exist yet. nothing to drop.'        
            try:
                cluster_desc={'ticker':tables.IntCol(),
                                  'kpi':tables.IntCol(),
                                  'c0':tables.FloatCol(),
                                  'c1':tables.FloatCol(),
                                  'c2':tables.FloatCol(),
                                  'c3':tables.FloatCol(),
                                  'c4':tables.FloatCol()}
                self.t_clusters=self.db_main.create_table('/','cluster_table',cluster_desc)
                self.t_clusters.cols.ticker.create_index()
                self.t_clusters.cols.kpi.create_index()
            except tables.exceptions.NodeError:
                self.t_clusters=self.db_main.get_node('/','cluster_table')
                print 'cluster table exists already.'
        else:
            self.t_clusters=self.db_main.get_node('/','cluster_table')
            print 'cluster table exists already.'

                
    def load_cluster_table(self):
        with open(commons.stats_path+'cluster.csv','r') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',')
            for row in csvreader:
                if len(row[0])>0:
                    try:
                        self.t_clusters.row['ticker']=self.ti_ticker_ids[row[0]]
                        self.t_clusters.row['kpi']=commons.kpi[row[1][1:]]
                        values=self.string_to_list(row[2])
                        values.sort()
                        self.t_clusters.row['c0']=values[0]
                        self.t_clusters.row['c1']=values[1]
                        self.t_clusters.row['c2']=values[2]
                        self.t_clusters.row['c3']=values[3]
                        self.t_clusters.row['c4']=values[4]
                        self.t_clusters.row.append()
                        print 'Stats for '+row[0]+' loaded.'
                    except KeyError:
                        print '.'
            csvfile.close()    
        try:
            with open(commons.stats_path+'cluster_generic.csv','r') as csvfile:
                csvreader = csv.reader(csvfile, delimiter=',')
                for row in csvreader:
                    if len(row[0])>0:                
                        self.t_clusters.row['ticker']=row[0].replace('SP','100').replace('_','00')
                        self.t_clusters.row['kpi']=commons.kpi[row[1][1:]]
                        values=self.string_to_list(row[2])
                        values.sort()
                        self.t_clusters.row['c0']=values[0]
                        self.t_clusters.row['c1']=values[1]
                        self.t_clusters.row['c2']=values[2]
                        self.t_clusters.row['c3']=values[3]
                        self.t_clusters.row['c4']=values[4]
                        self.t_clusters.row.append()
                csvfile.close()  
        except IOError:
            'Print general cluster csv not available.'
        self.t_clusters.flush()

    def load_stats_table(self):    
        with open(commons.stats_path+'stats.csv','r') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',')
            for row in csvreader:
#                print row
                self.t_stats.row['pca']=int(row[0])
                self.t_stats.row['ticker']=row[1]
                self.t_stats.row['model']=row[2]
                self.t_stats.row['kpi']=row[3]
                self.t_stats.row['accuracy']=row[4]
                self.t_stats.row.append()
            csvfile.close()    
    
        try:
            with open(commons.stats_path+'stats_generic.csv','r') as csvfile:
                csvreader = csv.reader(csvfile, delimiter=',')
                for row in csvreader:
                    self.t_stats.row['pca']=int(row[1])
                    self.t_stats.row['ticker']=row[0]
                    self.t_stats.row['model']=row[2]
                    self.t_stats.row['kpi']=row[3]
                    self.t_stats.row['accuracy']=row[4]
                    self.t_stats.row.append()
                csvfile.close()    
        except IOError:
            print 'General stats table not available.'
        self.t_stats.flush()
        
        print 'stats table initialized.'
        
#get ticker unique number
    def load_ticker_ids(self):
#        self.t_ticker_ids.remove_rows(0, self.t_ticker_ids.nrows)        
        all_rows=self.t_ticker_ids.read()
        for ticker,v in commons.sp500_ticker.items():
            ticker_id=self.t_ticker_ids.read_where('ticker=='+"'"+ticker+"'")
            if not any(ticker_id):
                if any(all_rows):
                    new_id=self.t_ticker_ids.nrows+1
                else:
                    new_id=1
                self.t_ticker_ids.row['ticker']=ticker
                self.t_ticker_ids.row['id']=new_id
                self.t_ticker_ids.row.append()
                self.t_ticker_ids.flush()
                all_rows=self.t_ticker_ids.read()
        print 'Ticker IDs table initialized.'

        
    def load_internal_ticker_ids(self):
        for ticker,v in commons.sp500_ticker.items():
            for row in self.t_ticker_ids.read_where('ticker=='+"'"+ticker+"'"):
                self.ti_ticker_ids[row[1]]=row[0]
        
    
    def update_q_table(self,state,action,reward):
        q_records=self.t_q.where('(state=='+str(self.cl_forecasts.get_q_key(state))+') & (action=='+str(action)+')')
        if any(q_records):
            for row in self.t_q.where('(state=='+str(self.cl_forecasts.get_q_key(state))+') & (action=='+str(action)+')'):
                row['reward']=reward
                row.update()
        else:
            self.t_q.row['state']=self.cl_forecasts.get_q_key(state)
            self.t_q.row['action']=action
            self.t_q.row['reward']=reward
            self.t_q.row.append()
            self.t_q.flush()
        self.q_log.row['time']=tm.time()            
        self.q_log.row['state']=self.cl_forecasts.get_q_key(state)
        self.q_log.row['action']=action
        self.q_log.row['reward']=reward
        self.q_log.row.append()
        self.q_log.flush()
    
            
    def get_max_q(self,state):
        max_action=''
        max_q=-1000
        x_random=False
        q_records=self.t_q.read_where('(state=='+str(self.cl_forecasts.get_q_key(state))+')')
        actions=[commons.action_code['sell'],commons.action_code['hold'],commons.action_code['buy']]
        for row in q_records:
            if row['reward']>max_q:
                max_action=row['action']
                max_q=row['reward']
                actions.remove(row['action'])
        if max_q==-1000:
            max_q=100
            max_action=random.choice([commons.action_code['sell'],commons.action_code['hold'],commons.action_code['buy']])
            x_random=True
        return max_q, max_action,x_random
        
    def get_reward(self,state,action_code):
        q_records=self.t_q.read_where('(state=='+str(self.cl_forecasts.get_q_key(state))+') & (action=='+str(action_code)+')')
        if any(q_records):
            for row in q_records:
                return float(row['reward'])
        else:
            return 50.
            
    def get_softmax_action(self,state,t):
        actions=[commons.action_code['sell'],commons.action_code['buy']]
#commons.action_code['hold'],
        distr=np.array([])
        e_q_sum=0        
        for b in actions:
            e_q_sum+=exp(self.get_reward(state,b)/t)
        for a in actions:
            e_q=0.
            e_q=exp(self.get_reward(state,a)/t)
            distr_a=np.array([])
            for i in range(int(e_q/e_q_sum*100)):
                distr_a=np.append(distr_a,a)
            distr=np.append(distr,distr_a)
        return distr[int(distr[int(random.random()*len(distr))])]

                     
    def string_to_list(self,a):
        x=list()
        f_n=0.
        n=''
        for c in a:
            if c in ['1','2','3','4','5','6','7','8','9','0','-','.','E','e']:
                n=n+c
            else:
                if len(n)>0 and c in [' ',']']:
                    f_n=float(n)
                    x.append(f_n)
                    n=''
        
        return x
                