import commons
import market
import pandas as pd   
import tables 
import time as tm

        
class investments(object):
    portfolio=dict()
    cash=dict()
    market=None
    portfolio_value=dict()
    portfolio_log=dict()
#    initialization_dix=0

    def __init__(self,initial_budget,market,init_dix,dba): 
        self.market=market
        self.initialize_cash(initial_budget)
        self.initialize_portfolio(init_dix)

        try:
            self.dba = tables.open_file(commons.stats_path+'t_log.h5', 'r+')
        except ValueError:
            print 'Database open already.'
            
        try:
            t_log_desc={'time':tables.TimeCol(),
                        'dix':tables.IntCol(),
                        'ticker':tables.StringCol(10),
                         'tx':tables.StringCol(10),
                         'price':tables.FloatCol(),
                         'volume':tables.IntCol(),
                         'close':tables.FloatCol(),
                         'cash_before':tables.FloatCol(),
                         'cash_after':tables.FloatCol()}
            self.t_log=self.dba.create_table('/','t_log',t_log_desc)
            self.t_log.cols.ticker.create_index()
        except tables.exceptions.NodeError:
            self.t_log=self.dba.get_node('/','t_log')
            print 'transaction log opened.'        

    def initialize_cash(self,initial_budget):
        for k,v in commons.sp500_index.items():
            index_t=v[-10:-2]
            self.cash[index_t]=initial_budget
        

    def initialize_portfolio(self,dix):
        for k,v in commons.sp500_index.items():
            index_t=v[-10:-2]    
            portfolio=dict()        
            self.portfolio[index_t]=self.market.get_index_portfolio(k,dix)
            for ticker,pct in self.portfolio[index_t].items():
                portfolio[ticker]=int(pct*self.cash[index_t]/self.market.get_closing_price(ticker,dix))
            for k,v in portfolio.items():
                self.cash[index_t]-=float(v*self.market.get_closing_price(k,dix))
            self.portfolio[index_t]=portfolio
            self.portfolio_log[index_t]=pd.DataFrame()
            
            
    def align_buying_list(self,buying_list,dix):
        delta=dict()
        cash_and_valuation=dict()
        aligned_order_list=dict()
        aligned_order_list_clean=dict()
        remaining_amount=dict(self.cash.items())
        
#1st calculate total value - portfolio + cash
        for k,v in commons.sp500_index.items():
            index_t=v[-10:-2]            
            cash_and_valuation[index_t]=0
            for ticker,volume in self.portfolio[index_t].items():
                cash_and_valuation[index_t]+=volume*self.market.get_closing_price(ticker,dix)
        for k,v in self.cash.items():
            cash_and_valuation[k]+=v

#get delta to target portfolio
        for ticker,price in buying_list.items():
            try:
                test=self.portfolio[index_t][ticker]
                delta=self.market.get_index_portfolio(commons.sp500_ticker[ticker],dix)[ticker]*cash_and_valuation[index_t]-\
                        self.portfolio[index_t][ticker]*self.market.get_closing_price(ticker,dix)
            except KeyError:
                delta=self.market.get_index_portfolio(commons.sp500_ticker[ticker],dix)[ticker]*cash_and_valuation[index_t]
            index_t=commons.sp500_index[commons.sp500_ticker[ticker]][-10:-2]
            if delta>remaining_amount[index_t]:
                delta=remaining_amount[index_t]
            if delta>0. and remaining_amount[index_t]>0:
                aligned_order_list[ticker]=int(delta/price)
                remaining_amount[index_t]=remaining_amount[index_t]-aligned_order_list[ticker]*price
#                print 'Buying List Alignment-Delta; Order List: ',aligned_order_list,' Remaining Amount: ',remaining_amount

#and spend what's left equally on the buying list         
        cum_weight=dict()                   
        for k,v in commons.sp500_composition.items():
            index_t=commons.sp500_index[k][-10:-2]
            cum_weight[index_t]=0
            for ticker,price in buying_list.items():
                if ticker in v:
                    cum_weight[index_t]+=self.market.get_index_portfolio(commons.sp500_ticker[ticker],dix)[ticker]

        for ticker,price in buying_list.items():                                           
            index_t=commons.sp500_index[commons.sp500_ticker[ticker]][-10:-2]
            try:
                aligned_order_list[ticker]=aligned_order_list[ticker]+int((self.market.get_index_portfolio(commons.sp500_ticker[ticker],dix)[ticker]/cum_weight[index_t])*\
                                       remaining_amount[index_t]/price)
            except KeyError:
                aligned_order_list[ticker]=0
                #            remaining_amount[index_t]-=aligned_order_list[ticker]*self.market.get_closing_price(ticker,dix)
#        print 'Buying List Alignment-Aligned; Order List: ',aligned_order_list,' Remaining Amount: ',remaining_amount
        for ticker,volume in aligned_order_list.items():
            if volume>0:
                aligned_order_list_clean[ticker]=volume
        return aligned_order_list_clean

#check buying list
    def check_buying_list(self,buying_list,dix):
        for k,v in commons.sp500_index.items():
            portfolio=self.market.get_index_portfolio(k,dix)
            for ticker in buying_list:
                if commons.sp500_ticker[ticker]==k:
                    found=''
                    for x,y in portfolio.items():
                        if ticker==x:
                            found='X'
                    if found=='':
                        print 'Missing: ',ticker
            
#execute order
    def execute_order(self,ticker,volume,dix,price,action,last_close):
        index_t=commons.sp500_index[commons.sp500_ticker[ticker]][-10:-2]
        cash_before=self.cash[index_t]
        self.t_log.row['time']=tm.time()
        self.t_log.row['dix']=dix
        self.t_log.row['ticker']=ticker
        self.t_log.row['price']=price
        self.t_log.row['volume']=volume
        self.t_log.row['close']=self.market.get_closing_price(ticker,dix)
        self.t_log.row['cash_before']=cash_before     
        self.t_log.row['cash_after']=self.cash[index_t]

        if (self.market.order_executable(ticker,dix,price,action)):
            if action==commons.action_code['buy']:
                try:
                    self.portfolio[index_t][ticker]+=volume
                except KeyError:
                    self.portfolio[index_t][ticker]=volume
                self.cash[index_t]-=price*volume
                self.t_log.row['tx']='buy'
            
            if action==commons.action_code['sell']:
                self.portfolio[index_t][ticker]-=volume
                self.cash[index_t]+=price*volume
                self.t_log.row['tx']='sell'
                
            self.t_log.row['cash_after']=self.cash[index_t]
        else:
            if action==commons.action_code['buy']:
                self.t_log.row['tx']='canc_buy'
            elif action==commons.action_code['sell']:
                self.t_log.row['tx']='canc_sell'
        self.t_log.row.append()
        self.t_log.flush()
        self.dba.flush()

        if (self.market.get_closing_price(ticker,dix)-last_close)>0 and action==commons.action_code['buy']:
            reward=200
        if (self.market.get_closing_price(ticker,dix)-last_close)>0 and action==commons.action_code['sell']:
            reward=-200
        if (self.market.get_closing_price(ticker,dix)-last_close)==0 and action==commons.action_code['buy']:
            reward=100
        if (self.market.get_closing_price(ticker,dix)-last_close)==0 and action==commons.action_code['sell']:
            reward=100
        if (self.market.get_closing_price(ticker,dix)-last_close)<0 and action==commons.action_code['buy']:
            reward=-200
        if (self.market.get_closing_price(ticker,dix)-last_close)<0 and action==commons.action_code['sell']:
            reward=+200
        
        return reward
        
    def get_portfolio_value(self,idx,dix):
        value=0
        for k,v in self.portfolio[idx].items():
            value+=v*self.market.get_closing_price(k,dix)
        return value
            
    def log_portfolio(self,dix):
        for k,v in self.portfolio.items():
            p_log=pd.DataFrame()
            for ticker,volume in v.items():
                p_log.ix[commons.date_index_external[dix],ticker]=volume
            self.portfolio_log[k]=self.portfolio_log[k].append(p_log)
            self.portfolio_log[k].to_hdf(commons.local_path+'data/Portfolio_'+k+'.h5','table',mode='w')
            
                
                
            
        