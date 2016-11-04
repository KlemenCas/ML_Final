import commons
from market import stock_market
from database import db
from forecasts import forecast
from portfolio import investments
import pandas as pd
import datetime as dt
import tables 
import time as tm

try:
    db_log = tables.open_file(commons.stats_path+'performance_log.h5', 'r+')
except ValueError:
    print 'Database open already.'
    
try:
    perf_desc={'time':tables.TimeCol(),
                'dix':tables.IntCol(),
                'index':tables.StringCol(10),
                 'p_value':tables.FloatCol(),
                 'cash':tables.FloatCol(),
                 'i_value':tables.IntCol()}
    p_log=db_log.create_table('/','p_log',perf_desc)
    
except tables.exceptions.NodeError:
    p_log=db_log.get_node('/','p_log')
    print 'transaction log opened.'        


initial_budget=1000000000
temperature=.9

m=stock_market()
f=forecast()
dba=db(f,'r+')
f.set_dba=dba
alpha=.2
gamma=.4
new_state=dict()

startdate=dt.datetime.today()-dt.timedelta(days=18250)
enddate=dt.datetime.today()
for k,v in commons.sp500_index.items():
    index_t=v[-10:-2]
    if startdate<=min(m.index_composition[index_t].index):
        startdate=min(m.index_composition[index_t].index)
    if enddate>max(m.index_composition[index_t].index):
        enddate=max(m.index_composition[index_t].index)
enddate-=-dt.timedelta(days=1)    
p=investments(initial_budget,m,commons.date_index_internal[startdate],dba)
        
def play_for_a_day(idx_external,dix):
#    print 'Processing date: ',commons.date_index_external[dix]
    state=dict()
    proposed_action=dict()
    buying_list=list()
    order_entry=dict()
    reward=dict()
    dba.db_main.flush()
    random_action=dict()

    for ticker in commons.sp500_composition[idx_external]:
        reward[ticker]=9999
        if commons.data_sp500_1st_date[ticker]<=commons.date_index_external[dix]:
            index_t=commons.sp500_index[idx_external][-10:-2]
            try:
                state[ticker]=new_state[ticker]
            except KeyError:
                state[ticker]=f.get_forecast_state(dba.t_stats,ticker,dix)
            proposed_action[ticker]=dba.get_softmax_action(ticker,state[ticker],temperature)
            max_q_action=dba.get_max_q(ticker,state[ticker])
            if max_q_action[2]==False and max_q_action[1]==proposed_action[ticker]:
                random_action[ticker]=False
            else:
                random_action[ticker]=True
##compare max_axction and proposed action and if different then set volume to 1
            next_dix=dix+1
    #sell
            if proposed_action[ticker]==commons.action_code['sell']:
                try:
                    if p.portfolio[index_t][ticker]>0:
                        if random_action[ticker]==False:
                            vol=p.portfolio[index_t][ticker]
                        else:
                            vol=1
                        opening_price=m.get_opening_price(ticker,next_dix)
                        forecast_price=f.get_order_price(dba,dba.ti_ticker_ids[ticker],state[ticker],dix,proposed_action[ticker],m.get_closing_price(ticker,dix))

                        if opening_price>forecast_price:
                            reward[ticker]=p.execute_order(ticker,vol,next_dix,opening_price,proposed_action[ticker],m.get_closing_price(ticker,dix))
                        else:
                            reward[ticker]=p.execute_order(ticker,vol,next_dix,forecast_price,proposed_action[ticker],m.get_closing_price(ticker,dix))

                except KeyError:
                    p.portfolio[index_t][ticker]=0
                    print 'New Ticker: ', ticker
    
    #buy, but only after everythin has been sold        
            if proposed_action[ticker]==commons.action_code['buy']:
                order_entry[ticker]=m.get_opening_price(ticker,next_dix)
                
    buying_list=p.align_buying_list(order_entry,dix)
    for ticker,volume in buying_list.items():
        if commons.data_sp500_1st_date[ticker]<=commons.date_index_external[dix]:
            if random_action[ticker]==False:
                vol=volume
            else:
                vol=1            
            opening_price=m.get_opening_price(ticker,next_dix)
            forecast_price=f.get_order_price(dba,dba.ti_ticker_ids[ticker],state[ticker],dix,commons.action_code['buy'],m.get_closing_price(ticker,dix))
            if opening_price<forecast_price:
                reward[ticker]=p.execute_order(ticker,vol,next_dix,opening_price,commons.action_code['buy'],m.get_closing_price(ticker,dix))
            else:
                reward[ticker]=p.execute_order(ticker,vol,next_dix,forecast_price,commons.action_code['buy'],m.get_closing_price(ticker,dix))
    
#on the way to the next q
    dix+=1
    for ticker in commons.sp500_composition[idx_external]:
        index_t=commons.sp500_index[idx_external][-10:-2]        
        if commons.data_sp500_1st_date[ticker]<=commons.date_index_external[dix]:
            new_state[ticker]=f.get_forecast_state(dba.t_stats,ticker,dix)
            try:
                if reward[ticker]!=9999:
                    newQ=dba.get_reward(ticker,state[ticker],proposed_action[ticker])+\
                            alpha*(reward[ticker]+gamma*dba.get_max_q(ticker,new_state[ticker])[0]-dba.get_reward(ticker,state[ticker],proposed_action[ticker]))
                    dba.update_q_table(ticker,state[ticker],proposed_action[ticker],newQ)
            except KeyError:
                p.portfolio[index_t][ticker]=0
                print 'Date:',dix,' New Ticker:',ticker

        
#SIMULATE
for d in pd.date_range(startdate,enddate):
    if d in m.data_sp500.index:
        if d!=startdate:
            m.align_index_portfolio(commons.date_index_internal[d])
        for k,v in commons.sp500_index.items():
            index_t=v[-10:-2]   
            p.log_portfolio(commons.date_index_internal[d],index_t)
            m.log_portfolio(commons.date_index_internal[d],index_t)
            play_for_a_day(k,commons.date_index_internal[d])
        for k,v in commons.sp500_index.items():
            print 'Date: ', d, ' Index: ', v[-10:-2], 'Portfolio: ',\
              int(p.get_portfolio_value(v[-10:-2],commons.date_index_internal[d])),' Cash: ',int(p.cash[v[-10:-2]]),\
              ' Total: ',int(p.get_portfolio_value(v[-10:-2],commons.date_index_internal[d]))+int(p.cash[v[-10:-2]]),\
              ' Index: ',int(m.index_portfolio_value(k,commons.date_index_internal[d]))
            p_log.row['time']=tm.mktime(d.timetuple())
            p_log.row['dix']=commons.date_index_internal[d]
            p_log.row['index']=v[-10:-2]
            p_log.row['p_value']=int(p.get_portfolio_value(v[-10:-2],commons.date_index_internal[d]))
            p_log.row['cash']=int(p.cash[v[-10:-2]])
            p_log.row['i_value']=int(m.index_portfolio_value(k,commons.date_index_internal[d]))
            p_log.row.append()
            p_log.flush()
            p_log.flush()
            db_log.flush()