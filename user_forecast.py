import commons
import datetime as dt
from market import stock_market
from database import db
from forecasts import forecast

import ttk as tt
import Tkinter as tk
import time as tm

class user_forecasts(object):    
    def __init__(self, top):
        self.demo_date=dt.date(2016,09,30)
        self.dix=commons.date_index_internal(demo_date)
        self.m=stock_market()
        self.f=forecast()
        self.dba=db(self.f)
        self.f.set_dba=self.dba        
        self.mLabel=tk.Tk()
        self.mlx = tk.Listbox(top)
        self.mlx.pack()
        for k,v in commons.sp500_ticker.items():
            self.mlx.insert('end', k)   
        self.mlx.bind('<<ListboxSelect>>',self.user_forecast)
        self.temperature=.5
   
    def user_forecast(self,evt):
        w = evt.widget
        index = int(w.curselection()[0])
        value = w.get(index)
        master.destroy()
        self.mLabel.geometry("800x100+300+350")    
        mlb=tk.Message(self.mLabel,text=(value+' selected. Preparing forecast.'),anchor='ne',width=600)    
        mlb.update()
        
        state=self.f.get_forecast_state(self.dba.t_stats,value,self.dix)
        proposed_action=self.dba.get_softmax_action(state,self.temperature)
        self.mLabel.destroy()
        self.mLabel=tk.Tk()
        self.mLabel.geometry("800x400+300+350")  
        mlb=tk.Message(self.mLabel,text=self.forecast_text(state,value,proposed_action),anchor='ne',width=600)    
        mlb.update()
        
    def forecast_text(self,state,ticker,proposed_action):
        forecast_text='Forecast for '+ticker+' performed. The model delivered the following results: '
        if state['1dd_Close']==-1:
            forecast_text+='Stock price will fall tomorrow. '
        else:
            forecast_text+='Stock price will raise tomorrow. '
        if state['5dd_Close']==-1:
            forecast_text+='Stock price will fall over the next 5 days. '
        else:
            forecast_text+='Stock price will raise over the next 5 days. '
        if state['20dd_Close']==-1:
            forecast_text+='Stock price will fall over the next 20 days. '
        else:
            forecast_text+='Stock price will raise over the next 20 days. '
        price_recommendation=self.f.get_order_price(self.dba,self.dba.ti_ticker_ids[ticker],self.f.get_q_key(state),\
                                               self.dix,proposed_action,self.m.get_closing_price(ticker,self.dix))
        forecast_text+=' In case of a transaction the recommended order price is: '+str(price_recommendation)
        return forecast_text
        
master = tk.Tk()
FC = user_forecasts(master)
master.mainloop()