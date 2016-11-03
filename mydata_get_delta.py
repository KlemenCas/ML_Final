#the code calls the methods in mydata to refresh everything to today

import commons
from mydata import mydata
import datetime as dt
import ttk as tt
import Tkinter as tk
import time as tm

requery_data=True
mGui = tk.Tk()
mLabel=tk.Tk()
    
#Initialize data retrieval
if requery_data==True:
    mGui.geometry("800x100+300+500")
    mLabel.geometry("800x100+300+350")
    i=0
    if commons.max_date<dt.datetime.strptime(dt.datetime.today().strftime("%Y-%m-%d"),'%Y-%m-%d'):
        mLabel.title('Updates')
        mlb=tk.Message(mLabel,text='This is the first call today. The database needs to be refreshed. This will take a while.',anchor='ne',width=600)
        mlb.pack()
        mlb.update()
        mGui.title('Progress')
        mpb = tt.Progressbar(mGui,orient ="horizontal",length = 700, mode ="determinate",variable='i')
        mpb.pack()
        mpb["maximum"] = 11
        mpb.update()
        tm.sleep(5)
        
        mlb["text"]='Reading SP500 composition from WIKI.'
        mlb.update()
        mpb["value"]=1
        mpb.update()
        dba=mydata(True,False,True)
        dba.init_sp500_ticker()
        
        mlb["text"]='Reading SP500 index prices.'    
        mlb.update()
        mpb["value"]=2
        mpb.update()
        dba.get_quandl_index()
    
        mlb["text"]='Reading local data and adjusted ticker prices.'    
        mlb.update()
        mpb["value"]=3
        mpb.update()    
        dba.refresh_wiki_sp500()
        
        mlb["text"]='Reading fundamentals.'    
        mlb.update()
        mpb["value"]=4
        mpb.update()    
        dba.get_fundamentals()
        
        mlb["text"]='Reading short sell data.'   
        mlb.update()
        mpb["value"]=5
        mpb.update()    
        dba.get_short_sell()
        
        mlb["text"]='Reading sentiment.'    
        mlb.update()
        mpb["value"]=6
        mpb.update()    
        dba.get_sentiment()
    
        dba.last_date=max(dba.data_sp500.index)    
    
        mlb["text"]='Correcting column names.'    
        mlb.update()
        mpb["value"]=7
        mpb.update()    
        dba.reset_data_sp500_columns()
    
        mlb["text"]='Calculating dates.'    
        mlb.update()
        mpb["value"]=8
        mpb.update()    
        dba.calc_sp500_1st_date()
    
        mlb["text"]='Updating Alpha and Beta.'    
        mlb.update()
        mpb["value"]=9
        mpb.update()    
        dba.calc_sector_betas('all')        
        dba.correct_nan_anb()
    
        mlb["text"]='Treatment of NaN values.'    
        mlb.update()
        mpb["value"]=10
        mpb.update()    
        dba.sp500_fillna()
    
        mlb["text"]='Calculating indicators.'    
        mlb.update()
        mpb["value"]=11
        mpb.update()        
        dba.calc_indicators()
    
    mGui.destroy()
    mLabel.destroy()            
            
