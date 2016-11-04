#the code calls the methods in mydata to refresh everything to until today
from mydata import mydata
import ttk as tt
import Tkinter as tk
import time as tm

class cl_get_delta(object):

    def __init__(self):    
        self.requery_data=True
#        self.mQuandlKey=tk.Tk()        
        self.mKey = tk.Tk()
        self.mKey.title('Please enter your Quandl key confirm.')
        self.mKey.geometry("800x100+300+500")
        self.mk = tk.Entry(self.mKey,width=50)
        self.mk.pack()
        self.mk.focus_set()
        self.mkb = tk.Button(self.mKey, text="Confirm", width=30, command=self.set_key)
        self.mkb.pack()
        tk.mainloop()

    
    def set_key(self):
        self.QuandlKey=self.mk.get()
        self.mKey.destroy()
        self.get_delta(self.QuandlKey)

    def get_delta(self,quandlkey):
        if self.requery_data==True:
            self.mGui = tk.Tk()
            self.mLabel=tk.Tk()

            self.mGui.geometry("800x100+300+500")
            self.mLabel.geometry("800x100+300+350")
        
            self.mLabel.title('Updates')
            mlb=tk.Message(self.mLabel,text='The database needs to be refreshed. This will take a while.',anchor='ne',width=600)
            mlb.pack()
            mlb.update()
            self.mGui.title('Progress')
            mpb = tt.Progressbar(self.mGui,orient ="horizontal",length = 700, mode ="determinate",variable='i')
            mpb.pack()
            mpb["maximum"] = 14
            mpb.update()
            tm.sleep(5)
            
            mlb["text"]='Reading SP500 composition from WIKI.'
            mlb.update()
            mpb["value"]=1
            mpb.update()
            dba=mydata(True,False,True,self.QuandlKey)
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
        
            mlb["text"]='Refreshing market cap.'    
            mlb.update()
            mpb["value"]=12
            mpb.update()        
            dba.get_marketcap()
        
            mlb["text"]='Refreshing index composition.'    
            mlb.update()
            mpb["value"]=13
            mpb.update()        
            dba.get_index_composition()

            mlb["text"]='Done. Closing in 3 sec.'    
            mlb.update()
            tm.sleep(1)
            mlb["text"]='Done. Closing in 2 sec.'    
            mlb.update()
            tm.sleep(1)
            mlb["text"]='Done. Closing in 1 sec.'    
            mlb.update()
            tm.sleep(1)
            mpb["value"]=13
            mpb.update()        
            dba.get_index_composition()
            
            self.mGui.destroy()
            self.mLabel.destroy()            
                    
x=cl_get_delta()