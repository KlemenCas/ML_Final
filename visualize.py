import commons
from scipy.stats import kendalltau
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
import tables
#from prettytable import PrettyTable
import csv

#t_log
dba = tables.open_file(commons.stats_path+'t_log.h5', 'r')  
t_log=dba.get_node('/','t_log')
with open(commons.stats_path+'t_log.csv','w') as csvfile:
    csvwriter = csv.writer(csvfile, delimiter=',')
    csvwriter.writerow(t_log.colnames)
    for row in t_log:
        xrow=list()
        try:
            index_t=commons.sp500_index[commons.sp500_ticker[row['ticker']]][-10:-2]
            xrow=list()         
            xrow.append(row[0])
            xrow.append(row[1])
            xrow.append(row[2])            
            xrow.append(commons.date_index_external[row[3]])
            xrow.append(row[4])            
            xrow.append(row[5])            
            xrow.append(row[6])            
            xrow.append(row[7])            
            xrow.append(row[8])            
            csvwriter.writerow(xrow)
        except KeyError:
            x=0
    csvfile.close()
# 
#dba = tables.open_file(commons.stats_path+'simulation.h5', 'r')  
#t_q=dba.get_node('/','q_table')  
#with open(commons.stats_path+'t_q.csv','w') as csvfile:
#    csvwriter = csv.writer(csvfile, delimiter=',')
#    csvwriter.writerow(t_q.colnames)
#    for row in t_q:
#        csvwriter.writerow(row[:])
#    csvfile.close()
#
##q_log
#q_log=dba.get_node('/','q_log')
#with open(commons.stats_path+'q_log.csv','w') as csvfile:
#    csvwriter = csv.writer(csvfile, delimiter=',')
#    csvwriter.writerow(q_log.colnames)
#    for row in q_log:
#        csvwriter.writerow(row[:])
#    csvfile.close()
    
#
dba = tables.open_file(commons.stats_path+'performance_log.h5', 'r')
p_log=dba.get_node('/','p_log')

for simrun in range(1,11):
    df=pd.DataFrame(columns=['Index','P_Total'])
    for row in p_log:
        if row['simrun']==simrun:
            df.ix[commons.date_index_external[row['dix']],['Index']]=row['i_value']
            df.ix[commons.date_index_external[row['dix']],['P_Total']]=row['p_value']+row['cash']
        #    df.ix[commons.date_index_external[row['dix']],['Cash']]=row['cash']
        #    df.ix[commons.date_index_external[row['dix']],['Portfolio']]=row['cash']
    
    df.to_csv(commons.stats_path+str(simrun)+'performance_log.csv')
    df.plot(kind='line',fontsize=20)

 