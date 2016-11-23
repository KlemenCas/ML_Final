import commons
from scipy.stats import kendalltau
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
import tables
import numpy as np
#from prettytable import PrettyTable
import csv

#t_log
#dba = tables.open_file(commons.stats_path+'t_log.h5', 'r')  
#t_log=dba.get_node('/','t_log')
#with open(commons.stats_path+'t_log.csv','w') as csvfile:
#    csvwriter = csv.writer(csvfile, delimiter=',')
#    csvwriter.writerow(t_log.colnames)
#    for row in t_log:
#        xrow=list()
#        try:
#            index_t=commons.sp500_index[commons.sp500_ticker[row['ticker']]][-10:-2]
#            xrow=list()         
#            xrow.append(row[0])
#            xrow.append(row[1])
#            xrow.append(row[2])            
#            xrow.append(commons.date_index_external[row[3]])
#            xrow.append(row[4])            
#            xrow.append(row[5])            
#            xrow.append(row[6])            
#            xrow.append(row[7])            
#            xrow.append(row[8])            
#            csvwriter.writerow(xrow)
#        except KeyError:
#            x=0
#    csvfile.close()
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

with open(commons.stats_path+'performance_log.csv','w') as csvfile:
    csvwriter = csv.writer(csvfile, delimiter=',')
    csvwriter.writerow(p_log.colnames)
    for row in p_log:
        csvwriter.writerow(row[:])
    csvfile.close()

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
#
#a=list(['1dm','2dm','5dm','30dsma','30dmx','30dmn','5dv','bbands','1dd','5dd','20dd','clr','chr','ny_ss','ns_ss','1er','2er','5er'])
#data=commons.read_dataframe(commons.local_path+'data/1dm.h5')
#
#sns.set()
#sns.heatmap(data)
#sns.plt.show()
#

#data=commons.read_dataframe(commons.local_path+'data/Xy_all_AAPL')
#columns=commons.Xy_columns(data,'Close',feature_only=True)
#startdate=commons.data_sp500_1st_date['AAPL']
#data=data.ix[startdate:,columns]
#f_columns=list()
#f_map=dict()
#for i in range(0,len(columns)):
#    f_columns.append('f'+str(i))
#    f_map['f'+str(i)]=columns[i]
#
#print f_map
#
#data.columns=f_columns
#corr=data.corr()
#
##set text
#plt.rc('xtick', labelsize=25)
#plt.rc('ytick', labelsize=25)
#
## Generate a mask for the upper triangle
#mask = np.zeros_like(corr, dtype=np.bool)
#mask[np.triu_indices_from(mask)] = True
#
## Set up the matplotlib figure
#f, ax = plt.subplots(figsize=(11, 9))
#
## Generate a custom diverging colormap
#cmap = sns.diverging_palette(220, 10, as_cmap=True)
#
## Draw the heatmap with the mask and correct aspect ratio
#sns.heatmap(corr, mask=mask, cmap=cmap, vmax=.3,
#            square=True, xticklabels=5, yticklabels=5,
#            linewidths=.5, cbar_kws={"shrink": .5}, ax=ax)
#
#plt.show()


