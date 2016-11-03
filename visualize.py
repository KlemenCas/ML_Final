import commons
from scipy.stats import kendalltau
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
import tables
from prettytable import PrettyTable
import csv

#t_log
dba = tables.open_file(commons.stats_path+'t_log.h5', 'r')  
t_log=dba.get_node('/','t_log')
pt_t_log=PrettyTable(t_log.colnames)
for row in t_log:
    index_t=commons.sp500_index[commons.sp500_ticker[row['ticker']]][-10:-2]
    pt_t_log.add_row(row[:])
print pt_t_log     

with open(commons.stats_path+'t_log.csv','w') as csvfile:
    csvwriter = csv.writer(csvfile, delimiter=',')
    csvwriter.writerow(t_log.colnames)
    for row in t_log:
        index_t=commons.sp500_index[commons.sp500_ticker[row['ticker']]][-10:-2]
        csvwriter.writerow(row[:])
    csvfile.close()
 

dba = tables.open_file(commons.stats_path+'simulation.h5', 'r')  
t_q=dba.get_node('/','q_table')
pt_t_log=PrettyTable(t_q.colnames)
for row in t_q:
    pt_t_log.add_row(row[:])
print pt_t_log     

with open(commons.stats_path+'t_q.csv','w') as csvfile:
    csvwriter = csv.writer(csvfile, delimiter=',')
    csvwriter.writerow(t_q.colnames)
    for row in t_q:
        csvwriter.writerow(row[:])
    csvfile.close()

#q_log
q_log=dba.get_node('/','q_log')
with open(commons.stats_path+'q_log.csv','w') as csvfile:
    csvwriter = csv.writer(csvfile, delimiter=',')
    csvwriter.writerow(q_log.colnames)
    for row in q_log:
        csvwriter.writerow(row[:])
    csvfile.close()
    
t_symbols=dba.get_node('/','ticker_symbols')
pt_t_log=PrettyTable(t_symbols.colnames)
for row in t_symbols:
    pt_t_log.add_row(row[:])
print pt_t_log   