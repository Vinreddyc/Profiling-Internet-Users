# -*- coding: utf-8 -*-
"""
Created on Tue Apr 16 01:13:31 2019

@author: Vineeth Reddy C
"""
import os
import glob
import pandas as pd
from pandasql import sqldf
import math
from scipy import stats

datafiles = glob.glob('C:/Users/Vineeth reddy/Desktop/InfoSec-MS/Information Security _ Privacy Material/*.xlsx')
path = ('C:/Users/Vineeth reddy/Desktop/InfoSec-MS/Information Security _ Privacy Material/')
print(len(datafiles))

df10_S_W1 = pd.DataFrame(columns= ['week','starttime','endtime','avginternetusage'])
df10_S_W2 = pd.DataFrame(columns= ['week','starttime','endtime','avginternetusage'])

df5_M_W1 = pd.DataFrame(columns= ['week','starttime','endtime','avginternetusage'])
df5_M_W2 = pd.DataFrame(columns= ['week','starttime','endtime','avginternetusage'])

df227_S_W1 = pd.DataFrame(columns= ['week','starttime','endtime','avginternetusage'])
df227_S_W2 = pd.DataFrame(columns= ['week','starttime','endtime','avginternetusage'])

# For 10 Seconds window
# Week1
lowTimeStampWeek1 = 1359982800000 # 8am, 4th Feb, 2013
highTimeStampWeek1 = 1360360800000 # 5pm, 8th Feb, 2013
cnt1 = 0
while lowTimeStampWeek1 < highTimeStampWeek1:
     df10_S_W1.loc[-1] = [1, lowTimeStampWeek1, ((lowTimeStampWeek1 + 10000) - 1), 0]
     df10_S_W1.index = df10_S_W1.index + 1
     lowTimeStampWeek1 = (lowTimeStampWeek1 + 10000)  #10secs intervals
     cnt1 = cnt1 + 1;
     if cnt1 == 3241: # skipping epochs if it is 5pm each day for the 10 seconds window
         lowTimeStampWeek1 = lowTimeStampWeek1 + 54000000
         cnt1 = 0

lowTimeStampWeek2 = 1360587600000
highTimeStampWeek2 = 1360965600000
cnt2 = 0
while lowTimeStampWeek2 < highTimeStampWeek2:
     df10_S_W2.loc[-1] = [2, lowTimeStampWeek2, ((lowTimeStampWeek2 + 10000) - 1), 0]
     df10_S_W2.index = df10_S_W2.index + 1
     lowTimeStampWeek2 = (lowTimeStampWeek2 + 10000)
     cnt2 = (cnt2 + 1)
     if cnt2 == 3241:
         lowTimeStampWeek2 = lowTimeStampWeek2 + 54000000
         cnt2 = 0
     
df_10S = pd.concat([df10_S_W1,df10_S_W2])
df_10S.sort_values(by=['starttime'])

# For 227 Seconds window
lowTimeStampWeek1 = 1359982800000
highTimeStampWeek1 = 1360360800000
cnt3 = 0
while lowTimeStampWeek1 < highTimeStampWeek1:
     df227_S_W1.loc[-1] = [1, lowTimeStampWeek1, ((lowTimeStampWeek1 + 227000) - 1), 0]
     df227_S_W1.index = df227_S_W1.index + 1
     lowTimeStampWeek1 = (lowTimeStampWeek1 + 227000)
     cnt3 = (cnt3 + 1)
     if cnt3 == 143:
         lowTimeStampWeek1 = lowTimeStampWeek1 + 54000000
         cnt3 = 0
     
lowTimeStampWeek2 = 1360587600000
highTimeStampWeek2 = 1360965600000
cnt4 = 0
while lowTimeStampWeek2 < highTimeStampWeek2:
     df227_S_W2.loc[-1] = [2, lowTimeStampWeek2, ((lowTimeStampWeek2 + 227000) - 1), 0]
     df227_S_W2.index = df227_S_W2.index + 1
     lowTimeStampWeek2 = (lowTimeStampWeek2 + 227000)
     cnt4 = (cnt4 + 1)
     if cnt4 == 143:
         lowTimeStampWeek2 = lowTimeStampWeek2 + 54000000
         cnt4 = 0

df_227S = pd.concat([df227_S_W1,df227_S_W2])
df_227S.sort_values(by=['starttime'])

# For 5 Minutes window
lowTimeStampWeek1 = 1359982800000
highTimeStampWeek1 = 1360360800000
cnt5 = 0
while lowTimeStampWeek1 < highTimeStampWeek1:
     df5_M_W1.loc[-1] = [1, lowTimeStampWeek1, ((lowTimeStampWeek1 + 300000) - 1), 0]
     df5_M_W1.index = df5_M_W1.index + 1
     lowTimeStampWeek1 = (lowTimeStampWeek1 + 300000)
     cnt5 = (cnt5 + 1)
     if cnt5 == 109:
         lowTimeStampWeek1 = lowTimeStampWeek1 + 54000000
         cnt5 = 0

lowTimeStampWeek2 = 1360587600000
highTimeStampWeek2 = 1360965600000
cnt6 = 0
while lowTimeStampWeek2 < highTimeStampWeek2:
     df5_M_W2.loc[-1] = [2, lowTimeStampWeek2, ((lowTimeStampWeek2 + 300000) - 1), 0]
     df5_M_W2.index = df5_M_W2.index + 1
     lowTimeStampWeek2 = (lowTimeStampWeek2 + 300000)
     cnt6 = (cnt6 + 1)
     if cnt6 == 109:
         lowTimeStampWeek2 = lowTimeStampWeek2 + 54000000
         cnt6 = 0
     
df_5M = pd.concat([df5_M_W1,df5_M_W2])
df_5M.sort_values(by=['starttime'])

user_id = 1
os.chdir(path)
for filename in os.listdir():
    user_10S = df_10S
    user_227S = df_227S
    user_5M = df_5M
    
    user_data = pd.read_excel(filename, sheetname='Sheet1')
    user_data = pd.DataFrame(user_data)
    
    #Filtering the data from 4th Feb to 15th Feb and removing all durations=0
    user_data = user_data[(user_data['Real First Packet'] >= 1359982800000) & (user_data['Real First Packet'] <= 1360965600000) & (user_data['Duration'] > 0)]
    
    user_data = user_data.sort_values(by=['Real First Packet'])
    
    frame_row_size = len(user_data.axes[0])
    if frame_row_size > 0:	
        user_data.loc[:,'InternetUsage'] = 0
        for i in user_data.iterrows():
            user_data.loc[:,'InternetUsage'] = user_data.loc[:,'doctets']/user_data.loc[:,'Duration']
        print(user_data.loc[:,'InternetUsage'])
    
        user_10S = sqldf("""SELECT a.week as week, a.starttime as starttime, a.endtime as endtime, case when d.InternetUsage is not null then avg(d.InternetUsage) else 0 end as avginternetusage from user_10S a Left outer join user_data d on d.[Real First Packet]>=a.starttime and d.[Real First Packet] <=a.endtime group by a.week, a.starttime, a.endtime order by a.starttime""", locals())
        user_227S = sqldf("""SELECT u.week as week, u.starttime as starttime, u.endtime as endtime, case when d.InternetUsage is not null then avg(d.InternetUsage) else 0 end as avginternetusage from user_227S u Left outer join user_data d on d.[Real First Packet]>=u.starttime and d.[Real First Packet] <=u.endtime group by u.week, u.starttime, u.endtime order by u.starttime""", locals())
        user_5M = sqldf("""SELECT m.week as week, m.starttime as starttime, m.endtime as endtime, case when d.InternetUsage is not null then avg(d.InternetUsage) else 0 end as avginternetusage from user_5M m Left outer join user_data d on d.[Real First Packet]>=m.starttime and d.[Real First Packet] <=m.endtime group by m.week, m.starttime, m.endtime order by m.starttime""", locals())
    
    os.makedirs(os.path.join('C:/Users/Vineeth reddy/Desktop/InfoSec-MS/Final Project/',  str(user_id)))
     
    user_10S.to_excel("C:/Users/Vineeth reddy/Desktop/InfoSec-MS/Final Project/"+str(user_id)+"/for_10secs.xlsx")
    user_227S.to_excel("C:/Users/Vineeth reddy/Desktop/InfoSec-MS/Final Project/"+str(user_id)+"/for_227secs.xlsx")
    user_5M.to_excel("C:/Users/Vineeth reddy/Desktop/InfoSec-MS/Final Project/"+str(user_id)+"/for_5mins.xlsx")
    user_id += 1

## p calculation
w, h = 54, 54;
p_compare_users = [[0 for x in range(w)] for y in range(h)] 

def finding_z(r1a2a, r1a2b, r2a2b, N):
    rm2 = (math.pow(r1a2a,2) + math.pow(r1a2b,2)) / 2
    f = (1 - r2a2b) / (2 * (1 - rm2))
    h = (1 - (f * rm2)) / (1 - rm2)
    z1a2b = (0.5) * (math.log((1 + r1a2b) / (1 - r1a2b)))
    z1a2a = (0.5) * (math.log((1 + r1a2a) / (1 - r1a2a)))
    z = (z1a2a - z1a2b) * (math.sqrt(N - 3) / (2 * (1 - r2a2b) * h))
    return(z)

def finding_p(z):
    p = 0.3275911
    a1 = 0.254829592
    a2 = -0.284496736
    a3 = 1.421413741
    a4 = -1.453152027
    a5 = 1.061405429
    
    if z < 0.0:
        sign = -1
    else:
        sign = 1
  
    x = abs(z) / math.sqrt(2.0)
  
    t = 1.0 / (1.0 + p * x)
  
    erf = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-x * x)
  
    return(0.5 * (1.0 + sign * erf))
    
def gen_user_data(file_name):
    print(file_name)
    userweek_1 = list()
    userweek_2 = list()
    for i in range(1,55):
        data_gen = pd.read_excel("C:/Users/Vineeth reddy/Desktop/InfoSec-MS/Final Project/"+str(i)+file_name)
        data_gen = pd.DataFrame(data_gen)
        userweek_1.append(data_gen[data_gen['week'] == 1].filter(['avginternetusage']))
        userweek_2.append(data_gen[data_gen['week'] == 2].filter(['avginternetusage']))        
    for j in range(0,54):
        for k in range(0,54):
                r1a2a_res, p1a2a_res = stats.spearmanr(userweek_1[j],userweek_2[j])
                r1a2b_res, p1a2b_res = stats.spearmanr(userweek_1[j],userweek_2[k])
                r2a2b_res, p2a2b_res = stats.spearmanr(userweek_2[j],userweek_2[k])
                if pd.isna(r1a2a_res):
                    r1a2a_res = 0;
                else:
                    r1a2a_res;
                if pd.isna(r1a2b_res): # updated conditions for p-value generation.
                    r1a2b_res = 0;
                else:
                    r1a2b_res;
                if pd.isna(r2a2b_res):
                    r2a2b_res = 0;
                else:
                    r2a2b_res;   
                if r1a2a_res == 1:
                    r1a2a_res = 0.99
                if r1a2b_res == 1:
                    r1a2b_res = 0.99
                if r2a2b_res == 1:
                    r2a2b_res = 0.99 
                z_val = finding_z(r1a2a_res, r1a2b_res, r2a2b_res, userweek_1[j].shape[0])
                #print(z_val)
                p_val = finding_p(z_val)
                #print(p_val)
                p_compare_users[j][k] = p_val
    final_data = pd.DataFrame(p_compare_users)    
    final_data.to_excel("C:/Users/Vineeth reddy/Desktop/InfoSec-MS/Final Project/p_val_folder"+file_name)

os.makedirs('C:/Users/Vineeth reddy/Desktop/InfoSec-MS/Final Project/p_val_folder/')
gen_user_data("/for_10secs.xlsx")
gen_user_data("/for_227secs.xlsx")
gen_user_data("/for_5mins.xlsx")


