#20200121 PHM_feature_extraction_final.py
import os
from datetime import timedelta
import pandas as pd
import multiprocessing as mp

def feature(i, data_folder):
    sitea_path = os.path.join(data_folder, 'plant_'+ str(i) +'a.csv')   #create raw file path
    siteb_path = os.path.join(data_folder, 'plant_'+ str(i) +'b.csv')
    sitec_path = os.path.join(data_folder, 'plant_'+ str(i) +'c.csv')
    sitea_df = pd.read_csv(sitea_path, names=['component','mea_time','s1','s2','s3','s4','r1','r2','r3','r4'])  #create raw file dataframe
    siteb_df = pd.read_csv(siteb_path, names=['area','mea_time','CEC','PI'])
    sitec_df = pd.read_csv(sitec_path, names=['start_time','end_time','type'])
    group_num = 1
    PI_avg = 0
    fault_type = 0
    d = 0
    e = 0

    sitea_df['s1r4'] = sitea_df.apply(lambda x: x['s1'] * int(x['r4']), axis=1) #s1[sensor1]*r4 
    sitea_df['s2r4'] = sitea_df.apply(lambda x: x['s2'] * int(x['r4']), axis=1)
    sitea_df['s3r4'] = sitea_df.apply(lambda x: x['s3'] * int(x['r4']), axis=1)
    sitea_df['s4r4'] = sitea_df.apply(lambda x: x['s4'] * int(x['r4']), axis=1) 
    sitea_df['PI_avg'] = PI_avg	#Regional instantaneous power average
    sitea_df['type'] = fault_type
    sitea_df['group'] = group_num	   
    siteb_df['group'] = group_num
    
    #group sitea_df and siteb_df
    for a in range(len(sitea_df)): 
        if a > 0:
            time1 = pd.to_datetime(sitea_df['mea_time'][a]) - pd.to_datetime(sitea_df['mea_time'][a-1])	#Next row meatime subtract previous row meatime
            
				#More than 14 minutes is different group
            if time1 > timedelta(minutes=14):
                for c in range(d,len(siteb_df)):
                    time2 = (pd.to_datetime(siteb_df['mea_time'][c]) - pd.to_datetime(sitea_df['mea_time'][a-1]))
                    if time2 > timedelta(minutes=14):
                        d = c
                        break
                    elif c == len(siteb_df)-1:
                        siteb_df['group'][c] = group_num 
                    else:
                        siteb_df['group'][c] = group_num
                group_num = group_num+1
                sitea_df['group'][a] = group_num
                
            elif a == len(sitea_df)-1:
                sitea_df['group'][a] = group_num
                for c in range(d,len(siteb_df)):
                    time2 = (pd.to_datetime(siteb_df['mea_time'][c]) - pd.to_datetime(sitea_df['mea_time'][a]))
                    if time2 > timedelta(minutes=14):
                        d = c
                        break
                    elif c == len(siteb_df)-1:
                        siteb_df['group'][c] = group_num 
                    else:
                        siteb_df['group'][c] = group_num                   
            else:
                sitea_df['group'][a] = group_num
    
    #fill the average of siteb_df['PI'] into sitea_df['PI_avg']
    for a in range(len(sitea_df)): 
        s1b_group_df = siteb_df.loc[siteb_df['group'] == sitea_df['group'][a]]
        sitea_df.loc[sitea_df['group'] == sitea_df['group'][a], ['PI_avg']] = s1b_group_df['PI'].mean()
    
    filterc = sitec_df["type"] < 6  #get fault type 1~5 data
    sitec_df = sitec_df[filterc]    #filter sitec_df
    sitec_df.sort_values(by = 'start_time', inplace=True) #20200109 sort start_time 
    sitec_df = sitec_df.reset_index(drop=True) #20200109 reset index
    
    #fill sitea_df['type']
    for a in range(len(sitea_df)):  
        print(sitea_df['mea_time'][a])
        for b in range(e,len(sitec_df)):
            if sitea_df['mea_time'][a] >= sitec_df['start_time'][b]:
                if sitea_df['mea_time'][a] <= sitec_df['end_time'][b]:
                    sitea_df['type'][a] = sitec_df['type'][b]
                    e = b
                    break
            else:
                break
    
    filtera = sitea_df["type"] > 0  #get fault type 1~5 data
    sitea_df = sitea_df[filtera]    #filter sitea_df
    sitea_df = sitea_df[['component','mea_time','s1r4','s2r4','s3r4','s4r4','PI_avg','type','group']]
    sitea_df.to_csv(r'D:\SVM\PHM\s'+ str(i) +r'a1.csv', index=False)
    siteb_df.to_csv(r'D:\SVM\PHM\s'+ str(i) +r'b1.csv', index=False)
    sitec_df.to_csv(r'D:\SVM\PHM\s'+ str(i) +r'c1.csv', index=False)

if __name__ == '__main__': 
    
    data_folder = r'D:\SVM\PHM\PHMtrain'		#raw data folder

    dirs = os.listdir(data_folder)  #list all files
    dirs = list(filter(lambda ele: 'b' in ele, dirs))   #remove duplicate file

    p1 = mp.Pool()  #build multiprocess pool
    
    for file in dirs:
        #by file name length to get plant number
        if len(file) < 13:
            r = p1.apply_async(feature, (file[6:7], data_folder))
        else:
            r = p1.apply_async(feature, (file[6:8], data_folder))
    
    p1.close()
    p1.join()   
