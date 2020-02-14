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



#SmartVM_Constructor_retrain_v2.py 
#20190823 1017 accept dispatching

#20190822 Q1: retrain db and folder inconsistent

#from Data_Preview import Data_Preview
from Data_PreProcess import Data_PreProcess_Train, Data_PreProcess_Test
from XDI import XDI_off_line_report, Build_XDI_Model
from YDI import YDI_off_line_report, Build_YDI_Model
from MXCI_MYCI_pre import pre_MXCI_MYCI
from MXCI_MYCI import MXCI_MYCI_offline
from Model_Selection import Model_Selection
from GP_TPE_tune import Model_tuning
from prediction import modelpredict
from train import modeltrain
from Read_path import read_path
from config import read_config
from CreateLog import WriteLog
from DB_operation import select_project_waitretrain, select_project_model_by_modelid, select_project_model_by_modelname_status, update_project_STATUS_by_projectid, update_project_model_retrainstarttime_by_modelid, update_project_model_retrainendtime_retrainresult_by_modelid, update_project_model_mae_mape_by_modelid

import os
import json
import traceback
from Path import All_path
from Data_Collector import Data_Collector
# from Data_Check import Data_Check
#from Exclusion import DataRemove, FeatureExclude

from shutil import copyfile
#import numpy as np
import pandas as pd
import datetime
#from json2csv import json2csv
import socket


class SuperVM():
    def __init__(self, sorce_dir, train_data_name, config_file_name, batch_data_name_list=None):
        self.base_path = os.path.join("../Cases/", sorce_dir)
        self.base_name = os.path.basename(os.path.abspath(self.base_path))
        self.train_data_name = train_data_name
        self.train_base, self.ext = os.path.splitext(self.train_data_name)
        self.train_data = os.path.join(self.base_path, train_data_name)
        self.config_file_name = config_file_name
        self.config_file = os.path.join(self.base_path, config_file_name)
        if batch_data_name_list:
            self.batch_data = [os.path.join(self.base_path, x) for x in batch_data_name_list]
        self.in_path = os.path.join(self.base_path, "path_config.json")
        self.mdoel_dict = {}
        self.mdoel_dict["3"] = {}
        self.mdoel_dict["3"]["Train"] = Data_PreProcess_Train
        self.mdoel_dict["3"]["Test"] = Data_PreProcess_Test       
        self.mdoel_dict["4"] = {}
        self.mdoel_dict["4"]["Train"] = Build_XDI_Model
        self.mdoel_dict["4"]["Test"] = XDI_off_line_report
        self.mdoel_dict["5"] = {}
        self.mdoel_dict["5"]["Train"] = Build_YDI_Model
        self.mdoel_dict["5"]["Test"] = YDI_off_line_report
        self.mdoel_dict["8"] = {}
        self.mdoel_dict["8"]["Train"] = pre_MXCI_MYCI
        self.mdoel_dict["8"]["Test"] = MXCI_MYCI_offline

        self.retrain_data = os.path.join(self.base_path, "retrain_data.csv")
        if not os.path.exists(self.retrain_data):
            copyfile(self.train_data, self.retrain_data)

        # Other Variables initialized in methods
        self.filter_feature = None
        self.feature_lists = None
        self.current_retrain_number = None
        self.current_batch_number = None
        self.path_config = None
        self.filter_feature_dict = None
        # self.filter_dir_path = None
        self.filter_dir_name = None

    def get_filter_feature(self):
        try:
            with open(self.config_file) as json_data:
                config = json.load(json_data)
        except Exception as e:
            config = None
            error_path = os.path.join(self.base_path, "error.log")
            with open(error_path, 'a') as file:
                file.write("Error while reading config file\n")
                file.write(traceback.format_exc(e))
                raise FileNotFoundError
        self.filter_feature = config["Filter_Feature"]
        
        #20190626 remove
        self.model_pred_name = config["Model_Pred_Name"]
        #self.model_pred_name = ["XGB", "PLS"]
        return None

    def get_feature_content(self, split_flag=None):
        try:
            training_data = pd.read_csv(self.train_data)
        except Exception as e:
            training_data = None
            error_path = os.path.join(self.base_path, "error.log")
            with open(error_path, 'a') as file:
                file.write("Error while reading raw data\n")
                file.write(str(e) + "\n")
                file.write(traceback.format_exc())
                raise FileNotFoundError
        self.feature_lists = training_data[self.filter_feature].unique().tolist()
        self.feature_lists.sort()
        if split_flag is not None:
            self.filter_feature_dict = {}
            for feature_list in self.feature_lists:
                tmp_data = training_data[training_data[self.filter_feature] == feature_list].copy().reset_index(drop=True)
                self.filter_feature_dict[feature_list] = \
                    os.path.join(self.base_path, self.train_base + "_" + self.filter_feature + "_" + str(feature_list) + self.ext)
                tmp_data.to_csv(self.filter_feature_dict[feature_list], index=False)
        return None

########################################################################################################################
    def create_path(self, filter_path, path_config=None, batch_flag=None, retrain_folder_path=None, training_data=None):
        if not os.path.exists(filter_path):
            os.makedirs(filter_path)
        #######
        if os.path.exists(self.in_path):
            self.get_saved_path_config()
            path_config = self.path_config
        #######
        path = All_path(filter_path, self.config_file, path_config)
        if batch_flag is None:
            if training_data is None:
                training_data = self.train_data
            path.init_train(training_data=training_data)
            path.init_merge()
            self.current_retrain_number = path.get_current_retrain_number()
        else:
            path.init_batch(self.batch_data, retrain_folder_path)
            self.current_batch_number = path.get_current_batch_number(retrain_folder_path)
        return path.get_path_config()

    def save_path_config(self):
        with open(self.in_path, 'w') as fp:
            json.dump(self.path_config, fp, indent=4, sort_keys=True)
        return None

    def create_many_path(self, batch_flag=None, training_data_dict=None):
        if batch_flag is None:
            self.path_config = None
        # self.filter_dir_path = {}
        self.filter_dir_name = {}
        for feature_list in self.feature_lists:
            if training_data_dict is None:
                training_data = self.train_data
            else:
                training_data = training_data_dict[feature_list]

            # self.filter_dir_path[feature_list] = \
            #     os.path.join(self.base_path, self.base_name + "_" + self.filter_feature + "_" + str(feature_list))
            self.filter_dir_name[feature_list] = self.base_name + "_" + self.filter_feature + "_" + str(feature_list)
            filter_dir_path = os.path.join(self.base_path, self.filter_dir_name[feature_list])
            self.path_config = self.create_path(filter_dir_path, self.path_config,
                                                training_data=training_data)
            if training_data_dict is not None:
                os.remove(training_data)
            self.save_path_config() #20190715 retrain mode please save in for loop end, avoid not archived
        return None

    def get_saved_path_config(self):
        #20190722 avoid open same path_config
        def timer(n):
            while True:                
                try:
                    with open(self.in_path) as json_data:
                        self.path_config = json.load(json_data)
                except Exception as e:
                    print("someone open path_config, please wait 5s")
                    timer.sleep(n)
                    """
                    error_path = os.path.join(self.base_path, "error.log")
                    with open(error_path, 'a') as file:
                        file.write("Error while reading path_config.json")
                        file.write(traceback.format_exc(e))
                    raise FileNotFoundError
                    """
                else:
                    break
        timer(5)
        return None
########################################################################################################################
    def get_max_retrain_num(self, retrain_num=None):
        if retrain_num is None:
            try:
                first_key = list(self.path_config.keys())[0]
                key_list = list(self.path_config[first_key].keys())
                key_list.remove("main")
                return max([int(x) for x in key_list])
            except:
                try:
                    self.get_saved_path_config()
                    first_key = list(self.path_config.keys())[0]
                    key_list = list(self.path_config[first_key].keys())
                    key_list.remove("main")
                    return max([int(x) for x in key_list])
                except Exception as e:
                    raise FileNotFoundError("path_config not found")
        else:
            return retrain_num

    def get_max_batch_num(self, retrain_num, batch_num=None):
        if batch_num is None:
            first_key = list(self.path_config.keys())[0]
            key_list = list(self.path_config[first_key][str(retrain_num)].keys())
            key_list.remove("main")
            return max([int(x) for x in key_list])
        else:
            return batch_num  
    
    def model_normal(self, path_dict, for_batch=None):
        base_num = 3
        #20191225 Used for normal retrain(no need tune)
        modeltrain(path_dict['6']["XGB"][str(base_num + 1)], 'XGB')
        modelpredict(path_dict['6']["XGB"][str(base_num + 2)], 'XGB', "train")

        modeltrain(path_dict['6']["PLS"][str(base_num + 1)], 'PLS')
        modelpredict(path_dict['6']["PLS"][str(base_num + 2)], 'PLS', "train")       
        return None
    
    def model_abnormal(self, path_dict, for_batch=None):
        base_num = 3
        #20191225 Used for normal retrain(no need tune)
        Model_tuning(path_dict['6']["XGB"][str(base_num)], 'XGB')
        modeltrain(path_dict['6']["XGB"][str(base_num + 1)], 'XGB')
        modelpredict(path_dict['6']["XGB"][str(base_num + 2)], 'XGB', "train")
        
        Model_tuning(path_dict['6']["PLS"][str(base_num)], 'PLS')
        modeltrain(path_dict['6']["PLS"][str(base_num + 1)], 'PLS')
        modelpredict(path_dict['6']["PLS"][str(base_num + 2)], 'PLS', "train")       
        return None
    
    def retrain_phase(self, modelid, base_name, retrain_num, batch_num=None):
        retrain_num = self.get_max_retrain_num(retrain_num)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)
        path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]

        print("RETRAIN phase")
        merge_flag = '2'
        test_flag = '3'              
        
        SVM_PROJECT_MODEL = select_project_model_by_modelid(modelid)
        retrain_type = SVM_PROJECT_MODEL.RETRAIN_TYPE[0]
        retrain_type = retrain_type.strip()
        
        update_project_model_retrainstarttime_by_modelid(datetime.datetime.now(), int(modelid))
        
        Data_PreProcess_Train(path_dict['3'][merge_flag], mode="Merge")
        Build_XDI_Model(path_dict['4'][merge_flag])
        XDI_off_line_report(path_dict['4'][test_flag], mode="Merge")
        Build_YDI_Model(path_dict['5'][merge_flag])
        YDI_off_line_report(path_dict['5'][test_flag], mode="Merge")
        #20191225 judge normal or abnormal
        if retrain_type == 'NORMAL':
            self.model_normal(path_dict)
        elif retrain_type == 'ABNORMAL':
            self.model_abnormal(path_dict)
            
        Model_Selection_check, Model_Selection_msg, win = Model_Selection(path_dict['7']['1'], mode="Merge")
        #20190717 retrain must save mae/mape to db, read train data
        output_paths = {}
        if str(win).upper() == 'XGB':           
            output_paths["Model_Selection_Report"] = os.path.join(path_dict['7']['1'], "Model_Selection_Report.csv")
            select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
            out_df = select_df[(select_df["Model"] == 'XGB') & (select_df["Data"] == 'Train')]           
            update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], modelid)
        elif str(win).upper() == 'PLS':
            output_paths["Model_Selection_Report"] = os.path.join(path_dict['7']['1'], "Model_Selection_Report.csv")
            select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
            out_df = select_df[(select_df["Model"] == 'PLS') & (select_df["Data"] == 'Train')]            
            update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], modelid)
        else:
            #20190808 avoid no win
            input_path = read_path(path_dict[str(7)]['1']) #20190710 avoid flat don't save mae mape to db
            mylog = WriteLog(input_path["log_path"], input_path["error_path"])     
            config = read_config(input_path["config_path"], mylog)  
            Predict_Model = config["Model_Selection"][config["Y"][0]]["Predict_Model"]            
            
            if str(Predict_Model).upper() == 'XGB':           
                output_paths["Model_Selection_Report"] = os.path.join(path_dict['7']['1'], "Model_Selection_Report.csv")
                select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
                out_df = select_df[(select_df["Model"] == 'XGB') & (select_df["Data"] == 'Train')]           
                update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], modelid)
            elif str(Predict_Model).upper() == 'PLS':
                output_paths["Model_Selection_Report"] = os.path.join(path_dict['7']['1'], "Model_Selection_Report.csv")
                select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
                out_df = select_df[(select_df["Model"] == 'PLS') & (select_df["Data"] == 'Train')]            
                update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], modelid)
                        
        pre_MXCI_MYCI(path_dict['8'][merge_flag])
        MXCI_MYCI_offline(path_dict['8'][test_flag],  mode="Merge")       
        
        update_project_model_retrainendtime_retrainresult_by_modelid(datetime.datetime.now(), 'OK', int(modelid))

        return None
    
    def retrain_many_phase(self, base_name, retrain_num, batch_num=None):
        retrain_num = self.get_max_retrain_num(retrain_num)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)
        path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]

        print("RETRAIN many phase")
        merge_flag = '2'
        test_flag = '3'
        
        #base_name = MODEL_NAME(m2m)               
        SVM_PROJECT_MODEL = select_project_model_by_modelname_status(base_name, "RETRAIN")
        retrain_type = SVM_PROJECT_MODEL.RETRAIN_TYPE[0]
        retrain_type = retrain_type.strip()
        
        update_project_model_retrainstarttime_by_modelid(datetime.datetime.now(), SVM_PROJECT_MODEL.MODEL_ID[0])

        Data_PreProcess_Train(path_dict['3'][merge_flag], mode="Merge")
        Build_XDI_Model(path_dict['4'][merge_flag])
        XDI_off_line_report(path_dict['4'][test_flag], mode="Merge")
        Build_YDI_Model(path_dict['5'][merge_flag])
        YDI_off_line_report(path_dict['5'][test_flag], mode="Merge")
        
        if retrain_type == 'NORMAL':
            self.model_normal(path_dict)
        elif retrain_type == 'ABNORMAL':
            self.model_abnormal(path_dict)
            
        Model_Selection_check, Model_Selection_msg, win = Model_Selection(path_dict['7']['1'], mode="Merge")
        
        output_paths = {}
        if str(win).upper() == 'XGB':           
            output_paths["Model_Selection_Report"] = os.path.join(path_dict['7']['1'], "Model_Selection_Report.csv")
            select_df = pd.read_csv(output_paths["Model_Selection_Report"])
			# 20190709 change Test to train By Peter Lin
            out_df = select_df[(select_df["Model"] == 'XGB') & (select_df["Data"] == 'Train')]            
            update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PROJECT_MODEL.MODEL_ID[0])
        elif str(win).upper() == 'PLS':
            output_paths["Model_Selection_Report"] = os.path.join(path_dict['7']['1'], "Model_Selection_Report.csv")
            select_df = pd.read_csv(output_paths["Model_Selection_Report"])
			# 20190709 change Test to train By Peter Lin			
            out_df = select_df[(select_df["Model"] == 'PLS') & (select_df["Data"] == 'Train')]            
            update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PROJECT_MODEL.MODEL_ID[0])
        else:
            #20190808 avoid no win
            input_path = read_path(path_dict[str(7)]['1']) #20190710 avoid flat don't save mae mape to db
            mylog = WriteLog(input_path["log_path"], input_path["error_path"])     
            config = read_config(input_path["config_path"], mylog)  
            Predict_Model = config["Model_Selection"][config["Y"][0]]["Predict_Model"]
            
            if str(Predict_Model).upper() == 'XGB':           
                output_paths["Model_Selection_Report"] = os.path.join(path_dict['7']['1'], "Model_Selection_Report.csv")
                select_df = pd.read_csv(output_paths["Model_Selection_Report"])
    			# 20190709 change Test to train in merger phase By Peter Lin
                out_df = select_df[(select_df["Model"] == 'XGB') & (select_df["Data"] == 'Train')]            
                update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PROJECT_MODEL.MODEL_ID[0])
            elif str(Predict_Model).upper() == 'PLS':
                output_paths["Model_Selection_Report"] = os.path.join(path_dict['7']['1'], "Model_Selection_Report.csv")
                select_df = pd.read_csv(output_paths["Model_Selection_Report"])
    			# 20190709 change Test to train in merge phase By Peter Lin			
                out_df = select_df[(select_df["Model"] == 'PLS') & (select_df["Data"] == 'Train')]            
                update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PROJECT_MODEL.MODEL_ID[0])
            
        pre_MXCI_MYCI(path_dict['8'][merge_flag])
        MXCI_MYCI_offline(path_dict['8'][test_flag],  mode="Merge")

        update_project_model_retrainendtime_retrainresult_by_modelid(datetime.datetime.now(), 'OK', int(SVM_PROJECT_MODEL.MODEL_ID[0]))

        return None
    
    def many_retrain_phase(self, retrain_num, batch_num):
        for feature in self.feature_lists:
            self.retrain_many_phase(base_name=self.filter_dir_name[feature], retrain_num=retrain_num, batch_num=batch_num)
        return None
        
    def many_data_porter(self, retrain_num, batch_num, mode, pause_flag):
        dc = Data_Collector(self.base_path, self.train_data, self.config_file, pause_flag)
        dc.init_collector_dir(retrain_num, batch_num)
        for feature in self.feature_lists:
            dc.data_porter(path_config=self.path_config, base_name=self.filter_dir_name[feature],
                           retrain_num=retrain_num, batch_num=batch_num, mode=mode,
                           feature_name=self.filter_feature, feature=feature)
        dc.data_organize(retrain_num, batch_num, mode,
                         feature_name=self.filter_feature, feature_lists=self.feature_lists)
        return None 

    def copy_model_param(self, retrain_num, batch_num):

        self.get_filter_feature()
        for model in self.model_pred_name:
            old_dir = self.path_config[self.base_name][str(retrain_num-1)][str(batch_num)]['6'][model]['0']
            old_path = os.path.join(old_dir, "paramsjson.json")
            new_dir = self.path_config[self.base_name][str(retrain_num)][str(batch_num)]['6'][model]['0']
            new_path = os.path.join(new_dir, "paramsjson.json")
            copyfile(old_path, new_path)
        old_dir = self.path_config[self.base_name][str(retrain_num - 1)][str(batch_num)]['0']['main']
        old_path = os.path.join(old_dir, self.config_file_name)
        new_dir = self.path_config[self.base_name][str(retrain_num)][str(batch_num)]['0']['main']
        new_path = os.path.join(new_dir, self.config_file_name)
        copyfile(old_path, new_path)
        return None

    def many_copy_model_param(self, retrain_num, batch_num):
        
        self.get_filter_feature()
        if self.feature_lists is None:
            self.get_feature_content()

        if self.filter_dir_name is None:
            self.filter_dir_name = {}
            for feature in self.feature_lists:
                self.filter_dir_name[feature] = self.base_name + "_" + self.filter_feature + "_" + str(feature)

        for feature in self.feature_lists:
            for model in self.model_pred_name:
                old_dir = self.path_config[self.filter_dir_name[feature]][str(retrain_num-1)][str(batch_num)]\
                          ['6'][model]['0']
                old_path = os.path.join(old_dir, "paramsjson.json")
                new_dir = self.path_config[self.filter_dir_name[feature]][str(retrain_num)][str(batch_num)]\
                          ['6'][model]['0']
                new_path = os.path.join(new_dir, "paramsjson.json")
                print(old_path, new_path)
                copyfile(old_path, new_path)
            old_dir = self.path_config[self.filter_dir_name[feature]][str(retrain_num - 1)][str(batch_num)]\
                                       ['0']['main']
            old_path = os.path.join(old_dir, self.config_file_name)
            new_dir = self.path_config[self.filter_dir_name[feature]][str(retrain_num)][str(batch_num)]\
                                       ['0']['main']
            new_path = os.path.join(new_dir, self.config_file_name)
            copyfile(old_path, new_path)
        return None

    def one_to_all_retrain(self, modelid):
        path = os.path.join(self.base_path, self.base_name)
        
        pause_flag = '0'
        dc = Data_Collector(self.base_path, self.train_data, self.config_file, pause_flag)
        self.path_config = self.create_path(path)
        self.save_path_config()

        retrain_num = self.get_max_retrain_num(retrain_num=self.current_retrain_number)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=None)
        print("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num))

        #20200109 Copy model parameter before retrain
        self.copy_model_param(retrain_num, batch_num)
        dc.init_collector_dir(retrain_num, batch_num)
        self.retrain_phase(modelid, base_name=self.base_name, retrain_num=retrain_num)
        dc.data_porter(path_config=self.path_config, base_name=self.base_name,
                       retrain_num=retrain_num, batch_num=batch_num, mode="Merge")
        
        SVM_PROJECT_MODEL = select_project_model_by_modelid(modelid)
        project_id = SVM_PROJECT_MODEL.PROJECT_ID[0]       
        update_project_STATUS_by_projectid("RETRAIN_OK", project_id) #20190823 update project status to retrain ok
        
        return None

    def many_to_many_retrain(self, modelid):
        pause_flag = '0'
        dc = Data_Collector(self.base_path, self.train_data, self.config_file, pause_flag)

        self.get_filter_feature()
        self.get_feature_content(split_flag=True)
        self.create_many_path(training_data_dict=self.filter_feature_dict)

        retrain_num = self.get_max_retrain_num(retrain_num=self.current_retrain_number)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=None)

        #20200109 Copy model parameter before retrain
        self.many_copy_model_param(retrain_num, batch_num)
        dc.init_collector_dir(retrain_num, batch_num)
        print("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num))

        self.many_retrain_phase(retrain_num=retrain_num, batch_num=batch_num)
        self.many_data_porter(retrain_num=retrain_num, batch_num=batch_num, mode="Merge", pause_flag=pause_flag)  
        
        SVM_PROJECT_MODEL = select_project_model_by_modelid(modelid)
        project_id = SVM_PROJECT_MODEL.PROJECT_ID[0]       
        update_project_STATUS_by_projectid("RETRAIN_OK", project_id)
        
        return None

if __name__ == '__main__':       
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    
    SVM_PP_MODEL =  select_project_waitretrain()
    
    if len(SVM_PP_MODEL) != 0:
        #20190823 training server must meets the local ip
        if SVM_PP_MODEL.TRAINING_SERVER[0] == ip:
        
            SVM = SuperVM(str(SVM_PP_MODEL.PROJECT_NAME[0]), str(SVM_PP_MODEL.UPLOAD_FILE[0]), r"Config.json")
            modelid = SVM_PP_MODEL.MODEL_ID[0]
            SVM_PROJECT_MODEL = select_project_model_by_modelid(modelid)
            project_id = SVM_PROJECT_MODEL.PROJECT_ID[0]
            
            update_project_STATUS_by_projectid("RETRAINING", project_id) #20190823 update project status to retraining
            
            if (str(SVM_PP_MODEL.MODEL_TYPE[0]) == '1') or (str(SVM_PP_MODEL.MODEL_TYPE[0]) == '2'):
                
                SVM.one_to_all_retrain(modelid)                                    
            
            elif str(SVM_PP_MODEL.MODEL_TYPE[0]) == '3':

                SVM.many_to_many_retrain(modelid)                              

		
		

# SmartVM_Constructor_online_x_combine_allrun.py
#20190902 add alone&all start-end time by log
#20190904 modify run for loop to avoid get same X

from Data_PreProcess import Data_PreProcess_Train, Data_PreProcess_Test, OnLine_Data_PreProcess
from XDI import XDI_off_line_report, Build_XDI_Model, OnLine_XDI
from YDI import YDI_off_line_report, Build_YDI_Model
from MXCI_MYCI_pre import pre_MXCI_MYCI
from MXCI_MYCI import CI_online, CI_online_multi
#from MXCI_MYCI_20190805 import MXCI_MYCI_offline, CI_online_old, CI_online_old_multi #20190827 use old CI_online, wait david check ok
#from XGB_model import xgb_online_predict
#from PLS_model import pls_online_predict_x, pls_online_predict_x_py
from Read_path import read_path
from config import read_config
from CreateLog import WriteLog
from DB_operation import select_project_with_model_by_projectid_onlineserver_predictstatus_modelstatus, select_project_model_by_projectid_status, select_project_model_by_projectid_status_filterfeature, select_project_config_by_modelid_parameter3RD, select_project_online_scantime_by_projectid_datatype, select_online_parameter_by_projectid_datatype, select_online_status_by_projectid_runindex, select_projectx_run_by_itime, select_projectx_parameter_data_by_runindex, select_projectx_predict_data_by_runindex_parameter_modelid, select_projectx_contribution_data_by_runindex_parameter_model, update_project_predictstatus_by_projectid, update_online_scantime_lastscantime_by_projectid_datatype, update_online_status_xdialarm_by_projectid_runindex, update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid, update_projectx_run_signal_by_runindex, update_projectx_run_xdialarm_by_runindex, update_projectx_contribution_data_contribution_by_runindex_parameter_model, insert_online_status_projectid_runindex_xdialarm_itime, insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict, insert_projectx_contribution_data_runindex_model_parameter_contribution, insert_svm_error_run_log
from prediction import modelpredict as OnlineModelPredict

import SystemConfig.ServerConfig as ServerConfig # Reaver
import ProcException as ProcEx # Reaver
import APLog as APLog # Reaver

import pandas as pd
import datetime
import os
import json
import traceback
from shutil import copyfile
import time
import socket
import requests

SystemLog_Path = ServerConfig.Log_Path + "\\" + datetime.datetime.now().strftime("%Y%m%d") + ServerConfig.Log_Keep_Day + "\\"  # Reaver
ErrorLog_Path = ServerConfig.Log_Path + "\\" + datetime.datetime.now().strftime("%Y%m%d") + ServerConfig.Log_Keep_Day + "\\"  # Reaver

class SuperVM():
    def __init__(self, sorce_dir, train_data_name, config_file_name, projectid, batch_data_name_list=None):
        basePath = os.path.join(ServerConfig.ModelFilePath, sorce_dir)
        self.base_path = basePath
        self.base_name = os.path.basename(os.path.abspath(self.base_path))
        self.train_data_name = train_data_name
        self.train_base, self.ext = os.path.splitext(self.train_data_name)       
        self.train_data = os.path.join(self.base_path, train_data_name)
        self.config_file = os.path.join(self.base_path, config_file_name)
        print(self.train_data )
        print(self.config_file )
        if batch_data_name_list:
            self.batch_data = [os.path.join(self.base_path, x) for x in batch_data_name_list]
        self.in_path = os.path.join(self.base_path, "path_config.json")
        print("self.in_path=" , self.in_path )
        self.mdoel_dict = {}
        self.mdoel_dict["3"] = {}
        self.mdoel_dict["3"]["Train"] = Data_PreProcess_Train
        self.mdoel_dict["3"]["Test"] = Data_PreProcess_Test
        self.mdoel_dict["4"] = {}
        self.mdoel_dict["4"]["Train"] = Build_XDI_Model
        self.mdoel_dict["4"]["Test"] = XDI_off_line_report
        self.mdoel_dict["5"] = {}
        self.mdoel_dict["5"]["Train"] = Build_YDI_Model
        self.mdoel_dict["5"]["Test"] = YDI_off_line_report
        #self.mdoel_dict["8"] = {}
        #self.mdoel_dict["8"]["Train"] = pre_MXCI_MYCI
        #self.mdoel_dict["8"]["Test"] = MXCI_MYCI_offline

        self.retrain_data = os.path.join(self.base_path, "retrain_data.csv")
        if not os.path.exists(self.retrain_data):
            copyfile(self.train_data, self.retrain_data)

        # Other Variables initialized in methods
        self.filter_feature = None
        self.feature_lists = None
        self.current_retrain_number = None
        self.current_batch_number = None
        self.path_config = None
        self.filter_feature_dict = None

        self.filter_dir_name = None
        
    
    def get_filter_feature(self):
        try:
            with open(self.config_file) as json_data:
                config = json.load(json_data)
        except Exception as e:
            config = None
            error_path = os.path.join(self.base_path, "error.log")
            with open(error_path, 'a') as file:
                file.write("Error while reading config file\n")
                file.write(traceback.format_exc(e))
                raise FileNotFoundError
        self.filter_feature = config["Filter_Feature"]
        self.model_pred_name = config["Model_Pred_Name"]
        return None
    

    def get_feature_content(self, split_flag=None):
        try:
            training_data = pd.read_csv(self.train_data)
        except Exception as e:
            training_data = None
            error_path = os.path.join(self.base_path, "error.log")
            with open(error_path, 'a') as file:
                file.write("Error while reading raw data\n")
                file.write(str(e) + "\n")
                file.write(traceback.format_exc())
                raise FileNotFoundError
        self.feature_lists = training_data[self.filter_feature].unique().tolist()
        self.feature_lists.sort()
        if split_flag is not None:
            self.filter_feature_dict = {}
            for feature_list in self.feature_lists:
                tmp_data = training_data[training_data[self.filter_feature] == feature_list].copy().reset_index(drop=True)
                self.filter_feature_dict[feature_list] = \
                    os.path.join(self.base_path, self.train_base + "_" + self.filter_feature + "_" + str(feature_list) + self.ext)
                tmp_data.to_csv(self.filter_feature_dict[feature_list], index=False)
        return None
########################################################################################################################
    def get_saved_path_config(self):
        try:
            with open(self.in_path) as json_data:
                tPath = json_data.read()
                tPath = tPath.replace(r"../Cases/", r"D:\\SVM\\Cases\\")
                tPath = tPath.replace(r"/", r"\\")
                self.path_config = json.loads(tPath)
        except Exception as e:
            error_path = os.path.join(self.base_path, "error.log")
            with open(error_path, 'a') as file:
                file.write("Error while reading path_config.json")
                file.write(traceback.format_exc(e))
            raise FileNotFoundError
        return None


    def get_max_retrain_num(self, retrain_num=None):
        if retrain_num is None:
            try:
                first_key = list(self.path_config.keys())[0]
                key_list = list(self.path_config[first_key].keys())
                key_list.remove("main")
                return max([int(x) for x in key_list])
            except:
                try:
                    self.get_saved_path_config()
                    first_key = list(self.path_config.keys())[0]
                    key_list = list(self.path_config[first_key].keys())
                    key_list.remove("main")
                    return max([int(x) for x in key_list])
                except Exception as e:
                    raise FileNotFoundError("path_config not found")
        else:
            return retrain_num

    def get_max_batch_num(self, retrain_num, batch_num=None):
        if batch_num is None:
            first_key = list(self.path_config.keys())[0]
            key_list = list(self.path_config[first_key][str(retrain_num)].keys())
            key_list.remove("main")
            return max([int(x) for x in key_list])
        else:
            return batch_num
    
    def model_predict_online_x(self, df_online_X, path_dict, projectid):
        input_path = read_path(path_dict['3']["3"])
        #self.mylog = WriteLog(input_path["log_path"], input_path["error_path"]) 
        mylog = WriteLog(SystemLog_Path + str(projectid) + ServerConfig.LogFileExtension, ErrorLog_Path + str(projectid) + ServerConfig.LogFileExtension)  # Reaver
        xgb_online_predict_start = datetime.datetime.now()  
        mylog.info(str(projectid) + "_xgb_online_predict_start: " + str(xgb_online_predict_start))
                        
        pls_online_predict_x_start = datetime.datetime.now()
        xgb_online_predict_interval = pls_online_predict_x_start - xgb_online_predict_start
        mylog.info(str(projectid) + "_pls_online_predict_x_start: " + str(pls_online_predict_x_start))
        
        mylog.info(str(projectid) + "_xgb_online_predict_interval: " + str(xgb_online_predict_interval))
          
        APLog.WriteApLog("config_path=" + input_path["config_path"], projectid)  
        
        config = read_config(input_path["config_path"], mylog)
        config_y = config["Y"][0]
        Baseline_Model = config["Model_Selection"][config_y]["Baseline_Model"] 
        Predict_Model = config["Model_Selection"][config_y]["Predict_Model"] 
        APLog.WriteApLog("Baseline_Model=" + Baseline_Model + " , Predict_Model=" + Predict_Model, projectid)  
        
        #20191022 add , to avoid 'int' is not iterable
        #20191205 all retrain
        #if (int(projectid) in (48,50,85,171,196,426,454,572,642,660,661,662,664,693,694,728,731,732,733,734,763,845,886,910,914,943,956,977,986,1001,1003,1005,1011,1019,1023,1035,1041,1054,1057,1145,1146,1147,1148,1159,1274,1290,1336,1379,1434,1469,1487,1511,1517,1519,1521)) or (int(projectid) > 1521):   
        APLog.WriteApLog("PLS Start", projectid)
        PLS_Path = str(path_dict['6']["PLS"]["5"]).replace("/", "\\")
        APLog.WriteApLog("PLS path_dict 6-PLS-5=" + PLS_Path, projectid)
        #pls_y_predc = pls_online_predict_x_py(PLS_Path, df_online_X, projectid)
        #xgb_y_pred = xgb_online_predict(path_dict['6']["XGB"]["5"], df_online_X) #20190702 eat 05_Prediction  
        
        pls_online_predict_x_end = datetime.datetime.now()
        pls_online_predict_x_interval = pls_online_predict_x_end - pls_online_predict_x_start
        mylog.info(str(projectid) + "_pls_online_predict_x_end: " + str(pls_online_predict_x_end))
        mylog.info(str(projectid) + "_pls_online_predict_x_interval: " + str(pls_online_predict_x_interval))
        APLog.WriteApLog("PLS End", projectid)
        #20190829 check type and return Predict_Model & Baseline_Model(str)
        #xgb_y_pred_list = xgb_y_pred.tolist()
        #pls_y_pred_list = pls_y_predc.tolist()
        
        tDataName = 'online'
        tModelName = Predict_Model
        Predict_data_folder = str(path_dict['6'][Predict_Model]["5"]).replace("/", "\\")        
        APLog.WriteApLog("tDataName=" + tDataName, projectid)
        APLog.WriteApLog("tModelName=" + tModelName, projectid)
        APLog.WriteApLog("Predict_data_folder=" + Predict_data_folder, projectid)
        
        Predict_infoChk , Predict_msg, Predict_value = OnlineModelPredict(Predict_data_folder, tModelName, tDataName, df_online_X)
        #tPredict_value = Predict_value.tolist()
        #APLog.WriteApLog("---tPredict_value=" + tPredict_value[0], projectid)  
        APLog.WriteApLog("Predict_infoChk=" + str(Predict_infoChk) + ", Predict_msg=" + str(Predict_msg)+ ", Predict_value=" +  str(Predict_value), projectid)  
        
        
        tModelName = Baseline_Model
        Baseline_data_folder = str(path_dict['6'][Baseline_Model]["5"]).replace("/", "\\")        
        APLog.WriteApLog("tDataName=" + tDataName, projectid)
        APLog.WriteApLog("tModelName=" + tModelName, projectid)
        APLog.WriteApLog("Baseline_data_folder=" + Baseline_data_folder, projectid)        
        
        Baseline_infoChk , Baseline_msg, Baseline_value = OnlineModelPredict(Baseline_data_folder, tModelName, tDataName, df_online_X)        
        APLog.WriteApLog("Baseline_infoChk=" + str(Baseline_infoChk) + ", Baseline_msg=" + str(Baseline_msg)+ ", Baseline_value=" + str(Baseline_value), projectid)  
                
        # return Predict_Model , Baseline_Model
        return Predict_value, Baseline_value
    
        #if Baseline_Model == "PLS":
        #    return xgb_y_pred, pls_y_pred_list[0]
        #else:
        #    return pls_y_pred_list[0], xgb_y_pred
        
        #else:         
        #    APLog.WriteApLog("SIMCA PLS Start", projectid)
        #    pls_online_predict_x(path_dict['6']["PLS"]["5"], df_online_X, projectid)
                        
        #    pls_y_name = config["Y"][0]+"_pred_PLS"
            
        #    pls_predict_test_path = os.path.join(path_dict['6']["PLS"]["5"], "trainPredResult.csv")
        #    pls_test=pd.read_csv(pls_predict_test_path)
        #    pls_test.rename({'Y_pred': pls_y_name}, axis=1, inplace=True) #20190722 after pls predict, must rename
        #    pls_y_pred = pls_test[pls_y_name]
        #    if len(pls_y_pred) == 0:
        #        pls_y_predc = 0
        #    else:
        #        pls_y_predc = pls_y_pred[0] #20190621 because return series, must assign location
        #    APLog.WriteApLog("SIMCA PLS End", projectid)
            
        #    pls_online_predict_x_end = datetime.datetime.now()
        #    pls_online_predict_x_interval = pls_online_predict_x_end - pls_online_predict_x_start
        #    mylog.info(str(projectid) + "_pls_online_predict_x_end: " + str(pls_online_predict_x_end))
        #    mylog.info(str(projectid) + "_pls_online_predict_x_interval: " + str(pls_online_predict_x_interval))
            
            #20190912 delete pls csv,avoid get pre data 
        #    try:
        #        os.remove(pls_predict_test_path)
        #    except OSError as e:
        #        print(e)
        #    else:
        #        print("File is deleted successfully")
            
            #20190829 check type and return Predict_Model & Baseline_Model(str)
        #    if Baseline_Model == "PLS":
        #        return xgb_y_pred, pls_y_predc 
        #    else:
        #        return pls_y_predc, xgb_y_pred
        
    def run_o2m(self, df, path_dict, projectid, project_id, datatype, SVM_PROJECT_MODEL, server_name, db_name):
        mylog = WriteLog(SystemLog_Path + str(projectid) + ServerConfig.LogFileExtension, ErrorLog_Path + str(projectid) + ServerConfig.LogFileExtension)  # Reaver
        #input_path = read_path(path_dict['3']["3"])
        #self.mylog = WriteLog(input_path["log_path"], input_path["error_path"])  
        OnLine_apply_start = datetime.datetime.now()  
        mylog.info(str(projectid) + "_OnLine_apply_start: " + str(OnLine_apply_start))
        mylog.info("run_o2m Start")
        APLog.WriteApLog("run_o2m Start", projectid)  
        #20190930 for different base and online server
        SVM_ONLINE_PARAMETER = select_online_parameter_by_projectid_datatype(projectid, datatype)
        SVM_PARAMETER_DATA1 = select_projectx_parameter_data_by_runindex(project_id, df['RUNINDEX'], server_name, db_name)
        SVM_ONLINE_PARAMETER.rename(columns = {'ONLINE_PARAMETER':'PARAMETER'}, inplace = True) 

        SVM_PARAMETER_MERGE = pd.merge(SVM_ONLINE_PARAMETER, SVM_PARAMETER_DATA1, how='left', on='PARAMETER')
        SVM_ONLINE_PARAMETER_null = SVM_PARAMETER_MERGE[SVM_PARAMETER_MERGE['PARAM_VALUE'].isnull()]
        SVM_ONLINE_PARAMETER_null = SVM_ONLINE_PARAMETER_null.reset_index(drop=True) #20191002 reset_index avoid for loop error
        SVM_ONLINE_PARAMETER_notnull = SVM_PARAMETER_MERGE[SVM_PARAMETER_MERGE['PARAM_VALUE'].notnull()]
        SVM_ONLINE_PARAMETER_notnull = SVM_ONLINE_PARAMETER_notnull.reset_index(drop=True)
        
        for k in range(len(SVM_ONLINE_PARAMETER_null)):
                                  
            if str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'TOOLID':
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['TOOLID']                           
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'LOTNAME':   
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['LOTNAME']                        
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'FOUPNAME':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['FOUPNAME']
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'OPID':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['OPID']
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'RECIPE':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['RECIPE']
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'ABBRID':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['ABBRID']
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'PRODUCT':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['PRODUCT']
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'MODELNO':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['MODELNO']
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'CHAMBER':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['CHAMBER']
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'SHEETID':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['SHEETID']
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'SLOTNO':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['SLOTNO']
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'POINT':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['POINT']
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'FILTER_FEATURE':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['FILTER_FEATURE']                   
        
        SVM_PARAMETER_DATA = pd.concat([SVM_ONLINE_PARAMETER_notnull, SVM_ONLINE_PARAMETER_null], axis=0, ignore_index=True) 
        #mylog.info(str(SVM_PARAMETER_DATA.shape))
        SVM_PARAMETER_DATA = SVM_PARAMETER_DATA.sort_values("SEQ").reset_index(drop=True)
        SVM_PARAMETER_DATA = SVM_PARAMETER_DATA[["OFFLINE_PARAMETER", "PARAM_VALUE"]]     
        #mylog.info(str(SVM_PARAMETER_DATA.shape))
                                                                                                                                                                                
        if len(SVM_PARAMETER_DATA) == 0:
            print('SVM_PARAMETER_DATA null')  #20190619 add inert sql in the future
            mylog.info("run_o2m SVM_PARAMETER_DATA null")
            APLog.WriteApLog("run_o2m SVM_PARAMETER_DATA null", projectid)
        else:                                           
            df_SVM_PARAMETER_DATA_lite_T = SVM_PARAMETER_DATA.T #20190619 行列互轉
            headers = df_SVM_PARAMETER_DATA_lite_T.iloc[0] #20190619 first row set header
            df_SVM_PARAMETER_DATA_lite_T_H  = pd.DataFrame(df_SVM_PARAMETER_DATA_lite_T.values[1:], columns=headers)
            
            #mylog.info(list(df_SVM_PARAMETER_DATA_lite_T_H.columns))
            
            print("online x phase[ns]")
            mylog.info("run_o2m online x phase[ns]")
            APLog.WriteApLog("run_o2m online x phase[ns]", projectid)
            OnLine_Data_PreProcess_start = datetime.datetime.now()  
            mylog.info(str(projectid) + "_OnLine_Data_PreProcess_start: " + str(OnLine_Data_PreProcess_start))
            #20191009 avoid runindex duplicate error 
            try:
                df_online_X, Missing_Ratio = OnLine_Data_PreProcess(df_SVM_PARAMETER_DATA_lite_T_H, path_dict['3']["3"], check_include_y=False) 
            except Exception as e:
                errMsg = ProcEx.ProcessException(e) #Reaver
                print(errMsg)
                mylog.info(str(projectid) + ' - ' + errMsg)
                mylog.error(df['RUNINDEX'])
                update_online_scantime_lastscantime_by_projectid_datatype(df['ITIME'], projectid, 'X')
                return #20191018 error return nothing
            
            for col in df_online_X.columns:
                if type(df_online_X[col].values[0]) == str:
                    continue
                if (abs(df_online_X[col].values[0]) < 1e-10) & (abs(df_online_X[col].values[0]) > 1e10):
                    print(col, df_online_X[col])
            df_online_X.T.to_csv(r"D:\SVM\Modules\df_x.csv", index=False)
            OnLine_XDI_start = datetime.datetime.now()
            OnLine_Data_PreProcess_interval = OnLine_XDI_start - OnLine_Data_PreProcess_start
            mylog.info(str(projectid) + "_OnLine_XDI_start: " + str(OnLine_XDI_start))
            mylog.info(str(projectid) + "_OnLine_Data_PreProcess_interval: " + str(OnLine_Data_PreProcess_interval))
            
            XDI_online_value, XDI_Threshold, XDI_Check, df_contri = OnLine_XDI(df_online_X, path_dict['4']["3"])
            
            model_predict_online_x_start = datetime.datetime.now()
            OnLine_XDI_interval = model_predict_online_x_start - OnLine_XDI_start
            mylog.info(str(projectid) + "_model_predict_online_x_start: " + str(model_predict_online_x_start))
            mylog.info(str(projectid) + "_OnLine_XDI_interval: " + str(OnLine_XDI_interval))
            #20191021 avoid retrain cause predict stop 
            #try:
            y_pred, y_base = self.model_predict_online_x(df_online_X, path_dict, str(projectid))
            #except Exception as e:
            #    errMsg = ProcEx.ProcessException(e) #Reaver
            #    APLog.WriteApLog(str(df['RUNINDEX']) + " -Error- " + errMsg , projectid)
                #mylog.info(str(projectid) + ' - ' + errMsg)
                #mylog.error(df['RUNINDEX'])
            #    return
            print('**********')
            print(y_pred)
            print(y_base)
            print('**********')
            CI_online_start = datetime.datetime.now()  
            mylog.info(str(projectid) + "_CI_online_start: " + str(CI_online_start))
            
            #20190828 int must equal int, execute CI_online/CI_online_old
            #if (int(projectid) in (50,67,95,171,174,426,454)) or (int(projectid) > 570):
            #try:
            MXCI_value, MXCI_T, MYCI_value, MYCI_T, light = CI_online(path_dict['8']["3"], df_online_X, y_pred, y_base)
            #except Exception as e:
            #    errMsg = ProcEx.ProcessException(e) #Reaver
            #    APLog.WriteApLog(str(df['RUNINDEX']) + " -Error- " + errMsg , projectid)
            #else:
            #    MXCI_value, MXCI_T, MYCI_value, MYCI_T, light = CI_online_old(path_dict['8']["3"], df_online_X, y_pred, y_base)
            
            print("-"*10)
            print(MXCI_value, MXCI_T, MYCI_value, MYCI_T, light)
            print("-"*10)
            
            CI_online_end = datetime.datetime.now()
            CI_online_interval = CI_online_end - CI_online_start
            mylog.info(str(projectid) + "_CI_online_end: " + str(CI_online_end))
            mylog.info(str(projectid) + "_CI_online_interval: " + str(CI_online_interval))
                                   
            #SVM_PROJECT_MODEL = select_project_model_by_projectid_status(projectid, "ONLINE")
            
            if XDI_online_value is not None:
                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, df['RUNINDEX'], 'XDI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, df['RUNINDEX'], 'XDI', XDI_online_value, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                
                else:  
                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, XDI_online_value, 0, df['RUNINDEX'], 'XDI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                      
            if XDI_Threshold is not None:
                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, df['RUNINDEX'], 'XDI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, df['RUNINDEX'], 'XDI_Threshold', XDI_Threshold, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)  
                else: 
                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, XDI_Threshold, 0, df['RUNINDEX'], 'XDI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)                                             
            
            if XDI_Check is not None:
                if int(XDI_Check) == 0:
                    SVM_ONLINE_STATUS = select_online_status_by_projectid_runindex(projectid, df['RUNINDEX'])
                    if len(SVM_ONLINE_STATUS) == 0:
                        insert_online_status_projectid_runindex_xdialarm_itime(projectid, df['RUNINDEX'], 1, datetime.datetime.now()) #20190710 xdi_alarm 0->1                                
                    else: 
                        update_online_status_xdialarm_by_projectid_runindex(1, projectid, df['RUNINDEX'])  
                    update_projectx_run_xdialarm_by_runindex(project_id, 1, df['RUNINDEX'], server_name, db_name)                                                   
            
            if y_pred is not None:
                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, df['RUNINDEX'], 'Predict_Y', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                if len(SVM_PROJECT_PREDICT_DATA) == 0: 
                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, df['RUNINDEX'], 'Predict_Y', y_pred, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                  
                else: 
                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, y_pred, 0, df['RUNINDEX'], 'Predict_Y', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
            
            #20190808 insert y_base to prdeict_data
            if y_base is not None:
                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, df['RUNINDEX'], 'Baseline_Y', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                if len(SVM_PROJECT_PREDICT_DATA) == 0: 
                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, df['RUNINDEX'], 'Baseline_Y', y_base, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                  
                else: 
                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, y_base, 0, df['RUNINDEX'], 'Baseline_Y', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
            
            if MXCI_value is not None:
                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, df['RUNINDEX'], 'MXCI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                if len(SVM_PROJECT_PREDICT_DATA) == 0:   
                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, df['RUNINDEX'], 'MXCI', MXCI_value, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)  
                else: 
                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MXCI_value, 0, df['RUNINDEX'], 'MXCI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                
            if MXCI_T is not None:
                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, df['RUNINDEX'], 'MXCI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, df['RUNINDEX'], 'MXCI_Threshold', MXCI_T, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                    
                else: 
                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MXCI_T, 0, df['RUNINDEX'], 'MXCI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                
            if MYCI_value is not None:
                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, df['RUNINDEX'], 'MYCI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                #MYCI_value = float(str(MYCI_value)[0:10]) #20190911 not additional to change digits
                MYCI_value = round(MYCI_value,50)
                if len(SVM_PROJECT_PREDICT_DATA) == 0:   
                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, df['RUNINDEX'], 'MYCI', MYCI_value, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                   
                else: 
                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MYCI_value, 0, df['RUNINDEX'], 'MYCI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                
            if MYCI_T is not None:
                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, df['RUNINDEX'], 'MYCI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, df['RUNINDEX'], 'MYCI_Threshold', MYCI_T, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                    
                else: 
                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MYCI_T, 0, df['RUNINDEX'], 'MYCI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
            
            if light is not None:
                update_projectx_run_signal_by_runindex(project_id, light, df['RUNINDEX'], server_name, db_name)
            
            if df_contri is not None:
                for j in range(df_contri.shape[0]):
                    SVM_PROJECT_CONTRIBUTION_DATA = select_projectx_contribution_data_by_runindex_parameter_model(project_id, df['RUNINDEX'], df_contri.Col_Name[j], 'XDI', server_name, db_name)
                    if len(SVM_PROJECT_CONTRIBUTION_DATA) == 0:  
                        insert_projectx_contribution_data_runindex_model_parameter_contribution(project_id, df['RUNINDEX'], 'XDI', df_contri.Col_Name[j], df_contri.Contribution[j], server_name, db_name)
                    else: 
                        update_projectx_contribution_data_contribution_by_runindex_parameter_model(project_id, df_contri.Contribution[j], df['RUNINDEX'], df_contri.Col_Name[j], 'XDI', server_name, db_name)
                        
            if Missing_Ratio is not None:
                for Missing in Missing_Ratio:
                    SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, df['RUNINDEX'], 'Missing_Ratio', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, df['RUNINDEX'], 'Missing_Ratio', Missing, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                    
                else: 
                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, Missing, 0, df['RUNINDEX'], 'Missing_Ratio', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)   

            update_online_scantime_lastscantime_by_projectid_datatype(df['ITIME'], projectid, 'X') 
            
            DB_end = datetime.datetime.now()
            DB_interval = DB_end - CI_online_end
            mylog.info(str(projectid) + "_DB_end: " + str(DB_end))
            mylog.info(str(projectid) + "_DB_interval: " + str(DB_interval))    
        
    
    def online_x_phase(self, projectid, datatype, server_name, db_name, base_name, retrain_num, batch_num=None):        
        retrain_num = self.get_max_retrain_num(retrain_num)
                
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)
                
        path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]
                
        #20190902 for presure test alone start-end time
        #input_path = read_path(path_dict['3']["3"])
        #self.mylog = WriteLog(input_path["log_path"], input_path["error_path"])  
        mylog = WriteLog(SystemLog_Path + str(projectid) + ServerConfig.LogFileExtension, ErrorLog_Path + str(projectid) + ServerConfig.LogFileExtension)  # Reaver
        o2m_start = datetime.datetime.now()  
        APLog.WriteApLog(str(projectid) + "_o2m_start: " + str(o2m_start), projectid)
        
        SVM_PROJECT_ONLINE_SCANTIME = select_project_online_scantime_by_projectid_datatype(projectid, datatype)
        
        #20190620 get server name
        #server_name_1 = str(SVM_PROJECT_ONLINE_SCANTIME.SERVER_NAME[0])
        
        
        
        if len(SVM_PROJECT_ONLINE_SCANTIME) == 0:
            print('online_x_phase no scantime') 
            APLog.WriteApLog("online_x_phase no scantime", projectid)                                                                  
        else:
            MAX_TIME = "9998-12-31 23:59:59.999"
            project_id = "SVM_PROJECT" + str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0])
            LAST_SCANTIME = datetime.datetime.strftime(SVM_PROJECT_ONLINE_SCANTIME.LAST_SCANTIME[0], '%Y-%m-%d %H:%M:%S.%f')[:-3] #20190704 get millisecond 3
            SVM_PROJECT_RUN = select_projectx_run_by_itime(project_id, LAST_SCANTIME, MAX_TIME, server_name, db_name)
            
            if len(SVM_PROJECT_RUN) == 0:
                print('SVM_PROJECT_RUN null')
            else:
                for index, project_run_row in SVM_PROJECT_RUN.iterrows():                    
                    project_run_row_runindex = project_run_row['RUNINDEX']
                    project_run_row_itime = datetime.datetime.strftime(project_run_row['ITIME'], '%Y-%m-%d %H:%M:%S.%f')[:-3]
                    APLog.WriteApLog("project_run_row.RUNINDEX=" + str(project_run_row_runindex), projectid)
                    APLog.WriteApLog("project_run_row.ITIME=" + str(project_run_row_itime), projectid)
                    try:
                        Run_Predict_StartTime = datetime.datetime.now()
                        SVM_PROJECT_MODEL = select_project_model_by_projectid_status(projectid, "ONLINE")
                        self.run_o2m(project_run_row, path_dict, projectid, project_id, datatype, SVM_PROJECT_MODEL, server_name, db_name)                        
                        Run_Predict_EndTime = datetime.datetime.now()
                        Run_Predict_Interval = (Run_Predict_EndTime - Run_Predict_StartTime).total_seconds()
                        insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, project_run_row_runindex, 'PREDICT_SECS', Run_Predict_Interval, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)
                        APLog.WriteApLog("project_run_row.Run_Predict_Interval=" + str(Run_Predict_Interval), projectid)                    
                    except Exception as e:
                        errMsg = ProcEx.ProcessException(e) #Reaver                        
                        APLog.WriteApLog("run_o2m RUNINDEX=" + str(project_run_row_runindex) + " errMsg=" + errMsg, projectid)
                        insert_svm_error_run_log(projectid, project_run_row_runindex, errMsg) # log error runindex
                        update_online_scantime_lastscantime_by_projectid_datatype(project_run_row_itime, projectid, 'X') # pass error run                    
                    
                #SVM_PROJECT_RUN.apply(self.run_o2m, axis=1, args=(path_dict, projectid, project_id, datatype, server_name, db_name,))
                                                                                                                                   
        o2m_end = datetime.datetime.now()
        o2m_interval = o2m_end - o2m_start
        mylog.info(str(projectid) + "_o2m_end: " + str(o2m_end))
        mylog.info(str(projectid) + "_o2m_interval: " + str(o2m_interval))
        return None
    
    def run_m2m(self, df, projectid, project_id, datatype, retrain_num, batch_num, FILTER_FEATURE, SVM_PROJECT_MODEL, server_name, db_name):
        mylog = WriteLog(SystemLog_Path + str(projectid) + ServerConfig.LogFileExtension, ErrorLog_Path + str(projectid) + ServerConfig.LogFileExtension)  # Reaver
        #20190930 for different base and online server
        SVM_ONLINE_PARAMETER = select_online_parameter_by_projectid_datatype(projectid, datatype)
        SVM_PARAMETER_DATA1 = select_projectx_parameter_data_by_runindex(project_id, df['RUNINDEX'], server_name, db_name)
        SVM_ONLINE_PARAMETER.rename(columns = {'ONLINE_PARAMETER':'PARAMETER'}, inplace = True) 

        SVM_PARAMETER_MERGE = pd.merge(SVM_ONLINE_PARAMETER, SVM_PARAMETER_DATA1, how='left', on='PARAMETER')
        SVM_ONLINE_PARAMETER_null = SVM_PARAMETER_MERGE[SVM_PARAMETER_MERGE['PARAM_VALUE'].isnull()]
        SVM_ONLINE_PARAMETER_notnull = SVM_PARAMETER_MERGE[SVM_PARAMETER_MERGE['PARAM_VALUE'].notnull()]
        SVM_ONLINE_PARAMETER_null = SVM_ONLINE_PARAMETER_null.reset_index(drop=True) #20191002 reset_index avoid for loop error
        SVM_ONLINE_PARAMETER_notnull = SVM_ONLINE_PARAMETER_notnull.reset_index(drop=True)
        
        #mylog = WriteLog('D:\\SVM\\test.log', 'D:\\SVM\\error.log')  
        #mylog.info('project' + str(projectid) + '_' + str(SVM_PROJECT_RUN.RUNINDEX[c]))
        #mylog.info('project' + str(projectid) + '_' + str(SVM_ONLINE_PARAMETER_null.shape))
        #mylog.info('project' + str(projectid) + '_' + str(SVM_ONLINE_PARAMETER_notnull.shape))
        
        for k in range(len(SVM_ONLINE_PARAMETER_null)):
            
            #mylog.info('project' + str(projectid) + '_' + str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]))
                                  
            if str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'TOOLID':
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['TOOLID']                           
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'LOTNAME':   
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['LOTNAME']                        
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'FOUPNAME':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['FOUPNAME']
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'OPID':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['OPID']
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'RECIPE':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['RECIPE']
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'ABBRID':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['ABBRID']
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'PRODUCT':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['PRODUCT']
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'MODELNO':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['MODELNO']
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'CHAMBER':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['CHAMBER']
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'SHEETID':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['SHEETID']
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'SLOTNO':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['SLOTNO']
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'POINT':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['POINT']
            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'FILTER_FEATURE':                           
                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = df['FILTER_FEATURE']
        
        SVM_PARAMETER_DATA = pd.concat([SVM_ONLINE_PARAMETER_notnull, SVM_ONLINE_PARAMETER_null], axis=0, ignore_index=True)    
        SVM_PARAMETER_DATA = SVM_PARAMETER_DATA.sort_values("SEQ").reset_index(drop=True)
        SVM_PARAMETER_DATA = SVM_PARAMETER_DATA[["OFFLINE_PARAMETER", "PARAM_VALUE"]]          
            
        if len(SVM_PARAMETER_DATA) == 0:
            print('SVM_PARAMETER_DATA null')  #20190619 add inert sql in the future
        else:                                                              
            try:
                db_p = df[FILTER_FEATURE]
            except:
                db_p = df['FILTER_FEATURE']
            
            base_name = projectid + '_' + FILTER_FEATURE + '_' + db_p
            path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]   
            
            #20190902 for presure test alone start-end time
            #input_path = read_path(path_dict['3']["3"])
            mylog = WriteLog(SystemLog_Path + str(projectid) + ServerConfig.LogFileExtension, ErrorLog_Path + str(projectid) + ServerConfig.LogFileExtension)  # Reaver
            #mylog = WriteLog(input_path["log_path"], input_path["error_path"])  
            APLog.WriteApLog("run_m2m Start", projectid)
            df_SVM_PARAMETER_DATA_lite_T = SVM_PARAMETER_DATA.T #20190619 行列互轉
            headers = df_SVM_PARAMETER_DATA_lite_T.iloc[0] #20190619 first row set header
            df_SVM_PARAMETER_DATA_lite_T_H  = pd.DataFrame(df_SVM_PARAMETER_DATA_lite_T.values[1:], columns=headers)
            
            #SVM_PARAMETER_DATA.iloc[0:0]
                
            print("online x phase[ns]")
            APLog.WriteApLog("online x phase[ns]", projectid)
            OnLine_Data_PreProcess_start = datetime.datetime.now()  
            mylog.info(str(projectid) + "_OnLine_Data_PreProcess_start: " + str(OnLine_Data_PreProcess_start))
                                    
            #20191009 avoid runindex duplicate error 
            try:
                df_online_X, Missing_Ratio = OnLine_Data_PreProcess(df_SVM_PARAMETER_DATA_lite_T_H, path_dict['3']["3"], check_include_y=False)
            except Exception as e:                
                mylog.error(e)
                mylog.error(df['RUNINDEX'])
                update_online_scantime_lastscantime_by_projectid_datatype(df['ITIME'], projectid, 'X')
                return #20191018 error return nothing
            
            OnLine_XDI_start = datetime.datetime.now()
            OnLine_Data_PreProcess_interval = OnLine_XDI_start - OnLine_Data_PreProcess_start
            mylog.info(str(projectid) + "_OnLine_XDI_start: " + str(OnLine_XDI_start))
            mylog.info(str(projectid) + "_OnLine_Data_PreProcess_interval: " + str(OnLine_Data_PreProcess_interval))
            
            XDI_online_value, XDI_Threshold, XDI_Check, df_contri = OnLine_XDI(df_online_X, path_dict['4']["3"])
            
            model_predict_online_x_start = datetime.datetime.now()
            OnLine_XDI_interval = model_predict_online_x_start - OnLine_XDI_start
            mylog.info(str(projectid) + "_model_predict_online_x_start: " + str(model_predict_online_x_start))
            mylog.info(str(projectid) + "_OnLine_XDI_interval: " + str(OnLine_XDI_interval))
            #20191021 avoid retrain cause predict stop 
            try:
                y_pred, y_base = self.model_predict_online_x(df_online_X, path_dict, str(projectid))
            except Exception as e:
                errMsg = ProcEx.ProcessException(e) #Reaver
                print(errMsg)
                mylog.info(str(projectid) + ' - ' + errMsg)
                APLog.WriteApLog(errMsg, projectid)
                APLog.WriteApLog("Error RUNINDEX=" + str(df['RUNINDEX']) , projectid)
                #mylog.error(df['RUNINDEX'])
                return            
            """
            print("-----------------")
            print('y_pred data type: ' + str(type(y_pred)))
            print('y_pred: ' + str(y_pred))
            print('y_base data type: ' + str(type(y_base)))
            print('y_base: ' + str(y_base))
            print("-----------------")
            """
            CI_online_start = datetime.datetime.now()  
            mylog.info(str(projectid) + "_CI_online_start: " + str(CI_online_start))
            APLog.WriteApLog("CI_online_start", projectid)
            #20190909 int must equal int, execute CI_online/CI_online_old_multi
            #if int(projectid) > 570:
            try:
                MXCI_value, MXCI_T, MYCI_value, MYCI_T, light = CI_online_multi(path_dict['8']["3"], df_online_X, y_pred, y_base, str(db_p))
            except Exception as e:
                errMsg = ProcEx.ProcessException(e) #Reaver
                APLog.WriteApLog(str(df['RUNINDEX']) + " -Error- " + errMsg , projectid)
            #else:
            #    MXCI_value, MXCI_T, MYCI_value, MYCI_T, light = CI_online_old_multi(path_dict['8']["3"], df_online_X, y_pred, y_base, db_p)
                                                            
            CI_online_end = datetime.datetime.now()
            CI_online_interval = CI_online_end - CI_online_start
            mylog.info(str(projectid) + "_CI_online_end: " + str(CI_online_end))
            mylog.info(str(projectid) + "_CI_online_interval: " + str(CI_online_interval))
            APLog.WriteApLog("CI_online_end", projectid)
            #try:
            #    SVM_PROJECT_MODEL = select_project_model_by_projectid_status_filterfeature(projectid, "ONLINE", df[FILTER_FEATURE])
            #except:
            #    SVM_PROJECT_MODEL = select_project_model_by_projectid_status_filterfeature(projectid, "ONLINE", df['FILTER_FEATURE'])
            
            if XDI_online_value is not None:
                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, df['RUNINDEX'], 'XDI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, df['RUNINDEX'], 'XDI', XDI_online_value, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                
                else:  
                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, XDI_online_value, 0, df['RUNINDEX'], 'XDI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                                               
            if XDI_Threshold is not None:
                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, df['RUNINDEX'], 'XDI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, df['RUNINDEX'], 'XDI_Threshold', XDI_Threshold, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)  
                else: 
                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, XDI_Threshold, 0, df['RUNINDEX'], 'XDI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)                                          
            
            if XDI_Check is not None:
                if int(XDI_Check) == 0:
                    SVM_ONLINE_STATUS = select_online_status_by_projectid_runindex(projectid, df['RUNINDEX'])
                    if len(SVM_ONLINE_STATUS) == 0:
                        insert_online_status_projectid_runindex_xdialarm_itime(projectid, df['RUNINDEX'], 1, datetime.datetime.now()) #20190710 xdi_alarm 0->1                                
                    else: 
                        update_online_status_xdialarm_by_projectid_runindex(1, projectid, df['RUNINDEX'])  
                    update_projectx_run_xdialarm_by_runindex(project_id, 1, df['RUNINDEX'], server_name, db_name)                                                                                   
            
            if y_pred is not None:
                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, df['RUNINDEX'], 'Predict_Y', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                if len(SVM_PROJECT_PREDICT_DATA) == 0: 
                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, df['RUNINDEX'], 'Predict_Y', y_pred, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                  
                else: 
                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, y_pred, 0, df['RUNINDEX'], 'Predict_Y', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
            
            #20190808 insert y_base to prdeict_data
            if y_base is not None:
                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, df['RUNINDEX'], 'Baseline_Y', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                if len(SVM_PROJECT_PREDICT_DATA) == 0: 
                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, df['RUNINDEX'], 'Baseline_Y', y_base, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                  
                else: 
                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, y_base, 0, df['RUNINDEX'], 'Baseline_Y', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
            
            if MXCI_value is not None:
                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, df['RUNINDEX'], 'MXCI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                if len(SVM_PROJECT_PREDICT_DATA) == 0:   
                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, df['RUNINDEX'], 'MXCI', MXCI_value, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)  
                else: 
                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MXCI_value, 0, df['RUNINDEX'], 'MXCI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                
            if MXCI_T is not None:
                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, df['RUNINDEX'], 'MXCI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, df['RUNINDEX'], 'MXCI_Threshold', MXCI_T, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                    
                else: 
                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MXCI_T, 0, df['RUNINDEX'], 'MXCI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                
            if MYCI_value is not None:
                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, df['RUNINDEX'], 'MYCI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                #MYCI_value = float(str(MYCI_value)[0:10]) #20190911 not additional to change digits
                MYCI_value = round(MYCI_value,50)
                if len(SVM_PROJECT_PREDICT_DATA) == 0:   
                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, df['RUNINDEX'], 'MYCI', MYCI_value, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                   
                else: 
                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MYCI_value, 0, df['RUNINDEX'], 'MYCI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                
            if MYCI_T is not None:
                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, df['RUNINDEX'], 'MYCI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, df['RUNINDEX'], 'MYCI_Threshold', MYCI_T, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                    
                else: 
                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MYCI_T, 0, df['RUNINDEX'], 'MYCI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)                            
                                                              
            if light is not None:
                update_projectx_run_signal_by_runindex(project_id, light, df['RUNINDEX'], server_name, db_name)
            
            if df_contri is not None:
                for j in range(df_contri.shape[0]):
                    SVM_PROJECT_CONTRIBUTION_DATA = select_projectx_contribution_data_by_runindex_parameter_model(project_id, df['RUNINDEX'], df_contri.Col_Name[j], 'XDI', server_name, db_name)
                    if len(SVM_PROJECT_CONTRIBUTION_DATA) == 0:  
                        insert_projectx_contribution_data_runindex_model_parameter_contribution(project_id, df['RUNINDEX'], 'XDI', df_contri.Col_Name[j], df_contri.Contribution[j], server_name, db_name)
                    else: 
                        update_projectx_contribution_data_contribution_by_runindex_parameter_model(project_id, df_contri.Contribution[j], df['RUNINDEX'], df_contri.Col_Name[j], 'XDI', server_name, db_name)
            
            if Missing_Ratio is not None:
                for Missing in Missing_Ratio:
                    SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, df['RUNINDEX'], 'Missing_Ratio', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, df['RUNINDEX'], 'Missing_Ratio', Missing, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                    
                else: 
                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, Missing, 0, df['RUNINDEX'], 'Missing_Ratio', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)   

            update_online_scantime_lastscantime_by_projectid_datatype(df['ITIME'], projectid, 'X')
        
            DB_end = datetime.datetime.now()
            DB_interval = DB_end - CI_online_end
            mylog.info(str(projectid) + "_DB_end: " + str(DB_end))
            mylog.info(str(projectid) + "_DB_interval: " + str(DB_interval))   
            APLog.WriteApLog("DB_end", projectid)
        
    def online_x_many_phase(self, projectid, model_id, datatype, server_name, db_name, retrain_num, batch_num=None):
        retrain_num = self.get_max_retrain_num(retrain_num)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)
        mylog = WriteLog(SystemLog_Path + str(projectid) + ServerConfig.LogFileExtension, ErrorLog_Path + str(projectid) + ServerConfig.LogFileExtension)  # Reaver
        #db_name = "APC"
        #user = "YL"
        #password = "YL$212"                  
        
        SVM_PROJECT_ONLINE_SCANTIME = select_project_online_scantime_by_projectid_datatype(projectid, datatype)
        #20190620 get server name
        #server_name_1 = str(SVM_PROJECT_ONLINE_SCANTIME.SERVER_NAME[0])
        #cnxn2 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server_name + ';DATABASE=' + db_name + ';UID=' + user + ';PWD=' + password)
        
        
        
        print("-------------")
        print(projectid)
        print("-------------")
        
        if len(SVM_PROJECT_ONLINE_SCANTIME) == 0:
            print('online_x_many_phase no scantime')
            APLog.WriteApLog("online_x_many_phase no scantime", projectid)                                                                                                                  
        else:
            projectid = str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0])        
            project_id = "SVM_PROJECT" + str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0])
            SVM_PROJECT_CONFIG = select_project_config_by_modelid_parameter3RD(model_id, "Filter_Feature")
            MAX_TIME = "9998-12-31 23:59:59.999"                        
            LAST_SCANTIME = datetime.datetime.strftime(SVM_PROJECT_ONLINE_SCANTIME.LAST_SCANTIME[0], '%Y-%m-%d %H:%M:%S.%f')[:-3] #20190704 get millisecond 3
            
            FILTER_FEATURE = SVM_PROJECT_CONFIG.PARAM_VALUE[0]
            SVM_PROJECT_RUN = select_projectx_run_by_itime(project_id, LAST_SCANTIME, MAX_TIME, server_name, db_name)
            print(SVM_PROJECT_RUN.head(1))
            if len(SVM_PROJECT_RUN) != 0:                                
                m2m_start = datetime.datetime.now()  
                mylog.info(str(projectid) + "_m2m_start: " + str(m2m_start))
                for index, project_run_row in SVM_PROJECT_RUN.iterrows():                    
                    project_run_row_runindex = project_run_row['RUNINDEX']
                    project_run_row_itime = datetime.datetime.strftime(project_run_row['ITIME'], '%Y-%m-%d %H:%M:%S.%f')[:-3]
                    APLog.WriteApLog("project_run_row.RUNINDEX=" + str(project_run_row_runindex), projectid)
                    APLog.WriteApLog("project_run_row.ITIME=" + str(project_run_row_itime), projectid)
                    try:
                        Run_Predict_StartTime = datetime.datetime.now()
                        try:
                            SVM_PROJECT_MODEL = select_project_model_by_projectid_status_filterfeature(projectid, "ONLINE", project_run_row[FILTER_FEATURE])
                        except:
                            SVM_PROJECT_MODEL = select_project_model_by_projectid_status_filterfeature(projectid, "ONLINE", project_run_row['FILTER_FEATURE'])
                        
                        self.run_m2m(project_run_row, projectid, project_id, datatype, retrain_num, batch_num, FILTER_FEATURE, SVM_PROJECT_MODEL, server_name, db_name)
                        Run_Predict_EndTime = datetime.datetime.now()
                        Run_Predict_Interval = (Run_Predict_EndTime - Run_Predict_StartTime).total_seconds()
                        insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, project_run_row_runindex, 'PREDICT_SECS', Run_Predict_Interval, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)
                        APLog.WriteApLog("project_run_row.Run_Predict_Interval=" + str(Run_Predict_Interval), projectid)
                    except Exception as e:
                        errMsg = ProcEx.ProcessException(e) #Reaver                        
                        APLog.WriteApLog("run_m2m RUNINDEX=" + str(project_run_row_runindex) + " errMsg=" + errMsg, projectid)
                        insert_svm_error_run_log(projectid, project_run_row_runindex, errMsg) # log error runindex
                        update_online_scantime_lastscantime_by_projectid_datatype(project_run_row_itime, projectid, 'X') # pass error run                    
                        
                #SVM_PROJECT_RUN.apply(self.run_m2m, axis=1, args=(projectid, project_id, datatype, retrain_num, batch_num, FILTER_FEATURE, server_name, db_name,))                  
                #for c in range(len(SVM_PROJECT_RUN)):
        
        m2m_end = datetime.datetime.now()
        m2m_interval = m2m_end - m2m_start
        mylog.info(str(projectid) + "_m2m_end: " + str(m2m_end))
        mylog.info(str(projectid) + "_m2m_interval: " + str(m2m_interval))
        return None
    
    
    def one_to_all_online_x(self, projectid, datatype, retrain_num, server_name, db_name):           
        #mylog = WriteLog(SystemLog_Path + str(projectid) + ServerConfig.LogFileExtension, ErrorLog_Path + str(projectid) + ServerConfig.LogFileExtension)  # Reaver
        retrain_num = self.get_max_retrain_num(retrain_num=retrain_num)
        self.get_saved_path_config()
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=None)
        print("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num))
        #mylog("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num)) # Reaver
        APLog.WriteApLog("one_to_all_online_x : " + "We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num), projectid)  # Reaver
        #20191106 avoid move data error cause predict status = 2, let project not predict
        try:
            self.online_x_phase(projectid, datatype, server_name, db_name, base_name=self.base_name, retrain_num=retrain_num, batch_num=batch_num)       
        except Exception as e:
            errMsg = ProcEx.ProcessException(e) #Reaver
            print(errMsg)
            APLog.WriteApLog("one_to_all_online_x : " + errMsg, projectid)  # Reaver
            
            update_project_predictstatus_by_projectid('1', projectid)            
            return None 
        else:
            update_project_predictstatus_by_projectid('1', projectid)
        
        return None  
    
    
    def many_to_many_online_x(self, projectid, model_id, datatype, retrain_num, server_name, db_name):
        #self.batch_data = batch_data
        mylog = WriteLog(SystemLog_Path + str(projectid) + ServerConfig.LogFileExtension, ErrorLog_Path + str(projectid) + ServerConfig.LogFileExtension)  # Reaver
        retrain_num = self.get_max_retrain_num(retrain_num=retrain_num)
        mylog.info("many_to_many_online_x=" + str(projectid))
        if self.filter_feature is None:
            self.get_filter_feature()

        if self.feature_lists is None:
            self.get_feature_content()

        if self.filter_dir_name is None:
            self.filter_dir_name = {}
            for feature in self.feature_lists:
                self.filter_dir_name[feature] = self.base_name + "_" + self.filter_feature + "_" + str(feature)
                
        self.get_saved_path_config()
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=None)
        print("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num))
        mylog.info("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num)) # Reaver
        #20191106 avoid move data error cause predict status = 2, let project not predict
        try:
            self.online_x_many_phase(projectid, model_id, datatype, server_name, db_name, retrain_num=retrain_num, batch_num=batch_num)
        except Exception as e:
            errMsg = ProcEx.ProcessException(e) #Reaver
            print(errMsg)
            mylog.info(str(projectid) + ' - ' + errMsg) 
            update_project_predictstatus_by_projectid('1', projectid)
            return None
        else:
            update_project_predictstatus_by_projectid('1', projectid)

        return None
  

if __name__ == '__main__':    
    def timer(n):
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        
        while True:
            
            #url = 'http://10.96.18.199/APC_WBS/APCService.asmx/Get_SmartPrediction_PredictProjectID' # Reaver
            url = ServerConfig.Wbs_Url + '/' + ServerConfig.Wbs_GetPredictProjectID # Reaver
            response = requests.get(url)
            data = response.json()
            print(url)
            print(response.text)
            if data['data']['Data'] is not None:
                
                projectid = int(data['data']['Data'][0]['PROJECT_ID'])
                #projectid = 3351
                print("--------------")
                print(projectid)
                print("--------------")
                #20190902 for presure test all start-end time
                All_start = datetime.datetime.now()
                
                LogDatePart = All_start.strftime("%Y%m%d")                      # Reaver
                #mylog = WriteLog('D:\\SVM\\System.log', 'D:\\SVM\\error.log')  # Reaver
                SystemLog_Path = ServerConfig.Log_Path + "\\" + LogDatePart + ServerConfig.Log_Keep_Day + "\\" + str(projectid) + ServerConfig.LogFileExtension # Reaver
                ErrorLog_Path = ServerConfig.Log_Path + "\\" + LogDatePart + ServerConfig.Log_Keep_Day + "\\" + str(projectid) + ServerConfig.LogFileExtension # Reaver
                mylog = WriteLog(SystemLog_Path, ErrorLog_Path)  # Reaver
                
                SmartPrediction_ConfigDB_IP = ServerConfig.SmartPrediction_DBServer_IP # Reaver
                SmartPrediction_ConfigDB = ServerConfig.SmartPrediction_Config_DB # Reaver
                mylog.info("SmartPrediction_ConfigDB_IP=" + SmartPrediction_ConfigDB_IP + " , SmartPrediction_ConfigDB=" + SmartPrediction_ConfigDB) # Reaver
                mylog.info("All_start: " + str(All_start))
                #SVM_PROJECT_MODEL = select_project_with_model_by_projectid_onlineserver_predictstatus_modelstatus(projectid, ip, '2', 'ONLINE') # Reaver
                SVM_PROJECT_MODEL = select_project_with_model_by_projectid_onlineserver_predictstatus_modelstatus(projectid, ip, '2', 'ONLINE', SmartPrediction_ConfigDB_IP, SmartPrediction_ConfigDB) # Reaver
                
                if len(SVM_PROJECT_MODEL) != 0:  
                    model_id = SVM_PROJECT_MODEL.MODEL_ID[0]
                    retrain_num = int(SVM_PROJECT_MODEL.MODEL_SEQ[0])
                    datatype = 'X'
                    server_name = SVM_PROJECT_MODEL.SERVER_NAME[0]
                    db_name = SVM_PROJECT_MODEL.DB_NAME[0]
                                
                    #mylog.info("server_name=" + server_name + " , db_name=" + db_name) # Reaver
                    APLog.WriteApLog("server_name=" + server_name + " , db_name=" + db_name,projectid )
                    if SVM_PROJECT_MODEL.MODEL_TYPE[0] in ('1','2'): 
                        #20191106 avoid move data error cause predict status = 2, let project not predict
                        try:
                            SVM = SuperVM(str(SVM_PROJECT_MODEL.PROJECT_NAME[0]), str(SVM_PROJECT_MODEL.UPLOAD_FILE[0]), r"Config.json", projectid)  
                        except Exception as e:
                            errMsg = ProcEx.ProcessException(e) #Reaver
                            print(errMsg)
                            APLog.WriteApLog("Main : " + errMsg, projectid) 
                            #mylog.info(str(projectid) + ' ' + str(e))
                            return None
                        
                        print("--------------")
                        print(projectid)
                        print(datatype)
                        print(retrain_num)
                        print(server_name)
                        print(db_name)
                        print("--------------")
                                                
                        SVM.one_to_all_online_x(projectid, datatype, retrain_num, server_name, db_name)   
                
                    else:
                        #20191106 avoid move data error cause predict status = 2, let project not predict
                        try:                                       
                            SVM = SuperVM(str(SVM_PROJECT_MODEL.PROJECT_NAME[0]), str(SVM_PROJECT_MODEL.UPLOAD_FILE[0]), r"Config.json", projectid)
                        except Exception as e:
                            errMsg = ProcEx.ProcessException(e) #Reaver
                            print(errMsg)
                            mylog.info(str(projectid) + ' - ' + errMsg) 
                            #mylog.info(str(projectid) + ' ' + str(e))    
                            update_project_predictstatus_by_projectid('1', projectid)
                            return None
                        
                        print("--------------")
                        print(projectid)
                        print(model_id)
                        print(datatype)
                        print(retrain_num)
                        print(server_name)
                        print(db_name)
                        print("--------------")
        
                        SVM.many_to_many_online_x(projectid, model_id, datatype, retrain_num, server_name, db_name) 
                
                #update_project_predictstatus_by_projectid('1', projectid)
                #20190902 for presure test all start-end time
                All_end = datetime.datetime.now()
                All_interval = All_end - All_start
                mylog.info("All_end: " + str(All_end))
                mylog.info("All_interval: " + str(All_interval))
    
                time.sleep(n)
            else:
                print('No project needs to be predicted')
                time.sleep(n*12)
    timer(5)



# SmartVM_Constructor_online_y_combine_mp.py
#20190823 1114 remove update svm_online_status

from Data_PreProcess import Data_PreProcess_Train, Data_PreProcess_Test, OnLine_Data_PreProcess
from XDI import XDI_off_line_report, Build_XDI_Model
from YDI import YDI_off_line_report, Build_YDI_Model, OnLine_YDI
from MXCI_MYCI_pre import pre_MXCI_MYCI
from MXCI_MYCI import MXCI_MYCI_offline
from DB_operation import select_project_model_by_projectid_status, select_project_model_by_projectid_status_filterfeature, select_online_parameter_by_projectid, select_online_parameter_by_projectid_datatype, select_project_online_scantime_model_y_by_onlineserver_status, select_project_online_scantime_y_many_by_onlineserver, select_project_online_scantime_by_projectid_datatype, select_project_config_by_modelid_parameter3RD, select_projectx_run_by_runindex, select_projectx_parameter_data_by_runindex, select_projectx_parameter_data_by_parameter_itime, select_projectx_predict_data_by_runindex_parameter_modelid, update_online_scantime_lastscantime_by_projectid_datatype, update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid, update_projectx_run_ydialarm_by_runindex, insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict

import os
import json
import traceback
# from Data_Check import Data_Check
from shutil import copyfile
import pandas as pd
import datetime
import time
import socket
import multiprocessing as mp
#import pyodbc
from Read_path import read_path
from CreateLog import WriteLog
from AI365_v3_realy import AI_365

import SystemConfig.ServerConfig as ServerConfig # Reaver
import ProcException as ProcEx                   # Reaver
import APLog as APLog                            # Reaver


class SuperVM():
    def __init__(self, sorce_dir, train_data_name, config_file_name, batch_data_name_list=None):
        self.base_path = os.path.join(ServerConfig.ModelFilePath, sorce_dir)
        self.base_name = os.path.basename(os.path.abspath(self.base_path))
        self.train_data_name = train_data_name
        self.train_base, self.ext = os.path.splitext(self.train_data_name)
        self.train_data = os.path.join(self.base_path, train_data_name)
        self.config_file = os.path.join(self.base_path, config_file_name)
    
        if batch_data_name_list:
            self.batch_data = [os.path.join(self.base_path, x) for x in batch_data_name_list]
        self.in_path = os.path.join(self.base_path, "path_config.json")
        self.mdoel_dict = {}
        self.mdoel_dict["3"] = {}
        self.mdoel_dict["3"]["Train"] = Data_PreProcess_Train
        self.mdoel_dict["3"]["Test"] = Data_PreProcess_Test
        self.mdoel_dict["4"] = {}
        self.mdoel_dict["4"]["Train"] = Build_XDI_Model
        self.mdoel_dict["4"]["Test"] = XDI_off_line_report
        self.mdoel_dict["5"] = {}
        self.mdoel_dict["5"]["Train"] = Build_YDI_Model
        self.mdoel_dict["5"]["Test"] = YDI_off_line_report
        self.mdoel_dict["8"] = {}
        self.mdoel_dict["8"]["Train"] = pre_MXCI_MYCI
        self.mdoel_dict["8"]["Test"] = MXCI_MYCI_offline

        self.retrain_data = os.path.join(self.base_path, "retrain_data.csv")
        
        if not os.path.exists(self.retrain_data):
            copyfile(self.train_data, self.retrain_data)
        
        # Other Variables initialized in methods
        self.filter_feature = None
        self.feature_lists = None
        self.current_retrain_number = None
        self.current_batch_number = None
        self.path_config = None
        self.filter_feature_dict = None
        # self.filter_dir_path = None
        self.filter_dir_name = None
    
    
    def get_filter_feature(self):
        print("self.config_file=" + self.config_file)
        try:
            with open(self.config_file) as json_data:
                config = json.load(json_data)
        except Exception as e:
            config = None
            error_path = os.path.join(self.base_path, "error.log")
            with open(error_path, 'a') as file:
                file.write("Error while reading config file\n")
                file.write(traceback.format_exc(e))
                raise FileNotFoundError
        self.filter_feature = config["Filter_Feature"]
        self.model_pred_name = config["Model_Pred_Name"]
        return None

    def get_feature_content(self, split_flag=None):
        try:
            training_data = pd.read_csv(self.train_data)
        except Exception as e:
            training_data = None
            error_path = os.path.join(self.base_path, "error.log")
            with open(error_path, 'a') as file:
                file.write("Error while reading raw data\n")
                file.write(str(e) + "\n")
                file.write(traceback.format_exc())
                raise FileNotFoundError
        self.feature_lists = training_data[self.filter_feature].unique().tolist()
        self.feature_lists.sort()
        if split_flag is not None:
            self.filter_feature_dict = {}
            for feature_list in self.feature_lists:
                tmp_data = training_data[training_data[self.filter_feature] == feature_list].copy().reset_index(drop=True)
                self.filter_feature_dict[feature_list] = \
                    os.path.join(self.base_path, self.train_base + "_" + self.filter_feature + "_" + str(feature_list) + self.ext)
                tmp_data.to_csv(self.filter_feature_dict[feature_list], index=False)
        return None

########################################################################################################################
    def get_saved_path_config(self):        
        try:
            with open(self.in_path) as json_data:
                tPath = json_data.read()
                tPath = tPath.replace(r"../Cases/", r"D:\\SVM\\Cases\\")
                tPath = tPath.replace(r"/", r"\\")
                #print(tPath)
                self.path_config = json.loads(tPath)
        except Exception as e:
            errMsg = ProcEx.ProcessException(e) # Reaver
            APLog.WriteApLog( errMsg , 0)       # Reaver
            error_path = os.path.join(self.base_path, "error.log")
            with open(error_path, 'a') as file:
                file.write("Error while reading path_config.json")
                file.write(traceback.format_exc(e))
            raise FileNotFoundError
        return None

########################################################################################################################
    def get_max_retrain_num(self, retrain_num=None):
        #print(self.path_config)
        if retrain_num is None:
            try:
                first_key = list(self.path_config.keys())[0]
                key_list = list(self.path_config[first_key].keys())
                key_list.remove("main")
                return max([int(x) for x in key_list])
            except:
                try:
                    self.get_saved_path_config()
                    first_key = list(self.path_config.keys())[0]
                    key_list = list(self.path_config[first_key].keys())
                    key_list.remove("main")
                    return max([int(x) for x in key_list])
                except Exception as e:
                    raise FileNotFoundError("path_config not found")
        else:
            return retrain_num

    def get_max_batch_num(self, retrain_num, batch_num=None):
        if batch_num is None:
            first_key = list(self.path_config.keys())[0]
            key_list = list(self.path_config[first_key][str(retrain_num)].keys())
            key_list.remove("main")
            return max([int(x) for x in key_list])
        else:
            return batch_num    
    
    #20190710 Predict YDI never enter abnormal mode
    def online_y_phase(self, projectid, datatype, server_name, db_name, base_name, retrain_num, batch_num=None):
        
        retrain_num = self.get_max_retrain_num(retrain_num)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)

        path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]                        
        SVM_PROJECT_ONLINE_SCANTIME = select_project_online_scantime_by_projectid_datatype(projectid, datatype)
        #20190620 get server name
        #server_name_1 = str(SVM_PROJECT_ONLINE_SCANTIME.SERVER_NAME[0])
             
        project_id = "SVM_PROJECT" + str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0])
        
        SVM_PROJECT_MODEL = select_project_model_by_projectid_status(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], "ONLINE")
        
        #20190809 go online parameter[datatype:Y] to get realY
        SVM_PROJECT_CONFIG = select_online_parameter_by_projectid_datatype(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], "Y")
        
        if len(SVM_PROJECT_CONFIG) == 0:
            print('config no data')                                                                                                                                                       
        else:                      
            LAST_SCANTIME = datetime.datetime.strftime(SVM_PROJECT_ONLINE_SCANTIME.LAST_SCANTIME[0], '%Y-%m-%d %H:%M:%S.%f')[:-3]
            SVM_PARAMETER_DATA_y = select_projectx_parameter_data_by_parameter_itime(project_id, SVM_PROJECT_CONFIG.ONLINE_PARAMETER[0], LAST_SCANTIME, server_name, db_name)
            APLog.WriteApLog("LAST_SCANTIME : " + LAST_SCANTIME, projectid)
            if len(SVM_PARAMETER_DATA_y) > 0:
                #print('SVM_PARAMETER_DATA_y null') 
            #else:                                                   
                SVM_PROJECT_RUN = select_projectx_run_by_runindex(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], server_name, db_name)
                
                APLog.WriteApLog("RUNINDEX : " + str(SVM_PARAMETER_DATA_y.RUNINDEX[0]), projectid)
                
                #20190930 for different base and online server
                SVM_ONLINE_PARAMETER = select_online_parameter_by_projectid(projectid)
                SVM_PARAMETER_DATA1 = select_projectx_parameter_data_by_runindex(project_id, SVM_PROJECT_RUN.RUNINDEX[0], server_name, db_name)
                SVM_ONLINE_PARAMETER.rename(columns = {'ONLINE_PARAMETER':'PARAMETER'}, inplace = True) 
    
                SVM_PARAMETER_MERGE = pd.merge(SVM_ONLINE_PARAMETER, SVM_PARAMETER_DATA1, how='left', on='PARAMETER')
                SVM_ONLINE_PARAMETER_null = SVM_PARAMETER_MERGE[SVM_PARAMETER_MERGE['PARAM_VALUE'].isnull()]
                SVM_ONLINE_PARAMETER_notnull = SVM_PARAMETER_MERGE[SVM_PARAMETER_MERGE['PARAM_VALUE'].notnull()]
                SVM_ONLINE_PARAMETER_null = SVM_ONLINE_PARAMETER_null.reset_index(drop=True) #20191002 reset_index avoid for loop error
                SVM_ONLINE_PARAMETER_notnull = SVM_ONLINE_PARAMETER_notnull.reset_index(drop=True)
                
                #SVM_ONLINE_PARAMETER_null.to_csv(r'D:\SVM\Cases\1274\111.csv', index=False)
                #SVM_ONLINE_PARAMETER_notnull.to_csv(r'D:\SVM\Cases\1274\222.csv', index=False)
                
                #input_path = read_path(path_dict['3']["3"])
                #mylog = WriteLog(input_path["log_path"], input_path["error_path"]) 
                #mylog.info(str(SVM_PARAMETER_DATA_y.RUNINDEX[0]))
                #mylog.info(str(LAST_SCANTIME))
                #mylog.info(str(SVM_ONLINE_PARAMETER_null.shape))
                #mylog.info(str(SVM_ONLINE_PARAMETER_notnull.shape))
                
                
                for k in range(len(SVM_ONLINE_PARAMETER_null)):
                                          
                    if str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'TOOLID':
                        SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.TOOLID[0]                           
                    elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'LOTNAME':   
                        SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.LOTNAME[0]                        
                    elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'FOUPNAME':                           
                        SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.FOUPNAME[0]
                    elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'OPID':                           
                        SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.OPID[0]
                    elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'RECIPE':                           
                        SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.RECIPE[0]
                    elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'ABBRID':                           
                        SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.ABBRID[0]
                    elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'PRODUCT':                           
                        SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.PRODUCT[0]
                    elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'MODELNO':                           
                        SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.MODELNO[0]
                    elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'CHAMBER':                           
                        SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.CHAMBER[0]
                    elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'SHEETID':                           
                        SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.SHEETID[0]
                    elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'SLOTNO':                           
                        SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.SLOTNO[0]
                    elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'POINT':                           
                        SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.POINT[0]
                    elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'FILTER_FEATURE':                           
                        SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.FILTER_FEATURE[0]
                
                SVM_PARAMETER_DATA = pd.concat([SVM_ONLINE_PARAMETER_notnull, SVM_ONLINE_PARAMETER_null], axis=0, ignore_index=True)    
                SVM_PARAMETER_DATA = SVM_PARAMETER_DATA.sort_values("SEQ").reset_index(drop=True)
                SVM_PARAMETER_DATA = SVM_PARAMETER_DATA[["OFFLINE_PARAMETER", "PARAM_VALUE"]] 
                
                #mylog.info(str(SVM_PARAMETER_DATA.shape))
                #SVM_PARAMETER_DATA.to_csv(r'D:\SVM\Cases\1274\333.csv', index=False)
                
                if len(SVM_PARAMETER_DATA) == 0:
                    print('SVM_PARAMETER_DATA null')  #20190619 add inert sql in the future
                else:
                    #df_SVM_PARAMETER_DATA_lite=SVM_PARAMETER_DATA.drop(['RUNINDEX','ITIME'], axis=1)
                    df_SVM_PARAMETER_DATA_lite_T = SVM_PARAMETER_DATA.T #20190619 行列互轉
                    headers = df_SVM_PARAMETER_DATA_lite_T.iloc[0] #20190619 first row set header
                    df_SVM_PARAMETER_DATA_lite_T_H  = pd.DataFrame(df_SVM_PARAMETER_DATA_lite_T.values[1:], columns=headers)
                    
                    print("online y phase[ns]")                    
                    #20191009 avoid runindex duplicate error 
                    try:
                        df_online_X, df_online_y = OnLine_Data_PreProcess(df_SVM_PARAMETER_DATA_lite_T_H, path_dict['3']["3"], check_include_y=True)
                    except Exception as e:
                        errMsg = ProcEx.ProcessException(e) # Reaver
                        APLog.WriteApLog( str(SVM_PROJECT_RUN.RUNINDEX[0]) + "," + errMsg , projectid) # Reaver
                        #mylog.error(e)
                        #mylog.error(SVM_PROJECT_RUN.RUNINDEX[0])
                        update_online_scantime_lastscantime_by_projectid_datatype(SVM_PARAMETER_DATA_y.ITIME[0], SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], 'Y')
                        return None
                        
                    df_YDI_online = OnLine_YDI(df_online_X, df_online_y, path_dict['5']["3"])
                    
                    print("----------------")
                    print(project_id)
                    print(SVM_PARAMETER_DATA_y.RUNINDEX[0])
                    print(SVM_PROJECT_MODEL.MODEL_ID[0])
                    print("----------------")
                    # for AI Health Report     
                    AI_365(SVM_PROJECT_ONLINE_SCANTIME.FAB[0], projectid, SVM_PROJECT_ONLINE_SCANTIME.AI365_PROJECT_NAME[0], SVM_PROJECT_ONLINE_SCANTIME.PROJECT_TITLE[0], SVM_PROJECT_MODEL.MODEL_ID[0], SVM_PARAMETER_DATA_y.RUNINDEX[0], SVM_PARAMETER_DATA_y.PARAM_VALUE[0], server_name, db_name)
                                       
                    if df_YDI_online is not None:
                        
                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Interval', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                        if len(SVM_PROJECT_PREDICT_DATA) == 0:  
                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Interval', df_YDI_online.Interval[0], SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                              
                        else: 
                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Interval[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Interval', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                        
                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI', df_YDI_online.YDI[0], SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                           
                        else: 
                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.YDI[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                        
                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                        if len(SVM_PROJECT_PREDICT_DATA) == 0:   
                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI_Threshold', df_YDI_online.YDI_Threshold[0], SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                   
                        else: 
                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.YDI_Threshold[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                        
                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Y_avg', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Y_avg', df_YDI_online.Y_avg[0], SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                                  
                        else: 
                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Y_avg[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Y_avg', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                        
                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Cluster_idx', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Cluster_idx', df_YDI_online.Cluster_idx[0], SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                                  
                        else: 
                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Cluster_idx[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Cluster_idx', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                        
                        if int(df_YDI_online.YDI_Check[0]) == 0:
                            update_projectx_run_ydialarm_by_runindex(project_id, 1, SVM_PARAMETER_DATA_y.RUNINDEX[0], server_name, db_name)
                        APLog.WriteApLog("online_y_phase YDI" , projectid)
                    update_online_scantime_lastscantime_by_projectid_datatype(SVM_PARAMETER_DATA_y.ITIME[0], SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], 'Y')
                    UpdateLastScanTime = datetime.datetime.strftime(SVM_PARAMETER_DATA_y.ITIME[0], '%Y-%m-%d %H:%M:%S.%f')[:-3]
                    APLog.WriteApLog("online_y_phase UpdateLastScanTime=" + UpdateLastScanTime , projectid)
        
        return None
    
    
    def online_y_many_phase(self, projectid, datatype, server_name, db_name, retrain_num, batch_num=None):
        retrain_num = self.get_max_retrain_num(retrain_num)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)

        #server_name = "10.96.18.199"
        #db_name = "APC"
        #user = "YL"
        #password = "YL$212"                  
        
        SVM_PROJECT_ONLINE_SCANTIME = select_project_online_scantime_by_projectid_datatype(projectid, datatype)
        
        #20190620 get server name
        #server_name_1 = str(SVM_PROJECT_ONLINE_SCANTIME.SERVER_NAME[0])
        #cnxn2 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server_name_1 + ';DATABASE=' + db_name + ';UID=' + user + ';PWD=' + password)
        
        project_id = "SVM_PROJECT" + str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0])
        
        #sql = "select * from SVM_PROJECT_MODEL where PROJECT_ID = \'" + str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0]) + "\'"
        #SVM_PROJECT_MODEL = pd.read_sql(sql, cnxn2)
        SVM_PROJECT_MODEL = select_project_model_by_projectid_status(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], "ONLINE")
        
        #20190809 go online parameter[datatype:Y] to get realY
        SVM_PROJECT_ONLINE_PARAMETER = select_online_parameter_by_projectid_datatype(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], "Y")
        
        if len(SVM_PROJECT_ONLINE_PARAMETER) > 0:
            #print("--------------------")
            #print(projectid)
            #print('config no data')   
            #print("--------------------")                                                                                                                             
        #else:            
            LAST_SCANTIME = datetime.datetime.strftime(SVM_PROJECT_ONLINE_SCANTIME.LAST_SCANTIME[0], '%Y-%m-%d %H:%M:%S.%f')[:-3]
            APLog.WriteApLog("LAST_SCANTIME : " + LAST_SCANTIME, projectid)
            #sql = "select * from " + str(project_id) + r"_PARAMETER_DATA where PARAMETER = '" + str(SVM_PROJECT_ONLINE_PARAMETER.ONLINE_PARAMETER[0]) + r"' and ITIME > '" + LAST_SCANTIME + r"'" 
            #SVM_PARAMETER_DATA_y = pd.read_sql(sql, cnxn2)
            SVM_PARAMETER_DATA_y = select_projectx_parameter_data_by_parameter_itime(project_id, SVM_PROJECT_ONLINE_PARAMETER.ONLINE_PARAMETER[0], LAST_SCANTIME, server_name, db_name)
            
            if len(SVM_PARAMETER_DATA_y) == 0:
                print("--------------------")
                print(projectid)
                print('SVM_PARAMETER_DATA_y null ns') 
                print("--------------------")
            else:
                #20190809 go project config[parameter3RD:Filter_Feature] to get FILTER_FEATURE
                SVM_PROJECT_CONFIG = select_project_config_by_modelid_parameter3RD(SVM_PROJECT_MODEL.MODEL_ID[0], "Filter_Feature")
                FILTER_FEATURE = SVM_PROJECT_CONFIG.PARAM_VALUE[0] 
                #sql = "select * from " + str(project_id) + "_RUN where RUNINDEX = " + str(SVM_PARAMETER_DATA_y.RUNINDEX[0])
                #SVM_PROJECT_RUN = pd.read_sql(sql, cnxn2)
                SVM_PROJECT_RUN = select_projectx_run_by_runindex(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], server_name, db_name)
                APLog.WriteApLog("RUNINDEX : " + str(SVM_PARAMETER_DATA_y.RUNINDEX[0]), projectid)
                if len(SVM_PROJECT_RUN) != 0:                    
                    
                    try:
                        db_p = SVM_PROJECT_RUN[FILTER_FEATURE][0]
                    except:
                        db_p = SVM_PROJECT_RUN['FILTER_FEATURE'][0]  
                        
                    #20190918 must convert str
                    base_name = str(projectid) + '_' + FILTER_FEATURE + '_' + db_p   
                    
                    path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]
                    
                    #20190930 for different base and online server
                    SVM_ONLINE_PARAMETER = select_online_parameter_by_projectid(projectid)
                    SVM_PARAMETER_DATA1 = select_projectx_parameter_data_by_runindex(project_id, SVM_PROJECT_RUN.RUNINDEX[0], server_name, db_name)
                    SVM_ONLINE_PARAMETER.rename(columns = {'ONLINE_PARAMETER':'PARAMETER'}, inplace = True) 
        
                    SVM_PARAMETER_MERGE = pd.merge(SVM_ONLINE_PARAMETER, SVM_PARAMETER_DATA1, how='left', on='PARAMETER')
                    SVM_ONLINE_PARAMETER_null = SVM_PARAMETER_MERGE[SVM_PARAMETER_MERGE['PARAM_VALUE'].isnull()]
                    SVM_ONLINE_PARAMETER_notnull = SVM_PARAMETER_MERGE[SVM_PARAMETER_MERGE['PARAM_VALUE'].notnull()]
                    SVM_ONLINE_PARAMETER_null = SVM_ONLINE_PARAMETER_null.reset_index(drop=True) #20191002 reset_index avoid for loop error
                    SVM_ONLINE_PARAMETER_notnull = SVM_ONLINE_PARAMETER_notnull.reset_index(drop=True)
                    
                    #input_path = read_path(path_dict['3']["3"])
                    #mylog = WriteLog(input_path["log_path"], input_path["error_path"]) 
                    #mylog.info(str(SVM_PARAMETER_DATA_y.RUNINDEX[0]))
                    #mylog.info(str(SVM_ONLINE_PARAMETER_null.shape))
                    #mylog.info(str(SVM_ONLINE_PARAMETER_notnull.shape))
                    
                    for k in range(len(SVM_ONLINE_PARAMETER_null)):
                                              
                        if str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'TOOLID':
                            SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.TOOLID[0]                           
                        elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'LOTNAME':   
                            SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.LOTNAME[0]                        
                        elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'FOUPNAME':                           
                            SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.FOUPNAME[0]
                        elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'OPID':                           
                            SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.OPID[0]
                        elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'RECIPE':                           
                            SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.RECIPE[0]
                        elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'ABBRID':                           
                            SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.ABBRID[0]
                        elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'PRODUCT':                           
                            SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.PRODUCT[0]
                        elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'MODELNO':                           
                            SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.MODELNO[0]
                        elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'CHAMBER':                           
                            SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.CHAMBER[0]
                        elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'SHEETID':                           
                            SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.SHEETID[0]
                        elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'SLOTNO':                           
                            SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.SLOTNO[0]
                        elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'POINT':                           
                            SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.POINT[0]
                        elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'FILTER_FEATURE':                           
                            SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.FILTER_FEATURE[0]
                    
                    SVM_PARAMETER_DATA = pd.concat([SVM_ONLINE_PARAMETER_notnull, SVM_ONLINE_PARAMETER_null], axis=0, ignore_index=True)    
                    SVM_PARAMETER_DATA = SVM_PARAMETER_DATA.sort_values("SEQ").reset_index(drop=True)
                    SVM_PARAMETER_DATA = SVM_PARAMETER_DATA[["OFFLINE_PARAMETER", "PARAM_VALUE"]]                                                                                       
                    
                    if len(SVM_PARAMETER_DATA) == 0:
                        print('SVM_PARAMETER_DATA null')  #20190619 add inert sql in the future
                    else:
                        df_SVM_PARAMETER_DATA_lite_T = SVM_PARAMETER_DATA.T #20190619 行列互轉
                        headers = df_SVM_PARAMETER_DATA_lite_T.iloc[0] #20190619 first row set header
                        df_SVM_PARAMETER_DATA_lite_T_H  = pd.DataFrame(df_SVM_PARAMETER_DATA_lite_T.values[1:], columns=headers)
                            
                        print("online y phase[ns]")
                        #20191009 avoid runindex duplicate error 
                        try:
                            df_online_X, df_online_y = OnLine_Data_PreProcess(df_SVM_PARAMETER_DATA_lite_T_H, path_dict['3']["3"], check_include_y=True)
                        except Exception as e:
                            errMsg = ProcEx.ProcessException(e) # Reaver
                            APLog.WriteApLog( str(SVM_PROJECT_RUN.RUNINDEX[0]) + " , " + errMsg , projectid) # Reaver
                            #mylog.error(e)
                            #mylog.error(SVM_PROJECT_RUN.RUNINDEX[0])
                            update_online_scantime_lastscantime_by_projectid_datatype(SVM_PARAMETER_DATA_y.ITIME[0], SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], 'Y')
                            return None
                            
                        df_YDI_online = OnLine_YDI(df_online_X, df_online_y, path_dict['5']["3"])

                        try:
                            SVM_PROJECT_MODEL = select_project_model_by_projectid_status_filterfeature(projectid, "ONLINE", SVM_PROJECT_RUN[FILTER_FEATURE][0])
                        except:
                            SVM_PROJECT_MODEL = select_project_model_by_projectid_status_filterfeature(projectid, "ONLINE", SVM_PROJECT_RUN['FILTER_FEATURE'][0])
                        
                        print("----------------")
                        print(project_id)
                        print(SVM_PARAMETER_DATA_y.RUNINDEX[0])
                        print("----------------")
        
                        AI_365(SVM_PROJECT_ONLINE_SCANTIME.FAB[0], projectid, SVM_PROJECT_ONLINE_SCANTIME.AI365_PROJECT_NAME[0], SVM_PROJECT_ONLINE_SCANTIME.PROJECT_TITLE[0], SVM_PROJECT_MODEL.MODEL_ID[0], SVM_PARAMETER_DATA_y.RUNINDEX[0], SVM_PARAMETER_DATA_y.PARAM_VALUE[0], server_name, db_name)
                        
                        if df_YDI_online is not None:  
                            
                            SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Interval', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                            if len(SVM_PROJECT_PREDICT_DATA) == 0:  
                                insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Interval', df_YDI_online.Interval[0], SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                              
                            else: 
                                update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Interval[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Interval', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                            
                            SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                            if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI', df_YDI_online.YDI[0], SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                           
                            else: 
                                update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.YDI[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                            
                            SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                            if len(SVM_PROJECT_PREDICT_DATA) == 0:   
                                insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI_Threshold', df_YDI_online.YDI_Threshold[0], SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                   
                            else: 
                                update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.YDI_Threshold[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                            
                            SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Y_avg', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                            if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Y_avg', df_YDI_online.Y_avg[0], SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                                  
                            else: 
                                update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Y_avg[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Y_avg', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                            
                            SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Cluster_idx', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                            if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Cluster_idx', df_YDI_online.Cluster_idx[0], SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name, db_name)                                                  
                            else: 
                                update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Cluster_idx[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Cluster_idx', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                            
                            if int(df_YDI_online.YDI_Check[0]) == 0:
                                update_projectx_run_ydialarm_by_runindex(project_id, 1, SVM_PARAMETER_DATA_y.RUNINDEX[0], server_name, db_name)
                            APLog.WriteApLog("online_y_many_phase YDI" , projectid)
                        update_online_scantime_lastscantime_by_projectid_datatype(SVM_PARAMETER_DATA_y.ITIME[0], SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], 'Y')
                        UpdateLastScanTime = datetime.datetime.strftime(SVM_PARAMETER_DATA_y.ITIME[0], '%Y-%m-%d %H:%M:%S.%f')[:-3]
                        APLog.WriteApLog("online_y_many_phase UpdateLastScanTime=" + UpdateLastScanTime , projectid)
        return None
    
    
    def one_to_all_online_y(self, projectid, datatype, retrain_num, server_name, db_name): 
        APLog.WriteApLog("" , projectid)
        APLog.WriteApLog("---- one_to_all_online_y Start ----" , projectid)     
        APLog.WriteApLog("server_name=" + server_name + " , db_name=" + db_name , projectid)
        retrain_num = self.get_max_retrain_num(retrain_num=retrain_num)
        self.get_saved_path_config()
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=None)
        
        self.online_y_phase(projectid, datatype, server_name, db_name, base_name=self.base_name, retrain_num=retrain_num, batch_num=batch_num)
        APLog.WriteApLog("---- one_to_all_online_y End   ----" , projectid)     
        return None
    
    
    def many_to_many_online_y(self, projectid, datatype, retrain_num, server_name, db_name):       
        APLog.WriteApLog("" , projectid)
        APLog.WriteApLog("---- many_to_many_online_y Start ----" , projectid)
        APLog.WriteApLog("server_name=" + server_name + " , db_name=" + db_name , projectid)        
        retrain_num = self.get_max_retrain_num(retrain_num=retrain_num)
        
        if self.filter_feature is None:
            self.get_filter_feature()
        APLog.WriteApLog("self.filter_feature=" + self.filter_feature, projectid)
        if self.feature_lists is None:
            self.get_feature_content()
        
        if self.filter_dir_name is None:
            self.filter_dir_name = {}
            for feature in self.feature_lists:
                self.filter_dir_name[feature] = self.base_name + "_" + self.filter_feature + "_" + str(feature)
        APLog.WriteApLog("self.filter_dir_name Count=" + str(len(self.filter_dir_name)), projectid)
        self.get_saved_path_config()
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=None)
        #print("-------------")
        #print(projectid)
        #print("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num))
        
        self.online_y_many_phase(projectid, datatype, server_name, db_name, retrain_num=retrain_num, batch_num=batch_num)
        APLog.WriteApLog("---- many_to_many_online_y End   ----" , projectid)
        return None
  
if __name__ == '__main__':        
    #20190627 online data must go 10.96.18.199, get SERVER_NAME to the server
    def timer(n):
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        SVM_PROJECT_ONLINE_SCANTIME = select_project_online_scantime_model_y_by_onlineserver_status(ip, "ONLINE") #20190828 onlone server must meets the local ip
        SVM_PROJECT_ONLINE_SCANTIME_many = select_project_online_scantime_y_many_by_onlineserver(ip)
        run_number = 0 #20190829 add run count
        while True:           
            p1 = mp.Pool(processes=8)  # 創建8條進程, wait all processes finish and go next cycle    
            p2 = mp.Pool(processes=8)
                                          
            if len(SVM_PROJECT_ONLINE_SCANTIME) != 0:
                
                for a in range(len(SVM_PROJECT_ONLINE_SCANTIME)):                    
                                        
                    projectid = SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[a]
                    #projectid = 48
                    #20191106 avoid move data error cause predict status = 2, let project not predict      
                    try:
                        SVM = SuperVM(str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_NAME[a]), str(SVM_PROJECT_ONLINE_SCANTIME.UPLOAD_FILE[a]), r"Config.json")   
                    except Exception as e:
                        errMsg = ProcEx.ProcessException(e) # Reaver
                        APLog.WriteApLog( errMsg , projectid) # Reaver
                        #print(errMsg)
                        #return None
                    
                    datatype = SVM_PROJECT_ONLINE_SCANTIME.DATATYPE[a].upper()
                    retrain_num = int(SVM_PROJECT_ONLINE_SCANTIME.MODEL_SEQ[a]) 
                    server_name = SVM_PROJECT_ONLINE_SCANTIME.SERVER_NAME[a]
                    db_name = SVM_PROJECT_ONLINE_SCANTIME.DB_NAME[a]
                    
                    print("--------------")
                    print(projectid)
                    print(datatype)
                    print(retrain_num)
                    print(server_name)
                    print(db_name)
                    print("--------------")
                    
                    try :                        
                        r = p1.apply_async(SVM.one_to_all_online_y, (projectid, datatype, retrain_num, server_name, db_name))
                        #r = SVM.one_to_all_online_y(projectid, datatype, retrain_num, server_name, db_name)
                        #print(r.get())
                    except Exception as e:
                        errMsg = ProcEx.ProcessException(e) # Reaver
                        APLog.WriteApLog( errMsg , projectid) # Reaver
                    
            if len(SVM_PROJECT_ONLINE_SCANTIME_many) != 0:
                
                for a in range(len(SVM_PROJECT_ONLINE_SCANTIME_many)):                    
                
                    projectid = SVM_PROJECT_ONLINE_SCANTIME_many.PROJECT_ID[a]
                    SVM_MODEL_ONLINE_Y_MANY = select_project_model_by_projectid_status(projectid, "ONLINE")
                    
                    if len(SVM_MODEL_ONLINE_Y_MANY) != 0:
                        
                        #20191106 avoid move data error cause predict status = 2, let project not predict      
                        try:
                            SVM = SuperVM(str(SVM_PROJECT_ONLINE_SCANTIME_many.PROJECT_NAME[a]), str(SVM_PROJECT_ONLINE_SCANTIME_many.UPLOAD_FILE[a]), r"Config.json") 
                        except Exception as e:
                            errMsg = ProcEx.ProcessException(e) # Reaver
                            APLog.WriteApLog( errMsg , projectid) # Reaver
                            #print(errMsg)
                            #return None
                        
                        datatype = SVM_PROJECT_ONLINE_SCANTIME_many.DATATYPE[a].upper()
                        retrain_num = int(SVM_MODEL_ONLINE_Y_MANY.MODEL_SEQ[0])
                        server_name = SVM_PROJECT_ONLINE_SCANTIME_many.SERVER_NAME[a]
                        db_name = SVM_PROJECT_ONLINE_SCANTIME_many.DB_NAME[a]
                        
                        print("--------------")
                        print(projectid)
                        print(datatype)
                        print(retrain_num)
                        print(server_name)
                        print(db_name)
                        print("--------------")
                        
                        try:
                            r = p2.apply_async(SVM.many_to_many_online_y, (projectid, datatype, retrain_num, server_name, db_name))
                            #SVM.many_to_many_online_y(projectid, datatype, retrain_num, server_name, db_name)
                        except Exception as e:
                            errMsg = ProcEx.ProcessException(e) # Reaver
                            APLog.WriteApLog( errMsg , projectid) # Reaver
            
            p1.close()
            p2.close()
            p1.join()  # 等待進程池中的事件執行完畢，回收進程池
            p2.join()
            
            run_number = run_number+1
            print("Round " + str(run_number))
            
            #20190829 add run count, 3600s scan db 
            if run_number == 600:
                SVM_PROJECT_ONLINE_SCANTIME = select_project_online_scantime_model_y_by_onlineserver_status(ip, "ONLINE")
                SVM_PROJECT_ONLINE_SCANTIME_many = select_project_online_scantime_y_many_by_onlineserver(ip)
                run_number = 0
                                 
            time.sleep(n)
    timer(3) 



# SmartVM_Constructor_online_xy_abnormal_mp.py
#20190912 delete pls csv,avoid get pre data 

from Data_PreProcess import Data_PreProcess_Train, Data_PreProcess_Test, OnLine_Data_PreProcess
from XDI import XDI_off_line_report, Build_XDI_Model, OnLine_XDI
from YDI import YDI_off_line_report, Build_YDI_Model, OnLine_YDI
from MXCI_MYCI_pre import pre_MXCI_MYCI
from MXCI_MYCI import CI_online
from MXCI_MYCI_20190805 import MXCI_MYCI_offline, CI_online_old #20190827 use old CI_online, wait david check ok
#from XGB_model import xgb_online_predict
#from PLS_model import pls_online_predict_x_abnormal, pls_online_predict_x_py
from Read_path import read_path
from config import read_config
from CreateLog import WriteLog
from DB_operation import select_project_model_otm_by_onlineserver_predictresult, select_project_by_projectid, select_online_parameter_by_projectid, select_online_parameter_by_projectid_datatype, select_project_model_by_projectid_predictresult, select_projectx_run_by_runindex, select_projectx_parameter_data_by_runindex, select_projectx_parameter_data_by_parameter_itime_itime, select_projectx_predict_data_by_runindex_parameter_modelid, update_project_model_predictresult_by_modelid, update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid, insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict
from prediction import modelpredict as OnlineModelPredict

import os
import json
import traceback
# from Data_Check import Data_Check
from shutil import copyfile
import pandas as pd
import datetime
import time
#import pyodbc
import socket
import multiprocessing as mp

import SystemConfig.ServerConfig as ServerConfig # Reaver
import ProcException as ProcEx                   # Reaver
import APLog as APLog                            # Reaver

SystemLog_Path = ServerConfig.Log_Path + "\\" + datetime.datetime.now().strftime("%Y%m%d") + ServerConfig.Log_Keep_Day + "\\"  # Reaver
ErrorLog_Path = ServerConfig.Log_Path + "\\" + datetime.datetime.now().strftime("%Y%m%d") + ServerConfig.Log_Keep_Day + "\\"  # Reaver


class SuperVM():
    def __init__(self, sorce_dir, train_data_name, config_file_name, batch_data_name_list=None):
        self.base_path = os.path.join(ServerConfig.ModelFilePath, sorce_dir)
        self.base_name = os.path.basename(os.path.abspath(self.base_path))
        self.train_data_name = train_data_name
        self.train_base, self.ext = os.path.splitext(self.train_data_name)
        self.train_data = os.path.join(self.base_path, train_data_name)
        self.config_file = os.path.join(self.base_path, config_file_name)
        if batch_data_name_list:
            self.batch_data = [os.path.join(self.base_path, x) for x in batch_data_name_list]
        self.in_path = os.path.join(self.base_path, "path_config.json")
        self.mdoel_dict = {}
        self.mdoel_dict["3"] = {}
        self.mdoel_dict["3"]["Train"] = Data_PreProcess_Train
        self.mdoel_dict["3"]["Test"] = Data_PreProcess_Test
        self.mdoel_dict["4"] = {}
        self.mdoel_dict["4"]["Train"] = Build_XDI_Model
        self.mdoel_dict["4"]["Test"] = XDI_off_line_report
        self.mdoel_dict["5"] = {}
        self.mdoel_dict["5"]["Train"] = Build_YDI_Model
        self.mdoel_dict["5"]["Test"] = YDI_off_line_report
        self.mdoel_dict["8"] = {}
        self.mdoel_dict["8"]["Train"] = pre_MXCI_MYCI
        self.mdoel_dict["8"]["Test"] = MXCI_MYCI_offline

        self.retrain_data = os.path.join(self.base_path, "retrain_data.csv")
        if not os.path.exists(self.retrain_data):
            copyfile(self.train_data, self.retrain_data)

        # Other Variables initialized in methods
        self.filter_feature = None
        self.feature_lists = None
        self.current_retrain_number = None
        self.current_batch_number = None
        self.path_config = None
        self.filter_feature_dict = None
        # self.filter_dir_path = None
        self.filter_dir_name = None

########################################################################################################################
    def get_saved_path_config(self):
        try:
            with open(self.in_path) as json_data:
                tPath = json_data.read()
                tPath = tPath.replace(r"../Cases/", r"D:\\SVM\\Cases\\")
                tPath = tPath.replace(r"/", r"\\")
                self.path_config = json.loads(tPath)
        except Exception as e:
            error_path = os.path.join(self.base_path, "error.log")
            with open(error_path, 'a') as file:
                file.write("Error while reading path_config.json")
                file.write(traceback.format_exc(e))
            raise FileNotFoundError
        return None

########################################################################################################################
    def get_max_retrain_num(self, retrain_num=None):
        if retrain_num is None:
            try:
                first_key = list(self.path_config.keys())[0]
                key_list = list(self.path_config[first_key].keys())
                key_list.remove("main")
                return max([int(x) for x in key_list])
            except:
                try:
                    self.get_saved_path_config()
                    first_key = list(self.path_config.keys())[0]
                    key_list = list(self.path_config[first_key].keys())
                    key_list.remove("main")
                    return max([int(x) for x in key_list])
                except Exception as e:
                    raise FileNotFoundError("path_config not found")
        else:
            return retrain_num

    def get_max_batch_num(self, retrain_num, batch_num=None):
        if batch_num is None:
            first_key = list(self.path_config.keys())[0]
            key_list = list(self.path_config[first_key][str(retrain_num)].keys())
            key_list.remove("main")
            return max([int(x) for x in key_list])
        else:
            return batch_num
    
    def model_predict_online_x(self, df_online_X, path_dict, projectid):        
        input_path = read_path(path_dict['6']["PLS"]["5"])
        #mylog = WriteLog(input_path["log_path"], input_path["error_path"])    
        mylog = WriteLog(SystemLog_Path + str(projectid) + ServerConfig.LogFileExtension, ErrorLog_Path + str(projectid) + ServerConfig.LogFileExtension)  # Reaver
        config = read_config(input_path["config_path"], mylog)
        config_y = config["Y"][0]
        Baseline_Model = config["Model_Selection"][config_y]["Baseline_Model"] 
        Predict_Model = config["Model_Selection"][config_y]["Predict_Model"] 
        
        #20191022 add , to avoid 'int' is not iterable
        #if (int(projectid) in (50,171,196,426,454,572,642,660,661,662,664,728,734,845,886,910,914,943,956,977,986,1001,1003,1005,1011,1019,1023,1035,1041,1054,1057,1145,1146,1147,1148,1159,1274,1290,1336,1434,1469,1511,1517,1519,1521)) or (int(projectid) > 1521):   
        #pls_y_predc = pls_online_predict_x_py(path_dict['6']["PLS"]["5"], df_online_X, projectid)
        #xgb_y_pred = xgb_online_predict(path_dict['6']["XGB"]["5"], df_online_X) #20190702 eat 05_Prediction  
            
        #20190829 check type and return Predict_Model & Baseline_Model(str)
        #pls_y_pred_list = pls_y_predc.tolist()
        tDataName = 'online'
        tModelName = Baseline_Model
        Baseline_data_folder = str(path_dict['6'][Baseline_Model]["5"]).replace("/", "\\")        
        APLog.WriteApLog("tDataName=" + tDataName, projectid)
        APLog.WriteApLog("tModelName=" + tModelName, projectid)
        APLog.WriteApLog("Baseline_data_folder=" + Baseline_data_folder, projectid)        
        
        Baseline_infoChk , Baseline_msg, Baseline_value = OnlineModelPredict(Baseline_data_folder, tModelName, tDataName, df_online_X)
                
        tModelName = Predict_Model
        Predict_data_folder = str(path_dict['6'][Predict_Model]["5"]).replace("/", "\\")        
        APLog.WriteApLog("tDataName=" + tDataName, projectid)
        APLog.WriteApLog("tModelName=" + tModelName, projectid)
        APLog.WriteApLog("Predict_data_folder=" + Predict_data_folder, projectid)
        
        Predict_infoChk , Predict_msg, Predict_value = OnlineModelPredict(Predict_data_folder, tModelName, tDataName, df_online_X)
        
        APLog.WriteApLog("Baseline_infoChk=" + str(Baseline_infoChk) + ", Baseline_msg=" + str(Baseline_msg)+ ", Baseline_value=" + str(Baseline_value), projectid)  
        APLog.WriteApLog("Predict_infoChk=" + str(Predict_infoChk) + ", Predict_msg=" + str(Predict_msg)+ ", Predict_value=" + str(Predict_value), projectid)  
        
        # return Predict_Model , Baseline_Model
        return Predict_value, Baseline_value
        #if Baseline_Model == "PLS":
        #    return xgb_y_pred, pls_y_pred_list[0]
        #else:
        #    return pls_y_pred_list[0], xgb_y_pred
        
        #else:         
        #    pls_online_predict_x_abnormal(path_dict['6']["PLS"]["5"], df_online_X)
        #    
        #    pls_y_name = config["Y"][0]+"_pred_PLS"
            
        #    pls_predict_test_path = os.path.join(path_dict['6']["PLS"]["5"], "trainPredResult.csv")
        #    pls_test=pd.read_csv(pls_predict_test_path)
        #    pls_test.rename({'Y_pred': pls_y_name}, axis=1, inplace=True) #20190722 after pls predict, must rename
        #    pls_y_pred = pls_test[pls_y_name]
    
        #    if len(pls_y_pred) == 0:
        #        pls_y_predc = 0
        #    else:
        #        pls_y_predc = pls_y_pred[0] #20190621 because return series, must assign location
                
        #    #20190912 delete pls csv,avoid get pre data 
        #    try:
        #        os.remove(pls_predict_test_path)
        #    except OSError as e:
        #        print(e)
        #    else:
        #        print("File is deleted successfully")
            
        #    #20190829 check type and return Predict_Model & Baseline_Model(str)
        #    if Baseline_Model == "PLS":
        #        return xgb_y_pred, pls_y_predc 
        #    else:
        #        return pls_y_predc, xgb_y_pred
    
    def online_xy_phase(self, projectid, server_name, db_name, base_name, retrain_num, batch_num=None):
        retrain_num = self.get_max_retrain_num(retrain_num)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)

        path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]
        SVM_PROJECT_OTM = select_project_by_projectid(projectid)
        
        #20190620 get server name
        #server_name_1 = str(SVM_PROJECT_OTM.SERVER_NAME[0])
        
        #db_name = "APC"
        #user = "YL"
        #password = "YL$212" 
        #cnxn2 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server_name_1 + ';DATABASE=' + db_name + ';UID=' + user + ';PWD=' + password)
        
        project_id = "SVM_PROJECT" + str(SVM_PROJECT_OTM.PROJECT_ID[0])
        
        SVM_PROJECT_MODEL_ABNORMAL = select_project_model_by_projectid_predictresult(SVM_PROJECT_OTM.PROJECT_ID[0], "False")
        
        if len(SVM_PROJECT_MODEL_ABNORMAL) == 0:
            print("no model in abnormal")
        else:
            #20190809 go online parameter[datatype:Y] to get realY                    
            SVM_PROJECT_CONFIG = select_online_parameter_by_projectid_datatype(SVM_PROJECT_OTM.PROJECT_ID[0], "Y")
            
            if len(SVM_PROJECT_CONFIG) == 0:
                print('config no data')                                                                                                                                                       
            else:       
                PREDICT_START_TIME = datetime.datetime.strftime(SVM_PROJECT_MODEL_ABNORMAL.PREDICT_START_TIME[0], '%Y-%m-%d %H:%M:%S.%f')[:-3]
                RETRAIN_END_TIME = datetime.datetime.strftime(SVM_PROJECT_MODEL_ABNORMAL.RETRAIN_END_TIME[0], '%Y-%m-%d %H:%M:%S.%f')[:-3]
                
                #sql = "select * from " + str(project_id) + r"_PARAMETER_DATA where PARAMETER = '" + str(SVM_PROJECT_CONFIG.ONLINE_PARAMETER[0]) + r"' and ITIME > '" + PREDICT_START_TIME + r"' and ITIME < '" + RETRAIN_END_TIME + r"' order by ITIME" 
                #SVM_PARAMETER_DATA_y = pd.read_sql(sql, cnxn2)
                SVM_PARAMETER_DATA_y = select_projectx_parameter_data_by_parameter_itime_itime(project_id, SVM_PROJECT_CONFIG.ONLINE_PARAMETER[0], PREDICT_START_TIME, RETRAIN_END_TIME, server_name, db_name)
                
                if len(SVM_PARAMETER_DATA_y) == 0:
                    print('SVM_PARAMETER_DATA_y null') 
                else:   
                    for m in range(len(SVM_PARAMETER_DATA_y)): 
                                                
                        #sql = "select * from " + str(project_id) + "_RUN where RUNINDEX = \'" + str(SVM_PARAMETER_DATA_y.RUNINDEX[m]) + "\'"
                        #SVM_PROJECT_RUN = pd.read_sql(sql, cnxn2)
                        SVM_PROJECT_RUN = select_projectx_run_by_runindex(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], server_name, db_name)
                        
                        #20190930 for different base and online server
                        SVM_ONLINE_PARAMETER = select_online_parameter_by_projectid(projectid)
                        SVM_PARAMETER_DATA1 = select_projectx_parameter_data_by_runindex(project_id, SVM_PROJECT_RUN.RUNINDEX[0], server_name, db_name)
                        SVM_ONLINE_PARAMETER.rename(columns = {'ONLINE_PARAMETER':'PARAMETER'}, inplace = True) 
            
                        SVM_PARAMETER_MERGE = pd.merge(SVM_ONLINE_PARAMETER, SVM_PARAMETER_DATA1, how='left', on='PARAMETER')
                        SVM_ONLINE_PARAMETER_null = SVM_PARAMETER_MERGE[SVM_PARAMETER_MERGE['PARAM_VALUE'].isnull()]
                        SVM_ONLINE_PARAMETER_notnull = SVM_PARAMETER_MERGE[SVM_PARAMETER_MERGE['PARAM_VALUE'].notnull()]
                        SVM_ONLINE_PARAMETER_null = SVM_ONLINE_PARAMETER_null.reset_index(drop=True) #20191002 reset_index avoid for loop error
                        SVM_ONLINE_PARAMETER_notnull = SVM_ONLINE_PARAMETER_notnull.reset_index(drop=True)
                        
                        for k in range(len(SVM_ONLINE_PARAMETER_null)):
                                                  
                            if str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'TOOLID':
                                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.TOOLID[0]                           
                            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'LOTNAME':   
                                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.LOTNAME[0]                        
                            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'FOUPNAME':                           
                                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.FOUPNAME[0]
                            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'OPID':                           
                                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.OPID[0]
                            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'RECIPE':                           
                                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.RECIPE[0]
                            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'ABBRID':                           
                                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.ABBRID[0]
                            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'PRODUCT':                           
                                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.PRODUCT[0]
                            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'MODELNO':                           
                                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.MODELNO[0]
                            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'CHAMBER':                           
                                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.CHAMBER[0]
                            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'SHEETID':                           
                                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.SHEETID[0]
                            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'SLOTNO':                           
                                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.SLOTNO[0]
                            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'POINT':                           
                                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.POINT[0]
                            elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'FILTER_FEATURE':                           
                                SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.FILTER_FEATURE[0]
                        
                        SVM_PARAMETER_DATA = pd.concat([SVM_ONLINE_PARAMETER_notnull, SVM_ONLINE_PARAMETER_null], axis=0, ignore_index=True)    
                        SVM_PARAMETER_DATA = SVM_PARAMETER_DATA.sort_values("SEQ").reset_index(drop=True)
                        SVM_PARAMETER_DATA = SVM_PARAMETER_DATA[["OFFLINE_PARAMETER", "PARAM_VALUE"]]                                                
                                                                                                                                                                                                                           
                        if len(SVM_PARAMETER_DATA) == 0:
                            print('SVM_PARAMETER_DATA null')  #20190619 add inert sql in the future
                        else:
                            #df_SVM_PARAMETER_DATA_lite=SVM_PARAMETER_DATA.drop(['RUNINDEX','ITIME'], axis=1)
                            df_SVM_PARAMETER_DATA_lite_T = SVM_PARAMETER_DATA.T #20190619 行列互轉
                            headers = df_SVM_PARAMETER_DATA_lite_T.iloc[0] #20190619 first row set header
                            df_SVM_PARAMETER_DATA_lite_T_H  = pd.DataFrame(df_SVM_PARAMETER_DATA_lite_T.values[1:], columns=headers)
                            
                            #input_path = read_path(path_dict['3']["3"])
                            #mylog = WriteLog(input_path["log_path"], input_path["error_path"])    
                            #20191009 avoid runindex duplicate error 
                            try:
                                df_online_X, df_online_y = OnLine_Data_PreProcess(df_SVM_PARAMETER_DATA_lite_T_H, path_dict['3']["3"], check_include_y=True)
                            except Exception as e:
                                errMsg = ProcEx.ProcessException(e) # Reaver
                                APLog.WriteApLog( str(SVM_PROJECT_RUN.RUNINDEX[0]) + "," + errMsg , projectid) # Reaver                        
                                #mylog.error(e)
                                #mylog.error(SVM_PROJECT_RUN.RUNINDEX[0])
                                continue
                            
                            XDI_online_value, XDI_Threshold, XDI_Check, df_contri = OnLine_XDI(df_online_X, path_dict['4']["3"])
                            df_YDI_online = OnLine_YDI(df_online_X, df_online_y, path_dict['5']["3"])
                            y_pred, y_base = self.model_predict_online_x(df_online_X, path_dict, projectid)
                            
                            
                            #20190910 int must equal int, execute CI_online/CI_online_old
                            if (projectid in (50,67,95,171,174,426,454)) or (projectid > 570):
                                MXCI_value, MXCI_T, MYCI_value, MYCI_T, light = CI_online(path_dict['8']["3"], df_online_X, y_pred, y_base)
                            else:
                                MXCI_value, MXCI_T, MYCI_value, MYCI_T, light = CI_online_old(path_dict['8']["3"], df_online_X, y_pred, y_base)
                            
                            if XDI_online_value is not None:
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'XDI', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'XDI', XDI_online_value, SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name, db_name)                                
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, XDI_online_value, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'XDI', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                                      
                            if XDI_Threshold is not None:
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'XDI_Threshold', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:  
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'XDI_Threshold', XDI_Threshold, SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name, db_name)  
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, XDI_Threshold, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'XDI_Threshold', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)                                             
                                                                                                           
                            if y_pred is not None:
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Predict_Y', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Predict_Y', y_pred, SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name, db_name)                                  
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, y_pred, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Predict_Y', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                            
                            #20190808 insert y_base to prdeict_data
                            if y_base is not None:
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Baseline_Y', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Baseline_Y', y_base, SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name, db_name)                                  
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, y_base, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Baseline_Y', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                            
                            if MXCI_value is not None:
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MXCI', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:   
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MXCI', MXCI_value, SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name, db_name)  
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MXCI_value, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MXCI', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                
                            if MXCI_T is not None:
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MXCI_Threshold', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MXCI_Threshold', MXCI_T, SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name, db_name)                                    
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MXCI_T, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MXCI_Threshold', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                
                            if MYCI_value is not None:
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MYCI', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                #MYCI_value = float(str(MYCI_value)[0:10]) #20190911 not additional to change digits
                                MYCI_value = round(MYCI_value,50)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MYCI', MYCI_value, SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name, db_name)                                   
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MYCI_value, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MYCI', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                
                            if MYCI_T is not None:
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MYCI_Threshold', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MYCI_Threshold', MYCI_T, SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name, db_name)                                    
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MYCI_T, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MYCI_Threshold', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                            
                            #20190923 modify isretrainpredict -> 1
                            if df_YDI_online is not None:
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Interval', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:  
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Interval', df_YDI_online.Interval[0], SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name, db_name)                              
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Interval[0], 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Interval', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'YDI', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'YDI', df_YDI_online.YDI[0], SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name, db_name)                                           
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.YDI[0], 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'YDI', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'YDI_Threshold', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:   
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'YDI_Threshold', df_YDI_online.YDI_Threshold[0], SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name, db_name)                                   
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.YDI_Threshold[0], 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'YDI_Threshold', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Y_avg', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Y_avg', df_YDI_online.Y_avg[0], SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name, db_name)                                                  
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Y_avg[0], 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Y_avg', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Cluster_idx', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Cluster_idx', df_YDI_online.Cluster_idx[0], SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name, db_name)                                                  
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Cluster_idx[0], 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Cluster_idx', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name, db_name)
                
                #20190808 have/no real y will update predict result to True
                update_project_model_predictresult_by_modelid("True", SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0])                                                                                        
        return None
    
    def one_to_all_online_xy(self, projectid, retrain_num, server_name, db_name):  
        APLog.WriteApLog("", projectid)
        APLog.WriteApLog("---- one_to_all_online_xy Start ----", projectid)
        APLog.WriteApLog("server_name=" + server_name + " , db_name=" + db_name , projectid)
        
        retrain_num = self.get_max_retrain_num(retrain_num=retrain_num)
        self.get_saved_path_config() #20190715 get path_config
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=None)
        APLog.WriteApLog("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num), projectid)
        
        self.online_xy_phase(projectid, server_name, db_name, base_name=self.base_name, retrain_num=retrain_num, batch_num=batch_num)
        
        APLog.WriteApLog("---- one_to_all_online_xy End   ----", projectid)
        return None
 
if __name__ == '__main__':        
    #20190627 online data must go 10.96.18.199, get SERVER_NAME to the server
    def timer(n):
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        SVM_PROJECT_OTM = select_project_model_otm_by_onlineserver_predictresult(ip, "False") #20190828 onlone server must meets the local ip
        run_number = 0 #20190829 add run count
        while True: 
            p1 = mp.Pool(processes=8)  # 創建8條進程, wait all processes finish and go next cycle  
            
            if len(SVM_PROJECT_OTM) != 0:
                
                for a in range(len(SVM_PROJECT_OTM)):                    
                    
                    projectid = SVM_PROJECT_OTM.PROJECT_ID[a]
                                                               
                    SVM = SuperVM(str(SVM_PROJECT_OTM.PROJECT_NAME[a]), str(SVM_PROJECT_OTM.UPLOAD_FILE[a]), r"Config.json")                                                                           
                    retrain_num = int(SVM_PROJECT_OTM.MODEL_SEQ[a])
                    server_name = SVM_PROJECT_OTM.SERVER_NAME[a]
                    db_name = SVM_PROJECT_OTM.DB_NAME[a]
                    
                    print("--------------")
                    print(projectid)
                    print(retrain_num)
                    print(server_name)
                    print(db_name)
                    print("--------------")
                    
                    r = p1.apply_async(SVM.one_to_all_online_xy, (projectid, retrain_num, server_name, db_name))
            
            p1.close()
            p1.join()  # 等待進程池中的事件執行完畢，回收進程池
            
            run_number = run_number+1
            print("Round " + str(run_number))
            
            #20190829 add run count, 3600s scan db 
            if run_number == 600:
                SVM_PROJECT_OTM = select_project_model_otm_by_onlineserver_predictresult(ip, "False") 
                run_number = 0
                                    
            time.sleep(n)
    timer(3) 



# SmartVM_Constructor_online_xy_many_abnormal_mp.py
#20190829 modify not for loop
#20190912 delete pls csv,avoid get pre data 

from Data_PreProcess import Data_PreProcess_Train, Data_PreProcess_Test, OnLine_Data_PreProcess
from XDI import XDI_off_line_report, Build_XDI_Model, OnLine_XDI
from YDI import YDI_off_line_report, Build_YDI_Model, OnLine_YDI
from MXCI_MYCI_pre import pre_MXCI_MYCI
from MXCI_MYCI import CI_online
from MXCI_MYCI_20190805 import MXCI_MYCI_offline, CI_online_old #20190827 use old CI_online, wait david check ok
#from XGB_model import xgb_online_predict
#from PLS_model import pls_online_predict_x_many_abnormal, pls_online_predict_x_py
from prediction import modelpredict as OnlineModelPredict
from Read_path import read_path
from config import read_config
from CreateLog import WriteLog
from DB_operation import select_project_mtm_by_onlineserver, select_project_by_projectid, select_online_parameter_by_projectid_datatype, select_project_model_by_projectid_predictresult, select_project_model_by_projectid_status2_filterfeature, select_project_config_by_modelid_parameter3RD, select_online_parameter_by_projectid, select_projectx_run_by_runindex, select_projectx_parameter_data_by_runindex, select_projectx_parameter_data_by_parameter_itime_itime, select_projectx_predict_data_by_runindex_parameter_modelid, update_project_model_predictresult_by_modelid, update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid, insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict

import os
import json
import traceback
# from Data_Check import Data_Check

from shutil import copyfile
import pandas as pd
#import pyodbc
import datetime
import time
import socket
import multiprocessing as mp

import SystemConfig.ServerConfig as ServerConfig # Reaver
import ProcException as ProcEx                   # Reaver
import APLog as APLog                            # Reaver

SystemLog_Path = ServerConfig.Log_Path + "\\" + datetime.datetime.now().strftime("%Y%m%d") + ServerConfig.Log_Keep_Day + "\\"  # Reaver
ErrorLog_Path = ServerConfig.Log_Path + "\\" + datetime.datetime.now().strftime("%Y%m%d") + ServerConfig.Log_Keep_Day + "\\"  # Reaver


class SuperVM():
    def __init__(self, sorce_dir, train_data_name, config_file_name, batch_data_name_list=None):
        self.base_path = os.path.join(ServerConfig.ModelFilePath, sorce_dir)
        self.base_name = os.path.basename(os.path.abspath(self.base_path))
        self.train_data_name = train_data_name
        self.train_base, self.ext = os.path.splitext(self.train_data_name)
        self.train_data = os.path.join(self.base_path, train_data_name)
        self.config_file = os.path.join(self.base_path, config_file_name)
        if batch_data_name_list:
            self.batch_data = [os.path.join(self.base_path, x) for x in batch_data_name_list]
        self.in_path = os.path.join(self.base_path, "path_config.json")
        self.mdoel_dict = {}
        self.mdoel_dict["3"] = {}
        self.mdoel_dict["3"]["Train"] = Data_PreProcess_Train
        self.mdoel_dict["3"]["Test"] = Data_PreProcess_Test
        self.mdoel_dict["4"] = {}
        self.mdoel_dict["4"]["Train"] = Build_XDI_Model
        self.mdoel_dict["4"]["Test"] = XDI_off_line_report
        self.mdoel_dict["5"] = {}
        self.mdoel_dict["5"]["Train"] = Build_YDI_Model
        self.mdoel_dict["5"]["Test"] = YDI_off_line_report
        self.mdoel_dict["8"] = {}
        self.mdoel_dict["8"]["Train"] = pre_MXCI_MYCI
        self.mdoel_dict["8"]["Test"] = MXCI_MYCI_offline

        self.retrain_data = os.path.join(self.base_path, "retrain_data.csv")
        if not os.path.exists(self.retrain_data):
            copyfile(self.train_data, self.retrain_data)

        # Other Variables initialized in methods
        self.filter_feature = None
        self.feature_lists = None
        self.current_retrain_number = None
        self.current_batch_number = None
        self.path_config = None
        self.filter_feature_dict = None
        # self.filter_dir_path = None
        self.filter_dir_name = None

    def get_filter_feature(self):
        try:
            with open(self.config_file) as json_data:
                config = json.load(json_data)
        except Exception as e:
            config = None
            error_path = os.path.join(self.base_path, "error.log")
            with open(error_path, 'a') as file:
                file.write("Error while reading config file\n")
                file.write(traceback.format_exc(e))
                raise FileNotFoundError
        self.filter_feature = config["Filter_Feature"]
        self.model_pred_name = config["Model_Pred_Name"]
        return None

    def get_feature_content(self, split_flag=None):
        try:
            training_data = pd.read_csv(self.train_data)
        except Exception as e:
            training_data = None
            error_path = os.path.join(self.base_path, "error.log")
            with open(error_path, 'a') as file:
                file.write("Error while reading raw data\n")
                file.write(str(e) + "\n")
                file.write(traceback.format_exc())
                raise FileNotFoundError
        self.feature_lists = training_data[self.filter_feature].unique().tolist()
        self.feature_lists.sort()
        if split_flag is not None:
            self.filter_feature_dict = {}
            for feature_list in self.feature_lists:
                tmp_data = training_data[training_data[self.filter_feature] == feature_list].copy().reset_index(drop=True)
                self.filter_feature_dict[feature_list] = \
                    os.path.join(self.base_path, self.train_base + "_" + self.filter_feature + "_" + str(feature_list) + self.ext)
                tmp_data.to_csv(self.filter_feature_dict[feature_list], index=False)
        return None

########################################################################################################################
    def get_saved_path_config(self):
        try:
            with open(self.in_path) as json_data:
                tPath = json_data.read()
                tPath = tPath.replace(r"../Cases/", r"D:\\SVM\\Cases\\")
                tPath = tPath.replace(r"/", r"\\")
                self.path_config = json.loads(tPath)
        except Exception as e:
            error_path = os.path.join(self.base_path, "error.log")
            with open(error_path, 'a') as file:
                file.write("Error while reading path_config.json")
                file.write(traceback.format_exc(e))
            raise FileNotFoundError
        return None

########################################################################################################################
    def get_max_retrain_num(self, retrain_num=None):
        if retrain_num is None:
            try:
                first_key = list(self.path_config.keys())[0]
                key_list = list(self.path_config[first_key].keys())
                key_list.remove("main")
                return max([int(x) for x in key_list])
            except:
                try:
                    self.get_saved_path_config()
                    first_key = list(self.path_config.keys())[0]
                    key_list = list(self.path_config[first_key].keys())
                    key_list.remove("main")
                    return max([int(x) for x in key_list])
                except Exception as e:
                    raise FileNotFoundError("path_config not found")
        else:
            return retrain_num

    def get_max_batch_num(self, retrain_num, batch_num=None):
        if batch_num is None:
            first_key = list(self.path_config.keys())[0]
            key_list = list(self.path_config[first_key][str(retrain_num)].keys())
            key_list.remove("main")
            return max([int(x) for x in key_list])
        else:
            return batch_num 
    
    def model_predict_online_x(self, df_online_X, path_dict, projectid):
                      
        input_path = read_path(path_dict['6']["PLS"]["5"])
        #mylog = WriteLog(input_path["log_path"], input_path["error_path"])    
        mylog = WriteLog(SystemLog_Path + str(projectid) + ServerConfig.LogFileExtension, ErrorLog_Path + str(projectid) + ServerConfig.LogFileExtension)  # Reaver
        config = read_config(input_path["config_path"], mylog)
        config_y = config["Y"][0]
        Baseline_Model = config["Model_Selection"][config_y]["Baseline_Model"]    
        Predict_Model = config["Model_Selection"][config_y]["Predict_Model"] 
        
        #20191022 add , to avoid 'int' is not iterable
        #if (int(projectid) in (48,85,693,694,731,732,733,763,1379,1487)) or (int(projectid) > 1521):   
        #xgb_y_pred = xgb_online_predict(path_dict['6']["XGB"]["5"], df_online_X) #20190702 eat 05_Prediction      
        #pls_y_predc = pls_online_predict_x_py(path_dict['6']["PLS"]["5"], df_online_X, projectid)
        
        tDataName = 'online'
        tModelName = Baseline_Model
        Baseline_data_folder = str(path_dict['6'][Baseline_Model]["5"]).replace("/", "\\")        
        APLog.WriteApLog("tDataName=" + tDataName, projectid)
        APLog.WriteApLog("tModelName=" + tModelName, projectid)
        APLog.WriteApLog("Baseline_data_folder=" + Baseline_data_folder, projectid)        
        
        Baseline_infoChk , Baseline_msg, Baseline_value = OnlineModelPredict(Baseline_data_folder, tModelName, tDataName, df_online_X)
                
        tModelName = Predict_Model
        Predict_data_folder = str(path_dict['6'][Predict_Model]["5"]).replace("/", "\\")        
        APLog.WriteApLog("tDataName=" + tDataName, projectid)
        APLog.WriteApLog("tModelName=" + tModelName, projectid)
        APLog.WriteApLog("Predict_data_folder=" + Predict_data_folder, projectid)
        
        Predict_infoChk , Predict_msg, Predict_value = OnlineModelPredict(Predict_data_folder, tModelName, tDataName, df_online_X)
        
        APLog.WriteApLog("Baseline_infoChk=" + str(Baseline_infoChk) + ", Baseline_msg=" + str(Baseline_msg)+ ", Baseline_value=" + str(Baseline_value), projectid)  
        APLog.WriteApLog("Predict_infoChk=" + str(Predict_infoChk) + ", Predict_msg=" + str(Predict_msg)+ ", Predict_value=" + str(Predict_value), projectid)  
        
        #20190829 check type and return Predict_Model & Baseline_Model(str)
        #pls_y_pred_list = pls_y_predc.tolist()
            
        # return Predict_Model , Baseline_Model
        return Predict_value, Baseline_value
        #if Baseline_Model == "PLS":
        #    return xgb_y_pred, pls_y_pred_list[0]
        #else:
        #    return pls_y_pred_list[0], xgb_y_pred
            
        #else:
        #    pls_online_predict_x_many_abnormal(path_dict['6']["PLS"]["5"], df_online_X)
            
        #    pls_y_name = config["Y"][0]+"_pred_PLS"
            
        #    pls_predict_test_path = os.path.join(path_dict['6']["PLS"]["5"], "trainPredResult.csv")
        #    pls_test=pd.read_csv(pls_predict_test_path)
        #    pls_test.rename({'Y_pred': pls_y_name}, axis=1, inplace=True) #20190722 after pls predict, must rename
        #    pls_y_pred = pls_test[pls_y_name]
            
        #    if len(pls_y_pred) == 0:
        #        pls_y_predc = 0
        #    else:
        #        pls_y_predc = pls_y_pred[0] #20190621 because return series, must assign location
            
        #    #20190912 delete pls csv,avoid get pre data 
        #    try:
        #        os.remove(pls_predict_test_path)
        #    except OSError as e:
        #        print(e)
        #    else:
        #        print("File is deleted successfully")
            
        #    #20190829 check type and return Predict_Model & Baseline_Model(str)
        #    if Baseline_Model == "PLS":            
        #        return xgb_y_pred, pls_y_predc 
        #    else:
        #        return pls_y_predc, xgb_y_pred
    
    def online_xy_many_phase(self, projectid, server_name, db_name, retrain_num, batch_num=None):
        retrain_num = self.get_max_retrain_num(retrain_num)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)

        SVM_PROJECT_MTM = select_project_by_projectid(projectid)
        
        #server_name = "10.96.18.199"
        #db_name = "APC"
        #user = "YL"
        #password = "YL$212"                  
                
        #20190620 get server name
        #server_name_1 = str(SVM_PROJECT_MTM.SERVER_NAME[0])
        #cnxn2 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server_name_1 + ';DATABASE=' + db_name + ';UID=' + user + ';PWD=' + password)
                
        project_id = "SVM_PROJECT" + str(SVM_PROJECT_MTM.PROJECT_ID[0])

        SVM_PROJECT_MODEL_ABNORMAL = select_project_model_by_projectid_predictresult(SVM_PROJECT_MTM.PROJECT_ID[0], "False")
        
        if len(SVM_PROJECT_MODEL_ABNORMAL) == 0:
            print("no model in manay abnormal")
        else:
            #20190809 go online parameter[datatype:Y] to get realY                   
            SVM_PROJECT_ONLINE_PARAMETER = select_online_parameter_by_projectid_datatype(SVM_PROJECT_MTM.PROJECT_ID[0], "Y")
            
            if len(SVM_PROJECT_ONLINE_PARAMETER) == 0:
                print('config no data')                                                                                                                                                       
            else:     
                PREDICT_START_TIME = datetime.datetime.strftime(SVM_PROJECT_MODEL_ABNORMAL.PREDICT_START_TIME[0], '%Y-%m-%d %H:%M:%S.%f')[:-3]
                RETRAIN_END_TIME = datetime.datetime.strftime(SVM_PROJECT_MODEL_ABNORMAL.RETRAIN_END_TIME[0], '%Y-%m-%d %H:%M:%S.%f')[:-3]
                
                SVM_PARAMETER_DATA_y = select_projectx_parameter_data_by_parameter_itime_itime(project_id, SVM_PROJECT_ONLINE_PARAMETER.ONLINE_PARAMETER[0], PREDICT_START_TIME, RETRAIN_END_TIME, server_name, db_name)
                
                if len(SVM_PARAMETER_DATA_y) == 0:
                    print('SVM_PARAMETER_DATA_y null') 
                else:   
                    for m in range(len(SVM_PARAMETER_DATA_y)): 
                        
                        #20190809 go project config[parameter3RD:Filter_Feature] to get FILTER_FEATURE
                        SVM_PROJECT_CONFIG = select_project_config_by_modelid_parameter3RD(SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], "Filter_Feature")
                        FILTER_FEATURE = SVM_PROJECT_CONFIG.PARAM_VALUE[0]                        
                        #sql = "select * from " + str(project_id) + "_RUN where RUNINDEX = \'" + str(SVM_PARAMETER_DATA_y.RUNINDEX[m]) + "\'"
                        #SVM_PROJECT_RUN = pd.read_sql(sql, cnxn2)
                        SVM_PROJECT_RUN = select_projectx_run_by_runindex(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], server_name, db_name)
                        
                        if len(SVM_PROJECT_RUN) != 0:
                            
                            try:
                                db_p = SVM_PROJECT_RUN[FILTER_FEATURE][0]
                            except:
                                db_p = SVM_PROJECT_RUN['FILTER_FEATURE'][0]
                                                                
                            base_name = projectid + '_' + FILTER_FEATURE + '_' + db_p
                            path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]
                            
                            #20190930 for different base and online server
                            SVM_ONLINE_PARAMETER = select_online_parameter_by_projectid(projectid)
                            SVM_PARAMETER_DATA1 = select_projectx_parameter_data_by_runindex(project_id, SVM_PROJECT_RUN.RUNINDEX[0], server_name, db_name)
                            SVM_ONLINE_PARAMETER.rename(columns = {'ONLINE_PARAMETER':'PARAMETER'}, inplace = True) 
                
                            SVM_PARAMETER_MERGE = pd.merge(SVM_ONLINE_PARAMETER, SVM_PARAMETER_DATA1, how='left', on='PARAMETER')
                            SVM_ONLINE_PARAMETER_null = SVM_PARAMETER_MERGE[SVM_PARAMETER_MERGE['PARAM_VALUE'].isnull()]
                            SVM_ONLINE_PARAMETER_notnull = SVM_PARAMETER_MERGE[SVM_PARAMETER_MERGE['PARAM_VALUE'].notnull()]
                            SVM_ONLINE_PARAMETER_null = SVM_ONLINE_PARAMETER_null.reset_index(drop=True) #20191002 reset_index avoid for loop error
                            SVM_ONLINE_PARAMETER_notnull = SVM_ONLINE_PARAMETER_notnull.reset_index(drop=True)
                            
                            for k in range(len(SVM_ONLINE_PARAMETER_null)):
                                                      
                                if str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'TOOLID':
                                    SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.TOOLID[0]                           
                                elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'LOTNAME':   
                                    SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.LOTNAME[0]                        
                                elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'FOUPNAME':                           
                                    SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.FOUPNAME[0]
                                elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'OPID':                           
                                    SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.OPID[0]
                                elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'RECIPE':                           
                                    SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.RECIPE[0]
                                elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'ABBRID':                           
                                    SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.ABBRID[0]
                                elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'PRODUCT':                           
                                    SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.PRODUCT[0]
                                elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'MODELNO':                           
                                    SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.MODELNO[0]
                                elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'CHAMBER':                           
                                    SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.CHAMBER[0]
                                elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'SHEETID':                           
                                    SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.SHEETID[0]
                                elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'SLOTNO':                           
                                    SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.SLOTNO[0]
                                elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'POINT':                           
                                    SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.POINT[0]
                                elif str(SVM_ONLINE_PARAMETER_null.PARAMETER[k]).upper() == 'FILTER_FEATURE':                           
                                    SVM_ONLINE_PARAMETER_null.PARAM_VALUE[k] = SVM_PROJECT_RUN.FILTER_FEATURE[0]
                            
                            SVM_PARAMETER_DATA = pd.concat([SVM_ONLINE_PARAMETER_notnull, SVM_ONLINE_PARAMETER_null], axis=0, ignore_index=True)    
                            SVM_PARAMETER_DATA = SVM_PARAMETER_DATA.sort_values("SEQ").reset_index(drop=True)
                            SVM_PARAMETER_DATA = SVM_PARAMETER_DATA[["OFFLINE_PARAMETER", "PARAM_VALUE"]]  
                                
                            if len(SVM_PARAMETER_DATA) == 0:
                                print('SVM_PARAMETER_DATA null')  #20190619 add inert sql in the future
                            else:
                                df_SVM_PARAMETER_DATA_lite_T = SVM_PARAMETER_DATA.T #20190619 行列互轉
                                headers = df_SVM_PARAMETER_DATA_lite_T.iloc[0] #20190619 first row set header
                                df_SVM_PARAMETER_DATA_lite_T_H  = pd.DataFrame(df_SVM_PARAMETER_DATA_lite_T.values[1:], columns=headers)
                                    
                                print("online x phase[ns]")
                                #input_path = read_path(path_dict['3']["3"])
                                #mylog = WriteLog(input_path["log_path"], input_path["error_path"])    
                                #20191009 avoid runindex duplicate error 
                                try:
                                    df_online_X, df_online_y = OnLine_Data_PreProcess(df_SVM_PARAMETER_DATA_lite_T_H, path_dict['3']["3"], check_include_y=True)
                                except Exception as e:
                                    errMsg = ProcEx.ProcessException(e) # Reaver
                                    APLog.WriteApLog( str(SVM_PROJECT_RUN.RUNINDEX[0]) + "," + errMsg , projectid) # Reaver
                                    #mylog.error(e)
                                    #mylog.error(SVM_PROJECT_RUN.RUNINDEX[0])
                                    continue
                                
                                XDI_online_value, XDI_Threshold, XDI_Check, df_contri = OnLine_XDI(df_online_X, path_dict['4']["3"])
                                df_YDI_online = OnLine_YDI(df_online_X, df_online_y, path_dict['5']["3"])
                                y_pred, y_base = self.model_predict_online_x(df_online_X, path_dict, projectid)
                                #20190912 add CI old/new                  
                                if int(projectid) > 570:
                                    MXCI_value, MXCI_T, MYCI_value, MYCI_T, light = CI_online(path_dict['8']["3"], df_online_X, y_pred, y_base)
                                else:
                                    MXCI_value, MXCI_T, MYCI_value, MYCI_T, light = CI_online_old(path_dict['8']["3"], df_online_X, y_pred, y_base)

                                try:
                                    SVM_PROJECT_MODEL = select_project_model_by_projectid_status2_filterfeature(projectid, "ABNORMAL", "ABNORMAL->ONLINE", SVM_PROJECT_RUN[FILTER_FEATURE][0])
                                except:
                                    SVM_PROJECT_MODEL = select_project_model_by_projectid_status2_filterfeature(projectid, "ABNORMAL", "ABNORMAL->ONLINE", SVM_PROJECT_RUN['FILTER_FEATURE'][0])
                        
                                if XDI_online_value is not None:
                                    SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'XDI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                    if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                        insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'XDI', XDI_online_value, SVM_PROJECT_MODEL.MODEL_ID[0], 1, server_name, db_name)                                
                                    else: 
                                        update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, XDI_online_value, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'XDI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                                          
                                if XDI_Threshold is not None:
                                    SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'XDI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                    if len(SVM_PROJECT_PREDICT_DATA) == 0:  
                                        insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'XDI_Threshold', XDI_Threshold, SVM_PROJECT_MODEL.MODEL_ID[0], 1, server_name, db_name)  
                                    else: 
                                        update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, XDI_Threshold, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'XDI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)                                                                                                                                     
                                
                                if y_pred is not None:
                                    SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Predict_Y', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                    if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                        insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Predict_Y', y_pred, SVM_PROJECT_MODEL.MODEL_ID[0], 1, server_name, db_name)                                  
                                    else: 
                                        update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, y_pred, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Predict_Y', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                
                                #20190808 insert y_base to prdeict_data
                                if y_base is not None:
                                    SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Baseline_Y', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                    if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                        insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Baseline_Y', y_base, SVM_PROJECT_MODEL.MODEL_ID[0], 1, server_name, db_name)                                  
                                    else: 
                                        update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, y_base, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Baseline_Y', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                
                                if MXCI_value is not None:
                                    SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MXCI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                    if len(SVM_PROJECT_PREDICT_DATA) == 0:   
                                        insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MXCI', MXCI_value, SVM_PROJECT_MODEL.MODEL_ID[0], 1, server_name, db_name)  
                                    else: 
                                        update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MXCI_value, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MXCI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                    
                                if MXCI_T is not None:
                                    SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MXCI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                    if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                        insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MXCI_Threshold', MXCI_T, SVM_PROJECT_MODEL.MODEL_ID[0], 1, server_name, db_name)                                    
                                    else: 
                                        update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MXCI_T, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MXCI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                    
                                if MYCI_value is not None:
                                    SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MYCI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                    #MYCI_value = float(str(MYCI_value)[0:10]) #20190911 not additional to change digits
                                    MYCI_value = round(MYCI_value,50)
                                    if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                        insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MYCI', MYCI_value, SVM_PROJECT_MODEL.MODEL_ID[0], 1, server_name, db_name)                                   
                                    else: 
                                        update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MYCI_value, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MYCI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                    
                                if MYCI_T is not None:
                                    SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MYCI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                    if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                        insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MYCI_Threshold', MYCI_T, SVM_PROJECT_MODEL.MODEL_ID[0], 1, server_name, db_name)                                    
                                    else: 
                                        update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MYCI_T, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MYCI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                
                                #20190923 modify isretrainpredict -> 1
                                if df_YDI_online is not None:
                                    SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Interval', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                    if len(SVM_PROJECT_PREDICT_DATA) == 0:  
                                        insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Interval', df_YDI_online.Interval[0], SVM_PROJECT_MODEL.MODEL_ID[0], 1, server_name, db_name)                              
                                    else: 
                                        update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Interval[0], 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Interval', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                    
                                    SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'YDI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                    if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                        insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'YDI', df_YDI_online.YDI[0], SVM_PROJECT_MODEL.MODEL_ID[0], 1, server_name, db_name)                                           
                                    else: 
                                        update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.YDI[0], 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'YDI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                    
                                    SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'YDI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                    if len(SVM_PROJECT_PREDICT_DATA) == 0:   
                                        insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'YDI_Threshold', df_YDI_online.YDI_Threshold[0], SVM_PROJECT_MODEL.MODEL_ID[0], 1, server_name, db_name)                                   
                                    else: 
                                        update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.YDI_Threshold[0], 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'YDI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                    
                                    SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Y_avg', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                    if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                        insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Y_avg', df_YDI_online.Y_avg[0], SVM_PROJECT_MODEL.MODEL_ID[0], 1, server_name, db_name)                                                  
                                    else: 
                                        update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Y_avg[0], 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Y_avg', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                    
                                    SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Cluster_idx', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                                    if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                        insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Cluster_idx', df_YDI_online.Cluster_idx[0], SVM_PROJECT_MODEL.MODEL_ID[0], 1, server_name, db_name)                                                  
                                    else: 
                                        update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Cluster_idx[0], 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Cluster_idx', SVM_PROJECT_MODEL.MODEL_ID[0], server_name, db_name)
                
                #20190808 have/no real y will update predict result to True                                    
                update_project_model_predictresult_by_modelid("True", SVM_PROJECT_MODEL.MODEL_ID[0])               
        return None

        
    def many_to_many_online_xy(self, projectid, retrain_num, server_name, db_name):
        #self.batch_data = batch_data
        retrain_num = self.get_max_retrain_num(retrain_num=retrain_num)

        if self.filter_feature is None:
            self.get_filter_feature()

        if self.feature_lists is None:
            self.get_feature_content()

        if self.filter_dir_name is None:
            self.filter_dir_name = {}
            for feature in self.feature_lists:
                self.filter_dir_name[feature] = self.base_name + "_" + self.filter_feature + "_" + str(feature)
        
        self.get_saved_path_config() #20190715 get path_config
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=None)
        self.online_xy_many_phase(projectid, server_name, db_name, retrain_num=retrain_num, batch_num=batch_num)

        return None
  
if __name__ == '__main__':        
    #20190627 online data must go 10.96.18.199, get SERVER_NAME to the server
    def timer(n):
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        SVM_PROJECT_MTM = select_project_mtm_by_onlineserver(ip) #20190828 onlone server must meets the local ip
        run_number = 0 #20190829 add run count
        while True:
            p1 = mp.Pool(processes=8)  # 創建8條進程, wait all processes finish and go next cycle 
                                              
            if len(SVM_PROJECT_MTM) != 0:
                
                for a in range(len(SVM_PROJECT_MTM)):                    
                    
                    projectid = SVM_PROJECT_MTM.PROJECT_ID[a]  
                    SVM_PROJECT_MODEL_MTM = select_project_model_by_projectid_predictresult(projectid, "False") 
                    
                    if len(SVM_PROJECT_MODEL_MTM) != 0:
                                          
                        SVM = SuperVM(str(SVM_PROJECT_MTM.PROJECT_NAME[a]), str(SVM_PROJECT_MTM.UPLOAD_FILE[a]), r"Config.json")                                                              
                        retrain_num = int(SVM_PROJECT_MODEL_MTM.MODEL_SEQ[0])
                        server_name = SVM_PROJECT_MTM.SERVER_NAME[a]
                        db_name = SVM_PROJECT_MTM.DB_NAME[a]
                        
                        print("--------------")
                        print(projectid)
                        print(retrain_num)
                        print(server_name)
                        print(db_name)
                        print("--------------")
                        
                        r = p1.apply_async(SVM.many_to_many_online_xy, (projectid, retrain_num, server_name, db_name))
                    
                    else:
                        print("SVM_PROJECT_MODEL_MTM None")
            
            p1.close()
            p1.join()  # 等待進程池中的事件執行完畢，回收進程池
            
            run_number = run_number+1
            print("Round " + str(run_number))
            
            #20190829 add run count, 3600s scan db 
            if run_number == 600:
                SVM_PROJECT_MTM = select_project_mtm_by_onlineserver(ip) 
                run_number = 0
                                   
            time.sleep(n)
    timer(3) 
