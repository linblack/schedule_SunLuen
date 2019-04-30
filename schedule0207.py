#main_CVD2E.py
# -*- coding: utf-8 -*-
from Parameter_Review import Param_Review
from Data_PreProcess import Data_PreProcess_Train, Data_PreProcess_Test
#from AskExclude import AskExclude
from XDI import XDI_off_line_report, Build_XDI_Model 
from YDI import YDI_off_line_report, Build_YDI_Model 
from MXCI_MYCI_pre import pre_MXCI_MYCI
from MXCI_MYCI import MXCI_MYCI_offline
from Model_Selection import Model_Selection
import os
from Path import All_path

from shutil import copyfile 
import pandas as pd
from XGB_model import xgb_execution


if __name__ == '__main__':
    ### Vriables ###
    sorce_dir = "CVD2E_Split1_Test"
    
    #####################################  
    base_path = os.path.join("../Cases/", sorce_dir)    #base_path = ../Cases/CVD2E_Split1_Test
    
    path = All_path(base_path)
    
    path.dir_init()
    
    ### 02_ParameterReview/ 
    Param_Review(path.path_02)    
    #input
    #"config_path": "../Cases/CVD2E_Split1_Test\\00_config/config.json",
    #"error_path": "../Cases/CVD2E_Split1_Test\\02_ParameterReview/error.log",
    #"log_path": "../Cases/CVD2E_Split1_Test\\99_LOG/System.log",
    #"raw_path": "C:\\Users\\BlakeLinAres\\Cases\\CVD2E_Split1_Test\\01_OriginalData\\raw_data.csv"
    
    #output
    #output_path["missing_path"] = os.path.join(folder_path, "missing_rate_list.csv")
    #output_path["outspec_path"] = os.path.join(folder_path, "outspec_table.csv")
    #output_path["summary_path"] = os.path.join(folder_path, "summary_table.csv")
    #Parameter_Review.png
    #PC_PRESSURE_MIN_STEP2.png
    #PC_PRESSURE_MIN_STEP4.png
    #####################################

    
    ### Ask if any cols need to be excluded
#    AskExclude(base_path)
    #####################################
    
    ### 03_PreprocessedData/
    Data_PreProcess_Train(path.path_03)
    Data_PreProcess_Test(path.path_03)
    #input
    #"config_path": "../Cases/CVD2E_Split1_Test\\00_config/config.json",
    #"error_path": "../Cases/CVD2E_Split1_Test\\03_PreprocessedData/error.log",
    #"log_path": "../Cases/CVD2E_Split1_Test\\99_LOG/System.log",
    #"raw_path": "../Cases/CVD2E_Split1_Test\\01_OriginalData/raw_data.csv"
    
    #output
    #output_path["data_train_path"] = os.path.join(folder_path, 'Data_Train.csv')
    #output_path["data_test_path"] = os.path.join(folder_path, 'Data_Test.csv')
    #output_path["x_train_path"] = os.path.join(folder_path, 'x_Train.csv')
    #output_path["x_test_path"] = os.path.join(folder_path, 'x_Test.csv')
    #output_path["y_train_path"] = os.path.join(folder_path, 'y_Train.csv')
    #output_path["y_test_path"] = os.path.join(folder_path, 'y_Test.csv')
    #####################################
    
    
    ### 04_XDI/
    Build_XDI_Model(path.path_04)
    XDI_off_line_report(path.path_04)
    #input
    #"config_path": "../Cases/CVD2E_Split1_Test\\00_config/config.json",
    #"error_path": "../Cases/CVD2E_Split1_Test\\04_XDI/error.log",
    #"log_path": "../Cases/CVD2E_Split1_Test\\99_LOG/System.log",
    #"x_test_path": "../Cases/CVD2E_Split1_Test\\03_PreprocessedData/x_Test.csv",
    #"x_train_path": "../Cases/CVD2E_Split1_Test\\03_PreprocessedData/x_Train.csv"
    
    #output
    #output_path["XDI_PCA_path"] = os.path.join(data_folder, "XDI_PCA.pkl")     
    #output_path["XDI_Data_transform_path"] = os.path.join(data_folder, "XDI_Data_transform.pkl") 
    #output_path["XDI_offline_pic_path"] = os.path.join(data_folder, "XDI.png") 
    #output_path["XDI_offline_path"] = os.path.join(data_folder, "XDI_offline.csv") 
    #output_path["XYDI_Data_transform_path"] = os.path.join(data_folder, "XYDI_Data_transform.pkl")     
    #####################################
    
    
    ### 05_YDI/
    Build_YDI_Model(path.path_05)
    YDI_off_line_report(path.path_05)
    #input
    #"XDI_PCA_path": "../Cases/CVD2E_Split1_Test\\04_XDI/XDI_PCA.pkl",
    #"XYDI_Data_transform_path": "../Cases/CVD2E_Split1_Test\\04_XDI/XYDI_Data_transform.pkl",
    #"config_path": "../Cases/CVD2E_Split1_Test\\00_config/config.json",
    #"error_path": "../Cases/CVD2E_Split1_Test\\05_YDI/error.log",
    #"log_path": "../Cases/CVD2E_Split1_Test\\99_LOG/System.log",
    #"x_test_path": "../Cases/CVD2E_Split1_Test\\03_PreprocessedData/x_Test.csv",
    #"x_train_path": "../Cases/CVD2E_Split1_Test\\03_PreprocessedData/x_Train.csv",
    #"y_test_path": "../Cases/CVD2E_Split1_Test\\03_PreprocessedData/y_Test.csv",
    #"y_train_path": "../Cases/CVD2E_Split1_Test\\03_PreprocessedData/y_Train.csv"
    
    #output
    #output_path["YDI_offline_pic_path"] = os.path.join(data_folder, "YDI.png")     
    #output_path["YDI_threshold_table_path"] = os.path.join(data_folder, "YDI_threshold_table.csv") 
    #output_path["YDI_Clustering_path"] = os.path.join(data_folder, "YDI_Clustering.pkl") 
    #output_path["YDI_offline_path"] = os.path.join(data_folder, "YDI_offline.csv") 
    #output_path["YDI_Clustering_result_path"] = os.path.join(data_folder, "YDI_Clustering/") 
    #####################################
    
    
#    ## Model1   
    xgb_execution(path.model_dir["XGB"])
    #input
    #"config_path": "../Cases/CVD2E_Split1_Test\\00_config/config.json",
    #"error_path": "../Cases/CVD2E_Split1_Test\\06_Model/XGB\\error.log",
    #"log_path": "../Cases/CVD2E_Split1_Test\\99_LOG/System.log",
    #"x_test_path": "../Cases/CVD2E_Split1_Test\\03_PreprocessedData/x_Test.csv",
    #"x_train_path": "../Cases/CVD2E_Split1_Test\\03_PreprocessedData/x_Train.csv",
    #"y_test_path": "../Cases/CVD2E_Split1_Test\\03_PreprocessedData/y_Test.csv",
    #"y_train_path": "../Cases/CVD2E_Split1_Test\\03_PreprocessedData/y_Train.csv"
    
    #output
    #output_path["Param_aftertuning_path"] = os.path.join(data_folder, "Param_aftertuning.csv")     
    #output_path["FeatureScore_path"] = os.path.join(data_folder, "FeatureScore.csv") 
    #output_path["Importance10_path"] = os.path.join(data_folder, "Importance10.jpg") 
    #output_path["Importance30_path"] = os.path.join(data_folder, "Importance30.jpg") 
    #output_path["testPredResult_path"] = os.path.join(data_folder, "testPredResult.csv")
    #output_path["trainPredResult_path"] = os.path.join(data_folder, "trainPredResult.csv") 
    #output_path["ModelSave_path"] = os.path.join(data_folder, "XGB.model") 
    """
    model_1_train = os.path.join(base_path, "backup/y_Train_XXB_pred.csv")
    model_1_test = os.path.join(base_path, "backup/y_Test_XXB_pred.csv")
    model_1_train_path = os.path.join(path.model_dir["XGB"], "trainPredResult.csv")
    model_1_test_path = os.path.join(path.model_dir["XGB"], "testPredResult.csv")
    copyfile(model_1_train, model_1_train_path)
    copyfile(model_1_test, model_1_test_path)
    
    model_1_train = pd.read_csv(model_1_train_path)
    model_1_test = pd.read_csv(model_1_test_path)   
    #xgb_execution(path.path_06 + 'XGB')
    """
#    ####################################


#    ## Model2
    model_2_train = os.path.join(base_path, "backup/y_Train_PPL_pred.csv")
    model_2_test = os.path.join(base_path, "backup/y_Test_PPL_pred.csv")
    model_2_train_path = os.path.join(path.model_dir["PLS"], "trainPredResult.csv")
    model_2_test_path = os.path.join(path.model_dir["PLS"], "testPredResult.csv")
    copyfile(model_2_train, model_2_train_path)
    copyfile(model_2_test, model_2_test_path)
    
    model_2_train = pd.read_csv(model_2_train_path)
    model_2_test = pd.read_csv(model_2_test_path)        
    ####################################
    
    # 08_SelectModel/
    ## Decide whether to upload the model
    Model_Selection(path.path_07)
    #input
    #"Model_Folder": "../Cases/CVD2E_Split1_Test\\06_Model/",
    #"config_path": "../Cases/CVD2E_Split1_Test\\00_config/config.json",
    #"error_path": "../Cases/CVD2E_Split1_Test\\07_SelectModel/error.log",
    #"log_path": "../Cases/CVD2E_Split1_Test\\99_LOG/System.log",
    #"x_test_path": "../Cases/CVD2E_Split1_Test\\03_PreprocessedData/x_Test.csv",
    #"x_train_path": "../Cases/CVD2E_Split1_Test\\03_PreprocessedData/x_Train.csv",
    #"y_test_path": "../Cases/CVD2E_Split1_Test\\03_PreprocessedData/y_Test.csv",
    #"y_train_path": "../Cases/CVD2E_Split1_Test\\03_PreprocessedData/y_Train.csv"
    
    #output
    #output_path["y_pred_merge_train"] = os.path.join(folder_path, "y_pred_merge_train.csv")
    #output_path["y_pred_merge_test"] = os.path.join(folder_path, "y_pred_merge_test.csv")
    #output_path["Model_Selection_Report"] = os.path.join(folder_path, "Report.csv")
    #####################################   
    
    # 09_CI/
    pre_MXCI_MYCI(path.path_08)
    MXCI_MYCI_offline(path.path_08)
    #input
    #"Model_Folder": "../Cases/CVD2E_Split1_Test\\06_Model/",
    #"config_path": "../Cases/CVD2E_Split1_Test\\00_config/config.json",
    #"error_path": "../Cases/CVD2E_Split1_Test\\07_SelectModel/error.log",
    #"log_path": "../Cases/CVD2E_Split1_Test\\99_LOG/System.log",
    #"x_test_path": "../Cases/CVD2E_Split1_Test\\03_PreprocessedData/x_Test.csv",
    #"x_train_path": "../Cases/CVD2E_Split1_Test\\03_PreprocessedData/x_Train.csv",
    #"y_test_path": "../Cases/CVD2E_Split1_Test\\03_PreprocessedData/y_Test.csv",
    #"y_train_path": "../Cases/CVD2E_Split1_Test\\03_PreprocessedData/y_Train.csv"
    
    #output
    #output_path["MXCI_fig_path"] = os.path.join(data_folder, "MXCI_offline.png")
    #output_path["MYCI_fig_path"] = os.path.join(data_folder, "MYCI_offline.png")
    #output_path["light_fig_path"] = os.path.join(data_folder, "MXCI_MYCI_light_offline.png")
    #output_path["MXCI_MYCI_offline_path"] = os.path.join(data_folder, "MXCI_MYCI_offline.csv")
    #output_path["MXCI_x_train_path"] = os.path.join(data_folder, "MXCI_x_train.pkl")
    ##################################### 
### Training END #####

################################################################################################
#Path.py
# -*- coding: utf-8 -*-
import os, json
from itertools import compress
import traceback



class All_path():
    def __init__(self, data_folder):
        self.data_folder = data_folder #data_folder = Cases/CVD2E_Split1_Test

        ######
        self.raw_data_default_name = "raw_data.csv"
        self.config_default_name = "config.json"
        self.raw_name_2 = os.path.join(self.data_folder, self.raw_data_default_name)
        self.config_name_2 = os.path.join(self.data_folder, self.config_default_name)
        
        self.path_00 = os.path.join(self.data_folder, "00_config/")
        self.path_01 = os.path.join(self.data_folder, "01_OriginalData/")
        self.path_02 = os.path.join(self.data_folder, "02_ParameterReview/")
        self.path_03 = os.path.join(self.data_folder, "03_PreprocessedData/")
        self.path_04 = os.path.join(self.data_folder, "04_XDI/")
        self.path_05 = os.path.join(self.data_folder, "05_YDI/")
        self.path_06 = os.path.join(self.data_folder, "06_Model/")
        self.path_07 = os.path.join(self.data_folder, "07_SelectModel/")
        self.path_08 = os.path.join(self.data_folder, "08_CI/")
        self.path_99 = os.path.join(self.data_folder, "99_LOG/")
        
        # 00_config/
        self.config_path = os.path.join(self.path_00, "config.json")
        
        # 01_OriginalData/
        self.raw_path =  os.path.join(self.path_01, "raw_data.csv")

        # 02_ParameterReview/     
#        self.missing_path = os.path.join(self.path_02, "missing_rate_list.csv")
#        self.outspec_path = os.path.join(self.path_02, "outspec_table.csv")
#        self.summary_path = os.path.join(self.path_02, "summary_table.csv")

        # 03_PreprocessedData/
#        self.data_train_path = os.path.join(self.path_03, 'Data_Train.csv')
#        self.data_test_path  = os.path.join(self.path_03, 'Data_Test.csv')
        self.x_train_path  = os.path.join(self.path_03, 'x_Train.csv')
        self.x_test_path   = os.path.join(self.path_03, 'x_Test.csv' )
        self.y_train_path  = os.path.join(self.path_03, 'y_Train.csv')
        self.y_test_path   = os.path.join(self.path_03, 'y_Test.csv' )
        
        # 04_XDI/
        self.XDI_PCA_path            = os.path.join(self.path_04, "XDI_PCA.pkl")
        self.XDI_Data_transform_path = os.path.join(self.path_04, "XDI_Data_transform.pkl")
        self.XDI_offline_pic_path = os.path.join(self.path_04, "XDI.png")
        self.XDI_offline_path        = os.path.join(self.path_04, "XDI_offline.csv")

        # 05_YDI/
        self.YDI_offline_pic_path = os.path.join(self.path_05, "YDI.png")
        self.YDI_threshold_table_path = os.path.join(self.path_05, "YDI_threshold_table.csv")
        self.YDI_Clustering_path = os.path.join(self.path_05, "YDI_Clustering.pkl")
        self.YDI_offline_path        = os.path.join(self.path_05, "YDI_offline.csv")
        self.YDI_Clustering_result_path = os.path.join(self.path_05, "YDI_Clustering/")       
            
        self.XYDI_Data_transform_path = os.path.join(self.path_04, "XYDI_Data_transform.pkl")

        # 06_Model         
#        model_pred_name = config["Model_pred_name"]
#        self.model_dir = {}
#        for name in model_pred_name:
#            self.model_dir[name] = os.path.join(self.path_07, name)
#            self.model_file[name] = {}
               
        # 07_SelectModel/
        self.y_pred_merge_train = os.path.join(self.path_07, "y_Pred_merge_train.csv")
        self.y_pred_merge_test = os.path.join(self.path_07, "y_Pred_merge_test.csv")
        
        # 08_CI/        
        self.MXCI_fig_path = os.path.join(self.path_08, "MXCI_offline.png")
        self.MYCI_fig_path = os.path.join(self.path_08, "MYCI_offline.png")
        self.light_fig_path = os.path.join(self.path_08, "MXCI_MYCI_light_offline.png")
        self.MXCI_MYCI_offline_path = os.path.join(self.path_08, "MXCI_MYCI_offline.csv")
        self.MXCI_x_train_path = os.path.join(self.path_08, "MXCI_x_train.pkl")

        # 99_LOG/    
        self.sys_log_path = os.path.join(self.path_99, "System.log")
        
        return None
 
    def exist_file_check(self):
        # check if config file exists, config_sample_split1.json & T75R4_combine_PM1.csv         
        config_check_mask = [x.endswith(".json") for x in os.listdir(self.data_folder)]
        config_check_list = list(compress(os.listdir(self.data_folder), config_check_mask))
        if len(config_check_list)==0:
            raise FileNotFoundError("Config file is not found in "+str(self.data_folder))
        elif len(config_check_list)>1:
            raise FileExistsError("More than one config file is found in "+str(self.data_folder))
        else:
            print("Found config file: "+config_check_list[0])

        # check if raw file exists        
        file_name_mask = [x.endswith(".csv") for x in os.listdir(self.data_folder)]
        file_name_list = list(compress(os.listdir(self.data_folder), file_name_mask)) # filter file extension with ".csv"
        if len(file_name_list)==0:
            raise FileNotFoundError("Raw data is not found in "+str(self.data_folder))
        elif len(file_name_list)>1:
            raise FileExistsError("More than one raw data is found in "+str(self.data_folder))
        else:
            print("Found raw data: "+file_name_list[0])
            
        raw_name_1 = os.path.join(self.data_folder, file_name_list[0])
        os.rename(raw_name_1, self.raw_name_2)
        
        config_name_1 = os.path.join(self.data_folder, config_check_list[0])
        os.rename(config_name_1, self.config_name_2)    
        
        return None    
  
    
    def init_folders(self):
        try:            
            with open(self.config_name_2) as json_data:
                config = json.load(json_data)
        except Exception as e:
            error_path = os.path.join(self.data_folder, "error.log")
            with open(error_path, 'a') as file:
                file.write("Error while reading config file")
                file.write(e)
                file.write(traceback.format_exc())            

        model_pred_name = config["Model_Pred_Name"]
        self.model_dir = {}
        for name in model_pred_name:
            self.model_dir[name] = os.path.join(self.path_06, name)
        # e.g.  path.model_dir["XGB"]
        
        path_list = [self.path_00, self.path_01, self.path_02, self.path_03, self.path_04, self.path_05, self.path_06,
                     self.path_07, self.path_08, self.path_99]
        other_dir = [self.YDI_Clustering_result_path]
        for value in self.model_dir.values():
            other_dir.append(value)

        path_list.extend(other_dir)
        for path in path_list:
            if not os.path.exists(path):
                os.makedirs(path)
        
        raw_path = os.path.join(self.path_01, self.raw_data_default_name)
        config_path = os.path.join(self.path_00, self.config_default_name)
        try:
            os.rename(self.raw_name_2, raw_path)
            os.rename(self.config_name_2, config_path)
        except Exception as e:
            error_path = os.path.join(self.data_folder, "error.log")
            with open(error_path, 'a') as file:
                file.write("Fail to rename file:")
                file.write(e)
                file.write(traceback.format_exc())            
            raise
        return None

    def create_path_files_init(self):
        self.file_path = {}
        self.file_path["config_path"] = self.config_path
        self.file_path["log_path"] = self.sys_log_path
        return None
         
    def create_path_files_save(self, path):
        self.file_path["error_path"] = os.path.join(path, "error.log")
        in_path = os.path.join(path, "file_path.json")
        with open(in_path , 'w') as fp:
            json.dump(self.file_path, fp, indent=4, sort_keys=True)
        return None
    
    def create_path_files(self):    
        # 02_ParameterReview/ 
        self.create_path_files_init()
        self.file_path["raw_path"] = os.path.abspath(self.raw_path)      
        self.create_path_files_save(self.path_02)
        
        
        # 03_PreprocessedData/
        self.create_path_files_init()
        self.file_path["raw_path"] = self.raw_path       
        self.create_path_files_save(self.path_03)
        
        
        # 04_XDI/
        self.create_path_files_init() 
        self.file_path["x_train_path"] = self.x_train_path
        self.file_path["x_test_path"] = self.x_test_path     
        self.create_path_files_save(self.path_04)
        
        
        # 05_YDI/
        self.create_path_files_init() 
        self.file_path["x_train_path"] = self.x_train_path
        self.file_path["x_test_path"] = self.x_test_path
        self.file_path["y_train_path"] = self.y_train_path
        self.file_path["y_test_path"] = self.y_test_path
        self.file_path["XDI_PCA_path"] = self.XDI_PCA_path
        self.file_path["XYDI_Data_transform_path"] = self.XYDI_Data_transform_path       
        self.create_path_files_save(self.path_05)     
        
        # 06_Model/
        for path in self.model_dir.keys():
            self.create_path_files_init() 
            self.file_path["x_train_path"] = self.x_train_path
            self.file_path["x_test_path"] = self.x_test_path
            self.file_path["y_train_path"] = self.y_train_path
            self.file_path["y_test_path"] = self.y_test_path
            self.create_path_files_save(self.model_dir[path])

        
        # 07_SelectModel/
        self.create_path_files_init()
        self.file_path["Model_Folder"] = self.path_06
        self.file_path["x_train_path"] = self.x_train_path
        self.file_path["x_test_path"] = self.x_test_path
        self.file_path["y_train_path"] = self.y_train_path
        self.file_path["y_test_path"] = self.y_test_path        
        self.create_path_files_save(self.path_07)
        
        # 08_CI/        
        self.create_path_files_init()
        self.file_path["x_train_path"] = self.x_train_path
        self.file_path["x_test_path"] = self.x_test_path
        self.file_path["y_pred_merge_train"] = self.y_pred_merge_train
        self.file_path["y_pred_merge_test"] = self.y_pred_merge_test   
        self.create_path_files_save(self.path_08)
    
    
    def dir_init(self):
        self.exist_file_check()
        self.init_folders()
        self.create_path_files()
        return None
    
########################################################################    
#Parameter_Review.py
# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
from scipy.stats import kurtosis, skew, entropy,linregress 
from Read_path import read_path
import re
from config import read_config
from CreateLog import WriteLog
plt.style.use("ggplot")


def Entropy(labels):
    # 计算概率分布
    probs = pd.Series(labels).value_counts() / len(labels)
    # 计算底数为base的熵
    en = entropy(probs)
    return en


def mean_avg_not_overlap(x, window):
    size = x.shape[0]
    step = size/window
    mean_avg = []
    for i in range(0,int(step)):
        init = i*window
        mean_avg.append(x[init:init+window].mean())
    return np.array(mean_avg)



def Param_Review(folder_path):
    ####### output path #######
    output_path = {}
    output_path["missing_path"] = os.path.join(folder_path, "missing_rate_list.csv")
    output_path["outspec_path"] = os.path.join(folder_path, "outspec_table.csv")
    output_path["summary_path"] = os.path.join(folder_path, "summary_table.csv")
    ############################
    
    input_path = read_path(folder_path)
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    mylog.init_logger()
    config = read_config(input_path["config_path"], mylog)
    
    mylog.info("-----Parameter Review------")
    
    std_T = config["ParamReview"]["std_T"]
    outspec_T = config["ParamReview"]["outspec_T"]
    missing_T = config["Remove_Missing_Value"]["Threshold"]
    exclude_col = config["Exclude_columns"]
    window = config["ParamReview"]["window"]
    
    try:
        df_1 = pd.read_csv(input_path["raw_path"], encoding="utf-8")
    except Exception as e:
        mylog.error("Para Review: Read raw data error")
        mylog.error_trace(e)

    
    ### Check for Chinese    
    check=False
    mylog.info("-----Parameter Review-----")
    mylog.info("**Check for Chinese characters**")
    cols = df_1.columns
    for col in cols:
#        if re.search(u'[\u4e00-\u9fff]', col):
        if re.search(u'[\u2E80-\u2FD5\u3190-\u319f\u3400-\u4DBF\u4E00-\u9FCC\uF900-\uFAAD]', col):
            check = True
            new_str = ""
            for string in col:
                if re.search(u'[\u2E80-\u2FD5\u3190-\u319f\u3400-\u4DBF\u4E00-\u9FCC\uF900-\uFAAD]', string):
                    new_str+="XX"
                else:
                    new_str+=string
            mylog.info(col+" ---> "+new_str)
            df_1 = df_1.rename(columns={col : new_str})
    mylog.info("**Done**")
    if check:
        try:
            df_1.to_csv(input_path["raw_path"], index=False)
        except Exception as e:
            mylog.error("Para Review: Error while saving raw data")
            mylog.error_trace(e)
              

    
    ### Three std
    try:
        if exclude_col:
            df_1 = df_1.drop(exclude_col, axis=1)
    except Exception as e:
        mylog.error("Para Review: Error while excluding features")
        mylog.error_trace(e)
        
    
    total_row_num = df_1.values.shape[0]
    missing_list = []
    ### drop cols that are type of object and Tell if col has NaN
#    print("Check missing values:")
    missing_table = pd.DataFrame(columns=["features", "missing rate"])
    not_num_col = list(df_1.select_dtypes(exclude=['number']).columns)
    for col in df_1.columns:
#        if df_1.loc[:,col].infer_objects().dtypes == "object":
#            object_col.append(col)
#            continue
        if col in not_num_col:
            continue
        nan_cnt = sum(df_1.loc[:,col].isna())
        missing_table.loc[col, "features"] = col
        missing_table.loc[col, "missing rate"] = nan_cnt/total_row_num
        if nan_cnt/total_row_num > missing_T:
            missing_list.append(col)

    try:          
        missing_table.to_csv(output_path["missing_path"], index=False)
    except Exception as e:
        mylog.warning("Para Review: Error while saving missing rate table")
        mylog.warning_trace(e)
            
    mylog.info("< Note that missing values are ignored and high-missing-rate features will be dropped during parameter review>")
    if exclude_col:
        mylog.info('Features that are specified to be excluded:')
        mylog.info(','.join(str(e) for e in exclude_col))
        mylog.info("")
    if not_num_col: 
        mylog.info('Features that are not numbers:')
        df_tmp = df_1[not_num_col]
        object_col = list(df_tmp.select_dtypes(include=['object']).columns)
        datetime_col = list(df_tmp.select_dtypes(include=['datetime']).columns)
        timedelta_col = list(df_tmp.select_dtypes(include=['timedelta']).columns)
        cat_col = list(df_tmp.select_dtypes(include=['category']).columns)
        others = list(df_tmp.select_dtypes(exclude=['object', 'datetime', 'timedelta', 'category']).columns)
        if object_col:
            mylog.info("--object--")
            mylog.info(','.join(str(e) for e in object_col))
        if datetime_col:
            mylog.info("--datetime--")
            mylog.info(','.join(str(e) for e in datetime_col))
        if timedelta_col:
            mylog.info("--timedelta--")
            mylog.info(','.join(str(e) for e in timedelta_col))
        if cat_col:          
            mylog.info("--category--")
            mylog.info(','.join(str(e) for e in cat_col))
        if others:
            mylog.info("--others--")
            mylog.info(','.join(str(e) for e in others))
        mylog.info("")
        
    if missing_list: 
        mylog.info('Features that have too many missing values:')
        mylog.info(','.join(str(e) for e in missing_list))
        mylog.info("")
    try:
        if not_num_col:
            df_1 = df_1.drop(not_num_col, axis=1)
    except Exception as e:
        mylog.error("Para Review: Error while dropping object-type features")
        object_str = ','.join(str(e) for e in not_num_col)
        mylog.error("Please check the type of these features:"+ object_str)
        mylog.error_trace(e)
        
    
    try:
        if missing_list:
            df_1 = df_1.drop(missing_list, axis=1)
    except Exception as e:
        mylog.error("Para Review: Error while dropping high-missing-rate features")
        missing_str = ','.join(str(e) for e in missing_list)
        mylog.error("Please check the these features:"+ missing_str)
        mylog.error_trace(e)


#    potential_out = []
    summary = pd.DataFrame(columns=["mean", "std", "num_out", "num_out(%)", "skewness", "kurtosis", "entropy"])
    for col in df_1.columns:
        try:
            param_col = df_1.loc[:,col].dropna()
            std = param_col.std()
            mean = param_col.mean()
            summary.loc[col, "skewness"] = skew(param_col)
            summary.loc[col, "kurtosis"] = kurtosis(param_col)
            summary.loc[col, "entropy"] = Entropy(param_col)
            summary.loc[col, "mean"] = mean
            summary.loc[col, "std"] = std
        
            num_out = 0
            for row in param_col:
                if abs(row - mean) > std_T*std:
                    num_out+=1
    #        if num_out!=0:
    #            potential_out.append(col)
            summary.loc[col, "num_out"] = num_out
            num_total = len(param_col.index.tolist())
            summary.loc[col, "num_out(%)"] = num_out/num_total*100
        except Exception as e:
            mylog.error("Para Review: Error while caluculating properties of "+str(col))
            mylog.error_trace(e)
            
        
    try:
        summary_out = summary[summary["num_out(%)"] > outspec_T*100]\
                    .sort_values(["num_out(%)"], ascending=False)\
                    .reset_index().apply(pd.to_numeric, errors='ignore')
    except Exception as e:
        mylog.error("Para Review: Error while reducing summary table")
        mylog.error_trace(e)
    

    for col in summary_out["index"].tolist():
        try:
            fig, ax = plt.subplots(figsize=(10,6))
            percent_str = "{0:.2f}".format(summary_out[summary_out["index"] == col]["num_out(%)"].values[0])
            ax.set_title(col+" ("+percent_str+" %)")
            sns.distplot(df_1[col].dropna(), ax=ax, kde=False)
            tmp_path = os.path.join(folder_path, col+".png")
            plt.savefig(tmp_path)
        except Exception as e:
            mylog.warning("Para Review: Error while creating outspec figures")
            mylog.warning_trace(e)

    
    try:
        number_table = pd.DataFrame(columns=["Number of parameters"], 
                                    data = summary_out["num_out(%)"].value_counts().values,
                                   index = summary_out["num_out(%)"].value_counts().index).sort_index().reset_index()
        number_table.rename(columns={"index": "Outspec Ratio (%)"}, inplace=True)
    except Exception as e:
        mylog.warning("Para Review: Error while creating outspec table")
        mylog.warning_trace(e)
    
    try:
        number_table.to_csv(output_path["outspec_path"],index=False)
    except Exception as e:
        mylog.warning("Para Review: Error while saving outspec table")
        mylog.warning_trace(e)    
        
    try:
        fig, ax = plt.subplots(figsize=(10,6))
        sns.barplot(x="Outspec Ratio (%)", y="Number of parameters", data=number_table, ax=ax);
        ax.set_xticklabels(labels=np.around(number_table["Outspec Ratio (%)"].values.tolist(), decimals=2))
        tmp_path = os.path.join(folder_path, "Parameter_Review.png")
        plt.savefig(tmp_path)
    except Exception as e:
        mylog.warning("Para Review: Error while creating Parameter Review figure")
        mylog.warning_trace(e)
              
    
    num_out_idx = summary_out["num_out(%)"].unique().tolist()
    num_out_dict = summary_out.groupby("num_out(%)")["index"].apply(dict) # saved as dictionary
    
    for idx in num_out_idx:
        mylog.info("Features that have "+"{0:.2f}".format(idx)+"% data out of "+str(std_T)+" std :")
        mylog.info(','.join(str(e) for e in num_out_dict[idx].tolist()))
        mylog.info("")

    try:
        summary_out.to_csv(output_path["summary_path"],index=False)
    except Exception as e:
        mylog.warning("Para Review: Error while saving summary")
        mylog.warning_trace(e)    
   
    ### constant
    const_params = []
    for col in df_1.columns:
        try:
            num_row = df_1.loc[:,col].dropna().values.shape[0]
            constant_entropy_limit = Entropy([0]*(num_row-1) + [1]*1)
            if summary.loc[col, "entropy"] < constant_entropy_limit:
                const_params.append(col)
        except Exception as e:
            mylog.warning("Para Review: Error while doing constant check of "+str(col))
            mylog.warning_trace(e)
        
    mylog.info("Total "+str(len(const_params))+" parameters are constant. List below:")
    mylog.info(','.join(str(e) for e in const_params))
    mylog.info("")
    

    ### trends
    cnt=0
    up = []
    dn = []
    
    for col in [elem for elem in df_1.columns if elem not in const_params]:
        try:
            ma = mean_avg_not_overlap(df_1[col].dropna().values, window)
            length = ma.shape[0]
            slope, _, corr, p_value, _ = linregress(ma, np.arange(length))
        #     r, p = pearsonr(np.arange(length), ma)
            if (p_value<0.05 and abs(corr)>0.7):
                cnt+=1
                if corr > 0 :
                    up.append(col)
                else:
                    dn.append(col)
        except Exception as e:
            mylog.warning("Para Review: Error while doing trend check of "+str(col))
            mylog.warning_trace(e)

    mylog.info("There are "+str(cnt)+" data with trend")
    mylog.info("Increase("+str(len(up))+") : ")
    mylog.info(','.join(str(e) for e in up))
    mylog.info("")
    mylog.info("Decrease("+str(len(dn))+") : ")
    mylog.info(','.join(str(e) for e in dn))
    mylog.info("")
    mylog.info('-----Parameter Review Done-----')

    return None

#######################################################################################
#Read_path.py
# -*- coding: utf-8 -*-
import json
import os

def Read_json(path):
    try:
        with open(path) as json_data:
            file = json.load(json_data)
        return file

    except Exception as e:
        import traceback
        error_path = os.path.join(path, "error.log")
        with open(error_path, 'a') as file:
            file.write("Error while reading file_path.json")
            file.write(traceback.format_exc())
        raise
        
def read_path(folder_path):
    input_file_path = os.path.join(folder_path, "file_path.json")
    return Read_json(input_file_path)   

###################################################################
#config.py
import json

def read_config(config_path, mylog):
    try:
        with open(config_path) as json_data:
            config_ = json.load(json_data)
        return config_

    except Exception as e:
        mylog.error('load config error:')
        mylog.error_trace(e)
        mylog.error("Please check your config again!")
        raise

def save_config(config_path, config, mylog):
    try:
        with open(config_path , 'w') as fp:
            json.dump(config, fp, indent=4, sort_keys=True)
        return None
    except Exception as e:
        mylog.error('save config error:\n', e)
        mylog.error_trace(e)
        mylog.error("Please check your config again!")
        raise    

################################################################
#CreateLog.py
# -*- coding: utf-8 -*-
import logging
import getpass

class WriteLog(object):
    def __init__(self, normal_log_path, error_log_path):
        self.normal_log_path = normal_log_path
        self.error_log_path = error_log_path
        self.user=getpass.getuser()
#        format='%(asctime)s - %(levelname)s - %(name)s: %(message)s'
        format='%(asctime)s - %(levelname)s : %(message)s'
        self.formatter=logging.Formatter(format)


        
    def setup_logger(self, name, log_path, level):
        """Function setup as many loggers as you want"""
        logger=logging.getLogger(name)
        logger.setLevel(level)
        if logger.handlers:
            logger.handlers = []
        
        streamhandler=logging.StreamHandler()
        streamhandler.setFormatter(self.formatter)
        logger.addHandler(streamhandler)
        filehandler=logging.FileHandler(log_path)
        filehandler.setFormatter(self.formatter)
        logger.addHandler(filehandler)
        return logger
    
    def init_logger(self):
        self.log = self.setup_logger("log", self.normal_log_path, logging.INFO)
        self.err_log = self.setup_logger("err_log", self.error_log_path, logging.ERROR)

    def debug(self, msg):
        self.log.debug(msg)
        
    def info(self, msg):
        self.log.info(msg)
        
    def warning(self, msg):
        self.log.warning(msg)
        
    def error(self, msg):
        self.log.error(msg)
        self.err_log.error(msg)
        
    def critical(self, msg):
        self.log.critical(msg)
        self.err_log.critical(msg)
        
    def log(self, level, msg):
        self.log.log(level, msg)
        self.err_log.log(level, msg)
        
    def setLevel(self, log, level):
        log.setLevel(level)

    def disable(self):  # close output of log
        logging.disable(50)

    def warning_trace(self, msg):
        self.log.warning(msg,exc_info=True)
        
    def error_trace(self, msg):
        self.log.error(msg,exc_info=True)
        self.err_log.error(msg,exc_info=True)

    def critical_trace(self, msg):
        self.log.critical(msg,exc_info=True)
        self.err_log.critical(msg,exc_info=True)
        
        
        
if __name__ == "__main__":
    mylog = WriteLog("./log.log", "./error.log")
    mylog.init_logger()
#    mylog.logger.info("GGGG")
#    mylog.logger_error.error("NOOOOO")
    mylog.error("Holy Shit")

######################################################
#Data_PreProcess.py
import pandas as pd
import datetime
import math
import os
from sklearn.model_selection import train_test_split
from config import read_config, save_config
from DataImputation import DataImputation, TESTDataImputation
from DataTransform import DataTransform, TESTDataTransform
from CreateLog import WriteLog
from Read_path import read_path

def CHECKCol(dfs_, col_list_):

    list_not_exists = []
    list_missings = []
    for col in col_list_:
        # check Merge col status (Exist Check)
        if col not in dfs_.columns:
            list_not_exists.append(col)
            continue

        # check Merge col status (Missing Check)
        if sum(dfs_[col].isna()) != 0:
            list_missings.append(col)

    return list_not_exists, list_missings


def CheckCatCol(dfs, configs, mylog):
    check_list = set()

    # the code presume the group list is not empty if the user choose group related function
    if "MergeCatCol" in configs["Preprocess_Step"]:
        check_list = check_list | set(configs["Merge_Category_Columns"]["Group_List"])

    if ("DataImpute" in configs["Preprocess_Step"]) & (configs["Data_Imputation"]["Group_Mode"] == 1):
        check_list = check_list | set(configs["Data_Imputation"]["Group_List"])

    if ("DataTrans" in configs["Preprocess_Step"]) & (configs["Data_Transform"]["Group_Mode"] == 1):
        check_list = check_list | set(configs["Data_Transform"]["Group_List"])

    if len(check_list) == 0:
        mylog.info("**Category Columns Check Passed**")
        return "OK"

    elif len(check_list) != 0:
        list_miss, list_not_exist = CHECKCol(dfs, check_list)

        if len(list_not_exist) + len(list_miss) == 0:
            return "OK"

        if len(list_not_exist) != 0:
            col_str = ', '.join(list_not_exist)
            mylog.error("The Following Columns don't exist: " + col_str)

        if len(list_miss) != 0:
            col_str = ', '.join(list_miss)
            mylog.error("The Following Columns exist missing value:" + col_str)

        return "NG"


def preSortGroupCol(configs):

    if "MergeCatCol" in configs["Preprocess_Step"]:
        configs["Merge_Category_Columns"]["Group_List"].sort()

    if ("DataImpute" in configs["Preprocess_Step"]) & (configs["Data_Imputation"]["Group_Mode"] == 1):
        configs["Data_Imputation"]["Same"] = 0
        configs["Data_Imputation"]["Group_List"].sort()
        if "MergeCatCol" in configs["Preprocess_Step"]:
            if configs["Merge_Category_Columns"]["Group_List"] == configs["Data_Imputation"]["Group_List"]:
                configs["Data_Imputation"]["Same"] = 1

    if ("DataTrans" in configs["Preprocess_Step"]) & (configs["Data_Transform"]["Group_Mode"] == 1):
        configs["Data_Transform"]["Same"] = 0
        configs["Data_Transform"]["Group_List"].sort()
        if "MergeCatCol" in configs["Preprocess_Step"]:
            if configs["Merge_Category_Columns"]["Group_List"] == configs["Data_Transform"]["Group_List"]:
                configs["Data_Transform"]["Same"] = 1

    return configs


def DataSplit(dfs, configs, output_path):
    if configs["Train_Test_Split"]["Assign_Spilt_Amount_Mode"] == 1:
        test_amount_ = configs["Train_Test_Split"]["testing_Amount"]
        dfs_train = dfs.iloc[:-test_amount_]
        dfs_test = dfs.iloc[-test_amount_:]
#        print(dfs_train.shape, dfs_test.shape)

    elif configs["Train_Test_Split"]["Assign_Spilt_Amount_Mode"] == 0:
        dfs_train, dfs_test = train_test_split(dfs,
                                               test_size=configs["Train_Test_Split"]["Parameter_Test_Rate"],
                                               shuffle=configs["Train_Test_Split"]["Setting_Random_Mode"] == 1)
#    print(path.data_train_path)
    dfs_train.to_csv(output_path["data_train_path"], index=False)
    dfs_test.to_csv(output_path["data_test_path"], index=False)
    return dfs_train, dfs_test


def removeObjectCol(dfs, configs, mylog):
    ori_columns = dfs.columns
    configs['Remove_Nonnumerical_Columns']["Time_Columns"] = []
    for col in ori_columns:
        if col in configs["Exclude_columns"]:
            continue

        if dfs[col].dtypes == 'object':
            try:
                dfs[col] = pd.to_datetime(dfs[col], format='%Y-%m-%d %H:%M:%S.%f')
                if configs['Remove_Nonnumerical_Columns']["Setting_Transform_Time_to_Seconds"] == 1:
#                    dfs[col] = dfs[col].apply(lambda x: transform2Second(x))
                    dfs[col] = dfs[col].apply(transform2Second, mylog=mylog)
                    configs['Remove_Nonnumerical_Columns']["Time_Columns"].append(col)

                elif configs['Remove_Nonnumerical_Columns']["Setting_Transform_Time_to_Seconds"] == 0:
                    configs["Exclude_columns"].append(col)
                    configs['Remove_Nonnumerical_Columns']["Time_Columns"].append(col)
            except:
                dfs = dfs.drop(col, axis=1)
                continue
    new_columns = dfs.columns
    # print(set(ori_columns) - set(new_columns))
    configs['Remove_Nonnumerical_Columns']["Remove_Nonnumerical_Columns"] = list(set(ori_columns) - set(new_columns))
    return dfs, configs


def TESTremoveObjectCol(dfs, configs, mylog):
#    print(configs['Remove_Nonnumerical_Columns']["Remove_Nonnumerical_Columns"])
    for col in configs['Remove_Nonnumerical_Columns']["Remove_Nonnumerical_Columns"]:
        try:
            dfs = dfs.drop(col, axis=1)
        except Exception as e:
            mylog.warning("Data PreProcess (Test): Cannot drop the nonnumerical feature: "+str(col))

    for col in configs['Remove_Nonnumerical_Columns']["Time_Columns"]:
        try:
            dfs[col] = pd.to_datetime(dfs[col], format='%Y-%m-%d %H:%M:%S.%f')
            if configs['Remove_Nonnumerical_Columns']["Setting_Transform_Time_to_Seconds"] == 1:
                dfs[col] = dfs[col].apply(transform2Second, mylog=mylog)
        except Exception as e:
            mylog.error("Data PreProcess (Test): CAN NOT TRANSFORM TIME TO SECOND: "+ str(col))
            mylog.error_trace(e)
            raise

    return dfs


def transform2Second(x, mylog):
    if pd.isnull(x):
        return math.nan
    else:
        try:
            return float((x - datetime.datetime(1970, 1, 1)).total_seconds())
        except Exception as e:
            mylog.warning("Cannot transform datatime to second")
            mylog.warning('x:', x)
            mylog.warning_trace(e)
            return math.nan


def removeMissingCol(dfs, configs):
    threshold = dfs.shape[0] * configs["Remove_Missing_Value"]["Threshold"]
    remove_cols = [c for c in dfs.columns if sum(dfs[c].isnull()) >= threshold]
    configs['Remove_Missing_Value']["Remove_Missing_Columns"] = remove_cols
    dfs = dfs.drop(remove_cols, axis=1)
    return dfs, configs


def TESTremoveMissingCol(dfs, configs):
    for col in configs['Remove_Missing_Value']["Remove_Missing_Columns"]:
        try:
            dfs = dfs.drop(col, axis=1)
        except:
            print("Data PreProcess (Test): Cannot drop cols missing in training data: \n", col)

    return dfs


def mergeCategoryCol(dfs, configs):

#    print(configs['Merge_Category_Columns'])
    merge_col_list = configs['Merge_Category_Columns']['Group_List']

    new_col_name = "_".join(merge_col_list)
    dfs[new_col_name] = dfs[merge_col_list].apply(lambda x: '_'.join(x), axis=1)
    configs['Merge_Category_Columns']['New_Columns'] = new_col_name
    configs["Exclude_columns"].append(new_col_name)

    return dfs, configs


def TESTmergeCategoryCol(dfs, configs):

    merge_col_list = configs['Merge_Category_Columns']['Group_List']
    new_col_name = "_".join(merge_col_list)
    dfs[new_col_name] = dfs[merge_col_list].apply(lambda x: '_'.join(x), axis=1)

    return dfs


def Data_PreProcess_Train(folder_path):
    #### 
    output_path={}
    output_path["data_train_path"] = os.path.join(folder_path, 'Data_Train.csv')
    output_path["data_test_path"] = os.path.join(folder_path, 'Data_Test.csv')
    output_path["x_train_path"] = os.path.join(folder_path, 'x_Train.csv')
    output_path["x_test_path"] = os.path.join(folder_path, 'x_Test.csv')
    output_path["y_train_path"] = os.path.join(folder_path, 'y_Train.csv')
    output_path["y_test_path"] = os.path.join(folder_path, 'y_Test.csv')    
    ####
    input_path = read_path(folder_path)
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    mylog.init_logger()
    config = read_config(input_path["config_path"], mylog)

    # Put in Y
    tmp = config["Exclude_columns"]
    tmp.extend(config["Y"])
    config["Exclude_columns"] = tmp
    
    mylog.info("-----Data PreProcess (Train)-----")

    try:
        df_train = pd.read_csv(input_path["raw_path"])
    except Exception as e:
        mylog.error('Data PreProcess (Train): Error while loading training data ')
        mylog.error_trace(e)
        raise

    try:
        df_train = df_train.drop(config['ParamReview']['RemoveInReview'], axis=1)

    except Exception as e:
        mylog.warning('Data PreProcess (Train): Cannot drop features assigned in Parameter Review')
        mylog.warning_trace(e)


    if CheckCatCol(df_train, config, mylog) == "NG":
        mylog.critical("Please contact Peter to solve the problem")
        raise

    config = preSortGroupCol(config)

    # TRAIN STEP
    mylog.info("**TRAINING PROCESS START**")
    for step_index in config['Preprocess_Step']:
        if step_index == "DataSplit":
            df_train, df_test = DataSplit(df_train, config, output_path)

        if step_index == "DelNonNumCol":
            mylog.info("Remove_Nonnumerical_Columns")
            df_train, config = removeObjectCol(df_train, config, mylog)
        elif step_index == "DelMissingCol":
            mylog.info("Remove_Missing_Value")
            df_train, config = removeMissingCol(df_train, config)
        elif step_index == "MergeCatCol":
            mylog.info("Merge_Category_Columns")
            df_train, config = mergeCategoryCol(df_train, config)
        elif step_index == "DataImpute":
            mylog.info("Data Imputation")
            df_train, config, error_msg = DataImputation(df_train, config)
            if error_msg != "OK":
                mylog.error("Data Process Interrupted")
                mylog.error_trace(error_msg)
                raise
        elif step_index == "DataTrans":
            mylog.info("Data Transform")
            df_train, config = DataTransform(df_train, config)

    df_train[config["Exclude_columns"]].to_csv(output_path["y_train_path"], index=False)
    df_train.drop(config["Y"], axis=1).to_csv(output_path["x_train_path"], index=False)

    # remove Y
    tmp = config["Exclude_columns"]
    tmp.remove(config["Y"][0])
    config["Exclude_columns"] = tmp
    save_config(input_path["config_path"],config, mylog)
    
    mylog.info("-----Data PreProcess (Train) Finished-----")

    return None

def Data_PreProcess_Test(folder_path):
    #### 
    output_path={}
    output_path["data_train_path"] = os.path.join(folder_path, 'Data_Train.csv')
    output_path["data_test_path"] = os.path.join(folder_path, 'Data_Test.csv')
    output_path["x_train_path"] = os.path.join(folder_path, 'x_Train.csv')
    output_path["x_test_path"] = os.path.join(folder_path, 'x_Test.csv')
    output_path["y_train_path"] = os.path.join(folder_path, 'y_Train.csv')
    output_path["y_test_path"] = os.path.join(folder_path, 'y_Test.csv')    
    ####
    input_path = read_path(folder_path)
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    mylog.init_logger()
    config = read_config(input_path["config_path"], mylog)

    # Put in Y
    tmp = config["Exclude_columns"]
    tmp.extend(config["Y"])
    config["Exclude_columns"] = tmp

    try:
        df_test = pd.read_csv(output_path["data_test_path"])
    except Exception as e:
        mylog.error("Data PreProcess (Test): Error while loading testing data ")
        mylog.error_trace(e)
        raise
        
    mylog.info("-----Data PreProcess (Test)-----")

    for col in config['ParamReview']['RemoveInReview']:
        try:
            df_test = df_test.drop(col, axis=1)
        except Exception as e:
            mylog.warning('Data PreProcess (Test): Cannot drop features assigned in Parameter Review')
            mylog.warning_trace(e)

    if CheckCatCol(df_test, config, mylog) == "NG":
        mylog.critical("Please contact Peter to solve the problem")
        raise

    # TEST STEP
    mylog.info("**TESTING PROCESS START**")
    for step_index in config['Preprocess_Step']:
        if step_index == "DelNonNumCol":
            mylog.info("Remove_Nonnumerical_Columns")
            df_test = TESTremoveObjectCol(df_test, config, mylog)
        elif step_index == "DelMissingCol":
            mylog.info("Remove_Missing_Value")
            df_test = TESTremoveMissingCol(df_test, config)
        elif step_index == "MergeCatCol":
            mylog.info("Merge_Category_Columns")
            df_test = TESTmergeCategoryCol(df_test, config)
        elif step_index == "DataImpute":
            mylog.info("Data Imputation")
            df_test, error_msg = TESTDataImputation(df_test, config)
            if error_msg != "OK":
                mylog.error("Data Process Interrupted")
                mylog.error_trace(error_msg)
                return
        elif step_index == "DataTrans":
            mylog.info("Data Transform")
            df_test = TESTDataTransform(df_test, config)

    df_test[config["Exclude_columns"]].to_csv(output_path["y_test_path"], index=False)
    df_test.drop(config["Y"], axis=1).to_csv(output_path["x_test_path"], index=False)

    # remove Y
    tmp = config["Exclude_columns"]
    for y in config["Y"]:
        tmp.remove(y)
    config["Exclude_columns"] = tmp
    save_config(input_path["config_path"],config, mylog)
    mylog.info("-----Data PreProcess (Test) Finished-----")
    
    return None

if __name__ == '__main__':

#    input_folder = '../data/'
#    folder_list = [input_folder + name + '/' for name in os.listdir(input_folder) if os.path.isdir(os.path.join(input_folder, name))]
#    print('Total log folders:', folder_list, "\n")
#
#    for folder_name in folder_list:
#
#        print('Pre-process Start', folder_name)
#
#        Data_PreProcess_Train(folder_name)
#        Data_PreProcess_Test(folder_name)
    from Path import All_path
    base_path = os.path.join("../Cases", "CVD2E_Split1_Test")
    path = All_path(base_path)
    path.dir_init()
    print("Data Preprocess")
    Data_PreProcess_Train(path.path_03)
    
####################################################################################################################################   
#DataImputation.py
import numpy as np

def ImputebySTD(dfs_, col_mean=None, col_median=None, col_std=None):

    if col_mean is None:
        col_mean = dfs_.mean()
    if col_std is None:
        col_std = dfs_.std()
    if col_median is None:
        col_median = dfs_.median()

    impute_list = [col_mean, col_median]

    ok_value = dfs_.loc[~dfs_.isna()]
    na_amount = sum(dfs_.isna())

    diff_std = []
    for impute_value in impute_list:
        new_std = np.append(np.array(ok_value), [impute_value] * na_amount).std()
        diff_std.append(abs(col_std - new_std))

    return impute_list[np.argmin(diff_std)]


def GroupColCheck(dfs_, configs_):

    if configs_['Data_Imputation']["Same"] == 1:
        group_by_col = configs_['Merge_Category_Columns']['New_Columns']

    elif configs_["Data_Imputation"]["Same"] == 0:
        merge_col_list = configs_['Data_Imputation']['Group_List']
        group_by_col = "_".join(merge_col_list)
        dfs_[group_by_col] = dfs_[merge_col_list].apply(lambda x: '_'.join(x), axis=1)

    return dfs_, group_by_col


def DataImputation(dfs, configs):
#    path = All_path(data_folders)

    configs['Data_Imputation']['Imputation_List'] = {}
    strategy = configs['Data_Imputation']['Strategy']

    if configs['Data_Imputation']['Group_Mode'] == 0:
        for col in dfs.columns:
            if col in configs["Exclude_columns"]:
                continue
            configs['Data_Imputation']['Imputation_List'][col] = {}
            configs['Data_Imputation']['Imputation_List'][col]["STD_CHECK"] = 0

            if strategy in ["Mean", "Median"]:
                if strategy == "Mean":
                    impute_value = dfs[col].mean()
                elif strategy == "Median":
                    impute_value = dfs[col].median()

                configs['Data_Imputation']['Imputation_List'][col]['DEFAULT_VALUE'] = impute_value
                if sum(dfs[col].isna()) != 0:
                    dfs[col].fillna(impute_value, inplace=True)

            elif strategy == "STD":
                configs['Data_Imputation']['Imputation_List'][col]['DEFAULT_Mean'] = dfs[col].mean()
                configs['Data_Imputation']['Imputation_List'][col]['DEFAULT_Median'] = dfs[col].median()
                if sum(dfs[col].isna()) == 0:
                    configs['Data_Imputation']['Imputation_List'][col]["STD_CHECK"] = 1
                elif sum(dfs[col].isna()) != 0:
                    impute_value = ImputebySTD(dfs[col])
                    configs['Data_Imputation']['Imputation_List'][col]['DEFAULT_VALUE'] = impute_value
                    dfs[col].fillna(impute_value, inplace=True)

        return dfs, configs, "OK"

    elif configs['Data_Imputation']['Group_Mode'] == 1:
        dfs, group_by_col = GroupColCheck(dfs, configs)

        if strategy in ["Median"]:
            for col in dfs.columns:
                if col in configs["Exclude_columns"]:
                    continue

                if col == group_by_col:
                    continue

                configs['Data_Imputation']['Imputation_List'][col] = {}
                col_median = dfs[col].median()
                configs['Data_Imputation']['Imputation_List'][col]['DEFAULT_VALUE'] = col_median

                for cat_type in np.unique(dfs[group_by_col]):
                    configs['Data_Imputation']['Imputation_List'][col][cat_type] = {}
                    impute_value = col_median
                    configs['Data_Imputation']['Imputation_List'][col][cat_type]["over30"] = 0
                    if dfs[col].loc[(dfs[group_by_col] == cat_type) &
                                    (~dfs[group_by_col].isna())].size > 30:
                        impute_value = dfs[col].loc[(dfs[group_by_col] == cat_type)].median()
                        configs['Data_Imputation']['Imputation_List'][col][cat_type]["over30"] = 1

                    elif dfs[col].loc[(dfs[group_by_col] == cat_type) &
                                      (~dfs[group_by_col].isna())].size < 30:
                        print(col, cat_type, "NO impute by Group")

                    configs['Data_Imputation']['Imputation_List'][col][cat_type]['DEFAULT_VALUE'] = impute_value
                    dfs[col].loc[(dfs[group_by_col] == cat_type)].fillna(impute_value, inplace=True)

                if sum(dfs[col].isna()):
                    dfs[col].fillna(col_median, inplace=True)
                    print("ERROR")

        elif strategy in ["Mean"]:
            for col in dfs.columns:
                if col in configs["Exclude_columns"]:
                    continue

                if col == group_by_col:
                    continue
                configs['Data_Imputation']['Imputation_List'][col] = {}
                col_mean = dfs[col].mean()
                configs['Data_Imputation']['Imputation_List'][col]['DEFAULT_VALUE'] = col_mean

                for cat_type in np.unique(dfs[group_by_col]):
                    configs['Data_Imputation']['Imputation_List'][col][cat_type] = {}
                    impute_value = col_mean
                    configs['Data_Imputation']['Imputation_List'][col][cat_type]["over30"] = 0
                    if dfs[col].loc[(dfs[group_by_col] == cat_type) &
                                    (~dfs[group_by_col].isna())].size > 29:
                        configs['Data_Imputation']['Imputation_List'][col][cat_type]["over30"] = 1
                        impute_value = dfs[col].loc[(dfs[group_by_col] == cat_type)].col_mean()

                    elif dfs[col].loc[(dfs[group_by_col] == cat_type) &
                                      (~dfs[group_by_col].isna())].size < 30:
                        print(col, cat_type, "NO impute by Group")

                    configs['Data_Imputation']['Imputation_List'][col][cat_type]['DEFAULT_VALUE'] = impute_value
                    dfs[col].loc[(dfs[group_by_col] == cat_type)].fillna(impute_value, inplace=True)

                if sum(dfs[col].isna()):
                    dfs[col].fillna(col_mean, inplace=True)
                    print("ERROR")

        elif strategy in ["STD"]:
            for col in dfs.columns:
                if col in configs["Exclude_columns"]:
                    continue

                if col == group_by_col:
                    continue

                configs['Data_Imputation']['Imputation_List'][col] = {}
                col_median = dfs[col].median()
                col_mean = dfs[col].mean()
                col_std = dfs[col].std()
                configs['Data_Imputation']['Imputation_List'][col]['DEFAULT_Mean'] = col_mean
                configs['Data_Imputation']['Imputation_List'][col]['DEFAULT_Median'] = col_median
                configs['Data_Imputation']['Imputation_List'][col]['DEFAULT_Median'] = col_std

                for cat_type in np.unique(dfs[group_by_col]):
                    configs['Data_Imputation']['Imputation_List'][col][cat_type] = {}
                    configs['Data_Imputation']['Imputation_List'][col][cat_type]["over30"] = 0
                    if dfs[col].loc[(dfs[group_by_col] == cat_type) &
                                    (~dfs[group_by_col].isna())].size > 29:
                        configs['Data_Imputation']['Imputation_List'][col][cat_type]["over30"] = 1
                        cat_mean = dfs[col].loc[(dfs[group_by_col] == cat_type)].mean()
                        cat_median = dfs[col].loc[(dfs[group_by_col] == cat_type)].median()
                        configs['Data_Imputation']['Imputation_List'][col][cat_type]['DEFAULT_Mean'] = cat_mean
                        configs['Data_Imputation']['Imputation_List'][col][cat_type]['DEFAULT_Median'] = cat_median
                        if sum(dfs[col].loc[(dfs[col].isna())]) != 0:
                            impute_value = ImputebySTD(dfs[col].loc[(dfs[group_by_col] == cat_type)])
                            dfs[col].loc[(dfs[group_by_col] == cat_type)].fillna(impute_value, inplace=True)
                            configs['Data_Imputation']['Imputation_List'][col][cat_type]['DEFAULT_VALUE'] = impute_value

                if sum(dfs[col].loc[(dfs[col].isna())]) != 0:
                    impute_value = ImputebySTD(dfs[col])
                    for cat_type in np.unique(dfs[group_by_col].loc[(dfs[col].isna())]):
                        configs['Data_Imputation']['Imputation_List'][col][cat_type]['DEFAULT_VALUE'] = impute_value
                    dfs[col].fillna(impute_value, inplace=True)

            if configs["Data_Imputation"]["Same"] == 0:
                dfs.drop(group_by_col, axis=1)

        else:
            return None


        return dfs, configs, "OK"


def TESTDataImputation(dfs, configs):
#    path = All_path(data_folders)
#    configs['Data_Imputation']['Imputation_List'] = {}
    strategy = configs['Data_Imputation']['Strategy']

    if configs['Data_Imputation']['Group_Mode'] == 0:
        for col in dfs.columns:
            if col in configs["Exclude_columns"]:
                continue

            if sum(dfs[col].isna()) == 0:
                continue

            if configs['Data_Imputation']['Imputation_List'][col]["STD_CHECK"] == 0:
                impute_value = configs['Data_Imputation']['Imputation_List'][col]['DEFAULT_VALUE']

            elif configs['Data_Imputation']['Imputation_List'][col]["STD_CHECK"] == 1:
                col_mean = configs['Data_Imputation']['Imputation_List'][col]['DEFAULT_Mean']
                col_median = configs['Data_Imputation']['Imputation_List'][col]['DEFAULT_Median']
                impute_value = ImputebySTD(dfs[col], col_mean, col_median)

            dfs[col].fillna(impute_value, inplace=True)

    if configs['Data_Imputation']['Group_Mode'] == 1:
        dfs, group_by_col = GroupColCheck(dfs, configs)
        for col in dfs.columns:
            if col in configs["Exclude columns"]:
                continue

            if col == group_by_col:
                continue

            if sum(dfs[col].isna()) == 0:
                continue

            if strategy in ["Mean", "Median"]:
                for cat_type in np.unique(dfs[group_by_col].loc[(dfs[col].isna())]):
                    impute_value = configs['Data Imputation']['Imputation List'][col][cat_type]['DEFAULT_VALUE']
                    dfs[col].loc[(dfs[group_by_col] == cat_type)].fillna(impute_value, inplace=True)

            elif strategy in ["STD"]:
                for cat_type in np.unique(dfs[group_by_col].loc[(dfs[col].isna())]):
                    if configs['Data_Imputation']['Imputation_List'][col][cat_type]["over30"] == 0:
                        impute_value = configs['Data Imputation']['Imputation List'][col][cat_type]['DEFAULT_VALUE']
                        dfs[col].loc[(dfs[group_by_col] == cat_type)].fillna(impute_value, inplace=True)

                    elif configs['Data_Imputation']['Imputation_List'][col][cat_type]["over30"] == 1:
                        try:
                            impute_value = configs['Data Imputation']['Imputation List'][col][cat_type]['DEFAULT_VALUE']
                            dfs[col].loc[(dfs[group_by_col] == cat_type)].fillna(impute_value, inplace=True)

                        except:
                            cat_mean = configs['Data_Imputation']['Imputation_List'][col][cat_type]['DEFAULT_Mean']
                            cat_median = configs['Data_Imputation']['Imputation_List'][col][cat_type]['DEFAULT_Median']
                            impute_value = ImputebySTD(dfs[col].loc[(dfs[group_by_col] == cat_type)],
                                                       cat_mean,
                                                       cat_median)
                            dfs[col].loc[(dfs[group_by_col] == cat_type)].fillna(impute_value, inplace=True)


        if configs["Data_Imputation"]["Same"] == 0:
            dfs.drop(group_by_col, axis=1)


    error_msgs = "OK"
    return dfs, error_msgs

#############################################################################################################################
#DataTransform.py
def DataTransform(dfs, configs):

#    path = All_path(data_folders)
    strategy = configs['Data_Transform']['Strategy']

    if configs['Data_Transform']['Group_Mode'] == 0:
        if strategy == "Z_Scale":
            configs['Data_Transform']['Z_Scale'] = {}
            for col in dfs.columns:
                if col in configs["Exclude_columns"]:
                    continue
#                print("Hello-1")
                configs['Data_Transform']['Z_Scale'][col] = {}
                col_std = dfs[col].std()
                col_mean = dfs[col].mean()
                configs['Data_Transform']['Z_Scale'][col]['Std'] = col_std
                configs['Data_Transform']['Z_Scale'][col]['Mean'] = col_mean
                if col_std == .0:
                    dfs[col] = dfs[col].transform(lambda x: x - x.mean())
                elif col_std != .0:
                    dfs[col] = dfs[col].transform(lambda x: (x - x.mean()) / (x.std()))
                    
    return dfs, configs


def TESTDataTransform(dfs, configs):

#    path = All_path(data_folders)
    strategy = configs['Data_Transform']['Strategy']

    if configs['Data_Transform']['Group_Mode'] == 0:
        if strategy == "Z_Scale":
            for col in dfs.columns:
                if col in configs["Exclude_columns"]:
                    continue

                col_std =configs['Data_Transform']['Z_Scale'][col]['Std']
                col_mean = configs['Data_Transform']['Z_Scale'][col]['Mean']

                if col_std != .0:
                    dfs[col] = dfs[col].transform(lambda x: (x - col_mean) / col_std)
                elif col_std == .0:
                    dfs[col] = dfs[col].transform(lambda x: x - col_mean)

    return dfs

#############################################################################################
#XDI.py
import numpy as np
from sklearn.decomposition import PCA
from sklearn import preprocessing
import matplotlib.pyplot as plt
from sklearn.externals import joblib
import  os
import XYDI
from config import read_config, save_config
import pandas as pd
from Read_path import read_path
from CreateLog import WriteLog
plt.style.use('ggplot')
plt.ioff()

def XDI_threshold_calculator(df_, setting_XDI_threshold_ratio, setting_filter_pca_threshold):

    XDI_loo = []
    for idx in range(df_.shape[0]):
        # loo PCA
        Model_pca_loo = PCA(n_components=setting_filter_pca_threshold).fit(df_.drop(idx, axis=0))
        X_train_loo = Model_pca_loo.transform(df_.drop(idx, axis=0))
        X_loo = Model_pca_loo.transform(np.array(df_.iloc[idx]).reshape(1,-1))

        # loo Z
        Model_z_loo = preprocessing.StandardScaler().fit(X_train_loo)
        X_loo = Model_z_loo.transform(X_loo)

        # calculate XDI loo
        X_loo_XDI = np.sqrt(np.sum(X_loo * X_loo))
        XDI_loo.append(X_loo_XDI)

    XDI_loo = np.array(XDI_loo)
    XDI_loo = np.sort(XDI_loo)[int(len(XDI_loo) * 0.05):int(len(XDI_loo) * 0.95)]
#    print(XDI_loo)
    XDI_threshold = np.mean(XDI_loo) + np.std(XDI_loo) * np.sqrt(1 / setting_XDI_threshold_ratio)
#    print(XDI_threshold)

    return XDI_threshold


def XDI_off_line_report(data_folder):
    ###
    output_path={}
    output_path["XDI_PCA_path"] = os.path.join(data_folder, "XDI_PCA.pkl")     
    output_path["XDI_Data_transform_path"] = os.path.join(data_folder, "XDI_Data_transform.pkl") 
    output_path["XDI_offline_pic_path"] = os.path.join(data_folder, "XDI.png") 
    output_path["XDI_offline_path"] = os.path.join(data_folder, "XDI_offline.csv") 
    output_path["XYDI_Data_transform_path"] = os.path.join(data_folder, "XYDI_Data_transform.pkl") 
    ###
    
    input_path = read_path(data_folder)
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    mylog.init_logger()
    config_ = read_config(input_path["config_path"], mylog)
    
    mylog.info("-----XDI Offline-----")
    
    X_train_ = XYDI.XDI_Offine_Test(data_folder, output_path, config_["Exclude_columns"], config_["XYDI"]["Data Transform Check"], 0)
    X_test_ = XYDI.XDI_Offine_Test(data_folder, output_path,  config_["Exclude_columns"], config_["XYDI"]["Data Transform Check"], 1)

    X_data_ = np.concatenate((X_train_, X_test_), axis=0)
    X_train_XDI_ = [np.sqrt(sum(i)) for i in X_data_ * X_data_]

    fig, ax = plt.subplots(figsize=(10, 6))
    plt.plot(X_train_XDI_, label='XDI',  c='b')
    ax.axhline(y=config_["XDI"]["XDI Threshold"], c='r', label='XDI threshold')
    ax.axvline(x=X_train_.shape[0], c='g', label='Train/Test Split Line')
    ax.set_title("XDI")
    plt.legend()
    plt.savefig(output_path["XDI_offline_pic_path"])
    plt.clf()
    
    XDI_offline = pd.DataFrame(data = np.array(X_train_XDI_), columns=["XDI"])
    XDI_offline.to_csv(output_path["XDI_offline_path"], index=False)
    
    mylog.info("XDI Threshold: "+str(config_["XDI"]["XDI Threshold"]))
    mylog.info("XDI is stored at: "+output_path["XDI_offline_path"])
    mylog.info("-----XDI Offline Done-----")

    return None


def Build_XDI_Model(data_folder):
    ###
    output_path={}
    output_path["XDI_PCA_path"] = os.path.join(data_folder, "XDI_PCA.pkl")     
    output_path["XDI_Data_transform_path"] = os.path.join(data_folder, "XDI_Data_transform.pkl") 
    output_path["XDI_offline_pic_path"] = os.path.join(data_folder, "XDI.png") 
    output_path["XDI_offline_path"] = os.path.join(data_folder, "XDI_offline.csv") 
    output_path["XYDI_Data_transform_path"] = os.path.join(data_folder, "XYDI_Data_transform.pkl") 
    ###
    
    input_path = read_path(data_folder)
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    mylog.init_logger()   
    mylog.info("-----XDI Model Building-----")

    X_train, config = XYDI.XYDI_PreWork_Train(data_folder, output_path["XYDI_Data_transform_path"])

    setting_XDI_threshold_ratio = config["XDI"]["XDI_threshold_ratio"]
    setting_filter_pca_threshold = config["XDI"]["filter_pca_threshold"]
    config["XDI"]["XDI Threshold"] = XDI_threshold_calculator(X_train, setting_XDI_threshold_ratio, setting_filter_pca_threshold)

    # PCA Module
    module_PCA = PCA(n_components=setting_filter_pca_threshold).fit(X_train)
    X_train_pca = module_PCA.transform(X_train)
    joblib.dump(module_PCA, output_path["XDI_PCA_path"])

    # PCA again after PCA
    scaler_pca_Z_score = preprocessing.StandardScaler().fit(X_train_pca)
    joblib.dump(scaler_pca_Z_score, output_path["XDI_Data_transform_path"])

    save_config(input_path["config_path"], config, mylog)
    
    mylog.info("-----XDI model Done-----")
        
    return None

###################################################################################################################################
#XYDI.py
import pandas as pd
from sklearn import preprocessing
from sklearn.externals import joblib
from config import read_config
from Read_path import read_path
from CreateLog import WriteLog

def XYDI_Pre_Process(input_path, exclude_cols, Mode):
    if Mode == 0:
        data = pd.read_csv(input_path["x_train_path"])

    if Mode == 1:
        data = pd.read_csv(input_path["x_test_path"])

    for col in exclude_cols:
        try:
            data = data.drop(col, axis=1)
        except Exception as e:
            print(e)
            return None

    return data


def XYDI_PreWork_Train(data_folder, XYDI_Data_transform_path):    
    input_path = read_path(data_folder)
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    mylog.init_logger()
    config = read_config(input_path["config_path"], mylog) 

    if config is None:
        return None

    data = XYDI_Pre_Process(input_path, config["Exclude_columns"], 0)

    config["XYDI"] = {}

    # check if Data Transform was executed in Data Pre-Process
    if "DataTrans" not in config["Preprocess_Step"]:
        config["XYDI"]["Data Transform Check"] = 0
        scaler_raw_Z_score = preprocessing.StandardScaler().fit(data)
        col_ = data.columns
        data = scaler_raw_Z_score.transform(data)
        data = pd.DataFrame(data=data, columns=col_)
        joblib.dump(scaler_raw_Z_score, XYDI_Data_transform_path)

    elif "DataTrans" in config["Preprocess_Step"]:
        config["XYDI"]["Data Transform Check"] = 1

    return data, config


def XYDI_PreWork_Test(input_path, XYDI_Data_transform_path, XDI_PCA_path, exclude_cols_, check_data_transform_, Mode_):  
    data = XYDI_Pre_Process(input_path, exclude_cols_, Mode_)

    # check if it is need to Z-scale first
    if check_data_transform_ == 0:
        module_transform_ = joblib.load(XYDI_Data_transform_path)
        data = module_transform_.transform(data)

    module_PCA_ = joblib.load(XDI_PCA_path)
    data = module_PCA_.transform(data)

    return data


def XDI_Offine_Test(data_folder, output_path, exclude_cols, check_data_transform, Mode):
    input_path = read_path(data_folder)
    
    data = XYDI_PreWork_Test(input_path, output_path["XYDI_Data_transform_path"], output_path["XDI_PCA_path"], exclude_cols, check_data_transform, Mode)

    scaler_pca_Z_score_ = joblib.load(output_path["XDI_Data_transform_path"])
    data = scaler_pca_Z_score_.transform(data)

    return data

######################################################################################################################################
#YDI.py
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.externals import joblib
import os
import matplotlib.cm as cm
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_samples, silhouette_score
import XYDI
from config import read_config
from Read_path import read_path
from CreateLog import WriteLog
plt.style.use('ggplot')
plt.ioff()

#setting_filter_pca_threshold = 0.8


def Clustering_Testing(X_train, output_path):
    max_clusters_amount = 10

    ideal_clusters_amount = 1
    max_silhouette_score = -1

    for n_clusters in range(2, max_clusters_amount + 1):
        clusterer = KMeans(n_clusters=n_clusters, random_state=0).fit(X_train)
        cluster_labels = clusterer.fit_predict(X_train)
        silhouette_avg = silhouette_score(X_train, cluster_labels)
        # print("For n_clusters =", n_clusters,
        #       "The average silhouette_score is :", silhouette_avg)
        if silhouette_avg > max_silhouette_score:
            max_silhouette_score = silhouette_avg
            ideal_clusters_amount = n_clusters
        
        if X_train.shape[1] == 1:
            continue
        # Draw Clustering Pic
        fig, (ax1, ax2) = plt.subplots(1, 2)
        fig.set_size_inches(18, 7)
        sample_silhouette_values = silhouette_samples(X_train, cluster_labels)
        y_lower = 10
        for i in range(n_clusters):
            # Aggregate the silhouette scores for samples belonging to
            # cluster i, and sort them
            ith_cluster_silhouette_values = \
                sample_silhouette_values[cluster_labels == i]

            ith_cluster_silhouette_values.sort()

            size_cluster_i = ith_cluster_silhouette_values.shape[0]
            y_upper = y_lower + size_cluster_i

            color = cm.nipy_spectral(float(i) / n_clusters)
            ax1.fill_betweenx(np.arange(y_lower, y_upper),
                              0, ith_cluster_silhouette_values,
                              facecolor=color, edgecolor=color, alpha=0.7)

            # Label the silhouette plots with their cluster numbers at the middle
            ax1.text(-0.05, y_lower + 0.5 * size_cluster_i, str(i))

            # Compute the new y_lower for next plot
            y_lower = y_upper + 10  # 10 for the 0 samples

        ax1.set_title("The silhouette plot for the various clusters.")
        ax1.set_xlabel("The silhouette coefficient values")
        ax1.set_ylabel("Cluster label")

        # The vertical line for average silhouette score of all the values
        ax1.axvline(x=silhouette_avg, color="red", linestyle="--")

        ax1.set_yticks([])  # Clear the yaxis labels / ticks
        ax1.set_xticks([-0.1, 0, 0.2, 0.4, 0.6, 0.8, 1])

        colors = cm.nipy_spectral(cluster_labels.astype(float) / n_clusters)
        ax2.scatter(X_train[:, 0], X_train[:, 1], marker='.', s=300, lw=0, alpha=0.7,
                    c=colors, edgecolor='k')

        # Labeling the clusters
        centers = clusterer.cluster_centers_
        # Draw white circles at cluster centers
        ax2.scatter(centers[:, 0], centers[:, 1], marker='o',
                    c="white", alpha=1, s=1000, edgecolor='k')

        for i, c in enumerate(centers):
            ax2.scatter(c[0], c[1], marker='$%d$' % i, alpha=1,
                        s=600, edgecolor='k')

        ax2.set_title("The visualization of the clustered data.")
        ax2.set_xlabel("Feature space for the 1st feature")
        ax2.set_ylabel("Feature space for the 2nd feature")
        YDI_tmp_path = os.path.join(output_path["YDI_Clustering_result_path"] , "cluster_" + str(n_clusters) + ".png")
        plt.savefig(YDI_tmp_path)
        plt.clf()

    return ideal_clusters_amount


def YDI_off_line_report(data_folder):
    ###
    output_path={}
    output_path["YDI_offline_pic_path"] = os.path.join(data_folder, "YDI.png")     
    output_path["YDI_threshold_table_path"] = os.path.join(data_folder, "YDI_threshold_table.csv") 
    output_path["YDI_Clustering_path"] = os.path.join(data_folder, "YDI_Clustering.pkl") 
    output_path["YDI_offline_path"] = os.path.join(data_folder, "YDI_offline.csv") 
    output_path["YDI_Clustering_result_path"] = os.path.join(data_folder, "YDI_Clustering/") 
    ###
    
    input_path = read_path(data_folder)
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    mylog.init_logger()
    config = read_config(input_path["config_path"], mylog)    
    
    mylog.info("-----YDI Offline-----")
        
    X_train = XYDI.XYDI_PreWork_Test(input_path, input_path["XYDI_Data_transform_path"], input_path["XDI_PCA_path"],
                                     config["Exclude_columns"], config["XYDI"]["Data Transform Check"], 0)
    X_test = XYDI.XYDI_PreWork_Test(input_path, input_path["XYDI_Data_transform_path"], input_path["XDI_PCA_path"],
                                     config["Exclude_columns"], config["XYDI"]["Data Transform Check"], 1)
    y_train = pd.read_csv(input_path["y_train_path"])
    y_train = y_train["Value"]
    y_test = pd.read_csv(input_path["y_test_path"])
    y_test = y_test["Value"]

    module_Clustering_ = joblib.load(output_path["YDI_Clustering_path"])

    X_data = np.concatenate((X_train, X_test), axis=0)
    y_data = np.concatenate((y_train, y_test), axis=0)
    cluster_idx = module_Clustering_.predict(X_data)

    df_YDI = pd.read_csv(output_path["YDI_threshold_table_path"])

    df_y_avg = df_YDI['Y_avg'].iloc[cluster_idx].reset_index(drop=True)
    DQIy = abs(y_data - df_y_avg) / df_y_avg
    list_DQIy_threshold = df_YDI['YDI_thrshold'].iloc[cluster_idx].reset_index(drop=True)


    fig, ax = plt.subplots(figsize=(10, 6))
    plt.plot(DQIy, label='YDI',  c='b')
    plt.plot(list_DQIy_threshold, label='YDI threshold', c='r')
    ax.axvline(x=X_train.shape[0], c='g', label='Train/Test Split Line')
    ax.set_title("YDI")
    plt.legend()
    plt.savefig(output_path["YDI_offline_pic_path"])
    plt.clf()
    
    YDI_offline = pd.DataFrame(data = np.array(DQIy), columns=["YDI"])
    YDI_offline["YDI Threshold"] = list_DQIy_threshold
    YDI_offline.to_csv(output_path["YDI_offline_path"], index=False)

    mylog.info("YDI and its Threshold are stored at: "+output_path["YDI_offline_path"])
    mylog.info("-----YDI Offline Done-----")
    return None


def Build_YDI_Model(data_folder):
    ###
    output_path={}
    output_path["YDI_offline_pic_path"] = os.path.join(data_folder, "YDI.png")     
    output_path["YDI_threshold_table_path"] = os.path.join(data_folder, "YDI_threshold_table.csv") 
    output_path["YDI_Clustering_path"] = os.path.join(data_folder, "YDI_Clustering.pkl") 
    output_path["YDI_offline_path"] = os.path.join(data_folder, "YDI_offline.csv") 
    output_path["YDI_Clustering_result_path"] = os.path.join(data_folder, "YDI_Clustering/") 
    ###
    
    input_path = read_path(data_folder)
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    mylog.init_logger()
    config_ = read_config(input_path["config_path"], mylog) 
    
    mylog.info("-----YDI Model Building-----")
    
    X_train = XYDI.XYDI_PreWork_Test(input_path, input_path["XYDI_Data_transform_path"], input_path["XDI_PCA_path"],
                                     config_["Exclude_columns"], config_["XYDI"]["Data Transform Check"], 0)
    y_train = pd.read_csv(input_path["y_train_path"])
    y_train = y_train["Value"]

    ideal_clusters_amount = Clustering_Testing(X_train, output_path)

    module_Clustering = KMeans(n_clusters=ideal_clusters_amount, random_state=0).fit(X_train)
    joblib.dump(module_Clustering, output_path["YDI_Clustering_path"])

    # 計算Kmeans分群後各個群 y的實際平均值
    list_cluster_y_avg = []
    list_cluster_y_range = []
    for cluster_idx in range(ideal_clusters_amount):
        list_cluster_index = np.where(np.array(module_Clustering.predict(X_train) == cluster_idx))
        list_cluster_y_avg.append(y_train.iloc[list_cluster_index].mean())
        list_cluster_y_range.append(y_train.iloc[list_cluster_index].max() - y_train.iloc[list_cluster_index].min())

    cluster_Max_Range = np.max(np.array(list_cluster_y_range))

    df_YDI = pd.DataFrame({"Cluster_idx": list(range(ideal_clusters_amount)),
                           "Y_avg": np.array(list_cluster_y_avg),
                           "YDI_thrshold": cluster_Max_Range / list_cluster_y_avg})

    df_YDI.to_csv(output_path["YDI_threshold_table_path"], index=False)

    mylog.info("-----YDI Model Done-----")

    return None

########################################################################################################################
#XGB_model.py
# coding: utf-8
# In[1]:
from Read_path import read_path
from config import read_config
from CreateLog import WriteLog

import xgboost as xgb
from xgboost import plot_importance
import operator
from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
#import os #新增前處理資料夾分類
from sklearn.model_selection import train_test_split
from scipy import stats ######找眾數######
import os

# # 我們來調參吧
def parameter_setting():#輸入()，輸出()
    #global Param
    part=10 #每個範圍切10段
    nround=1000 #最多跑1000顆
    ES=100
    #設定調參初始值
    Originalp=[[6, 1], 0, [1, 1], 0, 1, 0.1, 500]
    #依序輸入所有超參數的最小值與最大值。深、葉重、gamma、subsample、colsample_bytree、L1、L2、eta
    #float('inf')為無窮大，float('-inf')為負無窮
    p=[['max_depth','min_child_weight'],'gamma',['subsample','colsample_bytree'],'alpha','lambda','eta','nround']
    minPa=[[2, 1], 0, [0.0000001, 0.0000001], 0, 0, 0.00001]
    maxPa=[[float('inf'), float('inf')], float('inf'), [1, 1], float('inf'), float('inf'), 1.00001]
    #依序輸入所有超參數的首次測試極值
    topall=[[2, 1], 0 ,[0.5, 0.3], 0, 0.9, 0]
    tailall=[[10, 10], 3, [1, 1], 0.1, 1.1, 0.3]
    AllParam=pd.DataFrame() #畫好參數調整總表的表格圖
    ETALossv=[]  #紀錄ETA loss curve
    ETALossp=[]  #紀錄ETA之測試值
    ETALosst=[]
    
    ######這邊為設定驗證機制的部分########
    XGTime=0 #設定迴圈初始值
    die=0
    seed=7 #在CV中，ran之defult值為0;在valid中seed=7
    if die==0:
    ######以上為設定驗證機制的部分########
        X_train1, X_val, y_train1, y_val = train_test_split(X_train, y_train, test_size=0.3, random_state=seed)
        dtrain = xgb.DMatrix(X_train1, y_train1)
        dval = xgb.DMatrix(X_val, y_val)
        for j in range(len(p)-1):    
    #         print('Start the \'{}\' test.'.format(p[j]))
        #     M=pd.DataFrame() #清空儲存最小值的矩陣，換變數要記得改
            t=0 #由參數1測試是否停止的計數器
            t1=0 #由參數2測試是否停止的計數器
            t2=0 #紀錄測試次數的計數器
            top=topall[j] #給予起始範圍組
            tail=tailall[j]
    
        ########################################################    
        #####單變數測試##########################################
        ########################################################
    
            if j in [1,3,4,5]:
                choosemin=0 #若遇到同樣小的誤差，取較小的index
                #給予測試範圍中的測試點值
                if j == 5:
                    choosemin=-1  #若遇到同樣小的誤差，取較大的index
                while t <= 10:
                    if t!=0: #判斷是否改變測試區間
                        #處理自動轉換變數測試區間，一個變數值的轉換方式
                        if minL1!=minPa[j] and (minL1==L1[0] or minL1==L1[1]): #處理左端點
                            top=max(minPa[j], minL1-4*area)
                            tail=min(minL1+area, maxPa[j])
    #                         print('Min in the left side, move area.')
                        elif minL1!=maxPa[j] and (minL1==L1[-1] or minL1==L1[-2]): #處理右端點
                            top=max(minPa[j], minL1-area)
                            tail=min(minL1+4*area, maxPa[j])
    #                         print('Min in the right side, move area.')
                        else:
                            top=max(minPa[j], minL1-3*area)
                            tail=min(minL1+2*area, maxPa[j])#處理剩下的點
                            t=t+8
    #                         print('Min in the intermediate area, change gap.')
                #         print('The {}-th time range is ({},{}).'.format(t, top, tail))
                    if (t <= 10):
              ####開始暴力測試####
    #                     print('The {}-th time the range of \'{}\' is ({},{}).'.format(t2+1, p[j], top, tail))
                        area=(tail-top)/10
                        L1=np.append(np.arange(top, tail, area),[tail]) #別忘了最右邊的值，(0,3,0.3)
    #                     print(L1)
                        for i in range(len(L1)): #測試,i=0~10
                            Originalp[j]=L1[i] ########## Originalp[1]=0...Originalp[1]=3
    #                         print(Originalp)
    #                         print(p[j],' : ', Originalp[j])
                            params = {
                                'booster': 'gbtree',
                                'objective': 'reg:linear',
                                'eval_metric':'rmse',
                                'eta': Originalp[5],
                                'max_depth': Originalp[0][0],
                                'min_child_weight': Originalp[0][1],
                                'gamma': Originalp[1],
                                'subsample': Originalp[2][0],
                                'colsample_bytree': Originalp[2][1],
                                'reg_alpha': Originalp[3],
                                'lambda': Originalp[4]
                            }
    
                            evals_result = {}
                            try:
                                model = xgb.train(params, dtrain, num_boost_round=nround, evals=[(dtrain,'train'),(dval,'val')], early_stopping_rounds =ES, verbose_eval=False, evals_result=evals_result)                               
                            except Exception as e:
                                mylog.error("train error in j1345")
                                mylog.error_trace(e) 
                            else:
                                AllParam=AllParam.append(pd.DataFrame({'Tuning step':['step:{}'.format(j+1)] ,p[0][0]:[Originalp[0][0]], p[0][1]:[Originalp[0][1]], p[1]:[Originalp[1]], p[2][0]:[Originalp[2][0]], p[2][1]:[Originalp[2][1]], p[3]:[Originalp[3]], p[4]:[Originalp[4]], p[5]:[Originalp[5]], p[6]:[model.best_iteration], 'Validation loss': model.best_score}), ignore_index=True)
                                ###########這裡開始回傳參數值#########                           
                            M1=AllParam[AllParam['Validation loss']==AllParam['Validation loss'].min()].index.tolist()[choosemin]
                            M = AllParam.iloc[M1].to_frame().T.reset_index(drop=True) #只取最小loss的參數留下來
                            M = M.rename(columns={'Validation loss': 'test_loss_mean', p[6]: 'iteration'})
    
    
                            if j==5:
                                ##########這裡開始記錄下所有的loss歷程點###########
                                ETALossv.append(evals_result['val']['rmse']) #紀錄loss curve
                                ETALosst.append(evals_result['train']['rmse']) #紀錄loss curve
                                ETALossp.append([Originalp[j]]) #依次寫下相對的ETA點值
                        t2=t2+1
    #                     print(M)
                        minL1=M.at[0,p[j]] #回傳出現最小值的L1 
                        minloss=M.at[0, 'test_loss_mean']
                        miniter=M.at[0, 'iteration']            
    #                     print('In the {}-th time, min is {},iteration is {} and \'{}\' is {}.'\
    #                           .format(t2, minloss, miniter, p[j], minL1))
                        t=t+1
                if j==5:
                    Originalp[-1]=M.at[0,'iteration'] #把最後一次測出的最好顆樹替代初始值
                Originalp[j]=minL1 ##把測出的最好變數值替代初始值
        ########################################################    
        #####樹的外型############################################
        ########################################################
            elif j==0:
                while (t <= 10) and (t1 <= 10):
                    if t!=0: #判斷是否改變測試區間
                        for k in range(len(top)): #依次處理變數的計數列
                            if k==0 and (t <= 10):
                                if minL1[k]!=maxPa[j][k] and (minL1[k]==L1[k][-1] or minL1[k]==L1[k][-2]): #處理右端點
                                    tail[k]=min(minL1[k]+4*area, maxPa[j][k])
    #                                 print('Min of \'{}\' in the right side, move area.'.format(p[j][k]))
                                else: #因為整數不用切細可以直接收工
                                    t=t+10
    
                            elif k==1 and (t1 <= 10):
                                if minL1[k]!=maxPa[j][k] and (minL1[k]==L1[k][-1] or minL1[k]==L1[k][-2]): #處理右端點
                                    tail[k]=min(minL1[k]+4*area, maxPa[j][k])
    #                                 print('Min of \'{}\' in the right side, move area.'.format(p[j][k]))
                                else: #因為整數不用切細可以直接收工
                                    t1=t1+10
                    if (t <= 10) and (t1 <= 10):
                    ####開始暴力測試####
    #                     print('The {}-th time range, {} is ({},{}) and {} is ({},{}).'\
    #                           .format(t2+1, p[j][0], top[0], tail[0], p[j][1], top[1], tail[1])) 
                        area=1 #取整數
                        L1=[]
                        for k in range(len(top)): #分別建立兩參數的範圍與點值
                            L1.append(range(top[k], tail[k]+1)) #20190419 L1=[range(2, 11), range(1, 11)]
                        for a in range(len(L1[0])): #測試,len(L1[0])=9,len(L1[1])=10
                            for b in range(len(L1[1])):
                                i=(a+1)*(b+1)
                                Originalp[j][0]=L1[0][a]
                                Originalp[j][1]=L1[1][b]
    #                             print(Originalp)
    #                             print('\'max_depth\' is {}, \'min_child_weight\' is {}.'.format(Originalp[j][0], Originalp[j][1]))
                                params = {
                                    'booster': 'gbtree',
                                    'objective': 'reg:linear',
                                    'eval_metric':'rmse',
                                    'eta': Originalp[5],
                                    'max_depth': Originalp[0][0],
                                    'min_child_weight': Originalp[0][1],
                                    'gamma': Originalp[1],
                                    'subsample': Originalp[2][0],
                                    'colsample_bytree': Originalp[2][1],
                                    'reg_alpha': Originalp[3],
                                    'lambda': Originalp[4]
                                }
                                evals_result = {}
                                try:
                                    model = xgb.train(params, dtrain, num_boost_round=nround, evals=[(dtrain,'train'),(dval,'val')], early_stopping_rounds =ES, verbose_eval=False, evals_result=evals_result)                                   
                                except Exception as e:
                                    mylog.error("train error in j0")
                                    mylog.error_trace(e)
                                else:
                                    AllParam=AllParam.append(pd.DataFrame({'Tuning step':['step:{}'.format(j+1)] ,p[0][0]:[Originalp[0][0]], p[0][1]:[Originalp[0][1]], p[1]:[Originalp[1]], p[2][0]:[Originalp[2][0]], p[2][1]:[Originalp[2][1]], p[3]:[Originalp[3]], p[4]:[Originalp[4]], p[5]:[Originalp[5]], p[6]:[model.best_iteration], 'Validation loss': model.best_score}), ignore_index=True)
                                    ###########這裡開始回傳參數值#########                                
                                M = AllParam.iloc[AllParam['Validation loss'].idxmin(axis=1)].to_frame().T.reset_index(drop=True) #只取最小loss的參數留下來
                                M = M.rename(columns={'Validation loss': 'test_loss_mean', p[6]: 'iteration'})
    #                             print(M)
    
    
                        t2=t2+1
                        minL1=[]
                        for k in range(len(top)): #分別建立兩參數出現最小值的參數值
                            minL1.append(M[p[j][k]].reset_index(drop=True)[0]) #回傳出現最小值的L1                           
    #                     print('In the {}-th time, min is {}, iteration is {}, \'{}\' is {} and \'{}\' is {}.'.format(t2, M['test_loss_mean'].reset_index(drop=True)[0],\
    #                                                                                                              M['iteration'].reset_index(drop=True)[0],\
    #                                                                                                                  p[j][0], minL1[0], p[j][1], minL1[1]))
    
                        t=t+1
                        t1=t1+1  
                Originalp[j][0]=int(minL1[0]) #用測試完的最好值替代初始值
                Originalp[j][1]=int(minL1[1])
    
        #########################################################    
        #####樹取樣###############################################
        #########################################################
            elif j==2:
                while (t <= 10) and (t1 <= 10):
                    if t!=0: #判斷是否改變測試區間
                #         #處理自動轉換變數測試區間，兩個變數值的轉換方式
                        for k in range(len(top)): #依次處理變數的計數列
                            if k==0 and (t <= 10):
                                if minL1[k]!=minPa[j][k] and (minL1[k]==L1[k][0] or minL1[k]==L1[k][1]): #處理左端點
                                    top[k]=max(minPa[j][k], minL1[k]-4*area[k])
                                    tail[k]=min(minL1[k]+area[k], maxPa[j][k])
    #                                 print('Min of \'{}\' in the left side, move area.'.format(p[j][k]))
                                elif minL1[k]!=maxPa[j][k] and (minL1[k]==L1[k][-1] or minL1[k]==L1[k][-2]): #處理右端點
                                    top[k]=max(minPa[j][k], minL1[k]-area[k])
                                    tail[k]=min(minL1[k]+4*area[k], maxPa[j][k])
    #                                 print('Min of \'{}\' in the right side, move area.'.format(p[j][k]))
                                else:
                                    top[k]=max(minPa[j][k], minL1[k]-3*area[k])
                                    tail[k]=min(minL1[k]+2*area[k], maxPa[j][k])#處理剩下的點
                                    t=t+8
    #                                 print('Min of \'{}\' in the intermediate area, change gap.'.format(p[j][k]))
                            elif k==1 and (t1 <= 10):
                                if minL1[k]!=minPa[j][k] and (minL1[k]==L1[k][0] or minL1[k]==L1[k][1]): #處理左端點
                                    top[k]=max(minPa[j][k], minL1[k]-4*area[k])
                                    tail[k]=min(minL1[k]+area[k], maxPa[j][k])
    #                                 print('Min of \'{}\' in the left side, move area.'.format(p[j][k]))
                                elif minL1[k]!=maxPa[j][k] and (minL1[k]==L1[k][-1] or minL1[k]==L1[k][-2]): #處理右端點
                                    top[k]=max(minPa[j][k], minL1[k]-area[k])
                                    tail[k]=min(minL1[k]+4*area[k], maxPa[j][k])
    #                                 print('Min of \'{}\' in the right side, move area.'.format(p[j][k]))
                                else:
                                    top[k]=max(minPa[j][k], minL1[k]-3*area[k])
                                    tail[k]=min(minL1[k]+2*area[k], maxPa[j][k])#處理剩下的點
                                    t1=t1+8
    #                                 print('Min of \'{}\' in the intermediate area, change gap.'.format(p[j][k]))
                    if (t <= 10) and (t1 <= 10):
                    ####開始暴力測試####
    #                     print('The {}-th time range, {} is ({},{}) and {} is ({},{}).'\
    #                           .format(t2+1, p[j][0], top[0], tail[0], p[j][1], top[1], tail[1])) 
                        area=[]
                        L1=[]
                        for k in range(len(top)): #分別建立兩參數的範圍與點值
                            area.append((tail[k]-top[k])/part) #分等分用
                            L1.append(np.append(np.arange(top[k], tail[k], area[k]),[tail[k]])) #分等分用，別忘了最右邊的值
    #                     print(area)
    #                     print(L1)
                        for a in range(len(L1[0])): #測試
                            for b in range(len(L1[1])):
                                i=(a+1)*(b+1)
                                Originalp[j][0]=L1[0][a]
                                Originalp[j][1]=L1[1][b]
    #                             print(Originalp)
    #                             print('{} is {}, {} is {}.'.format(p[j][0], Originalp[j][0], p[j][1], Originalp[j][1]))
                                params = {
                                    'booster': 'gbtree',
                                    'objective': 'reg:linear',
                                    'eval_metric':'rmse',
                                    'eta': Originalp[5],
                                    'max_depth': Originalp[0][0],
                                    'min_child_weight': Originalp[0][1],
                                    'gamma': Originalp[1],
                                    'subsample': Originalp[2][0],
                                    'colsample_bytree': Originalp[2][1],
                                    'reg_alpha': Originalp[3],
                                    'lambda': Originalp[4]
                                }
                                evals_result = {}
                                try:
                                    model = xgb.train(params, dtrain, num_boost_round=nround, evals=[(dtrain,'train'),(dval,'val')], early_stopping_rounds =ES, verbose_eval=False, evals_result=evals_result)                                   
                                except Exception as e:
                                    mylog.error("train error in j2")
                                    mylog.error_trace(e)
                                else:
                                    AllParam=AllParam.append(pd.DataFrame({'Tuning step':['step:{}'.format(j+1)] ,p[0][0]:[Originalp[0][0]], p[0][1]:[Originalp[0][1]], p[1]:[Originalp[1]], p[2][0]:[Originalp[2][0]], p[2][1]:[Originalp[2][1]], p[3]:[Originalp[3]], p[4]:[Originalp[4]], p[5]:[Originalp[5]], p[6]:[model.best_iteration], 'Validation loss': model.best_score}), ignore_index=True)
                                    ###########這裡開始回傳參數值#########                                
                                M = AllParam.iloc[AllParam['Validation loss'].idxmin(axis=1)].to_frame().T.reset_index(drop=True) #只取最小loss的參數留下來
                                M = M.rename(columns={'Validation loss': 'test_loss_mean', p[6]: 'iteration'})
    #                             print(M)
    
                        t2=t2+1
                        minL1=[]
                        for k in range(len(top)): #分別建立兩參數出現最小值的參數值
                            minL1.append(M[p[j][k]].reset_index(drop=True)[0]) #回傳出現最小值的L1
    #                     print('In the {}-th time, min is {}, iteration is {}, \'{}\' is {} and \'{}\' is {}.'.format(t2, M['test_loss_mean'].reset_index(drop=True)[0],\
    #                                                                                                              M['iteration'].reset_index(drop=True)[0],\
    #                                                                                                                  p[j][0], minL1[0], p[j][1], minL1[1]))               
                        t=t+1
                        t1=t1+1
                Originalp[j][0]=minL1[0] #用測試完的最好值替代初始值
                Originalp[j][1]=minL1[1]
        Param=pd.DataFrame({'Params':p, 'Best Value':Originalp})
    #     print(AllParam)
    #     print(Param)
        
    ######以下為設定驗證機制的部分########
        #######開始訓練######
        bestP={}
        for i in range(len(Param)):
            if i in [1,3,4,5,6]:
                bestP[Param['Params'][i]]=Param['Best Value'][i]
            else:
                for j in range(len(Param['Best Value'][i])):
                    bestP[Param['Params'][i][j]]=Param['Best Value'][i][j]
        bestP
        dtrain = xgb.DMatrix(X_train, y_train) 
        watchlist = [(dtrain,'train')]
        evals_result = {}
        num_rounds = int(bestP['nround'])
    #     print('begin')
        try:
            model = xgb.train(bestP, dtrain, num_rounds, watchlist, evals_result=evals_result, verbose_eval=False)                        
        except Exception as e:
            mylog.error("train error before predict X")
            mylog.error_trace(e)
        else:
            pred = model.predict(xgb.DMatrix(X))
            arr=stats.mode(pred)                  
         
        if arr[1][0] < (len(X)/2):  #arr[1][0]為眾數出現次數
            die=die+1
        elif XGTime == 10:
            die=die+1
    #         print('We trained XGboost 10 times. But still false, please give us more data and check the correction of data.')
        else:
            seed= seed+1
    #         print('We use random seed: {}.'.format(seed))
        XGTime=XGTime+1
    #print('We use random seed: {}.'.format(seed))
    try:
        Param.to_csv(Param_aftertuning_path, index=False) #把變數檔存起來        
    except Exception as e:
        mylog.error("Param to csv error")
        mylog.error_trace(e)
    
    # # 把剛剛存起來的Param叫出來訓練    
    bestP={}
    for i in range(len(Param)):
        if i in [1,3,4,5,6]:
            bestP[Param['Params'][i]]=Param['Best Value'][i]
        else:
            for j in range(len(Param['Best Value'][i])):
                bestP[Param['Params'][i][j]]=Param['Best Value'][i][j]
    
    dtrain = xgb.DMatrix(X_train, y_train)
    watchlist = [(dtrain,'train')]
    evals_result = {}
    num_rounds = int(bestP['nround'])        
    try:
        model = xgb.train(bestP, dtrain, num_rounds, watchlist, evals_result=evals_result, verbose_eval=False)             
    except Exception as e:
        mylog.error("train error before save model")
        mylog.error_trace(e)
    else:
        model.save_model(ModelSave_path)    #20190426 save model for reuse

def xgb_execution(data_folder):##: 輸入()，輸出(啟動主程式，目前包成副函式，以後可在其他物件中，呼叫此副函式，減少物件的浪費)
    # 調整之前要先改位置的變數
    #global vset
    global X_train
    global y_train
    global X
    global mylog
    global Param_aftertuning_path
    global ModelSave_path
    #global error_log
    #global error_name
    
    #20190422 將從hotcode->讀取config==================================================
    #path='sp10402'
    #path = "Cases/AVM_sample_dir/02_sum"
    #vset='ACD6K_aftertuning{}'.format(path)
    #p='../../Output/{}'.format(vset)
    #if not os.path.isdir(p):
    #    os.mkdir(p)
    output_path={}
    output_path["Param_aftertuning_path"] = os.path.join(data_folder, "Param_aftertuning.csv")     
    output_path["FeatureScore_path"] = os.path.join(data_folder, "FeatureScore.csv") 
    output_path["Importance10_path"] = os.path.join(data_folder, "Importance10.jpg") 
    output_path["Importance30_path"] = os.path.join(data_folder, "Importance30.jpg") 
    output_path["testPredResult_path"] = os.path.join(data_folder, "testPredResult.csv")
    output_path["trainPredResult_path"] = os.path.join(data_folder, "trainPredResult.csv") 
    output_path["ModelSave_path"] = os.path.join(data_folder, "XGB.model")                    
    #IO_path = Read_in_out(path)
    #input_path, output_path = IO_path.Read_in_out_path()
    input_path = read_path(data_folder)    
    # init log
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    mylog.init_logger()
    
    mylog.info("-----XGB Model Building-----")
    # read config
    config = read_config(input_path["config_path"], mylog)
    #需要輸入的資料  train:174/test:20
    #x_Trainpath='../../DataSet/OriginalData/x_Train{}.csv'.format(path) #訓練資料集路徑
    #x_Testpath='../../DataSet/OriginalData/x_Test{}.csv'.format(path) #訓練資料集路徑
    #y_Trainpath='../../DataSet/OriginalData/y_Train{}.csv'.format(path) #訓練資料集路徑
    #y_Testpath='../../DataSet/OriginalData/y_Test{}.csv'.format(path) #訓練資料集路徑
    #delcol=['SHEET_ID'] #刪除之欄名list
    x_Trainpath=input_path["x_train_path"] #訓練資料集路徑
    x_Testpath=input_path["x_test_path"] #訓練資料集路徑
    y_Trainpath=input_path["y_train_path"] #訓練資料集路徑
    y_Testpath=input_path["y_test_path"] #訓練資料集路徑
    delcol = config["Exclude_columns"]  #20190429 delcol->Exclude_columns
    
    Param_aftertuning_path = output_path["Param_aftertuning_path"]
    FeatureScore_path = output_path["FeatureScore_path"]
    Importance10_path = output_path["Importance10_path"]
    Importance30_path = output_path["Importance30_path"]
    testPredResult_path = output_path["testPredResult_path"]
    trainPredResult_path = output_path["trainPredResult_path"]
    ModelSave_path = output_path["ModelSave_path"]
    #=================================================================================   
    #error_log = []
    #temp_tuple=()
    #error_name=['error_time','error_type','error_message']
    
    try:
        train=pd.read_csv(x_Trainpath)
        test=pd.read_csv(x_Testpath)      
        y_train=pd.read_csv(y_Trainpath).drop(delcol, axis=1)   #20190418 只留train的y    
        y_test=pd.read_csv(y_Testpath).drop(delcol, axis=1)     #20190418 只留test的y      
    except Exception as e:
        mylog.error("Read raw data error")
        mylog.error_trace(e) 
    else:
        X_train=train.drop(delcol, axis=1)                      #20190418 只留train的x
        X_test=test.drop(delcol, axis=1)                        #20190418 只留test的x
        X = pd.concat([X_train, X_test], axis=0, ignore_index=True) #20190329 垂直合併train及test的x 自動產生index       
    """  
    print(train.shape)  #20190329 維度[垂直,平行]
    print(test.shape)
    print(X_train.shape)
    print(y_train.shape)
    print(X_test.shape)
    print(y_test.shape)
    """    
    parameter_setting()
    #Param=pd.DataFrame() #20190415 畫好最好參數的表格圖
    #Param=parameter_setting(X_train,y_train,X)
    # In[11]:    
    try:
        model1=xgb.Booster(model_file=ModelSave_path)   #20190426 read model
        dtest = xgb.DMatrix(X_test)              
    except Exception as e:
        mylog.error("train error before predict test")
        mylog.error_trace(e)
    else:
        pre = model1.predict(dtest)                 
    # In[13]:  
    try:
        importance = model1.get_fscore()
        importance = sorted(importance.items(), key=operator.itemgetter(1))  
        df = pd.DataFrame(importance, columns=['feature', 'fscore'])
    except Exception as e:
        mylog.error("FeatureScore to csv error")
        mylog.error_trace(e)        
    else:       
        df.to_csv(FeatureScore_path, index=False)            
    
    try:
        plot_importance(model1,max_num_features=30)       
    except Exception as e:
        mylog.error("Importance30 save error")
        mylog.error_trace(e)        
    else:
        plt.savefig(Importance30_path)               
        # In[14]:
    try:
        plot_importance(model1,max_num_features=10)       
    except Exception as e:
        mylog.error("Importance10 save error")
        mylog.error_trace(e)      
    else:
        plt.savefig(Importance10_path)        
        # In[15]:
    try:
        sheet_test = test[delcol] 
        sheet_train = train[delcol]
        ans = pd.DataFrame(pre,columns=['Y_pred'])  #20190430 pred->Y_pred
        output = pd.concat([y_test,ans],axis=1)
        output = pd.concat([sheet_test,output],axis=1)
        output['differ']=np.abs(output['Value']-output['Y_pred'])   #20190430 pred->Y_pred
    except Exception as e:
        mylog.error("testPredResult to csv error")
        mylog.error_trace(e)
    else:       
        output.to_csv(testPredResult_path, index=False)        
        # In[16]:     
    try:
        dtrain = xgb.DMatrix(X_train)
        pre_train = model1.predict(dtrain)
        ans_train = pd.DataFrame(pre_train,columns=['Y_pred'])  #20190430 pred->Y_pred
        output_train = pd.concat([y_train,ans_train],axis=1)
        output_train = pd.concat([sheet_train,output_train],axis=1)
        output_train['differ']=np.abs(output_train['Value']-output_train['Y_pred']) #20190430 pred->Y_pred
    except Exception as e:
        mylog.error("trainPredResult to csv error")
        mylog.error_trace(e)
    else:          
        output_train.to_csv(trainPredResult_path, index=False)  
    
    mylog.info("-----XGB Model Done-----")       
#呼叫主函式    
#xgb_execution()

##############################################################################################################
#Model_Selection.py
from Read_path import read_path
from config import read_config, save_config
from CreateLog import WriteLog
import numpy as np
import os
import pandas as pd

def read_data(data_path_, mylog_):
    try:
        return pd.read_csv(data_path_)
    except Exception as e:
        mylog_.error("Read raw data error")
        mylog_.error_trace(e)
        raise


def argmin(lst):
    return min(range(len(lst)), key=lst.__getitem__)

# def argmax(lst):
#     return max(range(len(lst)), key=lst.__getitem__)




def data_merge(Train_mark, y_paths, model_paths, configs, mylogs):

    # trainPredResult.csv
    # testPredResult.csv

    if Train_mark == "Train":
        pred_file = "trainPredResult.csv"
    elif Train_mark == "Test":
        pred_file = "testPredResult.csv"
    else:
        print("Please check the input of the function")
        raise

    df_ = read_data(y_paths, mylogs)

    for model in configs["Model_Pred_Name"]:
        # read input file
        data_paths = os.path.join(model_paths, model, pred_file)
        data = read_data(data_paths, mylogs)
        if data.shape[0] != df_.shape[0]:
            mylogs.error("The shapes are different X in " + Train_mark + " and y_pred in " + model)
            raise

        data.rename(columns={"Y_pred": "Y_pred_" + model}, inplace=True)
        col = []
        col.extend(configs['Exclude_columns'])
        col.extend(["Y_pred_" + model])

        if len(configs['Exclude_columns']) > 0:
            df_ = pd.merge(df_, data[col], on=configs['Exclude_columns'])
        elif len(configs['Exclude_columns']) == 0:
            df_ = pd.concat([df_, data[col]], axis=1)
            mylogs.warning("No key in this case, data is concat directly")

    return df_


def Score_MAPE(y_true, y_pred):
    # mean_absolute_percentage_error
    return np.mean(np.abs((np.array(y_true) - np.array(y_pred)) / np.array(y_true)))

def Score_MAE(y_true, y_pred):
    # mean_absolute_error
    return np.mean(np.abs(np.array(y_true) - np.array(y_pred)))

def flag_data(df, col_list, Y_list, col_tag):
    diff = [float(abs(df[col] - df[Y_list])) for col in col_list]
    return col_tag[argmin(diff)]

def Model_Socring(dfs, configs, data_type):

    col_list = ["Y_pred_" + x for x in configs["Model_Pred_Name"]]
    dfs['Win'] = dfs.apply(flag_data, col_list=col_list, Y_list=configs["Y"], col_tag=configs["Model_Pred_Name"], axis=1)

    y_list = configs["Y"]
    pred_list = configs["Model_Pred_Name"]

    if len(y_list) == 1:
        evaluation = pd.DataFrame({"MAPE": [Score_MAPE(dfs[y_list], dfs["Y_pred_" + col]) for col in pred_list],
                                   "MAE": [Score_MAE(dfs[y_list], dfs["Y_pred_" + col]) for col in pred_list],
                                   "Win": [len(dfs['Win'].loc[dfs['Win'] == col].index) for col in pred_list],
                                   "Model": pred_list})

    # NOTE: this function is only deal the loss is lower is better case
    col_name = configs['Model_Selection']['Loss_Function'] + "_Check"
    evaluation[col_name] = evaluation[configs['Model_Selection']['Loss_Function']] < configs['Model_Selection']['Threshold']

    evaluation["Data"] = data_type

    return evaluation


def Model_Judger(dfs, configs, mylogs):

    ### this code does not check if there are at least two models in config["Predict_Model_list"]
    ### this should be checked before this function.

    col_name = configs['Model_Selection']['Loss_Function'] + "_Check"

    loss_win = dfs['Model'].iloc[dfs[configs['Model_Selection']['Loss_Function']].idxmin()]
    amount_win = dfs['Model'].iloc[dfs['Win'].idxmax()]

    if loss_win == amount_win:
        sec_idx = dfs[configs['Model_Selection']['Loss_Function']].nsmallest(2).index.get_level_values(0)[1]
        fst_model = loss_win
        sec_model = dfs['Model'].iloc[sec_idx]

        # print(dfs.loc[dfs['Model'] == fst_model, col_name].values[0])
        if not dfs.loc[dfs['Model'] == fst_model, col_name].values[0]:
            msg = fst_model + "'s " + configs['Model_Selection']['Loss_Function'] + " does not reach the threshold."
            mylogs.error(msg)
            raise

        if not dfs.loc[dfs['Model'] == sec_model, col_name].values[0]:
            msg = sec_model + "'s " + configs['Model_Selection']['Loss_Function'] + " does not reach the threshold."
            mylogs.warning(msg)

        configs["Model_Selection"]["Predict_Model"] = fst_model
        mylogs.info("The Predict Model is " + fst_model)
        configs["Model_Selection"]["Baseline_Model"] = sec_model
        mylogs.info("The Baseline Model is " + sec_model)

    elif loss_win != amount_win:
        mylogs.warning("System can't not choose the predict model.")
        configs["Model_Selection"]["Predict_Model"] = amount_win
        configs["Model_Selection"]["Baseline_Model"] = loss_win
        
#        raise

    return configs

def Model_Selection(folder_path):
    ####### output path #######
    output_path = {}
    output_path["y_pred_merge_train"] = os.path.join(folder_path, "y_pred_merge_train.csv")
    output_path["y_pred_merge_test"] = os.path.join(folder_path, "y_pred_merge_test.csv")
    output_path["Model_Selection_Report"] = os.path.join(folder_path, "Report.csv")
    ############################
    
    input_path = read_path(folder_path)
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    mylog.init_logger()
    config = read_config(input_path["config_path"], mylog)

    mylog.info("Module -Model Selection- Start!")

    df_train_y = data_merge("Train", input_path["y_train_path"], input_path["Model_Folder"], config, mylog)
    df_train_y.to_csv(output_path['y_pred_merge_train'],index=False)
    df_test_y = data_merge("Test", input_path["y_test_path"], input_path["Model_Folder"], config, mylog)
    df_test_y.to_csv(output_path['y_pred_merge_test'], index=False)

    mylog.info("Data is ready.")

    df_result_test = Model_Socring(df_test_y, config, "Test")
    df_result_train = Model_Socring(df_train_y, config, "Train")
    df_result = pd.concat([df_result_train, df_result_test], ignore_index=True)
    df_result.to_csv(output_path['Model_Selection_Report'], index=False)

    config = Model_Judger(df_result_test, config, mylog)

    mylog.info("Module -Model Selection- Finished!")
    save_config(input_path["config_path"], config, mylog)

    return None


if __name__ == "__main__":
    path = "../Cases/CVD2EE/08_SelectModel/"
    Model_Selection(path)

################################################################################################################
#MXCI_MYCI_pre.py
# -*- coding: utf-8 -*-
import pandas as pd
from Formula import integrand
from scipy.integrate import quad
import numpy as np
from Read_path import read_path
from sklearn.externals import joblib
from config import read_config, save_config
from CreateLog import WriteLog
import os

def pre_MXCI_MYCI(data_folder):
    ####### output path #######
    output_path = {}
    output_path["MXCI_fig_path"] = os.path.join(data_folder, "MXCI_offline.png")
    output_path["MYCI_fig_path"] = os.path.join(data_folder, "MYCI_offline.png")
    output_path["light_fig_path"] = os.path.join(data_folder, "MXCI_MYCI_light_offline.png")
    output_path["MXCI_MYCI_offline_path"] = os.path.join(data_folder, "MXCI_MYCI_offline.csv")
    output_path["MXCI_x_train_path"] = os.path.join(data_folder, "MXCI_x_train.pkl")
    ############################
    
    input_path = read_path(data_folder)
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    mylog.init_logger()
    config = read_config(input_path["config_path"], mylog)
    
    mylog.info("-----MXCI_MYCI preparation-----")


    try:        
        x_train = pd.read_csv(input_path["x_train_path"])
        y_pred_merge_train = pd.read_csv(input_path["y_pred_merge_train"])
    except Exception as e:
        mylog.error("pre-MXCI MYCI calculation: Error while read training data")
        mylog.error_trace(e)
        raise
     
    try:
        ture_Y = config["Y"][0]
        Model1 = config["Model_Selection"]["Predict_Model"]
        Model2 = config["Model_Selection"]["Baseline_Model"]
        filter_feature = config["pre-CI"]["filter_feature"]
        add_exclude_feature = config["pre-CI"]["add_exclude_feature"]
    except Exception as e:
        mylog.error("pre-MXCI MYCI calculation: Error while reading parameters in config")
        mylog.error_trace(e)
        raise
        
    try:
        exlude_list, df_drop = preMXCI(x_train, config["Exclude_columns"], mylog, filter_feature, add_exclude_feature)
        RI_T, y_mean, y_std = preMYCI(y_pred_merge_train, ture_Y, Model1, Model2 , mylog, filter_feature)
    except Exception as e:
        mylog.error("pre-MXCI MYCI calculation: Error while load parameters from preMXCI and preMYCI ")
        mylog.error_trace(e)        
        raise

    try:
        config['MYCI'] = (RI_T, y_mean, y_std)
        config['MXCI_exclude_list'] = exlude_list
    except Exception as e:
        mylog.error("pre-MXCI MYCI calculation: Error while save parameters to config ")
        mylog.error_trace(e)        
        raise    

    joblib.dump(np.array(df_drop), output_path["MXCI_x_train_path"])
    save_config(input_path["config_path"], config, mylog)
        
    mylog.info("-----Pre-MXCI/MYCI Calculation Done-----")

    return None


def preMYCI(df_y, ture_Y, Model1, Model2 , mylog, filter_feature):
    if filter_feature:
        for key, value in filter_feature.items():
            df_y = df_y[df_y[key].isin(value)].reset_index(drop=True)     
         
    try:
        y_mean = df_y[ture_Y].mean()
        y_std = df_y[ture_Y].std()
        
        y_model_1 = df_y["Y_pred_"+Model1]
        y_model_2 = df_y["Y_pred_"+Model2]
    except Exception as e:
        mylog.error("pre-MYCI calculation: Error while reading y_train table ")
        mylog.error_trace(e)
        raise    
    
    y_model_1_scaled = (y_model_1-y_mean)/y_std
    y_model_2_scaled = (y_model_2-y_mean)/y_std   
    RI_train = []
    for i in range(len(y_model_1_scaled.values.tolist())):
        RI_train.append(quad(integrand, (y_model_1_scaled[i]+y_model_2_scaled[i])/2 , np.inf, args=(y_model_1_scaled[i],y_model_2_scaled[i]))[0])
    
    RI_T = min(RI_train)

    mylog.info("---MYCI info---")
    mylog.info("mean: "+str(y_mean))
    mylog.info("std: "+str(y_std))
    mylog.info("Threshold: "+str(RI_T))
    return RI_T, y_mean, y_std
    

def preMXCI(x_train, exlude_list, mylog, filter_feature, add_exclude_feature):
    df_train_drop = x_train.copy()

    # Keep only KPC data
#    point_ID = df_train_drop["Point_ID"].unique().tolist()[0]
#    df_train_drop = df_train_drop[df_train_drop["Point_ID"] == point_ID].reset_index(drop=True) 
    if filter_feature:
        for key, value in filter_feature.items():
            df_train_drop = df_train_drop[df_train_drop[key].isin(value)].reset_index(drop=True)
        
        
#    MQC_list = ['CVD MQC (AH_MP)','CVD MQC X' ,'CVD MQC Y']
    if exlude_list:
        exlude_list.extend(add_exclude_feature)
    for exclude in exlude_list:            
        if exclude in df_train_drop.columns:
            df_train_drop = df_train_drop.drop([exclude], axis=1)
    
    # drop the constant cols
    constant_col = []
    cols = df_train_drop.columns    
    for col in cols:
        if len(df_train_drop[col].unique().tolist()) == 1:
            constant_col.append(col)
    df_train_drop = df_train_drop.drop(constant_col, axis=1)

    # correlation coef filter
    coef = 0.7
    R = df_train_drop.corr().values
    df_R = pd.DataFrame(data=R)
    param_size = len(df_train_drop.columns.tolist())
    remove_list = []
    for i in range(param_size):
        idx = df_R[i].loc[abs(df_R[i])>coef].index.tolist()
        idx.remove(i)
        remove_list.extend(idx)  
    remove_list = list(set(remove_list))
#    print("length of removal list", len(remove_list))
#    print("number of left params", len(df_R.index)-len(remove_list))
    remove_index = df_train_drop.columns[[remove_list]].tolist()
    df_drop_list = df_train_drop.drop(columns=remove_index, axis=1).values.tolist()
    
    mylog.info("---MXCI info---")
    mylog.info("Exluded features:")
    mylog.info(",".join(str(e) for e in exlude_list))
    mylog.info("Constant features:")
    mylog.info(",".join(str(e) for e in constant_col))
    mylog.info("Highly correlated features:")
    mylog.info(",".join(str(e) for e in remove_index))        
        
    MXCI_exclude_list = exlude_list
    MXCI_exclude_list.extend(constant_col)
    MXCI_exclude_list.extend(remove_index)
    return MXCI_exclude_list, df_drop_list

#########################################################################################
#Formula.py
# -*- coding: utf-8 -*-
import math
import numpy as np

def integrand(x,a,b):
    sigma = 1
    mu = min(a,b)
    coef = 1/math.sqrt(2*np.pi)/sigma
    return 2*coef*math.exp(-0.5*((x-mu)/sigma)**2)

####################################################
#MXCI_MYCI.py
# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from scipy.integrate import quad
#from sklearn import preprocessing
from sklearn.neighbors import LocalOutlierFactor
from Formula import integrand
from Read_path import read_path
import matplotlib.pyplot as plt
from sklearn.externals import joblib
from config import read_config
from CreateLog import WriteLog
import os
plt.style.use('ggplot')
plt.ioff()

def MXCI_MYCI_offline(data_folder):    
    ####### output path #######
    output_path = {}
    output_path["MXCI_fig_path"] = os.path.join(data_folder, "MXCI_offline.png")
    output_path["MYCI_fig_path"] = os.path.join(data_folder, "MYCI_offline.png")
    output_path["light_fig_path"] = os.path.join(data_folder, "MXCI_MYCI_light_offline.png")
    output_path["MXCI_MYCI_offline_path"] = os.path.join(data_folder, "MXCI_MYCI_offline.csv")
    output_path["MXCI_x_train_path"] = os.path.join(data_folder, "MXCI_x_train.pkl")
    ############################
    
    input_path = read_path(data_folder)
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    mylog.init_logger()
    config = read_config(input_path["config_path"], mylog)
    
    mylog.info("-----MXCI_MYCI Offline-----") 

    try:
        Model1 = config["Model_Selection"]["Predict_Model"]
        Model2 = config["Model_Selection"]["Baseline_Model"]
        Model1 = "Y_pred_"+Model1
        Model2 = "Y_pred_"+Model2
        filter_feature = config["pre-CI"]["filter_feature"]
        RI_T, y_mean, y_std = config['MYCI']
        MXCI_exclude_list = config['MXCI_exclude_list']            
    except Exception as e:
        mylog.error("MXCI MYCI calculation: Error while reading parameters in config")
        mylog.error_trace(e)
        raise
    
    try:
        df_drop_np = joblib.load(output_path["MXCI_x_train_path"])
    except Exception as e:
        mylog.error("MXCI MYCI calculation: Error while loading MXCI training data")
        mylog.error_trace(e)
        raise        
    
    try:
        x_train = pd.read_csv(input_path["x_train_path"])
        x_test = pd.read_csv(input_path["x_test_path"])
        y_pred_train = pd.read_csv(input_path["y_pred_merge_train"])
        y_pred_test = pd.read_csv(input_path["y_pred_merge_test"])
    except Exception as e:
        mylog.error("MXCI MYCI calculation: Error while reading data")
        mylog.error_trace(e)
        raise        
    
    x_data = pd.concat([x_train, x_test]).reset_index(drop=True)
    y_data = pd.concat([y_pred_train, y_pred_test]).reset_index(drop=True)
    
    ex_cols = config["Exclude_columns"]
    ex_data = x_data[ex_cols]
    
    if filter_feature:
        for key, value in filter_feature.items():
            x_data = x_data[x_data[key].isin(value)].reset_index(drop=True)
            y_data = y_data[y_data[key].isin(value)].reset_index(drop=True)
            x_train = x_train[x_train[key].isin(value)].reset_index(drop=True)
    
    x_data = x_data.drop( MXCI_exclude_list, axis=1)
    
    MXCI_line, MXCI_T_line, MYCI_line, light = MXCI_MYCI_offline_judge(x_data, y_data, Model1, Model2, RI_T, y_mean, y_std, df_drop_np)

    edge_line = len(x_train.index.tolist())
    
    try:
        # MXCI
        fig, ax = plt.subplots(figsize=(10,6))
        ax.plot(MXCI_line, color="b", marker="o", label = "MXCI")
        ax.plot(MXCI_T_line, color="r", marker="o", label = "MXCI Threshold")
        ax.axvline(x = edge_line, c="g", label="Train/Test Split Line")
        ax.legend()
        ax.set_title("MXCI")
        plt.savefig(output_path["MXCI_fig_path"])
        plt.clf()
        
        # MYCI
        fig, ax = plt.subplots(figsize=(10,6))
        ax.plot(MYCI_line, color="b", marker="o", label = "MYCI")
        ax.axhline(y=RI_T, c = "r", label = "MYCI Threshold" )
        ax.axvline(x = edge_line, c="g", label="Train/Test Split Line")
        ax.legend()
        ax.set_title("MYCI")
        plt.savefig(output_path["MYCI_fig_path"]) 
        plt.clf()    
    
        # Light
        fig, ax = plt.subplots(figsize=(10,6))
        color = ["g", "b", "orange", "r"]
        label = ["Green light", "Blue Light", "Yellow Light", "Red Light"]
        for j in range(4):
            bars = [k for k in light if k==j+1]
            bars_pos = [i for i, k in enumerate(light) if k==j+1]
            if len(bars) > 0:
                ax.scatter(bars_pos,bars, color=color[j], label = label[j])
    #            ax.bar(bars_pos,bars, color=color[j], label = label[j])      
        ax.axvline(x = edge_line+0.5, c="g", label="Train/Test Split Line")
        ax.legend()
        ax.set_title("MXCI/MYCI Lights")
        plt.savefig(output_path["light_fig_path"])
        plt.clf()    
    except Exception as e:
        mylog.warning("MXCI MYCI calculation: Error while producing plots")
        mylog.warning_trace(e)
    
    MXCI_MYCI_offline = pd.DataFrame({
            "MXCI": np.array(MXCI_line),
            "MXCI Threshold" : np.array(MXCI_T_line),
            "MYCI": np.array(MYCI_line),
            "MYCI Threshold" : np.array([RI_T]*len(MYCI_line)),
            "Light" : np.array(light)
            })
    for col in ex_cols:
        MXCI_MYCI_offline[col] = ex_data.loc[:, col]
        
    MXCI_MYCI_offline.to_csv(output_path["MXCI_MYCI_offline_path"], index=False)

    mylog.info("MXCI MYCI is stored at: "+output_path["MXCI_MYCI_offline_path"])
    mylog.info("-----MXCI_MYCI Offline Done-----")

    return None


def MXCI_MYCI_offline_judge(x_data, y_data, Model1, Model2, RI_T, y_mean, y_std, df_drop_np):
    MXCI_line = []
    MXCI_T_line = []
    MYCI_line = []
    light = []   

    for i in x_data.index.tolist():
        y_model_1 = y_data.loc[i, Model1]
        y_model_2 = y_data.loc[i, Model2]
        x_test = x_data.loc[i, :]
        MYCI_index, RI = MYCI(RI_T, y_mean, y_std, y_model_1, y_model_2)
        MYCI_line.append(RI)
        MXCI_index, GSI, GSI_T = MXCI(df_drop_np, x_test)
        MXCI_line.append(GSI)
        MXCI_T_line.append(GSI_T)
        
        if MYCI_index:
            if MXCI_index:
                light.append(1)
#                print("Green Light")
#                return 1
            else:
                light.append(2)
#                print("Blue Light")
#                return 2
        else:
            if MXCI_index:
                light.append(3)
#                print("Yellow Light")
#                return 3
            else:
                light.append(4)
#                print("Red Light")
#                return 4
                
    return  MXCI_line, MXCI_T_line, MYCI_line, light

### TO-DO
def MXCI_MYCI_online_judge(x_data, y_data, Model1, Model2, RI_T, y_mean, y_std, df_drop_np, filter_feature):
    MXCI_line = []
    MXCI_T_line = []
    MYCI_line = []
    light = []
    if not filter_feature:
        for key, value in filter_feature.items():
            x_data = x_data[x_data[key].isin(value)].reset_index(drop=True)
            y_data = y_data[y_data[key].isin(value)].reset_index(drop=True)   

    for i in x_data.index.tolist():
        y_model_1 = y_data.loc[i, Model1]
        y_model_2 = y_data.loc[i, Model2]
        x_test = x_data.loc[i, :]
        MYCI_index, RI = MYCI(RI_T, y_mean, y_std, y_model_1, y_model_2)
        MYCI_line.append(RI)
        MXCI_index, GSI, GSI_T = MXCI(df_drop_np, x_test)
        MXCI_line.append(GSI)
        MXCI_T_line.append(GSI_T)
        
        if MYCI_index:
            if MXCI_index:
                light.append(1)
#                print("Green Light")
#                return 1
            else:
                light.append(2)
#                print("Blue Light")
#                return 2
        else:
            if MXCI_index:
                light.append(3)
#                print("Yellow Light")
#                return 3
            else:
                light.append(4)
#                print("Red Light")
#                return 4
                
    return  MXCI_line, MXCI_T_line, MYCI_line, light


def MYCI(RI_T, y_mean, y_std, y_model_1, y_model_2):
    y_model_1_scaled = (y_model_1-y_mean)/y_std
    y_model_2_scaled = (y_model_2-y_mean)/y_std   
    RI = quad(integrand, (y_model_1_scaled+y_model_2_scaled)/2 , np.inf, args=(y_model_1_scaled,y_model_2_scaled))[0]
    
    RI_index = True   
    if RI < RI_T:
        RI_index = False
    
#    print("MYCI threshold is "+"{0:.5f}".format(RI_T)+", and MYCI is "+"{0:.5f}".format(RI))
    return RI_index, RI


def MXCI(df_drop_np, x_test):  

    clf_LOF = LocalOutlierFactor(n_neighbors=5, contamination=0.05)
    clf_LOF.fit_predict(np.vstack((df_drop_np, x_test.values)))
    X_scores = clf_LOF.negative_outlier_factor_
    GSI_T = min(X_scores[:-1])
    GSI_index = True
    if X_scores[-1] < GSI_T:
        GSI_index = False
#    print("MXCI threshold is "+"{0:.5f}".format(GSI_T)+", and MXCI is "+"{0:.5f}".format(X_scores[-1]))
    return GSI_index, X_scores[-1] , GSI_T
