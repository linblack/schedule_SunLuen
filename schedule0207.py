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


   
