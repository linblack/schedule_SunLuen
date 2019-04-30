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
