# SmartVM_Constructor_dispatching.py
# -*- coding: utf-8 -*-
#20190822 open 8 program for offline & retrain
#20190822 1643 update project_id and training server
#20190823 1017 add retrain dispatching
import time
import os
from DB_operation import select_project_creating, select_project_waitcreate, select_project_model_confirmok_o2m, select_project_model_confirmok_m2m, select_project_waitretrain, update_project_STATUS_trainingserver_by_projectid 
import socket

if __name__ == '__main__': 
    def timer(n):
        while True:     
            #20190730 control process number, no execute while no model need training 
            SVM_PROJECT_RUN = select_project_creating()            
            
            hostname = socket.gethostname()
            ip = socket.gethostbyname(hostname)
            
            if len(SVM_PROJECT_RUN) < 9:
                
                SVM_PROJECT_WAITCREATE = select_project_waitcreate()
                SVM_PROJECT_CONFIRMOK_O2M = select_project_model_confirmok_o2m()
                SVM_PROJECT_CONFIRMOK_M2M = select_project_model_confirmok_m2m() 
                SVM_PROJECT_WAITRETRAIN =  select_project_waitretrain()
                
                if len(SVM_PROJECT_WAITCREATE) != 0:
                    project_id = SVM_PROJECT_WAITCREATE.PROJECT_ID[0]
                    update_project_STATUS_trainingserver_by_projectid('CREATEDD', ip, project_id) #20190822 update project status to CREATEDD, avoid mutual grab
                    
                    Execute = r"start SmartVM_Constructor.bat"
                    os.system(Execute)
                    print("open_finish_for_WAITCREATE")
                    time.sleep(n)    
                elif len(SVM_PROJECT_CONFIRMOK_O2M) != 0:
                    #20190823 training server must meets the local ip
                    if SVM_PROJECT_CONFIRMOK_O2M.TRAINING_SERVER[0] == ip:
                        Execute = r"start SmartVM_Constructor.bat"
                        os.system(Execute)
                        print("open_finish_for_confirmok_o2m")
                        time.sleep(n)
                    else:
                        print("please check o2m training server")
                
                elif len(SVM_PROJECT_CONFIRMOK_M2M) != 0:
                    if SVM_PROJECT_CONFIRMOK_M2M.TRAINING_SERVER[0] == ip:
                        Execute = r"start SmartVM_Constructor.bat"
                        os.system(Execute)
                        print("open_finish_for_confirmok_m2m")
                        time.sleep(n)
                    else:
                        print("please check m2m training server")
                #20190823 dispatch retrain
                elif len(SVM_PROJECT_WAITRETRAIN) != 0:
                    if SVM_PROJECT_WAITRETRAIN.TRAINING_SERVER[0] == ip:
                        Execute = r"start SmartVM_Constructor_Retrain.bat"
                        os.system(Execute)
                        print("open_finish_for_WAITRETRAIN")
                        time.sleep(n)
                    else:
                        print("please check retrain training server")
                
                else:
                    print("no model need train")
                    time.sleep(n)
                                                                   
            else:
                print("train model full")
                time.sleep(n*6)
            
    timer(20)  
    
    
    
# SmartVM_Constructor_mp_v4.py
#20190822 error stop[return 1]
#20190822 1518 mark dataremove
#20190822 1540 MXCI_MYCI_offline return None error
#20190827 error echanism work[MODEL_STATUS, o2o&o2m&m2m]
#20190828 modify model_status location(ok:last, error&NG:advance) 
#20190830 add XDI NG confirm[o2o&o2m, m2m not]
#20190802 DataRomove errormessage not update to db
#20190919 add ftp
#20190924 add estone
#20190926 modify model_selection[add fail, True->1]
#20191012 add training according to error step[o2o,o2m] 

from Data_Preview import Data_Preview
from Data_PreProcess import Data_PreProcess_Train, Data_PreProcess_Test
from XDI import XDI_off_line_report, Build_XDI_Model
from YDI import YDI_off_line_report, Build_YDI_Model
from MXCI_MYCI_pre import pre_MXCI_MYCI
from MXCI_MYCI import MXCI_MYCI_offline
from Model_Selection import Model_Selection
from prediction import modelpredict
from GP_TPE_tune import Model_tuning
from train import modeltrain
from Read_path import read_path
from config import read_config
from CreateLog import WriteLog
from DB_operation import DB_Connection, select_project_by_status_ip, select_project_model_by_modelid, select_project_model_by_modelname, select_project_maxmodel_by_projectid, select_project_model_confirmok_o2m, select_project_model_confirmok_m2m, select_project_with_model_by_projectid, update_project_STATUS_by_projectid, update_project_model_modelstatus_by_modelid, update_project_model_modelstatus_modelstep_waitconfirm_by_modelid, update_project_model_mae_mape_by_modelid, update_project_model_errorconfirm_by_projectid, update_project_model_errorconfirm_errormessage_by_modelid, insert_estone_projectid_mapkey_detectedtime_estonetime_estoneuserid_estonetaskid_estonestatus
import SystemConfig.ServerConfig as ServerConfig # Reaver
from Estone import Estone_post_web

import os
import json
import traceback
from Path import All_path
from Data_Collector import Data_Collector
# from Data_Check import Data_Check
from Exclusion import DataRemove

#from shutil import copyfile
import pandas as pd
import time
import datetime
from json2csv import json2csv
import zipfile
import socket
from ftp import ftp #20190919 add ftp dowload function
#import multiprocessing as mp


class SuperVM():
    def __init__(self, sorce_dir, train_data_name, config_file_name, batch_data_name_list=None):
        self.base_path = os.path.join("../Cases/", sorce_dir)
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
        #20190829 build retrain_data.csv cancel
        """
        self.retrain_data = os.path.join(self.base_path, "retrain_data.csv")
        if not os.path.exists(self.retrain_data):
            copyfile(self.train_data, self.retrain_data)
        """
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
        self.save_path_config()
        return None
    
    def create_many_path_pause(self, batch_flag=None, training_data_dict=None):
        
        self.filter_dir_name = {}
        for feature_list in self.feature_lists:
            self.filter_dir_name[feature_list] = self.base_name + "_" + self.filter_feature + "_" + str(feature_list)
            
        return None

    def get_saved_path_config(self):
        try:
            with open(self.in_path) as json_data:
                self.path_config = json.load(json_data)
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
        
    def zip_project(self, project_id):
        
        SourcePath = os.path.join("D:/SVM/Cases/", str(project_id))
        Sourcefoldername = str(project_id)
        ZipTopath = r"D:\SVM\model_out"
        
        os.chdir(ZipTopath)
        
        def zipdir(path, ziph):
            # ziph is zipfile handle
            for root, dirs, files in os.walk(path):
                for file in files:
                    ziph.write(os.path.join(root, file))
                    
        zipf = zipfile.ZipFile(str(Sourcefoldername) + '.zip', 'w', zipfile.ZIP_DEFLATED)
        zipdir(SourcePath, zipf)
        zipf.close()

    def model(self, path_dict):
        base_num = 0
        input_path = read_path(path_dict['6']["PLS"][str(base_num+2)])
        mylog = WriteLog(input_path["log_path"], input_path["error_path"])     
        
        Step_start = datetime.datetime.now()
            
        Model_tuning(path_dict['6']["XGB"][str(base_num)], 'XGB')
        modeltrain(path_dict['6']["XGB"][str(base_num + 1)], 'XGB')
        modelpredict(path_dict['6']["XGB"][str(base_num + 2)], 'XGB', "train")
        modelpredict(path_dict['6']["XGB"][str(base_num + 2)], 'XGB', "test")
        
        Step_end = datetime.datetime.now()
        Step_interval = Step_end - Step_start
        mylog.info("Step4_XGB: " + str(Step_interval))
        
        Step_start = datetime.datetime.now()

        Model_tuning(path_dict['6']["PLS"][str(base_num)], 'PLS')
        modeltrain(path_dict['6']["PLS"][str(base_num + 1)], 'PLS')
        modelpredict(path_dict['6']["PLS"][str(base_num + 2)], 'PLS', "train")
        modelpredict(path_dict['6']["PLS"][str(base_num + 2)], 'PLS', "test")
        
        Step_end = datetime.datetime.now()
        Step_interval = Step_end - Step_start
        mylog.info("Step4_PLS: " + str(Step_interval))
        
        return "OK"
    
    def model_merge(self, path_dict, for_batch=None):
        base_num = 3
        
        modeltrain(path_dict['6']["XGB"][str(base_num + 1)], 'XGB')
        modelpredict(path_dict['6']["XGB"][str(base_num + 2)], 'XGB', "train")

        modeltrain(path_dict['6']["PLS"][str(base_num + 1)], 'PLS')
        modelpredict(path_dict['6']["PLS"][str(base_num + 2)], 'PLS', "train")       
        return None

    def training_phase(self, project_id, dc_instance, retrain_num, batch_num=None):
        retrain_num = 0
        batch_num = 0
        path_dict = self.path_config[self.base_name][str(retrain_num)][str(batch_num)]
        #20200103 modify to read ServerConfig
        cnxn2 = DB_Connection(server_name=ServerConfig.SmartPrediction_DBServer_IP, db_name=ServerConfig.SmartPrediction_Config_DB)
        
        SVM_PP_MODEL = select_project_with_model_by_projectid(project_id)
        user_deptid = SVM_PP_MODEL.USER_DEPTID[0]
        project_title = SVM_PP_MODEL.PROJECT_TITLE[0]
        user_empno = SVM_PP_MODEL.USER_EMPNO[0]
        
        train_flag = '0'
        test_flag = '1'
        
        #20190815 add Step time interval[o2m]
        Step_start = datetime.datetime.now()
        
        Data_Preview_status, Data_Preview_msg = Data_Preview(path_dict['2']["main"])
        
        input_path = read_path(path_dict['2']["main"])
        mylog = WriteLog(input_path["log_path"], input_path["error_path"])  
        
        json2csv(input_path["config_path"]) #20190702 update and output config.csv
        dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="DataPreview",
                                         retrain_num=retrain_num, batch_num=batch_num)       
        
        print(Data_Preview_status)
        if Data_Preview_status == "NG":
                
            cursor1 = cnxn2.cursor()
            cursor1.execute("UPDATE SVM_PROJECT_MODEL SET MODEL_SEQ = ?, MODEL_STATUS = ?, BATCH_SEQ = ?, IS_ONLINE = ?, MODEL_STEP = ?, WAIT_CONFIRM = ? WHERE MODEL_ID = ?", str(retrain_num), '1', str(batch_num), 0 , '1', 1 , int(SVM_PP_MODEL.MODEL_ID[0]))
            cnxn2.commit()            
            update_project_STATUS_by_projectid("CREATING_PAUSE", project_id) #20190729 Because NG, change project status, return function and not execute down   
            update_project_model_errorconfirm_errormessage_by_modelid(0, Data_Preview_msg, SVM_PP_MODEL.MODEL_ID[0])            
            mylog.info("Data_Preview_status NG")  
            
            #20190924 add estone
            """
            model_step = '1'
            map_key, plan_start_time, estone_taskid, estone_status = Estone_post_web(project_id, model_step, user_deptid, project_title, user_empno)
            if estone_status is not None:
                insert_estone_projectid_mapkey_detectedtime_estonetime_estoneuserid_estonetaskid_estonestatus(project_id, map_key, plan_start_time, plan_start_time, user_empno, estone_taskid, estone_status)
            """
            return 1        
        else:              
            cursor1 = cnxn2.cursor()
            cursor1.execute("UPDATE SVM_PROJECT_MODEL SET MODEL_SEQ = ?, MODEL_STATUS = ?, BATCH_SEQ = ?, IS_ONLINE = ?, MODEL_STEP = ?, WAIT_CONFIRM = ? WHERE MODEL_ID = ?", str(retrain_num), '1', str(batch_num), 0, '1', 0, int(SVM_PP_MODEL.MODEL_ID[0]))
            cnxn2.commit()  
        
        Step_end = datetime.datetime.now()
        Step_interval = Step_end - Step_start
        print('Step1: ' + str(Step_interval))
        mylog.info("Step1: " + str(Step_interval))
        
        Step_start = datetime.datetime.now()
        #20190806 show error message and update project status to CREATING_ERROR, 20190820 combine peter version
        Data_PreProcess_Train_check, Data_PreProcess_Train_msg = Data_PreProcess_Train(path_dict['3'][train_flag], mode="Train")
        if Data_PreProcess_Train_check == 'ERROR':
            update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
            #20190809 when error update error_confirm[1] and error_message to model, 20190822 error and stop program[return 1]
            update_project_model_errorconfirm_errormessage_by_modelid(1, Data_PreProcess_Train_msg, SVM_PP_MODEL.MODEL_ID[0])
            update_project_model_modelstatus_by_modelid('2', SVM_PP_MODEL.MODEL_ID[0])
            return 1
            
        Data_PreProcess_Test_check, Data_PreProcess_Test_msg = Data_PreProcess_Test(path_dict['3'][test_flag])
        if Data_PreProcess_Test_check == 'ERROR':
            update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
            update_project_model_errorconfirm_errormessage_by_modelid(1, Data_PreProcess_Test_msg, SVM_PP_MODEL.MODEL_ID[0])
            update_project_model_modelstatus_by_modelid('2', SVM_PP_MODEL.MODEL_ID[0])
            return 1
        
        Step_end = datetime.datetime.now()
        Step_interval = Step_end - Step_start
        print('Step2: ' + str(Step_interval))
        mylog.info("Step2: " + str(Step_interval))
        
        update_project_model_modelstatus_by_modelid('2', SVM_PP_MODEL.MODEL_ID[0])      
        
        Step_start = datetime.datetime.now()
                     
        Build_XDI_Model_check, Build_XDI_Model_msg = Build_XDI_Model(path_dict['4'][train_flag])
        if Build_XDI_Model_check == 'ERROR':
            update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
            update_project_model_errorconfirm_errormessage_by_modelid(1, Build_XDI_Model_msg, SVM_PP_MODEL.MODEL_ID[0])
            update_project_model_modelstatus_by_modelid('3', SVM_PP_MODEL.MODEL_ID[0])
            return 1
                           
        XDI_off_line_report_check, XDI_off_line_report_msg = XDI_off_line_report(path_dict['4'][test_flag], mode="Train")
        if XDI_off_line_report_check == 'ERROR':
            update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
            update_project_model_errorconfirm_errormessage_by_modelid(1, XDI_off_line_report_msg, SVM_PP_MODEL.MODEL_ID[0])
            update_project_model_modelstatus_by_modelid('3', SVM_PP_MODEL.MODEL_ID[0])
            return 1
                
        json2csv(input_path["config_path"])        
        dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="XDI",
                                         retrain_num=retrain_num, batch_num=batch_num)
        
        Step_end_xdi = datetime.datetime.now()
        Step_interval = Step_end_xdi - Step_start
        print('Step3_XDI: ' + str(Step_interval))
        mylog.info("Step3_XDI: " + str(Step_interval))
                     
        Build_YDI_Model_check, Build_YDI_Model_msg = Build_YDI_Model(path_dict['5'][train_flag])
        if Build_YDI_Model_check == 'ERROR':
            update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
            update_project_model_errorconfirm_errormessage_by_modelid(1, Build_YDI_Model_msg, SVM_PP_MODEL.MODEL_ID[0])
            update_project_model_modelstatus_by_modelid('3', SVM_PP_MODEL.MODEL_ID[0])
            return 1
            
        YDI_off_line_report_status, YDI_off_line_report_msg = YDI_off_line_report(path_dict['5'][test_flag], mode="Train")
        
        json2csv(input_path["config_path"])
        dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="YDI",
                                         retrain_num=retrain_num, batch_num=batch_num)
        
        if YDI_off_line_report_status == 'ERROR':
            update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
            update_project_model_errorconfirm_errormessage_by_modelid(1, YDI_off_line_report_msg, SVM_PP_MODEL.MODEL_ID[0])
            update_project_model_modelstatus_by_modelid('3', SVM_PP_MODEL.MODEL_ID[0])
            return 1
        #20190830 add XDI NG confirm
        elif XDI_off_line_report_check == "NG":
            update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('3', '3', 1, SVM_PP_MODEL.MODEL_ID[0])            
            update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)
            update_project_model_errorconfirm_errormessage_by_modelid(0, XDI_off_line_report_msg, SVM_PP_MODEL.MODEL_ID[0])            
            mylog.info("XDI_off_line_report_status NG") 
            """
            model_step = '3'
            map_key, plan_start_time, estone_taskid, estone_status = Estone_post_web(project_id, model_step, user_deptid, project_title, user_empno)
            if estone_status is not None:
                insert_estone_projectid_mapkey_detectedtime_estonetime_estoneuserid_estonetaskid_estonestatus(project_id, map_key, plan_start_time, plan_start_time, user_empno, estone_taskid, estone_status)
            """
            return 1  
        elif YDI_off_line_report_status == "NG":
            update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('3', '3', 1, SVM_PP_MODEL.MODEL_ID[0])            
            update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)
            update_project_model_errorconfirm_errormessage_by_modelid(0, YDI_off_line_report_msg, SVM_PP_MODEL.MODEL_ID[0])            
            mylog.info("YDI_off_line_report_status NG")  
            """
            model_step = '3'
            map_key, plan_start_time, estone_taskid, estone_status = Estone_post_web(project_id, model_step, user_deptid, project_title, user_empno)
            if estone_status is not None:
                insert_estone_projectid_mapkey_detectedtime_estonetime_estoneuserid_estonetaskid_estonestatus(project_id, map_key, plan_start_time, plan_start_time, user_empno, estone_taskid, estone_status)
            """
            return 1
        else:
            update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('3', '3', 0, SVM_PP_MODEL.MODEL_ID[0])
        
        dataremove_xdi_status, dataremove_xdi_msg = DataRemove(path_dict['4']["1"], "XDI")  #20190703 remove after step3
        dataremove_ydi_status, dataremove_ydi_msg = DataRemove(path_dict['5']["1"], "YDI")  #20190703 remove after step3

        Step_end = datetime.datetime.now()
        Step_interval = Step_end - Step_end_xdi
        print('Step3_YDI: ' + str(Step_interval))
        mylog.info("Step3_YDI: " + str(Step_interval))
        
        Step_interval = Step_end - Step_start
        print('Step3: ' + str(Step_interval))
        mylog.info("Step3: " + str(Step_interval))        
        
        Step_start = datetime.datetime.now()
         
        model_status = self.model(path_dict) #20190806 xgb & pls execute success will return OK
        if model_status != "OK":
            mylog.error("model error")
            update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
            update_project_model_errorconfirm_errormessage_by_modelid(1, "model error", SVM_PP_MODEL.MODEL_ID[0])
            update_project_model_modelstatus_by_modelid('4', SVM_PP_MODEL.MODEL_ID[0])
            return 1
            
        json2csv(input_path["config_path"])
        dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="Model",
                                         retrain_num=retrain_num, batch_num=batch_num)
        
        Step_end = datetime.datetime.now()
        Step_interval = Step_end - Step_start
        print('Step4: ' + str(Step_interval))
        mylog.info("Step4: " + str(Step_interval))
        
        update_project_model_modelstatus_by_modelid('4', SVM_PP_MODEL.MODEL_ID[0])
        
        Step_start = datetime.datetime.now()
        
        Model_Selection_check, Model_Selection_msg, win = Model_Selection(path_dict['7'][train_flag], mode="Train")
        output_paths = {}
        
        json2csv(input_path["config_path"])        
        dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="SelectModel",
                                         retrain_num=retrain_num, batch_num=batch_num)
        
        if Model_Selection_check == 'ERROR':
            update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
            update_project_model_errorconfirm_errormessage_by_modelid(1, Model_Selection_msg, SVM_PP_MODEL.MODEL_ID[0])
            update_project_model_modelstatus_by_modelid('5', SVM_PP_MODEL.MODEL_ID[0])
            return 1
        elif Model_Selection_check == "NG":
            update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('5', '5', 1, SVM_PP_MODEL.MODEL_ID[0])  
            update_project_model_errorconfirm_errormessage_by_modelid(0, Model_Selection_msg, SVM_PP_MODEL.MODEL_ID[0])
            update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)            
            mylog.info("Model_Selection NG")
            """
            model_step = '5'
            map_key, plan_start_time, estone_taskid, estone_status = Estone_post_web(project_id, model_step, user_deptid, project_title, user_empno)
            if estone_status is not None:
                insert_estone_projectid_mapkey_detectedtime_estonetime_estoneuserid_estonetaskid_estonestatus(project_id, map_key, plan_start_time, plan_start_time, user_empno, estone_taskid, estone_status)            
            """
            return 1
        elif Model_Selection_check == "Fail":
            update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('5', '5', 2, SVM_PP_MODEL.MODEL_ID[0])  
            update_project_model_errorconfirm_errormessage_by_modelid(0, Model_Selection_msg, SVM_PP_MODEL.MODEL_ID[0])       
            mylog.info("Model_Selection Fail")                     
        else:
            update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('5', '5', 0, SVM_PP_MODEL.MODEL_ID[0])
        
        #20190701 modify in the future(use XGB, PLS current),read config after user select 
        if str(win).upper() == 'XGB':           
            output_paths["Model_Selection_Report"] = os.path.join(path_dict['7'][train_flag], "Model_Selection_Report.csv")
            select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
            out_df = select_df[(select_df["Model"] == 'XGB') & (select_df["Data"] == 'Test')]            
            update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PP_MODEL.MODEL_ID[0])
        elif str(win).upper() == 'PLS':
            output_paths["Model_Selection_Report"] = os.path.join(path_dict['7'][train_flag], "Model_Selection_Report.csv")
            select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
            out_df = select_df[(select_df["Model"] == 'PLS') & (select_df["Data"] == 'Test')]            
            update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PP_MODEL.MODEL_ID[0])
        else:
            input_path = read_path(path_dict[str(7)]['0']) #20190710 avoid flat don't save mae mape to db
            mylog = WriteLog(input_path["log_path"], input_path["error_path"])     
            config = read_config(input_path["config_path"], mylog)  
            Predict_Model = config["Model_Selection"][config["Y"][0]]["Predict_Model"]
            
            if str(Predict_Model).upper() == 'XGB':           
                output_paths["Model_Selection_Report"] = os.path.join(path_dict['7'][train_flag], "Model_Selection_Report.csv")
                select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
                out_df = select_df[(select_df["Model"] == 'XGB') & (select_df["Data"] == 'Test')]               
                update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PP_MODEL.MODEL_ID[0])
            elif str(Predict_Model).upper() == 'PLS':
                output_paths["Model_Selection_Report"] = os.path.join(path_dict['7'][train_flag], "Model_Selection_Report.csv")
                select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
                out_df = select_df[(select_df["Model"] == 'PLS') & (select_df["Data"] == 'Test')]               
                update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PP_MODEL.MODEL_ID[0])
        
        Step_end = datetime.datetime.now()
        Step_interval = Step_end - Step_start
        print('Step5: ' + str(Step_interval))
        mylog.info("Step5: " + str(Step_interval))
        
        Step_start = datetime.datetime.now()
        
        pre_mxci_myci_status, pre_mxci_myci_msg = pre_MXCI_MYCI(path_dict['8'][train_flag])
        if pre_mxci_myci_status == "NG":
            update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
            update_project_model_errorconfirm_errormessage_by_modelid(1, pre_mxci_myci_msg, SVM_PP_MODEL.MODEL_ID[0])
            update_project_model_modelstatus_by_modelid('6', SVM_PP_MODEL.MODEL_ID[0])
            return 1
        
        Step_end = datetime.datetime.now()
        Step_interval = Step_end - Step_start
        print('Step6: ' + str(Step_interval))
        mylog.info("Step6: " + str(Step_interval))
        
        update_project_model_modelstatus_by_modelid('6', SVM_PP_MODEL.MODEL_ID[0])
        
        Step_start = datetime.datetime.now()
                
        mxci_myci_offline_status, mxci_myci_offline_msg = MXCI_MYCI_offline(path_dict['8'][test_flag], mode="Train")
        if mxci_myci_offline_status == "NG":
            update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
            update_project_model_errorconfirm_errormessage_by_modelid(1, mxci_myci_offline_msg, SVM_PP_MODEL.MODEL_ID[0])
            update_project_model_modelstatus_by_modelid('7', SVM_PP_MODEL.MODEL_ID[0])
            return 1
            
        json2csv(input_path["config_path"])
        dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="CI",
                                         retrain_num=retrain_num, batch_num=batch_num)
        
        Step_end = datetime.datetime.now()
        Step_interval = Step_end - Step_start
        print('Step7: ' + str(Step_interval))
        mylog.info("Step7: " + str(Step_interval))
        
        update_project_model_modelstatus_by_modelid('7', SVM_PP_MODEL.MODEL_ID[0])
        
        return None
    
    def training_phase_pause(self, project_id, dc_instance, retrain_num, batch_num=None):
        retrain_num = 0
        batch_num = 0
        
        path_dict = self.path_config[self.base_name][str(retrain_num)][str(batch_num)]
        
        SVM_PP_MODEL = select_project_with_model_by_projectid(project_id)
        user_deptid = SVM_PP_MODEL.USER_DEPTID[0]
        project_title = SVM_PP_MODEL.PROJECT_TITLE[0]
        user_empno = SVM_PP_MODEL.USER_EMPNO[0]
        
        train_flag = '0'
        test_flag = '1'
        input_path = read_path(path_dict['2']["main"])
        mylog = WriteLog(input_path["log_path"], input_path["error_path"])
        
        #20190731 check step1 3 5, update creating_run and test
        if SVM_PP_MODEL.MODEL_STEP[0] == '1':
            #if SVM_PP_MODEL.WAIT_CONFIRM[0] == True:
            if SVM_PP_MODEL.WAIT_CONFIRM[0] == 1:
                time.sleep(10) #20190802 avoid execute the same project
                update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)                                
                return 1
            else:    
                if (SVM_PP_MODEL.MODEL_STATUS[0] in ('1','2')) and (SVM_PP_MODEL.ERROR_CONFIRM[0] == False):
                                
                    Data_PreProcess_Train_check, Data_PreProcess_Train_msg = Data_PreProcess_Train(path_dict['3'][train_flag], mode="Train")
                    if Data_PreProcess_Train_check == 'ERROR':
                        update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                        update_project_model_errorconfirm_errormessage_by_modelid(1, Data_PreProcess_Train_msg, SVM_PP_MODEL.MODEL_ID[0])
                        update_project_model_modelstatus_by_modelid('2', SVM_PP_MODEL.MODEL_ID[0])
                        return 1
                                        
                    Data_PreProcess_Test_check, Data_PreProcess_Test_msg = Data_PreProcess_Test(path_dict['3'][test_flag])
                    if Data_PreProcess_Test_check == 'ERROR':
                        update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                        update_project_model_errorconfirm_errormessage_by_modelid(1, Data_PreProcess_Test_msg, SVM_PP_MODEL.MODEL_ID[0])
                        update_project_model_modelstatus_by_modelid('2', SVM_PP_MODEL.MODEL_ID[0])
                        return 1
                    
                    update_project_model_modelstatus_by_modelid('2', SVM_PP_MODEL.MODEL_ID[0])
                
                SVM_PP_MODEL1 = select_project_with_model_by_projectid(project_id)
                
                if (SVM_PP_MODEL1.MODEL_STATUS[0] in ('2','3')) and (SVM_PP_MODEL1.ERROR_CONFIRM[0] == False):
                                                            
                    Build_XDI_Model_check, Build_XDI_Model_msg = Build_XDI_Model(path_dict['4'][train_flag])
                    if Build_XDI_Model_check == 'ERROR':
                        update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                        update_project_model_errorconfirm_errormessage_by_modelid(1, Build_XDI_Model_msg, SVM_PP_MODEL.MODEL_ID[0])
                        update_project_model_modelstatus_by_modelid('3', SVM_PP_MODEL.MODEL_ID[0])
                        return 1
                                     
                    XDI_off_line_report_check, XDI_off_line_report_msg = XDI_off_line_report(path_dict['4'][test_flag], mode="Train")
                    if XDI_off_line_report_check == 'ERROR':
                        update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                        update_project_model_errorconfirm_errormessage_by_modelid(1, XDI_off_line_report_msg, SVM_PP_MODEL.MODEL_ID[0])
                        update_project_model_modelstatus_by_modelid('3', SVM_PP_MODEL.MODEL_ID[0])
                        return 1
                    
                    json2csv(input_path["config_path"])        
                    dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="XDI",
                                                     retrain_num=retrain_num, batch_num=batch_num)
                                   
                    Build_YDI_Model_check, Build_YDI_Model_msg = Build_YDI_Model(path_dict['5'][train_flag])
                    if Build_YDI_Model_check == 'ERROR':
                        update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                        update_project_model_errorconfirm_errormessage_by_modelid(1, Build_YDI_Model_msg, SVM_PP_MODEL.MODEL_ID[0])
                        update_project_model_modelstatus_by_modelid('3', SVM_PP_MODEL.MODEL_ID[0])
                        return 1
    
                    YDI_off_line_report_status, YDI_off_line_report_msg = YDI_off_line_report(path_dict['5'][test_flag], mode="Train")
                    
                    json2csv(input_path["config_path"])
                    dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="YDI",
                                                     retrain_num=retrain_num, batch_num=batch_num)
                                    
                    if YDI_off_line_report_status == "ERROR":
                        update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                        update_project_model_errorconfirm_errormessage_by_modelid(1, YDI_off_line_report_msg, SVM_PP_MODEL.MODEL_ID[0])
                        update_project_model_modelstatus_by_modelid('3', SVM_PP_MODEL.MODEL_ID[0])
                        return 1 
                    #20190830 add XDI NG confirm
                    elif XDI_off_line_report_check == "NG":
                        update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('3', '3', 1, SVM_PP_MODEL.MODEL_ID[0])            
                        update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)
                        update_project_model_errorconfirm_errormessage_by_modelid(0, XDI_off_line_report_msg, SVM_PP_MODEL.MODEL_ID[0])            
                        mylog.info("XDI_off_line_report_status NG")   
                        """
                        model_step = '3'
                        map_key, plan_start_time, estone_taskid, estone_status = Estone_post_web(project_id, model_step, user_deptid, project_title, user_empno)
                        if estone_status is not None:
                            insert_estone_projectid_mapkey_detectedtime_estonetime_estoneuserid_estonetaskid_estonestatus(project_id, map_key, plan_start_time, plan_start_time, user_empno, estone_taskid, estone_status)
                        """
                        return 1
                    elif YDI_off_line_report_status == "NG":
                        update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('3', '3', 1, SVM_PP_MODEL.MODEL_ID[0])                   
                        update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)      
                        update_project_model_errorconfirm_errormessage_by_modelid(0, YDI_off_line_report_msg, SVM_PP_MODEL.MODEL_ID[0])
                        mylog.info("YDI_off_line_report_status pause NG1")
                        """
                        model_step = '3'
                        map_key, plan_start_time, estone_taskid, estone_status = Estone_post_web(project_id, model_step, user_deptid, project_title, user_empno)
                        if estone_status is not None:
                            insert_estone_projectid_mapkey_detectedtime_estonetime_estoneuserid_estonetaskid_estonestatus(project_id, map_key, plan_start_time, plan_start_time, user_empno, estone_taskid, estone_status)
                        """
                        return 1
                    else:
                        update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('3', '3', 0, SVM_PP_MODEL.MODEL_ID[0])                
                    
                    dataremove_xdi_status, dataremove_xdi_msg = DataRemove(path_dict['4']["1"], "XDI")  #20190703 remove after step3 
                    dataremove_ydi_status, dataremove_ydi_msg = DataRemove(path_dict['5']["1"], "YDI")  #20190703 remove after step3
                  
                    model_status = self.model(path_dict)
                    if model_status != "OK":
                        mylog.error("model error")
                        update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                        update_project_model_errorconfirm_errormessage_by_modelid(1, "model error", SVM_PP_MODEL.MODEL_ID[0])
                        update_project_model_modelstatus_by_modelid('4', SVM_PP_MODEL.MODEL_ID[0])
                        return 1
                
                    json2csv(input_path["config_path"])
                    dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="Model",
                                                     retrain_num=retrain_num, batch_num=batch_num)  
                    
                    update_project_model_modelstatus_by_modelid('4', SVM_PP_MODEL.MODEL_ID[0])               
                    
                    #msg_Model, win = Model_Selection(path_dict['7'][train_flag], mode="Train")
                    Model_Selection_check, Model_Selection_msg, win = Model_Selection(path_dict['7'][train_flag], mode="Train")
                    output_paths = {}
                    
                    json2csv(input_path["config_path"])        
                    dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="SelectModel",
                                                     retrain_num=retrain_num, batch_num=batch_num)
                    
                    if Model_Selection_check == "ERROR":
                        update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                        update_project_model_errorconfirm_errormessage_by_modelid(1, Model_Selection_msg, SVM_PP_MODEL.MODEL_ID[0])
                        update_project_model_modelstatus_by_modelid('5', SVM_PP_MODEL.MODEL_ID[0])
                        return 1
                    elif Model_Selection_check == "NG":        
                        update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('5', '5', 1, SVM_PP_MODEL.MODEL_ID[0])                    
                        update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)   
                        update_project_model_errorconfirm_errormessage_by_modelid(0, Model_Selection_msg, SVM_PP_MODEL.MODEL_ID[0])
                        mylog.info("Model_Selection pause NG1")
                        """
                        model_step = '5'
                        map_key, plan_start_time, estone_taskid, estone_status = Estone_post_web(project_id, model_step, user_deptid, project_title, user_empno)
                        if estone_status is not None:
                            insert_estone_projectid_mapkey_detectedtime_estonetime_estoneuserid_estonetaskid_estonestatus(project_id, map_key, plan_start_time, plan_start_time, user_empno, estone_taskid, estone_status)
                        """
                        return 1
                    elif Model_Selection_check == "Fail":
                        update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('5', '5', 2, SVM_PP_MODEL.MODEL_ID[0])  
                        update_project_model_errorconfirm_errormessage_by_modelid(0, Model_Selection_msg, SVM_PP_MODEL.MODEL_ID[0])       
                        mylog.info("Model_Selection Fail")     
                    else:
                        update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('5', '5', 0, SVM_PP_MODEL.MODEL_ID[0])
                    
                    #20190701 modify in the future(use XGB, PLS current),read config after user select 
                    if str(win).upper() == 'XGB':           
                        output_paths["Model_Selection_Report"] = os.path.join(path_dict['7'][train_flag], "Model_Selection_Report.csv")
                        select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
                        out_df = select_df[(select_df["Model"] == 'XGB') & (select_df["Data"] == 'Test')]            
                        update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PP_MODEL.MODEL_ID[0])
                    elif str(win).upper() == 'PLS':
                        output_paths["Model_Selection_Report"] = os.path.join(path_dict['7'][train_flag], "Model_Selection_Report.csv")
                        select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
                        out_df = select_df[(select_df["Model"] == 'PLS') & (select_df["Data"] == 'Test')]            
                        update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PP_MODEL.MODEL_ID[0])
                    else:
                        input_path = read_path(path_dict[str(7)]['0']) #20190710 avoid flat don't save mae mape to db
                        mylog = WriteLog(input_path["log_path"], input_path["error_path"])     
                        config = read_config(input_path["config_path"], mylog)  
                        Predict_Model = config["Model_Selection"][config["Y"][0]]["Predict_Model"]
                        
                        if str(Predict_Model).upper() == 'XGB':           
                            output_paths["Model_Selection_Report"] = os.path.join(path_dict['7'][train_flag], "Model_Selection_Report.csv")
                            select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
                            out_df = select_df[(select_df["Model"] == 'XGB') & (select_df["Data"] == 'Test')]               
                            update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PP_MODEL.MODEL_ID[0])
                        elif str(Predict_Model).upper() == 'PLS':
                            output_paths["Model_Selection_Report"] = os.path.join(path_dict['7'][train_flag], "Model_Selection_Report.csv")
                            select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
                            out_df = select_df[(select_df["Model"] == 'PLS') & (select_df["Data"] == 'Test')]               
                            update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PP_MODEL.MODEL_ID[0])
                                                    
                    pre_mxci_myci_status, pre_mxci_myci_msg = pre_MXCI_MYCI(path_dict['8'][train_flag])
                    if pre_mxci_myci_status == "NG":
                        update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                        update_project_model_errorconfirm_errormessage_by_modelid(1, pre_mxci_myci_msg, SVM_PP_MODEL.MODEL_ID[0])
                        update_project_model_modelstatus_by_modelid('6', SVM_PP_MODEL.MODEL_ID[0])
                        return 1         
                    
                    update_project_model_modelstatus_by_modelid('6', SVM_PP_MODEL.MODEL_ID[0])
                    
                    mxci_myci_offline_status, mxci_myci_offline_msg = MXCI_MYCI_offline(path_dict['8'][test_flag], mode="Train")
                    if mxci_myci_offline_status == "NG":
                        update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                        update_project_model_errorconfirm_errormessage_by_modelid(1, mxci_myci_offline_msg, SVM_PP_MODEL.MODEL_ID[0])
                        update_project_model_modelstatus_by_modelid('7', SVM_PP_MODEL.MODEL_ID[0])
                        return 1
                
                    json2csv(input_path["config_path"])
                    dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="CI",
                                                     retrain_num=retrain_num, batch_num=batch_num)
                    
                    update_project_model_modelstatus_by_modelid('7', SVM_PP_MODEL.MODEL_ID[0])
                                                                           
        elif SVM_PP_MODEL.MODEL_STEP[0] == '3':
            #if SVM_PP_MODEL.WAIT_CONFIRM[0] == True:
            if SVM_PP_MODEL.WAIT_CONFIRM[0] == 1:
                time.sleep(10)
                update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)
                return 1
            else:
                if (SVM_PP_MODEL.MODEL_STATUS[0] in ('3','4')) and (SVM_PP_MODEL.ERROR_CONFIRM[0] == False):
                              
                    dataremove_xdi_status, dataremove_xdi_msg = DataRemove(path_dict['4']["1"], "XDI")  #20190703 remove after step3                       
                    dataremove_ydi_status, dataremove_ydi_msg = DataRemove(path_dict['5']["1"], "YDI")  #20190703 remove after step3
                                
                    model_status = self.model(path_dict)
                    if model_status != "OK":
                        mylog.error("model error")
                        update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                        update_project_model_errorconfirm_errormessage_by_modelid(1, "model error", SVM_PP_MODEL.MODEL_ID[0])
                        update_project_model_modelstatus_by_modelid('4', SVM_PP_MODEL.MODEL_ID[0])
                        return 1
                        
                    json2csv(input_path["config_path"])
                    dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="Model",
                                                     retrain_num=retrain_num, batch_num=batch_num)
                    
                    update_project_model_modelstatus_by_modelid('4', SVM_PP_MODEL.MODEL_ID[0])
                
                SVM_PP_MODEL1 = select_project_with_model_by_projectid(project_id)
                
                if (SVM_PP_MODEL1.MODEL_STATUS[0] in ('4','5')) and (SVM_PP_MODEL1.ERROR_CONFIRM[0] == False):
                         
                    #msg_Model, win = Model_Selection(path_dict['7'][train_flag], mode="Train")
                    Model_Selection_check, Model_Selection_msg, win = Model_Selection(path_dict['7'][train_flag], mode="Train")
                    output_paths = {}
                    
                    json2csv(input_path["config_path"])        
                    dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="SelectModel",
                                                     retrain_num=retrain_num, batch_num=batch_num)
                    
                    if Model_Selection_check == "ERROR":
                        update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                        update_project_model_errorconfirm_errormessage_by_modelid(1, Model_Selection_msg, SVM_PP_MODEL.MODEL_ID[0])
                        update_project_model_modelstatus_by_modelid('5', SVM_PP_MODEL.MODEL_ID[0])
                        return 1
                    elif Model_Selection_check == "NG":        
                        update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('5', '5', 1, SVM_PP_MODEL.MODEL_ID[0])                    
                        update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)      
                        update_project_model_errorconfirm_errormessage_by_modelid(0, Model_Selection_msg, SVM_PP_MODEL.MODEL_ID[0])
                        mylog.info("Model_Selection pause NG2")       
                        """
                        model_step = '5'
                        map_key, plan_start_time, estone_taskid, estone_status = Estone_post_web(project_id, model_step, user_deptid, project_title, user_empno)
                        if estone_status is not None:
                            insert_estone_projectid_mapkey_detectedtime_estonetime_estoneuserid_estonetaskid_estonestatus(project_id, map_key, plan_start_time, plan_start_time, user_empno, estone_taskid, estone_status)
                        """
                        return 1
                    elif Model_Selection_check == "Fail":
                        update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('5', '5', 2, SVM_PP_MODEL.MODEL_ID[0])  
                        update_project_model_errorconfirm_errormessage_by_modelid(0, Model_Selection_msg, SVM_PP_MODEL.MODEL_ID[0])       
                        mylog.info("Model_Selection Fail")  
                    else:
                        update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('5', '5', 0, SVM_PP_MODEL.MODEL_ID[0])
                    
                    #20190701 modify in the future(use XGB, PLS current),read config after user select 
                    if str(win).upper() == 'XGB':           
                        output_paths["Model_Selection_Report"] = os.path.join(path_dict['7'][train_flag], "Model_Selection_Report.csv")
                        select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
                        out_df = select_df[(select_df["Model"] == 'XGB') & (select_df["Data"] == 'Test')]            
                        update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PP_MODEL.MODEL_ID[0])
                    elif str(win).upper() == 'PLS':
                        output_paths["Model_Selection_Report"] = os.path.join(path_dict['7'][train_flag], "Model_Selection_Report.csv")
                        select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
                        out_df = select_df[(select_df["Model"] == 'PLS') & (select_df["Data"] == 'Test')]            
                        update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PP_MODEL.MODEL_ID[0])
                    else:
                        input_path = read_path(path_dict[str(7)]['0']) #20190710 avoid flat don't save mae mape to db
                        mylog = WriteLog(input_path["log_path"], input_path["error_path"])     
                        config = read_config(input_path["config_path"], mylog)  
                        Predict_Model = config["Model_Selection"][config["Y"][0]]["Predict_Model"]
                        
                        if str(Predict_Model).upper() == 'XGB':           
                            output_paths["Model_Selection_Report"] = os.path.join(path_dict['7'][train_flag], "Model_Selection_Report.csv")
                            select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
                            out_df = select_df[(select_df["Model"] == 'XGB') & (select_df["Data"] == 'Test')]               
                            update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PP_MODEL.MODEL_ID[0])
                        elif str(Predict_Model).upper() == 'PLS':
                            output_paths["Model_Selection_Report"] = os.path.join(path_dict['7'][train_flag], "Model_Selection_Report.csv")
                            select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
                            out_df = select_df[(select_df["Model"] == 'PLS') & (select_df["Data"] == 'Test')]               
                            update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PP_MODEL.MODEL_ID[0])
                                                    
                    pre_mxci_myci_status, pre_mxci_myci_msg = pre_MXCI_MYCI(path_dict['8'][train_flag])
                    if pre_mxci_myci_status == "NG":
                        update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                        update_project_model_errorconfirm_errormessage_by_modelid(1, pre_mxci_myci_msg, SVM_PP_MODEL.MODEL_ID[0])
                        update_project_model_modelstatus_by_modelid('6', SVM_PP_MODEL.MODEL_ID[0])
                        return 1
                    
                    update_project_model_modelstatus_by_modelid('6', SVM_PP_MODEL.MODEL_ID[0])
                    
                    mxci_myci_offline_status, mxci_myci_offline_msg = MXCI_MYCI_offline(path_dict['8'][test_flag], mode="Train")
                    if mxci_myci_offline_status == "NG":
                        update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                        update_project_model_errorconfirm_errormessage_by_modelid(1, mxci_myci_offline_msg, SVM_PP_MODEL.MODEL_ID[0])
                        update_project_model_modelstatus_by_modelid('7', SVM_PP_MODEL.MODEL_ID[0])
                        return 1
                        
                    json2csv(input_path["config_path"])
                    dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="CI",
                                                     retrain_num=retrain_num, batch_num=batch_num)
                    
                    update_project_model_modelstatus_by_modelid('7', SVM_PP_MODEL.MODEL_ID[0])
                                                   
        elif SVM_PP_MODEL.MODEL_STEP[0] == '5':
            #if SVM_PP_MODEL.WAIT_CONFIRM[0] == True:
            if SVM_PP_MODEL.WAIT_CONFIRM[0] == 1:
                time.sleep(10)
                update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)
                return 1
            else:
                if (SVM_PP_MODEL.MODEL_STATUS[0] in ('5','6')) and (SVM_PP_MODEL.ERROR_CONFIRM[0] == False):
                    
                    output_paths = {}
                    input_path = read_path(path_dict[str(7)]['0']) #20190710 avoid flat don't save mae mape to db
                    mylog = WriteLog(input_path["log_path"], input_path["error_path"])     
                    config = read_config(input_path["config_path"], mylog)  
                    Predict_Model = config["Model_Selection"][config["Y"][0]]["Predict_Model"]
                    
                    if str(Predict_Model).upper() == 'XGB':           
                        output_paths["Model_Selection_Report"] = os.path.join(path_dict['7'][train_flag], "Model_Selection_Report.csv")
                        select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
                        out_df = select_df[(select_df["Model"] == 'XGB') & (select_df["Data"] == 'Test')]               
                        update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PP_MODEL.MODEL_ID[0])
                    elif str(Predict_Model).upper() == 'PLS':
                        output_paths["Model_Selection_Report"] = os.path.join(path_dict['7'][train_flag], "Model_Selection_Report.csv")
                        select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
                        out_df = select_df[(select_df["Model"] == 'PLS') & (select_df["Data"] == 'Test')]               
                        update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PP_MODEL.MODEL_ID[0])
                                                    
                    pre_mxci_myci_status, pre_mxci_myci_msg = pre_MXCI_MYCI(path_dict['8'][train_flag])
                    if pre_mxci_myci_status == "NG":
                        update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                        update_project_model_errorconfirm_errormessage_by_modelid(1, pre_mxci_myci_msg, SVM_PP_MODEL.MODEL_ID[0])
                        update_project_model_modelstatus_by_modelid('6', SVM_PP_MODEL.MODEL_ID[0])
                        return 1
                    
                    update_project_model_modelstatus_by_modelid('6', SVM_PP_MODEL.MODEL_ID[0])
                
                SVM_PP_MODEL1 = select_project_with_model_by_projectid(project_id)
                
                if (SVM_PP_MODEL1.MODEL_STATUS[0] in ('6','7')) and (SVM_PP_MODEL1.ERROR_CONFIRM[0] == False):                                  
                 
                    mxci_myci_offline_status, mxci_myci_offline_msg = MXCI_MYCI_offline(path_dict['8'][test_flag], mode="Train")
                    if mxci_myci_offline_status == "NG":
                        update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                        update_project_model_errorconfirm_errormessage_by_modelid(1, mxci_myci_offline_msg, SVM_PP_MODEL.MODEL_ID[0])
                        update_project_model_modelstatus_by_modelid('7', SVM_PP_MODEL.MODEL_ID[0])
                        return 1
                        
                    json2csv(input_path["config_path"])
                    dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="CI",
                                                     retrain_num=retrain_num, batch_num=batch_num)
                    
                    update_project_model_modelstatus_by_modelid('7', SVM_PP_MODEL.MODEL_ID[0])
                                                            
        return None
        
    def merge_phase(self, base_name, retrain_num, batch_num=None):
        retrain_num = 0
        batch_num = 0
        path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]

        print("MERGE phase")
        merge_flag = '2'
        test_flag = '3'

        info_check, msg = Data_PreProcess_Train(path_dict['3'][merge_flag], mode="Merge")
        print(info_check, msg)
        info_check, msg = Build_XDI_Model(path_dict['4'][merge_flag])
        print(info_check, msg)
        info_check, msg = XDI_off_line_report(path_dict['4'][test_flag], mode="Merge")
        print(info_check, msg)
        info_check, msg = Build_YDI_Model(path_dict['5'][merge_flag])
        print(info_check, msg)
        info_check, msg = YDI_off_line_report(path_dict['5'][test_flag], mode="Merge")
        print(info_check, msg)
        self.model_merge(path_dict)
        info_check, msg, pred_model = Model_Selection(path_dict['7']['1'], mode="Merge")
        print(info_check, msg, pred_model)
        nfo_check, msg = pre_MXCI_MYCI(path_dict['8'][merge_flag])
        print(info_check, msg)
        nfo_check, msg = MXCI_MYCI_offline(path_dict['8'][test_flag],  mode="Merge")
        print(info_check, msg)
        return None

    def module_for_loop(self, model_num, retrain_num, batch_num, dc):
        train_flag = '0'
        test_flag = '1'
        Step5_NG = 0
        Step5_ERROR = 0
        Step5_count = 1 
        
        for feature in self.feature_lists:
            
            print(model_num, self.filter_feature, feature)
            
            SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[feature]) #m2m modelname = self.filter_dir_name[feature]
            project_id = SVM_PROJECT_MODEL.PROJECT_ID[0]
            model_id = SVM_PROJECT_MODEL.MODEL_ID[0]
            
            path_dict = self.path_config[self.filter_dir_name[feature]][str(retrain_num)][str(batch_num)]            
            input_path = read_path(path_dict[str(3)]['0'])
            mylog = WriteLog(input_path["log_path"], input_path["error_path"]) 
                        
            if model_num == 3:              
                Data_PreProcess_Train_check, Data_PreProcess_Train_msg = self.mdoel_dict[str(model_num)]["Train"](path_dict[str(model_num)][train_flag], mode="Train")
                if Data_PreProcess_Train_check == 'ERROR':
                    update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                    update_project_model_errorconfirm_errormessage_by_modelid(1, Data_PreProcess_Train_msg, model_id)
                    update_project_model_modelstatus_by_modelid('2', model_id)
                    return 1
                              
                Data_PreProcess_Test_check, Data_PreProcess_Test_msg = self.mdoel_dict[str(model_num)]["Test"](path_dict[str(model_num)][test_flag])
                if Data_PreProcess_Test_check == 'ERROR':
                    update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                    update_project_model_errorconfirm_errormessage_by_modelid(1, Data_PreProcess_Test_msg, model_id)
                    update_project_model_modelstatus_by_modelid('2', model_id)
                    return 1

                json2csv(input_path["config_path"])
            
            elif model_num == 4:
                Step_start = datetime.datetime.now()
                
                Build_XDI_Model_check, Build_XDI_Model_msg = self.mdoel_dict[str(model_num)]["Train"](path_dict[str(model_num)][train_flag])
                if Build_XDI_Model_check == 'ERROR':
                    update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                    update_project_model_errorconfirm_errormessage_by_modelid(1, Build_XDI_Model_msg, model_id)
                    update_project_model_modelstatus_by_modelid('3', model_id)
                    return 1
                
                XDI_off_line_report_check, XDI_off_line_report_msg = self.mdoel_dict[str(model_num)]["Test"](path_dict[str(model_num)][test_flag], mode="Train")
                if XDI_off_line_report_check == 'ERROR':
                    update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                    update_project_model_errorconfirm_errormessage_by_modelid(1, XDI_off_line_report_msg, model_id)
                    update_project_model_modelstatus_by_modelid('3', model_id)
                    return 1

                json2csv(input_path["config_path"])
                
                Step_end = datetime.datetime.now()
                Step_interval = Step_end - Step_start
                print('Step3_XDI: ' + str(Step_interval))
                mylog.info("Step3_XDI: " + str(Step_interval))
                           
            elif model_num == 5:
                Step_start = datetime.datetime.now()
                
                Build_YDI_Model_check, Build_YDI_Model_msg = self.mdoel_dict[str(model_num)]["Train"](path_dict[str(model_num)][train_flag])
                YDI_off_line_report_status, YDI_off_line_report_msg = self.mdoel_dict[str(model_num)]["Test"](path_dict[str(model_num)][test_flag], mode="Train")
                json2csv(input_path["config_path"])
                
                if Build_YDI_Model_check == 'ERROR':
                    Step5_ERROR = 1                    
                if YDI_off_line_report_status == "ERROR":
                    Step5_ERROR = 1
                elif YDI_off_line_report_status == "NG":
                    Step5_NG = 1
                update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('3', '3', 0, SVM_PROJECT_MODEL.MODEL_ID[0])               
                
                if Step5_count == len(self.feature_lists):
                    dc.YDI_collector(path_config=self.path_config, filter_dir_name=self.filter_dir_name,
                         retrain_num=retrain_num, batch_num=batch_num,
                         feature_lists=self.feature_lists, merge_flag=None)
                    
                    if Step5_ERROR == 1:
                        update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                        update_project_model_errorconfirm_errormessage_by_modelid(1, YDI_off_line_report_msg, model_id)
                        update_project_model_modelstatus_by_modelid('3', model_id) 
                        return 1
                    elif Step5_NG == 1:     
                       #20191014 in reponse max model <> max filter_feature
                       SVM_MAXMODEL = select_project_maxmodel_by_projectid(project_id) 
                       update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('3', '3', 1, SVM_MAXMODEL.MODEL_ID[0])                       
                       update_project_STATUS_by_projectid("CREATING_PAUSE", project_id) #20190729 Because NG, change project status, return function and not execute down    
                       update_project_model_errorconfirm_errormessage_by_modelid(0, YDI_off_line_report_msg, model_id)
                       mylog.info("Wait user check YDI NG!!!")   
                       """
                       SVM_PP_MODEL = select_project_with_model_by_projectid(project_id)
                       model_step = '3'
                       user_deptid = SVM_PP_MODEL.USER_DEPTID[0]
                       project_title = SVM_PP_MODEL.PROJECT_TITLE[0]
                       user_empno = SVM_PP_MODEL.USER_EMPNO[0]
                       map_key, plan_start_time, estone_taskid, estone_status = Estone_post_web(project_id, model_step, user_deptid, project_title, user_empno)
                       if estone_status is not None:
                           insert_estone_projectid_mapkey_detectedtime_estonetime_estoneuserid_estonetaskid_estonestatus(project_id, map_key, plan_start_time, plan_start_time, user_empno, estone_taskid, estone_status)
                       """
                       return 1
                       
                    DataRemove(path_dict['4']["1"], "XDI")  #20190703 remove after step3      
                    DataRemove(path_dict['5']["1"], "YDI")  #20190703 remove after step3

                Step_end = datetime.datetime.now()
                Step_interval = Step_end - Step_start
                print('Step3_YDI: ' + str(Step_interval))
                mylog.info("Step3_YDI: " + str(Step_interval))
                
            else:
                pre_mxci_myci_status, pre_mxci_myci_msg = self.mdoel_dict[str(model_num)]["Train"](path_dict[str(model_num)][train_flag])
                if pre_mxci_myci_status == "NG": 
                    update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                    update_project_model_errorconfirm_errormessage_by_modelid(1, pre_mxci_myci_msg, model_id)
                    update_project_model_modelstatus_by_modelid('6', model_id) 
                    return 1
                
                mxci_myci_offline_status, mxci_myci_offline_msg = self.mdoel_dict[str(model_num)]["Test"](path_dict[str(model_num)][test_flag], mode="Train")
                if mxci_myci_offline_status == "NG":
                    update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                    update_project_model_errorconfirm_errormessage_by_modelid(1, mxci_myci_offline_msg, model_id)
                    update_project_model_modelstatus_by_modelid('7', model_id) 
                    return 1

                json2csv(input_path["config_path"])                

            Step5_count = Step5_count+1                                          
            
        return None

    def model_selection_for_loop(self, retrain_num, batch_num, dc):        
        #xgb_win = 0
        Step7_NG = 0
        Step7_ERROR = 0
        Step7_FAIL = 0
        Step7_count = 1
        
        for feature in self.feature_lists:
            path_dict = self.path_config[self.filter_dir_name[feature]][str(retrain_num)][str(batch_num)]
            #msg_Model, win = Model_Selection(path_dict[str(7)]['0'], mode="Train")
            Model_Selection_check, Model_Selection_msg, win = Model_Selection(path_dict[str(7)]['0'], mode="Train")
            output_paths = {}
            
            SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[feature])
            project_id = SVM_PROJECT_MODEL.PROJECT_ID[0]
            
            input_path = read_path(path_dict[str(7)]['0'])
            mylog = WriteLog(input_path["log_path"], input_path["error_path"])
            json2csv(input_path["config_path"])
            
            if Model_Selection_check == "ERROR":
                Step7_ERROR = 1
            elif Model_Selection_check == "NG":
                Step7_NG = 1
            elif Model_Selection_check == "Fail":
                Step7_FAIL = 1
            update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('5', '5', 0, SVM_PROJECT_MODEL.MODEL_ID[0])
               
            config = read_config(input_path["config_path"], mylog)  
            
            if Step7_count == len(self.feature_lists):
                dc.Selection_collector(path_config=self.path_config, filter_dir_name=self.filter_dir_name,
                               retrain_num=retrain_num, batch_num=batch_num,
                               feature_lists=self.feature_lists, merge_flag=None)
                
                if Step7_ERROR == 1:
                    update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                    update_project_model_errorconfirm_errormessage_by_modelid(1, Model_Selection_msg, SVM_PROJECT_MODEL.MODEL_ID[0])
                    update_project_model_modelstatus_by_modelid('5', SVM_PROJECT_MODEL.MODEL_ID[0]) 
                    return 1
                elif Step7_NG == 1:
                    SVM_MAXMODEL = select_project_maxmodel_by_projectid(project_id) 
                    update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('5', '5', 1, SVM_MAXMODEL.MODEL_ID[0])                   
                    update_project_STATUS_by_projectid("CREATING_PAUSE", project_id) #20190729 Because NG, change project status, return function and not execute down               
                    update_project_model_errorconfirm_errormessage_by_modelid(0, Model_Selection_msg, SVM_PROJECT_MODEL.MODEL_ID[0])
                    mylog.info("Wait user select model!!!") 
                    """
                    SVM_PP_MODEL = select_project_with_model_by_projectid(project_id)
                    model_step = '5'
                    user_deptid = SVM_PP_MODEL.USER_DEPTID[0]
                    project_title = SVM_PP_MODEL.PROJECT_TITLE[0]
                    user_empno = SVM_PP_MODEL.USER_EMPNO[0]
                    map_key, plan_start_time, estone_taskid, estone_status = Estone_post_web(project_id, model_step, user_deptid, project_title, user_empno)
                    if estone_status is not None:
                        insert_estone_projectid_mapkey_detectedtime_estonetime_estoneuserid_estonetaskid_estonestatus(project_id, map_key, plan_start_time, plan_start_time, user_empno, estone_taskid, estone_status)
                    """
                    return 1
                elif Step7_FAIL == 1:
                    SVM_MAXMODEL = select_project_maxmodel_by_projectid(project_id) 
                    update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('5', '5', 2, SVM_MAXMODEL.MODEL_ID[0])  
                    update_project_model_errorconfirm_errormessage_by_modelid(0, Model_Selection_msg, SVM_PROJECT_MODEL.MODEL_ID[0])       
                    mylog.info("Model_Selection Fail") 
               

            #20190701 modify in the future(use XGB, PLS current)
            if str(win).upper() == 'XGB':
                #xgb_win = xgb_win+1                
                output_paths["Model_Selection_Report"] = os.path.join(path_dict[str(7)]['0'], "Model_Selection_Report.csv")
                select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
                out_df = select_df[(select_df["Model"] == 'XGB') & (select_df["Data"] == 'Test')]                
                update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PROJECT_MODEL.MODEL_ID[0])
            elif str(win).upper() == 'PLS':
                output_paths["Model_Selection_Report"] = os.path.join(path_dict[str(7)]['0'], "Model_Selection_Report.csv")
                select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
                out_df = select_df[(select_df["Model"] == 'PLS') & (select_df["Data"] == 'Test')]                
                update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PROJECT_MODEL.MODEL_ID[0])
            else:
                Predict_Model = config["Model_Selection"][config["Y"][0]]["Predict_Model"]
                
                if str(Predict_Model).upper() == 'XGB':
                    #xgb_win = xgb_win+1                
                    output_paths["Model_Selection_Report"] = os.path.join(path_dict[str(7)]['0'], "Model_Selection_Report.csv")
                    select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
                    out_df = select_df[(select_df["Model"] == 'XGB') & (select_df["Data"] == 'Test')]                
                    update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PROJECT_MODEL.MODEL_ID[0])
                elif str(Predict_Model).upper() == 'PLS':
                    output_paths["Model_Selection_Report"] = os.path.join(path_dict[str(7)]['0'], "Model_Selection_Report.csv")
                    select_df = pd.read_csv(output_paths["Model_Selection_Report"])           
                    out_df = select_df[(select_df["Model"] == 'PLS') & (select_df["Data"] == 'Test')]                
                    update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PROJECT_MODEL.MODEL_ID[0])
                                              
            Step7_count = Step7_count+1
                                                     
        return None

    def many_merge_phase(self, retrain_num, batch_num):
        for feature in self.feature_lists:
            self.merge_phase(base_name=self.filter_dir_name[feature], retrain_num=retrain_num, batch_num=batch_num)
        return None

    def many_model_phase(self, retrain_num, batch_num, project_id):
        for feature in self.feature_lists:
            path_dict = self.path_config[self.filter_dir_name[feature]][str(retrain_num)][str(batch_num)]
            SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[feature]) #m2m modelname = self.filter_dir_name[feature]
            model_id = SVM_PROJECT_MODEL.MODEL_ID[0]
            model_status = self.model(path_dict)
            if model_status != "OK":     
                update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                update_project_model_errorconfirm_errormessage_by_modelid(1, "model error", model_id)
                update_project_model_modelstatus_by_modelid('4', model_id) 
                return 1
        return None

    def many_to_many(self, project_id):
        
        pause_flag = '0'
        dc = Data_Collector(self.base_path, self.train_data, self.config_file, pause_flag)  #create Data_Collector folder, base.path=../Cases/PSH_Demo 
        dc.create_file_path() # for DataPreview
        
        retrain_num = 0
        batch_num = 0 
        print("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num))
        
        self.get_filter_feature()
        self.get_feature_content(split_flag=True)
        self.create_many_path(training_data_dict=self.filter_feature_dict) 
        
        #20190618 judge Data_Preview_status in future
        cnxn1 = DB_Connection(server_name=ServerConfig.SmartPrediction_DBServer_IP, db_name=ServerConfig.SmartPrediction_Config_DB)
        for x in self.filter_dir_name.keys():

            SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[x])
            
            cursor1 = cnxn1.cursor()
            cursor1.execute("UPDATE SVM_PROJECT_MODEL SET MODEL_SEQ = ?, MODEL_STATUS = ?, BATCH_SEQ = ?, IS_ONLINE = ?, MODEL_STEP = ?, WAIT_CONFIRM = ? WHERE MODEL_ID = ?", str(retrain_num), '1', str(batch_num), 0, '1', 0, int(SVM_PROJECT_MODEL.MODEL_ID[0]))
            cnxn1.commit()       
        
        Data_Preview_status, Data_Preview_msg = Data_Preview(dc.data_preview_path)                                                        
                
        dc.init_collector_dir(retrain_num, batch_num)
        
        self.module_for_loop(model_num=3, retrain_num=retrain_num, batch_num=batch_num, dc=dc)
        
        for x in self.filter_dir_name.keys():
            SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[x])
            update_project_model_modelstatus_by_modelid('2', SVM_PROJECT_MODEL.MODEL_ID[0])       
          
        self.module_for_loop(model_num=4, retrain_num=retrain_num, batch_num=batch_num, dc=dc)
        dc.XDI_collector(path_config=self.path_config, filter_dir_name=self.filter_dir_name,
                         retrain_num=retrain_num, batch_num=batch_num,
                         feature_lists=self.feature_lists, merge_flag=None)
        step3_result = self.module_for_loop(model_num=5, retrain_num=retrain_num, batch_num=batch_num, dc=dc)
        if step3_result == 1:
            return 1
        
        for x in self.filter_dir_name.keys():
            SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[x])
            update_project_model_modelstatus_by_modelid('3', SVM_PROJECT_MODEL.MODEL_ID[0])   
            
        self.many_model_phase(retrain_num=retrain_num, batch_num=batch_num, project_id=project_id)           
        dc.Model_collector(path_config=self.path_config, filter_dir_name=self.filter_dir_name,
                           retrain_num=retrain_num, batch_num=batch_num, feature_name=self.filter_feature,
                           feature_lists=self.feature_lists, merge_flag=None)
        
        for x in self.filter_dir_name.keys():
            SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[x])            
            update_project_model_modelstatus_by_modelid('4', SVM_PROJECT_MODEL.MODEL_ID[0])
            
        step5_result = self.model_selection_for_loop(retrain_num=retrain_num, batch_num=batch_num, dc=dc)
        if step5_result == 1:
            return 1
        
        for x in self.filter_dir_name.keys():
            SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[x])
            update_project_model_modelstatus_by_modelid('5', SVM_PROJECT_MODEL.MODEL_ID[0])
            
        self.module_for_loop(model_num=8, retrain_num=retrain_num, batch_num=batch_num, dc=dc)        
            
        dc.CI_collector(path_config=self.path_config, filter_dir_name=self.filter_dir_name,
                        retrain_num=retrain_num, batch_num=batch_num,
                        feature_lists=self.feature_lists, merge_flag=None)  
        
        #20190808 because module_for_loop[8] include step6 & 7
        for x in self.filter_dir_name.keys():
            SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[x])
            update_project_model_modelstatus_by_modelid('7', SVM_PROJECT_MODEL.MODEL_ID[0])
        
        self.many_merge_phase(retrain_num=retrain_num, batch_num=batch_num)
        self.many_data_porter(retrain_num=retrain_num, batch_num=batch_num, mode="Merge", pause_flag=pause_flag)
        
        cursor1 = cnxn1.cursor()
        cursor1.execute("UPDATE SVM_PROJECT SET STATUS = ? WHERE PROJECT_ID = ?", "CREATED_OK", int(project_id))
        cnxn1.commit() 
        
        self.zip_project(project_id)
        
        return None
    

    def many_to_many_pause(self, project_id):
        
        pause_flag = '1'
        dc = Data_Collector(self.base_path, self.train_data, self.config_file, pause_flag)  #create Data_Collector folder, base.path=../Cases/PSH_Demo 
        
        self.get_filter_feature()
        self.get_feature_content(split_flag=True)
        self.create_many_path_pause(training_data_dict=self.filter_feature_dict) #20190815 create filter_dir_name dictionary
        self.get_saved_path_config() #20190815 get_saved_path_config
                                         
        retrain_num = 0
        batch_num = 0
        
        #20190618 judge Data_Preview_status in future
        cnxn1 = DB_Connection(server_name=ServerConfig.SmartPrediction_DBServer_IP, db_name=ServerConfig.SmartPrediction_Config_DB)      
        dc.init_collector_dir(retrain_num, batch_num)
        print("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num))
        
        SVM_MAXMODEL = select_project_maxmodel_by_projectid(project_id)
        if SVM_MAXMODEL.MODEL_STEP[0] == '3':
            #if SVM_MAXMODEL.WAIT_CONFIRM[0] == True:
            if SVM_MAXMODEL.WAIT_CONFIRM[0] == 1:
                time.sleep(10)
                update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)
                return 1
            else:                                      
                self.many_model_phase(retrain_num=retrain_num, batch_num=batch_num, project_id=project_id)           
                dc.Model_collector(path_config=self.path_config, filter_dir_name=self.filter_dir_name,
                                   retrain_num=retrain_num, batch_num=batch_num, feature_name=self.filter_feature,
                                   feature_lists=self.feature_lists, merge_flag=None)
                
                for x in self.filter_dir_name.keys():        
                    SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[x])            
                    update_project_model_modelstatus_by_modelid('4', SVM_PROJECT_MODEL.MODEL_ID[0])                               
                       
                step5_result = self.model_selection_for_loop(retrain_num=retrain_num, batch_num=batch_num, dc=dc)
                if step5_result == 1:
                    return 1
                
                for x in self.filter_dir_name.keys():        
                    SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[x])            
                    update_project_model_modelstatus_by_modelid('5', SVM_PROJECT_MODEL.MODEL_ID[0])              
                    
                self.module_for_loop(model_num=8, retrain_num=retrain_num, batch_num=batch_num, dc=dc)                
                    
                dc.CI_collector(path_config=self.path_config, filter_dir_name=self.filter_dir_name,
                                retrain_num=retrain_num, batch_num=batch_num,
                                feature_lists=self.feature_lists, merge_flag=None)
                
                for x in self.filter_dir_name.keys():      
                    SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[x])
                    update_project_model_modelstatus_by_modelid('7', SVM_PROJECT_MODEL.MODEL_ID[0])
                                        
                self.many_merge_phase(retrain_num=retrain_num, batch_num=batch_num)
                self.many_data_porter(retrain_num=retrain_num, batch_num=batch_num, mode="Merge", pause_flag=pause_flag)
                
                cursor1 = cnxn1.cursor()
                cursor1.execute("UPDATE SVM_PROJECT SET STATUS = ? WHERE PROJECT_ID = ?", "CREATED_OK", int(project_id))
                cnxn1.commit()
                
                self.zip_project(project_id)
        
        elif SVM_MAXMODEL.MODEL_STEP[0] == '5':
            #if SVM_MAXMODEL.WAIT_CONFIRM[0] == True:
            if SVM_MAXMODEL.WAIT_CONFIRM[0] == 1:
                time.sleep(10)
                update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)
                return 1
            else:                       
                self.module_for_loop(model_num=8, retrain_num=retrain_num, batch_num=batch_num, dc=dc)
                                    
                dc.CI_collector(path_config=self.path_config, filter_dir_name=self.filter_dir_name,
                                retrain_num=retrain_num, batch_num=batch_num,
                                feature_lists=self.feature_lists, merge_flag=None)
                
                for x in self.filter_dir_name.keys():        
                    SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[x])
                    update_project_model_modelstatus_by_modelid('7', SVM_PROJECT_MODEL.MODEL_ID[0])
                                        
                self.many_merge_phase(retrain_num=retrain_num, batch_num=batch_num)
                self.many_data_porter(retrain_num=retrain_num, batch_num=batch_num, mode="Merge", pause_flag=pause_flag)
                
                cursor1 = cnxn1.cursor()
                cursor1.execute("UPDATE SVM_PROJECT SET STATUS = ? WHERE PROJECT_ID = ?", "CREATED_OK", int(project_id))
                cnxn1.commit()
                
                self.zip_project(project_id)
             
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

    def one_to_all(self, project_id):
        path = os.path.join(self.base_path, self.base_name)
        
        pause_flag = '0'
        dc = Data_Collector(self.base_path, self.train_data, self.config_file, pause_flag)
        self.path_config = self.create_path(path)
        self.save_path_config()
        
        retrain_num = 0
        batch_num = 0
        
        print("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num))
        dc.init_collector_dir(retrain_num, batch_num)
        training_result = self.training_phase(project_id, retrain_num=self.current_retrain_number, dc_instance=dc)
        if training_result == 1:
            return 1   
        
        dc.data_porter(path_config=self.path_config, base_name=self.base_name,
                       retrain_num=retrain_num, batch_num=batch_num, mode="Train")
        
        self.merge_phase(base_name=self.base_name, retrain_num=retrain_num)
        dc.data_porter(path_config=self.path_config, base_name=self.base_name,
                       retrain_num=retrain_num, batch_num=batch_num, mode="Merge")
        
        cnxn1 = DB_Connection(server_name=ServerConfig.SmartPrediction_DBServer_IP, db_name=ServerConfig.SmartPrediction_Config_DB)
        cursor1 = cnxn1.cursor()
        cursor1.execute("UPDATE SVM_PROJECT SET STATUS = ? WHERE PROJECT_ID = ?", "CREATED_OK", int(project_id))
        cnxn1.commit() 
        
        self.zip_project(project_id) #20190805 add zip file to D:\SVM\model_out for AI_365
        
        return None
    
    def one_to_all_pause(self, project_id):                
        #20190815 avoid pause mode add data_collector retrain number
        pause_flag = '1'
        dc = Data_Collector(self.base_path, self.train_data, self.config_file, pause_flag)
        self.get_saved_path_config()
        
        retrain_num = 0
        batch_num = 0
        print("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num))
        dc.init_collector_dir(retrain_num, batch_num) #20190802 must execute for data collect
        training_result = self.training_phase_pause(project_id, retrain_num=self.current_retrain_number, dc_instance=dc)
        if training_result == 1:
            return 1
        
        dc.data_porter(path_config=self.path_config, base_name=self.base_name,
                       retrain_num=retrain_num, batch_num=batch_num, mode="Train")
        
        self.merge_phase(base_name=self.base_name, retrain_num=retrain_num)
        dc.data_porter(path_config=self.path_config, base_name=self.base_name,
                       retrain_num=retrain_num, batch_num=batch_num, mode="Merge")
        
        cnxn1 = DB_Connection(server_name=ServerConfig.SmartPrediction_DBServer_IP, db_name=ServerConfig.SmartPrediction_Config_DB)
        cursor1 = cnxn1.cursor()
        cursor1.execute("UPDATE SVM_PROJECT SET STATUS = ? WHERE PROJECT_ID = ?", "CREATED_OK", int(project_id))
        cnxn1.commit() 
        
        self.zip_project(project_id)
        
        return None
        
if __name__ == '__main__':   
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
            
    SVM_PP_MODEL = select_project_by_status_ip("CREATEDD", ip) #20190822 CREATED->CREATEDD + ip, for multiserver
    
    if len(SVM_PP_MODEL) != 0:        

        project_id = SVM_PP_MODEL.PROJECT_ID[0]
        print("------------")
        print(project_id)
        print("------------")
        
        update_project_STATUS_by_projectid("CREATING", project_id)
        
        try:
            ftp(str(project_id))
        except Exception as e:
            update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
            
        #20190805 SVN error update status, avoid execute recycle
        try:                
            SVM = SuperVM(str(SVM_PP_MODEL.PROJECT_NAME[0]), str(SVM_PP_MODEL.UPLOAD_FILE[0]), r"Config.json")
        except Exception as e:
            update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                                               
        if (str(SVM_PP_MODEL.MODEL_TYPE[0]) == '1') or (str(SVM_PP_MODEL.MODEL_TYPE[0]) == '2'):            
            SVM.one_to_all(str(project_id)) #20190820 must convert project_id type to string                                
           
        elif str(SVM_PP_MODEL.MODEL_TYPE[0]) == '3':
            SVM.many_to_many(str(project_id))

    else:
        SVM_PROJECT_CONFIRMOK_O2M = select_project_model_confirmok_o2m()
        
        if len(SVM_PROJECT_CONFIRMOK_O2M) != 0:
            #20190823 training server must meets the local ip
            if SVM_PROJECT_CONFIRMOK_O2M.TRAINING_SERVER[0] == ip:
        
                SVM_MODEL = select_project_model_by_modelid(SVM_PROJECT_CONFIRMOK_O2M.MODEL_ID[0])
                              
                project_id = SVM_MODEL.PROJECT_ID[0]            
                print("------------")
                print(project_id)
                print("------------")
                
                update_project_STATUS_by_projectid("CREATING_RUN", project_id)
                update_project_model_errorconfirm_by_projectid(0, project_id) #20190823 update model error_confirm = 0
                
                try:
                    SVM = SuperVM(str(SVM_PROJECT_CONFIRMOK_O2M.PROJECT_NAME[0]), str(SVM_PROJECT_CONFIRMOK_O2M.UPLOAD_FILE[0]), r"Config.json")                
                except Exception as e:
                    update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
    
                SVM.one_to_all_pause(str(project_id))                                                 

        else:
            SVM_PROJECT_CONFIRMOK_M2M = select_project_model_confirmok_m2m()
            
            if len(SVM_PROJECT_CONFIRMOK_M2M) != 0:
                
                if SVM_PROJECT_CONFIRMOK_M2M.TRAINING_SERVER[0] == ip:
                
                    SVM_MODEL = select_project_model_by_modelid(SVM_PROJECT_CONFIRMOK_M2M.MODEL_ID[0])
                              
                    project_id = SVM_MODEL.PROJECT_ID[0]               
                    print("------------")
                    print(project_id)
                    print("------------")
                    
                    update_project_STATUS_by_projectid("CREATING_RUN", project_id)
                    update_project_model_errorconfirm_by_projectid(0, project_id)
                    
                    try:
                        SVM = SuperVM(str(SVM_PROJECT_CONFIRMOK_M2M.PROJECT_NAME[0]), str(SVM_PROJECT_CONFIRMOK_M2M.UPLOAD_FILE[0]), r"Config.json")             
                    except Exception as e:
                        update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                                                                
                    SVM.many_to_many_pause(str(project_id))                      
                    
                   
                
# SmartVM_Constructor_retrain_v2.py
#20190823 1017 accept dispatching
#20190822 Q1: retrain db and folder inconsistent

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
from DB_operation import select_project_with_model_by_projectid_onlineserver_predictstatus_modelstatus, select_project_model_by_projectid_status, select_project_model_by_projectid_status_filterfeature, select_project_config_by_modelid_parameter3RD, select_project_online_scantime_by_projectid_datatype, select_online_parameter_by_projectid_datatype, select_online_status_by_projectid_runindex, select_projectx_run_by_itime, select_projectx_parameter_data_by_runindex, select_projectx_predict_data_by_runindex_parameter_modelid, select_projectx_contribution_data_by_runindex_parameter_model, update_project_predictstatus_by_projectid, update_online_scantime_lastscantime_by_projectid_datatype, update_online_status_xdialarm_by_projectid_runindex, update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid, update_projectx_run_signal_by_runindex, update_projectx_run_xdialarm_by_runindex, update_projectx_contribution_data_contribution_by_runindex_parameter_model, insert_online_status_projectid_runindex_xdialarm_itime, insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict, insert_projectx_contribution_data_runindex_model_parameter_contribution
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
        
    def run_o2m(self, df, path_dict, projectid, project_id, datatype, server_name, db_name):
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
            try:
                y_pred, y_base = self.model_predict_online_x(df_online_X, path_dict, str(projectid))
            except Exception as e:
                errMsg = ProcEx.ProcessException(e) #Reaver
                APLog.WriteApLog(str(df['RUNINDEX']) + " -Error- " + errMsg , projectid)
                #mylog.info(str(projectid) + ' - ' + errMsg)
                #mylog.error(df['RUNINDEX'])
                return
            print('**********')
            print(y_pred)
            print(y_base)
            print('**********')
            CI_online_start = datetime.datetime.now()  
            mylog.info(str(projectid) + "_CI_online_start: " + str(CI_online_start))
            
            #20190828 int must equal int, execute CI_online/CI_online_old
            #if (int(projectid) in (50,67,95,171,174,426,454)) or (int(projectid) > 570):
            try:
                MXCI_value, MXCI_T, MYCI_value, MYCI_T, light = CI_online(path_dict['8']["3"], df_online_X, y_pred, y_base)
            except Exception as e:
                errMsg = ProcEx.ProcessException(e) #Reaver
                APLog.WriteApLog(str(df['RUNINDEX']) + " -Error- " + errMsg , projectid)
            #else:
            #    MXCI_value, MXCI_T, MYCI_value, MYCI_T, light = CI_online_old(path_dict['8']["3"], df_online_X, y_pred, y_base)
            
            print("-"*10)
            print(MXCI_value, MXCI_T, MYCI_value, MYCI_T, light)
            print("-"*10)
            
            CI_online_end = datetime.datetime.now()
            CI_online_interval = CI_online_end - CI_online_start
            mylog.info(str(projectid) + "_CI_online_end: " + str(CI_online_end))
            mylog.info(str(projectid) + "_CI_online_interval: " + str(CI_online_interval))
                                   
            SVM_PROJECT_MODEL = select_project_model_by_projectid_status(projectid, "ONLINE")
            
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
        mylog.info(str(projectid) + "_o2m_start: " + str(o2m_start))
        
        SVM_PROJECT_ONLINE_SCANTIME = select_project_online_scantime_by_projectid_datatype(projectid, datatype)
        
        #20190620 get server name
        #server_name_1 = str(SVM_PROJECT_ONLINE_SCANTIME.SERVER_NAME[0])
        
        project_id = "SVM_PROJECT" + str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0])
        
        if len(SVM_PROJECT_ONLINE_SCANTIME) == 0:
            print('online_x_phase no scantime')                                                                   
        else:
            MAX_TIME = "9998-12-31 23:59:59.999"
                       
            LAST_SCANTIME = datetime.datetime.strftime(SVM_PROJECT_ONLINE_SCANTIME.LAST_SCANTIME[0], '%Y-%m-%d %H:%M:%S.%f')[:-3] #20190704 get millisecond 3
            SVM_PROJECT_RUN = select_projectx_run_by_itime(project_id, LAST_SCANTIME, MAX_TIME, server_name, db_name)
            
            if len(SVM_PROJECT_RUN) == 0:
                print('SVM_PROJECT_RUN null')
            else:
                SVM_PROJECT_RUN.apply(self.run_o2m, axis=1, args=(path_dict, projectid, project_id, datatype, server_name, db_name,))                                
                #for c in range(len(SVM_PROJECT_RUN)):
                                                                                                                                   
        o2m_end = datetime.datetime.now()
        o2m_interval = o2m_end - o2m_start
        mylog.info(str(projectid) + "_o2m_end: " + str(o2m_end))
        mylog.info(str(projectid) + "_o2m_interval: " + str(o2m_interval))
        return None
    
    def run_m2m(self, df, projectid, project_id, datatype, retrain_num, batch_num, FILTER_FEATURE, server_name, db_name):
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
            try:
                SVM_PROJECT_MODEL = select_project_model_by_projectid_status_filterfeature(projectid, "ONLINE", df[FILTER_FEATURE])
            except:
                SVM_PROJECT_MODEL = select_project_model_by_projectid_status_filterfeature(projectid, "ONLINE", df['FILTER_FEATURE'])
            
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
        
        projectid = str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0])        
        project_id = "SVM_PROJECT" + str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0])
        
        print("-------------")
        print(projectid)
        print("-------------")
        
        if len(SVM_PROJECT_ONLINE_SCANTIME) == 0:
            print('online_x_many_phase no scantime')                                                                                                                 
        else:
            SVM_PROJECT_CONFIG = select_project_config_by_modelid_parameter3RD(model_id, "Filter_Feature")
            MAX_TIME = "9998-12-31 23:59:59.999"                        
            LAST_SCANTIME = datetime.datetime.strftime(SVM_PROJECT_ONLINE_SCANTIME.LAST_SCANTIME[0], '%Y-%m-%d %H:%M:%S.%f')[:-3] #20190704 get millisecond 3
            
            FILTER_FEATURE = SVM_PROJECT_CONFIG.PARAM_VALUE[0]
            SVM_PROJECT_RUN = select_projectx_run_by_itime(project_id, LAST_SCANTIME, MAX_TIME, server_name, db_name)
            print(SVM_PROJECT_RUN.head(1))
            if len(SVM_PROJECT_RUN) != 0:
                                
                m2m_start = datetime.datetime.now()  
                mylog.info(str(projectid) + "_m2m_start: " + str(m2m_start))
                SVM_PROJECT_RUN.apply(self.run_m2m, axis=1, args=(projectid, project_id, datatype, retrain_num, batch_num, FILTER_FEATURE, server_name, db_name,))                  
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
                    #AI_365(SVM_PROJECT_ONLINE_SCANTIME.FAB[0], projectid, SVM_PROJECT_ONLINE_SCANTIME.AI365_PROJECT_NAME[0], SVM_PROJECT_ONLINE_SCANTIME.PROJECT_TITLE[0], SVM_PROJECT_MODEL.MODEL_ID[0], SVM_PARAMETER_DATA_y.RUNINDEX[0], SVM_PARAMETER_DATA_y.PARAM_VALUE[0], server_name, db_name)
                                       
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
        
                        #AI_365(SVM_PROJECT_ONLINE_SCANTIME.FAB[0], projectid, SVM_PROJECT_ONLINE_SCANTIME.AI365_PROJECT_NAME[0], SVM_PROJECT_ONLINE_SCANTIME.PROJECT_TITLE[0], SVM_PROJECT_MODEL.MODEL_ID[0], SVM_PARAMETER_DATA_y.RUNINDEX[0], SVM_PARAMETER_DATA_y.PARAM_VALUE[0], server_name, db_name)
                        
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
    
    
   
# DB_operation.py
# -*- coding: utf-8 -*-
import pyodbc
import pandas as pd
import SystemConfig.ServerConfig as ServerConfig # Reaver
import APLog as APLog                            # Reaver

def DB_Connection(server_name="10.97.36.137", db_name = "APC"):
    #server_name = "10.97.36.137"
    #db_name = "APC"
    user = ServerConfig.UID
    password = ServerConfig.PWD      
    cnxn1 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server_name + ';DATABASE=' + db_name + ';UID=' + user + ';PWD=' + password)
    
    return cnxn1

def select_project_creating(server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT WHERE UPPER(STATUS) in ('CREATING','CREATING_RUN','RETRAINING') order by ITIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_waitcreate(server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT WHERE UPPER(STATUS) in ('CREATED','CREATING_ERROR_OK') order by ITIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_waitretrain(server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT A, SVM_PROJECT_MODEL B where A.PROJECT_ID = B.PROJECT_ID AND UPPER(A.STATUS) = 'RETRAIN' AND UPPER(B.STATUS) = 'RETRAIN' AND RETRAIN_START_TIME is null"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model


def select_project_AI365(server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT WHERE UPPER(STATUS) = 'ONLINE' AND MODEL_TYPE IN ('1','2','3') order by ITIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_otm_by_onlineserver(online_server, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT where MODEL_TYPE IN ('1','2') AND UPPER(STATUS) IN ('ONLINE','RETRAIN','RETRAINING','RETRAIN_OK') AND ONLINE_SERVER = '" + str(online_server) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_otm_by_onlineserver_predictresult(online_server, predict_result, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select C.*, D.MODEL_ID, D.MODEL_SEQ from (select * from SVM_PROJECT where MODEL_TYPE IN ('1','2') AND UPPER(STATUS) IN ('ONLINE','RETRAIN','RETRAINING','RETRAIN_OK') AND ONLINE_SERVER = '" + str(online_server) + r"') C, (select * from SVM_PROJECT_MODEL where UPPER(RETRAIN_TYPE) in ('ABNORMAL','ABNORMAL->ONLINE') AND PREDICT_RESULT = '" + str(predict_result) + r"' and PREDICT_START_TIME is not null and RETRAIN_END_TIME is not null) D where C.PROJECT_ID = D.PROJECT_ID"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_mtm_by_onlineserver(online_server, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT where MODEL_TYPE IN ('3') AND UPPER(STATUS) IN ('ONLINE','RETRAIN','RETRAINING','RETRAIN_OK') AND ONLINE_SERVER = '" + str(online_server) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_mtm_by_onlineserver_predictresult(online_server, predict_result, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select C.*, D.MODEL_ID, D.MODEL_SEQ from (select * from SVM_PROJECT where MODEL_TYPE IN ('3') AND UPPER(STATUS) IN ('ONLINE','RETRAIN','RETRAINING','RETRAIN_OK') AND ONLINE_SERVER = '" + str(online_server) + r"') C right join (select * from SVM_PROJECT_MODEL where UPPER(RETRAIN_TYPE) in ('ABNORMAL','ABNORMAL->ONLINE') AND PREDICT_RESULT = '" + str(predict_result) + r"' and PREDICT_START_TIME is not null and RETRAIN_END_TIME is not null) D on C.PROJECT_ID = D.PROJECT_ID"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_by_projectid(project_id, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 =ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT where PROJECT_ID = '" + str(project_id) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model


def select_project_by_status(status, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 =ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT where UPPER(STATUS) = '" + str(status) + r"' order by ITIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_by_status_ip(status, ip, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT where UPPER(STATUS) = '" + str(status) + r"' and TRAINING_SERVER = '" + str(ip) +  r"' order by ITIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_config_by_modelid_1ST_2ND_3RD(model_id, parameter_1st, parameter_2nd, parameter_3rd, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT_CONFIG where MODEL_ID = '" + str(model_id) + r"' and UPPER(PARAMETER_1ST) = '" + str(parameter_1st) + r"' and UPPER(PARAMETER_2ND) = '" + str(parameter_2nd) + r"' and UPPER(PARAMETER_3RD) = '" + str(parameter_3rd) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_config_by_modelid_parameter3RD(model_id, parameter_3rd, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT_CONFIG where MODEL_ID = '" + str(model_id) + r"' and UPPER(PARAMETER_3RD) = '" + str(parameter_3rd) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_confirmok_o2m(server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT A, SVM_PROJECT_MODEL B WHERE A.PROJECT_ID = B.PROJECT_ID AND UPPER(A.STATUS) in ('CREATING_PAUSE') AND A.MODEL_TYPE IN ('1','2') AND B.WAIT_CONFIRM in (0,2) order by A.ITIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_confirmok_m2m(server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT C, (select * from SVM_PROJECT_MODEL A, (select max(MODEL_ID) as max_model from SVM_PROJECT_MODEL group by PROJECT_ID) B where A.MODEL_ID = B.max_model and WAIT_CONFIRM in (0,2)) D where c.PROJECT_ID = D.PROJECT_ID and c.MODEL_TYPE = '3' and upper(c.STATUS) = 'CREATING_PAUSE' order by C.ITIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_by_modelid(model_id, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT_MODEL where MODEL_ID = '" + str(model_id) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_by_modelname(model_name, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT_MODEL where MODEL_NAME = '" + str(model_name) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_by_modelname_status(model_name, status, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT_MODEL where MODEL_NAME = '" + str(model_name) + r"' AND UPPER(STATUS) = '" + str(status) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_by_projectid(project_id, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT_MODEL where PROJECT_ID = '" + str(project_id) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_maxmodel_by_projectid(project_id, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT_MODEL A, (select max(MODEL_ID) as max_model from SVM_PROJECT_MODEL where PROJECT_ID = '" + str(project_id) + r"' group by PROJECT_ID) B where A.MODEL_ID = B.max_model"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_with_model_by_projectid(project_id, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT A, SVM_PROJECT_MODEL B where A.PROJECT_ID = B.PROJECT_ID AND A.PROJECT_ID = '" + str(project_id) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_with_model_by_projectid_onlineserver_predictstatus_modelstatus(project_id, online_server, predict_status, model_status, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 =ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT A, SVM_PROJECT_MODEL B where A.PROJECT_ID = B.PROJECT_ID AND A.PROJECT_ID = '" + str(project_id) + r"' AND A.ONLINE_SERVER = '" + str(online_server) + r"' AND A.PREDICT_STATUS = '" + str(predict_status) + r"' and B.STATUS = '" + str(model_status) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_by_projectid_abnormal(project_id, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT_MODEL where PROJECT_ID = '" + str(project_id) + r"' AND UPPER(RETRAIN_TYPE) in ('ABNORMAL','ABNORMAL->ONLINE')"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_by_projectid_status(project_id, status, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT_MODEL where PROJECT_ID = '" + str(project_id) + r"' AND UPPER(STATUS) = '" + str(status) + r"' order by MODEL_ID desc"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_by_projectid_status_filterfeature(project_id, status, filter_feature, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT_MODEL where PROJECT_ID = '" + str(project_id) + r"' AND UPPER(STATUS) = '" + str(status) + r"' AND FILTER_FEATURE = '" + str(filter_feature) + r"' order by MODEL_ID desc"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_by_projectid_status2_filterfeature(project_id, status1, status2, filter_feature, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT_MODEL where PROJECT_ID = '" + str(project_id) + r"' AND UPPER(STATUS) in ('" + str(status1) + r"','" + str(status2) + r"') AND FILTER_FEATURE = '" + str(filter_feature) + r"' order by MODEL_ID desc"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_by_projectid_predictresult(project_id, predict_result, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_PROJECT_MODEL where PROJECT_ID = '" + str(project_id) + r"' AND UPPER(RETRAIN_TYPE) in ('ABNORMAL','ABNORMAL->ONLINE') AND PREDICT_RESULT = '" + str(predict_result) + r"' and PREDICT_START_TIME is not null and RETRAIN_END_TIME is not null"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_online_scantime_by_projectid_datatype(project_id, datatype, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_ONLINE_SCANTIME where PROJECT_ID = '" + str(project_id)  + r"' AND UPPER(DATATYPE) = '" + str(datatype) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_online_scantime_x_by_onlineserver(online_server, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select A.*, B.DATATYPE, B.NO, B.LAST_SCANTIME from SVM_PROJECT A, SVM_ONLINE_SCANTIME B where A.PROJECT_ID = B.PROJECT_ID AND A.MODEL_TYPE IN ('1','2') AND UPPER(A.STATUS) IN ('ONLINE','RETRAIN','RETRAINING','RETRAIN_OK') AND UPPER(B.DATATYPE) IN ('X') AND ONLINE_SERVER = '" + str(online_server)  + r"' order by B.LAST_SCANTIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_online_scantime_model_x_by_onlineserver_status(online_server, status, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select C.*, D.MODEL_ID, D.MODEL_SEQ from (select A.*, B.DATATYPE, B.NO, B.LAST_SCANTIME from SVM_PROJECT A, SVM_ONLINE_SCANTIME B where A.PROJECT_ID = B.PROJECT_ID AND A.MODEL_TYPE IN ('1','2') AND UPPER(A.STATUS) IN ('ONLINE','RETRAIN','RETRAINING','RETRAIN_OK') AND UPPER(B.DATATYPE) IN ('X') AND ONLINE_SERVER = '" + str(online_server) + r"') C left join (select * from SVM_PROJECT_MODEL where UPPER(STATUS) = '" + str(status) + r"') D on C.PROJECT_ID = D.PROJECT_ID order by C.LAST_SCANTIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_online_scantime_x_many_by_onlineserver(online_server, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select A.*, B.DATATYPE, B.NO, B.LAST_SCANTIME from SVM_PROJECT A, SVM_ONLINE_SCANTIME B where A.PROJECT_ID = B.PROJECT_ID AND A.MODEL_TYPE IN ('3') AND UPPER(A.STATUS) IN ('ONLINE','RETRAIN','RETRAINING','RETRAIN_OK') AND UPPER(B.DATATYPE) IN ('X') AND ONLINE_SERVER = '" + str(online_server)  + r"' order by B.LAST_SCANTIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_online_scantime_y_by_onlineserver(online_server, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select A.*, B.DATATYPE, B.NO, B.LAST_SCANTIME from SVM_PROJECT A, SVM_ONLINE_SCANTIME B where A.PROJECT_ID = B.PROJECT_ID AND A.MODEL_TYPE IN ('1','2') AND UPPER(A.STATUS) IN ('ONLINE','RETRAIN','RETRAINING','RETRAIN_OK') AND UPPER(B.DATATYPE) IN ('Y') AND ONLINE_SERVER = '" + str(online_server)  + r"' order by B.LAST_SCANTIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_online_scantime_model_y_by_onlineserver_status(online_server, status, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select C.*, D.MODEL_ID, D.MODEL_SEQ from (select A.*, B.DATATYPE, B.NO, B.LAST_SCANTIME from SVM_PROJECT A, SVM_ONLINE_SCANTIME B where A.PROJECT_ID = B.PROJECT_ID AND A.MODEL_TYPE IN ('1','2') AND UPPER(A.STATUS) IN ('ONLINE','RETRAIN','RETRAINING','RETRAIN_OK') AND UPPER(B.DATATYPE) IN ('Y') AND ONLINE_SERVER = '" + str(online_server) + r"') C left join (select * from SVM_PROJECT_MODEL where UPPER(STATUS) = '" + str(status) + r"') D on C.PROJECT_ID = D.PROJECT_ID order by C.LAST_SCANTIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_online_scantime_y_many_by_onlineserver(online_server, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select A.*, B.DATATYPE, B.NO, B.LAST_SCANTIME from SVM_PROJECT A, SVM_ONLINE_SCANTIME B where A.PROJECT_ID = B.PROJECT_ID AND A.MODEL_TYPE IN ('3') AND UPPER(A.STATUS) IN ('ONLINE','RETRAIN','RETRAINING','RETRAIN_OK') AND UPPER(B.DATATYPE) IN ('Y') AND ONLINE_SERVER = '" + str(online_server)  + r"' order by B.LAST_SCANTIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_online_scantime_by_projectid_datatype(project_id, datatype, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select A.*, B.DATATYPE, B.NO, B.LAST_SCANTIME from SVM_PROJECT A, SVM_ONLINE_SCANTIME B where A.PROJECT_ID = B.PROJECT_ID AND UPPER(A.STATUS) IN ('ONLINE','RETRAIN','RETRAINING','RETRAIN_OK') AND A.PROJECT_ID = '" + str(project_id) + r"' AND B.DATATYPE = '" + str(datatype) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_online_parameter_by_projectid(project_id, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_ONLINE_PARAMETER where PROJECT_ID = '" + str(project_id) + r"' order by SEQ"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_online_parameter_by_projectid_xy(project_id, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_ONLINE_PARAMETER where PROJECT_ID = '" + str(project_id) + r"' and UPPER(DATATYPE) in ('X','Y') order by SEQ"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_online_parameter_by_projectid_datatype(project_id, datatype, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_ONLINE_PARAMETER where PROJECT_ID = '" + str(project_id) + r"' and DATATYPE = '" + datatype + r"' order by SEQ"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_online_parameter_and_projectx_parameter_data_by_projectid_datatype_runindex_null(project_id, projectid, datatype, runindex, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select a.*, b.PARAM_VALUE from (select * from svm_online_parameter where project_id = '" + str(projectid) + r"' and datatype = '" + datatype + r"') a LEFT JOIN (select * from " + str(project_id) + r"_parameter_data where runindex = '" + str(runindex) + r"') b on a.online_parameter = b.parameter where b.param_value is null order by a.seq"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_online_parameter_and_projectx_parameter_data_by_projectid_datatype_runindex_notnull(project_id, projectid, datatype, runindex, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select a.*, b.PARAM_VALUE from (select * from svm_online_parameter where project_id = '" + str(projectid) + r"' and datatype = '" + datatype + r"') a LEFT JOIN (select * from " + str(project_id) + r"_parameter_data where runindex = '" + str(runindex) + r"') b on a.online_parameter = b.parameter where b.param_value is not null order by a.seq"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_online_status_by_projectid_runindex(project_id, runindex, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 =ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from SVM_ONLINE_STATUS where PROJECT_ID = '" + str(project_id) + r"' and RUNINDEX = '" + str(runindex) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model
#mark
def select_projectx_10run_by_itime(project_id, last_scantime, max_time, server_name1="10.97.36.137", db_name1 = "APC"):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select top 10 * from " + str(project_id) + "_RUN where ITIME > '" + last_scantime + r"' AND ITIME < '" + max_time + r"' order by ITIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_projectx_run_by_itime(project_id, last_scantime, max_time, server_name1="10.97.36.137", db_name1 = "APC"):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = r"select * from " + str(project_id) + "_RUN where ITIME > '" + last_scantime + r"' AND ITIME < '" + max_time + r"' order by ITIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_projectx_run_by_runindex(project_id, runindex, server_name1="10.97.36.137", db_name1 = "APC"):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = "select * from " + str(project_id) + r"_RUN where RUNINDEX = '" + str(runindex) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_projectx_parameter_data_by_runindex(project_id, runindex, server_name1="10.97.36.137", db_name1 = "APC"):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = "select * from " + str(project_id) + "_PARAMETER_DATA where RUNINDEX = '" + str(runindex) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

#mark
def select_projectx_parameter_data_by_runindex_parameter(project_id, runindex, parameter, server_name1="10.97.36.137", db_name1 = "APC"):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = "select * from " + str(project_id) + "_PARAMETER_DATA where RUNINDEX = " + str(runindex) + r" and UPPER(PARAMETER) = '" + str(parameter).upper() + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_projectx_parameter_data_by_parameter_itime(project_id, parameter, itime, server_name1="10.97.36.137", db_name1 = "APC"):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    #sql = "select * from " + str(project_id) + r"_PARAMETER_DATA where UPPER(PARAMETER) = '" + str(parameter.upper()) + r"' and ITIME > '" + itime + r"' order by ITIME" 
    sql = "select B.* from " + str(project_id) + r"_RUN as A inner join " + str(project_id) + r"_PARAMETER_DATA as B on A.RUNINDEX = B.RUNINDEX Where A.MEATIME is not null and UPPER(B.PARAMETER) = '" + str(parameter.upper()) + r"' and  B.ITIME > '" + itime + r"' order by B.ITIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_projectx_parameter_data_by_parameter_itime_itime(project_id, parameter, itime1, itime2, server_name1="10.97.36.137", db_name1 = "APC"):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    #sql = "select * from " + str(project_id) + r"_PARAMETER_DATA where UPPER(PARAMETER) = '" + str(parameter.upper()) + r"' and ITIME > '" + itime1 + r"' and ITIME < '" + itime2 + r"' order by ITIME" 
    sql = "select B.* from " + str(project_id) + r"_RUN as A inner join " + str(project_id) + r"_PARAMETER_DATA as B on A.RUNINDEX = B.RUNINDEX Where A.MEATIME is not null and UPPER(B.PARAMETER) = '" + str(parameter.upper()) + r"' and  B.ITIME > '" + itime1 + r"' and B.ITIME < '" + itime2 + r"' order by B.ITIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_projectx_predict_data_all_by_runindex_parameter_isretrainpredict(project_id, runindex, parameter, is_retrain_predict, server_name1="10.97.36.137", db_name1 = "APC"):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = "select top 1 * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(runindex) + r" and PARAMETER = '" + str(parameter) + r"' and IS_RETRAIN_PREDICT = '" + str(is_retrain_predict) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

#20191009 select top1 1 Coulum to speed up
def select_projectx_predict_data_by_runindex_parameter_modelid(project_id, runindex, parameter, model_id, server_name1="10.97.36.137", db_name1 = "APC"):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = "select top 1 RUNINDEX from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(runindex) + r" and PARAMETER = '" + str(parameter) + r"' and MODEL_ID = " + str(model_id)
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model
#20191009 select top1 1 Coulum to speed up
def select_projectx_contribution_data_by_runindex_parameter_model(project_id, runindex, parameter, model, server_name1="10.97.36.137", db_name1 = "APC"):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    sql = "select top 1 RUNINDEX from " + str(project_id) + "_CONTRIBUTION_DATA where RUNINDEX = " + str(runindex) + r" and PARAMETER = '" + str(parameter) + r"' and MODEL = '" + str(model) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def update_project_STATUS_by_projectid(status, project_id, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_PROJECT SET STATUS = ? WHERE PROJECT_ID = ?", str(status), int(project_id))
    cnxn1.commit()

def update_project_predictstatus_by_projectid(predict_status, project_id, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_PROJECT SET PREDICT_STATUS = ? WHERE PROJECT_ID = ?", str(predict_status), int(project_id))
    cnxn1.commit()

def update_project_STATUS_trainingserver_by_projectid(status, training_server, project_id, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_PROJECT SET STATUS = ?, TRAINING_SERVER = ? WHERE PROJECT_ID = ?", str(status), str(training_server), int(project_id))
    cnxn1.commit()

def update_project_model_modelstatus_by_modelid(model_status, model_id, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_PROJECT_MODEL SET MODEL_STATUS = ? WHERE MODEL_ID = ?", str(model_status), int(model_id))
    cnxn1.commit()
    
def update_project_model_retrainstarttime_by_modelid(retrain_start_time, model_id, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_PROJECT_MODEL SET RETRAIN_START_TIME = ? WHERE MODEL_ID = ?", retrain_start_time, int(model_id))
    cnxn1.commit()
    
def update_project_model_predictresult_by_modelid(predict_result, model_id, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()
    sql = 'UPDATE SVM_PROJECT_MODEL SET PREDICT_RESULT = \'{}\' WHERE MODEL_ID = {}'.format(str(predict_result), int(model_id))
    cursor1.execute(sql)
    #cursor1.execute("UPDATE SVM_PROJECT_MODEL SET PREDICT_RESULT = ? WHERE MODEL_ID = ?", str(predict_result), int(model_id))
    cnxn1.commit()

def update_project_model_retrainendtime_retrainresult_by_modelid(retrain_end_time, retrain_result, model_id, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_PROJECT_MODEL SET RETRAIN_END_TIME = ?, RETRAIN_RESULT = ? WHERE MODEL_ID = ?", retrain_end_time, str(retrain_result), int(model_id))
    cnxn1.commit()
    
def update_project_model_modelstatus_modelstep_waitconfirm_by_modelid(model_status, model_step, wait_confirm, model_id, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_PROJECT_MODEL SET MODEL_STATUS = ?, MODEL_STEP = ?, WAIT_CONFIRM = ? WHERE MODEL_ID = ?", str(model_status), str(model_step), int(wait_confirm), int(model_id))
    cnxn1.commit()
    
def update_project_model_mae_mape_by_modelid(mae, mape, model_id, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_PROJECT_MODEL SET MAE = ?, MAPE = ? WHERE MODEL_ID = ?", float(mae), float(mape), int(model_id))
    cnxn1.commit()

def update_project_model_errorconfirm_by_projectid(error_confirm, project_id, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_PROJECT_MODEL SET ERROR_CONFIRM = ? WHERE PROJECT_ID = ?", int(error_confirm), int(project_id))
    cnxn1.commit()
    
def update_project_model_errorconfirm_errormessage_by_modelid(error_confirm, error_message, model_id, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_PROJECT_MODEL SET ERROR_CONFIRM = ?, ERROR_MESSAGE = ? WHERE MODEL_ID = ?", int(error_confirm), str(error_message), int(model_id))
    cnxn1.commit()
    
def update_online_scantime_lastscantime_by_projectid_datatype(last_scantime, project_id, datatype, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_ONLINE_SCANTIME SET LAST_SCANTIME = ? WHERE PROJECT_ID = ? AND DATATYPE = ?", last_scantime, int(project_id), str(datatype))
    cnxn1.commit()
    
def update_online_status_xdialarm_by_projectid_runindex(xdi_alarm, project_id, runindex, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_ONLINE_STATUS SET XDI_ALARM = ? WHERE PROJECT_ID = ? AND RUNINDEX = ?", int(xdi_alarm), int(project_id), int(runindex))
    cnxn1.commit()

def update_online_status_ydialarm_by_projectid_runindex(ydi_alarm, project_id, runindex, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_ONLINE_STATUS SET YDI_ALARM = ? WHERE PROJECT_ID = ? AND RUNINDEX = ?", int(ydi_alarm), int(project_id), int(runindex))
    cnxn1.commit()
    
def update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(projectx_predict_data, param_value, is_retrain_predict, runindex, parameter, model_id, server_name1="10.97.36.137", db_name1 = "APC"):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()
    sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {}, ITIME = getdate() WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(projectx_predict_data+"_PREDICT_DATA", param_value, is_retrain_predict, int(runindex), str(parameter), int(model_id))
    cursor1.execute(sql)       
    cnxn1.commit() 

def update_projectx_run_signal_by_runindex(projectx_run, signal, runindex, server_name1="10.97.36.137", db_name1 = "APC"):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()
    sql = 'UPDATE {} SET SIGNAL = {} WHERE RUNINDEX = {}'.format(projectx_run+"_RUN", int(signal), int(runindex))
    cursor1.execute(sql)       
    cnxn1.commit() 

def update_projectx_run_xdialarm_by_runindex(projectx_run, xdi_alarm, runindex, server_name1="10.97.36.137", db_name1 = "APC"):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()
    sql = 'UPDATE {} SET XDI_ALARM = {} WHERE RUNINDEX = {}'.format(projectx_run+"_RUN", int(xdi_alarm), int(runindex))
    cursor1.execute(sql)       
    cnxn1.commit() 

def update_projectx_run_ydialarm_by_runindex(projectx_run, ydi_alarm, runindex, server_name1="10.97.36.137", db_name1 = "APC"):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()
    sql = 'UPDATE {} SET YDI_ALARM = {} WHERE RUNINDEX = {}'.format(projectx_run+"_RUN", int(ydi_alarm), int(runindex))
    cursor1.execute(sql)       
    cnxn1.commit() 

def update_projectx_contribution_data_contribution_by_runindex_parameter_model(projectx_contribution_data, contribution, runindex, parameter, model, server_name1="10.97.36.137", db_name1 = "APC"):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()
    sql = 'UPDATE {} SET CONTRIBUTION = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL = \'{}\''.format(projectx_contribution_data+"_CONTRIBUTION_DATA", float(contribution), int(runindex), str(parameter), str(model))
    cursor1.execute(sql)       
    cnxn1.commit() 
    
def insert_online_status_projectid_runindex_xdialarm_itime(project_id, runindex, xdi_alarm, itime, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()  
    cursor1.execute("insert into SVM_ONLINE_STATUS(PROJECT_ID, RUNINDEX, XDI_ALARM, ITIME) values (?,?,?,?)", int(project_id), int(runindex), int(xdi_alarm), itime)
    cnxn1.commit() 

def insert_online_status_projectid_runindex_ydialarm_itime(project_id, runindex, ydi_alarm, itime, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()  
    cursor1.execute("insert into SVM_ONLINE_STATUS(PROJECT_ID, RUNINDEX, YDI_ALARM, ITIME) values (?,?,?,?)", int(project_id), int(runindex), int(ydi_alarm), itime)
    cnxn1.commit() 

def insert_estone_projectid_mapkey_detectedtime_estonetime_estoneuserid_estonetaskid_estonestatus(project_id, map_key, detected_time, estone_time, estone_userid, estone_taskid, estone_status, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()  
    cursor1.execute("insert into SVM_ESTONE(PROJECT_ID, MAP_KEY, DETECTED_TIME, ESTONE_TIME, ESTONE_USERID, ESTONE_TASKID, ESTONE_STATUS) values (?,?,?,?,?,?,?)", int(project_id), map_key, detected_time, estone_time, estone_userid, str(estone_taskid), str(estone_status))
    cnxn1.commit() 
    
def insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(projectx_predict_data, runindex, parameter, param_value, model_id, is_retrain_predict, server_name1=ServerConfig.SmartPrediction_DBServer_IP, db_name1 = ServerConfig.SmartPrediction_Config_DB):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
        
    #APLog.WriteApLog("server_name1:"+db_name1+ str(projectx_predict_data) ,  3344)
    cursor1 = cnxn1.cursor()
    sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},getdate())'.format(projectx_predict_data+"_PREDICT_DATA", int(runindex), str(parameter), param_value, int(model_id), is_retrain_predict)
    #APLog.WriteApLog("insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict:" + sql , 3344)
    
    cursor1.execute(sql)       
    cnxn1.commit()  
    
def insert_projectx_contribution_data_runindex_model_parameter_contribution(projectx_contribution_data, runindex, model, parameter, contribution, server_name1="10.97.36.137", db_name1 = "APC"):
    cnxn1 = DB_Connection(server_name=server_name1, db_name=db_name1)
    
    cursor1 = cnxn1.cursor()
    sql = 'INSERT INTO {} (RUNINDEX,MODEL,PARAMETER,CONTRIBUTION,ITIME) VALUES ({},\'{}\',\'{}\',{},getdate())'.format(projectx_contribution_data+"_CONTRIBUTION_DATA", int(runindex), str(model), str(parameter), float(contribution))
    cursor1.execute(sql)       
    cnxn1.commit() 
    
   

# Data_Preview.py
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
import multiprocessing
try:
    import pandas_profiling as pp
    # pp_exist = True
    pp_exist = False
except:
    pp_exist = False
plt.style.use("ggplot")


def Entropy(labels):
    probs = pd.Series(labels).value_counts() / len(labels)
    en = entropy(probs)
    return en


def mean_avg_not_overlap(x, window):
    size = x.shape[0]
    step = size/window
    mean_avg = []
    for i in range(0, int(step)):
        init = i*window
        mean_avg.append(x[init:init+window].mean())
    return np.array(mean_avg)



def Data_Preview(folder_path):
    ####### output path #######
    output_path = {}
    output_path["missing_path"] = os.path.join(folder_path, "missing_rate_list.csv")
    output_path["outspec_path"] = os.path.join(folder_path, "outspec_table.csv")
    output_path["summary_path"] = os.path.join(folder_path, "summary_table.csv")
    output_path["chi_path"] = os.path.join(folder_path, "chinese_transform.csv")
    output_path["missing_summary_path"] = os.path.join(folder_path, "Missing_summary.csv")
    output_path["pfr"] = os.path.join(folder_path, "pandas_profile.html")
    ############################
    error_msg = "Please contact APC team to solve the problem."
    msg = None
    input_path = read_path(folder_path)
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    mylog.init_logger()
    config = read_config(input_path["config_path"], mylog)
    
    mylog.info("-----Data Preview------")
    
    std_T = config["DataPreview"]["STD_T"]
    outspec_T = config["DataPreview"]["Outspec_T"]
    missing_T = config["Remove_Missing_Value"]["Threshold"]
    exclude_col = config["Index_Columns"]
    window = config["DataPreview"]["Window"]
    y_values = config["Y"]
    exclude_col.extend(y_values)

    status = "OK"
    
    try:
        df_1 = pd.read_csv(input_path["raw_path"],encoding="utf-8")
    except Exception as e:
        mylog.warning("Fail to read in utf-8")
        try:
            df_1 = pd.read_csv(input_path["raw_path"], encoding="big5")
        except Exception as e:
            mylog.warning("Fail to read in big5")
            try:
                df_1 = pd.read_csv(input_path["raw_path"], encoding="big5hkscs")
            except Exception as e:
                mylog.warning("Fail to read in big5hkscs")
                mylog.error("Data Preview: Read raw data error")
                mylog.error_trace(e)
                msg = error_msg
                return status, msg
    
    ### Check for Chinese    
    check=False
    mylog.info("**Check for Chinese characters**")
    cols = df_1.columns
    chi_df = pd.DataFrame(columns=["Before", "After"])
    for col in cols:
        if re.search(u'[\u2E80-\u2FD5\u3190-\u319f\u3400-\u4DBF\u4E00-\u9FCC\uF900-\uFAAD]', col):
            check = True
            new_str = ""
            for string in col:
                if re.search(u'[\u2E80-\u2FD5\u3190-\u319f\u3400-\u4DBF\u4E00-\u9FCC\uF900-\uFAAD]', string):
                    new_str+="XX"
                else:
                    new_str+=string
            mylog.info(col+" ---> "+new_str)
            chi_df.loc[col,"Before"] = col
            chi_df.loc[col,"After"] = new_str
            df_1 = df_1.rename(columns={col : new_str})

    if check:
        try:
            df_1.to_csv(input_path["raw_path"], index=False)
        except Exception as e:
            mylog.error("Data Review: Error while saving raw data")
            mylog.error_trace(e)
            msg = error_msg
        try:
            chi_df.to_csv(output_path["chi_path"], index=False)
        except Exception as e:
            mylog.error("Data Preview: Error while saving chinese_transform.csv")
            mylog.error_trace(e)
            msg = error_msg
    mylog.info("**Done**")

    ### pandas-profiling
    if pp_exist:
        mylog.info("---Pandas Profiling Start---")
        # print(multiprocessing.cpu_count())
        try:
            pfr = pp.ProfileReport(df_1, check_correlation=False, pool_size=1)
            pfr.to_file(output_path["pfr"])
        except Exception as e:
            try:
                pfr = pp.ProfileReport(df_1, check_correlation_pearson=False, pool_size=1)
                pfr.to_file(output_path["pfr"])
            except Exception as e:
                mylog.error("Data Preview: Error while doing pandas-profiling")
                mylog.error_trace(e)
                msg = error_msg
        mylog.info("---Pandas Profiling Done---")

    ### check exclude_col
    if exclude_col:
        for col in exclude_col:
            df_1[col] = df_1[col].astype(str)
        df_1["Test_exclude_col"] = df_1[exclude_col].apply(lambda x: '_'.join(x), axis=1)
        if df_1.duplicated('Test_exclude_col').sum() > 0:
            mylog.error("Data Preview: Duplicated index columns")
            return status, "Duplicated index columns"
        else:
            df_1 = df_1.drop(["Test_exclude_col"], axis=1)

    ### shape of data
    mylog.info("Shape of data: {}".format(df_1.shape))

    ### Three std
    try:
        if exclude_col:
            df_1 = df_1.drop(exclude_col, axis=1)
    except Exception as e:
        mylog.error("Data Preview: Error while excluding features")
        mylog.error_trace(e)
        msg = error_msg
    
    total_row_num = df_1.values.shape[0]
    missing_list = []
    # drop cols that are type of object and Tell if col has NaN
    missing_table = pd.DataFrame(columns=["Features", "Missing_rate_percentage"])
    not_num_col = list(df_1.select_dtypes(exclude=['number']).columns)
    missing_summary = pd.DataFrame(columns=["Missing_Number", "No_Missing_Number",
                                            "Missing_and_Exceed_Number", "Total_Number"])
    missing_number = 0
    total_number = len(df_1.columns)
    for col in df_1.columns:
        if col in not_num_col:
            continue
        nan_cnt = sum(df_1.loc[:, col].isna())
        if nan_cnt > 0:
            missing_number += 1
        missing_table.loc[col, "Features"] = col
        missing_table.loc[col, "Missing_rate_percentage"] = nan_cnt/total_row_num*100
        if nan_cnt/total_row_num > missing_T:
            missing_list.append(col)

    missing_and_exceed = len(missing_list)
    missing_summary["Missing_Number"] = [missing_number]
    missing_summary["No_Missing_Number"] = [total_number - missing_number]
    missing_summary["Missing_and_Exceed_Number"] = [missing_and_exceed]
    missing_summary["Total_Number"] = [total_number]
    missing_summary.to_csv(output_path["missing_summary_path"], index=False)

    missing_table.sort_values(by=["Missing_rate_percentage"], ascending=False)
    missing_table_over_T = missing_table[missing_table["Missing_rate_percentage"] >= missing_T*100].copy()

    if missing_table_over_T.values.shape[0] != 0:
        try:
            fig, ax = plt.subplots(figsize=(10, 6))
            g = sns.barplot(x="Features", y="Missing_rate_percentage", data=missing_table_over_T, ax=ax)
            g.set_xticklabels(g.get_xticklabels(), rotation=90)
            plt.tight_layout()
            tmp_path = os.path.join(folder_path, "Missing_rate.png")
            plt.savefig(tmp_path)
        except Exception as e:
            mylog.warning("Data Preview: Error while creating missing rate figure.")
            mylog.warning_trace(e)
            msg = error_msg

        try:
            missing_table_over_T.to_csv(output_path["missing_path"], index=False)
        except Exception as e:
            mylog.warning("Data Preview: Error while saving missing rate table")
            mylog.warning_trace(e)
            msg = error_msg
    else:
        missing_table.to_csv(output_path["missing_path"], index=False)
        mylog.info("Data Preview: **No high missing rate feature**")

    mylog.info("< Note that missing values are ignored and high-missing-rate features will be "
               "dropped during data preview>")
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
        mylog.error("Data Preview: Error while dropping object-type features")
        object_str = ','.join(str(e) for e in not_num_col)
        mylog.error("Please check the type of these features:"+ object_str)
        mylog.error_trace(e)
        msg = error_msg
        return status, msg
    
    try:
        if missing_list:
            df_1 = df_1.drop(missing_list, axis=1)
    except Exception as e:
        mylog.error("Data Preview: Error while dropping high-missing-rate features")
        missing_str = ','.join(str(e) for e in missing_list)
        mylog.error("Please check the these features:"+ missing_str)
        mylog.error_trace(e)
        msg = error_msg
        return status, msg

#    potential_out = []
    summary = pd.DataFrame(columns=["feature", "mean", "std", "num_out", "num_out_percentage",
                                    "skewness", "kurtosis", "entropy"])
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
            summary.loc[col, "feature"] = col
        
            num_out = 0
            for row in param_col:
                if abs(row - mean) > std_T*std:
                    num_out+=1

            summary.loc[col, "num_out"] = num_out
            num_total = len(param_col.index.tolist())
            summary.loc[col, "num_out_percentage"] = num_out/num_total*100
        except Exception as e:
            mylog.error("Data Preview: Error while caluculating properties of "+str(col))
            mylog.error_trace(e)
            msg = error_msg
            return status, msg
    summary = summary.sort_values(by="num_out_percentage", ascending=False)
    try:
        summary.to_csv(output_path["summary_path"], index=False)
    except Exception as e:
        mylog.warning("Data Preview: Error while saving summary table")
        mylog.warning_trace(e)
        
    try:
        summary_out = summary[summary["num_out_percentage"] > outspec_T*100]\
                    .sort_values(["num_out_percentage"], ascending=False)\
                    .reset_index().apply(pd.to_numeric, errors='ignore')
    except Exception as e:
        mylog.error("Data Preview: Error while reducing summary table")
        mylog.error_trace(e)
        msg = error_msg
        return status, msg

    if summary_out.values.shape[0] != 0:
        for col in summary_out["index"].tolist():
            try:
                fig, ax = plt.subplots(figsize=(10,6))
                percent_str = "{0:.2f}".format(summary_out[summary_out["index"] == col]["num_out_percentage"].values[0])
                ax.set_title(col+" ("+percent_str+" %)")
                sns.distplot(df_1[col].dropna(), ax=ax, kde=False)
                tmp_path = os.path.join(folder_path, col+".png")
                plt.savefig(tmp_path)
            except Exception as e:
                mylog.warning("Data Preview: Error while creating outspec figures")
                mylog.warning_trace(e)

        try:
            number_table = pd.DataFrame(columns=["Number of features"],
                                        data = summary_out["num_out_percentage"].value_counts().values,
                                       index = summary_out["num_out_percentage"].value_counts().index).sort_index().reset_index()
            number_table.rename(columns={"index": "Outspec_Ratio_percentage"}, inplace=True)
        except Exception as e:
            mylog.warning("Data Preview: Error while creating outspec table")
            mylog.warning_trace(e)
            msg = error_msg

        try:
            number_table.to_csv(output_path["outspec_path"],index=False)
        except Exception as e:
            mylog.warning("Data Preview: Error while saving outspec table")
            mylog.warning_trace(e)
            msg = error_msg
            return status, msg

        try:
            fig, ax = plt.subplots(figsize=(10,6))
            sns.barplot(x="Outspec_Ratio_percentage", y="Number of features", data=number_table, ax=ax)
            ax.set_xticklabels(labels=np.around(number_table["Outspec_Ratio_percentage"].values.tolist(), decimals=2))
            tmp_path = os.path.join(folder_path, "Data_Preview.png")
            plt.savefig(tmp_path)
        except Exception as e:
            mylog.warning("Data Review: Error while creating Data Preview figure")
            mylog.warning_trace(e)
            msg = error_msg

        num_out_idx = summary_out["num_out_percentage"].unique().tolist()
        num_out_dict = summary_out.groupby("num_out_percentage")["index"].apply(dict) # saved as dictionary

        for idx in num_out_idx:
            mylog.info("Features that have "+"{0:.2f}".format(idx)+"% data out of "+str(std_T)+" std :")
            mylog.info(','.join(str(e) for e in num_out_dict[idx].tolist()))
            mylog.info("")

        status = "NG"
    else:
        summary_out.to_csv(output_path["outspec_path"],index=False)
        mylog.info("Data Preview: **No feature is outspec**")

    ### constant
    const_params = []
    for col in df_1.columns:
        try:
            num_row = df_1.loc[:,col].dropna().values.shape[0]
            constant_entropy_limit = Entropy([0]*(num_row-1) + [1]*1)
            if summary.loc[col, "entropy"] < constant_entropy_limit:
                const_params.append(col)
        except Exception as e:
            mylog.warning("Data Preview: Error while doing constant check of "+str(col))
            mylog.warning_trace(e)
            msg = error_msg

    mylog.info("Total "+str(len(const_params))+" features are constant. List below:")
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
            mylog.warning("Data Review: Error while doing trend check of "+str(col))
            mylog.warning_trace(e)
            msg = error_msg

    mylog.info("There are "+str(cnt)+" data with trend")
    mylog.info("Increase("+str(len(up))+") : ")
    mylog.info(','.join(str(e) for e in up))
    mylog.info("")
    mylog.info("Decrease("+str(len(dn))+") : ")
    mylog.info(','.join(str(e) for e in dn))
    mylog.info("")
    mylog.info('-----Data Preview Done-----')

    return status, msg
#####################################################


if __name__ == "__main__":
    # path = "../Cases/CVD2E_Split1_Test/CVD2E_Split1_Test_00/00/02_ParameterReview/"
    # path = "../Cases/PSH_Demo/PSH_00/00/02_ParameterReview/"
    path = "../Cases/Test/02_ParameterReview/"
    Data_Preview(path)


	
# Data_PreProcess.py
import pandas as pd
import datetime
import numpy as np
import os
from sklearn.model_selection import train_test_split
from config import read_config, save_config
from DataImputation import DataImputation, TESTDataImputation
from DataTransform import DataTransform, TESTDataTransform, OnlineDataTransform
from CreateLog import WriteLog
from Read_path import read_path
from DataTransformY import BuildDataTransformY, TrainDataTransformY

#########################
# ToDo deal with no string data in train but in test problem
#########################


def Preprocess_Prepare(folder_paths, Mode):
    ####
    output_paths = {}
    if Mode == "Train":
        output_paths["data_train_path"] = os.path.join(folder_paths, 'Data_train.csv')
        output_paths["data_test_path"] = os.path.join(folder_paths, 'Data_test.csv')
        output_paths["x_train_path"] = os.path.join(folder_paths, 'x_Train.csv')
        output_paths["x_train_no_z_path"] = os.path.join(folder_paths, 'x_Train_no_z.csv')
        output_paths["y_train_path"] = os.path.join(folder_paths, 'y_Train.csv')
        output_paths["Cat_config_path"] = os.path.join(folder_paths, 'Cat_Config.json')
        output_paths["DataImpute_config_path"] = os.path.join(folder_paths, 'DataImpute_Config.json')
        output_paths["DataTrans_config_path"] = os.path.join(folder_paths, 'DataTrans_Config.json')
        output_paths["DelMissingCol_config_path"] = os.path.join(folder_paths, 'DelMissingCol_Config.json')
        output_paths["DelNonNumCol_config_path"] = os.path.join(folder_paths, 'DelNonNumCol_Config.json')
        output_paths["config_Y_path"] = os.path.join(folder_paths, 'config_Y.json')
        output_paths["y_train_T_path"] = os.path.join(folder_paths, 'y_Train_T.csv')
    elif Mode == "Test":
        output_paths["x_test_path"] = os.path.join(folder_paths, 'x_Test.csv')
        output_paths["y_test_path"] = os.path.join(folder_paths, 'y_Test.csv')
        output_paths["x_test_no_z_path"] = os.path.join(folder_paths, 'x_Test_no_z.csv')

    ####
    input_paths = read_path(folder_paths)
    mylogs = WriteLog(input_paths["log_path"], input_paths["error_path"])
    configs = read_config(input_paths["config_path"], mylogs)

    return input_paths, output_paths, mylogs, configs


def read_data(data_paths, mylogs):
    try:
        dfs = pd.read_csv(data_paths)
    except Exception as e:
        mylogs.error("Data Pre-Process Read Data Error!!!")
        mylogs.error_trace(e)
        raise

    return dfs


def RemoveColsInReview(dfs, remove_cols, mylogs):
    try:
        return dfs.drop(remove_cols, axis=1)

    except Exception as e:
        mylogs.warning('Data PreProcess: Cannot drop features assigned in Parameter Review')
        mylogs.warning('Data PreProcess: Start drop one by one')
        mylogs.warning_trace(e)
        for cols in remove_cols:
            try:
                dfs = dfs.drop(cols, axis=1)
            except Exception as ee:
                mylogs.warning(cols + " is missing already.")
                mylogs.warning_trace(ee)
        mylogs.warning('Data PreProcess: Revised drop cols mode finished')
        return dfs


def CHECKImputeCol(dfs_, col_list_, cat_configs_, mylogs_):

    list_not_exist = []
    for col in col_list_:
        # check Merge col status (Exist Check)
        if col not in dfs_.columns:
            list_not_exist.append(col)
            continue

        # check Merge col status (Missing Check)
        na_amount = sum(dfs_[col].isna())
        if na_amount != 0:
            impute_value = cat_configs_[col]["Impute_Values"]
            dfs_[col].fillna(impute_value, inplace=True)
            msg = " ".join([str(na_amount), "missing values are found in", col, ". "])
            print(msg)
            print(impute_value)
            msg = msg + " ".join(["They are imputed by", impute_value, "."])
            print(impute_value)
            mylogs_.info(msg)

    if len(list_not_exist) != 0:
        col_str = ', '.join(list_not_exist)
        mylogs_.info("The Following Columns don't exist: " + col_str)

    return dfs_


def CheckCatCol(dfs, configs, cat_config_paths, mylogs):
    check_list = set()
    cat_configs = {}

    # group mode ON means group list is not empty
    if ("DataImpute" in configs["Data_Preprocess_Steps"]) & (len(configs["Data_Imputation"]["Group_List"]) != 0):
        check_list = check_list | set(configs["Data_Imputation"]["Group_List"])

    if ("DataTrans" in configs["Data_Preprocess_Steps"]) & (len(configs["Data_Transform"]["Group_List"]) != 0):
        check_list = check_list | set(configs["Data_Transform"]["Group_List"])

    if len(check_list) == 0:
        cat_configs["Group_Mode"] = 0
        mylogs.info("Data PreProcess\tThere are no Category Columns have to be Checked")

    elif len(check_list) != 0:
        cat_configs["Cat_list"] = list(check_list)
        cat_configs["Group_Mode"] = 1
        for col in check_list:
            # 20190820 prevent na value is the mode drop the na first.
            list_col = list(dfs[col].copy().dropna())
            impute_value = max(list_col, key=list_col.count)
            cat_configs[col] = {}
            cat_configs[col]["Impute_Values"] = impute_value

        dfs = CHECKImputeCol(dfs, check_list, cat_configs, mylogs)

    save_config(cat_config_paths, cat_configs, mylogs)

    return dfs, check_list


def TESTCheckCatCol(dfs, configs, cat_config_paths, mylogs):

    cat_configs = read_config(cat_config_paths, mylogs)
    if cat_configs["Group_Mode"] == 1:
        dfs = CHECKImputeCol(dfs, cat_configs["Cat_list"], cat_configs, mylogs)

    return dfs


def preSortGroupCol(configs):
    configs["Data_Imputation"]["Group_List"].sort()
    configs["Data_Transform"]["Group_List"].sort()
    return configs


def DataSplit(dfs, configs, output_path, mylogs):

    # the Data Split Module is designed based on Sci-kit Learn
    # This Module supports test rate is zero.
    # If this module occurs ERROR (including train rate is zero),
    #  the module will split data by default setting: test rate 20% & no random

    # Future work: reduce config to two parameter 1. size(int or float) 2. random

    try:
        _error_mode = 0
        n_sample = len(dfs)

        if n_sample == 0:
            mylogs.error("NO DATA ERROR (Data Split)\t Please Check your Data again.")
            raise ValueError("At least one array required as input")

        elif n_sample == 1:
            mylogs.warning("DATA WARNING (Data Split)\t ONLY ONE DATA, it would be the train data")
            dfs_train = dfs.copy()
            dfs_test = pd.DataFrame(columns=dfs.columns)

        elif n_sample >= 2:

            if configs["Data_Split"]["Setting_Spilt_Mode"] in ["Amount", "Ratio"]:

                shuffle_check = configs["Data_Split"]["Setting_Random_Mode"] == 1

                if configs["Data_Split"]["Setting_Spilt_Mode"] == "Amount":
                    test_size = configs["Data_Split"]["Parameter_Split_Amount"]
                    if (np.array(test_size).dtype.kind == 'i') & (test_size < n_sample) & (test_size > 0):

                        dfs_train, dfs_test = train_test_split(dfs,
                                                               test_size=test_size,
                                                               shuffle=shuffle_check)

                    elif (np.array(test_size).dtype.kind == 'i') & (test_size == 0):
                        dfs_train = dfs.copy()
                        dfs_test = pd.DataFrame(columns=dfs.columns)

                    else:
                        _error_mode = 1

                elif configs["Data_Split"]["Setting_Spilt_Mode"] == "Ratio":
                    test_size = configs["Data_Split"]["Parameter_Test_Rate"]
                    if (np.array(test_size).dtype.kind == 'f') & (test_size > 0) & (test_size < 1):
                        dfs_train, dfs_test = train_test_split(dfs,
                                                               test_size=test_size,
                                                               shuffle=shuffle_check)

                    elif (np.array(test_size).dtype.kind in ['f', "i"]) & (test_size == 0):
                        dfs_train = dfs.copy()
                        dfs_test = pd.DataFrame(columns=dfs.columns)

                    else:
                        _error_mode = 1

            else:
                _error_mode = 1

    except Exception as e:
        mylogs.warning("EXECUTION ERROR in Data Split Module\t Module will execute by default value.")
        mylogs.warning_trace(e)
        dfs_train, dfs_test = train_test_split(dfs, test_size=0.2, shuffle=False)

    # config error
    if _error_mode == 1:
        mylogs.warning("INPUT ERROR in The Data Split\t Module will execute by default value.")
        mylogs.warning("Input config Value:\t" + str(configs["Data_Split"]))
        mylogs.warning("Input data shape:\t" + str(dfs.shape))
        dfs_train, dfs_test = train_test_split(dfs, test_size=0.2, shuffle=False)

    dfs_train.to_csv(output_path["data_train_path"], index=False)
    dfs_test.to_csv(output_path["data_test_path"], index=False)

    return dfs_train, dfs_test


def removeObjectCol(dfs, configs, mylogs):

    ## if user choose not to transform time to secs the columns will be remove.

    DelNonNumCol_configs = {}
    DelNonNumCol_configs["Time_Columns"] = []
    ori_columns = dfs.columns
    for col in ori_columns:
        # 20190826 revised because this function will delete group col
        # if (col in configs["Index_Columns"]) | (col in configs["Y"]):
        #     continue
        if (col in configs["IDX"]) | (col in configs["Y"]):
            continue

        if dfs[col].dtypes == 'object':
            try:
                dfs[col] = pd.to_datetime(dfs[col], format='%Y-%m-%d %H:%M:%S.%f')
                if configs['Remove_Nonnumerical_Columns']["Setting_Transform_Time_to_Seconds"] == 1:
                    dfs[col] = dfs[col].apply(transform2Second, mylog=mylogs)
                    DelNonNumCol_configs["Time_Columns"].append(col)
                elif configs['Remove_Nonnumerical_Columns']["Setting_Transform_Time_to_Seconds"] == 0:
                    dfs = dfs.drop(col, axis=1)

            except:
                dfs = dfs.drop(col, axis=1)
                continue
    new_columns = dfs.columns
    # print(set(ori_columns) - set(new_columns))
    DelNonNumCol_configs["Remove_Nonnumerical_Columns"] = list(set(ori_columns) - set(new_columns))
    return dfs, configs, DelNonNumCol_configs


def TESTremoveObjectCol(dfs, configs, DelNonNumCol_configs, mylogs):

    for col in DelNonNumCol_configs["Remove_Nonnumerical_Columns"]:
        try:
            dfs = dfs.drop(col, axis=1)
        except Exception as e:
            mylogs.warning("Data PreProcess (Test): Cannot drop the nonnumerical feature: " + str(col))
            mylogs.warning_trace(e)

    for col in DelNonNumCol_configs["Time_Columns"]:
        try:
            dfs[col] = pd.to_datetime(dfs[col], format='%Y-%m-%d %H:%M:%S.%f')
            if configs['Remove_Nonnumerical_Columns']["Setting_Transform_Time_to_Seconds"] == 1:
                dfs[col] = dfs[col].apply(transform2Second, mylog=mylogs)
        except Exception as e:
            mylogs.warning("Data PreProcess (Test): CAN NOT TRANSFORM TIME TO SECOND: " + str(col))
            mylogs.warning("Data PreProcess (Test): " + str(col) + " will be replace to an empty col.")
            mylogs.warning_trace(e)
            dfs[col] = np.nan

    return dfs


def transform2Second(x, mylog):
    if pd.isnull(x):
        return np.nan
    else:
        try:
            return float((x - datetime.datetime(1970, 1, 1)).total_seconds())
        except Exception as e:
            mylog.warning("Cannot transform datatime to second")
            mylog.warning('x:', x)
            mylog.warning_trace(e)
            return np.nan


def removeMissingCol(dfs, configs, mylogs):

    DelMissingCol_configs = {}
    threshold = dfs.shape[0] * configs["Remove_Missing_Value"]["Threshold"]
    remove_cols = [c for c in dfs.columns if sum(dfs[c].isnull()) >= threshold]

    # Prevent mis-delete Index Columns
    remove_cols = [x for x in remove_cols if x not in configs["IDX"]]
    #
    DelMissingCol_configs["Remove_Missing_Columns"] = remove_cols
    dfs = dfs.drop(remove_cols, axis=1)

    msg = "The following col are remove because of high missing rate: " + ", ".join(remove_cols)
    mylogs.info(msg)
    return dfs, configs, DelMissingCol_configs


def TESTremoveMissingCol(dfs, DelMissingCol_configs, mylogs):
    for col in DelMissingCol_configs["Remove_Missing_Columns"]:
        try:
            dfs = dfs.drop(col, axis=1)
        except:
            msg = "Data PreProcess (Test): Cannot drop missing col: " + str(col)
            mylogs.warning(msg)

    return dfs


def mergeCategoryCol(dfs, configs):

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


def string2float(x, mylog):
    if pd.isnull(x):
        return np.nan
    else:
        try:
            return float(x)
        except Exception as e:
            msgs = "Cannot transform to float:\t" + x
            mylog.warning(msgs)
            mylog.warning_trace(e)
            return np.nan


def OnlineDataFloatTypeCheck(dfs, configs, cat_config_paths, non_num_config_paths, missing_config_path, mylogs):

    DelNonNumCol_configs = read_config(non_num_config_paths, mylogs)
    DelMissingCol_configs = read_config(missing_config_path, mylogs)
    cat_configs = read_config(cat_config_paths, mylogs)

    if cat_configs["Group_Mode"] == 1:
        cat_list = cat_configs["Cat_list"]
    elif cat_configs["Group_Mode"] == 0:
        cat_list = []
    else:
        mylogs.warning("Data PreProcess Online\tCat config ERROR")
        cat_list = []

    for col in dfs.columns:
        # 20190820 Y should be transform to float
        if col in configs["IDX"]:
            continue
        # if (col in configs["IDX"]) | (col in configs["Y"]):
        #     continue
        if (col in cat_list) | (col in DelMissingCol_configs["Remove_Missing_Columns"]):
            continue
        if (col in DelNonNumCol_configs["Remove_Nonnumerical_Columns"]) | (col in DelNonNumCol_configs["Time_Columns"]):
            continue

        # dfs[col] = "膜偏量測圖形"
        dfs[col] = dfs[col].apply(string2float, mylog=mylogs)

    return dfs


def OnlineDataMissingDetection(dfs, configs, mylogs):

    missing_lists = []

    dfs = dfs.drop(configs["IDX"], axis=1)
    x_amount = dfs.shape[1]
    for i in dfs.index:
        missing_lists.append(sum(dfs.loc[i].isna())/x_amount)

    return missing_lists


def Data_PreProcess_Train(folder_path, mode):
    input_path, output_path, mylog, config = Preprocess_Prepare(folder_path, Mode="Train")

    config["IDX"] = config["Index_Columns"].copy()
    if config["Filter_Feature"] is not None:
        if config["Filter_Feature"] not in config["Index_Columns"]:
            config["IDX"].append(config["Filter_Feature"])

    if mode in ["Train"]:
        mylog.info("Data PreProcess (Train)\tStart")
    elif mode in ["Merge"]:
        mylog.info("Data PreProcess (Merge)\tStart")

    try:
        df_train = read_data(input_path["raw_path"], mylog)
    except Exception as e:
        mylog.error("Data PreProcess (Train/Merge)\tRead Data Error")
        mylog.error_trace(e)
        return "ERROR", "Please Call APC for Troubleshooting."

    # Remove the Cols which are deleted by user in Data Preview
    df_train = RemoveColsInReview(df_train, config['DataPreview']['RemoveInReview'], mylog)

    # # Build Y Transform Model
    # try:
    #     mylog.info("Data PreProcess (Train/Merge)\tBuild Y Transform Model")
    #     Y_config = BuildDataTransformY(df_train, config, "Z_Score", mylog)
    #     save_config(output_path["config_Y_path"], Y_config, mylog)
    #
    # except Exception as e:
    #     mylog.error("Data PreProcess (Train/Merge)\tBuild Y Transform Model Error")
    #     mylog.error_trace(e)
    #     return "ERROR", "Please Call APC for Troubleshooting."

    try:
        df_train, group_list = CheckCatCol(df_train, config, output_path['Cat_config_path'], mylog)
        # print(config["IDX"])
        for col in group_list:
            if col not in config["IDX"]:
                config["IDX"].append(col)
        # print(config["IDX"])
    except Exception as e:
        mylog.error("Data PreProcess (Train/Merge)\tChech Category Col ERROR")
        mylog.error_trace(e)
        return "ERROR", "Please Call APC for Troubleshooting."
    
    config = preSortGroupCol(config)
    
    # TRAIN STEP
    mylog.info("**TRAINING PROCESS START**")
    for step_index in config['Data_Preprocess_Steps']:
        if step_index == "DataSplit":
            try:
                if mode in ["Train"]:
                    mylog.info("Data PreProcess (Train/Merge)\tData Split")
                    df_train, df_test = DataSplit(df_train, config, output_path, mylog)

                elif mode in ["Merge"]:
                    df_train.to_csv(output_path["data_train_path"], index=False)

            except Exception as e:
                mylog.error("Data PreProcess (Train/Merge)\tData Split ERROR")
                mylog.error_trace(e)
                return "ERROR", "Please Call APC for Troubleshooting."

            # Build Y Transform Model
            try:
                mylog.info("Data PreProcess (Train/Merge)\tBuild Y Transform Model")
                Y_config = BuildDataTransformY(df_train, config, "Z_Score", mylog)
                save_config(output_path["config_Y_path"], Y_config, mylog)

            except Exception as e:
                mylog.error("Data PreProcess (Train/Merge)\tBuild Y Transform Model Error")
                mylog.error_trace(e)
                return "ERROR", "Please Call APC for Troubleshooting."

        if step_index == "DelNonNumCol":
            try:
                mylog.info("Data PreProcess (Train/Merge)\tRemove Nonnumerical Columns")
                df_train, config, DelNonNumCol_config = removeObjectCol(df_train, config, mylog)
                save_config(output_path["DelNonNumCol_config_path"], DelNonNumCol_config, mylog)
                continue
            except Exception as e:
                mylog.error("Data PreProcess (Train/Merge)\tRemove Nonnumerical Columns ERROR")
                mylog.error_trace(e)
                return "ERROR", "Please Call APC for Troubleshooting."
            
        elif step_index == "DelMissingCol":
            try:
                mylog.info("Data PreProcess (Train/Merge)\tRemove Missing Value")
                df_train, config, DelMissingCol_config = removeMissingCol(df_train, config, mylog)
                save_config(output_path["DelMissingCol_config_path"], DelMissingCol_config, mylog)
                continue
            except Exception as e:
                mylog.error("Data PreProcess (Train/Merge)\tRemove Missing Value ERROR")
                mylog.error_trace(e)
                return "ERROR", "Please Call APC for Troubleshooting."
                    
        # elif step_index == "MergeCatCol":
        #     mylog.info("Merge_Category_Columns")
        #     df_train, config = mergeCategoryCol(df_train, config)
        elif step_index == "DataImpute":
            try:
                mylog.info("Data PreProcess (Train/Merge)\tData Imputation")
                df_train, config, DataImpute_config = DataImputation(df_train, config)
                save_config(output_path["DataImpute_config_path"], DataImpute_config, mylog)
                continue
            except Exception as e:
                mylog.error("Data PreProcess (Train/Merge)\tData Imputation ERROR")
                mylog.error_trace(e)
                return "ERROR", "Please Call APC for Troubleshooting."

        elif step_index == "DataTrans":
            try:
                mylog.info("Data PreProcess (Train/Merge)\tData Transform")
                df_train.to_csv(output_path["x_train_no_z_path"], index=False)
                df_train, config, DataTrans_config = DataTransform(df_train, config)
                save_config(output_path["DataTrans_config_path"], DataTrans_config, mylog)
                continue
            except Exception as e:
                mylog.error("Data PreProcess (Train/Merge)\tData Transform ERROR")
                mylog.error_trace(e)
                return "ERROR", "Please Call APC for Troubleshooting."

    try:
        tmp = config["IDX"].copy()
        tmp.extend(config["Y"])

        # y_train
        df_train[tmp].to_csv(output_path["y_train_path"], index=False)

        # y_train_T
        df_train = TrainDataTransformY(df_train, Y_config, mylog)
        df_train[tmp].to_csv(output_path["y_train_T_path"], index=False)

        # x_train
        df_train.drop(config["Y"], axis=1).to_csv(output_path["x_train_path"], index=False)

        # save config
        save_config(input_path["config_path"], config, mylog)
    except Exception as e:
        mylog.error("Data PreProcess (Train/Merge)\tOutput Result ERROR")
        mylog.error_trace(e)
        return "ERROR", "Please Call APC for Troubleshooting."
    
    mylog.info("Data PreProcess (Train/Merge)\tFinished")

    return "OK", "Data Pre-Process Completed"


def Data_PreProcess_Test(folder_path):

    input_path, output_path, mylog, config = Preprocess_Prepare(folder_path, Mode="Test")

    mylog.info("Data PreProcess (Test)\tStart")

    try:
        df_test = read_data(input_path["data_test"], mylog)
    except Exception as e:
        mylog.error("Data PreProcess (Test)\tRead Data ERROR")
        mylog.error_trace(e)
        return "ERROR", "Please Call APC for Troubleshooting."

    # Remove the Cols which are deleted by user in Data Preview
    # 20190829 Remove this line because this line has been executed in training process
    # df_test = RemoveColsInReview(df_test, config['DataPreview']['RemoveInReview'], mylog)

    try:
        df_test = TESTCheckCatCol(df_test, config, input_path['Cat_config_path'], mylog)
    except Exception as e:
        mylog.error("Data PreProcess (Test)\tChech Category Col ERROR")
        mylog.error_trace(e)
        return "ERROR", "Please Call APC for Troubleshooting."

    # TEST STEP
    mylog.info("**TESTING PROCESS START**")
    for step_index in config['Data_Preprocess_Steps']:
        if step_index == "DelNonNumCol":
            try:
                mylog.info("Data PreProcess (Test)\tRemove Nonnumerical Columns")
                DelNonNumCol_config = read_config(input_path["DelNonNumCol_config_path"], mylog)
                df_test = TESTremoveObjectCol(df_test, config, DelNonNumCol_config, mylog)
                continue
            except Exception as e:
                mylog.error("Data PreProcess (Test)\tRemove Nonnumerical Columns ERROR")
                mylog.error_trace(e)
                return "ERROR", "Please Call APC for Troubleshooting."

        elif step_index == "DelMissingCol":
            try:
                mylog.info("Data PreProcess (Test)\tRemove Missing Value")
                DelMissingCol_config = read_config(input_path["DelMissingCol_config_path"], mylog)
                df_test = TESTremoveMissingCol(df_test, DelMissingCol_config, mylog)
            except Exception as e:
                mylog.error("Data PreProcess (Test)\tRemove Missing Value ERROR")
                mylog.error_trace(e)
                return "ERROR", "Please Call APC for Troubleshooting."
        # elif step_index == "MergeCatCol":
        #     mylog.info("Merge_Category_Columns")
        #     df_test = TESTmergeCategoryCol(df_test, config)
        elif step_index == "DataImpute":
            try:
                mylog.info("Data PreProcess (Test)\tData Imputation")
                DataImpute_config = read_config(input_path["DataImpute_config_path"], mylog)
                df_test = TESTDataImputation(df_test, config, DataImpute_config)
            except Exception as e:
                mylog.error("Data PreProcess (Test)\tData Imputation ERROR")
                mylog.error_trace(e)
                return "ERROR", "Please Call APC for Troubleshooting."

        elif step_index == "DataTrans":
            try:
                mylog.info("Data PreProcess (Test)\tData Transform")
                DataTrans_config = read_config(input_path["DataTrans_config_path"], mylog)
                df_test.to_csv(output_path["x_test_no_z_path"], index=False)
                df_test = TESTDataTransform(df_test, config, DataTrans_config)

            except Exception as e:
                mylog.error("Data PreProcess (Test)\tData Transform ERROR")
                mylog.error_trace(e)
                return "ERROR", "Please Call APC for Troubleshooting."
    try:
        tmp = config["IDX"].copy()
        tmp.extend(config["Y"])
        df_test[tmp].to_csv(output_path["y_test_path"], index=False)
        df_test.drop(config["Y"], axis=1).to_csv(output_path["x_test_path"], index=False)
    except Exception as e:
        mylog.error("Data PreProcess (Test)\tOutput Result ERROR")
        mylog.error_trace(e)
        return "ERROR", "Please Call APC for Troubleshooting."

    mylog.info("Data PreProcess (Test)\tFinished")

    return "OK", "Data Pre-Process Completed"


def OnLine_Data_PreProcess(df_online, folder_path, check_include_y):

    input_path, output_path, mylog, config = Preprocess_Prepare(folder_path, Mode="Test")
    mylog.info("Data PreProcess (Online)\tStart")

    df_online = RemoveColsInReview(df_online, config['DataPreview']['RemoveInReview'], mylog)

    df_online = OnlineDataFloatTypeCheck(df_online,
                                         config,
                                         input_path['Cat_config_path'],
                                         input_path["DelNonNumCol_config_path"],
                                         input_path["DelMissingCol_config_path"],
                                         mylog)

    df_online = TESTCheckCatCol(df_online, config, input_path['Cat_config_path'], mylog)

    missing_result_list = OnlineDataMissingDetection(df_online, config, mylog)

    for step_index in config['Data_Preprocess_Steps']:
        if step_index == "DelNonNumCol":
            # mylog.info("Remove_Nonnumerical_Columns")
            DelNonNumCol_config = read_config(input_path["DelNonNumCol_config_path"], mylog)
            df_online = TESTremoveObjectCol(df_online, config, DelNonNumCol_config, mylog)

        elif step_index == "DelMissingCol":
            # mylog.info("Remove_Missing_Value")
            DelMissingCol_config = read_config(input_path["DelMissingCol_config_path"], mylog)
            df_online = TESTremoveMissingCol(df_online, DelMissingCol_config, mylog)

        elif step_index == "DataImpute":
            # mylog.info("Data Imputation")
            DataImpute_config = read_config(input_path["DataImpute_config_path"], mylog)
            df_online = TESTDataImputation(df_online, config, DataImpute_config)

        elif step_index == "DataTrans":
            # mylog.info("Data Transform")
            DataTrans_config = read_config(input_path["DataTrans_config_path"], mylog)
            # df_online = OnlineDataTransform(df_online, config, DataTrans_config)
            df_online = TESTDataTransform(df_online, config, DataTrans_config)

    if check_include_y is True:
        tmp = config["IDX"].copy()
        tmp.extend(config["Y"])
        df_online_y = df_online[tmp]
        df_online_X = df_online.drop(config["Y"], axis=1)
        mylog.info("Data PreProcess (Online)\tFinished")
        return df_online_X, df_online_y

    elif check_include_y is False:
        mylog.info("Data PreProcess (Online)\tFinished")
        return df_online, missing_result_list


if __name__ == '__main__':

    path = "../../Cases/T75R4_20190523/T75R4_20190523/T75R4_20190523_00/00/03_PreprocessedData/00_Model_building"
    input_paths = read_path(path)
    df = pd.read_csv(input_paths["raw_path"])

    with open(input_paths["config_path"]) as json_data:
        import json
        config = json.load(json_data)

    Y_columns = config["Y"]

    path = "../../Cases/T75R4_20190523/T75R4_20190523/T75R4_20190523_00/00/03_PreprocessedData/01_Test"
    for idx in range(df.shape[0]):

        # With Y
        if idx in [0]:
            df_tmp = pd.DataFrame(data=df.iloc[idx].values.reshape(1, -1), columns=df.columns)

            print(df_tmp)
            print("input type:", type(df_tmp))
            print("input shape:", df_tmp.shape)
            X, y = OnLine_Data_PreProcess(df_tmp, path, check_include_y=True)

            print("Output type:", type(X), type(y))
            print("Output shape:", X.shape, y.shape)

        # Without Y
        elif idx in [1]:
            df_tmp = pd.DataFrame(data=df.iloc[idx].values.reshape(1, -1), columns=df.columns)
            df_tmp = df_tmp.drop(Y_columns, axis=1)

            print(df_tmp)
            print("input type:", type(df_tmp))
            print("input shape:", df_tmp.shape)

            X, missing_Result = OnLine_Data_PreProcess(df_tmp, path, check_include_y=False)

            print("Output type:", type(X))
            print("Output shape:", X.shape)
            print("Output missing status:", type(missing_Result), len(missing_Result))

        # multiple rows
        elif idx in [2]:
            df_tmp = df.iloc[[0, 2, 4, 7, 8]]

            print(df_tmp)
            print("input type:", type(df_tmp))
            print("input shape:", df_tmp.shape)
            X, y = OnLine_Data_PreProcess(df_tmp, path, check_include_y=True)

            print("Output X:", type(X), X.shape)
            print("Output y:", type(y), y.shape)

        else:
            break

        import time
        time.sleep(3)
        print("\n\n\n\n")

	
	
# XDI.py
import numpy as np
from sklearn.decomposition import PCA
from sklearn import preprocessing
import matplotlib.pyplot as plt
from sklearn.externals import joblib
import  os
from config import read_config, save_config
import pandas as pd
from Read_path import read_path
from CreateLog import WriteLog
plt.style.use('ggplot')
plt.ioff()


def XDI_prepare(folder_paths, Mode):
    ###

    input_paths = read_path(folder_paths)
    mylogs = WriteLog(input_paths["log_path"], input_paths["error_path"])
    configs = read_config(input_paths["config_path"], mylogs)

    if Mode in ["OnLine"]:
        return input_paths, mylogs, configs

    output_paths = {}
    if Mode in ["Train"]:
        output_paths["XDI_PCA_path"] = os.path.join(folder_paths, "XDI_PCA.pkl")
        output_paths["XDI_DataTrans_path"] = os.path.join(folder_paths, "XDI_DataTrans.pkl")
        output_paths["XDI_PreWork_DataTrans_path"] = os.path.join(folder_paths, "XDI_PreWork_DataTrans.pkl")

    if Mode in ["Test"]:
        output_paths["XDI_offline_path"] = os.path.join(folder_paths, "XDI_offline.csv")
        output_paths["XDI_alarm_report_path"] = os.path.join(folder_paths, "XDI_Alarm_Report/")
        output_paths["XDI_offline_pic_path"] = os.path.join(folder_paths, "XDI.png")
        path_ = output_paths["XDI_alarm_report_path"]
        if not os.path.exists(path_):
            os.makedirs(path_)

    return input_paths, output_paths, mylogs, configs


def read_data(data_path_, mylog_):
    try:
        data_ = pd.read_csv(data_path_)
    except Exception as e:
        mylog_.error("XDI: Read Raw Data ERROR.")
        mylog_.error_trace(e)
        raise FileNotFoundError
    return data_

def XDI_Pre_Process(data_paths, index_cols, mylogs):

    data = read_data(data_paths, mylogs)
    df_idx = data[index_cols]

    for col in index_cols:
        try:
            data = data.drop(col, axis=1)
        except Exception as e:
            msg = "XDI(Pre-process): There is no col named" + str(col) + "already."
            mylogs.warning(msg)
            mylogs.warning_trace(e)

    return data, df_idx


def XDI_PreWork_Train(data_path, configs, XDI_PreWork_DataTrans_paths, mylogs):

    data, df_idx = XDI_Pre_Process(data_path, configs["IDX"], mylogs)

    # check if Data Transform was executed in Data Pre-Process
    if "DataTrans" not in configs["Data_Preprocess_Steps"]:
        configs["XDI"]["Data_Transform_Check"] = 0
        scaler_raw_Z_score = preprocessing.StandardScaler().fit(data)
        col_ = data.columns
        data = scaler_raw_Z_score.transform(data)
        data = pd.DataFrame(data=data, columns=col_)
        joblib.dump(scaler_raw_Z_score, XDI_PreWork_DataTrans_paths)
        return data, configs

    elif "DataTrans" in configs["Data_Preprocess_Steps"]:
        configs["XDI"]["Data_Transform_Check"] = 1
        return data, configs


def XDI_PreWork_Test(data_paths, input_paths, index_cols, check_prework_DataTrans, mylogs):

    data, df_idx = XDI_Pre_Process(data_paths, index_cols, mylogs)

    if check_prework_DataTrans == 0:
        module_transform_ = joblib.load(input_paths["XDI_PreWork_DataTrans_path"])
        col_ = data.columns
        data = module_transform_.transform(data)
        data = pd.DataFrame(data=data, columns=col_)

    raw_data = data.copy()

    module_PCA_ = joblib.load(input_paths["XDI_PCA_path"])
    data = module_PCA_.transform(data)

    scaler_pca_Z_score_ = joblib.load(input_paths["XDI_DataTrans_path"])
    data = scaler_pca_Z_score_.transform(data)

    return raw_data, data, df_idx


def XDI_judger(XDI_value, threshold):
    if XDI_value <= threshold:
        return "OK"
    elif XDI_value > threshold:
        return "NG"


def XDI_threshold_calculator(df_, XDI_threshold, pca_threshold):
    XDI_loo = []
    for idx in range(df_.shape[0]):
        # loo PCA
        Model_pca_loo = PCA(n_components=pca_threshold).fit(df_.drop(idx, axis=0))
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
    XDI_threshold = np.mean(XDI_loo) + np.std(XDI_loo) * np.sqrt(1 / XDI_threshold)

    return XDI_threshold


def XDI_Alarm_report(data_trians, data_tests, XDI_tables, idx_cols, output_paths, mylogs):
    data_all = pd.concat([data_trians, data_tests], axis=0, ignore_index=True)
    NG_idx = XDI_tables.loc[XDI_tables["Result"] == "NG"].index
    for i in NG_idx:
        mark = XDI_tables["Type"].iloc[i]
        base_data = data_trians.copy()
        if mark == "Train":
            base_data = base_data.drop(i, axis=0)
        target_data = data_all.iloc[i]

        contribution_table = base_data.mean()-target_data
        contribution_table = contribution_table.abs().sort_values(ascending=False)

        msg = "\t".join([str(col) + ": " + str(XDI_tables[col].iloc[i]) for col in idx_cols])
        mylogs.warning("XDI Alarm\tIndex:\t" + msg)

        filename_change_part = "_".join([str(col) + "_" + str(XDI_tables[col].iloc[i]) for col in idx_cols])

        filename = "_".join(["Contribution", filename_change_part, ".csv"])

        output_path_ = os.path.join(output_paths["XDI_alarm_report_path"], filename)

        df_contri = pd.DataFrame({"Col_Name": contribution_table.index,
                                  "Contribution": contribution_table.values})

        # Index_Columns Output: One to one
        # for col in idx_cols:
        #     df_contri[col] = XDI_tables[col].iloc[i]

        # Index_Columns Output: All in one
        # idx_col_name = "_".join(idx_cols)
        idx_col_values_list = []

        for col in idx_cols:
            idx_col_values_list.append(str(XDI_tables[col].iloc[i]))

        # idx_col_values = "_".join(idx_col_values_list)
        df_contri["Index_Columns"] = "_".join(idx_col_values_list)

        df_contri.to_csv(output_path_, index=False)

        filename = "_".join(["Contribution_top_20", filename_change_part, ".png"])
        output_path_ = os.path.join(output_paths["XDI_alarm_report_path"], filename)

        if df_contri.shape[0] < 20:
            contri_amount = df_contri.shape[0]
        elif df_contri.shape[0] >= 20:
            contri_amount = 20

        fig, ax = plt.subplots(figsize=(10, 6))
        plt.barh(range(contri_amount, 0, -1), df_contri["Contribution"].iloc[:contri_amount], label="contribution")
        plt.yticks(range(contri_amount, 0, -1), df_contri["Col_Name"].iloc[:contri_amount])
        ax.set_title("Top 20 Contribution")
        plt.legend()
        plt.tight_layout()
        plt.savefig(output_path_)
        plt.clf()
        plt.cla()
        plt.close()
    return None


def XDI_off_line_report(data_folder, mode):
    input_path, output_path, mylog, config = XDI_prepare(data_folder, Mode="Test")

    mylog.info("XDI (Report)\tStart")

    try:
        if mode in ["Train", "Merge"]:
            df_raw_train, X_train, df_train_idx = XDI_PreWork_Test(input_path["x_train_path"],
                                                                   input_path,
                                                                   config["IDX"],
                                                                   config["XDI"]["Data_Transform_Check"],
                                                                   mylog)
        elif mode in ["Batch"]:
            df_raw_train, X_train, df_train_idx = XDI_PreWork_Test(input_path["x_train_path"],
                                                                   input_path,
                                                                   config["IDX"],
                                                                   config["XDI"]["Data_Transform_Check"],
                                                                   mylog)
        else:
            mylog.error("XDI (Report)\tKey: mode is Error: only Train, Merge, Batch")
            return "ERROR", "Please Call APC for Troubleshooting."

    except Exception as e:
        mylog.error("XDI (Report)\tData Prepare ERROR")
        mylog.error_trace(e)
        return "ERROR", "Please Call APC for Troubleshooting."

    try:
        if mode in ["Train", "Batch"]:
            df_raw_test, X_test, df_test_idx = XDI_PreWork_Test(input_path["x_test_path"],
                                                                input_path,
                                                                config["IDX"],
                                                                config["XDI"]["Data_Transform_Check"],
                                                                mylog)

            X_data = np.concatenate((X_train, X_test), axis=0)
            df_XDI = pd.concat([df_train_idx, df_test_idx], axis=0, ignore_index=True)
            X_XDI = [np.sqrt(sum(i)) for i in X_data * X_data]
            df_XDI['XDI'] = X_XDI
            df_XDI["Result"] = np.vectorize(XDI_judger)(df_XDI['XDI'], config["XDI"]["XDI_Threshold"])
            df_XDI["Type"] = ["Train"] * X_train.shape[0] + ["Test"] * X_test.shape[0]

        elif mode in ["Merge"]:
            df_raw_test = pd.DataFrame(columns=df_raw_train.columns)

            X_data = X_train
            df_XDI = df_train_idx
            X_XDI = [np.sqrt(sum(i)) for i in X_data * X_data]
            df_XDI['XDI'] = X_XDI
            df_XDI["Result"] = np.vectorize(XDI_judger)(df_XDI['XDI'], config["XDI"]["XDI_Threshold"])
            df_XDI["Type"] = ["Train"] * X_train.shape[0]

        else:
            mylog.error("XDI (Report)\tKey: mode is Error: only Train, Merge, Batch")
            return "ERROR", "Please Call APC for Troubleshooting."

    except Exception as e:
        mylog.error("XDI (Report)\tXDI Value Calculation ERROR")
        mylog.error_trace(e)
        return "ERROR", "Please Call APC for Troubleshooting."

    try:
        XDI_Alarm = "OK"
        if "NG" in df_XDI["Result"].values:
            XDI_Alarm_report(df_raw_train, df_raw_test, df_XDI, config["Index_Columns"], output_path, mylog)
            XDI_Alarm = "NG"

        df_XDI["Index_Columns"] = df_XDI[config["Index_Columns"]].apply(lambda x: '_'.join(str(e) for e in x), axis=1)
        if config["Filter_Feature"] is not None:
            df_XDI["Filter_Feature"] = df_XDI[config["Filter_Feature"]]
        else:
            df_XDI["Filter_Feature"] = df_XDI[config["Index_Columns"]].apply(lambda x: '_'.join(str(e) for e in x), axis=1)

        df_XDI.to_csv(output_path["XDI_offline_path"], index=False)
    except Exception as e:
        mylog.error("XDI (Report)\tXDI Report ERROR")
        mylog.error_trace(e)
        return "ERROR", "Please Call APC for Troubleshooting."

    try:
        fig, ax = plt.subplots(figsize=(10, 6))
        plt.plot(X_XDI, label='XDI',  c='b')
        if df_XDI["Result"].isin(["NG"]).any():
            idx = df_XDI["Result"].loc[df_XDI["Result"] == "NG"].index
            plt.scatter(idx, df_XDI["XDI"].iloc[idx], c="r", label="NG")
        ax.axhline(y=config["XDI"]["XDI_Threshold"], c='r', label='XDI threshold')
        ax.axvline(x=X_train.shape[0] - 0.5, c='g', label='Train/Test Split Line')
        ax.set_title("XDI")
        plt.legend()
        plt.savefig(output_path["XDI_offline_pic_path"])
        plt.clf()

    except Exception as e:
        mylog.error("XDI (Report)\tXDI Chart ERROR")
        mylog.error_trace(e)
        return "ERROR", "Please Call APC for Troubleshooting."

    # df_XDI["Index_Columns"] = df_XDI[config["Index_Columns"]].apply(lambda x: '_'.join(str(e) for e in x), axis=1)
    # if config["Filter_Feature"] is not None:
    #     df_XDI["Filter_Feature"] = df_XDI[config["Filter_Feature"]]

    # df_XDI.to_csv(output_path["XDI_offline_path"], index=False)

    mylog.info("XDI (Report)\tOffline Report is stored at: " + output_path["XDI_offline_path"])
    mylog.info("XDI (Report)\tFinished")

    if XDI_Alarm in ["OK"]:
        return "OK", "XDI (Report) Completed"
    elif XDI_Alarm in ["NG"]:
        return "NG", "XDI Alarm. Please Check. Thank You."


def Build_XDI_Model(data_folder):

    input_path, output_path, mylog, config = XDI_prepare(data_folder, Mode="Train")
    mylog.info("XDI (Build)\tStart")

    try:
        X_train, config = XDI_PreWork_Train(input_path["x_train_path"],
                                            config,
                                            output_path["XDI_PreWork_DataTrans_path"],
                                            mylog)
    except Exception as e:
        mylog.error("XDI (Build)\tRead Data ERROR")
        mylog.error_trace(e)
        return "ERROR", "Please Call APC for Troubleshooting."


    setting_XDI_threshold = config["XDI"]["XDI_threshold_ratio"]
    setting_pca_threshold = config["XDI"]["filter_pca_threshold"]
    mylog.info("XDI (Build)\tThreshold Calculate Start")

    try:
        config["XDI"]["XDI_Threshold"] = XDI_threshold_calculator(X_train, setting_XDI_threshold, setting_pca_threshold)
    except Exception as e:
        mylog.error("XDI (Build)\tCalculate Threshold ERROR")
        mylog.error_trace(e)
        return "ERROR", "Please Call APC for Troubleshooting."

    mylog.info("XDI (Build)\tThreshold: " + str(config["XDI"]["XDI_Threshold"]))
    mylog.info("XDI (Build)\tThreshold Calculate Finished")
    # mylog.info("----------LOO End--------------")

    # PCA Module
    try:
        module_PCA = PCA(n_components=setting_pca_threshold).fit(X_train)
        X_train_pca = module_PCA.transform(X_train)
        joblib.dump(module_PCA, output_path["XDI_PCA_path"])
    except Exception as e:
        mylog.error("XDI (Build)\tBuild PCA Model ERROR")
        mylog.error_trace(e)
        return "ERROR", "Please Call APC for Troubleshooting."

    # PCA again after PCA
    try:
        scaler_pca_Z_score = preprocessing.StandardScaler().fit(X_train_pca)
        joblib.dump(scaler_pca_Z_score, output_path["XDI_DataTrans_path"])
    except Exception as e:
        mylog.error("XDI (Build)\tBuild Data transform Model ERROR")
        mylog.error_trace(e)
        return "ERROR", "Please Call APC for Troubleshooting."

    save_config(input_path["config_path"], config, mylog)
    mylog.info("XDI (Build)\tFinished")
        
    return "OK", "XDI (Build) Completed"


def OnLine_XDI(df_online, data_folder):

    input_path, mylog, config = XDI_prepare(data_folder, Mode="OnLine")

    for col in config["IDX"]:
        try:
            df_online = df_online.drop(col, axis=1)
        except Exception as e:
            msg = "XDI(Pre-process): There is no col named" + str(col) + "already."
            mylog.warning(msg)
            mylog.warning_trace(e)

    if config["XDI"]["Data_Transform_Check"] == 0:
        module_transform_ = joblib.load(input_path["XDI_PreWork_DataTrans_path"])
        df_online = module_transform_.transform(df_online)

    X_online = df_online.copy()

    module_PCA_ = joblib.load(input_path["XDI_PCA_path"])
    X_online = module_PCA_.transform(X_online)

    scaler_pca_Z_score_ = joblib.load(input_path["XDI_DataTrans_path"])
    X_online = scaler_pca_Z_score_.transform(X_online)[0]

    XDI_online_value = np.sqrt(sum(X_online * X_online))

    mylog.info("XDI\tXDI_Value is " + str(XDI_online_value))

    contribution_table = df_online.iloc[0].abs().sort_values(ascending=False)

    df_contri = pd.DataFrame({"Col_Name": contribution_table.index,
                              "Contribution": contribution_table.values})

    XDI_Threshold = config["XDI"]["XDI_Threshold"]
    XDI_check = XDI_Threshold > XDI_online_value


    if XDI_check:
        # OK
        return XDI_online_value, XDI_Threshold, 1, df_contri[:20]
    else:
        # NG
        return XDI_online_value, XDI_Threshold, 0, df_contri[:20]


if __name__ == "__main__":
    path = "/home/petertylin/PycharmProjects/00_Projects/00_AVM/AVM_System/Cases/T75R4_20190523/T75R4_20190523/T75R4_20190523_03/00/04_XDI/03_Test"
    input_paths = read_path(path)

    df = pd.read_csv(input_paths["x_train_path"])
    for idx in range(df.shape[0]):
        # print(type(df.iloc[idx]))
        df_tmp = pd.DataFrame(data=df.iloc[idx].values.reshape(1,-1), columns=df.columns)
        XDI, Threshold, XDI_check, df_con = OnLine_XDI(df_tmp, path)
        # print(type(X), type(y))

        print("input type:", type(df_tmp))
        print("input shape:", df_tmp.shape)

        print("Output type:", type(XDI), type(Threshold), type(XDI_check), type(df_con))
        print("Output shape:", XDI.shape, df_con.shape)
        print("Output XDI Value", XDI)
        print("Output XDI threshold", Threshold)
        print("Output XDI check", XDI_check)
        print(df_con)
        break
	
	
	
# YDI.py
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.externals import joblib
from sklearn.decomposition import PCA
from sklearn import preprocessing
import os
import matplotlib.cm as cm
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_samples, silhouette_score
from config import read_config, save_config
from Read_path import read_path
from CreateLog import WriteLog
plt.style.use('ggplot')
plt.ioff()

#setting_filter_pca_threshold = 0.8


# this YDI module is for single Y, the multiple Y

def YDI_prepare(folder_paths, Mode):

    input_paths = read_path(folder_paths)
    mylogs = WriteLog(input_paths["log_path"], input_paths["error_path"])
    configs = read_config(input_paths["config_path"], mylogs)

    if Mode in ["OnLine"]:
        return input_paths, mylogs, configs

    ###
    output_paths = {}
    if Mode in ["Train"]:
        output_paths["YDI_Group_path"] = os.path.join(folder_paths, "YDI_Group/")
        output_paths["YDI_threshold_table_path"] = os.path.join(folder_paths, "YDI_threshold_table.csv")
        output_paths["YDI_PreWork_DataTrans"] = os.path.join(folder_paths, "YDI_PreWork_DataTrans.pkl")
        path_ = output_paths["YDI_Group_path"]
        if not os.path.exists(path_):
            os.makedirs(path_)

    if Mode in ["Test"]:
        output_paths["YDI_Offline_report_path"] = os.path.join(folder_paths, "YDI_Offline_Report.csv")
        output_paths["YDI_Group_path"] = os.path.join(folder_paths, "YDI_Group/")
        path_ = output_paths["YDI_Group_path"]
        if not os.path.exists(path_):
            os.makedirs(path_)

    return input_paths, output_paths, mylogs, configs


def read_data(data_path_, mylog_):
    try:
        data_ = pd.read_csv(data_path_)
    except Exception as e:
        mylog_.error("YDI: Read Raw Data ERROR.")
        mylog_.error_trace(e)

    return data_


def YDI_Pre_Process(data_paths, index_cols, mylogs):

    data = read_data(data_paths, mylogs)
    df_idx = data[index_cols].copy()

    for col in index_cols:
        try:
            data = data.drop(col, axis=1)
        except Exception as e:
            msg = "YDI(Pre-process): There is no col named" + str(col) + "already."
            mylogs.warning(msg)
            mylogs.warning_trace(e)

    return data, df_idx


def YDI_PreWork_Train(data_path, configs, YDI_PreWork_DataTrans_paths, mylogs):

    # check if Data Transform was executed in Data Pre-Process
    if "DataTrans" not in configs["Data_Preprocess_Steps"]:
        data, df_idx = YDI_Pre_Process(data_path, configs["IDX"], mylogs)
        configs["YDI"]["Data_Transform_Check"] = 0
        scaler_raw_Z_score = preprocessing.StandardScaler().fit(data)
        col_ = data.columns
        data = scaler_raw_Z_score.transform(data)
        data = pd.DataFrame(data=data, columns=col_)
        data = pd.concat([data, df_idx], axis=1)
        joblib.dump(scaler_raw_Z_score, YDI_PreWork_DataTrans_paths)

    elif "DataTrans" in configs["Data_Preprocess_Steps"]:
        data = read_data(data_path, mylogs)
        configs["YDI"]["Data_Transform_Check"] = 1

    return data, configs


def YDI_PreWork_Test(data_paths, index_cols, check_prework_DataTrans, YDI_PreWork_DataTrans_paths, mylogs):

    if check_prework_DataTrans == 0:
        data, df_idx = YDI_Pre_Process(data_paths, index_cols, mylogs)
        col_ = data.columns
        module_transform_ = joblib.load(YDI_PreWork_DataTrans_paths)
        data = module_transform_.transform(data)
        data = pd.DataFrame(data=data, columns=col_)
        data = pd.concat([data, df_idx], axis=1)

    elif check_prework_DataTrans == 1:
        data = read_data(data_paths, mylogs)
    # print(data)
    return data


def Clustering_Testing(X_train, cluster_path):
    max_clusters_amount = min(10, X_train.shape[0]-1)
    # print(max_clusters_amount)

    ideal_clusters_amount = 1
    max_silhouette_score = -1

    for n_clusters in range(2, max_clusters_amount + 1):
        clusterer = KMeans(n_clusters=n_clusters, random_state=0).fit(X_train)
        cluster_labels = clusterer.fit_predict(X_train)
        # print(cluster_labels)
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

        # print(X_train)
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
        YDI_tmp_path = os.path.join(cluster_path, "cluster_" + str(n_clusters) + ".png")
        plt.savefig(YDI_tmp_path)
        plt.clf()

    return ideal_clusters_amount


def YDI_judger(YDI_value, threshold):
    # print(YDI_value, type(YDI_value))
    if np.isnan(YDI_value):
        return "NY"
    if YDI_value > threshold:
        return "NG"
    elif YDI_value <= threshold:
        return "OK"



def YDI_threshold_generator(X_trains, y_trains, Y_cols, pca_threshold, groups, group_path):

    if groups is None:
        group_folder_path_ = os.path.join(group_path, "NO_GROUP")
    elif groups is not None:
        group_folder_path_ = os.path.join(group_path, str(groups))

    if not os.path.exists(group_folder_path_):
        os.makedirs(group_folder_path_)

    cluster_pic_path = os.path.join(group_folder_path_, "YDI_Clustering/")
    if not os.path.exists(cluster_pic_path):
        os.makedirs(cluster_pic_path)

    pca_model_path = os.path.join(group_folder_path_, "YDI_PCA.pkl")
    module_PCA = PCA(n_components=pca_threshold).fit(X_trains)
    X_trains = module_PCA.transform(X_trains)
    joblib.dump(module_PCA, pca_model_path)


    ideal_clusters_amount = Clustering_Testing(X_trains, cluster_pic_path)
    module_Clustering = KMeans(n_clusters=ideal_clusters_amount, random_state=0).fit(X_trains)

    cluster_model_path = os.path.join(group_folder_path_, "YDI_Clustering.pkl")
    joblib.dump(module_Clustering, cluster_model_path)

    # 計算Kmeans分群後各個群 y的實際平均值
    list_cluster_y_avg = []
    list_cluster_y_range = []

    # df_YDI = pd.DataFrame({"Y": [],
    #                        "Cluster_idx": [],
    #                        "Y_avg": [],
    #                        "YDI_thrshold": []})
    df_YDI = pd.DataFrame()
    for col in Y_cols:
        y_values = y_trains[col]
        for cluster_idx in range(ideal_clusters_amount):
            list_cluster_index = np.where(np.array(module_Clustering.predict(X_trains) == cluster_idx))
            list_cluster_values = np.array(y_trains.iloc[list_cluster_index])
            list_cluster_y_avg.append(np.mean(list_cluster_values))
            list_cluster_y_range.append(np.max(list_cluster_values) - np.min(list_cluster_values))

        cluster_Max_Range = np.max(list_cluster_y_range)
        y_avg = np.array(list_cluster_y_avg)

        '''
        y avg revised goal:
        
        -1: * -1
        0 : + 1
        +1: do nothing
        
        
        '''
        y_avg_re = (y_avg + ((y_avg == 0) + 0)) * (((y_avg < 0) * -1) + (y_avg >= 0) * 1)

        y_YDI = pd.DataFrame({"Y": [col] * ideal_clusters_amount,
                              "Cluster_idx": list(range(ideal_clusters_amount)),
                              "Y_avg": list_cluster_y_avg,
                              "YDI_Thrshold": cluster_Max_Range / y_avg_re,
                              "Interval": cluster_Max_Range})

        df_YDI = pd.concat([df_YDI, y_YDI], axis=0)

    threshold_table_path = os.path.join(group_folder_path_, "YDI_threshold_table.csv")
    df_YDI.to_csv(threshold_table_path, index=False)
    return df_YDI


def YDI_report_generator(X_trains, X_tests, y_trains, y_tests, train_idxs, test_idxs,
                         Y_cols, group_col_name, groups, input_group_path, output_group_path, Mode):

    if groups is None:
        group_folder_path_ = os.path.join(input_group_path, "NO_GROUP")
    elif groups is not None:
        group_folder_path_ = os.path.join(input_group_path, str(groups))

    # YDI_result_path = os.path.join(output_group_path, "YDI_Group/")
    # if not os.path.exists(YDI_result_path):
    #     os.makedirs(YDI_result_path)

    pca_model_path = os.path.join(group_folder_path_, "YDI_PCA.pkl")
    module_PCA_ = joblib.load(pca_model_path)
    X_train = module_PCA_.transform(X_trains)

    if Mode in ["Train", "Batch"]:
        if X_tests.shape[0] == 0:
            X_test = np.array([])
            X_data = X_train
        elif X_tests.shape[0] != 0:
            X_test = module_PCA_.transform(X_tests)
            X_data = np.concatenate((X_train, X_test), axis=0)
    elif Mode in ["Merge"]:
        X_test = np.array([])
        X_data = X_train


    cluster_model_path = os.path.join(group_folder_path_, "YDI_Clustering.pkl")
    module_Clustering_ = joblib.load(cluster_model_path)


    df_y = pd.concat([y_trains, y_tests], axis=0)
    # y_data = np.concatenate((y_train, y_test), axis=0)
    cluster_idx = module_Clustering_.predict(X_data)

    threshold_table_path = os.path.join(group_folder_path_, "YDI_threshold_table.csv")
    df_YDI_table = pd.read_csv(threshold_table_path)
    df_YDI = pd.concat([train_idxs, test_idxs], axis=0, ignore_index=True)


    YDI_offline_group_table = pd.DataFrame()
    for col in Y_cols:

        df_YDI_table_y = df_YDI_table.loc[(df_YDI_table["Y"] == col)].set_index(['Cluster_idx'])

        y_avg = np.array(df_YDI_table_y['Y_avg'].iloc[cluster_idx])
        YDI_threshold = np.array(df_YDI_table_y['YDI_Thrshold'].iloc[cluster_idx])
        y_values = np.array(df_y[col])
        y_avg_re = (y_avg + ((y_avg == 0) + 0)) * (((y_avg < 0) * -1) + (y_avg >= 0) * 1)
        YDI_values = np.abs(y_values - y_avg) / y_avg_re

        df_YDI["YDI"] = np.array(YDI_values)
        df_YDI["YDI_Threshold"] = YDI_threshold
        df_YDI["Cluster_idx"] = cluster_idx
        df_YDI["Y_avg"] = y_avg
        df_YDI["Y_values"] = y_values
        df_YDI["Y"] = col
        df_YDI["Data"] = ["Train"] * X_train.shape[0] + ["Test"] * X_test.shape[0]
        df_YDI["Interval"] = np.array(df_YDI_table_y['Interval'].iloc[cluster_idx])

        # print(df_YDI['YDI'])
        df_YDI["Result"] = np.vectorize(YDI_judger)(df_YDI['YDI'], df_YDI["YDI_Threshold"])

        fig, ax = plt.subplots(figsize=(10, 6))
        # plt.plot(YDI_values, label='YDI', c='b')
        plt.scatter(range(YDI_values.shape[0]), YDI_values, c="b", label="YDI")
        plt.plot(YDI_threshold, label='YDI threshold', c='r')
        if df_YDI["Result"].isin(["NG"]).any():
            idx = df_YDI["Result"].loc[df_YDI["Result"] == "NG"].index
            plt.scatter(idx, df_YDI["YDI"].iloc[idx], c="r", label="NG")
        ax.axvline(x=X_train.shape[0], c='g', label='Train/Test Split Line')
        ax.set_title("YDI  (" + str(col) + ")")
        plt.legend()
        if group_col_name is not None:
            filename = "_".join(["YDI_offline", str(col), str(group_col_name), str(groups)])
        elif group_col_name is None:
            filename = "_".join(["YDI_offline", str(col)])
        pic_path = output_group_path + filename + ".png"
        plt.savefig(pic_path)
        plt.clf()

        YDI_path = output_group_path + filename + ".csv"
        df_YDI.to_csv(YDI_path, index=False)
        YDI_offline_group_table = pd.concat([YDI_offline_group_table, df_YDI], axis=0)

    return YDI_offline_group_table


def YDI_Data_Prepare(X_paths, y_paths, YDI_DataTrans_paths, configs, mylogs):
    dfs = YDI_PreWork_Test(X_paths,
                           configs["IDX"],
                           configs["YDI"]["Data_Transform_Check"],
                           YDI_DataTrans_paths,
                           mylogs)

    dfs_idx = dfs[configs["IDX"]]
    dfs_y = read_data(y_paths, mylogs)
    dfs_y = dfs_y[configs["Y"]]

    return dfs, dfs_idx, dfs_y


def YDI_off_line_report(data_folder, mode):

    input_path, output_path, mylog, config = YDI_prepare(data_folder, Mode="Test")

    mylog.info("YDI (Report)\tStart")
    try:
        if mode in ["Train", "Merge"]:
            X_train, df_train_idx, y_train = YDI_Data_Prepare(input_path["x_train_path"],
                                                              input_path["y_train_path"],
                                                              input_path["YDI_PreWork_DataTrans"],
                                                              config,
                                                              mylog)
        elif mode in ["Batch"]:
            X_train, df_train_idx, y_train = YDI_Data_Prepare(input_path["x_train_path"],
                                                              input_path["y_train_path"],
                                                              input_path["YDI_PreWork_DataTrans"],
                                                              config,
                                                              mylog)
        else:
            mylog.error("YDI (Report)\tKey: mode is Error: only Train, Merge, Batch")
            return "ERROR", "Please Call APC for Troubleshooting."

        if mode in ["Train", "Batch"]:
            X_test, df_test_idx, y_test = YDI_Data_Prepare(input_path["x_test_path"],
                                                           input_path["y_test_path"],
                                                           input_path["YDI_PreWork_DataTrans"],
                                                           config,
                                                           mylog)
        elif mode in ["Merge"]:
            X_test = pd.DataFrame(columns=X_train.columns)
            df_test_idx = pd.DataFrame(columns=df_train_idx.columns)
            y_test = pd.DataFrame(columns=y_train.columns)

    except Exception as e:
        mylog.error("YDI (Report)\tData Prepare ERROR")
        mylog.error_trace(e)
        return "ERROR", "Please Call APC for Troubleshooting."


    if config["Filter_Feature"] is not None:
        group_col = config["Filter_Feature"]
        YDI_Off_line_table = pd.DataFrame()
        for groups in np.unique(X_train[group_col]):
            try:
                group_idx_train = X_train.loc[X_train[group_col] == groups].index
                group_idx_test = X_test.loc[X_test[group_col] == groups].index

                X_train_data = X_train.iloc[group_idx_train].reset_index(drop=True)
                X_train_data = X_train_data.drop(config["IDX"], axis=1)
                y_train_data = y_train.iloc[group_idx_train].reset_index(drop=True)

                X_test_data = X_test.iloc[group_idx_test].reset_index(drop=True)
                X_test_data = X_test_data.drop(config["IDX"], axis=1)
                y_test_data = y_test.iloc[group_idx_test].reset_index(drop=True)

                train_idx_data = df_train_idx.iloc[group_idx_train].reset_index(drop=True)
                test_idx_data = df_test_idx.iloc[group_idx_test].reset_index(drop=True)

                # print(groups)
                # print(X_train_data.shape, y_train_data.shape, X_test_data.shape, y_test_data.shape)

                YDI_Off_line_group_table = YDI_report_generator(X_train_data,
                                                                X_test_data,
                                                                y_train_data,
                                                                y_test_data,
                                                                train_idx_data,
                                                                test_idx_data,
                                                                config["Y"],
                                                                group_col,
                                                                groups,
                                                                input_path["YDI_Group_path"],
                                                                output_path["YDI_Group_path"],
                                                                Mode=mode)


                YDI_Off_line_group_table["Filter_Feature"] = groups

                YDI_Off_line_table = pd.concat([YDI_Off_line_table, YDI_Off_line_group_table], axis=0)

            except Exception as e:
                mylog.error("YDI (Report)\tYDI Calculation (Group) ERROR")
                error_msg = "YDI (Report)\tGroup :" + str(groups)
                mylog.error(error_msg)
                mylog.error_trace(e)
                return "ERROR", "Please Call APC for Troubleshooting."


    elif config["Filter_Feature"] is None:
        try:
#            X_train = X_train.drop(config["Index_Columns"], axis=1)
#            X_test = X_test.drop(config["Index_Columns"], axis=1)
            X_train = X_train.drop(config["IDX"], axis=1)
            X_test = X_test.drop(config["IDX"], axis=1)
            YDI_Off_line_table = YDI_report_generator(X_train,
                                                      X_test,
                                                      y_train,
                                                      y_test,
                                                      df_train_idx,
                                                      df_test_idx,
                                                      config["Y"],
                                                      None,
                                                      None,
                                                      input_path["YDI_Group_path"],
                                                      output_path["YDI_Group_path"],
                                                      Mode=mode)

            YDI_Off_line_table["Filter_Feature"] = \
                YDI_Off_line_table[config["Index_Columns"]].apply(lambda x: '_'.join(str(e) for e in x), axis=1)

        except Exception as e:
            mylog.error("YDI (Report)\tYDI Calculation (NO Group) ERROR")
            mylog.error_trace(e)
            return "ERROR", "Please Call APC for Troubleshooting."

    try:
        YDI_Off_line_table["Index_Columns"] = \
            YDI_Off_line_table[config["Index_Columns"]].apply(lambda x: '_'.join(str(e) for e in x), axis=1)
        YDI_Off_line_table.to_csv(output_path["YDI_Offline_report_path"],index=False)

    except Exception as e:
        mylog.error("YDI (Report)\tYDI Report Output ERROR")
        mylog.error_trace(e)
        return "ERROR", "Please Call APC for Troubleshooting."

    YDI_Alarm = "OK"
    if "NG" in YDI_Off_line_table["Result"].values:
        YDI_Alarm = "NG"

    mylog.info("YDI\tOffline Report Finished")

    if YDI_Alarm in ["OK"]:
        return "OK", "YDI (Report) Completed"
    elif YDI_Alarm in ["NG"]:
        return "NG", "YDI Alarm. Please Check. Thank You."


def Build_YDI_Model(data_folder):

    input_path, output_path, mylog, config = YDI_prepare(data_folder, Mode="Train")

    mylog.info("YDI\tModel Building Start")

    try:
        X_train, config = YDI_PreWork_Train(input_path["x_train_path"],
                                            config,
                                            output_path["YDI_PreWork_DataTrans"],
                                            mylog)

        y_train = read_data(input_path["y_train_path"], mylog)
        y_train = y_train[config["Y"]]

    except Exception as e:
        mylog.error("YDI (Build)\tYDI Data Prepare ERROR")
        mylog.error_trace(e)
        return "ERROR", "Please Call APC for Troubleshooting."

    if config["Filter_Feature"] is not None:
        group_col = config["Filter_Feature"]
        YDI_threshold_table = pd.DataFrame()
        for groups in np.unique(X_train[group_col]):
            try:
                group_idx_list = X_train.loc[X_train[group_col] == groups].index

                X_data = X_train.iloc[group_idx_list].reset_index(drop=True)
                X_data = X_data.drop(config["IDX"], axis=1)
                y_data = y_train.iloc[group_idx_list].reset_index(drop=True)
                # print(groups)
                # print(X_data.shape, y_data.shape)
                YDI_threshold_table_G = YDI_threshold_generator(X_data,
                                                                y_data,
                                                                config["Y"],
                                                                config["YDI"]["filter_pca_threshold"],
                                                                groups,
                                                                output_path["YDI_Group_path"])

                YDI_threshold_table_G[config["Filter_Feature"]] = groups
                YDI_threshold_table = pd.concat([YDI_threshold_table, YDI_threshold_table_G], axis=0)

            except Exception as e:
                mylog.error("YDI (Build)\tYDI Model (Group) ERROR")
                error_msg = "YDI (Build)\tGroup :" + str(groups)
                mylog.error(error_msg)
                mylog.error_trace(e)
                return "ERROR", "Please Call APC for Troubleshooting."

    else:
        try:
            X_train = X_train.drop(config["IDX"], axis=1)
            YDI_threshold_table = YDI_threshold_generator(X_train,
                                                          y_train,
                                                          config["Y"],
                                                          config["YDI"]["filter_pca_threshold"],
                                                          None,
                                                          output_path["YDI_Group_path"])

        except Exception as e:
            mylog.error("YDI (Build)\tYDI Model (NO Group) ERROR")
            mylog.error_trace(e)
            return "ERROR", "Please Call APC for Troubleshooting."

    try:
        YDI_threshold_table.to_csv(output_path["YDI_threshold_table_path"], index=False)
        mylog.info("YDI\tModel Building Finished")
        save_config(input_path["config_path"], config, mylog)

    except Exception as e:
        mylog.error("YDI (Build)\tYDI Table ERROR")
        mylog.error_trace(e)
        return "ERROR", "Please Call APC for Troubleshooting."

    return "OK", "XDI (Build) Completed"


def YDI_report_generator_Online(df_x, df_y, Y_col_list, group_col_name, groups, input_group_path):

    if groups is None:
        group_folder_path_ = os.path.join(input_group_path, "NO_GROUP")
    elif groups is not None:
        group_folder_path_ = os.path.join(input_group_path, str(groups))


    pca_model_path = os.path.join(group_folder_path_, "YDI_PCA.pkl")
    module_PCA_ = joblib.load(pca_model_path)
    df_x_pca = module_PCA_.transform(df_x)

    cluster_model_path = os.path.join(group_folder_path_, "YDI_Clustering.pkl")
    module_Clustering_ = joblib.load(cluster_model_path)

    # y_data = np.concatenate((y_train, y_test), axis=0)
    cluster_idx = module_Clustering_.predict(df_x_pca)[0]

    threshold_table_path = os.path.join(group_folder_path_, "YDI_threshold_table.csv")
    df_YDI_table = pd.read_csv(threshold_table_path)


    list_YDI_values = []
    list_YDI_check = []
    list_YDI_Threshold = []
    list_Y_avg = []
    list_interval = []

    for col in Y_col_list:
        df_YDI_table_y = df_YDI_table.loc[(df_YDI_table["Y"] == col)].set_index(['Cluster_idx'])
        y_avg = df_YDI_table_y['Y_avg'].iloc[cluster_idx]
        y_avg_re = (y_avg + ((y_avg == 0) + 0)) * (((y_avg < 0) * -1) + (y_avg >= 0) * 1)
        YDI_values = np.abs(df_y[col].iloc[-1] - y_avg) / y_avg_re
        YDI_threshold = df_YDI_table_y['YDI_Thrshold'].iloc[cluster_idx]
        YDI_interval = df_YDI_table_y['Interval'].iloc[cluster_idx]

        list_YDI_values.append(YDI_values)
        list_Y_avg.append(y_avg)
        list_YDI_Threshold.append(YDI_threshold)
        list_interval.append(YDI_interval)


        YDI_check = YDI_threshold > YDI_values

        if YDI_check:
            # OK
            list_YDI_check.append(1)
        else:
            # NG
            list_YDI_check.append(0)


    df_YDI_online = pd.DataFrame({
                                  "Y": Y_col_list,
                                  "Cluster_idx": cluster_idx,
                                  "YDI": list_YDI_values,
                                  "YDI_Threshold": list_YDI_Threshold,
                                  "YDI_Check": list_YDI_check,
                                  "Interval": list_interval,
                                  "Y_avg": list_Y_avg
                                  })
    return df_YDI_online


def df_drop_col(dfs, drop_col_list, mylogs):
    df_idx = dfs[drop_col_list]

    for col in drop_col_list:
        try:
            dfs = dfs.drop(col, axis=1)
        except Exception as e:
            msg = "YDI(Pre-process): There is no col named" + str(col) + "already."
            mylogs.warning(msg)
            mylogs.warning_trace(e)

    return dfs, df_idx


def OnLine_YDI(df_online_x, df_online_y, data_folder):

    input_path, mylog, config = YDI_prepare(data_folder, Mode="OnLine")

    mylog.info("YDI Online\n Start")

    df_x, df_x_idx = df_drop_col(df_online_x, config["IDX"], mylog)
    df_y, df_y_idx = df_drop_col(df_online_y, config["IDX"], mylog)

    if config["YDI"]["Data_Transform_Check"] == 0:
        module_transform_ = joblib.load(input_path["YDI_PreWork_DataTrans_path"])
        df_x = module_transform_.transform(df_x)

    if config["Filter_Feature"] is not None:
        group_col = config["Filter_Feature"]
        groups = df_x_idx[group_col].iloc[-1]

        df_YDI_online = YDI_report_generator_Online(df_x, df_y,
                                                    config["Y"], config["Filter_Feature"],
                                                    groups, input_path["YDI_Group_path"])
    else:
        df_YDI_online = YDI_report_generator_Online(df_x, df_y,
                                                    config["Y"], config["Filter_Feature"],
                                                    None, input_path["YDI_Group_path"])

    mylog.info("YDI Online\n Finished")
    return df_YDI_online


if __name__ == "__main__":
    path = "/home/petertylin/PycharmProjects/00_Projects/00_Smart_Prediction/AVM_System/Cases/T75R4_20190523/T75R4_20190523/T75R4_20190523_03/00/05_YDI/03_Test"
    path = "/home/petertylin/PycharmProjects/00_Projects/00_Smart_Prediction/AVM_System/Cases/TTP/TTP/TTP_00/00/05_YDI/01_Test"
    input_paths = read_path(path)

    df_x = pd.read_csv(input_paths["x_train_path"])
    df_y = pd.read_csv(input_paths["y_train_path"])
    for idx in range(df_x.shape[0]):
        df_tmp_x = pd.DataFrame(data=df_x.iloc[idx].values.reshape(1,-1), columns=df_x.columns)
        df_tmp_y = pd.DataFrame(data=df_y.iloc[idx].values.reshape(1, -1), columns=df_y.columns)
        # print(df_tmp_x)
        # print(df_tmp_y)
        X = OnLine_YDI(df_tmp_x, df_tmp_y, path)

        print("input type:", type(df_tmp_x), type(df_tmp_y))
        print("input shape:", df_tmp_x.shape, df_tmp_y.shape)

        print("Output type:", type(X))
        print("Output shape:", X.shape)
        print("Output YDI table")
        print(X)

        break
	
	
	
# GP_TPE_tune.py

# coding: utf-8

# In[1]:


from Read_path import read_path
from config import read_config, save_config
from CreateLog import WriteLog

import numpy as np
from matplotlib import pyplot as plt
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import Matern
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error
import os #新增前處理資料夾分類
from decimal import Decimal
from hyperopt import fmin, tpe, hp, partial, Trials, pyll, JOB_STATE_DONE, STATUS_OK
from hyperopt.fmin import generate_trials_to_calculate
import json
from scipy import stats ######找眾數######
import importlib
import configparser


# # GP

# In[2]:


def SetIniP(number,p,topall,tailall, iniP_seed, changeint_p_index, changeran_p_byStep_index): #按變數特性與指定點數找出隨機點(先驗點群，與後驗測試找EI)
    paramList=[]
    for i in range(len(p)):
        if i in changeint_p_index:
            np.random.seed(iniP_seed)
            paramList.append(np.random.randint(topall[i], tailall[i], size=number))
        elif i in changeran_p_byStep_index: #範圍先*100取整數再除100就可以讓取到的變數只到小數點第二位
            np.random.seed(iniP_seed)
            paramList.append(np.random.randint(topall[i]*100, tailall[i]*100, size=number)/100)
        else:
            np.random.seed(iniP_seed)
            paramList.append(np.random.uniform(topall[i], tailall[i], size=number))
#     print(paramList)
    paramList= np.array(paramList).T
    return paramList


# In[3]:


#######設定XGboost訓練方法######
def model_loss(next_params,topall,tailall,X_train1,X_val,y_train1,y_val,changeint_p_index,changeran_p_byStep_index,TuneModel):#將下一點之array先轉成XG吃的型態再送進XG訓練，再求validation loss，送出y(即loss值) 不能在這裡直接改Decimal因為TPE不吃
    next_params=list(next_params)
    for i in range(len(next_params)):
        if i in changeint_p_index:
            if int(next_params[i]-topall[i])<=0:
                next_params[i]=topall[i]
            else:
                next_params[i]=int(next_params[i])
        elif i in changeran_p_byStep_index:
            next_params[i]=Decimal(next_params[i]).quantize(Decimal('0.00'))
#     print(next_params)
    return TuneModel.trainES(next_params, X_train1, X_val, y_train1, y_val)


# In[4]:


def GP(paramList, y, kernel): #把變數集與核送進來
    gp = GaussianProcessRegressor(kernel=kernel)
    gp.fit(paramList, y)  #對X,y模型做回歸
    return gp


# In[5]:


def EI(paramList_tries, gp, y_min): #找出擁有最大EI的測試點
#     print(paramList_tries.shape)
    y_mean, y_std = gp.predict(paramList_tries, return_std=True)
    y_std=np.array([np.array([y_std[i]]) for i in range(len(y_std))]) #把y_std轉成跟y_mean一樣的形式
    ei = y_min - (y_mean - 1.96 * y_std)
    return ei


# In[6]:


def next_p(paramList_tries, ei):
    ei[ei < 0] = 0
    max_index = ei.argmax()
    next_parameter = paramList_tries[max_index]
    return ei.max(), next_parameter


# In[7]:


def EI_nextP(try_num, iter_num, gp, y_min,p,topall,tailall, bounds, iniP_seed, changeint_p_index, changeran_p_byStep_index): #找出最大預期改進值的超參數組
#隨機10萬點尋找最大改進值
    np.random.seed(iniP_seed)
    paramList_tries = SetIniP(try_num,p,topall,tailall, iniP_seed, changeint_p_index, changeran_p_byStep_index)
    ei=EI(paramList_tries, gp, y_min)
#     print('ei are: ',ei)
    EI_max, next_parameter=next_p(paramList_tries, ei)
#     print('max ei is: ', EI_max, 'next_parameter is: ', next_parameter)
    return next_parameter


# In[8]:


def GP_EI(paramList, y, kernel, GPtimes, try_num, iter_num, p, topall, tailall,          X_train1,X_val,y_train1,y_val,changeint_p_index,changeran_p_byStep_index, bounds, iniP_seed,TuneModel):
    for i in range(GPtimes): #每次做GPtimes=50-10=40次
        gp=GP(paramList, y, kernel)
        y_min=min(y)
        next_parameter=EI_nextP(try_num, iter_num, gp, y_min,p,topall,tailall, bounds, iniP_seed, changeint_p_index, changeran_p_byStep_index)
        paramList= np.row_stack((paramList,next_parameter)) #把新得到的超參組加到原超參組群的最後面
        next_y=model_loss(next_parameter,topall,tailall,X_train1,X_val,y_train1,y_val,changeint_p_index,changeran_p_byStep_index, TuneModel) #把下一點送訓得到新loss值
        next_y= np.array([[next_y]])  #幫next_y轉成與y同形態
        y= np.row_stack((y, next_y))  #把新得到的loss加到原loss群的最後面
    min_index= y.argmin()
    best_parameter = paramList[min_index]
    best_y = y[min_index]
    return y, paramList, min_index, best_parameter, best_y


# In[9]:


def probability_ES(y,times): #計算歷史出現較佳y的機率，減掉上一個做法的次數
    everbetter=1
    best=y[0]
    for i in range(len(y)):
        if y[i]<best:
            everbetter+=1
            best=y[i]
#             print(y[i])
    prob=everbetter/(len(y)-times)
    last_y_num=len(y)
#     print('The early stop check probability: ',prob)
    return prob, everbetter


# In[10]:


def change_area(best_parameter, topall, tailall, p, changeint_p_index, minPa, maxPa): #改範圍函數
    t1=0 #計算是否有人改範圍
    for j in range(len(p)):
        if j in changeint_p_index: #整數部分只改深度跟葉子重，因為採樣範圍已經完整
            if best_parameter[j]==tailall[j]-1: #因為左邊已到底，右邊無邊界，而且整數採樣不含右邊界，所以要減1
                tailall[j]=int(best_parameter[j]+4)  #這邊改成直接把範圍加入考慮，而左邊以到底，所以左邊不動，只增加右邊
                t1+=1
        else:
            area=float(Decimal((tailall[j]-topall[j])/20).quantize(Decimal('0.000')))
            if (best_parameter[j] != minPa[j]) and(best_parameter[j]-topall[j]<area): #增加左邊界
                testtoparea=topall[j]  #######這邊要加到新版的改變數
                topall[j]=max(best_parameter[j]-20*area, minPa[j])
                if topall[j]!=testtoparea:
                    t1+=1
            if (best_parameter[j] != maxPa[j]) and(tailall[j]-best_parameter[j]<area): #增加右邊界
                testtailarea=tailall[j]  #######這邊要加到新版的改變數
                tailall[j]=min(best_parameter[j]+20*area, maxPa[j])
                if testtailarea!=tailall[j]:
                    t1+=1
#     print('There are {} different boundary.'.format(t1))
    return topall, tailall, t1


# # 進入TPE

# In[11]:


def argsDict_tranform(argsDict, changetranform, p, changeint_p_index): #將值域送進規定值中，因為quniform有問題
    argsDict1={}
    argsDict1=argsDict.copy() #這樣做才不會改變輸入的變數
    for i in changeint_p_index:
        argsDict1[p[i]] = argsDict1[p[i]] + changetranform[i]
    return argsDict1
#######設定XGboost調參的初始空間取法######
def setspace(p, tailall,topall, changetranform, changeint_p_index):
    hplist=[]
    for i in range(len(p)):
        if i in changeint_p_index:
            hplist.append(hp.randint(p[i], tailall[i]-changetranform[i]))
        else:
            hplist.append(hp.uniform(p[i], topall[i], tailall[i]))   
    space = dict(zip(p,hplist))
    return space


# In[12]:


def oldp_trails(p, paramList, y, tailall, topall, changeint_p_index, changetranform): #把點轉成trails##
    p1=p+['loss'] #把loss加到超參類別的最後一項
    df = pd.DataFrame(columns = p1) #把GP的結果整理成dataframe
    for i in range(len(p1)):
        if i in changeint_p_index:
            int_list=[]
            for j in paramList.T[i]:
                if int(j)-changetranform[i]<=0:
                    int_list.append(0)
                else:
                    int_list.append(int(j)-changetranform[i])  #轉int 接下來減掉未來要加上的1跟2
            df[p1[i]]=int_list
        elif i==(len(p1)-1):
            df[p1[i]]=y
        else:
            df[p1[i]]=paramList.T[i]
    space=setspace(p, tailall,topall, changetranform, changeint_p_index)
    test_trials=df_trials(df, space, changeint_p_index,p) #呼叫樓下的df_trials
    return test_trials, space


# In[13]:


def df_trials(df, space, changeint_p_index,p):  ####把dataframe轉成trails#####
    hyperopt_selection = pyll.stochastic.sample(space)
    test_trials = Trials()  #仿造hyperopt生成歷史檔test_trials
    for tid, (index, row) in enumerate(df.iterrows()):
        ######這一段為把該轉成整數的轉成整數############
        vals={}
        for key in hyperopt_selection.keys():
            if key in [p[j] for j in changeint_p_index]:
                vals[key] = [int(round(row[key]))]
            else:
                vals[key] = [row[key]]
        ######這一段為把該轉成整數的轉成整數############        
        
        hyperopt_trial = Trials().new_trial_docs(
            tids=[tid],
            specs=[None], 
            results=[{'loss': row['loss'], 'status': STATUS_OK}],  
            miscs=[{'tid': tid
                    , 'cmd': ('domain_attachment', 'FMinIter_Domain')
                    , 'idxs': {**{key: [tid] for key in hyperopt_selection.keys()}}
                    , 'vals': vals
                    , 'workdir': None}]
           )
        hyperopt_trial[0]['state'] = JOB_STATE_DONE

        test_trials.insert_trial_docs(hyperopt_trial) 
        test_trials.refresh()

    return test_trials


# In[14]:


#目前model是TPE_model_factory，要記得改後面執行時的變數model
def TPE_EI(model, space, TPEtimes, test_trials, iniP_seed): 
#     algo = partial(tpe.suggest, n_startup_jobs=1) #不用再設隨機跑20點，因為前面的GP已經累積超過預設的20個，隨機取點的機制已經失效
    best = fmin(model, space, algo=tpe.suggest, max_evals=TPEtimes+len(test_trials.losses()),                trials=test_trials, rstate=np.random.RandomState(iniP_seed))
    return best, test_trials


# In[15]:


###########製作調參之點歷史紀錄dataframe#######
def TPE_his_point(p, test_trials, changetranform, changeint_p_index):
    tpe_results = pd.DataFrame()  
    for i in range(len(p)):
        if i in changeint_p_index:
            tpe_results[p[i]]=[k+changetranform[i] for k in test_trials.idxs_vals[1][p[i]]]
        else:
            tpe_results[p[i]]=test_trials.idxs_vals[1][p[i]]
    return tpe_results


# In[16]:


def final_params(best_parameter, minPa, p): #將最佳超參作成最後送訓的超參
    for k in range(len(best_parameter)):
        if best_parameter[k] < minPa[k]:
            best_parameter[k] = minPa[k]
    return best_parameter


# In[17]:


def GP_TPE_3to1(X_train1, y_train1, X_val, y_val,topall, tailall, iniP, p, minPa, maxPa, changeint_p_index,                changeran_p_byStep_index, bounds, change_area_time, GPtimes_original, GP_maxp, kernel, try_num, iter_num,                probabilty_step, changetranform, TPEtimes_original, TPE_maxp, ChangeareaTime, mylog, iniP_seed, data_folder,TuneModel):
    loss3to1=[] #紀錄三次調參後每次的最佳值
    param3to1=[] #紀錄三次調參後每次的最佳參數
    
    for num in range(3): 
        
        output_path={}
        output_path["Param_aftertuning_path"] = os.path.join(data_folder, "Parameter_aftertuning_{}.csv".format(num))
        Param_aftertuning_path = output_path["Param_aftertuning_path"]
        
#         print('iniP_seed:',iniP_seed)
        paramList=SetIniP(iniP,p,topall,tailall, iniP_seed, changeint_p_index, changeran_p_byStep_index) #先隨機取20點
        y= np.array([model_loss(paramList[i],topall,tailall,X_train1,X_val,y_train1,y_val,changeint_p_index,changeran_p_byStep_index,TuneModel) for i in range(paramList.shape[0])])[:, np.newaxis] #求出前10點的loss，並轉到正確的shape
        #######進入高斯調參迴圈########
        last_y_num=0 
        everbetter=1
        cat=0
        GPtimes=GPtimes_original
        TPEtimes=TPEtimes_original
        Modeltime=0
        GPcosttime=0
        while cat<change_area_time: #至多改幾次範圍
            if cat>0:
                GPtimes=ChangeareaTime
            t=0
            while t<GP_maxp: #至多跑t次高斯調參
                ######把VTL之GP+EI做完#######
                # tStart = time.time()#計時開始
    #             print('The shape of paramList is:', paramList.shape)
    #             print('The shape of y is:', y.shape)
    #             print('The shape of yd is:', yd.shape)
    
                #做第一次GP+EI
                try:
                    y, paramList, min_index, best_parameter, best_y=GP_EI(paramList, y, kernel, GPtimes, try_num, iter_num, p,topall,                                                                          tailall,X_train1,X_val, y_train1,y_val,changeint_p_index,                                                                          changeran_p_byStep_index,bounds,iniP_seed,TuneModel)
                except Exception as e:
                    mylog.error("Running {}-th times GP is wrong!".format(t))
                    mylog.error_trace(e)
                Modeltime+=GPtimes
                t+=GPtimes
                GPtimes=1
                prob, everbetter=probability_ES(y,GPcosttime/(cat+1))
                if prob<probabilty_step:
                    break
            try:
                topall, tailall, t1= change_area(best_parameter, topall, tailall, p, changeint_p_index, minPa, maxPa)  #改範圍
            except Exception as e:
                mylog.error("Running {}-th times GP change_area is wrong!".format(cat))
                mylog.error_trace(e)
            Modeltime+=t
            cat+=1
            if t1==0:
                cat+=change_area_time  
            GPcosttime=Modeltime
#         print('The best parameter is: ', best_parameter)
#         print('The best y is: ', best_y)
        points_df=pd.DataFrame(dict(zip(p,paramList.T)))
        points_df['loss']=y
        try:
            points_df.to_csv(Param_aftertuning_path, index=False)
        except Exception as e:
            mylog.error("The {}-th times GP error!".format(num))
            mylog.error_trace(e)
#         print('In GP, XGboost is called {} times.'.format(XGtime))
#         print('topall:', topall)
#         print('tailall:', tailall)
#         print('################Let us do TPE!###############')
    ##########################進入TPE調參迴圈#########################(以上已經確認無誤)
    #     t=0
        last_y_num=0 
        everbetter=1
        cat=0
        while cat<change_area_time: #至多改幾次範圍
            if cat>0:
                TPEtimes=ChangeareaTime
            t=0
            while t<TPE_maxp: #至多跑t次TPE調參
                ###此處要check 覆蓋新的paramList, y
                try:
                    test_trials, space=oldp_trails(p, paramList, y, tailall, topall, changeint_p_index, changetranform) #製作歷史點的線索與定義超參選取方法與範圍，p為超參類別名
                except Exception as e:
                    mylog.error("Making {}-th times GP hisotry points trails is wrong!".format(t))
                    mylog.error_trace(e)
                #######設定TPE之XGboost訓練方法######因為這裡要自己去吃上面的變數 不能用指定的  所以只能在這邊定義
                def TPE_model_factory(argsDict):
                    nonlocal changetranform
                    argsDict1 = argsDict_tranform(argsDict, changetranform,p, changeint_p_index)  
                    return TuneModel.trainES_TPE(argsDict1, X_train1, X_val, y_train1, y_val)
                #######設定TPE之XGboost訓練方法######因為這裡要自己去吃上面的變數 不能用指定的  所以只能在這邊定義
                try:
                    best, test_trials=TPE_EI(TPE_model_factory, space, TPEtimes, test_trials, iniP_seed)
                except Exception as e:
                    mylog.error("Running {}-th times TPE is wrong!".format(t))
                    mylog.error_trace(e)
                try:
                    tpe_results=TPE_his_point(p, test_trials, changetranform, changeint_p_index) #製作TPE歷史點
                except Exception as e:
                    mylog.error("Making {}-th times TPE hisotry points trails is wrong!".format(t))
                    mylog.error_trace(e)
                paramList=np.array([tpe_results[i].tolist() for i in p]).T #這是將TPE之歷史點轉成跟paramList一樣的形式，加入原來的paramList
                y=test_trials.losses() #把loss加進去原來的y
                best_parameter=[]  #把最佳參數組之型態轉成list才能餵回去改範圍的函數，而且整數要先轉回來 best不是真的最佳點!!!
                for i in range(len(p)):
                    if i in changeint_p_index:
                        best_parameter.append(best[p[i]]+changetranform[i])
                    else:
                        best_parameter.append(best[p[i]])
                best_y=min(y) #把最好的y寫下來
                t+=TPEtimes
                TPEtimes=1
                prob, everbetter=probability_ES(y,GPcosttime/(cat+2)) #算出現最佳值機率
                if prob<probabilty_step:
                    break               
            try:
                topall, tailall, t1= change_area(best_parameter, topall, tailall, p, changeint_p_index, minPa, maxPa)  #改範圍
            except Exception as e:
                mylog.error("Running {}-th times TPE change_area is wrong!".format(cat))
                mylog.error_trace(e)
            Modeltime+=t
            cat+=1
            if t1==0:
                cat+=change_area_time
            tpe_results['loss(MSE)']=test_trials.losses()
            try:
                tpe_results.to_csv(Param_aftertuning_path, index=False)
            except Exception as e:
                mylog.error("The {}-th times TPE error!".format(num))
                mylog.error_trace(e)
            GPcosttime=Modeltime
#             print('We train {} times.'.format(len(tpe_results)))
#             print('################Let us do the next GP+TPE!###############')
        loss3to1.append(best_y)  #紀錄三次調參後每次的最佳值
        param3to1.append(best_parameter) #紀錄三次調參後每次的最佳參數
        iniP_seed+=1
    best_y3to1=min(loss3to1)
    best_parameter=param3to1[np.argmin(loss3to1)]
#     print('The early stop check probability: ', prob)
#         print('The best parameter is: ', best_parameter)
    print('The best loss(MSE) is: ', best_y3to1)
#         print('The XGboost is called {} times.'.format(XGtime))

    ###########################超參送訓+測試資料集#######################
    #############用Early stop找出best round###########
    try:
        params=final_params(best_parameter, minPa, p)
    except Exception as e:
        mylog.error("Find final Param error")
        mylog.error_trace(e) 
    try:
        model, nround = TuneModel.trainES_model(params, X_train1, X_val, y_train1, y_val)
    except Exception as e:
        mylog.error("train error before predict merge_X_train")
        mylog.error_trace(e)
    else:
        return model, params, nround 


# In[18]:


# # 我們來調參吧
def Model_tuning(data_folder, ModelName):

    conf = configparser.ConfigParser() 
    conf.read("Algorithm_config.ini")  
    params_path = conf.get(ModelName, 'params')
    model_path = conf.get(ModelName, 'model')
    TuneModel= importlib.import_module(model_path) # 動態import 20191022改讀取ini檔
    
####################初始值設定#######################
    input_path = read_path(data_folder) #read 00\06_Model\XGB\00_Parameter_tuning\file_path.json   
    # init log
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    #mylog.init_logger() #設定info存在System.log,error存在error.log
    # read config
    config = read_config(input_path["config_path"], mylog) #read 00\00_config\config.json
    
####################讀資料進來#######################
    mylog.info("-----{} tuning-----".format(ModelName))
             
    x_Trainpath=input_path["x_train_path"] #訓練資料集路徑 
#     y_Trainpath=input_path["y_train_path"] #訓練資料集路徑
    y_Trainpath=input_path["y_train_T_path"] #訓練資料集路徑 20191022 y_train_path -> y_train_T_path for Z-score
    delcol = config["IDX"]  #20190613 Index_Columns->IDX, avoid y_train error[2 column]
    
    try:
        train=pd.read_csv(x_Trainpath)   
        y_train=pd.read_csv(y_Trainpath).drop(delcol, axis=1)        
    except Exception as e:
        mylog.error("Read raw data error")
        mylog.error_trace(e) 
        return "ERROR", "Please Call APC for Troubleshooting."
    X_train=train.drop(delcol, axis=1)    
####################讀資料進來#######################

    ModelTime1=0  #調參檢驗結果記數器
    
    #########################讀取V3 預設參數##############
    with open(params_path) as json_data:  #20191022改讀取ini檔
        model_params = json.load(json_data)
    seed= model_params['seed'] 
    p= model_params['p'] #要調的參數名
    ES= model_params['ES']
    if len(p)>=1:
        X_train1, X_val, y_train1, y_val=train_test_split(X_train, y_train, test_size=0.3, random_state=seed)
        minPa= model_params['minPa'] #各參數最小值
        maxPa= model_params['maxPa'] #各參數最大值
        changeint_p_index= model_params['changeint_p_index'] ###要轉成整數的超參索引值。如XG的max_depth#####
        changeran_p_index= model_params['changeran_p_index'] ###連續型隨機取的超參，如XG的'gamma','alpha','lambda','learning_rate'#
        changeran_p_byStep_index= model_params['changeran_p_byStep_index'] #連續型位數只取到小數點兩位，如XG的取樣比率subsample,colsample_bytree
        changetranform=model_params['changetranform'] #因TPE需要，須將整數型超參移到從0開始的係數取出來
        #依序輸入所有超參數的首次測試極值
        topall= model_params['topall'] #拉大正規化數值
        tailall= model_params['tailall']
        bounds = np.array([topall, tailall]).T #將每次的測試範圍轉成array

        with open('GP_TPE_config.json') as json_data:
            tune_params = json.load(json_data)
        change_area_time= tune_params['change_area_time'] #至多改幾次範圍
        ###########高斯與EI的參數###########
        iniP_seed=tune_params['iniP_seed']
        iniP= tune_params['iniP'] #起頭測試幾點再高斯
        GPtimes_original= tune_params['GPtimes_original'] #起跑高斯過程的次數，即一共增加多少點
        GP_maxp= tune_params['GP_maxp'] #每次高斯至多跑幾次，可能因為ES而提前停下
        kernel= 1.0 * Matern(length_scale=1.0, length_scale_bounds=(1e-1, 10.0), nu=1.5) #定義高斯kernel的各種參數
        try_num= tune_params['try_num'] #EI
        iter_num= tune_params['iter_num']   #EI
        probabilty_step= tune_params['probabilty_step'] #調參之earlystop門檻
        ###########高斯與EI的參數###########
        TPEtimes_original= tune_params['TPEtimes_original'] #跑TPE過程的次數，即一共增加多少點
        TPE_maxp= tune_params['TPE_maxp'] #每次TPE至多跑幾次，可能因為ES而提前停下
        ChangeareaTime= tune_params['ChangeareaTime']
####################初始值設定#######################

##########送進含有檢驗機制的切分與調參###############
    die=0
    while die==0:
        if len(p)>=1:
            try:
                model, params, nround=GP_TPE_3to1(X_train1, y_train1, X_val, y_val,topall, tailall, iniP, p, minPa, maxPa, changeint_p_index,                    changeran_p_byStep_index, bounds, change_area_time, GPtimes_original, GP_maxp, kernel, try_num, iter_num,                    probabilty_step, changetranform, TPEtimes_original, TPE_maxp, ChangeareaTime, mylog, iniP_seed, data_folder, TuneModel)  ####找最佳超參####
            except Exception as e:
                    mylog.error("GP_TPE_3to1 error!")
                    mylog.error_trace(e)
                    return "ERROR", "Please Call APC for Troubleshooting."
        else:
            try:
                model, nround = TuneModel.trainES_model(X_train, y_train, seed) #只需要找nround這唯一一種超參時，留給使用CV的機會
                params=[]
                X_train1=X_train
            except Exception as e:
                mylog.error("train error before predict merge_X_train")
                mylog.error_trace(e)
                return "ERROR", "Please Call APC for Troubleshooting."
        pred = TuneModel.predict(model, X_train1) #來檢查是否預測超過一半相同
        arr=stats.mode(pred)

        # 20191218
        paramsjson_path = os.path.join(data_folder, "paramsjson.json")  # save paramsjson to paramsjson.json
        paramsjson = {}
        paramsjson["params"] = params
        paramsjson["nround"] = nround

        if arr[1][0] < (len(X_train1)/2):  #arr[1][0]為眾數出現次數，通過檢驗進入下一步
            die=die+1
            # ########將最後所有的超參記錄在json檔中###########
            # 20191218
            # paramsjson_path = os.path.join(data_folder, "paramsjson.json")  # save paramsjson to paramsjson.json
            # paramsjson = {}
            # #             if len(p)>=1:
            # paramsjson["params"] = params
            # paramsjson["nround"] = nround
            try:
                save_config(paramsjson_path, paramsjson, mylog)
                return "OK", "tuning Completed!"
            except Exception as e:
                mylog.error("Params and nround to json error!")
                mylog.error_trace(e)  
                return "ERROR", "Please Call APC for Troubleshooting."
            
        elif ModelTime1 == 10:
            die=die+1
            mylog.error("We trained {} 10 times. But still false, please give us more data and check the correction of data.".format(ModelName))
            save_config(paramsjson_path, paramsjson, mylog)
            return "NG", "We tune {} 10 times. But still false, please give us more data and check the correction of data.".format(ModelName)
        else:
            seed= seed+1
            ModelTime1=ModelTime1+1

		
		
# train.py

# coding: utf-8

# In[1]:


from Read_path import read_path
from config import read_config, save_config
from CreateLog import WriteLog
import xgboost as xgb
import pandas as pd
import os #新增前處理資料夾分類
import json
import importlib
import configparser
try:
    import shap
    shap_on = True
except:
    shap_on = False

# In[2]:


#############正式訓練，可以用於test前也可以用於之後############
def modeltrain(data_folder, ModelName):  #read 06_Model/XGB/01_Model_building
    conf = configparser.ConfigParser() 
    conf.read("Algorithm_config.ini")  
    model_path = conf.get(ModelName, 'model')
    TuneModel= importlib.import_module(model_path) # 動態import 20191022改讀取ini檔
#     TuneModel= importlib.import_module('{}_model'.format(ModelName)) # 動態import
    ###########創造存檔路徑##############
    output_path={}
    output_path["ModelSave_path"] = os.path.join(data_folder, "{}.model".format(ModelName))   
    output_path["FeatureScore_path"] = os.path.join(data_folder, "FeatureScore.csv")
    output_path["summary_path"] = os.path.join(data_folder, "summary_plot_old.png")
    output_path["decision_train_path"] = os.path.join(data_folder, "decision_plot_train.png")
    output_path["pdp_path"] = os.path.join(data_folder, "pdp_plot.png")
    output_path["shap_values_path"] = os.path.join(data_folder, "shap_values.pkl")
    output_path["FI"] = os.path.join(data_folder, "Feature_Importance.csv")
    ModelSave_path = output_path["ModelSave_path"]
    ###########創造存檔路徑##############
    
    input_path = read_path(data_folder)
    # init log
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    #mylog.init_logger() #設定info存在System.log,error存在error.log    
    mylog.info("-----{} building-----".format(ModelName))
    # read config
    config = read_config(input_path["config_path"], mylog) #read 00\00_config\config.json
    paramsjson = read_config(input_path["paramsjson_path"], mylog) ######讀取超參群json檔(這邊要請David加)
    try:
        params=paramsjson["params"]
    except Exception as e:
        mylog.error("Read paramsjson error")
        mylog.error_trace(e) 
        return "ERROR", "Please Call APC for Troubleshooting."
    nround=paramsjson["nround"]
#     print(params)
    
    x_Trainpath=input_path['x_train_path'] #訓練資料集路徑
#     y_Trainpath=input_path['y_train_path'] #訓練資料集路徑
    y_Trainpath=input_path['y_train_T_path'] #訓練資料集路徑 20191022 y_train_path -> y_train_T_path for Z-score
    FeatureScore_path = output_path["FeatureScore_path"]
    delcol = config["IDX"]   
    
    try:
        train=pd.read_csv(x_Trainpath)    
        y_train=pd.read_csv(y_Trainpath).drop(delcol, axis=1)      
    except Exception as e:
        mylog.error("Read raw data error")
        mylog.error_trace(e) 
        return "ERROR", "Please Call APC for Troubleshooting."
    
    X_train=train.drop(delcol, axis=1)

    if shap_on:
        try:
            x_train_no_z = pd.read_csv(input_path['x_train_no_z_path'])
        except Exception as e:
            mylog.error("Read raw data error")
            mylog.error_trace(e)
            return "ERROR", "Please Call APC for Troubleshooting."

        x_train_no_z = x_train_no_z.drop(delcol, axis=1)
        x_train_no_z = x_train_no_z.drop(config["Y"], axis=1)
    else:
        x_train_no_z = None
    
    print("************")
    print('p ',params)
    print('n ',nround)
    print('xt ',X_train)
    print('yt ',y_train)
    print('ip ',input_path)
    print('op ',output_path)
    print('xtn ',x_train_no_z)
    print('m ',mylog)
    print("************")
    try:
        model = TuneModel.train(params, nround, X_train, y_train, input_path, output_path, x_train_no_z, mylog)
    except Exception as e:
        mylog.error("train error before predict X")
        mylog.error_trace(e)
        return "ERROR", "Please Call APC for Troubleshooting."
 
    df = TuneModel.importanceDf(model,X_train)
    df.to_csv(FeatureScore_path, index=False)  #儲存變數重要性
    TuneModel.SaveModel(model, ModelSave_path)#儲存Model檔
    
    return "OK", "{} train Completed!".format(ModelName)



# prediction.py

# coding: utf-8

# In[1]:


from Read_path import read_path
from config import read_config, save_config
from CreateLog import WriteLog
import xgboost as xgb
import pandas as pd
import os #新增前處理資料夾分類
import json
import operator
import importlib
import configparser


# In[2]:


def modelpredict(data_folder, ModelName, dataName, df_x_test=None):  #read 06_Model/XGB/01_Model_building以及資料集名稱(train、test、online、batch的dataFrame)
    #####可以使用dataName來決定是要送dataframe或者csv路徑進來######
    ### "online"：dataName進+送數據出去，"others"：csv進出 ###
    conf = configparser.ConfigParser() 
    conf.read("Algorithm_config.ini")  
    model_path = conf.get(ModelName, 'model')
    TuneModel= importlib.import_module(model_path) # 動態import 20191022改讀取ini檔
    
    input_path = read_path(data_folder)
    # init log
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    #mylog.init_logger() #設定info存在System.log,error存在error.log    
    mylog.info("-----{} prediction-----".format(ModelName))
    # read config
    config = read_config(input_path["config_path"], mylog) #read 00\00_config\config.json
    delcol = config["IDX"]
    
    model_path_final=input_path[model_path]
    model = TuneModel.LoadModel(model_path_final)   # load data   
    
    y_config = config["Y"]
    y_pred = y_config[0] + "_pred"
      
    if dataName == 'online':
        if df_x_test is None:
            mylog.error("Must give me df_x_test online!")
            return "ERROR", "No x data.", "No pred."
        else:
            X_test=df_x_test.drop(delcol, axis=1)
            
        try:
            pre = TuneModel.predict(model, X_test)              
            # Add for Z-score 20191022
            from DataTransformY import OnlineDataInverseY
            Y_config = read_config(input_path["config_Y_path"], mylog)
            pre = OnlineDataInverseY(pre, y_config[0], Y_config)
            
        except Exception as e:
            mylog.error("predict error")
            mylog.error_trace(e)
        else:
            return "OK", "Prediction finished.", pre[0]
    else:
        x_path=input_path['x_{}_path'.format(dataName)] #訓練資料集路徑
        ###########創造存檔路徑##############
        output_path={}
        output_path["{}_path".format(dataName)] = os.path.join(data_folder, "{}PredResult.csv".format(dataName))    
        PredResult_path = output_path["{}_path".format(dataName)]
        # print(PredResult_path)
        ###########創造存檔路徑##############
        if df_x_test is not None:
            mylog.error("Should not give me df_x_test!") 
            return "ERROR", "There should be no df_x_test in {}.".format(dataName), "No pred." 
        else:
            try:
                train=pd.read_csv(x_path) ###這裡沒有y####       
            except Exception as e:
                mylog.error("Read raw data error")
                mylog.error_trace(e) 
                return "ERROR", "Please Call APC for Troubleshooting.", "No pred."
            ######進入預測主程式#####
            try:
                X_train = train.drop(delcol, axis=1) 
                pre = TuneModel.predict(model, X_train) 
                sheet_train = train[delcol]
                ans = pd.DataFrame(pre,columns=[y_pred])
                
                # Add for Z-score 20191022
                from DataTransformY import TestDataInverseY
                Y_config = read_config(input_path["config_Y_path"], mylog)
                # print(ans)
                ans = TestDataInverseY(ans, Y_config, mylog)
                # print(ans)
                
                output = pd.concat([sheet_train,ans],axis=1)
            except Exception as e:
                mylog.error("Predict error")
                mylog.error_trace(e)
                return "ERROR", "Please Call APC for Troubleshooting.", "No pred."
            else:       
                output.to_csv(PredResult_path, index=False)  
                return "OK", "Prediction finished.", "See csv"
    mylog.info("-----XGB predict Done-----")



# 
