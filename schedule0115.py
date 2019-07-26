from Data_Preview import Data_Preview
from Data_PreProcess import Data_PreProcess_Train, Data_PreProcess_Test
from XDI import XDI_off_line_report, Build_XDI_Model
from YDI import YDI_off_line_report, Build_YDI_Model
from MXCI_MYCI_pre import pre_MXCI_MYCI
from MXCI_MYCI import MXCI_MYCI_offline
from Model_Selection import Model_Selection
from XGB_model import xgb_tuning, xgb_build, xgb_predict, xgb_merge_tuning, xgb_merge_predict, xgb_batch_predict
from PLS_model import pls_build, pls_predict, pls_merge_predict, pls_batch_predict
from Read_path import read_path
from config import read_config
from CreateLog import WriteLog
from DB_operation import DB_Connection, select_project_by_status, select_project_model_by_modelid, select_project_model_by_modelname, select_project_with_model_by_projectid, update_project_STATUS_by_projectid, update_project_model_modelstatus_by_modelid, update_project_model_modelstatus_modelstep_waitconfirm_by_modelid, update_project_model_mae_mape_by_modelid

import os
import json
import traceback
from Path import All_path
from Data_Collector import Data_Collector
# from Data_Check import Data_Check
from Exclusion import DataRemove

from shutil import copyfile
import pandas as pd
import time
from json2csv import json2csv
import multiprocessing as mp


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
        self.save_path_config()
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

    def model(self, path_dict, merge_flag=None):
        if merge_flag is None:
            base_num = 0
            xgb_tuning(path_dict['6']["XGB"][str(base_num)])
            xgb_build(path_dict['6']["XGB"][str(base_num+1)])
            xgb_predict(path_dict['6']["XGB"][str(base_num+2)])
            
            #20190626 temporarily mark
            pls_build(path_dict['6']["PLS"][str(base_num+1)])
            pls_predict(path_dict['6']["PLS"][str(base_num+2)])
            
            input_path = read_path(path_dict['6']["PLS"][str(base_num+2)])
            mylog = WriteLog(input_path["log_path"], input_path["error_path"])     
            config = read_config(input_path["config_path"], mylog)
            
            x_Trainpath=input_path["x_train_path"] #訓練資料集路徑
            x_Testpath=input_path["x_test_path"] #訓練資料集路徑
            delcol = config["IDX"] 
            pls_y_name = config["Y"][0]+"_pred_PLS" #20190710 read config y 
                      
            train=pd.read_csv(x_Trainpath)
            test=pd.read_csv(x_Testpath)
            sheet_train = train[delcol]
            sheet_test = test[delcol]
            
            pls_predict_train_path = os.path.join(path_dict['6']["PLS"][str(base_num+2)], "trainPredResult.csv")
            pls_predict_test_path = os.path.join(path_dict['6']["PLS"][str(base_num+2)], "testPredResult.csv")
            
            xgb_predict_train_path = os.path.join(path_dict['6']["XGB"][str(base_num+2)], "trainPredResult.csv")
            xgb_predict_test_path = os.path.join(path_dict['6']["XGB"][str(base_num+2)], "testPredResult.csv")
            
            #20190626 temporarily mark
            pls_train=pd.read_csv(pls_predict_train_path)
            pls_test=pd.read_csv(pls_predict_test_path)
            #pls_train=pd.read_csv(xgb_predict_train_path)
            #pls_test=pd.read_csv(xgb_predict_test_path)            
            #pls_train['Value_pred_XGB'] = pls_train['Value_pred_XGB']*1.2
            #pls_test['Value_pred_XGB'] = pls_test['Value_pred_XGB']*1.2
            
            #20190626 temporarily mark
            pls_train.rename({'Y_pred': pls_y_name}, axis=1, inplace=True)
            pls_test.rename({'Y_pred': pls_y_name}, axis=1, inplace=True)
            #pls_train.rename({'Value_pred_XGB': 'Value_pred_PLS'}, axis=1, inplace=True)
            #pls_test.rename({'Value_pred_XGB': 'Value_pred_PLS'}, axis=1, inplace=True)
            
            #20190626 temporarily mark
            output_train = pd.concat([sheet_train,pls_train],axis=1)
            output_test = pd.concat([sheet_test,pls_test],axis=1)
            output_train.to_csv(pls_predict_train_path, index=False) 
            output_test.to_csv(pls_predict_test_path, index=False) 
            #pls_train.to_csv(pls_predict_train_path, index=False) 
            #pls_test.to_csv(pls_predict_test_path, index=False) 
        else:
            base_num = 3
            xgb_merge_tuning(path_dict['6']["XGB"][str(base_num)])
            xgb_build(path_dict['6']["XGB"][str(base_num+1)])
            xgb_merge_predict(path_dict['6']["XGB"][str(base_num+2)])
            
            #20190626 temporarily mark
            pls_build(path_dict['6']["PLS"][str(base_num+1)])
            pls_merge_predict(path_dict['6']["PLS"][str(base_num+2)])
            
            input_path = read_path(path_dict['6']["PLS"][str(base_num+2)])
            mylog = WriteLog(input_path["log_path"], input_path["error_path"])   
            config = read_config(input_path["config_path"], mylog)
            
            x_Trainpath=input_path["x_train_path"] #訓練資料集路徑
            delcol = config["IDX"] 
            pls_y_name = config["Y"][0]+"_pred_PLS"
            
            train=pd.read_csv(x_Trainpath)
            sheet_train = train[delcol]
            
            pls_predict_train_path = os.path.join(path_dict['6']["PLS"][str(base_num+2)], "trainPredResult.csv")
            xgb_predict_train_path = os.path.join(path_dict['6']["XGB"][str(base_num+2)], "trainPredResult.csv")
            
            #20190626 temporarily mark
            pls_train=pd.read_csv(pls_predict_train_path)
            #pls_train=pd.read_csv(xgb_predict_train_path)
            #pls_train['Value_pred_XGB'] = pls_train['Value_pred_XGB']*1.2
            
            #20190626 temporarily mark
            pls_train.rename({'Y_pred': pls_y_name}, axis=1, inplace=True)
            #pls_train.rename({'Value_pred_XGB': 'Value_pred_PLS'}, axis=1, inplace=True)
            
            #20190626 temporarily mark
            output_train = pd.concat([sheet_train,pls_train],axis=1)
            output_train.to_csv(pls_predict_train_path, index=False)   
            #pls_train.to_csv(pls_predict_train_path, index=False)

    def model_predict(self, path_dict):
        xgb_batch_predict(path_dict['6']["XGB"]["main"])           
        pls_batch_predict(path_dict['6']["PLS"]["main"])
        
        input_path = read_path(path_dict['6']["PLS"]["main"])
        mylog = WriteLog(input_path["log_path"], input_path["error_path"])    
        config = read_config(input_path["config_path"], mylog)
            
        x_Testpath=input_path["x_test_path"] #訓練資料集路徑
        delcol = config["IDX"] 
            
        test=pd.read_csv(x_Testpath)
        sheet_test = test[delcol]
        
        pls_predict_test_path = os.path.join(path_dict['6']["PLS"]["main"], "testPredResult.csv")
        pls_test=pd.read_csv(pls_predict_test_path)
        pls_test.rename({'Y_pred': 'Value_pred_PLS'}, axis=1, inplace=True)
            
        output_test = pd.concat([sheet_test,pls_test],axis=1)
        output_test.to_csv(pls_predict_test_path, index=False)

        return None

    def training_phase(self, project_id, dc_instance, retrain_num, batch_num=None):
        retrain_num = self.get_max_retrain_num(retrain_num)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)
        path_dict = self.path_config[self.base_name][str(retrain_num)][str(batch_num)]
        
        cnxn2 = DB_Connection()
        
        SVM_PP_MODEL = select_project_with_model_by_projectid(project_id)
        
        train_flag = '0'
        test_flag = '1'
        Data_Preview_status = Data_Preview(path_dict['2']["main"])
        
        input_path = read_path(path_dict['2']["main"])
        json2csv(input_path["config_path"]) #20190702 update and output config.csv
        dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="DataPreview",
                                         retrain_num=retrain_num, batch_num=batch_num)       
        
        # FeatureExclude(path_dict['2']["main"])
        print(Data_Preview_status)
        if Data_Preview_status == "NG":
                
            cursor1 = cnxn2.cursor()
            cursor1.execute("UPDATE SVM_PROJECT_MODEL SET MODEL_SEQ = ?, MODEL_STATUS = ?, BATCH_SEQ = ?, IS_ONLINE = ?, MODEL_STEP = ?, WAIT_CONFIRM = ? WHERE MODEL_ID = ?", str(retrain_num), '1', str(batch_num), 0 , '1', 1 , int(SVM_PP_MODEL.MODEL_ID[0]))
            cnxn2.commit()
                
            def timer(n):
                while True:
                    return_confirm = select_project_model_by_modelid(SVM_PP_MODEL.MODEL_ID[0])
                    if str(return_confirm.WAIT_CONFIRM[0]) == "False":
                        break
                    else:
                        time.sleep(n) 
            timer(10)            
        else:              
            cursor1 = cnxn2.cursor()
            cursor1.execute("UPDATE SVM_PROJECT_MODEL SET MODEL_SEQ = ?, MODEL_STATUS = ?, BATCH_SEQ = ?, IS_ONLINE = ?, MODEL_STEP = ?, WAIT_CONFIRM = ? WHERE MODEL_ID = ?", str(retrain_num), '1', str(batch_num), 0, '1', 0, int(SVM_PP_MODEL.MODEL_ID[0]))
            cnxn2.commit()           

        Data_PreProcess_Train(path_dict['3'][train_flag])
        Data_PreProcess_Test(path_dict['3'][test_flag])
        
        update_project_model_modelstatus_by_modelid('2', SVM_PP_MODEL.MODEL_ID[0])

        Build_XDI_Model(path_dict['4'][train_flag])
        XDI_off_line_report(path_dict['4'][test_flag], mode="Train")
        
        json2csv(input_path["config_path"])        
        dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="XDI",
                                         retrain_num=retrain_num, batch_num=batch_num)

        Build_YDI_Model(path_dict['5'][train_flag])
        YDI_off_line_report_status = YDI_off_line_report(path_dict['5'][test_flag], mode="Train")
        
        json2csv(input_path["config_path"])
        dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="YDI",
                                         retrain_num=retrain_num, batch_num=batch_num)
        
        
        if YDI_off_line_report_status == "NG":
            update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('3', '3', 1, SVM_PP_MODEL.MODEL_ID[0])
                
            def timer(n):
                while True:
                    return_confirm = select_project_model_by_modelid(SVM_PP_MODEL.MODEL_ID[0])
                    if str(return_confirm.WAIT_CONFIRM[0]) == "False":
                        break
                    else:
                        time.sleep(n)
            timer(10)
        else:
            update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('3', '3', 0, SVM_PP_MODEL.MODEL_ID[0])
        
        DataRemove(path_dict['4']["1"], "XDI")  #20190703 remove after step3
        DataRemove(path_dict['5']["1"], "YDI")  #20190703 remove after step3
                 
        self.model(path_dict)
        json2csv(input_path["config_path"])
        dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="Model",
                                         retrain_num=retrain_num, batch_num=batch_num)
        
        update_project_model_modelstatus_by_modelid('4', SVM_PP_MODEL.MODEL_ID[0])

        msg_Model, win = Model_Selection(path_dict['7'][train_flag], mode="Train")
        output_paths = {}
        
        json2csv(input_path["config_path"])        
        dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="SelectModel",
                                         retrain_num=retrain_num, batch_num=batch_num)
        
        if msg_Model == "NG":

            update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('5', '5', 1, SVM_PP_MODEL.MODEL_ID[0])
                
            def timer(n):
                while True:

                    return_confirm = select_project_model_by_modelid(SVM_PP_MODEL.MODEL_ID[0])
                    if str(return_confirm.WAIT_CONFIRM[0]) == "False":
                        break
                    else:
                        time.sleep(n)
            timer(10)
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

        pre_MXCI_MYCI(path_dict['8'][train_flag])
        
        update_project_model_modelstatus_by_modelid('6', SVM_PP_MODEL.MODEL_ID[0])
        
        MXCI_MYCI_offline(path_dict['8'][test_flag], mode="Train")
        json2csv(input_path["config_path"])
        dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="CI",
                                         retrain_num=retrain_num, batch_num=batch_num)
        
        update_project_model_modelstatus_by_modelid('7', SVM_PP_MODEL.MODEL_ID[0])
        return None
        
    def merge_phase(self, base_name, retrain_num, batch_num=None):
        retrain_num = self.get_max_retrain_num(retrain_num)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)
        path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]

        print("MERGE phase")
        merge_flag = '2'
        test_flag = '3'

        Data_PreProcess_Train(path_dict['3'][merge_flag], merge_flag=True)
        Build_XDI_Model(path_dict['4'][merge_flag])
        XDI_off_line_report(path_dict['4'][test_flag], mode="Merge")
        Build_YDI_Model(path_dict['5'][merge_flag])
        YDI_off_line_report(path_dict['5'][test_flag], mode="Merge")

        self.model(path_dict, merge_flag=True)
        Model_Selection(path_dict['7']['1'], mode="Merge")
        pre_MXCI_MYCI(path_dict['8'][merge_flag])
        MXCI_MYCI_offline(path_dict['8'][test_flag],  mode="Merge")

        return None

    def module_for_loop(self, model_num, retrain_num, batch_num, dc):
        train_flag = '0'
        test_flag = '1'
        Step5_NG = 0
        Step5_count = 1 
        #cnxn2 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server_name + ';DATABASE=' + db_name + ';UID=' + user + ';PWD=' + password)
        
        for feature in self.feature_lists:
            
            print(model_num, self.filter_feature, feature)
            
            SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[feature]) #m2m modelname = self.filter_dir_name[feature]
            
            path_dict = self.path_config[self.filter_dir_name[feature]][str(retrain_num)][str(batch_num)]            
            input_path = read_path(path_dict[str(3)]['0'])
                        
            if model_num == 3:
                self.mdoel_dict[str(model_num)]["Train"](path_dict[str(model_num)][train_flag])
                self.mdoel_dict[str(model_num)]["Test"](path_dict[str(model_num)][test_flag])
                json2csv(input_path["config_path"])
            elif model_num == 5:
                self.mdoel_dict[str(model_num)]["Train"](path_dict[str(model_num)][train_flag])
                YDI_off_line_report_status = self.mdoel_dict[str(model_num)]["Test"](path_dict[str(model_num)][test_flag], mode="Train")
                json2csv(input_path["config_path"])
                
                if YDI_off_line_report_status == "NG":
                    Step5_NG = 1
                update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('3', '3', 0, SVM_PROJECT_MODEL.MODEL_ID[0])               
                
                if Step5_count == len(self.feature_lists):
                    dc.YDI_collector(path_config=self.path_config, filter_dir_name=self.filter_dir_name,
                         retrain_num=retrain_num, batch_num=batch_num,
                         feature_lists=self.feature_lists, merge_flag=None)
                    if Step5_NG == 1:
                       print("Wait user check YDI NG")
                       update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('3', '3', 1, SVM_PROJECT_MODEL.MODEL_ID[0])
                            
                       def timer(n):
                           while True:
                               return_confirm = select_project_model_by_modelname(self.filter_dir_name[feature])
                               if str(return_confirm.WAIT_CONFIRM[0]) == "False":
                                   break
                               else:
                                   time.sleep(n)
                       timer(10) 
                       DataRemove(path_dict['4']["1"], "XDI")  #20190716 if YDI NG, wait user check and remove after step3
                       DataRemove(path_dict['5']["1"], "YDI")  #20190703 if YDI NG, wait user check and remove after step3
            else:
                self.mdoel_dict[str(model_num)]["Train"](path_dict[str(model_num)][train_flag])
                self.mdoel_dict[str(model_num)]["Test"](path_dict[str(model_num)][test_flag], mode="Train")  

            Step5_count = Step5_count+1                                          
            
        return None

    def model_selection_for_loop(self, retrain_num, batch_num, dc):        
        #xgb_win = 0
        Step7_NG = 0
        Step7_count = 1
        
        for feature in self.feature_lists:
            path_dict = self.path_config[self.filter_dir_name[feature]][str(retrain_num)][str(batch_num)]
            msg_Model, win = Model_Selection(path_dict[str(7)]['0'], mode="Train")
            output_paths = {}
            
            SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[feature])
            input_path = read_path(path_dict[str(7)]['0'])
            json2csv(input_path["config_path"])
            
            if msg_Model == "NG":
                Step7_NG = 1
            update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('5', '5', 0, SVM_PROJECT_MODEL.MODEL_ID[0])
            
            input_path = read_path(path_dict[str(7)]['0'])
            mylog = WriteLog(input_path["log_path"], input_path["error_path"])     
            config = read_config(input_path["config_path"], mylog)  
            config["Model_Selection"][config["Y"][0]]["Predict_Model"]
            
            if Step7_count == len(self.feature_lists):
                dc.Selection_collector(path_config=self.path_config, filter_dir_name=self.filter_dir_name,
                               retrain_num=retrain_num, batch_num=batch_num,
                               feature_lists=self.feature_lists, merge_flag=None)
                if Step7_NG == 1:
                   update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('5', '5', 1, SVM_PROJECT_MODEL.MODEL_ID[0])
                        
                   def timer(n):
                       while True:
                           return_confirm = select_project_model_by_modelname(self.filter_dir_name[feature])
                           if str(return_confirm.WAIT_CONFIRM[0]) == "False":
                               break
                           else:
                               time.sleep(n)
                   timer(10)
            
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
                input_path = read_path(path_dict[str(7)]['0']) #20190710 avoid flat don't save mae mape to db
                mylog = WriteLog(input_path["log_path"], input_path["error_path"])     
                config = read_config(input_path["config_path"], mylog)  
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

    def many_model_phase(self, retrain_num, batch_num):
        for feature in self.feature_lists:
            path_dict = self.path_config[self.filter_dir_name[feature]][str(retrain_num)][str(batch_num)]
            self.model(path_dict)
        return None

    def many_to_many(self, project_id):
        dc = Data_Collector(self.base_path, self.train_data, self.config_file)  #create Data_Collector folder, base.path=../Cases/PSH_Demo 
        dc.create_file_path() # for DataPreview

        Data_Preview_status = Data_Preview(dc.data_preview_path)
        
        self.get_filter_feature()
        self.get_feature_content(split_flag=True)
        self.create_many_path(training_data_dict=self.filter_feature_dict)
                                         
        retrain_num = self.get_max_retrain_num(retrain_num=self.current_retrain_number)
        batch_num = self.get_max_batch_num(retrain_num=self.current_retrain_number, batch_num=None)
        
        #20190618 judge Data_Preview_status in future
        cnxn1 = DB_Connection()
        for x in self.filter_dir_name.keys():

            SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[x])
            
            cursor1 = cnxn1.cursor()
            cursor1.execute("UPDATE SVM_PROJECT_MODEL SET MODEL_SEQ = ?, MODEL_STATUS = ?, BATCH_SEQ = ?, IS_ONLINE = ?, MODEL_STEP = ?, WAIT_CONFIRM = ? WHERE MODEL_ID = ?", str(retrain_num), '1', str(batch_num), 0, '1', 0, int(SVM_PROJECT_MODEL.MODEL_ID[0]))
            cnxn1.commit() 
                
        dc.init_collector_dir(retrain_num, batch_num)
        print("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num))
        self.module_for_loop(model_num=3, retrain_num=retrain_num, batch_num=batch_num, dc=dc)
        
        for x in self.filter_dir_name.keys():

            SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[x])
            update_project_model_modelstatus_by_modelid('2', SVM_PROJECT_MODEL.MODEL_ID[0])            
          
        self.module_for_loop(model_num=4, retrain_num=retrain_num, batch_num=batch_num, dc=dc)
        dc.XDI_collector(path_config=self.path_config, filter_dir_name=self.filter_dir_name,
                         retrain_num=retrain_num, batch_num=batch_num,
                         feature_lists=self.feature_lists, merge_flag=None)
        self.module_for_loop(model_num=5, retrain_num=retrain_num, batch_num=batch_num, dc=dc)
            
        self.many_model_phase(retrain_num=retrain_num, batch_num=batch_num)           
        dc.Model_collector(path_config=self.path_config, filter_dir_name=self.filter_dir_name,
                           retrain_num=retrain_num, batch_num=batch_num, feature_name=self.filter_feature,
                           feature_lists=self.feature_lists, merge_flag=None)
        
        for x in self.filter_dir_name.keys():

            SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[x])            
            update_project_model_modelstatus_by_modelid('4', SVM_PROJECT_MODEL.MODEL_ID[0])
            
        self.model_selection_for_loop(retrain_num=retrain_num, batch_num=batch_num, dc=dc)
            
        self.module_for_loop(model_num=8, retrain_num=retrain_num, batch_num=batch_num, dc=dc)
        
        for x in self.filter_dir_name.keys():

            SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[x])
            update_project_model_modelstatus_by_modelid('6', SVM_PROJECT_MODEL.MODEL_ID[0])
            
        dc.CI_collector(path_config=self.path_config, filter_dir_name=self.filter_dir_name,
                        retrain_num=retrain_num, batch_num=batch_num,
                        feature_lists=self.feature_lists, merge_flag=None)
        
        for x in self.filter_dir_name.keys():

            SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[x])
            update_project_model_modelstatus_by_modelid('7', SVM_PROJECT_MODEL.MODEL_ID[0])

        self.many_merge_phase(retrain_num=retrain_num, batch_num=batch_num)
        self.many_data_porter(retrain_num=retrain_num, batch_num=batch_num, mode="Merge")
        
        cnxn1 = DB_Connection()
        cursor1 = cnxn1.cursor()
        cursor1.execute("UPDATE SVM_PROJECT SET STATUS = ? WHERE PROJECT_ID = ?", "CREATED_OK", int(project_id))
        cnxn1.commit() 
        return None
        
    def many_data_porter(self, retrain_num, batch_num, mode):
        dc = Data_Collector(self.base_path, self.train_data, self.config_file)
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
        dc = Data_Collector(self.base_path, self.train_data, self.config_file)
        self.path_config = self.create_path(path)
        self.save_path_config()
        retrain_num = self.get_max_retrain_num(retrain_num=self.current_retrain_number)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=None)
        print("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num))
        dc.init_collector_dir(retrain_num, batch_num)
        self.training_phase(project_id, retrain_num=self.current_retrain_number, dc_instance=dc)
        
        self.merge_phase(base_name=self.base_name, retrain_num=retrain_num)
        dc.data_porter(path_config=self.path_config, base_name=self.base_name,
                       retrain_num=retrain_num, batch_num=batch_num, mode="Merge")
        
        cnxn1 = DB_Connection()
        cursor1 = cnxn1.cursor()
        cursor1.execute("UPDATE SVM_PROJECT SET STATUS = ? WHERE PROJECT_ID = ?", "CREATED_OK", int(project_id))
        cnxn1.commit() 
        return None

if __name__ == '__main__':   
    
    def timer(n):
        while True:

            SVM_PP_MODEL = select_project_by_status("CREATED")
            
            if len(SVM_PP_MODEL) != 0:
                
                pool=mp.Pool(16)
                
                for a in range(len(SVM_PP_MODEL)):
                
                    SVM = SuperVM(str(SVM_PP_MODEL.PROJECT_NAME[a]), str(SVM_PP_MODEL.UPLOAD_FILE[a]), r"Config.json")
                    project_id = SVM_PP_MODEL.PROJECT_ID[a]
                    update_project_STATUS_by_projectid("CREATEING", project_id)
                    
                    if (str(SVM_PP_MODEL.MODEL_TYPE[a]) == '1') or (str(SVM_PP_MODEL.MODEL_TYPE[a]) == '2'):
                        
                        pool.apply_async(SVM.one_to_all, args=(project_id,))                                  
                       
                    elif str(SVM_PP_MODEL.MODEL_TYPE[a]) == '3':
    
                        pool.apply_async(SVM.many_to_many, args=(project_id,))
                
                pool.close()
                pool.join()
            else:
                print("No train model!!!")
            
            time.sleep(n)
    timer(10)    
