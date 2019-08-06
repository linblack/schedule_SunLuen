#20190806

#AI365_test_v2.py
# -*- coding: utf-8 -*-
import json
import requests
import pandas as pd
import datetime
import time

from DB_operation import select_project_AI365, select_project_model_by_projectid_status, select_project_config_by_modelid_1ST_2ND_3RD

def timer(n):
    while True:
        serverIp = "10.88.26.112"
        serverPort = "8080"
        trxCat = "modelstatus"
        connTimeOut = 60
        waitTimeOut = 60
        model_NG = 0
        
        SVM_PROJECT_365 = select_project_AI365() #20190717 use o2m temporary, update all in the future
        
        if len(SVM_PROJECT_365) != 0:
            
            for a in range(len(SVM_PROJECT_365)):
                
                #fab = "L7B"
                fab = SVM_PROJECT_365.FAB[a]       
                #projectID = "L7B000181" #20190717 update [APC].[dbo].[SVM_PROJECT].[AI365_PROJECT_ID] in the future
                projectID = SVM_PROJECT_365.AI365_PROJECT_NAME[a]
                
                SVM_PROJECT_MODEL_365 = select_project_model_by_projectid_status(SVM_PROJECT_365.PROJECT_ID[a], "ONLINE")
                
                if len(SVM_PROJECT_MODEL_365) != 0:
                    
                    model_count = 1
                    mae_mape_sum = 0.0
                    
                    for b in range(len(SVM_PROJECT_MODEL_365)):
                        
                        SVM_PROJECT_CONFIG_LOSSFUNCTION = select_project_config_by_modelid_1ST_2ND_3RD(SVM_PROJECT_MODEL_365.MODEL_ID[b], "MODEL_SELECTION", "VALUE", "LOSS_FUNCTION")
                        SVM_PROJECT_CONFIG_THRESHOLD = select_project_config_by_modelid_1ST_2ND_3RD(SVM_PROJECT_MODEL_365.MODEL_ID[b], "MODEL_SELECTION", "VALUE", "THRESHOLD")
                        
                        if SVM_PROJECT_CONFIG_LOSSFUNCTION.PARAM_VALUE[0].upper() == 'MAE':
                            mae_mape_sum = mae_mape_sum + SVM_PROJECT_MODEL_365.MAE[b]
                        elif SVM_PROJECT_CONFIG_LOSSFUNCTION.PARAM_VALUE[0].upper() == 'MAPE':
                            mae_mape_sum = mae_mape_sum + SVM_PROJECT_MODEL_365.MAPE[b]
                        
                        if model_count == len(SVM_PROJECT_MODEL_365):
                            
                            mae_mape_avg = mae_mape_sum / len(SVM_PROJECT_MODEL_365)
                            
                            if mae_mape_avg < float(SVM_PROJECT_CONFIG_THRESHOLD.PARAM_VALUE[0]):
                                kpi_operator = 'LT'
                            else:
                                kpi_operator = 'GE'
                                model_NG = model_NG + 1
                            
                            mae_mape_avg_str = str(mae_mape_avg) #20190725 kpi_value length must 10 or less
                            
                            PROJECT_TITLE = SVM_PROJECT_365.PROJECT_TITLE[a][0:99] #20190802 model_name length must 100 or less
        
                            # ------------------- Update Model Health ----------------------#
                            df_Model = pd.DataFrame (
                                                   {
                                                       'model_name' : [PROJECT_TITLE],  #[APC].[dbo].[SVM_PROJECT_MODEL].[MODEL_TITLE]  
                                                       'model_version' : ['1' ]     #[APC].[dbo].[SVM_PROJECT_MODEL].[MODEL_SEQ]  
                                                   }
                                          )
                            
                            df_ModelKpi = pd.DataFrame (
                                                   {
                                                       'model_name' : [PROJECT_TITLE],                              
                                                       'kpi_value' : [mae_mape_avg_str[0:10]],
                                                       'kpi_spec' : [SVM_PROJECT_CONFIG_THRESHOLD.PARAM_VALUE[0]],
                                                       'kpi_name' : [SVM_PROJECT_CONFIG_LOSSFUNCTION.PARAM_VALUE[0].upper()],
                                                       'kpi_operator' : [kpi_operator] # >= , >= , > value=9 spec=10 operator=GE >= alarm , value=11 spec=10 operator=GE >= OK
                                                   }
                                           )
                            #kpi的數值與規格的運算子(望大or望小)
                            # > : GT (大於 Great Than)
                            # < : LT (小於 Less Than)
                            # ≧ : GE (大於等於 Great Equal )
                            # ≦ : LE (小於等於 Less Equal)
                            
                            mData = json.loads('{"fab" : "'+ fab + '", "project_id" : "'+ projectID + '", "model_infos" : []}')
                            
                            for index, row in df_Model.iterrows():
                                print(row['model_name'])
                                mData['model_infos'].append (
                                        {
                                            "model_name": row['model_name'] , "model_version": row['model_version'],
                                            "kpi_infos":[]
                                        }
                                )
                                
                                df_Kpi = df_ModelKpi[(df_ModelKpi.model_name == row['model_name'] )]    
                                print("df_Kpi.shape=", df_Kpi.shape)
                                for indexKpi, rowKpi in df_Kpi.iterrows():
                                    idx = len(mData['model_infos']) - 1
                                    mData['model_infos'][idx]['kpi_infos'].append (
                                            {
                                                 "kpi_value": rowKpi['kpi_value'] , "kpi_spec": rowKpi['kpi_spec'],  
                                                 "kpi_name": rowKpi['kpi_name'] , "kpi_operator": rowKpi['kpi_operator']
                                            }
                                        )
                            
                            print(json.dumps(mData))
                            
                                              
                            print(mData['model_infos'][0]['model_name'])
                            rptData = json.dumps(mData) # object to string
                            url = 'http://' + serverIp + ":" + serverPort + "/" + trxCat + "/"
                            print("url=",url)
                            headers = {'Content-Type': 'application/json'}
                            response = requests.post(url, data=rptData, headers=headers,timeout=(connTimeOut, waitTimeOut))
                            print(datetime.datetime.now(),"Status=", response.status_code, "response=" , response.text)   #{"rtn_code": "00000", "rtn_msg": "Successfully!“}
                            
                        model_count = model_count + 1
                        
        # ------------------- Update Project Health ----------------------#
        if (model_NG/len(SVM_PROJECT_365)) >= 0.5:
            aiStatus = "ERR"
        else:
            aiStatus = "RUN"
        #aiStatus = "RUN" # RUN , ERR
        statusExpireTime = "720" # minutes
        trxCat = "servicestatus"
        pData = {
                 'fab':fab,                                                   
                 'project_id': projectID,
                 'ai_status': aiStatus,
                 'status_expire_time': statusExpireTime
                }
        
        rptData = json.dumps(pData)
        url = 'http://' + serverIp + ":" + serverPort + "/" + trxCat + "/"
        print("rptData=",rptData)
        print("url=",url)
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, data=rptData, headers=headers,timeout=(connTimeOut, waitTimeOut))
        print(datetime.datetime.now(),"Status=", response.status_code, "response=" , response.text)    #{"rtn_code": "00000", "rtn_msg": "Successfully!“}

        time.sleep(n)

timer(43000) #20190717 0.5D-200s=>43000
#------------------------------------------------------------

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
#--------------------------------------------------------------        
  
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
        self.init_logger()
        
    def setup_logger(self, name, log_path, level):
        """Function setup as many loggers as you want"""
        logger = logging.getLogger(name)
        logging.addLevelName(25, "ONLINE")
        logger.setLevel(level)
        if logger.handlers:
            logger.handlers = []
        
        streamhandler = logging.StreamHandler()
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

    def online(self, msg):
        self.log.log(logging.getLevelName("ONLINE"), msg)

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
        self.log.warning(msg, exc_info=True)
        
    def error_trace(self, msg):
        self.log.error(msg, exc_info=True)
        self.err_log.error(msg, exc_info=True)

    def critical_trace(self, msg):
        self.log.critical(msg, exc_info=True)
        self.err_log.critical(msg, exc_info=True)
        
    @staticmethod
    def init_log(normal_log_path, error_log_path):
        return WriteLog(normal_log_path, error_log_path).init_logger()
        
if __name__ == "__main__":
    mylog = WriteLog("./log.log", "./error.log")
    mylog.online("Holy Shit")

#----------------------------------------------------------------------
#Data_Check.py
from CreateLog import WriteLog
import pandas as pd
from config import read_config
import os

def Data_Check(folder_path):
    input_path = read_path(folder_path)
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    mylog.info("-----Data Check-----")

    df_new = read_csv(input_path["raw_path"], "Data Col Check(new_path)", mylog)
    df_old = read_csv(input_path["old_path"], "Data Col Check(old_path)", mylog)

    config = read_config(input_path["config_path"], mylog)
    y_name = config["Y"][0]
    try:
        df = Y_col_impute(df_new, df_old, y_name, mylog)
        if df.values.shape[1] != 0:
            df.to_csv(input_path["raw_path"], index=False)
            df_new = df.copy()
    except Exception as e:
        mylog.error("Error while doing Y col imputation")
        mylog.error(e)
        return "NG", "Please contact APC team to solve the problem."
    status, msg = row_check(config, df_new, folder_path, mylog)
    mylog.info("-----Data Check Done-----")
    return status, msg


def Y_col_impute(df_new, df_old, y_name, mylog):
    if y_name not in df_new.columns:
        df_new[y_name] = None
    idx, df = Data_Col_Check(df_new, df_old, mylog)
    if not idx:
        if df.values.shape[1] == 0:
            raise SystemError("Data comparison fails.")
        else:
            return df
    else:
        return pd.DataFrame()


def Data_Col_Check(df_new, df_old, mylog):
    df_new_list = df_new.columns.tolist()
    df_old_list = df_old.columns.tolist()
    mylog.online("-----Data Col Check------")
    if df_new_list == df_old_list:
        mylog.online("-----Data Col Check Passed------")
        return True, pd.DataFrame()
    else:
        diff_set_less = set(df_old_list) - set(df_new_list)
        diff_set_more = set(df_new_list) - set(df_old_list)
        if diff_set_less:
            if diff_set_more:
                show_deficiency_message(diff_set_less, mylog)
                show_redundancy_message(diff_set_more, mylog)
                mylog.online("-----Data Col Check Done------")
                return False, pd.DataFrame()
            else:
                show_deficiency_message(diff_set_less, mylog)
                mylog.online("-----Data Col Check Done------")
                return False, pd.DataFrame()
        else:
            if diff_set_more:
                show_redundancy_message(diff_set_more, mylog)
                mylog.online("-----Data Col Check Done------")
                return False, pd.DataFrame()
            else:
                df = df_new[df_old_list].copy()
                mylog.warning("Feature order is changed by system.")
                mylog.online("-----Data Col Check Done------")
                return False, df


def show_deficiency_message(diff_set, mylog):
    mylog.error("New data misses something.")
    mylog.error("Deficiency: " + ",".join(str(x) for x in diff_set))
    return None


def show_redundancy_message(diff_set, mylog):
    mylog.error("New data has something more.")
    mylog.error("Redundancy: " + ",".join(str(x) for x in diff_set))
    return None


def read_csv(path, program_name, mylog):
    try:
        df_raw = pd.read_csv(path, encoding="utf-8")
    except Exception as e:
        mylog.warning("Fail to read in utf-8")
        try:
            df_raw = pd.read_csv(path, encoding="big5")
        except Exception as e:
            mylog.warning("Fail to read in big5")
            try:
                df_raw = pd.read_csv(path, encoding="big5hkscs")
            except Exception as e:
                mylog.warning("Fail to read in big5hkscs")
                df_raw = None
                mylog.error(program_name+" : Read data error")
                mylog.error_trace(e)
    return df_raw


def row_check(config, df, folder_path, mylog):
    mylog.online("-----Data Row Check------")
    col_num = len(df.columns)
    if col_num == 0:
        mylog.warning("No column in the dataframe.")
        return None

    try:
        missing_T = config["row_missing_T"]
        ex_cols = config["Index_Columns"]
    except Exception as e:
        mylog.error("Error while reading config parameter.")
        mylog.error_trace(e)
        raise KeyError

    try:
        columns = ex_cols.copy()
        columns.extend(["Index", "Missing_Rate"])
        idx_table = pd.DataFrame(columns=columns)
        df = df.copy()
        for idx, row in df.iterrows():
            miss_num = row.isnull().sum()
            rate = miss_num/col_num
            print(rate)
            if rate > missing_T:
                mylog.warning("Row index "+str(idx)+" has high missing rate " + "{0:.2f}".format(rate) +
                              " ("+str(int(miss_num))+" missed)")
                key = df.loc[idx, ex_cols].tolist()
                key.extend([idx, rate])
                idx_table.loc[len(idx_table)] = key

        tmp_path = os.path.join(folder_path, "missing_rate_table.csv")
        idx_table.to_csv(tmp_path, index=False)
    except Exception as e:
        mylog.error("Error while doing row check")
        mylog.error(e)
        return "NG", "Please contact APC team to solve the problem."
    mylog.info("-----Data Row Done------")
    return "OK", None



if __name__ == "__main__":
    from Read_path import read_path
    new_path = "/home/davidswwang/PycharmProjects/01_AVM/Code/Cases/New_path_test/New_path_test_00/01/02_DataReview/new.csv"
    old_path = "/home/davidswwang/PycharmProjects/01_AVM/Code/Cases/New_path_test/New_path_test_00/01/02_DataReview/old.csv"
    folder_path = "/home/davidswwang/PycharmProjects/01_AVM/Code/Cases/New_path_test/New_path_test_00/01/02_DataReview/"

    input_path = read_path(folder_path)
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    mylog.init_logger()

    df_new = read_csv(new_path, "Data Col Check(new_path)", mylog)
    df_old = read_csv(old_path, "Data Col Check(old_path)", mylog)

    y_name = "YY"

    # df = Y_col_impute(df_new, df_old, y_name, mylog)
    # print(df)
    df = pd.DataFrame({
        "A":[1, 3, 10, None, None],
        "B":[None,2, 20, None, 9],
        "C":[None,6, 60, None, 8],
        "KK":["A1", "A2", "A3", "A4", "A5"]
    })
    config = {}
    config["row_missing_T"] = 0.5
    config["Index_Columns"] = ["KK"]
    row_check(config , df, folder_path, mylog)
    
#-----------------------------------------------------------------------------------------------------
#Data_Collector.py
import os
import json
from shutil import copyfile
import pandas as pd
import traceback
class Data_Collector:
    def __init__(self, base_path, train_path, config_path):
        self.base_path = base_path
        self.train_path = train_path
        self.config_path = config_path
        self.collector_path = os.path.join(base_path, "Data_Collector")

        self.DC_path = {}
        self.DC_path["DataPreview"] = {}
        self.DC_path["DataPreview"]["main"] = os.path.join(self.collector_path, "02_DataPreview")
        self.DC_path["XDI"] = {}
        self.DC_path["XDI"]["main"] = os.path.join(self.collector_path, "04_XDI")
        self.DC_path["YDI"] = {}
        self.DC_path["YDI"]["main"] = os.path.join(self.collector_path, "05_YDI")
        self.DC_path["Model"] = {}
        self.DC_path["Model"]["main"] = os.path.join(self.collector_path, "06_Model")
        self.DC_path["SelectModel"] = {}
        self.DC_path["SelectModel"]["main"] = os.path.join(self.collector_path, "07_SelectModel")
        self.DC_path["CI"] = {}
        self.DC_path["CI"]["main"] = os.path.join(self.collector_path, "08_CI")
        self.retrain_digit = str(4)
        self.retrain_format = "{0:0" + self.retrain_digit + "d}"

        if not os.path.exists(self.DC_path["DataPreview"]["main"]):
            os.makedirs(self.DC_path["DataPreview"]["main"])
        data_preview_dir_list = os.listdir(self.DC_path["DataPreview"]["main"])
        data_preview_dir_list = [int(x) for x in data_preview_dir_list]
        if data_preview_dir_list:
            current_retrain_num = max(data_preview_dir_list)
            next_retrain_num = current_retrain_num + 1
            if not os.path.exists(os.path.join(self.DC_path["DataPreview"]["main"], str(next_retrain_num))):
                self.data_preview_path = os.path.join(self.DC_path["DataPreview"]["main"],
                                                      self.retrain_format.format(next_retrain_num),
                                                      "00", "00_Training_Report")
                os.makedirs(self.data_preview_path)

        else:
            self.data_preview_path = os.path.join(self.DC_path["DataPreview"]["main"],
                                                  "0000", "00", "00_Training_Report")
            os.makedirs(self.data_preview_path)

        try:
            with open(self.config_path) as json_data:
                config = json.load(json_data)
        except Exception as e:
            config = None
            print("--------------")
            print(self.config_path)
            print("--------------")
            error_path = os.path.join(self.base_path, "error.log")
            with open(error_path, 'a') as file:
                file.write(self.config_path)
                file.write("Error while reading config file\n")
                file.write(str(e) + "\n")
                file.write(traceback.format_exc())
                raise FileNotFoundError
        self.model_pred_name = config["Model_Pred_Name"]

        # Other Variables initialized in methods
        self.feature_lists = None

    def init_collector_dir(self, retrain_num, batch_num):
        if batch_num == 0:
            sub_list = ["00_Training_Report", "01_Merge_Report"]
        else:
            sub_list = ["00_Batch_Report"]

        for super_path in ["DataPreview", "XDI", "YDI", "Model", "SelectModel", "CI"]:
            if retrain_num not in self.DC_path[super_path]:
                self.DC_path[super_path][retrain_num] = {}
                self.DC_path[super_path][retrain_num]["main"] = \
                    os.path.join(self.DC_path[super_path]["main"], self.retrain_format.format(retrain_num))
                self.DC_path[super_path][retrain_num][batch_num] = {}
                self.DC_path[super_path][retrain_num][batch_num]["main"] = \
                    os.path.join(self.DC_path[super_path][retrain_num]["main"], "{0:02d}".format(batch_num))
            else:
                if batch_num not in self.DC_path[super_path][retrain_num]:
                    self.DC_path[super_path][retrain_num][batch_num] = {}
                    self.DC_path[super_path][retrain_num][batch_num]["main"] = \
                        os.path.join(self.DC_path[super_path][retrain_num]["main"], "{0:02d}".format(batch_num))
                else:
                    print("Path exists: "+self.DC_path[super_path][retrain_num][batch_num])
                    continue

            omg_path = self.DC_path[super_path][retrain_num][batch_num]["main"]
            for path in sub_list:
                if super_path == "Model":
                    continue
                self.DC_path[super_path][retrain_num][batch_num][path] = os.path.join(omg_path, path)
                if not os.path.exists(self.DC_path[super_path][retrain_num][batch_num][path]):
                    os.makedirs(self.DC_path[super_path][retrain_num][batch_num][path])

        for model_name in self.model_pred_name:
            self.DC_path["Model"][retrain_num][batch_num][model_name] = {}
            self.DC_path["Model"][retrain_num][batch_num][model_name]["main"] = \
                os.path.join(self.DC_path["Model"][retrain_num][batch_num]["main"], model_name)
            for path in sub_list:
                self.DC_path["Model"][retrain_num][batch_num][model_name][path] = \
                    os.path.join(self.DC_path["Model"][retrain_num][batch_num][model_name]["main"], path)
                if not os.path.exists(self.DC_path["Model"][retrain_num][batch_num][model_name][path]):
                    os.makedirs(self.DC_path["Model"][retrain_num][batch_num][model_name][path])

        # for super_path in ["DataPreview"]:
        #     if not os.path.exists(self.DC_path[super_path]["main"]):
        #         os.makedirs(self.DC_path[super_path]["main"])
        return None

    def create_file_path(self):
        file_path = dict()
        file_path["config_path"] = self.config_path
        file_path["error_path"] = os.path.join(self.data_preview_path, "error.log")
        file_path["log_path"] = os.path.join(self.data_preview_path, "System.log")
        file_path["raw_path"] = self.train_path
        with open(os.path.join(self.data_preview_path, "file_path.json"), 'w') as fp:
            json.dump(file_path, fp, indent=4, sort_keys=True)
        return None

    def allocation(self, filter_feature_dict, filter_dir_path, retrain_num, batch_num):
        for key, value in filter_feature_dict.items():
            base_name = os.path.basename(os.path.abspath(filter_dir_path[key]))
            raw_data_name = os.path.basename(os.path.abspath(filter_feature_dict[key]))
            target_path = os.path.join(filter_dir_path[key],
                                       base_name+"_"+self.retrain_format.format(retrain_num),
                                       "{0:02d}".format(batch_num), "01_OriginalData", raw_data_name)
            os.rename(value, target_path)
        return None

    def XDI_collector(self, path_config, filter_dir_name, retrain_num, batch_num, feature_lists=None, merge_flag=None):
        if feature_lists is None:
            if not hasattr(self, "feature_lists"):
                raise KeyError("feature_lists not found ")
        else:
            self.feature_lists = feature_lists
        if merge_flag is None:
            source_flag = '1'
            target_flag = "00_Training_Report"
        else:
            source_flag = '3'
            target_flag = "01_Merge_Report"

        XDI_offline = pd.DataFrame()
        XDI_contribution = pd.DataFrame()
        for feature in self.feature_lists:
            path_dict = path_config[filter_dir_name[feature]][str(retrain_num)][str(batch_num)]
            source = os.path.join(path_dict['4'][source_flag], "XDI_offline.csv")
            if not os.path.exists(source):
                print("XDI_collector: File doesn't exist: " + source)
                continue
            else:
                tmp_data = pd.read_csv(source)
                XDI_offline = pd.concat([XDI_offline, tmp_data], ignore_index=True)

            alarm_report_path = os.path.join(path_dict['4'][source_flag], "XDI_Alarm_Report")
            alarm_reports = os.listdir(alarm_report_path)
            alarm_reports = [x for x in alarm_reports if ".csv" in x]
            for alarm_report in alarm_reports:
                source = os.path.join(alarm_report_path, alarm_report)
                if not os.path.exists(source):
                    print("XDI_collector: File doesn't exist: " + source)
                    continue
                else:
                    tmp_data = pd.read_csv(source)
                    XDI_contribution = pd.concat([XDI_contribution, tmp_data], ignore_index=True)
                    # target = os.path.join(self.DC_path["XDI"][retrain_num][batch_num][target_flag], alarm_report)
                    # copyfile(source, target)

        target = os.path.join(self.DC_path["XDI"][retrain_num][batch_num][target_flag], "XDI_offline.csv")
        # Add col Num
        # XDI_offline = XDI_offline.rename_axis('Num').reset_index(drop=False)
        # left_col = XDI_offline.columns[1:].tolist()
        # XDI_offline = XDI_offline[left_col+[XDI_offline.columns[0]]]
        XDI_offline["Num"] = list(range(len(XDI_offline.index)))
        XDI_offline.to_csv(target, index=False)
        target = os.path.join(self.DC_path["XDI"][retrain_num][batch_num][target_flag], "Contribution.csv")
        XDI_contribution.to_csv(target, index=False)
        return None

    def YDI_collector(self, path_config, filter_dir_name, retrain_num, batch_num, feature_lists=None, merge_flag=None):
        if feature_lists is None:
            if not hasattr(self, "feature_lists"):
                raise KeyError("feature_lists not found ")
        else:
            self.feature_lists = feature_lists
        if merge_flag is None:
            source_flag = '1'
            target_flag = "00_Training_Report"
        else:
            source_flag = '3'
            target_flag = "01_Merge_Report"

        YDI_offline = pd.DataFrame()
        for feature in self.feature_lists:
            path_dict = path_config[filter_dir_name[feature]][str(retrain_num)][str(batch_num)]
            source = os.path.join(path_dict['5'][source_flag], "YDI_Offline_Report.csv")
            if not os.path.exists(source):
                print("YDI_collector: File doesn't exist: " + source)
                continue
            else:
                tmp_data = pd.read_csv(source)
                YDI_offline = pd.concat([YDI_offline, tmp_data], ignore_index=True)

        target = os.path.join(self.DC_path["YDI"][retrain_num][batch_num][target_flag], "YDI_Offline_Report.csv")
        YDI_offline["Num"] = list(range(len(YDI_offline.index)))
        YDI_offline.to_csv(target, index=False)
        return None

    def Model_collector(self, path_config, filter_dir_name, retrain_num, batch_num,
                        feature_name, feature_lists=None, merge_flag=None):
        if feature_lists is None:
            if not hasattr(self, "feature_lists"):
                raise KeyError("feature_lists not found ")
        else:
            self.feature_lists = feature_lists
        if merge_flag is None:
            source_flag = '2'
            feature_flag = '1'
            target_flag = "00_Training_Report"
            outputfiles = ["trainPredResult.csv", "testPredResult.csv"]
        else:
            source_flag = '5'
            feature_flag = '4'
            target_flag = "01_Merge_Report"
            outputfiles = ["trainPredResult.csv"]

        for file in outputfiles:
            Result = pd.DataFrame()
            for model_name in self.model_pred_name:
                for feature in self.feature_lists:
                    path_dict = path_config[filter_dir_name[feature]][str(retrain_num)][str(batch_num)]
                    source = os.path.join(path_dict['6'][model_name][source_flag], file)
                    if not os.path.exists(source):
                        print("Model_collector: File doesn't exist: " + source)
                        continue
                    else:
                        tmp_data = pd.read_csv(source)
                        Result = pd.concat([Result, tmp_data], ignore_index=True)

                target = os.path.join(self.DC_path["Model"][retrain_num][batch_num][model_name][target_flag], file)
                Result.to_csv(target, index=False)

        for model_name in self.model_pred_name:
            for feature in self.feature_lists:
                path_dict = path_config[filter_dir_name[feature]][str(retrain_num)][str(batch_num)]
                source = os.path.join(path_dict['6'][model_name][feature_flag], "FeatureScore.csv")
                if not os.path.exists(source):
                    print("Model_collector: File doesn't exist: " + source)
                    continue
                else:
                    source_file = pd.read_csv(source)
                    source_file["Filter_Feature"] = str(feature)
                    ###
                    # target = os.path.join(self.DC_path["Model"][retrain_num][batch_num][model_name][target_flag],
                    #                       "FeatureScore" + "_" + str(feature_name) + "_" + str(feature) + ".csv")
                    ###
                    target = os.path.join(self.DC_path["Model"][retrain_num][batch_num][model_name][target_flag],
                                          "FeatureScore.csv")
                    source_file.to_csv(target, index=False)
        return None

    def Selection_collector(self, path_config, filter_dir_name, retrain_num, batch_num,
                            feature_lists=None, merge_flag=None):
        if feature_lists is None:
            if not hasattr(self, "feature_lists"):
                raise KeyError("feature_lists not found ")
        else:
            self.feature_lists = feature_lists

        if merge_flag is None:
            source_flag = '0'
            target_flag = "00_Training_Report"
        else:
            source_flag = '1'
            target_flag = "01_Merge_Report"

        y_pred_report = pd.DataFrame()
        model_select_report = pd.DataFrame()
        for feature in self.feature_lists:
            path_dict = path_config[filter_dir_name[feature]][str(retrain_num)][str(batch_num)]
            source = os.path.join(path_dict['7'][source_flag], "y_pred_report.csv")
            if not os.path.exists(source):
                print("Selection_collector: File doesn't exist: " + source)
                continue
            else:
                tmp_data = pd.read_csv(source)
                y_pred_report = pd.concat([y_pred_report, tmp_data], ignore_index=True)

            source = os.path.join(path_dict['7'][source_flag], "Model_Selection_Report.csv")
            if not os.path.exists(source):
                print("Selection_collector: File doesn't exist: " + source)
                continue
            else:
                tmp_data = pd.read_csv(source)
                tmp_data["Filter_Feature"] = str(feature)
                model_select_report = pd.concat([model_select_report, tmp_data], ignore_index=True)

        target = os.path.join(self.DC_path["SelectModel"][retrain_num][batch_num][target_flag], "y_pred_report.csv")
        y_pred_report["Num"] = list(range(len(y_pred_report.index)))
        y_pred_report.to_csv(target, index=False)
        target = os.path.join(self.DC_path["SelectModel"][retrain_num][batch_num][target_flag],
                              "Model_Selection_Report.csv")
        model_select_report["Num"] = list(range(len(model_select_report.index)))
        model_select_report.to_csv(target, index=False)
        return None

    def CI_collector(self, path_config, filter_dir_name, retrain_num, batch_num, feature_lists=None, merge_flag=None):
        if feature_lists is None:
            if not hasattr(self, "feature_lists"):
                raise KeyError("feature_lists not found ")
        else:
            self.feature_lists = feature_lists
        if merge_flag is None:
            source_flag = '1'
            target_flag = "00_Training_Report"
        else:
            source_flag = '3'
            target_flag = "01_Merge_Report"

        CI_offline = pd.DataFrame()
        for feature in self.feature_lists:
            path_dict = path_config[filter_dir_name[feature]][str(retrain_num)][str(batch_num)]
            source = os.path.join(path_dict['8'][source_flag], "MXCI_MYCI_offline.csv")
            if not os.path.exists(source):
                print("CI_collector: File doesn't exist: " + source)
                continue
            else:
                tmp_data = pd.read_csv(source)
                CI_offline = pd.concat([CI_offline, tmp_data], ignore_index=True)

        target = os.path.join(self.DC_path["CI"][retrain_num][batch_num][target_flag], "MXCI_MYCI_offline.csv")
        CI_offline["Num"] = list(range(len(CI_offline.index)))
        CI_offline.to_csv(target, index=False)
        return None

    def one_to_all_collector(self, path_config, base_name, retrain_num, batch_num, situation,
                             feature_name=None, feature=None):
        source_flag = '1'
        select_flag = '0'
        target_flag = "00_Training_Report"

        path_dict = path_config[base_name][str(retrain_num)][str(batch_num)]

        if situation == "DataPreview":
            file_list = os.listdir(path_dict['2']["main"])
            file_list = [x for x in file_list if ".csv" in x]
            for file in file_list:
                source = os.path.join(path_dict['2']["main"], file)
                if not os.path.exists(source):
                    print("File doesn't exist: " + source)
                    continue
                else:
                    target = os.path.join(self.DC_path["DataPreview"][retrain_num][batch_num][target_flag], file)
                    copyfile(source, target)
        elif situation == "XDI":
            source = os.path.join(path_dict['4'][source_flag], "XDI_offline.csv")
            if not os.path.exists(source):
                print("File doesn't exist: " + source)
            else:
                if feature_name:
                    target = os.path.join(self.DC_path["XDI"][retrain_num][batch_num][target_flag],
                                          "XDI_offline" + "_" + str(feature_name) + "_" + str(feature) + ".csv")
                    copyfile(source, target)
                else:
                    target = os.path.join(self.DC_path["XDI"][retrain_num][batch_num][target_flag], "XDI_offline.csv")
                    df = pd.read_csv(source)
                    df["Num"] = list(range(len(df.index)))
                    df.to_csv(target, index=False)
                # copyfile(source, target)

            alarm_report_path = os.path.join(path_dict['4'][source_flag], "XDI_Alarm_Report")
            alarm_reports = os.listdir(alarm_report_path)
            alarm_reports = [x for x in alarm_reports if ".csv" in x]
            XDI_contribution = pd.DataFrame()
            for alarm_report in alarm_reports:
                source = os.path.join(alarm_report_path, alarm_report)
                if not os.path.exists(source):
                    print("File doesn't exist: " + source)
                else:
                    tmp_data = pd.read_csv(source)
                    XDI_contribution = pd.concat([XDI_contribution, tmp_data], ignore_index=True)
            if feature_name:
                target = os.path.join(self.DC_path["XDI"][retrain_num][batch_num][target_flag],
                                      "Contribution" + "_" + str(feature_name) + "_" + str(feature) + ".csv")
            else:
                target = os.path.join(self.DC_path["XDI"][retrain_num][batch_num][target_flag], "Contribution.csv")
            XDI_contribution.to_csv(target, index=False)

        elif situation == "YDI":
            source = os.path.join(path_dict['5'][source_flag], "YDI_Offline_Report.csv")
            if not os.path.exists(source):
                print("File doesn't exist: " + source)
            else:
                if feature_name:
                    target = os.path.join(self.DC_path["YDI"][retrain_num][batch_num][target_flag],
                                          "YDI_Offline_Report" + "_" + str(feature_name) + "_" + str(feature) + ".csv")
                    copyfile(source, target)
                else:
                    target = os.path.join(self.DC_path["YDI"][retrain_num][batch_num][target_flag],
                                          "YDI_Offline_Report.csv")
                    df = pd.read_csv(source)
                    df["Num"] = list(range(len(df.index)))
                    df.to_csv(target, index=False)
                # copyfile(source, target)

        elif situation == "SelectModel":
            file_list = os.listdir(path_dict['7'][select_flag])
            file_list = [x for x in file_list if ".csv" in x]
            for file in file_list:
                source = os.path.join(path_dict['7'][select_flag], file)
                if not os.path.exists(source):
                    print("File doesn't exist: " + source)
                else:
                    if feature_name:
                        ext = file[-4:]
                        dir_name = file[:-4]
                        if dir_name == "y_pred_merge_test" or dir_name == "y_pred_merge_train":
                            continue
                        target = os.path.join(self.DC_path["SelectModel"][retrain_num][batch_num][target_flag],
                                              dir_name + "_" + str(feature_name) + "_" + str(feature) + ext)
                        copyfile(source, target)
                    else:
                        target = os.path.join(self.DC_path["SelectModel"][retrain_num][batch_num][target_flag], file)
                        df = pd.read_csv(source)
                        df["Num"] = list(range(len(df.index)))
                        df.to_csv(target, index=False)
                    # copyfile(source, target)

        elif situation == "CI":
            source = os.path.join(path_dict['8'][source_flag], "MXCI_MYCI_offline.csv")
            if not os.path.exists(source):
                print("File doesn't exist: " + source)
            else:
                if feature_name:
                    target = os.path.join(self.DC_path["CI"][retrain_num][batch_num][target_flag],
                                          "MXCI_MYCI_offline" + "_" + str(feature_name) + "_" + str(feature) + ".csv")
                    copyfile(source, target)
                else:
                    target = os.path.join(self.DC_path["CI"][retrain_num][batch_num][target_flag],
                                          "MXCI_MYCI_offline.csv")
                    df = pd.read_csv(source)
                    df["Num"] = list(range(len(df.index)))
                    df.to_csv(target, index=False)
                # copyfile(source, target)

        elif situation == "Model":
            source_flag = '2'
            feature_flag = '1'
            target_flag = "00_Training_Report"
            outputfiles = ["trainPredResult.csv", "testPredResult.csv"]

            for file in outputfiles:
                for model_name in self.model_pred_name:
                    source = os.path.join(path_dict['6'][model_name][source_flag], file)
                    if not os.path.exists(source):
                        print("File doesn't exist: " + source)
                        continue
                    if feature_name:
                        ext = file[-4:]
                        dir_name = file[:-4]
                        target = os.path.join(self.DC_path["Model"][retrain_num][batch_num][model_name][target_flag],
                                              dir_name + "_" + str(feature_name) + "_" + str(feature) + ext)
                    else:
                        target = os.path.join(self.DC_path["Model"][retrain_num][batch_num][model_name][target_flag], file)
                    copyfile(source, target)

            for model_name in self.model_pred_name:
                source = os.path.join(path_dict['6'][model_name][feature_flag], "FeatureScore.csv")
                if not os.path.exists(source):
                    print("Model_collector: File doesn't exist: " + source)
                    continue
                else:
                    if feature_name:
                        source_file = pd.read_csv(source)
                        source_file["Filter_Feature"] = str(feature)
                        target = os.path.join(self.DC_path["Model"][retrain_num][batch_num][model_name][target_flag],
                                              "FeatureScore" + "_" + str(feature_name) + "_" + str(feature) + ".csv")
                        source_file.to_csv(target, index=False)
                    else:
                        target = os.path.join(self.DC_path["Model"][retrain_num][batch_num][model_name][target_flag],
                                              "FeatureScore.csv")
                        copyfile(source, target)
        return None

    def data_porter(self, path_config, base_name, retrain_num, batch_num, mode, feature_name=None, feature=None):
        if mode == "Train":
            source_flag = '1'
            select_flag = '0'
            target_flag = "00_Training_Report"
        elif mode == "Merge":
            source_flag = '3'
            select_flag = '1'
            target_flag = "01_Merge_Report"
        elif mode == "Batch":
            source_flag = "main"
            select_flag = "main"
            target_flag = "00_Batch_Report"
        else:
            raise KeyError("Mode doesn't exist")

        path_dict = path_config[base_name][str(retrain_num)][str(batch_num)]

        if mode == "Train":
            file_list = os.listdir(path_dict['2']["main"])
            file_list = [x for x in file_list if ".csv" in x]
            for file in file_list:
                source = os.path.join(path_dict['2']["main"], file)
                if not os.path.exists(source):
                    print("File doesn't exist: " + source)
                    continue
                else:
                    target = os.path.join(self.DC_path["DataPreview"][retrain_num][batch_num][target_flag], file)
                    copyfile(source, target)

        source = os.path.join(path_dict['4'][source_flag], "XDI_offline.csv")
        if not os.path.exists(source):
            print("File doesn't exist: " + source)
        else:
            if feature_name:
                target = os.path.join(self.DC_path["XDI"][retrain_num][batch_num][target_flag],
                                      "XDI_offline" + "_" + str(feature_name) + "_" + str(feature) + ".csv")
                copyfile(source, target)
            else:
                target = os.path.join(self.DC_path["XDI"][retrain_num][batch_num][target_flag], "XDI_offline.csv")
                df = pd.read_csv(source)
                df["Num"] = list(range(len(df.index)))
                df.to_csv(target, index=False)
            # copyfile(source, target)

        alarm_report_path = os.path.join(path_dict['4'][source_flag], "XDI_Alarm_Report")
        alarm_reports = os.listdir(alarm_report_path)
        alarm_reports = [x for x in alarm_reports if ".csv" in x]
        XDI_contribution = pd.DataFrame()
        for alarm_report in alarm_reports:
            source = os.path.join(alarm_report_path, alarm_report)
            if not os.path.exists(source):
                print("File doesn't exist: " + source)
            else:
                tmp_data = pd.read_csv(source)
                XDI_contribution = pd.concat([XDI_contribution, tmp_data], ignore_index=True)
        #20190715 XDI_contribution is not None, and to csv
        if len(XDI_contribution) != 0:
            if feature_name:
                target = os.path.join(self.DC_path["XDI"][retrain_num][batch_num][target_flag],
                                      "Contribution" + "_" + str(feature_name) + "_" + str(feature) + ".csv")
            else:
                target = os.path.join(self.DC_path["XDI"][retrain_num][batch_num][target_flag], "Contribution.csv")
            XDI_contribution.to_csv(target, index=False)

        source = os.path.join(path_dict['5'][source_flag], "YDI_Offline_Report.csv")
        if not os.path.exists(source):
            print("File doesn't exist: " + source)
        else:
            if feature_name:
                target = os.path.join(self.DC_path["YDI"][retrain_num][batch_num][target_flag],
                                      "YDI_Offline_Report" + "_" + str(feature_name) + "_" + str(feature) + ".csv")
                copyfile(source, target)
            else:
                target = os.path.join(self.DC_path["YDI"][retrain_num][batch_num][target_flag],
                                      "YDI_Offline_Report.csv")
                df = pd.read_csv(source)
                df["Num"] = list(range(len(df.index)))
                df.to_csv(target, index=False)
            # copyfile(source, target)

        file_list = os.listdir(path_dict['7'][select_flag])
        file_list = [x for x in file_list if ".csv" in x]
        for file in file_list:
            source = os.path.join(path_dict['7'][select_flag], file)
            if not os.path.exists(source):
                print("File doesn't exist: " + source)
            else:
                if feature_name:
                    ext = file[-4:]
                    dir_name = file[:-4]
                    if dir_name == "y_pred_merge_test" or dir_name == "y_pred_merge_train":
                        continue
                    target = os.path.join(self.DC_path["SelectModel"][retrain_num][batch_num][target_flag],
                                          dir_name + "_" + str(feature_name) + "_" + str(feature) + ext)
                    copyfile(source, target)
                else:
                    target = os.path.join(self.DC_path["SelectModel"][retrain_num][batch_num][target_flag], file)
                    df = pd.read_csv(source)
                    df["Num"] = list(range(len(df.index)))
                    df.to_csv(target, index=False)
                # copyfile(source, target)

        source = os.path.join(path_dict['8'][source_flag], "MXCI_MYCI_offline.csv")
        if not os.path.exists(source):
            print("File doesn't exist: " + source)
        else:
            if feature_name:
                target = os.path.join(self.DC_path["CI"][retrain_num][batch_num][target_flag],
                                      "MXCI_MYCI_offline" + "_" + str(feature_name) + "_" + str(feature) + ".csv")
                copyfile(source, target)
            else:
                target = os.path.join(self.DC_path["CI"][retrain_num][batch_num][target_flag], "MXCI_MYCI_offline.csv")
                df = pd.read_csv(source)
                df["Num"] = list(range(len(df.index)))
                df.to_csv(target, index=False)
            # copyfile(source, target)

        # Model
        if mode == "Train":
            source_flag = '2'
            feature_flag = '1'
            target_flag = "00_Training_Report"
            outputfiles = ["trainPredResult.csv", "testPredResult.csv"]
        elif mode == "Merge":
            source_flag = '5'
            feature_flag = '4'
            target_flag = "01_Merge_Report"
            outputfiles = ["trainPredResult.csv"]
        elif mode == "Batch":
            source_flag = 'main'
            feature_flag = None
            target_flag = "00_Batch_Report"
            outputfiles = ["testPredResult.csv"]
        else:
            raise KeyError("Mode doesn't exist")

        for file in outputfiles:
            for model_name in self.model_pred_name:
                source = os.path.join(path_dict['6'][model_name][source_flag], file)
                if not os.path.exists(source):
                    print("File doesn't exist: " + source)
                    continue
                if feature_name:
                    ext = file[-4:]
                    dir_name = file[:-4]
                    target = os.path.join(self.DC_path["Model"][retrain_num][batch_num][model_name][target_flag],
                                          dir_name + "_" + str(feature_name) + "_" + str(feature) + ext)
                else:
                    target = os.path.join(self.DC_path["Model"][retrain_num][batch_num][model_name][target_flag], file)
                copyfile(source, target)

        if mode != "Batch":
            for model_name in self.model_pred_name:
                source = os.path.join(path_dict['6'][model_name][feature_flag], "FeatureScore.csv")
                if not os.path.exists(source):
                    print("Model_collector: File doesn't exist: " + source)
                    continue
                else:
                    if feature_name:
                        source_file = pd.read_csv(source)
                        source_file["Filter_Feature"] = str(feature)
                        ###
                        # target = os.path.join(self.DC_path["Model"][retrain_num][batch_num][model_name][target_flag],
                        #                       "FeatureScore" + "_" + str(feature_name) + "_" + str(feature) + ".csv")
                        ###
                        target = os.path.join(self.DC_path["Model"][retrain_num][batch_num][model_name][target_flag],
                                              "FeatureScore.csv")
                        source_file.to_csv(target, index=False)
                    else:
                        target = os.path.join(self.DC_path["Model"][retrain_num][batch_num][model_name][target_flag],
                                              "FeatureScore.csv")
                        copyfile(source, target)

        return None

    def data_organize(self, retrain_num, batch_num, mode, feature_name, feature_lists=None):
        if feature_lists is None:
            if not hasattr(self, "feature_lists"):
                raise KeyError("feature_lists not found ")
        else:
            self.feature_lists = feature_lists

        if mode == "Train":
            target_flag = "00_Training_Report"
        elif mode == "Merge":
            target_flag = "01_Merge_Report"
        elif mode == "Batch":
            target_flag = "00_Batch_Report"
        else:
            raise KeyError("Mode doesn't exist")

        pocket_dict = {}
        pocket_dict["XDI"] = ["XDI_offline", "Contribution"]
        pocket_dict["YDI"] = ["YDI_Offline_Report"]
        pocket_dict["CI"] = ["MXCI_MYCI_offline"]
        pocket_dict["SelectModel"] = ["Model_Selection_Report", "y_pred_report"]

        for key, pocket_names in pocket_dict.items():
            target_path = self.DC_path[key][retrain_num][batch_num][target_flag]
            for pocket_name in pocket_names:
                final_file = pd.DataFrame()
                for feature in self.feature_lists:
                    target = pocket_name + "_" + str(feature_name) + "_" + str(feature) + ".csv"
                    tmp_target = os.path.join(target_path, target)
                    if not os.path.exists(tmp_target):
                        print("Data Organize: File doesn't exist: " + tmp_target)
                        continue
                    tmp = pd.read_csv(tmp_target)
                    if pocket_name == "Model_Selection_Report":
                        tmp["Filter_Feature"] = str(feature)
                    final_file = pd.concat([final_file, tmp])
                    os.remove(os.path.join(target_path, target))

                final_file["Num"] = list(range(len(final_file.index)))
                final_file.to_csv(os.path.join(target_path, pocket_name+".csv"), index=False)

        for model_name in self.model_pred_name:
            target_path = self.DC_path["Model"][retrain_num][batch_num][model_name][target_flag]
            for pocket_name in ["testPredResult", "trainPredResult"]:
                final_file = pd.DataFrame()
                for feature in self.feature_lists:
                    target = pocket_name + "_" + str(feature_name) + "_" + str(feature) + ".csv"
                    tmp_target = os.path.join(target_path, target)
                    if not os.path.exists(tmp_target):
                        print("Data Organize: File doesn't exist: " + tmp_target)
                        continue
                    tmp = pd.read_csv(tmp_target)
                    final_file = pd.concat([final_file, tmp])
                    os.remove(os.path.join(target_path, target))
                final_file.to_csv(os.path.join(target_path, pocket_name + ".csv"), index=False)


if __name__ == "__main__":
    base_path = "../Cases/PSH_Demo"
    train_path = ""
    config_path = ""
    DC = Data_Collector(base_path, train_path, config_path)

#------------------------------------------------------------------------------------------------------
#Data_PreProcess.py
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


def Preprocess_Prepare(folder_paths, Mode):
    ####
    output_paths = {}
    if Mode == "Train":
        output_paths["data_train_path"] = os.path.join(folder_paths, 'Data_train.csv')
        output_paths["data_test_path"] = os.path.join(folder_paths, 'Data_test.csv')
        output_paths["x_train_path"] = os.path.join(folder_paths, 'x_Train.csv')
        output_paths["y_train_path"] = os.path.join(folder_paths, 'y_Train.csv')
        output_paths["Cat_config_path"] = os.path.join(folder_paths, 'Cat_Config.json')
        output_paths["DataImpute_config_path"] = os.path.join(folder_paths, 'DataImpute_Config.json')
        output_paths["DataTrans_config_path"] = os.path.join(folder_paths, 'DataTrans_Config.json')
        output_paths["DelMissingCol_config_path"] = os.path.join(folder_paths, 'DelMissingCol_Config.json')
        output_paths["DelNonNumCol_config_path"] = os.path.join(folder_paths, 'DelNonNumCol_Config.json')
    elif Mode == "Test":
        output_paths["x_test_path"] = os.path.join(folder_paths, 'x_Test.csv')
        output_paths["y_test_path"] = os.path.join(folder_paths, 'y_Test.csv')
    ####

    input_paths = read_path(folder_paths)

    # if Mode == "Test":
    #     yoyo = folder_paths.replace("01_Test", "00_Model_building")
    #     input_paths['Cat_config_path'] = os.path.join(yoyo, 'Cat_Config.json')

    mylogs = WriteLog(input_paths["log_path"], input_paths["error_path"])
    # mylogs.init_logger()
    configs = read_config(input_paths["config_path"], mylogs)

    return input_paths, output_paths, mylogs, configs


def read_data(data_paths, mylogs):
    try:
        dfs = pd.read_csv(data_paths)
    except Exception as e:
        mylogs.error("Data Pre-Process Read Data Error!!!")
        mylogs.error_trace(e)

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
            msg = msg + " ".join(["They are impute by", impute_value, "."])
            mylogs_.info(msg)

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
        mylogs.info("**Category Columns Check Passed**")

    elif len(check_list) != 0:
        cat_configs["Cat_list"] = list(check_list)
        cat_configs["Group_Mode"] = 1
        for col in check_list:
            list_col = list(dfs[col])
            impute_value = max(list_col, key=list_col.count)
            cat_configs[col] = {}
            cat_configs[col]["Impute_Values"] = impute_value

        dfs = CHECKImputeCol(dfs, check_list, cat_configs, mylogs)

    save_config(cat_config_paths,cat_configs, mylogs)

    return dfs


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

    try:
        if configs["Data_Split"]["Setting_Spilt_Mode"] == "Amount":
            test_amount_ = configs["Data_Split"]["Parameter_Split_Amount"]
            dfs_train = dfs.iloc[:-test_amount_]
            dfs_test = dfs.iloc[-test_amount_:]
    #        print(dfs_train.shape, dfs_test.shape)

        elif configs["Data_Split"]["Setting_Spilt_Mode"] == "Ratio":
            dfs_train, dfs_test = train_test_split(dfs,
                                                   test_size=configs["Data_Split"]["Parameter_Test_Rate"],
                                                   shuffle=configs["Data_Split"]["Setting_Random_Mode"] == 1)
    except Exception as e:
        mylogs.warning("There is something wrong in Data Split Setting, the module is execute as default value")
        mylogs.warning_trace(e)
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
        if (col in configs["Index_Columns"]) | (col in configs["Y"]):
            continue

        if dfs[col].dtypes == 'object':
            try:
                dfs[col] = pd.to_datetime(dfs[col], format='%Y-%m-%d %H:%M:%S.%f')
                if configs['Remove_Nonnumerical_Columns']["Setting_Transform_Time_to_Seconds"] == 1:
                    dfs[col] = dfs[col].apply(transform2Second, mylog=mylogs)
                    DelNonNumCol_configs["Time_Columns"].append(col)

            except:
                dfs = dfs.drop(col, axis=1)
                continue
    new_columns = dfs.columns
    # print(set(ori_columns) - set(new_columns))
    DelNonNumCol_configs["Remove_Nonnumerical_Columns"] = list(set(ori_columns) - set(new_columns))
    return dfs, configs, DelNonNumCol_configs


def TESTremoveObjectCol(dfs, configs, DelNonNumCol_configs, mylogs):
#    print(configs['Remove_Nonnumerical_Columns']["Remove_Nonnumerical_Columns"])
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
            mylogs.warning("Data PreProcess (Test): CAN NOT TRANSFORM TIME TO SECOND: "+ str(col))
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


def Data_PreProcess_Train(folder_path, merge_flag = None):

    input_path, output_path, mylog, config =Preprocess_Prepare(folder_path, Mode="Train")

    config["IDX"] = config["Index_Columns"].copy()
    if config["Filter_Feature"] is not None:
        if config["Filter_Feature"] not in config["Index_Columns"]:
            config["IDX"].append(config["Filter_Feature"])

    mylog.info("-----Data PreProcess (Train) Start!!!-----")

    df_train = read_data(input_path["raw_path"], mylog)

    df_train = RemoveColsInReview(df_train, config['DataPreview']['RemoveInReview'], mylog)

    df_train = CheckCatCol(df_train, config, output_path['Cat_config_path'], mylog)
    
    config = preSortGroupCol(config)
    
    # TRAIN STEP
    mylog.info("**TRAINING PROCESS START**")
    for step_index in config['Data_Preprocess_Steps']:
        if step_index == "DataSplit":
            if merge_flag is None:
                df_train, df_test = DataSplit(df_train, config, output_path, mylog)
                
            elif merge_flag == True:
                df_train.to_csv(output_path["data_train_path"], index=False)
                continue
            

        if step_index == "DelNonNumCol":
            mylog.info("Remove_Nonnumerical_Columns")
            df_train, config, DelNonNumCol_config = removeObjectCol(df_train, config, mylog)
            save_config(output_path["DelNonNumCol_config_path"], DelNonNumCol_config, mylog)
            
        elif step_index == "DelMissingCol":
            mylog.info("Remove_Missing_Value")
            df_train, config, DelMissingCol_config = removeMissingCol(df_train, config, mylog)
            save_config(output_path["DelMissingCol_config_path"], DelMissingCol_config, mylog)
                    
        # elif step_index == "MergeCatCol":
        #     mylog.info("Merge_Category_Columns")
        #     df_train, config = mergeCategoryCol(df_train, config)
        elif step_index == "DataImpute":
            mylog.info("Data Imputation")
            df_train, config, DataImpute_config = DataImputation(df_train, config)
            save_config(output_path["DataImpute_config_path"], DataImpute_config, mylog)

        elif step_index == "DataTrans":
            mylog.info("Data Transform")
            df_train, config, DataTrans_config = DataTransform(df_train, config)
            save_config(output_path["DataTrans_config_path"], DataTrans_config, mylog)

    tmp = config["IDX"].copy()
    tmp.extend(config["Y"])
    
    df_train[tmp].to_csv(output_path["y_train_path"], index=False)
    df_train.drop(config["Y"], axis=1).to_csv(output_path["x_train_path"], index=False)

    # remove Y
    # print(config["Index_Columns"])
    save_config(input_path["config_path"],config, mylog)
    
    mylog.info("-----Data PreProcess (Train) Finished-----")

    return None

def Data_PreProcess_Test(folder_path):

    input_path, output_path, mylog, config = Preprocess_Prepare(folder_path, Mode="Test")

    mylog.info("-----Data PreProcess (Test)-----")

    df_test = read_data(input_path["data_test"], mylog)
    df_test = RemoveColsInReview(df_test, config['DataPreview']['RemoveInReview'], mylog)

    df_test = TESTCheckCatCol(df_test, config, input_path['Cat_config_path'], mylog)

    # TEST STEP
    mylog.info("**TESTING PROCESS START**")
    for step_index in config['Data_Preprocess_Steps']:
        if step_index == "DelNonNumCol":
            mylog.info("Remove_Nonnumerical_Columns")
            DelNonNumCol_config = read_config(input_path["DelNonNumCol_config_path"], mylog)
            df_test = TESTremoveObjectCol(df_test, config, DelNonNumCol_config, mylog)
        elif step_index == "DelMissingCol":
            mylog.info("Remove_Missing_Value")
            DelMissingCol_config = read_config(input_path["DelMissingCol_config_path"], mylog)
            df_test = TESTremoveMissingCol(df_test, DelMissingCol_config, mylog)
        # elif step_index == "MergeCatCol":
        #     mylog.info("Merge_Category_Columns")
        #     df_test = TESTmergeCategoryCol(df_test, config)
        elif step_index == "DataImpute":
            mylog.info("Data Imputation")
            DataImpute_config = read_config(input_path["DataImpute_config_path"], mylog)
            df_test = TESTDataImputation(df_test, config, DataImpute_config)

        elif step_index == "DataTrans":
            mylog.info("Data Transform")
            DataTrans_config = read_config(input_path["DataTrans_config_path"], mylog)
            df_test = TESTDataTransform(df_test, config, DataTrans_config)

    tmp = config["IDX"].copy()
    tmp.extend(config["Y"])
    df_test[tmp].to_csv(output_path["y_test_path"], index=False)
    df_test.drop(config["Y"], axis=1).to_csv(output_path["x_test_path"], index=False)

    # save_config(input_path["config_path"],config, mylog)
    mylog.info("-----Data PreProcess (Test) Finished-----")
    
    return None

def OnLine_Data_PreProcess_no_y(df_online ,folder_path):

    input_path, output_path, mylog, config = Preprocess_Prepare(folder_path, Mode="Test")
    mylog.info("Data PreProcess\tOnline Start")

    df_online = RemoveColsInReview(df_online, config['DataPreview']['RemoveInReview'], mylog)
    df_online = TESTCheckCatCol(df_online, config, input_path['Cat_config_path'], mylog)

    for step_index in config['Data_Preprocess_Steps']:
        if step_index == "DelNonNumCol":
            # mylog.info("Remove_Nonnumerical_Columns")
            DelNonNumCol_config = read_config(input_path["DelNonNumCol_config_path"], mylog)
            df_online = TESTremoveObjectCol(df_online, config, DelNonNumCol_config, mylog)
        elif step_index == "DelMissingCol":
            # mylog.info("Remove_Missing_Value")
            DelMissingCol_config = read_config(input_path["DelMissingCol_config_path"], mylog)
            df_online = TESTremoveMissingCol(df_online, DelMissingCol_config, mylog)
        # elif step_index == "MergeCatCol":
        #     mylog.info("Merge_Category_Columns")
        #     df_test = TESTmergeCategoryCol(df_test, config)
        elif step_index == "DataImpute":
            # mylog.info("Data Imputation")
            DataImpute_config = read_config(input_path["DataImpute_config_path"], mylog)
            df_online = TESTDataImputation(df_online, config, DataImpute_config)

        elif step_index == "DataTrans":
            # mylog.info("Data Transform")
            DataTrans_config = read_config(input_path["DataTrans_config_path"], mylog)
            df_online = OnlineDataTransform(df_online, config, DataTrans_config)


    tmp = config["IDX"].copy()
    tmp.extend(config["Y"])
    #df_online_y = df_online[tmp]
    #df_online_X = df_online.drop(config["Y"], axis=1)
    mylog.info("Data PreProcess\tOnline Finished")

    return df_online

def OnLine_Data_PreProcess(df_online ,folder_path):

    input_path, output_path, mylog, config = Preprocess_Prepare(folder_path, Mode="Test")
    mylog.info("Data PreProcess\tOnline Start")

    df_online = RemoveColsInReview(df_online, config['DataPreview']['RemoveInReview'], mylog)
    df_online = TESTCheckCatCol(df_online, config, input_path['Cat_config_path'], mylog)

    for step_index in config['Data_Preprocess_Steps']:
        if step_index == "DelNonNumCol":
            # mylog.info("Remove_Nonnumerical_Columns")
            DelNonNumCol_config = read_config(input_path["DelNonNumCol_config_path"], mylog)
            df_online = TESTremoveObjectCol(df_online, config, DelNonNumCol_config, mylog)
        elif step_index == "DelMissingCol":
            # mylog.info("Remove_Missing_Value")
            DelMissingCol_config = read_config(input_path["DelMissingCol_config_path"], mylog)
            df_online = TESTremoveMissingCol(df_online, DelMissingCol_config, mylog)
        # elif step_index == "MergeCatCol":
        #     mylog.info("Merge_Category_Columns")
        #     df_test = TESTmergeCategoryCol(df_test, config)
        elif step_index == "DataImpute":
            # mylog.info("Data Imputation")
            DataImpute_config = read_config(input_path["DataImpute_config_path"], mylog)
            df_online = TESTDataImputation(df_online, config, DataImpute_config)

        elif step_index == "DataTrans":
            # mylog.info("Data Transform")
            DataTrans_config = read_config(input_path["DataTrans_config_path"], mylog)
            df_online = OnlineDataTransform(df_online, config, DataTrans_config)


    tmp = config["IDX"].copy()
    tmp.extend(config["Y"])
    df_online_y = df_online[tmp]
    df_online_X = df_online.drop(config["Y"], axis=1)
    mylog.info("Data PreProcess\tOnline Finished")

    return df_online_X, df_online_y

if __name__ == '__main__':

    path = "/home/petertylin/PycharmProjects/00_Projects/00_AVM/AVM_System/Cases/T75R4_20190523/T75R4_20190523/T75R4_20190523_03/00/03_PreprocessedData/02_Merge"
    input_paths = read_path(path)
    df = pd.read_csv(input_paths["raw_path"])

    path = "/home/petertylin/PycharmProjects/00_Projects/00_AVM/AVM_System/Cases/T75R4_20190523/T75R4_20190523/T75R4_20190523_03/00/03_PreprocessedData/03_Test"
    for idx in range(df.shape[0]):
        # print(type(df.iloc[idx]))
        df_tmp = pd.DataFrame(data=df.iloc[idx].values.reshape(1,-1), columns=df.columns)

        print(df_tmp)
        print("input type:", type(df_tmp))
        print("input shape:", df_tmp.shape)
        X, y = OnLine_Data_PreProcess(df_tmp, path)

        print("Output type:", type(X), type(y))
        print("Output shape:", X.shape, y.shape)

        break
        
#----------------------------------------------------------------------------------------------
#Data_Preview.py
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
    for col in df_1.columns:
#        if df_1.loc[:,col].infer_objects().dtypes == "object":
#            object_col.append(col)
#            continue
        if col in not_num_col:
            continue
        nan_cnt = sum(df_1.loc[:,col].isna())
        missing_table.loc[col, "Features"] = col
        missing_table.loc[col, "Missing_rate_percentage"] = nan_cnt/total_row_num*100
        if nan_cnt/total_row_num > missing_T:
            missing_list.append(col)
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

            
    mylog.info("< Note that missing values are ignored and high-missing-rate features will be dropped during data preview>")
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
            sns.barplot(x="Outspec_Ratio_percentage", y="Number of features", data=number_table, ax=ax);
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

#--------------------------------------------------------------------------------------------
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

    # if configs_['Data_Imputation']["Same"] == 1:
    #     group_by_col = configs_['Merge_Category_Columns']['New_Columns']
    #
    # elif configs_["Data_Imputation"]["Same"] == 0:
    #     merge_col_list = configs_['Data_Imputation']['Group_List']
    #     group_by_col = "_".join(merge_col_list)
    #     dfs_[group_by_col] = dfs_[merge_col_list].apply(lambda x: '_'.join(x), axis=1
    merge_col_list = configs_['Data_Imputation']['Group_List']
    group_by_col = "PANDAS_KING_DAVID"
    # dfs_[group_by_col] = dfs_[merge_col_list].apply(lambda x: '_'.join(str(x)), axis=1)
    dfs_[group_by_col] = dfs_[merge_col_list].apply(lambda x: '_'.join(str(e) for e in x), axis=1)

    return dfs_, group_by_col


def DataImputation(dfs, configs):

    DataImpute_configs = {}

    strategy = configs['Data_Imputation']['Strategy']

    if len(configs["Data_Imputation"]["Group_List"]) == 0:
        for col in dfs.columns:
            if (col in configs["Index_Columns"]) | (col in configs["Y"]):
                continue
            DataImpute_configs[col] = {}
            DataImpute_configs[col]["STD_CHECK"] = 0

            if strategy in ["Mean", "Median"]:
                if strategy == "Mean":
                    impute_value = dfs[col].mean()
                elif strategy == "Median":
                    impute_value = dfs[col].median()

                DataImpute_configs[col]['DEFAULT_VALUE'] = impute_value
                if sum(dfs[col].isna()) != 0:
                    dfs[col].fillna(impute_value, inplace=True)

            elif strategy == "STD":
                DataImpute_configs[col]['DEFAULT_Mean'] = dfs[col].mean()
                DataImpute_configs[col]['DEFAULT_Median'] = dfs[col].median()
                if sum(dfs[col].isna()) == 0:
                    DataImpute_configs[col]["STD_CHECK"] = 1
                elif sum(dfs[col].isna()) != 0:
                    impute_value = ImputebySTD(dfs[col])
                    DataImpute_configs[col]['DEFAULT_VALUE'] = impute_value
                    dfs[col].fillna(impute_value, inplace=True)

        return dfs, configs, DataImpute_configs

    elif len(configs["Data_Imputation"]["Group_List"]) != 0:
        dfs, group_by_col = GroupColCheck(dfs, configs)

        if strategy in ["Median"]:
            for col in dfs.columns:
                if (col in configs["Index_Columns"]) | (col in configs["Y"]):
                    continue

                if col == group_by_col:
                    continue

                DataImpute_configs[col] = {}
                col_median = dfs[col].median()
                DataImpute_configs[col]['DEFAULT_VALUE'] = col_median

                for cat_type in np.unique(dfs[group_by_col]):
                    DataImpute_configs[col][cat_type] = {}
                    impute_value = col_median
                    DataImpute_configs[col][cat_type]["over30"] = 0
                    if dfs[col].loc[(dfs[group_by_col] == cat_type) &
                                    (~dfs[group_by_col].isna())].size > 30:
                        impute_value = dfs[col].loc[(dfs[group_by_col] == cat_type)].median()
                        DataImpute_configs[col][cat_type]["over30"] = 1

                    elif dfs[col].loc[(dfs[group_by_col] == cat_type) &
                                      (~dfs[group_by_col].isna())].size < 30:
                        print(col, cat_type, "NO impute by Group")

                    DataImpute_configs[col][cat_type]['DEFAULT_VALUE'] = impute_value
                    dfs[col].loc[(dfs[group_by_col] == cat_type)].fillna(impute_value, inplace=True)

                if sum(dfs[col].isna()):
                    dfs[col].fillna(col_median, inplace=True)
                    print("ERROR")

        elif strategy in ["Mean"]:
            for col in dfs.columns:
                if (col in configs["Index_Columns"]) | (col in configs["Y"]):
                    continue

                if col == group_by_col:
                    continue
                DataImpute_configs[col] = {}
                col_mean = dfs[col].mean()
                DataImpute_configs[col]['DEFAULT_VALUE'] = col_mean

                for cat_type in np.unique(dfs[group_by_col]):
                    DataImpute_configs[col][cat_type] = {}
                    impute_value = col_mean
                    DataImpute_configs[col][cat_type]["over30"] = 0
                    if dfs[col].loc[(dfs[group_by_col] == cat_type) &
                                    (~dfs[group_by_col].isna())].size > 29:
                        DataImpute_configs[col][cat_type]["over30"] = 1
                        impute_value = dfs[col].loc[(dfs[group_by_col] == cat_type)].col_mean()

                    elif dfs[col].loc[(dfs[group_by_col] == cat_type) &
                                      (~dfs[group_by_col].isna())].size < 30:
                        print(col, cat_type, "NO impute by Group")

                    DataImpute_configs[col][cat_type]['DEFAULT_VALUE'] = impute_value
                    dfs[col].loc[(dfs[group_by_col] == cat_type)].fillna(impute_value, inplace=True)

                if sum(dfs[col].isna()):
                    dfs[col].fillna(col_mean, inplace=True)
                    print("ERROR")

        elif strategy in ["STD"]:
            for col in dfs.columns:
                if (col in configs["Index_Columns"]) | (col in configs["Y"]):
                    continue

                if col == group_by_col:
                    continue

                DataImpute_configs[col] = {}
                col_median = dfs[col].median()
                col_mean = dfs[col].mean()
                col_std = dfs[col].std()
                DataImpute_configs[col]['DEFAULT_Mean'] = col_mean
                DataImpute_configs[col]['DEFAULT_Median'] = col_median
                DataImpute_configs[col]['DEFAULT_Median'] = col_std

                for cat_type in np.unique(dfs[group_by_col]):
                    DataImpute_configs[col][cat_type] = {}
                    DataImpute_configs[col][cat_type]["over30"] = 0
                    if dfs[col].loc[(dfs[group_by_col] == cat_type) &
                                    (~dfs[group_by_col].isna())].size > 29:
                        DataImpute_configs[col][cat_type]["over30"] = 1
                        cat_mean = dfs[col].loc[(dfs[group_by_col] == cat_type)].mean()
                        cat_median = dfs[col].loc[(dfs[group_by_col] == cat_type)].median()
                        DataImpute_configs[col][cat_type]['DEFAULT_Mean'] = cat_mean
                        DataImpute_configs[col][cat_type]['DEFAULT_Median'] = cat_median
                        if sum(dfs[col].loc[(dfs[col].isna())]) != 0:
                            impute_value = ImputebySTD(dfs[col].loc[(dfs[group_by_col] == cat_type)])
                            dfs[col].loc[(dfs[group_by_col] == cat_type)].fillna(impute_value, inplace=True)
                            DataImpute_configs[col][cat_type]['DEFAULT_VALUE'] = impute_value

                if sum(dfs[col].loc[(dfs[col].isna())]) != 0:
                    impute_value = ImputebySTD(dfs[col])
                    for cat_type in np.unique(dfs[group_by_col].loc[(dfs[col].isna())]):
                        DataImpute_configs[col][cat_type]['DEFAULT_VALUE'] = impute_value
                    dfs[col].fillna(impute_value, inplace=True)

        else:
            return None

        dfs = dfs.drop(group_by_col, axis=1)

        return dfs, configs, DataImpute_configs


def TESTDataImputation(dfs, configs, DataImpute_configs):

    strategy = configs['Data_Imputation']['Strategy']

    if len(configs["Data_Imputation"]["Group_List"]) == 0:
        for col in dfs.columns:
            if (col in configs["Index_Columns"]) | (col in configs["Y"]):
                continue
            
            ## 20190805 online check empty col: empty col will be present as ""
            if dfs[col].iloc[0] == "":
#                print("=___________________________=")
                dfs[col].iloc[0] = np.nan
            
            if sum(dfs[col].isna()) == 0:
                continue

            if DataImpute_configs[col]["STD_CHECK"] == 0:
                impute_value = DataImpute_configs[col]['DEFAULT_VALUE']

            elif DataImpute_configs[col]["STD_CHECK"] == 1:
                col_mean = DataImpute_configs[col]['DEFAULT_Mean']
                col_median = DataImpute_configs[col]['DEFAULT_Median']
                impute_value = ImputebySTD(dfs[col], col_mean, col_median)

            dfs[col].fillna(impute_value, inplace=True)

    if len(configs["Data_Imputation"]["Group_List"]) != 0:
        dfs, group_by_col = GroupColCheck(dfs, configs)
        for col in dfs.columns:
            if (col in configs["Index_Columns"]) | (col in configs["Y"]):
                continue

            if col == group_by_col:
                continue
            
            ## 20190805 online check empty col: empty col will be present as ""
            if dfs[col].iloc[0] == "":
#                print("=___________________________=")
                dfs[col].iloc[0] = np.nan

            if sum(dfs[col].isna()) == 0:
                continue

            if strategy in ["Mean", "Median"]:
                for cat_type in np.unique(dfs[group_by_col].loc[(dfs[col].isna())]):
                    impute_value = DataImpute_configs[col][cat_type]['DEFAULT_VALUE']
                    dfs[col].loc[(dfs[group_by_col] == cat_type)].fillna(impute_value, inplace=True)

            elif strategy in ["STD"]:
                for cat_type in np.unique(dfs[group_by_col].loc[(dfs[col].isna())]):
                    if DataImpute_configs[col][cat_type]["over30"] == 0:
                        impute_value = DataImpute_configs[col][cat_type]['DEFAULT_VALUE']
                        dfs[col].loc[(dfs[group_by_col] == cat_type)].fillna(impute_value, inplace=True)

                    elif DataImpute_configs[col][cat_type]["over30"] == 1:
                        try:
                            impute_value = DataImpute_configs[col][cat_type]['DEFAULT_VALUE']
                            dfs[col].loc[(dfs[group_by_col] == cat_type)].fillna(impute_value, inplace=True)

                        except:
                            cat_mean = DataImpute_configs[col][cat_type]['DEFAULT_Mean']
                            cat_median = DataImpute_configs[col][cat_type]['DEFAULT_Median']
                            impute_value = ImputebySTD(dfs[col].loc[(dfs[group_by_col] == cat_type)],
                                                       cat_mean,
                                                       cat_median)
                            dfs[col].loc[(dfs[group_by_col] == cat_type)].fillna(impute_value, inplace=True)

        dfs = dfs.drop(group_by_col, axis=1)

    return dfs

#--------------------------------------------------------------------------------------------------------------
#DataTransform.py

def DataTransform(dfs, configs):

    DataTrans_configs = {}
    strategy = configs['Data_Transform']['Strategy']

    if len(configs["Data_Transform"]["Group_List"]) == 0:
        if strategy == "Z_Scale":
            DataTrans_configs['Z_Scale'] = {}
            for col in dfs.columns:
                if (col in configs["IDX"]) | (col in configs["Y"]):
                    continue
#                print("Hello-1")
                DataTrans_configs['Z_Scale'][col] = {}
                col_std = dfs[col].std(ddof=0)
                col_mean = dfs[col].mean()
                DataTrans_configs['Z_Scale'][col]['Std'] = col_std
                DataTrans_configs['Z_Scale'][col]['Mean'] = col_mean
                if col_std == .0:
                    dfs[col] = dfs[col] - col_mean
                elif col_std != .0:
                    dfs[col] = (dfs[col] - col_mean) / col_std
                    
    return dfs, configs, DataTrans_configs


def TESTDataTransform(dfs, configs, DataTrans_configs):

    strategy = configs['Data_Transform']['Strategy']

    if len(configs["Data_Transform"]["Group_List"]) == 0:
        if strategy == "Z_Scale":
            for idx, col in enumerate(dfs.columns):
                # print(idx, col)
                if (col in configs["IDX"]) | (col in configs["Y"]):
                    continue
                # print("---------------------------------")
                col_std = DataTrans_configs['Z_Scale'][col]['Std']
                col_mean = DataTrans_configs['Z_Scale'][col]['Mean']

                if col_std != .0:
                    dfs[col] = (dfs[col] - col_mean) / col_std
                elif col_std == .0:
                    dfs[col] = dfs[col] - col_mean

    return dfs

def OnlineDataTransform(dfs, configs, DataTrans_configs):

    strategy = configs['Data_Transform']['Strategy']

    if len(configs["Data_Transform"]["Group_List"]) == 0:
        if strategy == "Z_Scale":
            for idx, col in enumerate(dfs.columns):
                # print(idx, col)
                if (col in configs["IDX"]) | (col in configs["Y"]):
                    continue

                col_std = DataTrans_configs['Z_Scale'][col]['Std']
                col_mean = DataTrans_configs['Z_Scale'][col]['Mean']
                
#                print(dfs[col].iloc[0])
#                print(float(dfs[col].iloc[0]))
#                print("Q_______________________________Q")
                if dfs[col].iloc[0] == "":
                    print("Q_______________________________Q", col)
                    print(dfs[col].shape)
                    for i in range(len(dfs[col])):
                        print(dfs[col].iloc[i])
                        print("trans", type(dfs[col].iloc[i]), dfs[col].iloc[i])
                    print(dfs[col])
                    print("Oh here is it")
                    print("Q_______________________________Q")
                
                
                if col_std != .0:
                    dfs[col] = (float(dfs[col].iloc[0]) - col_mean) / col_std
                elif col_std == .0:
                    dfs[col] = float(dfs[col].iloc[0]) - col_mean

    return dfs

#------------------------------------------------------------------------------
#DB_operation.py
# -*- coding: utf-8 -*-
import pyodbc
import pandas as pd

def DB_Connection(server_name="10.96.18.199"):
    #server_name = "10.96.18.199"
    db_name = "APC"
    user = "YL"
    password = "YL$212"        
    cnxn1 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server_name + ';DATABASE=' + db_name + ';UID=' + user + ';PWD=' + password)
    
    return cnxn1

def select_project_creating(server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT WHERE UPPER(STATUS) in ('CREATING','CREATING_RUN') order by ITIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_waitcreate(server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT WHERE UPPER(STATUS) in ('CREATED','CREATING_ERROR_OK') order by ITIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model


def select_project_AI365(server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT WHERE UPPER(STATUS) = 'ONLINE' AND MODEL_TYPE = '2' order by ITIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_otm(server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT where MODEL_TYPE IN ('1','2') AND UPPER(STATUS) IN ('ONLINE','RETRAIN')"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_mtm(server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT where MODEL_TYPE IN ('3') AND UPPER(STATUS) IN ('ONLINE','RETRAIN')"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_by_projectid(project_id, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT where PROJECT_ID = '" + str(project_id) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model


def select_project_by_status(status, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT where UPPER(STATUS) = '" + str(status) + r"' order by ITIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_config_by_modelid_1ST_2ND_3RD(model_id, parameter_1st, parameter_2nd, parameter_3rd, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT_CONFIG where MODEL_ID = '" + str(model_id) + r"' and UPPER(PARAMETER_1ST) = '" + str(parameter_1st) + r"' and UPPER(PARAMETER_2ND) = '" + str(parameter_2nd) + r"' and UPPER(PARAMETER_3RD) = '" + str(parameter_3rd) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_config_by_modelid_parameter3RD(model_id, parameter_3rd, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT_CONFIG where MODEL_ID = '" + str(model_id) + r"' and UPPER(PARAMETER_3RD) = '" + str(parameter_3rd) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_confirmok_o2m(server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT A, SVM_PROJECT_MODEL B WHERE A.PROJECT_ID = B.PROJECT_ID AND UPPER(A.STATUS) in ('CREATING_PAUSE') AND A.MODEL_TYPE IN ('1','2') AND B.WAIT_CONFIRM = '0' order by A.ITIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_confirmok_m2m(server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT C, (select * from SVM_PROJECT_MODEL A, (select max(MODEL_ID) as max_model from SVM_PROJECT_MODEL group by PROJECT_ID) B where A.MODEL_ID = B.max_model and WAIT_CONFIRM = '0') D where c.PROJECT_ID = D.PROJECT_ID and c.MODEL_TYPE = '3' and upper(c.STATUS) = 'CREATING_PAUSE' order by C.ITIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_by_modelid(model_id, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT_MODEL where MODEL_ID = '" + str(model_id) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_by_modelname(model_name, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT_MODEL where MODEL_NAME = '" + str(model_name) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_by_modelname_status(model_name, status, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT_MODEL where MODEL_NAME = '" + str(model_name) + r"' AND UPPER(STATUS) = '" + str(status) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_by_projectid(project_id, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT_MODEL where PROJECT_ID = '" + str(project_id) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_maxmodel_by_projectid(project_id, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT_MODEL A, (select max(MODEL_ID) as max_model from SVM_PROJECT_MODEL where PROJECT_ID = '" + str(project_id) + r"' group by PROJECT_ID) B where A.MODEL_ID = B.max_model"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_with_model_by_projectid(project_id, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT A, SVM_PROJECT_MODEL B where A.PROJECT_ID = B.PROJECT_ID AND A.PROJECT_ID = '" + str(project_id) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_by_projectid_abnormal(project_id, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT_MODEL where PROJECT_ID = '" + str(project_id) + r"' AND UPPER(RETRAIN_TYPE) in ('ABNORMAL','ABNORMAL->ONLINE')"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_by_projectid_status(project_id, status, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT_MODEL where PROJECT_ID = '" + str(project_id) + r"' AND UPPER(STATUS) = '" + str(status) + r"' order by MODEL_ID desc"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_by_projectid_status_filterfeature(project_id, status, filter_feature, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT_MODEL where PROJECT_ID = '" + str(project_id) + r"' AND UPPER(STATUS) = '" + str(status) + r"' AND FILTER_FEATURE = '" + str(filter_feature) + r"' order by MODEL_ID desc"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_by_projectid_status2_filterfeature(project_id, status1, status2, filter_feature, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT_MODEL where PROJECT_ID = '" + str(project_id) + r"' AND UPPER(STATUS) in ('" + str(status1) + r"','" + str(status2) + r"') AND FILTER_FEATURE = '" + str(filter_feature) + r"' order by MODEL_ID desc"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_model_by_projectid_predictresult(project_id, predict_result, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_PROJECT_MODEL where PROJECT_ID = '" + str(project_id) + r"' AND UPPER(RETRAIN_TYPE) in ('ABNORMAL','ABNORMAL->ONLINE') AND PREDICT_RESULT = '" + str(predict_result) + r"' and PREDICT_START_TIME is not null and RETRAIN_END_TIME is not null"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

#20190730 EXCLUDE PROJECT 62
def select_project_online_scantime_x(server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select A.*, B.DATATYPE, B.NO, B.LAST_SCANTIME from SVM_PROJECT A, SVM_ONLINE_SCANTIME B where A.PROJECT_ID = B.PROJECT_ID AND A.MODEL_TYPE IN ('1','2') AND UPPER(A.STATUS) IN ('ONLINE','RETRAIN') AND UPPER(B.DATATYPE) IN ('X') AND A.PROJECT_ID != '62' order by B.LAST_SCANTIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_online_scantime_x_many(server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select A.*, B.DATATYPE, B.NO, B.LAST_SCANTIME from SVM_PROJECT A, SVM_ONLINE_SCANTIME B where A.PROJECT_ID = B.PROJECT_ID AND A.MODEL_TYPE IN ('3') AND UPPER(A.STATUS) IN ('ONLINE','RETRAIN') AND UPPER(B.DATATYPE) IN ('X') order by B.LAST_SCANTIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

#20190730 EXCLUDE PROJECT 62
def select_project_online_scantime_y(server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select A.*, B.DATATYPE, B.NO, B.LAST_SCANTIME from SVM_PROJECT A, SVM_ONLINE_SCANTIME B where A.PROJECT_ID = B.PROJECT_ID AND A.MODEL_TYPE IN ('1','2') AND UPPER(A.STATUS) IN ('ONLINE','RETRAIN') AND UPPER(B.DATATYPE) IN ('Y') AND A.PROJECT_ID != '62' order by B.LAST_SCANTIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_online_scantime_y_many(server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select A.*, B.DATATYPE, B.NO, B.LAST_SCANTIME from SVM_PROJECT A, SVM_ONLINE_SCANTIME B where A.PROJECT_ID = B.PROJECT_ID AND A.MODEL_TYPE IN ('3') AND UPPER(A.STATUS) IN ('ONLINE','RETRAIN') AND UPPER(B.DATATYPE) IN ('Y') order by B.LAST_SCANTIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_project_online_scantime_by_projectid_datatype(project_id, datatype, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select A.*, B.DATATYPE, B.NO, B.LAST_SCANTIME from SVM_PROJECT A, SVM_ONLINE_SCANTIME B where A.PROJECT_ID = B.PROJECT_ID AND UPPER(A.STATUS) IN ('ONLINE','RETRAIN') AND A.PROJECT_ID = '" + str(project_id) + r"' AND B.DATATYPE = '" + str(datatype) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_online_parameter_by_projectid(project_id, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_ONLINE_PARAMETER where PROJECT_ID = '" + str(project_id) + r"' order by SEQ"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_online_parameter_by_projectid_xy(project_id, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_ONLINE_PARAMETER where PROJECT_ID = '" + str(project_id) + r"' and UPPER(DATATYPE) in ('X','Y') order by SEQ"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_online_parameter_by_projectid_datatype(project_id, datatype, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_ONLINE_PARAMETER where PROJECT_ID = '" + str(project_id) + r"' and DATATYPE = '" + datatype + r"' order by SEQ"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_online_status_by_projectid_runindex(project_id, runindex, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from SVM_ONLINE_STATUS where PROJECT_ID = '" + str(project_id) + r"' and RUNINDEX = '" + str(runindex) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_projectx_run_by_itime(project_id, last_scantime, max_time, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = r"select * from " + str(project_id) + "_RUN where ITIME > '" + last_scantime + r"' AND ITIME < '" + max_time + r"' order by ITIME"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_projectx_run_by_runindex(project_id, runindex, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = "select * from " + str(project_id) + r"_RUN where RUNINDEX = '" + str(runindex) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_projectx_parameter_data_by_runindex_parameter(project_id, runindex, parameter, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = "select * from " + str(project_id) + "_PARAMETER_DATA where RUNINDEX = " + str(runindex) + r" and UPPER(PARAMETER) = '" + str(parameter).upper() + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_projectx_parameter_data_by_parameter_itime(project_id, parameter, itime, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = "select * from " + str(project_id) + r"_PARAMETER_DATA where UPPER(PARAMETER) = '" + str(parameter.upper()) + r"' and ITIME > '" + itime + r"' order by ITIME" 
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_projectx_parameter_data_by_parameter_itime_itime(project_id, parameter, itime1, itime2, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = "select * from " + str(project_id) + r"_PARAMETER_DATA where UPPER(PARAMETER) = '" + str(parameter.upper()) + r"' and ITIME > '" + itime1 + r"' and ITIME < '" + itime2 + r"' order by ITIME" 
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_projectx_predict_data_by_runindex_parameter_modelid(project_id, runindex, parameter, model_id, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(runindex) + r" and PARAMETER = '" + str(parameter) + r"' and MODEL_ID = " + str(model_id)
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def select_projectx_contribution_data_by_runindex_parameter_model(project_id, runindex, parameter, model, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    sql = "select * from " + str(project_id) + "_CONTRIBUTION_DATA where RUNINDEX = " + str(runindex) + r" and PARAMETER = '" + str(parameter) + r"' and MODEL = '" + str(model) + r"'"
    df_project_model = pd.read_sql(sql, cnxn1)
    
    return df_project_model

def update_project_STATUS_by_projectid(status, project_id, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_PROJECT SET STATUS = ? WHERE PROJECT_ID = ?", str(status), int(project_id))
    cnxn1.commit()

def update_project_model_modelstatus_by_modelid(model_status, model_id, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_PROJECT_MODEL SET MODEL_STATUS = ? WHERE MODEL_ID = ?", str(model_status), int(model_id))
    cnxn1.commit()
    
def update_project_model_retrainstarttime_by_modelid(retrain_start_time, model_id, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_PROJECT_MODEL SET RETRAIN_START_TIME = ? WHERE MODEL_ID = ?", retrain_start_time, int(model_id))
    cnxn1.commit()
    
def update_project_model_predictresult_by_modelid(predict_result, model_id, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    cursor1 = cnxn1.cursor()
    sql = 'UPDATE SVM_PROJECT_MODEL SET PREDICT_RESULT = \'{}\' WHERE MODEL_ID = {}'.format(str(predict_result), int(model_id))
    cursor1.execute(sql)
    #cursor1.execute("UPDATE SVM_PROJECT_MODEL SET PREDICT_RESULT = ? WHERE MODEL_ID = ?", str(predict_result), int(model_id))
    cnxn1.commit()
    
def update_project_model_modelstatus_modelstep_waitconfirm_by_modelid(model_status, model_step, wait_confirm, model_id, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_PROJECT_MODEL SET MODEL_STATUS = ?, MODEL_STEP = ?, WAIT_CONFIRM = ? WHERE MODEL_ID = ?", str(model_status), str(model_step), int(wait_confirm), int(model_id))
    cnxn1.commit()
    
def update_project_model_mae_mape_by_modelid(mae, mape, model_id, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_PROJECT_MODEL SET MAE = ?, MAPE = ? WHERE MODEL_ID = ?", float(mae), float(mape), int(model_id))
    cnxn1.commit()
    
def update_online_scantime_lastscantime_by_projectid_datatype(last_scantime, project_id, datatype, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_ONLINE_SCANTIME SET LAST_SCANTIME = ? WHERE PROJECT_ID = ? AND DATATYPE = ?", last_scantime, int(project_id), str(datatype))
    cnxn1.commit()
    
def update_online_status_xdialarm_by_projectid_runindex(xdi_alarm, project_id, runindex, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_ONLINE_STATUS SET XDI_ALARM = ? WHERE PROJECT_ID = ? AND RUNINDEX = ?", int(xdi_alarm), int(project_id), int(runindex))
    cnxn1.commit()

def update_online_status_ydialarm_by_projectid_runindex(ydi_alarm, project_id, runindex, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    cursor1 = cnxn1.cursor()
    cursor1.execute("UPDATE SVM_ONLINE_STATUS SET YDI_ALARM = ? WHERE PROJECT_ID = ? AND RUNINDEX = ?", int(ydi_alarm), int(project_id), int(runindex))
    cnxn1.commit()
    
def update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(projectx_predict_data, param_value, is_retrain_predict, runindex, parameter, model_id, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    cursor1 = cnxn1.cursor()
    sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {}, ITIME = getdate() WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(projectx_predict_data+"_PREDICT_DATA", param_value, is_retrain_predict, int(runindex), str(parameter), int(model_id))
    cursor1.execute(sql)       
    cnxn1.commit() 

def update_projectx_run_signal_by_runindex(projectx_run, signal, runindex, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    cursor1 = cnxn1.cursor()
    sql = 'UPDATE {} SET SIGNAL = {} WHERE RUNINDEX = {}'.format(projectx_run+"_RUN", int(signal), int(runindex))
    cursor1.execute(sql)       
    cnxn1.commit() 

def update_projectx_run_xdialarm_by_runindex(projectx_run, xdi_alarm, runindex, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    cursor1 = cnxn1.cursor()
    sql = 'UPDATE {} SET XDI_ALARM = {} WHERE RUNINDEX = {}'.format(projectx_run+"_RUN", int(xdi_alarm), int(runindex))
    cursor1.execute(sql)       
    cnxn1.commit() 

def update_projectx_run_ydialarm_by_runindex(projectx_run, ydi_alarm, runindex, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    cursor1 = cnxn1.cursor()
    sql = 'UPDATE {} SET YDI_ALARM = {} WHERE RUNINDEX = {}'.format(projectx_run+"_RUN", int(ydi_alarm), int(runindex))
    cursor1.execute(sql)       
    cnxn1.commit() 

def update_projectx_contribution_data_contribution_by_runindex_parameter_model(projectx_contribution_data, contribution, runindex, parameter, model, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    cursor1 = cnxn1.cursor()
    sql = 'UPDATE {} SET CONTRIBUTION = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL = \'{}\''.format(projectx_contribution_data+"_CONTRIBUTION_DATA", float(contribution), int(runindex), str(parameter), str(model))
    cursor1.execute(sql)       
    cnxn1.commit() 
    
def insert_online_status_projectid_runindex_xdialarm_itime(project_id, runindex, xdi_alarm, itime, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    cursor1 = cnxn1.cursor()  
    cursor1.execute("insert into SVM_ONLINE_STATUS(PROJECT_ID, RUNINDEX, XDI_ALARM, ITIME) values (?,?,?,?)", int(project_id), int(runindex), int(xdi_alarm), itime)
    cnxn1.commit() 

def insert_online_status_projectid_runindex_ydialarm_itime(project_id, runindex, ydi_alarm, itime, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    cursor1 = cnxn1.cursor()  
    cursor1.execute("insert into SVM_ONLINE_STATUS(PROJECT_ID, RUNINDEX, YDI_ALARM, ITIME) values (?,?,?,?)", int(project_id), int(runindex), int(ydi_alarm), itime)
    cnxn1.commit() 
    
def insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(projectx_predict_data, runindex, parameter, param_value, model_id, is_retrain_predict, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    cursor1 = cnxn1.cursor()
    sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},getdate())'.format(projectx_predict_data+"_PREDICT_DATA", int(runindex), str(parameter), param_value, int(model_id), is_retrain_predict)
    cursor1.execute(sql)       
    cnxn1.commit()  
    
def insert_projectx_contribution_data_runindex_model_parameter_contribution(projectx_contribution_data, runindex, model, parameter, contribution, server_name1="10.96.18.199"):
    cnxn1 = DB_Connection(server_name=server_name1)
    
    cursor1 = cnxn1.cursor()
    sql = 'INSERT INTO {} (RUNINDEX,MODEL,PARAMETER,CONTRIBUTION,ITIME) VALUES ({},\'{}\',\'{}\',{},getdate())'.format(projectx_contribution_data+"_CONTRIBUTION_DATA", int(runindex), str(model), str(parameter), float(contribution))
    cursor1.execute(sql)       
    cnxn1.commit() 
    
#--------------------------------------------------------------------------------------------------------------------------------------   
#Exclusion.py
from config import read_config, save_config
from CreateLog import WriteLog
from Read_path import read_path
import pandas as pd
from Data_Check import read_csv
from MXCI_MYCI_pre import split_MXCI_train_path
import os

def FeatureExclude(folder_path):
    input_path = read_path(folder_path)
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    mylog.init_logger()

    Ans = input("Any features need to be excluded? Please enter them all at once.\n")
    mylog.info("-----Ask if any cols need to be excluded-----")
    if Ans == "":
        mylog.info("No feature needs to be excluded.")
        ex_col = []
    else:
        mylog.info("User list:")
        mylog.info(Ans)
        ex_col = Ans.split(",")

    mylog.info("-----End of exclusion----")
    if len(ex_col) != 0:
        config = read_config(input_path["config_path"], mylog)
        config["DataReview"]["RemoveInReview"] = ex_col
        save_config(input_path["config_path"], config, mylog)
    return None


def DataRemove(folder_path, remove_flag=None):
    error_msg = "Please contact APC team to solve the problem."
    msg = None
    input_path = read_path(folder_path)
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    mylog.init_logger()
    config = read_config(input_path["config_path"], mylog)

    if remove_flag == "XDI":
        try:
            path = input_path["XDI_data_remove_path"]
        except Exception as e:
            mylog.warning("XDI_data_remove.csv doesn't exist")
            return "NG", error_msg

    elif remove_flag == "YDI":
        try:
            path = input_path["YDI_data_remove_path"]
        except Exception as e:
            mylog.warning("YDI_data_remove.csv doesn't exist")
            return "NG", error_msg
    else:
        mylog.warning("Unseen flag" + str(remove_flag))
        return "NG", error_msg

    mylog.info("-----DataRemove-----")
    mylog.info("Remove Data from: " + path)
    if not os.path.exists(path):
        mylog.warning("DataRemove: path not exist" + path)
        mylog.warning("folder_path: " + folder_path)
        return "NG", error_msg
    try:
        remove_df = read_csv(path, "DataRemove", mylog)
    except Exception as e:
        mylog.warning("DataRemove: Error while reading data from" + path)
        mylog.warning(e)
        return "NG", error_msg
    remove_list = remove_df.columns.tolist()
    ex_cols = config["Index_Columns"]
    if set(remove_list) != set(ex_cols):
        mylog.warning("DataRemove : Columns are not matched in process" + remove_flag)
        return "NG", error_msg

    remove_list_idx = remove_df.apply(lambda x: ''.join(str(e) for e in x), axis=1).tolist()

    df_paths = [input_path["x_train_path"], input_path["x_test_path"], input_path["y_train_path"], input_path["y_test_path"], input_path["raw_path"]]
    # df_paths = [input_path["x_test_path"],input_path["y_test_path"]]

    try:
        for df_path in df_paths:
            df = read_csv(df_path, "DataRemove", mylog)
            dirname, base, ext = split_MXCI_train_path(df_path)
            resave_path = os.path.join(dirname, base+"_origin"+ ext)
            if not os.path.exists(resave_path):
                df.to_csv(resave_path, index=False)
            # if base == "raw_data":
            #     os.rename(os.path.join(dirname, base+"_origin"+ ext),
            #               os.path.join(dirname, base+"_origin_"+ remove_flag + ext))

            df["idx"] = df[remove_list].apply(lambda x: ''.join(str(e) for e in x), axis=1)
            df = df[~df["idx"].isin(remove_list_idx)]
            df = df.drop(["idx"], axis=1)
            df.to_csv(df_path, index=False)
    except Exception as e:
        mylog.warning("Error while deleting data")
        mylog.warning_trace(e)
        return "NG", error_msg
    mylog.info("-----DataRemove Done-----")
    return "OK", msg


if __name__ == "__main__":
    folder_path = "../Cases/Test/04_XDI/"
    DataRemove(folder_path, remove_flag="XDI")

#-----------------------------------------------------------------------------------------------
#Formula.py
# -*- coding: utf-8 -*-

import math
import numpy as np

def integrand(x,a,b):
    sigma = 1
    mu = min(a,b)
    coef = 1/math.sqrt(2*np.pi)/sigma
    return 2*coef*math.exp(-0.5*((x-mu)/sigma)**2)

#------------------------------------------------------------------
#json2csv.py
import pandas as pd
# from config import read_config
import os
import time

def read_config(config_path):
    import json
    with open(config_path) as json_data:
        config_ = json.load(json_data)
    return config_

def json2csv(config_path):
    config = read_config(config_path)
    dict_config = {}
    for key in config:
        # print(key)
        # print(config[key])
        if type(config[key]) == dict:
            for sub_key in config[key]:
                key_name = key + "+" + sub_key
                # print(key_name)
                # print(type(config[key][sub_key]))
                if type(config[key][sub_key]) == dict:
                    for sub_key_2 in config[key][sub_key]:
                        key_name_2 = key_name + "+" + sub_key_2
                        if type(config[key][sub_key][sub_key_2]) == list:

                            if len(config[key][sub_key][sub_key_2]) == 0:
                                dict_config[key_name_2] = None

                            elif len(config[key][sub_key][sub_key_2]) != 0:
                                dict_config[key_name_2] = "+".join(config[key][sub_key][sub_key_2])

                        elif type(config[key][sub_key][sub_key_2]) != list:
                            dict_config[key_name_2] = config[key][sub_key][sub_key_2]

                elif type(config[key][sub_key]) == list:

                    if len(config[key][sub_key]) == 0:
                        dict_config[key_name] = None

                    elif len(config[key][sub_key]) != 0:
                        dict_config[key_name] = "+".join(config[key][sub_key])

                elif type(config[key][sub_key]) != list:
                    dict_config[key_name] = config[key][sub_key]

                # print(df)

        elif type(config[key]) != dict:
            if type(config[key]) == list:
                if len(config[key]) == 0:
                    dict_config[key] = None
                elif len(config[key]) != 0:
                    dict_config[key] = "+".join(config[key])
            elif type(config[key]) != list:
                dict_config[key] = config[key]

            # print(df)

    # print(dict_config)

    df = pd.DataFrame(data=dict_config, index=[0])
    dirname, basename = os.path.split(config_path)
    base, ext = os.path.splitext(basename)
    #20190717 avoid someone open config.csv, cause to_csv fail
    def timer(n):
        while True:
            try:
                df.to_csv(os.path.join(dirname, base+".csv"), index=False)
            except:
                print("someone open config.csv, wait 5s")
                time.sleep(n)
            else:
                break               
    timer(5)

def write_data(filename, data):
    import csv
    with open(filename, 'w') as out:
        w = csv.writer(out)
        w.writerows(data)


def json2csv_list_mode():

    config = read_config('config.json')

    list_idx = []
    list_value = []
    for key in config:

        if type(config[key]) == dict:
            # print(config[key])
            for sub_key in config[key]:
                key_name = key + "+" + sub_key
                list_idx.append(key_name)

                if type(config[key][sub_key]) == list:
                    if len(config[key][sub_key]) == 0:
                        list_value.append(None)
                    elif len(config[key][sub_key]) != 0:
                        list_value.append("+".join(config[key][sub_key]))

                elif type(config[key][sub_key]) != list:
                    list_value.append(config[key][sub_key])

        elif type(config[key]) != dict:
            list_idx.append(key)
            if type(config[key]) == list:
                if len(config[key]) == 0:
                    list_value.append(None)
                elif len(config[key]) != 0:
                    list_value.append("+".join(config[key]))
            elif type(config[key]) != list:
                list_value.append(config[key])

    # for idx, val in zip(list_idx, list_value):
    #     print(idx, val)

    config_list = [list_idx, list_value]
    write_data("config.csv", config_list)


if __name__ == "__main__":

    # print("+".join(["value"]))
    json2csv___()
    
#--------------------------------------------------------------------
#Model_Selection.py
"""
This Code is written by PeterTYLin,
If there is any problem, please call DavidSWWang. XD QQ

This Function will only execute in offline. Online will not execute this model.
This Function is not support multi-objective value.
The multi ver. would be develop in the short future
"""
from Read_path import read_path
from config import read_config, save_config
from CreateLog import WriteLog
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import math
plt.style.use("ggplot")


def Model_Selection_Prepare(data_folders, Mode):
    ####### output path #######
    output_paths = {}
    if Mode in ["Train", "Batch"]:
        output_paths["y_pred_merge_train"] = os.path.join(data_folders, "y_pred_merge_train.csv")
        output_paths["y_pred_merge_test"] = os.path.join(data_folders, "y_pred_merge_test.csv")
        output_paths["y_pred_report_path"] = os.path.join(data_folders, "y_pred_report.csv")
    elif Mode =="Merge":
        output_paths["y_pred_merge_train"] = os.path.join(data_folders, "y_pred_merge_train.csv")
        output_paths["y_pred_report_path"] = os.path.join(data_folders, "y_pred_report.csv")

    output_paths["Model_Selection_Report"] = os.path.join(data_folders, "Model_Selection_Report.csv")
    output_paths["Model_Predict_Pic"] = os.path.join(data_folders, "pic/")
    ############################


    input_paths = read_path(data_folders)
    mylogs = WriteLog(input_paths["log_path"], input_paths["error_path"])
    mylogs.init_logger()
    configs = read_config(input_paths["config_path"], mylogs)

    path_ = output_paths["Model_Predict_Pic"]
    if not os.path.exists(path_):
        os.makedirs(path_)

    return input_paths, output_paths, mylogs, configs

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


def data_path(func_types, data_types, models, model_paths, mylog_):

    # trainPredResult.csv
    # testPredResult.csv

    if func_types in ["Train"]:
        func_folder = "02_Prediction"
    elif func_types in ["Merge"]:
        func_folder = "05_Prediction"
    elif func_types in ["Batch"]:
        func_folder = None
    else:
        mylog_.error("Please check the input of the Function.")

    if data_types == "Train":
        pred_file = "trainPredResult.csv"
    elif data_types == "Test":
        pred_file = "testPredResult.csv"
    else:
        mylog_.error("Please check the input of the Function.")

    if func_types in ["Batch"]:
        data_paths = os.path.join(model_paths, models, pred_file)
    else:
        data_paths = os.path.join(model_paths, models, func_folder, pred_file)

    return data_paths


def data_merge(func_type, data_type, y_paths, model_paths, output_paths, configs, mylogs):

    df_ = read_data(y_paths, mylogs)

    for model in configs["Model_Pred_Name"]:
        # read input file
        mode_pred_path = data_path(func_type, data_type, model, model_paths, mylogs)
        data = read_data(mode_pred_path, mylogs)

        if data.shape[0] != df_.shape[0]:
            mylogs.error("The shapes are different y_real in " + data_type + " and y_pred in " + model)

        col = configs['IDX'].copy()
        for y_col in configs["Y"]:
            y_pred_name = y_col + "_pred"
            y_pred_model_name = y_pred_name + "_" + model
            data.rename(columns={y_pred_name: y_pred_model_name}, inplace=True)
            col.append(y_pred_model_name)

        if len(configs['IDX']) > 0:
            df_ = pd.merge(df_, data[col], on=configs['IDX'])
        elif len(configs['IDX']) == 0:
            df_ = pd.concat([df_, data[col]], axis=1)
            mylogs.warning("No key in this case, data is concat directly")

    df_.to_csv(output_paths, index=False)
    df_["Type"] = data_type
    return df_


def Score_MAPE(y_true, y_pred):
    # mean_absolute_percentage_error
    if 0 in y_true.tolist():
        return Score_MAE(y_true, y_pred)
    return np.mean(np.abs((np.array(y_true) - np.array(y_pred)) / np.array(y_true)))


def Score_MAE(y_true, y_pred):
    return np.mean(np.abs(np.array(y_true) - np.array(y_pred)))


def flag_data(df, col_list, Y_Column, col_tag):
    if math.isnan(df[Y_Column]):
        return np.nan
    diff = [float(abs(df[col] - df[Y_Column])) for col in col_list]
    return col_tag[argmin(diff)]


def Model_Socring(dfs, configs, data_type, mylog):

    data_size = dfs.shape[0]
    evaluation_table = pd.DataFrame()
    for y_col in configs["Y"]:
        col_list = [y_col + "_pred_" + x for x in configs["Model_Pred_Name"]]
        win_col_name = y_col + "_Win"
        dfs[win_col_name] = dfs.apply(flag_data, col_list=col_list, Y_Column=y_col, col_tag=configs["Model_Pred_Name"], axis=1)
        na_amount = dfs[y_col].isna().sum()

        if na_amount == data_size:
            msg = "No real Y(" + y_col + ") in " + data_type + "data"
            mylog.warning(msg)
        elif na_amount < data_size:
            dfs_tmp = dfs[~dfs[y_col].isna()].copy()

            evaluation = pd.DataFrame({"Y" : y_col,
                                       "MAPE": [Score_MAPE(dfs_tmp[y_col], dfs_tmp[y_col + "_pred_" + col]) for col in configs["Model_Pred_Name"]],
                                       "MAE": [Score_MAE(dfs_tmp[y_col], dfs_tmp[y_col + "_pred_" + col]) for col in configs["Model_Pred_Name"]],
                                       "Win": [len(dfs_tmp[win_col_name].loc[dfs_tmp[win_col_name] == col].index) for col in configs["Model_Pred_Name"]],
                                       "Model": configs["Model_Pred_Name"]})

            # NOTE: this function is only deal the loss is lower is better case
            if configs['Model_Selection'][y_col]['Loss_Function'] in ["MAPE", "MAE"]:
                evaluation["Check"] = evaluation[configs['Model_Selection'][y_col]['Loss_Function']] < configs['Model_Selection'][y_col]['Threshold']
                evaluation["Loss_Function"] = configs['Model_Selection'][y_col]['Loss_Function']

        evaluation_table = pd.concat([evaluation_table, evaluation], axis=0)
    evaluation_table["Data"] = data_type

    return evaluation_table


def Model_Judger(dfs, configs, mylogs):

    ### this code does not check if there are at least two models in config["Predict_Model_list"]
    ### this should be checked before this function.

    msg_Models = "OK"
    for y_col in configs["Y"]:
        dfs_y = dfs.loc[dfs["Y"] == y_col].copy()

        loss_func_key = configs['Model_Selection'][y_col]['Loss_Function']
        threshold_pass_idx = dfs_y.loc[dfs_y["Check"]].index
        if threshold_pass_idx.shape[0] == 0:
            fst_model = None
            sec_model = None

        elif threshold_pass_idx.shape[0] == 1:
            fst_model = dfs_y['Model'].iloc[threshold_pass_idx[0]]
            if loss_func_key in ["MAE", "MAPE"]:
                sec_idx = dfs[loss_func_key].nsmallest(2).index.get_level_values(0)[1]
                sec_model = dfs_y['Model'].iloc[sec_idx]

        elif threshold_pass_idx.shape[0] > 1:
            if loss_func_key in ["MAE", "MAPE"]:
                dfs_y = dfs_y.iloc[threshold_pass_idx]
                loss_win = dfs_y['Model'].iloc[dfs_y[loss_func_key].idxmin()]
                amount_win = dfs_y['Model'].iloc[dfs_y['Win'].idxmax()]

                if loss_win == amount_win:
                    fst_model = loss_win
                    sec_idx = dfs_y[loss_func_key].nsmallest(2).index.get_level_values(0)[1]
                    sec_model = dfs_y['Model'].iloc[sec_idx]

                if loss_win != amount_win:
                    fst_model = None
                    sec_model = None

        configs["Model_Selection"][y_col]["Predict_Model"] = fst_model
        configs["Model_Selection"][y_col]["Baseline_Model"] = sec_model

        if fst_model is None:
            mylogs.warning("Model Can't Judge the Predict Model & Baseline Model")
            msg_Models = "NG"

        elif fst_model is not None:
            mylogs.info("The Predict Model of " + y_col + " is " + fst_model)
            mylogs.info("The Baseline Model of " + y_col + " is " + sec_model)


    return configs, msg_Models

def Result_Ouput(df_trains, df_tests, output_path, configs):
    len_ = len(df_tests.index)


    for y in configs["Y"]:
        true_y_len = (~df_tests[y].isnull()).sum()
        pred_list = configs["Model_Pred_Name"]
        fig, ax = plt.subplots(figsize=(10, 6))

        dfs = pd.concat([df_trains, df_tests], ignore_index=True)

        for models in pred_list:
            y_pred_model_name = y + "_pred_" + models
            plt.plot(dfs[y_pred_model_name], label=models)

        if true_y_len == 0 or true_y_len == len_:
            ax.plot(dfs[y], label=y, c='b', linestyle="--")
        else:
            test_idx = np.array(dfs[y].dropna().index.tolist())
            ax.scatter(test_idx, dfs[y].dropna().values, c='b', label=y)

        ax.axvline(x=df_trains.shape[0]-0.5, c='g', label='Train/Test Split Line')
        plt.legend()
        ax.set_title(y)
        plt.savefig(output_path + "prdict_result_" + str(y) + ".png")
        plt.clf()
    return None


def Model_Selection(folder_path, mode):
    Mode = mode # Changed by David the Evil, Bite me
    input_path, output_path, mylog, config = Model_Selection_Prepare(folder_path, Mode)

#    for y in config["Y"]:
#        config["Model_Selection"][y] = {}
#        config["Model_Selection"][y]["Loss_Function"] = "MAPE"
#        config["Model_Selection"][y]["Threshold"] = 0.05

    mylog.info("Module -Model Selection- Start!")

    if Mode in ["Train", "Merge"]:
        df_train_y = data_merge(Mode,
                                "Train",
                                input_path["y_train_path"],
                                input_path["Model_Folder"],
                                output_path['y_pred_merge_train'],
                                config,
                                mylog)

    elif Mode in ["Batch"]:
        if os.path.exists(input_path["y_pred_merge_train"]):
            df_train_y = read_data(input_path["y_pred_merge_train"], mylog)
            # df_train_y.to_csv(output_path["y_pred_merge_train"], index=False)

        if not os.path.exists(input_path["y_pred_merge_train"]):

            model_folder_list = str.split(input_path["Model_Folder"], "/")
            model_folder_list[-2] = "00"
            model_folder_path = os.path.join(*model_folder_list)

            df_train_y = data_merge(Mode,
                                    "Train",
                                    input_path["y_train_path"],
                                    model_folder_path,
                                    output_path['y_pred_merge_train'],
                                    config,
                                    mylog)
    else:
        df_train_y = None

    if Mode in ["Train", "Batch"]:
        df_test_y = data_merge(Mode,
                               "Test",
                               input_path["y_test_path"],
                               input_path["Model_Folder"],
                               output_path['y_pred_merge_test'],
                               config,
                               mylog)

        df_result_train = Model_Socring(df_train_y, config, "Train", mylog)
        df_result_test = Model_Socring(df_test_y, config, "Test", mylog)

    elif Mode in ["Merge"]:
        df_test_y = pd.DataFrame(columns=df_train_y.columns)
        df_result_train = Model_Socring(df_train_y, config, "Train", mylog)
        df_result_test = pd.DataFrame(columns=df_result_train.columns)

    df_y = pd.concat([df_train_y, df_test_y], ignore_index=True)

    df_y["Index_Columns"] = df_y[config["Index_Columns"]].apply(lambda x: '_'.join(str(e) for e in x), axis=1)
    if config['Filter_Feature'] is not None:
        df_y["Filter_Feature"] = df_y[config['Filter_Feature']]
    else:
        df_y["Filter_Feature"] = df_y[config["Index_Columns"]].apply(lambda x: '_'.join(str(e) for e in x), axis=1)

    df_y.to_csv(output_path['y_pred_report_path'], index=False)
    Result_Ouput(df_train_y, df_test_y, output_path["Model_Predict_Pic"], config)

    df_result = pd.concat([df_result_train, df_result_test], ignore_index=True)
    df_result.to_csv(output_path['Model_Selection_Report'], index=False)

    msg_Model = "OK"
    if Mode in ["Train"]:
        config, msg_Model = Model_Judger(df_result_test, config, mylog)
        save_config(input_path["config_path"], config, mylog)
        
        if msg_Model == "OK":
            return msg_Model, config["Model_Selection"][config["Y"][0]]["Predict_Model"]
        else:
            return msg_Model, msg_Model
    #20190717 add Merge mode for retrain, save mae/mape to db
    elif Mode in ["Merge"]:
        return msg_Model, config["Model_Selection"][config["Y"][0]]["Predict_Model"]
            
    mylog.info("Module -Model Selection- Finished!")    

if __name__ == "__main__":
    # path = "../Cases_20190430/Test/07_SelectModel/"

    path = "/home/petertylin/PycharmProjects/00_Projects/00_AVM/AVM_System/Cases/T75R4_20190523/T75R4_20190523/T75R4_20190523_40/00/07_SelectModel/00_Training/"
    Model_Selection(path, Mode="Train")
    path = "/home/petertylin/PycharmProjects/00_Projects/00_AVM/AVM_System/Cases/T75R4_20190523/T75R4_20190523/T75R4_20190523_40/00/07_SelectModel/01_Merge/"
    Model_Selection(path, Mode="Merge")

#------------------------------------------------------------------------------------------------------------------------------------------
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
from MXCI_MYCI_pre import split_MXCI_train_path, get_MXCI_train_path
import os
plt.style.use('ggplot')
plt.ioff()


def MXCI_MYCI_offline(data_folder, mode):
    ####### output path #######
    output_path = {}
    output_path["MXCI_fig_path"] = os.path.join(data_folder, "MXCI_offline.png")
    output_path["MYCI_fig_path"] = os.path.join(data_folder, "MYCI_offline.png")
    output_path["light_fig_path"] = os.path.join(data_folder, "MXCI_MYCI_light_offline.png")
    output_path["MXCI_MYCI_offline_path"] = os.path.join(data_folder, "MXCI_MYCI_offline.csv")
    # output_path["MXCI_x_train_path"] = os.path.join(data_folder, "MXCI_x_train.pkl")
    ############################
    
    input_path = read_path(data_folder)
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])

    config = read_config(input_path["config_path"], mylog)
    CI_config = read_config(input_path["CI_config_path"], mylog)
    
    mylog.info("-----MXCI_MYCI Offline-----")

    ex_cols = config["IDX"].copy()
    try:
        x_data_load, y_data_load, x_train = load_data(input_path, mode, mylog)
    except Exception as e:
        mylog.error("Error while loading data")
        mylog.error(e)
        return "NG", "Please contact APC team to solve the problem."

    filter_feature = config["Filter_Feature"]
    MXCI_MYCI_offline = pd.DataFrame()
    try:
        if filter_feature:
            for ture_Y in config["Y"]:
                Model1, Model2 = load_MYCI_config(config, ture_Y, mylog)
                dirname, base, ext = split_MXCI_train_path(input_path["MXCI_x_train_path"])
                MXCI_keys = [x for x in CI_config['MXCI_exclude_list'].keys()]
                MXCI_keys.sort()
                columns = ["MXCI", "MXCI_Threshold", "Type",
                           ture_Y + "_MYCI",
                           ture_Y + "_MYCI_Threshold",
                           ture_Y + "_Light"
                           ]
                MXCI_MYCI_offline_tmp = pd.DataFrame(columns=columns)

                for feature in MXCI_keys:
                    df_drop_np = load_MXCI_pkl(get_MXCI_train_path(dirname, base, ext, filter_feature, feature), mylog)
                    MXCI_exclude_list, add_exclude_feature = load_MXCI_config(CI_config['MXCI_exclude_list'][feature],
                                                                              config["pre_CI"], mylog)
                    RI_T, y_mean, y_std = load_MYCI_threshold(CI_config["MYCI"][ture_Y][feature], mylog)
                    x_data, y_data, edge_line, ex_data, split = data_preCI(x_data_load, y_data_load,
                                                                           x_train, MXCI_exclude_list, ex_cols,
                                                                           filter_feature, feature, add_exclude_feature,
                                                                           mode, mylog)
                    if x_data.shape[1] == 0:
                        mylog.warning("The features are all dropped during feature reduction, MXCI/MYCI fail in " +
                                      filter_feature + " " + str(feature))
                        continue

                    MXCI_line, MXCI_T_line, MYCI_line, light = MXCI_MYCI_offline_judge(x_data, y_data, Model1, Model2,
                                                                                       RI_T, y_mean, y_std, df_drop_np,
                                                                                       mylog)

                    MXCI_MYCI_offline_tmp = data_collection(MXCI_MYCI_offline_tmp, MXCI_line, MXCI_T_line, MYCI_line,
                                                            RI_T, light, split, ex_cols, ex_data, ture_Y, mylog)

                    mylog.info("MXCI_MYCI calculation: " + filter_feature + " " + str(feature) + " done.")

                MXCI_MYCI_offline = pd.concat([MXCI_MYCI_offline, MXCI_MYCI_offline_tmp], axis=1)
                # ploting(MXCI_MYCI_offline, output_path, filter_feature, MXCI_keys, mylog)

        else:
            for ture_Y in config["Y"]:
                Model1, Model2 = load_MYCI_config(config, ture_Y, mylog)
                feature = None
                columns = ["MXCI", "MXCI_Threshold", "Type",
                           ture_Y + "_MYCI",
                           ture_Y + "_MYCI_Threshold",
                           ture_Y + "_Light"
                           ]
                MXCI_MYCI_offline_tmp = pd.DataFrame(columns=columns)
                df_drop_np = load_MXCI_pkl(input_path["MXCI_x_train_path"], mylog)
                MXCI_exclude_list, add_exclude_feature = load_MXCI_config(CI_config['MXCI_exclude_list']["NoGroup"],
                                                                          config["pre_CI"], mylog)
                RI_T, y_mean, y_std = load_MYCI_threshold(CI_config["MYCI"][ture_Y]["NoGroup"], mylog)
                x_data, y_data, edge_line, ex_data, split = data_preCI(x_data_load, y_data_load,
                                                                       x_train, MXCI_exclude_list, ex_cols,
                                                                       filter_feature, feature, add_exclude_feature,
                                                                       mode, mylog)
                if x_data.shape[1] == 0:
                    mylog.warning("The features are all dropped during feature reduction")
                    return None

                MXCI_line, MXCI_T_line, MYCI_line, light = MXCI_MYCI_offline_judge(x_data, y_data, Model1, Model2, RI_T,
                                                                                   y_mean, y_std, df_drop_np, mylog)

                MXCI_MYCI_offline_tmp = data_collection(MXCI_MYCI_offline_tmp, MXCI_line, MXCI_T_line,
                                                        MYCI_line, RI_T, light, split, ex_cols, ex_data, ture_Y, mylog)
                MXCI_MYCI_offline = pd.concat([MXCI_MYCI_offline, MXCI_MYCI_offline_tmp], axis=1)
                # ploting(MXCI_MYCI_offline, output_path, filter_feature, MXCI_keys, mylog)
    except Exception as e:
        mylog.error("Error while calculating MXCI/MYCI.")
        mylog.error(e)
        return "NG", "Please contact APC team to solve the problem."
###
    if mode == "Batch" and os.path.exists(input_path["MXCI_MYCI_offline_path"]):
        MXCI_MYCI_offline_origin = pd.read_csv(input_path["MXCI_MYCI_offline_path"])
        MXCI_MYCI_offline = pd.concat([MXCI_MYCI_offline_origin, MXCI_MYCI_offline])
###
    try:
        data_saving(MXCI_MYCI_offline, ex_cols, filter_feature, output_path)
    except Exception as e:
        mylog.error("Error while saveing data.")
        mylog.error(e)
        return "NG", "Please contact APC team to solve the problem."
    # MXCI_MYCI_offline.to_csv(output_path["MXCI_MYCI_offline_path"] , index=False)
    mylog.info("MXCI MYCI file is stored at: " + output_path["MXCI_MYCI_offline_path"])
    mylog.info("-----MXCI_MYCI Offline Done-----")

    return "OK", None


def data_saving(MXCI_MYCI_offline, ex_cols, filter_feature, output_path):
    MXCI_MYCI_offline["Index_Columns"] = MXCI_MYCI_offline[ex_cols].apply(lambda x: '_'.join(str(e) for e in x), axis=1)
    if filter_feature:
        MXCI_MYCI_offline["Filter_Feature"] = MXCI_MYCI_offline[filter_feature]
    else:
        MXCI_MYCI_offline["Filter_Feature"] =\
            MXCI_MYCI_offline[ex_cols].apply(lambda x: '_'.join(str(e) for e in x), axis=1)
    MXCI_MYCI_offline.to_csv(output_path["MXCI_MYCI_offline_path"], index=False)
    return None


def load_MXCI_config(MXCI_exclude_list_config, pre_CI_config, mylog):
    try:
        MXCI_exclude_list = MXCI_exclude_list_config.copy()
        add_exclude_feature = pre_CI_config["add_exclude_feature"]
    except Exception as e:
        mylog.error("MXCI MYCI calculation: Error while loading MXCI parameters from config")
        mylog.error_trace(e)
        raise
    return MXCI_exclude_list, add_exclude_feature


def load_MXCI_pkl(MXCI_x_train_path, mylog):
    try:
        df_drop_np = joblib.load(MXCI_x_train_path)
    except Exception as e:
        mylog.error("MXCI MYCI calculation: Error while loading MXCI training data")
        mylog.error_trace(e)
        raise
    return df_drop_np


def load_MYCI_config(config, ture_Y, mylog):
    try:
        Model1 = config["Model_Selection"][ture_Y]["Predict_Model"]
        Model2 = config["Model_Selection"][ture_Y]["Baseline_Model"]
    except Exception as e:
        mylog.error("MXCI MYCI calculation: Error while loading MYCI parameters from config")
        mylog.error_trace(e)
        raise
    Model1 = ture_Y + "_pred_" + Model1
    Model2 = ture_Y + "_pred_" + Model2
    return Model1, Model2


def load_MYCI_threshold(MYCI_config, mylog):
    try:
        RI_T = MYCI_config["RI_T"]
        y_std = MYCI_config["y_std"]
        y_mean = MYCI_config["y_mean"]
    except Exception as e:
        mylog.error("MXCI MYCI calculation: Error while loading MYCI parameters from config")
        mylog.error_trace(e)
        raise
    return RI_T, y_mean, y_std


def fully_load_data(input_path):
    x_train = pd.read_csv(input_path["x_train_path"])
    x_test = pd.read_csv(input_path["x_test_path"])
    y_pred_train = pd.read_csv(input_path["y_pred_merge_train"])
    y_pred_test = pd.read_csv(input_path["y_pred_merge_test"])
    x_data = pd.concat([x_train, x_test]).reset_index(drop=True)
    y_data = pd.concat([y_pred_train, y_pred_test]).reset_index(drop=True)
    return x_data, y_data, x_train


def load_data(input_path, mode, mylog):
    try:
        if mode == "Train":
            x_data, y_data, x_train = fully_load_data(input_path)
        elif mode == "Merge":
            x_data = pd.read_csv(input_path["x_train_path"])
            y_data = pd.read_csv(input_path["y_pred_merge_train"])
            x_train = x_data.copy()
        elif mode == "Batch":
            if not os.path.exists(input_path["MXCI_MYCI_offline_path"]):
                x_data, y_data, x_train = fully_load_data(input_path)
                mylog.info("Merged MXCI_MYCI_offline.csv is missing, predict CI again with merged data.")
            else:
                # print("MXCI_MYCI_offline.csv EXISTS.")
                x_data = pd.read_csv(input_path["x_test_path"])
                y_data = pd.read_csv(input_path["y_pred_merge_test"])
                x_train = x_data.copy()
        else:
            mylog.error("Mode doesn't exist. It should be 'Train', 'Merge' or 'Batch'.")
            raise KeyError
    except Exception as e:
        mylog.error("MXCI MYCI calculation: Error while reading data")
        mylog.error_trace(e)
        raise FileNotFoundError
    return x_data, y_data, x_train


def data_preCI(x_data, y_data, x_train, MXCI_exclude_list, ex_cols, filter_feature, feature, add_exclude_feature, mode, mylog):
    # reduce row
    if filter_feature:
        x_data = x_data[x_data[filter_feature]==int(feature)].copy().reset_index(drop=True)
        y_data = y_data[y_data[filter_feature]==int(feature)].copy().reset_index(drop=True)
        x_train = x_train[x_train[filter_feature]==int(feature)].copy().reset_index(drop=True)

    # get the exclude cols after row reduction
    if ex_cols:
        if add_exclude_feature:
            ex_cols.append(add_exclude_feature)
        ex_data = x_data[ex_cols]
    else:
        ex_data = pd.DataFrame()

    # reduce col
    x_data = x_data.drop(MXCI_exclude_list, axis=1)

    edge_line = len(x_train.index) - 0.5
    train_data_len = len(x_train.index)
    if mode == "Train":
        all_data_len = len(x_data.index)
        split = ["Train"] * train_data_len
        split.extend(["Test"]*(all_data_len - train_data_len))
    elif mode == "Merge":
        split = ["Train"] * train_data_len
    elif mode == "Batch":
        split = ["Test"] * train_data_len
    else:
        mylog.error("Mode doesn't exist. It should be 'Train', 'Merge' or 'Batch'.")
        raise KeyError

    return x_data, y_data, edge_line, ex_data, split


def plot_MXCI(MXCI_line, MXCI_T_line, edge_line, filter_feature, feature, output_path):
    # MXCI
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(MXCI_line, color="b", marker="o", label="MXCI")
    ax.plot(MXCI_T_line, color="r", marker="o", label="MXCI Threshold")
    if edge_line is not None:
        ax.axvline(x=edge_line, c="g", label="Train/Test Split Line")
    ax.legend()
    ax.set_title("MXCI")
    if filter_feature:
        dirname, base, ext = split_MXCI_train_path(output_path["MXCI_fig_path"])
        plt.savefig(get_MXCI_train_path(dirname, base, ext, filter_feature, feature))
    else:
        plt.savefig(output_path["MXCI_fig_path"])
    plt.clf()
    return None


def plot_MYCI(MYCI_line, RI_T, edge_line, filter_feature, feature, output_path):
    # MYCI
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(MYCI_line, color="b", marker="o", label="MYCI")
    ax.axhline(y=RI_T, c="r", label="MYCI Threshold")
    if edge_line is not None:
        ax.axvline(x=edge_line, c="g", label="Train/Test Split Line")
    ax.legend()
    ax.set_title("MYCI")
    if filter_feature:
        dirname, base, ext = split_MXCI_train_path(output_path["MYCI_fig_path"])
        plt.savefig(get_MXCI_train_path(dirname, base, ext, filter_feature, feature))
    else:
        plt.savefig(output_path["MYCI_fig_path"])
    plt.clf()
    return None

def plot_light(light, edge_line, filter_feature, feature, output_path):
    # Light
    fig, ax = plt.subplots(figsize=(10, 6))
    color = ["g", "b", "orange", "r"]
    label = ["Green light", "Blue Light", "Yellow Light", "Red Light"]
    for j in range(4):
        bars = [k for k in light if k == j + 1]
        bars_pos = [i for i, k in enumerate(light) if k == j + 1]
        if len(bars) > 0:
            ax.scatter(bars_pos, bars, color=color[j], label=label[j])
    if edge_line is not None:
        ax.axvline(x=edge_line, c="g", label="Train/Test Split Line")
    ax.legend()
    ax.set_title("MXCI/MYCI Lights")
    if filter_feature:
        dirname, base, ext = split_MXCI_train_path(output_path["light_fig_path"])
        plt.savefig(get_MXCI_train_path(dirname, base, ext, filter_feature, feature))
    else:
        plt.savefig(output_path["light_fig_path"])
    plt.clf()
    return None

def plot_all(MXCI_MYCI_offline, output_path, filter_feature, feature):
    MXCI_line = MXCI_MYCI_offline["MXCI"].values
    MXCI_T_line = MXCI_MYCI_offline["MXCI_Threshold"].values
    MYCI_line = MXCI_MYCI_offline["MYCI"].values
    RI_T = MXCI_MYCI_offline["MYCI_Threshold"].values[0]
    light = MXCI_MYCI_offline["Light"].values
    edge_line = MXCI_MYCI_offline.loc[MXCI_MYCI_offline["Type"] == "Train", "Type"].count() - 0.5

    plot_MXCI(MXCI_line, MXCI_T_line, edge_line, filter_feature, feature, output_path)
    plot_MYCI(MYCI_line, RI_T, edge_line, filter_feature, feature, output_path)
    plot_light(light, edge_line, filter_feature, feature, output_path)
    return None


def ploting(MXCI_MYCI_offline, output_path, filter_feature, MXCI_keys, mylog):
    try:
        if filter_feature:
            for feature in MXCI_keys:
                MXCI_MYCI_offline_tmp = MXCI_MYCI_offline[MXCI_MYCI_offline[filter_feature] == int(feature)]\
                                        .copy().reset_index(drop=True)
                plot_all(MXCI_MYCI_offline_tmp, output_path, filter_feature, feature)
        else:
            plot_all(MXCI_MYCI_offline, output_path, filter_feature)

    except Exception as e:
        mylog.warning("MXCI MYCI calculation: Error while producing plots")
        mylog.warning_trace(e)
    return


def data_collection(MXCI_MYCI_offline, MXCI_line, MXCI_T_line, MYCI_line, RI_T, light, split,
                    ex_cols, ex_data, ture_Y, mylog):
    tmp = pd.DataFrame({
        "MXCI": np.array(MXCI_line),
        "MXCI_Threshold": np.array(MXCI_T_line),
        "Type": np.array(split),
        ture_Y + "_MYCI": np.array(MYCI_line),
        ture_Y + "_MYCI_Threshold": np.array([RI_T] * len(MYCI_line)),
        ture_Y + "_Light": np.array(light)
    })

    if ex_cols:
        for col in ex_cols:
            tmp[col] = ex_data.loc[:, col]

    try:
        MXCI_MYCI_offline = pd.concat([MXCI_MYCI_offline, tmp], ignore_index=True)
    except Exception as e:
        mylog.warning("MXCI MYCI calculation: Error while concating CI data.")

    return MXCI_MYCI_offline


def MXCI_MYCI_offline_judge(x_data, y_data, Model1, Model2, RI_T, y_mean, y_std, df_drop_np, mylog):
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

        if df_drop_np.shape[1] != 0:
            # print(df_drop_np.shape)
            # print(x_test.shape)
            x_df = pd.DataFrame(data=np.expand_dims(x_test, axis=0), columns=x_test.index)
            MXCI_index, GSI, GSI_T = MXCI(df_drop_np, x_df)
            MXCI_line.append(GSI)
            MXCI_T_line.append(GSI_T)
        else:
            mylog.warning("MXCI MYCI calculation: Training data of MXCI is empty. MXCI index fails.")
            MXCI_index = False
            MXCI_line.append(np.nan)
            MXCI_T_line.append(np.nan)

        if MYCI_index:
            if MXCI_index:
                light.append(1) # "Green Light"
            else:
                light.append(2) # "Blue Light"
        else:
            if MXCI_index:
                light.append(3) # "Yellow Light"
            else:
                light.append(4) # "Red Light"
                
    return MXCI_line, MXCI_T_line, MYCI_line, light


def MYCI(RI_T, y_mean, y_std, y_model_1, y_model_2):
    y_model_1_scaled = (y_model_1-y_mean)/y_std
    y_model_2_scaled = (y_model_2-y_mean)/y_std   
    RI = quad(integrand, (y_model_1_scaled+y_model_2_scaled)/2 , np.inf, args=(y_model_1_scaled,y_model_2_scaled))[0]
    
    RI_index = True   
    if RI < RI_T:
        RI_index = False
    
#    print("MYCI threshold is "+"{0:.5f}".format(RI_T)+", and MYCI is "+"{0:.5f}".format(RI))
    return RI_index, RI


def MXCI(df_drop_np, x_df):
    clf_LOF = LocalOutlierFactor(n_neighbors=5, contamination=0.05)
    merge = pd.concat([df_drop_np, x_df])

    clf_LOF.fit_predict(merge.values)
    X_scores = clf_LOF.negative_outlier_factor_
    GSI_T = min(X_scores[:-1])
    GSI_index = True
    if X_scores[-1] < GSI_T:
        GSI_index = False

    return GSI_index, X_scores[-1], GSI_T


#################################################################################################################
def MXCI_MYCI_online_judge(x_data, y_pred, y_base, RI_T, y_mean, y_std, df_drop_np, mylog):
    MYCI_index, MYCI_value = MYCI(RI_T, y_mean, y_std, y_pred, y_base)

    if df_drop_np.shape[1] != 0:
        MXCI_index, MXCI_value, MXCI_T = MXCI(df_drop_np, x_data)
    else:
        mylog.warning("MXCI MYCI calculation: Training data of MXCI is empty. MXCI index fails.")
        MXCI_index = False
        MXCI_value, MXCI_T = None, None

    if MYCI_index:
        if MXCI_index:
            light = 1  # "Green Light"
        else:
            light = 2  # "Blue Light"
    else:
        if MXCI_index:
            light = 3  # "Yellow Light"
        else:
            light = 4  # "Red Light"

    return MXCI_value, MXCI_T, MYCI_value, light


def CI_online_data_preprocess(df_x, MXCI_exclude_list, ex_cols, add_exclude_feature):
    # get the exclude cols after row reduction
    x_data = df_x.copy()
    if ex_cols:
        if add_exclude_feature:
            ex_cols.append(add_exclude_feature)
        ex_data = x_data[ex_cols]
    else:
        ex_data = pd.DataFrame()

    # reduce col
    x_data = x_data.drop(MXCI_exclude_list, axis=1)
    return x_data, ex_data


def CI_online(data_folder, df_x, y_pred, y_base):
    input_path = read_path(data_folder)
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    config = read_config(input_path["config_path"], mylog)
    CI_config = read_config(input_path["CI_config_path"], mylog)
    mylog.online("-----MXCI_MYCI Online-----")
    ex_cols = config["IDX"].copy()
    filter_feature = config["Filter_Feature"]

    if filter_feature:
        dirname, base, ext = split_MXCI_train_path(input_path["MXCI_x_train_path"])
        MXCI_keys = [x for x in CI_config['MXCI_exclude_list'].keys()]
        MXCI_keys.sort()
        feature = df_x[filter_feature][0] 
        df_drop_np = load_MXCI_pkl(get_MXCI_train_path(dirname, base, ext, filter_feature, int(feature)), mylog) #20190708 str->int[02->2]
    else:
        feature = "NoGroup"
        df_drop_np = load_MXCI_pkl(input_path["MXCI_x_train_path"], mylog)
    
    MXCI_exclude_list, add_exclude_feature = load_MXCI_config(CI_config['MXCI_exclude_list'][feature],
                                                              config["pre_CI"], mylog)
        
    RI_T, y_mean, y_std = load_MYCI_threshold(CI_config["MYCI"][config["Y"][0]][feature], mylog)
    x_data, ex_data = CI_online_data_preprocess(df_x, MXCI_exclude_list, ex_cols, add_exclude_feature)
    MXCI_value, MXCI_T, MYCI_value, light = MXCI_MYCI_online_judge(x_data, y_pred, y_base,
                                                                   RI_T, y_mean, y_std, df_drop_np, mylog)
    return MXCI_value, MXCI_T, MYCI_value, RI_T, light

if __name__ == "__main__":
    # path = "../Cases/CVD2E_Split1_Test/CVD2E_Split1_Test_00/00/08_CI/"
    path = "../Cases/PSH_Demo/PSH_00/00/08_CI/"
    MXCI_MYCI_offline(path)

