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

#-------------------------------------------------------------------------------------------------------------
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
from json2csv import json2csv
import matplotlib.pyplot as plt


def pre_MXCI_MYCI(data_folder):
    ####### output path #######
    output_path = {}
    output_path["MXCI_fig_path"] = os.path.join(data_folder, "MXCI_offline.png")
    output_path["MYCI_fig_path"] = os.path.join(data_folder, "MYCI_offline.png")
    output_path["light_fig_path"] = os.path.join(data_folder, "MXCI_MYCI_light_offline.png")
    output_path["MXCI_MYCI_offline_path"] = os.path.join(data_folder, "MXCI_MYCI_offline.csv")
    output_path["MXCI_x_train_path"] = os.path.join(data_folder, "MXCI_x_train.pkl")
    output_path["CI_config_path"] = os.path.join(data_folder, "CI_config.json")
    ############################
    
    input_path = read_path(data_folder)
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    # mylog.init_logger()
    config = read_config(input_path["config_path"], mylog)
    mylog.info("-----MXCI_MYCI preparation-----")

    try:        
        x_train = pd.read_csv(input_path["x_train_path"])
        y_pred_merge_train = pd.read_csv(input_path["y_pred_merge_train"])
    except Exception as e:
        mylog.error("pre-MXCI MYCI calculation: Error while read training data")
        mylog.error_trace(e)
        return "NG", "Please contact APC team to solve the problem."
        # raise

    # MXCI
    try:
        filter_feature = config["Filter_Feature"]
        add_exclude_feature = config["pre_CI"]["add_exclude_feature"]
    except Exception as e:
        mylog.error("pre-MXCI MYCI calculation (MXCI): Error while reading parameters in config")
        mylog.error_trace(e)
        return "NG", "Please contact APC team to solve the problem."
        # raise

    try:
        exlude_list = config["IDX"].copy()
        ex_list = preMXCI(x_train, exlude_list, mylog, filter_feature, output_path["MXCI_x_train_path"], add_exclude_feature)
        CI_config = {}
        CI_config['MXCI_exclude_list'] = ex_list
    except Exception as e:
        mylog.error("pre-MXCI MYCI calculation: Error while loading parameters from preMXCI")
        mylog.error_trace(e)
        return "NG", "Please contact APC team to solve the problem."
        # raise

    # MYCI
    try:
        CI_config['MYCI'] = {}
        for ture_Y in config["Y"]:
            Model1 = config["Model_Selection"][ture_Y]["Predict_Model"]
            Model2 = config["Model_Selection"][ture_Y]["Baseline_Model"]
            RI_info = preMYCI(y_pred_merge_train, ture_Y, Model1, Model2, mylog, filter_feature)
            CI_config['MYCI'][ture_Y] = RI_info
    except Exception as e:
        mylog.error("pre-MXCI MYCI calculation (MYCI): Error while calculating preMYCI")
        mylog.error_trace(e)
        return "NG", "Please contact APC team to solve the problem."
        # raise

    save_config(input_path["config_path"], config, mylog)
    save_config(output_path["CI_config_path"], CI_config, mylog)

    json2csv(input_path["config_path"])
    json2csv(output_path["CI_config_path"])

    mylog.info("-----Pre-MXCI/MYCI Calculation Done-----")
    return "OK", None


def preMYCI(df_y, ture_Y, Model1, Model2 , mylog, filter_feature):
    df_y_c = df_y.copy()

    MYCI_dict = {}
    if filter_feature:
        filter_feature_list = df_y_c[filter_feature].unique().tolist()

        for feature in filter_feature_list:
            mylog.info("---MYCI info:" + filter_feature + "  " + str(feature) + " ---")
            try:
                df_y_c_few = df_y_c.loc[df_y_c[filter_feature] == feature].reset_index(drop=True)
            except Exception as e:
                mylog.error("pre-MYCI calculation: Error while selecting feature in y.")
                mylog.error("Please check if feature exists in the prediction result.")
                mylog.error_trace(e)
                raise
            # print(df_y_c_few.head())
            MYCI_dict[feature] = preMYCI_2(df_y_c_few, ture_Y, Model1, Model2, mylog)
        return MYCI_dict

    else:
        mylog.info("---MYCI info---")
        MYCI_dict["NoGroup"] = preMYCI_2(df_y_c, ture_Y, Model1, Model2, mylog)
        return MYCI_dict


def preMYCI_2(df_y, ture_Y, Model1, Model2 , mylog):
    try:
        y_mean = df_y[ture_Y].mean()
        y_std = df_y[ture_Y].std()
        
        y_model_1 = df_y[ture_Y+"_pred_"+Model1]
        y_model_2 = df_y[ture_Y+"_pred_"+Model2]

    except Exception as e:
        mylog.error("pre-MYCI calculation: Error while reading y_train table ")
        mylog.error_trace(e)
        raise    

    y_model_1_scaled = (y_model_1-y_mean)/y_std
    y_model_2_scaled = (y_model_2-y_mean)/y_std
    RI_train = []
    for i in range(len(y_model_1_scaled.values.tolist())):
        RI_train.append(quad(integrand, (y_model_1_scaled[i]+y_model_2_scaled[i])/2 , np.inf, args=(y_model_1_scaled[i],y_model_2_scaled[i]))[0])

    # print(RI_train)
    RI_T = min(RI_train)
    # print(RI_T)

    mylog.info("mean: "+str(y_mean))
    mylog.info("std: "+str(y_std))
    mylog.info("Threshold: "+str(RI_T))
    preMYCI_dict = dict()
    preMYCI_dict["RI_T"] = RI_T
    preMYCI_dict["y_mean"] = y_mean
    preMYCI_dict["y_std"] = y_std
    return preMYCI_dict


def split_MXCI_train_path(MXCI_x_train_path):
    dirname, basename = os.path.split(MXCI_x_train_path)
    base, ext = os.path.splitext(basename)
    return dirname, base, ext


def get_MXCI_train_path(dirname, base, ext, filter_feature, feature):
    return os.path.join(dirname, base+"_"+filter_feature+"_"+str(feature)+ext)


def preMXCI(x_train, exlude_list, mylog, filter_feature, MXCI_x_train_path, add_exclude_feature):
    df_train_drop = x_train.copy()
    MXCI_exclude_dict = {}
    if filter_feature:
        try:
            filter_feature_list = df_train_drop[filter_feature].unique().tolist()
            filter_feature_list.sort()
        except Exception as e:
            mylog.error("pre-MXCI calculation: Error while selecting feature in x.")
            mylog.error("Please check if feature exists in the x data.")
            mylog.error_trace(e)
            raise
        try:
            dirname, base, ext = split_MXCI_train_path(MXCI_x_train_path)
            for feature in filter_feature_list:
                mylog.info("---MXCI info: " + filter_feature + "  " + str(feature) + " ---")
                df_train = df_train_drop.loc[df_train_drop[filter_feature] == feature].copy().reset_index(drop=True)
                MXCI_exclude_list, df_drop = preMXCI_2(df_train, exlude_list, mylog, add_exclude_feature)
                MXCI_exclude_dict[feature] = MXCI_exclude_list
                joblib.dump(df_drop, get_MXCI_train_path(dirname, base, ext, filter_feature, feature))
        except Exception as e:
            mylog.error("pre-MXCI calculation: Error while calculating MXCI_exclude_list.")
            mylog.error_trace(e)
            raise
        return MXCI_exclude_dict

    else:
        mylog.info("---MXCI info---")
        try:
            MXCI_exclude_list ,df_drop = preMXCI_2(df_train_drop, exlude_list, mylog, add_exclude_feature)
            MXCI_exclude_dict["NoGroup"] = MXCI_exclude_list
        except Exception as e:
            mylog.error("pre-MXCI calculation: Error while calculating MXCI_exclude_list.")
            mylog.error_trace(e)
            raise
        joblib.dump(df_drop, MXCI_x_train_path)
        return MXCI_exclude_dict


def preMXCI_2(x_train, ex_list, mylog, add_exclude_feature):
    df_train = x_train.copy()
    df_train_size = df_train.shape

    exlude_list = ex_list.copy()
    if add_exclude_feature:
        exlude_list.extend(add_exclude_feature)
    for exclude in exlude_list:
        if exclude in x_train.columns:
            df_train = df_train.drop([exclude], axis=1)

    # drop the constant cols
    constant_col = []
    cols = df_train.columns
    for col in cols:
        if len(df_train[col].unique().tolist()) == 1:
            constant_col.append(col)
    if constant_col:
        df_const = df_train[constant_col]
        df_train = df_train.drop(constant_col, axis=1)

    # correlation coef filter
    coef = 0.7
    R = df_train.corr().values
    df_R = pd.DataFrame(data=R)
    param_size = len(df_train.columns.tolist())
    remove_list = []

    for i in range(param_size):
        idx = df_R[i].loc[abs(df_R[i]) > coef].index.tolist()
        idx.remove(i)
        remove_list.extend(idx)
    remove_list = list(set(remove_list))
    #    print("length of removal list", len(remove_list))
    #    print("number of left params", len(df_R.index)-len(remove_list))
    remove_index = df_train.columns[[remove_list]].tolist()
    df_train = df_train.drop(columns=remove_index, axis=1)
    if constant_col:
        df_train = pd.concat([df_train, df_const], axis=1)

    mylog.info("Exluded features (" + str(len(exlude_list)) + "):")
    mylog.info(",".join(str(e) for e in exlude_list))
    mylog.info("Constant features(" + str(len(constant_col)) + "):")
    mylog.info(",".join(str(e) for e in constant_col))
    mylog.info("Highly correlated features (" + str(len(remove_index)) + "):")
    mylog.info(",".join(str(e) for e in remove_index))
    # mylog.info("Total number of excluded list: " + str(len(exlude_list) + len(constant_col) + len(remove_index)))
    mylog.info("Total number of excluded list: " + str(len(exlude_list) + len(remove_index)))
    mylog.info("Size of the original data: " + str(df_train_size))
    mylog.info("Size of the left data: " + str(df_train.shape))
    if df_train.shape[1] == 0:
        mylog.warning("There is no feature left. MXCI cannot work properly.")

    MXCI_exclude_list = exlude_list
    # MXCI_exclude_list.extend(constant_col)
    MXCI_exclude_list.extend(remove_index)

    return MXCI_exclude_list, df_train


if __name__ == "__main__":
    # path = "../Cases/CVD2E_Split1_Test/CVD2E_Split1_Test_00/00/08_CI/"
    path = "../Cases/PSH_Demo/PSH_00/00/08_CI/"
    pre_MXCI_MYCI(path)

#---------------------------------------------------------------------------------------------------------------------
#Path.py
# -*- coding: utf-8 -*-
import os, json
import traceback
from shutil import copyfile

class All_path():
    def __init__(self, root, config_file, path_config=None):
        self.root = root
        self.root_name = os.path.basename(os.path.abspath(self.root))
        norm_root = os.path.normpath(self.root)
        self.up_root = os.path.split(norm_root)[0]
        self.raw_data_default_name = "raw_data.csv"
        self.config_default_name = "config.json"
        self.sys_log_default_name = "System.log"
        self.config_file = config_file
        self.sub_path = ["00_Model_building", "01_Test", "02_Merge", "03_Test"]
        self.model_steps = ["00_Parameter_tuning", "01_Model_building", "02_Prediction",
                            "03_Merge_Parameter_tuning", "04_Merge_Model_building", "05_Prediction"]
        self.structure_list = ["00_Config/", "01_OriginalData/", "02_DataPreview/", "03_PreprocessedData/", "04_XDI/",
                               "05_YDI/", "06_Model/", "07_SelectModel/", "08_CI/", "99_LOG/"]
        self.select_model_dir = ["00_Training", "01_Merge"]
        self.retrain_digit = str(4)
        self.retrain_format = "{0:0" + self.retrain_digit + "d}"

        try:
            with open(self.config_file) as json_data:
                config = json.load(json_data)
        except Exception as e:
            error_path = os.path.join(self.root, "error.log")
            with open(error_path, 'a') as file:
                file.write("Error while reading config file\n")
                file.write(str(e) + "\n")
                file.write(traceback.format_exc())
                raise

        try:
            self.model_pred_name = config["Model_Pred_Name"]
        except Exception as e:
            error_path = os.path.join(self.root, "error.log")
            with open(error_path, 'a') as file:
                file.write("Error while reading 'Model_Pred_Name' in config\n")
                file.write(str(e) + "\n")
                file.write(traceback.format_exc())
                raise

        if path_config is None:
            self.path_config = {}

        else:
            self.path_config = path_config

        if self.root_name not in self.path_config:
            self.path_config[self.root_name] = {}
            self.path_config[self.root_name]["main"] = self.root

##############################################################################################################
    def get_current_retrain_number(self):
        retrain_list = []
        for filename in os.listdir(self.root):
            if os.path.isdir(os.path.join(self.root, filename)):
                retrain_list.append(filename)
        retrain_list = [dir[-2:] for dir in retrain_list if self.root_name in dir]
        if retrain_list:
            retrain_list = list(map(int, retrain_list))
            return max(retrain_list)
        else:
            return None


    def get_current_batch_number(self, retrain_folder_path):
        batch_list = os.listdir(retrain_folder_path)
        if batch_list:
            batch_list = list(map(int, batch_list))
            return max(batch_list)
        else:
            return None


    def retrain_check(self):
        num = self.get_current_retrain_number()
        if num is None:
            self.retrain_folder = os.path.join(self.root, self.root_name+"_0000")
            num = 0
        else:
            self.retrain_folder = os.path.join(self.root, self.root_name + "_" +
                                               self.retrain_format.format(num+1))
            num += 1

        os.makedirs(self.retrain_folder)

        self.path_config[self.root_name][str(num)] = {}
        self.path_config[self.root_name][str(num)]["main"] = self.retrain_folder
        self.retrain_num = num
        return self.retrain_folder


    def batch_check(self, retrain_folder=None):
        if retrain_folder is None:
            num = self.get_current_retrain_number()
            if num is None:
                raise SystemError("No retrain folder is created.")
            self.retrain_folder = os.path.join(self.root, self.root_name + "_" +
                                               self.retrain_format.format(num))
            self.retrain_num = num
        else:
            self.retrain_folder = retrain_folder
            self.retrain_num = int(retrain_folder[-2:])

        batch_list = os.listdir(self.retrain_folder)

        if not batch_list:
            self.data_folder = os.path.join(self.retrain_folder, "00")
            num = 0
        else:
            batch_list = list(map(int, batch_list))
            num = max(batch_list) + 1
            self.data_folder = os.path.join(self.retrain_folder, "{0:02d}".format(num))

        os.makedirs(self.data_folder)

        self.path_config[self.root_name][str(self.retrain_num)][str(num)] = {}
        self.path_config[self.root_name][str(self.retrain_num)][str(num)]["main"] = self.data_folder
        self.batch_num = num

        return self.data_folder

##############################################################################################################

    def specify_path(self, retrain_num, batch_num, batch_flag=None):
        self.retrain_num = retrain_num
        self.batch_num = batch_num
        self.retrain_folder = os.path.join(self.root, self.root_name + "_" +
                                           self.retrain_format.format(retrain_num))
        self.data_folder = os.path.join(self.retrain_folder, "{0:02d}".format(batch_num))
        self.get_folder_path(batch_flag)
        return self.data_folder

##############################################################################################################
    def get_folder_path(self, batch_flag=None):
        # As self.data_folder exists
        self.main_path = {}
        out_path = self.path_config[self.root_name][str(self.retrain_num)][str(self.batch_num)]
        for i in range(len(self.structure_list)-1):
            self.main_path[i] = {}
            self.main_path[i]["main"] = os.path.join(self.data_folder, self.structure_list[i])
            out_path[str(i)] = {}
            out_path[str(i)]["main"] = self.main_path[i]["main"]

        self.main_path[99] = {}
        self.main_path[99]["main"] = os.path.join(self.data_folder, "99_LOG/")
        out_path[str(99)] = {}
        out_path[str(99)]["main"] = self.main_path[99]["main"]

        if batch_flag is None:
            for i in [3,4,5,8]:
                for idx, sub_path in enumerate(self.sub_path):
                    self.main_path[i][sub_path] = os.path.join(self.main_path[i]["main"], sub_path)
                    out_path[str(i)][str(idx)] = self.main_path[i][sub_path]

            for idx, dir_ in enumerate(self.select_model_dir):
                self.main_path[7][dir_] = os.path.join(self.main_path[7]["main"], dir_)
                out_path[str(7)][str(idx)] = self.main_path[7][dir_]

        for name in self.model_pred_name:
            self.main_path[6][name] = {}
            self.main_path[6][name]["main"] = os.path.join(self.main_path[6]["main"], name)
            out_path[str(6)][name] = {}
            out_path[str(6)][name]["main"] = self.main_path[6][name]["main"]
            if batch_flag is None:
                for idx, model_step in enumerate(self.model_steps):
                    self.main_path[6][name][model_step] = os.path.join(self.main_path[6][name]["main"], model_step)
                    out_path[str(6)][name][str(idx)] = self.main_path[6][name][model_step]
        return None

    def get_path_config(self):
        return self.path_config

    def save_path_config(self, path):
        in_path = os.path.join(path, "path_config.json")
        with open(in_path , 'w') as fp:
            json.dump(self.path_config, fp, indent=4, sort_keys=True)
        return None

    def init_folders(self, batch_flag=None):
        paths = [self.main_path[i]["main"] for i in range(9)]
        paths.append(self.main_path[99]["main"])

        if batch_flag is None:
            for sub_path in self.sub_path:
                tmp_path = [self.main_path[i][sub_path] for i in [3, 4, 5, 8]]
                paths.extend(tmp_path)

            tmp_path = [self.main_path[7][dir_] for dir_ in self.select_model_dir]
            paths.extend(tmp_path)

            for name in self.model_pred_name:
                for model_step in self.model_steps:
                    paths.append(self.main_path[6][name][model_step])
        else:
            for name in self.model_pred_name:
                paths.append(self.main_path[6][name]["main"])

        for path in paths:
            if not os.path.exists(path):
                os.makedirs(path)
        return None


    def get_file_path(self, merge_flag=None):
        if merge_flag is None:
            path_0 = self.sub_path[0]
            model_path_1 = self.model_steps[0]
            model_path_2 = self.model_steps[1]
            select_path = self.select_model_dir[0]
        else:
            path_0 = self.sub_path[2]
            model_path_1 = self.model_steps[3]
            model_path_2 = self.model_steps[4]
            select_path = self.select_model_dir[1]

        # General
        # 00_config
        self.config_path = os.path.join(self.main_path[0]["main"], self.config_default_name)
        # 01_OriginalData
        self.raw_path = os.path.join(self.main_path[1]["main"], self.raw_data_default_name)
        # 99_LOG
        self.sys_log_path = os.path.join(self.main_path[99]["main"], self.sys_log_default_name)

        # Output of each folder
        # 02_DataReview/
#        self.missing_path = os.path.join(self.main_path[2]["main"], "missing_rate_list.csv")
#        self.outspec_path = os.path.join(self.main_path[2]["main"], "outspec_table.csv")
#        self.summary_path = os.path.join(self.main_path[2]["main"], "summary_table.csv")

        # 03_PreprocessedData/
        # Model_building
        self.data_train_path = os.path.join(self.main_path[3][path_0], 'Data_Train.csv')
        self.data_test_path  = os.path.join(self.main_path[3][path_0], 'Data_Test.csv')
        self.x_train_path  = os.path.join(self.main_path[3][path_0], 'x_Train.csv')
        self.y_train_path   = os.path.join(self.main_path[3][path_0], 'y_Train.csv')
        self.Cat_config_path = os.path.join(self.main_path[3][path_0], 'Cat_Config.json')
        self.DataImpute_config_path = os.path.join(self.main_path[3][path_0], 'DataImpute_Config.json')
        self.DataTrans_config_path = os.path.join(self.main_path[3][path_0], 'DataTrans_Config.json')
        self.DelMissingCol_config_path = os.path.join(self.main_path[3][path_0], 'DelMissingCol_Config.json')
        self.DelNonNumCol_config_path = os.path.join(self.main_path[3][path_0], 'DelNonNumCol_Config.json')

        # 04_XDI/
        # Model_building
        self.XDI_PCA_path            = os.path.join(self.main_path[4][path_0], "XDI_PCA.pkl")
        self.XDI_DataTrans_path = os.path.join(self.main_path[4][path_0], "XDI_DataTrans.pkl")
        self.XDI_PreWork_DataTrans_path = os.path.join(self.main_path[4][path_0], "XDI_PreWork_DataTrans.pkl")

        # 05_YDI/
        # Model_building
        # self.YDI_threshold_table_path   = os.path.join(self.main_path[5][self.sub_path_model], "YDI_threshold_table.csv")
        # self.YDI_Clustering_path        = os.path.join(self.main_path[5][self.sub_path_model], "YDI_Clustering.pkl")
        self.YDI_Group = os.path.join(self.main_path[5][path_0], "YDI_Group/")
        self.YDI_threshold_table_path = os.path.join(self.main_path[5][path_0], "YDI_threshold_table.csv")
        self.YDI_PreWork_DataTrans = os.path.join(self.main_path[5][path_0], "YDI_PreWork_DataTrans.pkl")

        # 06_Model
        for name in self.model_pred_name:
            if name == "XGB":
                self.xgb_tuning = os.path.join(self.main_path[6][name][model_path_1], "Parameter_aftertuning.csv")
                self.xgb_model = os.path.join(self.main_path[6][name][model_path_2], "XGB.model")
                self.xgb_parameter_path = os.path.join(self.main_path[6][name][model_path_1], "xgb_parameter.json")

            elif name == "PLS":
                self.pls_train_usp = os.path.join(self.main_path[6][name][model_path_2], "Train.usp")

        # 07_SelectModel/
        self.y_pred_merge_train = os.path.join(self.main_path[7][select_path], "y_pred_merge_train.csv")
        self.y_pred_merge_test = os.path.join(self.main_path[7][select_path], "y_pred_merge_test.csv")

        # 08_CI/
        # Model_building
        self.MXCI_x_train_path = os.path.join(self.main_path[8][path_0], "MXCI_x_train.pkl")
        self.CI_config_path = os.path.join(self.main_path[8][path_0], "CI_config.json")

        return None

    def get_batch_file_path(self, batch_flag=None, merge_flag=None):
        # model_path = self.model_steps[2] / self.model_steps[5]
        # sub_path = self.sub_path[1] / self.sub_path[3]
        if merge_flag is None:
            sub_path = self.sub_path[1]
            model_path = self.model_steps[2]
        else:
            sub_path = self.sub_path[3]
            model_path = self.model_steps[5]
        # batch data will be saved in different places
        self.save_path = {}
        self.save_path[6] = {}
        if batch_flag is None:
            for i in [3,4,5,8]:
                self.save_path[i] = self.main_path[i][sub_path]
            self.save_path[7] = self.main_path[7]["main"]
            for name in self.model_pred_name:
                self.save_path[6][name] = self.main_path[6][name][model_path]
        else:
            for i in [3,4,5,7,8]:
                self.save_path[i] = self.main_path[i]["main"]
            for name in self.model_pred_name:
                self.save_path[6][name] = self.main_path[6][name]["main"]

        # 99_LOG
        self.sys_log_path = os.path.join(self.main_path[99]["main"], self.sys_log_default_name)

        # 03_PreprocessedData/
        self.y_test_path  = os.path.join(self.save_path[3], 'y_Test.csv')
        self.x_test_path  = os.path.join(self.save_path[3], 'x_Test.csv')

        # 04_XDI/
        # self.XDI_offline_pic_path    = os.path.join(self.save_path[4], "XDI.png")
        # self.XDI_offline_path        = os.path.join(self.save_path[4], "XDI_offline.csv")
        self.XDI_offline_path = os.path.join(self.retrain_folder, "00", self.structure_list[4], self.sub_path[3],
                                             "XDI_offline.csv")

        # 05_YDI/
        # self.YDI_offline_path = os.path.join(self.save_path[5], "YDI_offline.csv")
        # self.YDI_offline_pic_path       = os.path.join(self.save_path[5], "YDI.png")


        # 06_Model

        # for name in self.model_pred_name:
        #     if name == "XGB":
        #         self.xgb_predict_feature_score = os.path.join(self.save_path[6][name], "FeatureScore.csv")
        #         self.xgb_predict_importance_10 = os.path.join(self.save_path[6][name], "Importance10.jpg")
        #         self.xgb_predict_importance_30 = os.path.join(self.save_path[6][name], "Importance30.jpg")
        #         self.xgb_predict_test = os.path.join(self.save_path[6][name], "testPredResult.csv")
        #         self.xgb_predict_train = os.path.join(self.save_path[6][name], "trainPredResult.csv")
        #
        #     elif name == "PLS":
        #         self.pls_predict_test = os.path.join(self.save_path[6][name], "testPredResult.csv")
        #         self.pls_predict_train = os.path.join(self.save_path[6][name], "trainPredResult.csv")
        #         self.pls_train_xlsx = os.path.join(self.save_path[6][name], "Train.xlsx")
        #         # self.pls_train_usp = os.path.join(self.save_path[6][name], "Train.usp")
        #         self.pls_test_xlsx = os.path.join(self.save_path[6][name], "Test.xlsx")
        #         self.pls_pred_test = os.path.join(self.save_path[6][name], "PredTest.xlsx")

        # 07_SelectModel/
        if batch_flag is not None:
            self.y_pred_merge_train = os.path.join(self.save_path[7], "y_pred_merge_train.csv")
            self.y_pred_merge_test = os.path.join(self.save_path[7], "y_pred_merge_test.csv")
            self.y_pred_merge_train_special = os.path.join(self.retrain_folder, "00", self.structure_list[7],
                                                           self.select_model_dir[1], "y_pred_merge_train.csv")

        # 08_CI/
        # self.MXCI_fig_path = os.path.join(self.save_path[8], "MXCI_offline.png")
        # self.MYCI_fig_path = os.path.join(self.save_path[8], "MYCI_offline.png")
        # self.light_fig_path = os.path.join(self.save_path[8], "MXCI_MYCI_light_offline.png")
        # self.MXCI_MYCI_offline_path = os.path.join(self.main_path[8][sub_path], "MXCI_MYCI_offline.csv")
        self.MXCI_MYCI_offline_path = os.path.join(self.retrain_folder, "00", self.structure_list[8], self.sub_path[3],
                                                   "MXCI_MYCI_offline.csv")
        return None

##############################################################################################################
    def training_ext_check(self):
        self.file_exist_check(self.root, self.training_data, True)
        self.file_exist_check(self.root, self.config_file, True)
        self.file_extension_check(self.root, ".csv", self.training_data, True)
        self.file_extension_check(self.root, ".json", self.config_file, True)
        return None

    def batch_ext_check(self, batch_data_list):
        left_list = []
        for batch in batch_data_list:
            idx1 = self.file_exist_check(self.root, batch, False)
            idx2 = self.file_extension_check(self.root, ".csv", batch, False)
            if idx1 and idx2:
                left_list.append(batch)
        return left_list

    def split_path(self, path):
        dirname, basename = os.path.split(path)
        base, ext = os.path.splitext(basename)
        return dirname, base, ext

    def rename_path(self, source_path):
        dirname, base, ext = self.split_path(source_path)
        new_path = os.path.join(dirname, base + "_copy" + ext)
        os.rename(source_path, new_path)
        return new_path


    def file_extension_check(self, error_folder, ext, file, raise_flag):
        extention = os.path.splitext(file)[-1].lower()
        if extention != ext:
            error_path = os.path.join(error_folder, "error.log")
            with open(error_path, 'a') as file:
                file.write(os.path.basename(file) + " should be "+ ext +" file\n")
                print(os.path.basename(file) + " should be "+ ext +" file")
            if raise_flag:
                raise FileNotFoundError
            return False
        return True

    def file_exist_check(self, error_folder, file, raise_flag):
        if not os.path.exists(file):
            error_path = os.path.join(error_folder, "error.log")
            with open(error_path, 'a') as f:
                f.write("Error while loading file: "+ file +"\n")
                print("Error while loading file: "+ file)
                if raise_flag:
                    raise FileNotFoundError
            return False
        return True


##############################################################################################################
    def get_train_file(self):
        raw_path = os.path.join(self.main_path[1]["main"], self.raw_data_default_name)
        config_path = os.path.join(self.main_path[0]["main"], self.config_default_name)
        try:
            # os.rename(self.raw_name, raw_path)
            # os.rename(self.config_name, config_path)
            copyfile(self.training_data, raw_path)
            copyfile(self.config_file, config_path)
        except Exception as e:
            error_path = os.path.join(self.data_folder, "error.log")
            with open(error_path, 'a') as file:
                file.write("Fail to rename file:\n")
                file.write(str(e)+"\n")
                file.write(traceback.format_exc())            
            raise
        return None

    def get_batch_file(self, batch, batch_num):
        sys_path = os.path.join(self.root, "System.log")
        self.check_previous_file()
        try:
            with open(sys_path, 'a') as file:
                self.batch_name = "batch_data_"+"{0:02d}".format(batch_num)+".csv"
                self.old_path = self.raw_path
                self.raw_path = os.path.join(self.main_path[1]["main"], self.batch_name)
                # self.batch_path = os.path.join(self.root, self.batch_name)
                copyfile(batch, self.raw_path)
                file.write("Batch comparison :" + os.path.basename(batch)+" , "+"batch_data_"+
                           "{0:02d}".format(batch_num) + ".csv\n")
        except:
            error_path = os.path.join(self.root, "error.log")
            with open(error_path, 'a') as file:
                file.write("Fail to copy the batch:" + batch + "\n")
            raise FileNotFoundError

        return None


    def check_previous_file(self):
        if not hasattr(self, "raw_path"):
            self.raw_path = os.path.join(self.retrain_folder, "00", self.structure_list[1],
                                         self.raw_data_default_name)
        else:
            print("raw_path")
        if not hasattr(self, "config_path"):
            self.config_path = os.path.join(self.retrain_folder, "00", self.structure_list[0],
                                            self.config_default_name)
        else:
            print("config_path")

        if not hasattr(self, "x_train_path"):
            self.x_train_path = os.path.join(self.retrain_folder, "00", self.structure_list[3],
                                             self.sub_path[2], "x_Train.csv")
        else:
            print("x_train_path")

        if not hasattr(self, "y_train_path"):
            self.y_train_path = os.path.join(self.retrain_folder, "00", self.structure_list[3],
                                             self.sub_path[2], "y_Train.csv")
        else:
            print("y_train_path")

        if not hasattr(self, "DataImpute_config_path"):
            self.DataImpute_config_path = os.path.join(self.retrain_folder, "00", self.structure_list[3],
                                                       self.sub_path[2], "DataImpute_Config.json")
        else:
            print("DataImpute_config_path")

        if not hasattr(self, "DataTrans_config_path"):
            self.DataTrans_config_path = os.path.join(self.retrain_folder, "00", self.structure_list[3],
                                                      self.sub_path[2], "DataTrans_Config.json")
        else:
            print("DataTrans_config_path")

        if not hasattr(self, "DelMissingCol_config_path"):
            self.DelMissingCol_config_path = os.path.join(self.retrain_folder, "00", self.structure_list[3],
                                                          self.sub_path[2], "DelMissingCol_Config.json")
        else:
            print("DelMissingCol_config_path")

        if not hasattr(self, "DelNonNumCol_config_path"):
            self.DelNonNumCol_config_path = os.path.join(self.retrain_folder, "00", self.structure_list[3],
                                                         self.sub_path[2], "DelNonNumCol_Config.json")
        else:
            print("DelNonNumCol_config_path")

        if not hasattr(self, "Cat_config_path"):
            self.Cat_config_path = os.path.join(self.retrain_folder, "00", self.structure_list[3],
                                                self.sub_path[2], "Cat_Config.json")
        else:
            print("Cat_config_path")

        if not hasattr(self, "XDI_PCA_path"):
            self.XDI_PCA_path = os.path.join(self.retrain_folder, "00", self.structure_list[4],
                                             self.sub_path[2], "XDI_PCA.pkl")
        else:
            print("XDI_PCA_path")

        if not hasattr(self, "XDI_DataTrans_path"):
            self.XDI_DataTrans_path = os.path.join(self.retrain_folder, "00", self.structure_list[4],
                                                   self.sub_path[2], "XDI_DataTrans.pkl")
        else:
            print("XDI_DataTrans_path")

        if not hasattr(self, "XDI_PreWork_DataTrans_path"):
            self.XDI_PreWork_DataTrans_path = os.path.join(self.retrain_folder, "00", self.structure_list[4],
                                                           self.sub_path[2], "XDI_PreWork_DataTrans.pkl")
        else:
            print("XDI_PreWork_DataTrans_path")

        if not hasattr(self, "YDI_Group"):
            self.YDI_Group = os.path.join(self.retrain_folder, "00", self.structure_list[5],
                                          self.sub_path[2], "YDI_Group/")
        else:
            print("YDI_Group")

        if not hasattr(self, "YDI_threshold_table_path"):
            self.YDI_threshold_table_path = os.path.join(self.retrain_folder, "00", self.structure_list[5],
                                                         self.sub_path[2], "YDI_threshold_table.csv")
        else:
            print("YDI_threshold_table_path")

        if not hasattr(self, "YDI_PreWork_DataTrans"):
            self.YDI_PreWork_DataTrans = os.path.join(self.retrain_folder, "00", self.structure_list[5],
                                                      self.sub_path[2], "YDI_PreWork_DataTrans.pkl")
        else:
            print("YDI_PreWork_DataTrans")

        if not hasattr(self, "xgb_tuning"):
            self.xgb_tuning = os.path.join(self.retrain_folder, "00", self.structure_list[6], "XGB",
                                           self.model_steps[0], "Parameter_aftertuning.csv")
        else:
            print("xgb_tuning")

        if not hasattr(self, "xgb_model"):
            self.xgb_model = os.path.join(self.retrain_folder, "00", self.structure_list[6], "XGB",
                                          self.model_steps[1], "XGB.model")
        else:
            print("xgb_model")

        if not hasattr(self, "pls_train_usp"):
            self.pls_train_usp = os.path.join(self.retrain_folder, "00", self.structure_list[6], "PLS",
                                              self.model_steps[1], "Train.usp")
        else:
            print("pls_train_usp")

        if not hasattr(self, "Previous_Model_Folder"):
            self.Previous_Model_Folder = os.path.join(self.retrain_folder, "00", self.structure_list[6])
        else:
            print("Previous_Model_Folder")

        if not hasattr(self, "CI_config_path"):
            self.CI_config_path = os.path.join(self.retrain_folder, "00", self.structure_list[8],
                                               self.sub_path[2], "CI_config.json")
        else:
            print("CI_config_path")

        if not hasattr(self, "MXCI_x_train_path"):
            self.MXCI_x_train_path = os.path.join(self.retrain_folder, "00", self.structure_list[8],
                                                  self.sub_path[2], "MXCI_x_train.pkl")
        else:
            print("MXCI_x_train_path")
        return None
##############################################################################################################
    def create_path_files_init(self):
        self.file_path = {}
        self.file_path["config_path"] = os.path.abspath(self.config_path)
        self.file_path["log_path"] = os.path.abspath(self.sys_log_path)
        return None
         
    def create_path_files_save(self, path):
        self.file_path["error_path"] = os.path.abspath(os.path.join(path, "error.log"))
        in_path = os.path.join(path, "file_path.json")
        with open(in_path, 'w') as fp:
            json.dump(self.file_path, fp, indent=4, sort_keys=True)
        return None


    def create_path_files(self, merge_flag=None):
        if merge_flag is None:
            path_0 = self.sub_path[0]
            model_path_1 = self.model_steps[0]
            model_path_2 = self.model_steps[1]
            dir_ = self.select_model_dir[0]
        else:
            path_0 = self.sub_path[2]
            model_path_1 = self.model_steps[3]
            model_path_2 = self.model_steps[4]
            dir_ = self.select_model_dir[1]

        # 02_DataReview/
        self.create_path_files_init()
        self.file_path["raw_path"] = os.path.abspath(self.raw_path)
        self.create_path_files_save(self.main_path[2]["main"])

        # 03_PreprocessedData/
        # Model_building
        self.create_path_files_init()
        self.file_path["raw_path"] = os.path.abspath(self.raw_path)
        self.create_path_files_save(self.main_path[3][path_0])

        # 04_XDI/
        # Model_building
        self.create_path_files_init() 
        self.file_path["x_train_path"] = os.path.abspath(self.x_train_path)
        self.create_path_files_save(self.main_path[4][path_0])

        # 05_YDI/
        # Model_building
        self.create_path_files_init()
        self.file_path["x_train_path"] = os.path.abspath(self.x_train_path)
        self.file_path["y_train_path"] = os.path.abspath(self.y_train_path)
        self.create_path_files_save(self.main_path[5][path_0])

        # 06_Model/
        for name in self.model_pred_name:
            if name == "XGB":
                # 00_Parameter_tuning
                self.create_path_files_init()
                self.file_path["x_train_path"] = os.path.abspath(self.x_train_path)
                self.file_path["x_test_path"] = os.path.abspath(self.x_test_path)
                self.file_path["y_train_path"] = os.path.abspath(self.y_train_path)
                self.create_path_files_save(self.main_path[6][name][model_path_1])
                # 01_Model_building
                self.create_path_files_init()
                self.file_path["xgb_tuning"] = os.path.abspath(self.xgb_tuning)
                self.file_path["x_train_path"] = os.path.abspath(self.x_train_path)
                self.file_path["y_train_path"] = os.path.abspath(self.y_train_path)
                self.file_path["xgb_parameter_path"] = os.path.abspath(self.xgb_parameter_path)
                self.create_path_files_save(self.main_path[6][name][model_path_2])

            elif name == "PLS":
                # 01_Model_building
                empty_x_test = os.path.join(self.main_path[6][name][model_path_2], "x_Test_empty.csv")
                empty_y_test = os.path.join(self.main_path[6][name][model_path_2], "y_Test_empty.csv")
                self.create_path_files_init()
                self.file_path["x_train_path"] = os.path.abspath(self.x_train_path)
                self.file_path["y_train_path"] = os.path.abspath(self.y_train_path)
                self.file_path["x_test_path"] = os.path.abspath(empty_x_test)
                self.file_path["y_test_path"] = os.path.abspath(empty_y_test)
                # self.file_path["x_test_path"] = os.path.abspath(self.x_test_path)
                # self.file_path["y_test_path"] = os.path.abspath(self.y_test_path)
                self.create_path_files_save(self.main_path[6][name][model_path_2])


        # 07_SelectModel/
        self.create_path_files_init()
        self.file_path["Model_Folder"] = os.path.abspath(self.main_path[6]["main"])
        self.file_path["y_train_path"] = os.path.abspath(self.y_train_path)
        if merge_flag is None:
            # print(self.main_path[7][self.select_model_dir[0]])
            self.file_path["y_test_path"] = os.path.abspath(self.y_test_path)
            # print(self.file_path)
        self.create_path_files_save(self.main_path[7][dir_])
        
        # 08_CI/
        # Model_building
        self.create_path_files_init()
        self.file_path["x_train_path"] = os.path.abspath(self.x_train_path)
        self.file_path["y_pred_merge_train"] = os.path.abspath(self.y_pred_merge_train)
        self.create_path_files_save(self.main_path[8][path_0])
        return None

    def data_preprocess_test_path(self, batch_flag=None):
        if batch_flag is None:
            self.data_test = os.path.join(self.main_path[3][self.sub_path[0]], "Data_test.csv")
        else:
            self.data_test = self.raw_path
        return None

    def create_batch_path_files(self, batch_flag=None):
        # 02_DataReview/
        self.create_path_files_init()
        self.file_path["raw_path"] = os.path.abspath(self.raw_path)
        if hasattr(self, "old_path"):
            self.file_path["old_path"] = os.path.abspath(self.old_path)
        self.create_path_files_save(self.main_path[2]["main"])
        # 03_PreprocessedData/
        self.create_path_files_init()
        self.file_path["data_test"] = os.path.abspath(self.data_test)
        self.file_path['Cat_config_path'] = os.path.abspath(self.Cat_config_path)
        self.file_path["DataImpute_config_path"] = os.path.abspath(self.DataImpute_config_path)
        self.file_path["DataTrans_config_path"] = os.path.abspath(self.DataTrans_config_path)
        self.file_path["DelMissingCol_config_path"] = os.path.abspath(self.DelMissingCol_config_path)
        self.file_path["DelNonNumCol_config_path"] = os.path.abspath(self.DelNonNumCol_config_path)
        self.create_path_files_save(self.save_path[3])
        # 04_XDI/
        self.create_path_files_init()
        self.file_path["raw_path"] = os.path.abspath(self.raw_path)
        self.file_path["x_test_path"] = os.path.abspath(self.x_test_path)
        self.file_path["x_train_path"] = os.path.abspath(self.x_train_path)
        self.file_path["y_train_path"] = os.path.abspath(self.y_train_path)
        self.file_path["y_test_path"] = os.path.abspath(self.y_test_path)
        self.file_path["XDI_PCA_path"] = os.path.abspath(self.XDI_PCA_path)
        self.file_path["XDI_DataTrans_path"] = os.path.abspath(self.XDI_DataTrans_path)
        self.file_path["XDI_PreWork_DataTrans_path"] = os.path.abspath(self.XDI_PreWork_DataTrans_path)
        self.file_path["XDI_data_remove_path"] = os.path.abspath(os.path.join(self.up_root, "XDI_data_remove.csv"))
        self.file_path["XDI_offline_path"] = os.path.abspath(self.XDI_offline_path)
        self.create_path_files_save(self.save_path[4])
        # 05_YDI/
        self.create_path_files_init()
        self.file_path["YDI_Group_path"] = os.path.abspath(self.YDI_Group)
        self.file_path["raw_path"] = os.path.abspath(self.raw_path)
        self.file_path["y_train_path"] = os.path.abspath(self.y_train_path)
        self.file_path["x_train_path"] = os.path.abspath(self.x_train_path)
        self.file_path["y_test_path"] = os.path.abspath(self.y_test_path)
        self.file_path["x_test_path"] = os.path.abspath(self.x_test_path)
        self.file_path["YDI_threshold_table_path"] = os.path.abspath(self.YDI_threshold_table_path)
        self.file_path["YDI_PreWork_DataTrans"] = os.path.abspath(self.YDI_PreWork_DataTrans)
        self.file_path["YDI_data_remove_path"] = os.path.abspath(os.path.join(self.up_root, "YDI_data_remove.csv"))
        self.create_path_files_save(self.save_path[5])
        # 06_Model/
        for name in self.model_pred_name:
            if name == "XGB":
                self.create_path_files_init()
                self.file_path["xgb_model"] = os.path.abspath(self.xgb_model)
                self.file_path["x_train_path"] = os.path.abspath(self.x_train_path)
                self.file_path["y_train_path"] = os.path.abspath(self.y_train_path)
                self.file_path["x_test_path"] = os.path.abspath(self.x_test_path)
                self.file_path["y_test_path"] = os.path.abspath(self.y_test_path)
                self.create_path_files_save(self.save_path[6][name])

            elif name == "PLS":
                # Prediction
                self.create_path_files_init()
                basename = os.path.basename(os.path.abspath(self.save_path[6][name]))
                self.file_path["x_train_path"] = os.path.abspath(self.x_train_path)
                self.file_path["y_train_path"] = os.path.abspath(self.y_train_path)
                self.file_path["x_test_path"] = os.path.abspath(self.x_test_path)
                self.file_path["y_test_path"] = os.path.abspath(self.y_test_path)
                if basename == name:
                    empty_x_train = os.path.join(self.save_path[6][name], "x_Train_empty.csv")
                    empty_y_train = os.path.join(self.save_path[6][name], "y_Train_empty.csv")
                    self.file_path["x_train_path"] = os.path.abspath(empty_x_train)
                    self.file_path["y_train_path"] = os.path.abspath(empty_y_train)

                self.file_path["pls_train_usp"] = os.path.abspath(self.pls_train_usp)
                self.create_path_files_save(self.save_path[6][name])

        # # 07_SelectModel/
        if batch_flag is not None:
            self.create_path_files_init()
            self.file_path["Previous_Model_Folder"] = os.path.abspath(self.Previous_Model_Folder)
            self.file_path["Model_Folder"] = os.path.abspath(self.main_path[6]["main"])
            self.file_path["y_train_path"] = os.path.abspath(self.y_train_path)
            self.file_path["y_test_path"] = os.path.abspath(self.y_test_path)
            self.file_path["y_pred_merge_train"] = os.path.abspath(self.y_pred_merge_train_special)
            self.create_path_files_save(self.save_path[7])

        # 08_CI/
        self.create_path_files_init()
        self.file_path["MXCI_x_train_path"] = os.path.abspath(self.MXCI_x_train_path)
        self.file_path["x_train_path"] = os.path.abspath(self.x_train_path)
        self.file_path["x_test_path"] = os.path.abspath(self.x_test_path)
        self.file_path["y_pred_merge_train"] = os.path.abspath(self.y_pred_merge_train)
        self.file_path["y_pred_merge_test"] = os.path.abspath(self.y_pred_merge_test)
        self.file_path["CI_config_path"] = os.path.abspath(self.CI_config_path)
        self.file_path["MXCI_MYCI_offline_path"] = os.path.abspath(self.MXCI_MYCI_offline_path)
        self.create_path_files_save(self.save_path[8])

##############################################################################################################

    def batch_process(self, batch_list, retrain_folder_path):
        # check the batch quality
        leftlist = self.batch_ext_check(batch_list)

        num = self.get_current_batch_number(retrain_folder_path)
        if num != None:
            batch_num = num + 1
        else:
            batch_num = 0
            print("WARNING: NO MODEL DETECTED. PLEASE CREATE 00 FILE FIRST.")

        for batch in leftlist:
            self.batch_check(retrain_folder_path)
            self.get_folder_path(batch_flag=True)
            self.get_batch_file_path(batch_flag=True)
            self.init_folders(batch_flag=True)
            self.get_batch_file(batch, batch_num)
            self.data_preprocess_test_path(batch_flag=True)
            self.create_batch_path_files(batch_flag=True)
            batch_num += 1
        return None


    def init_train(self, training_data, batch_list=[]):
        self.training_data = training_data

        # create retrain_folder
        retrain_folder_path = self.retrain_check()
        # create data_folder
        self.batch_check(retrain_folder_path)
        # get path under data_folder
        self.get_folder_path()
        self.get_file_path()
        self.get_batch_file_path()
        # check data
        self.training_ext_check()
        # create folders under data_folder
        self.init_folders()
        # copy raw and config file
        self.get_train_file()
        # create file_path.json
        self.create_path_files()
        self.data_preprocess_test_path()
        self.create_batch_path_files()
        if batch_list:
            self.batch_process(batch_list, retrain_folder_path)
        return None


    def init_merge(self):
        self.get_file_path(merge_flag=True)
        self.get_batch_file_path(merge_flag=True)
        self.create_path_files(merge_flag=True) # create file_path.json
        self.data_preprocess_test_path()
        self.create_batch_path_files()
        return

    def init_batch(self, batch_list, retrain_folder_path=None):
        if retrain_folder_path is None:
            retrain_num = self.get_current_retrain_number()
            if retrain_num is None:
                print("WARNING: NO MODEL DETECTED.")
                raise SystemError
            else:
                retrain_folder_path = os.path.join(self.root,
                                                   os.path.basename(self.root)+"_"+
                                                   self.retrain_format.format(retrain_num))
        else:
            if not os.path.isdir(retrain_folder_path):
                print("No dir is found in " + retrain_folder_path)
                raise FileNotFoundError

        self.batch_process(batch_list, retrain_folder_path)
        return None

if __name__ == "__main__":
    sorce_dir = "New_path_test"
    base_path = os.path.join("../Cases/", sorce_dir)  # base_path = ../Cases/CVD2E_Split1_Test
    train_data = os.path.join(base_path, "T75R4_combine_PM1_first10sheet.csv")
    config_file = os.path.join(base_path, "config-1.json")
    batch_data = [os.path.join(base_path, "T75R4_combine_PM1.csv")]

    path = All_path(base_path, config_file)
    path.retrain_check()
    # print(path.path_config["New_path_test"]["main"])
    # print(path.path_config["New_path_test"][1]["main"])
    path.batch_check()
    # print(path.path_config["New_path_test"][1][0]["main"])
    path.get_folder_path()

    path_config = path.get_path_config()
    # print()
    # print(path_config["New_path_test"][1][0][6]["XGB"]["02_Prediction"])

    path2 = All_path(base_path, config_file, path_config)
    path2.retrain_check()
    path2.batch_check()
    path2.get_folder_path()
    path_config2 = path2.get_path_config()

    path2.save_path_config(path_config2["New_path_test"]["main"])

#----------------------------------------------------------------------------------
#PLS.model.py
# -*- coding: utf-8 -*-
import subprocess
from Read_path import read_path
from config import read_config, save_config
from CreateLog import WriteLog
import time
import os
import pandas as pd

def pls_build(data_folder):   
    data_folder = os.path.abspath(data_folder)  #20190613 relative path -> Absolute path
    #infile=open(r"D:/AVM/Modules/AVM_PLS_Build_Test.bat", "w")#Opens the file
    infile=open(r"D:/SVM/Modules/AVM_PLS_Build_Test.bat", "w")#Opens the file
    #batcode = r"start " + r"D:/AVM/Modules/AVM_PLS_Build/AVM_PLS_Build.exe " + data_folder + r"/file_path.json"
    batcode = r"start " + r"D:/SVM/Modules/AVM_PLS_Build/AVM_PLS_Build.exe " + data_folder + r"/file_path.json"
    infile.write(batcode)#Writes the desired contents to the file
    infile.close()#Closes the file    
    #subprocess.call("AVM_PLS_Build_Test.bat", cwd=r"D:/AVM/Modules")
    
    input_path = read_path(data_folder)
    input_file_path = os.path.join(data_folder, "file_path.json")
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    #mylog.init_logger() #設定info存在System.log,error存在error.log       
    config = read_config(input_path["config_path"], mylog)
    
    x_Trainpath=input_path["x_train_path"] #訓練資料集路徑
    #x_Testpath=input_path["x_test_path"] #訓練資料集路徑
    y_Trainpath=input_path["y_train_path"] #訓練資料集路徑
    #y_Testpath=input_path["y_test_path"]
    delcol = config["IDX"] 
    
    #20190611 avoid Unnamed column
    x_train=pd.read_csv(x_Trainpath)
    if x_train.shape[0] == 0:
        x_train_del=pd.read_csv(x_Trainpath,index_col=-1).drop(delcol, axis=1)       
    else:
        x_train_del=x_train.drop(delcol, axis=1)
    
    y_train=pd.read_csv(y_Trainpath)
    if y_train.shape[0] == 0:
        y_train_del=pd.read_csv(y_Trainpath,index_col=-1).drop(delcol, axis=1)       
    else:
        y_train_del=y_train.drop(delcol, axis=1)

    input_path["x_train_path_del"] = os.path.join(data_folder, "x_Train_del.csv")
    input_path["y_train_path_del"] = os.path.join(data_folder, "y_Train_del.csv")
    input_path["x_test_path_del"] = os.path.join(data_folder, "x_test_del.csv")
    input_path["y_test_path_del"] = os.path.join(data_folder, "y_test_del.csv")
    save_config(input_file_path, input_path, mylog)
    
    x_train_del.to_csv(input_path["x_train_path_del"], index=False)
    y_train_del.to_csv(input_path["y_train_path_del"], index=False)
    #x_test_del.to_csv(input_path["x_test_path_del"], index=False)
    #y_test_del.to_csv(input_path["y_test_path_del"], index=False)
    
    subprocess.call("AVM_PLS_Build_Test.bat", cwd=r"D:/SVM/Modules")

    model_pls_build_path = os.path.join(data_folder, "Train.usp")
    def timer(n):
        while True:
            if os.path.isfile(model_pls_build_path):
                break
            else:
                time.sleep(n)
    timer(10)

def pls_predict(data_folder):   
    data_folder = os.path.abspath(data_folder)
    #infile=open(r"D:/AVM/Modules/AVM_PLS_Predict_Test.bat", "w")#Opens the file
    #batcode = r"start " + r"D:/AVM/Modules/AVM_PLS_Predict/AVM_PLS_Predict.exe " + data_folder + r"/file_path.json"
    infile=open(r"D:/SVM/Modules/AVM_PLS_Predict_Test.bat", "w")#Opens the file
    batcode = r"start " + r"D:/SVM/Modules/AVM_PLS_Predict/AVM_PLS_Predict.exe " + data_folder + r"/file_path.json"
    infile.write(batcode)#Writes the desired contents to the file
    infile.close()#Closes the file    
    #subprocess.call("AVM_PLS_Predict_Test.bat", cwd=r"D:/AVM/Modules")
    
    input_path = read_path(data_folder)
    input_file_path = os.path.join(data_folder, "file_path.json")
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    #mylog.init_logger() #設定info存在System.log,error存在error.log       
    config = read_config(input_path["config_path"], mylog)
    
    x_Trainpath=input_path["x_train_path"] #訓練資料集路徑
    x_Testpath=input_path["x_test_path"] #訓練資料集路徑
    y_Trainpath=input_path["y_train_path"] #訓練資料集路徑
    y_Testpath=input_path["y_test_path"]
    delcol = config["IDX"] 
    
    #20190611 avoid Unnamed column
    x_train=pd.read_csv(x_Trainpath)
    if x_train.shape[0] == 0:
        x_train_del=pd.read_csv(x_Trainpath,index_col=-1).drop(delcol, axis=1)       
    else:
        x_train_del=x_train.drop(delcol, axis=1)
    
    y_train=pd.read_csv(y_Trainpath)
    if y_train.shape[0] == 0:
        y_train_del=pd.read_csv(y_Trainpath,index_col=-1).drop(delcol, axis=1)       
    else:
        y_train_del=y_train.drop(delcol, axis=1)
    
    x_test=pd.read_csv(x_Testpath)
    if x_test.shape[0] == 0:
        x_test_del=pd.read_csv(x_Testpath,index_col=-1).drop(delcol, axis=1)       
    else:
        x_test_del=x_test.drop(delcol, axis=1)
        
    y_test=pd.read_csv(y_Testpath)
    if y_test.shape[0] == 0:
        y_test_del=pd.read_csv(y_Testpath,index_col=-1).drop(delcol, axis=1)       
    else:
        y_test_del=y_test.drop(delcol, axis=1)

    input_path["x_train_path_del"] = os.path.join(data_folder, "x_Train_del.csv")
    input_path["y_train_path_del"] = os.path.join(data_folder, "y_Train_del.csv")
    input_path["x_test_path_del"] = os.path.join(data_folder, "x_test_del.csv")
    input_path["y_test_path_del"] = os.path.join(data_folder, "y_test_del.csv")
    save_config(input_file_path, input_path, mylog)
    
    x_train_del.to_csv(input_path["x_train_path_del"], index=False)
    y_train_del.to_csv(input_path["y_train_path_del"], index=False)
    x_test_del.to_csv(input_path["x_test_path_del"], index=False)
    y_test_del.to_csv(input_path["y_test_path_del"], index=False)
    
    subprocess.call("AVM_PLS_Predict_Test.bat", cwd=r"D:/SVM/Modules")
    
    model_pls_predict_path = os.path.join(data_folder, "testPredResult.csv")
    def timer(n):
        while True:
            if os.path.isfile(model_pls_predict_path):
                break
            else:
                time.sleep(n)
    timer(10)
    
    
def pls_merge_predict(data_folder):   
    data_folder = os.path.abspath(data_folder)
    #infile=open(r"D:/AVM/Modules/AVM_PLS_Predict_Test.bat", "w")#Opens the file
    #batcode = r"start " + r"D:/AVM/Modules/AVM_PLS_Predict/AVM_PLS_Predict.exe " + data_folder + r"/file_path.json"
    infile=open(r"D:/SVM/Modules/AVM_PLS_Predict_Test.bat", "w")#Opens the file
    batcode = r"start " + r"D:/SVM/Modules/AVM_PLS_Predict/AVM_PLS_Predict.exe " + data_folder + r"/file_path.json"
    infile.write(batcode)#Writes the desired contents to the file
    infile.close()#Closes the file    
    #subprocess.call("AVM_PLS_Predict_Test.bat", cwd=r"D:/AVM/Modules")
    
    input_path = read_path(data_folder)
    input_file_path = os.path.join(data_folder, "file_path.json")
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    #mylog.init_logger() #設定info存在System.log,error存在error.log       
    config = read_config(input_path["config_path"], mylog)
    
    x_Trainpath=input_path["x_train_path"] #訓練資料集路徑
    #x_Testpath=input_path["x_test_path"] #訓練資料集路徑
    y_Trainpath=input_path["y_train_path"] #訓練資料集路徑
    #y_Testpath=input_path["y_test_path"]
    delcol = config["IDX"] 
    
    #20190611 avoid Unnamed column
    x_train=pd.read_csv(x_Trainpath)
    if x_train.shape[0] == 0:
        x_train_del=pd.read_csv(x_Trainpath,index_col=-1).drop(delcol, axis=1)       
    else:
        x_train_del=x_train.drop(delcol, axis=1)
    
    y_train=pd.read_csv(y_Trainpath)
    if y_train.shape[0] == 0:
        y_train_del=pd.read_csv(y_Trainpath,index_col=-1).drop(delcol, axis=1)       
    else:
        y_train_del=y_train.drop(delcol, axis=1)

    input_path["x_train_path_del"] = os.path.join(data_folder, "x_Train_del.csv")
    input_path["y_train_path_del"] = os.path.join(data_folder, "y_Train_del.csv")
    input_path["x_test_path_del"] = os.path.join(data_folder, "x_test_del.csv")
    input_path["y_test_path_del"] = os.path.join(data_folder, "y_test_del.csv")
    save_config(input_file_path, input_path, mylog)
    
    x_train_del.to_csv(input_path["x_train_path_del"], index=False)
    y_train_del.to_csv(input_path["y_train_path_del"], index=False)
    #x_test_del.to_csv(input_path["x_test_path_del"], index=False)
    #y_test_del.to_csv(input_path["y_test_path_del"], index=False)
    
    subprocess.call("AVM_PLS_Predict_Test.bat", cwd=r"D:/SVM/Modules")
    
    model_pls_predict_path = os.path.join(data_folder, "trainPredResult.csv")
    def timer(n):
        while True:
            if os.path.isfile(model_pls_predict_path):
                break
            else:
                time.sleep(n)
    timer(10)
    
def pls_batch_predict(data_folder):   
    data_folder = os.path.abspath(data_folder)
    #infile=open(r"D:/AVM/Modules/AVM_PLS_Predict_Test.bat", "w")#Opens the file
    #batcode = r"start " + r"D:/AVM/Modules/AVM_PLS_Predict/AVM_PLS_Predict.exe " + data_folder + r"/file_path.json"
    infile=open(r"D:/SVM/Modules/AVM_PLS_Predict_Test.bat", "w")#Opens the file
    batcode = r"start " + r"D:/SVM/Modules/AVM_PLS_Predict/AVM_PLS_Predict.exe " + data_folder + r"/file_path.json"
    infile.write(batcode)#Writes the desired contents to the file
    infile.close()#Closes the file    
    #subprocess.call("AVM_PLS_Predict_Test.bat", cwd=r"D:/AVM/Modules")
    
    input_path = read_path(data_folder)
    input_file_path = os.path.join(data_folder, "file_path.json")
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    #mylog.init_logger() #設定info存在System.log,error存在error.log       
    config = read_config(input_path["config_path"], mylog)
    
    #x_Trainpath=input_path["x_train_path"] #訓練資料集路徑
    x_Testpath=input_path["x_test_path"] #訓練資料集路徑
    #y_Trainpath=input_path["y_train_path"] #訓練資料集路徑
    y_Testpath=input_path["y_test_path"]
    delcol = config["IDX"] 
    
    #20190611 avoid Unnamed column
    """
    x_train=pd.read_csv(x_Trainpath)
    if x_train.shape[0] == 0:
        x_train_del=pd.read_csv(x_Trainpath,index_col=-1).drop(delcol, axis=1)       
    else:
        x_train_del=x_train.drop(delcol, axis=1)
    
    y_train=pd.read_csv(y_Trainpath)
    if y_train.shape[0] == 0:
        y_train_del=pd.read_csv(y_Trainpath,index_col=-1).drop(delcol, axis=1)       
    else:
        y_train_del=y_train.drop(delcol, axis=1)
    """
    x_test=pd.read_csv(x_Testpath)
    if x_test.shape[0] == 0:
        x_test_del=pd.read_csv(x_Testpath,index_col=-1).drop(delcol, axis=1)       
    else:
        x_test_del=x_test.drop(delcol, axis=1)
        
    y_test=pd.read_csv(y_Testpath)
    if y_test.shape[0] == 0:
        y_test_del=pd.read_csv(y_Testpath,index_col=-1).drop(delcol, axis=1)       
    else:
        y_test_del=y_test.drop(delcol, axis=1)

    input_path["x_train_path_del"] = os.path.join(data_folder, "x_Train_del.csv")
    input_path["y_train_path_del"] = os.path.join(data_folder, "y_Train_del.csv")
    input_path["x_test_path_del"] = os.path.join(data_folder, "x_test_del.csv")
    input_path["y_test_path_del"] = os.path.join(data_folder, "y_test_del.csv")
    save_config(input_file_path, input_path, mylog)
    
    #x_train_del.to_csv(input_path["x_train_path_del"], index=False)
    #y_train_del.to_csv(input_path["y_train_path_del"], index=False)
    x_test_del.to_csv(input_path["x_test_path_del"], index=False)
    y_test_del.to_csv(input_path["y_test_path_del"], index=False)
    
    subprocess.call("AVM_PLS_Predict_Test.bat", cwd=r"D:/SVM/Modules")
    
    model_pls_predict_path = os.path.join(data_folder, "testPredResult.csv")
    def timer(n):
        while True:
            if os.path.isfile(model_pls_predict_path):
                break
            else:
                time.sleep(n)
    timer(10)

def pls_online_predict_x(data_folder, df_x_test):   
    data_folder = os.path.abspath(data_folder)
    infile=open(r"D:/SVM/Modules/AVM_PLS_Predict_x.bat", "w")#Opens the file
    batcode = r"start " + r"D:/SVM/Modules/AVM_PLS_Predict/AVM_PLS_Predict.exe " + data_folder + r"/file_path.json"
    infile.write(batcode)#Writes the desired contents to the file
    infile.close()#Closes the file    
    
    input_path = read_path(data_folder)
    #input_file_path = os.path.join(data_folder, "file_path.json")
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])  
    config = read_config(input_path["config_path"], mylog)
    delcol = config["IDX"] 
    
    df_x_test_del=df_x_test.drop(delcol, axis=1)
    """ 20190710 avoid save config simultaneously to permission denind
    input_path["x_train_path_del"] = os.path.join(data_folder, "x_Train_del.csv")
    input_path["y_train_path_del"] = os.path.join(data_folder, "y_Train_del.csv")
    input_path["x_test_path_del"] = os.path.join(data_folder, "x_test_del.csv")
    input_path["y_test_path_del"] = os.path.join(data_folder, "y_test_del.csv")
    save_config(input_file_path, input_path, mylog)
    """
    #df_x_test_del.to_csv(input_path["x_test_path_del"], index=False)
    df_x_test_del.to_csv(input_path["x_train_path_del"], index=False)
    
    subprocess.call("AVM_PLS_Predict_x.bat", cwd=r"D:/SVM/Modules")
    
    model_pls_predict_path = os.path.join(data_folder, "trainPredResult.csv")
    print(model_pls_predict_path)
    def timer(n):
        while True:
            if os.path.isfile(model_pls_predict_path):
                time.sleep(5) #20190708 for testPredResult.csv has exist
                break
            else:
                time.sleep(n)
    timer(5)

def pls_online_predict_x_abnormal(data_folder, df_x_test):   
    data_folder = os.path.abspath(data_folder)
    infile=open(r"D:/SVM/Modules/AVM_PLS_Predict_x_abnormal.bat", "w")#Opens the file
    batcode = r"start " + r"D:/SVM/Modules/AVM_PLS_Predict/AVM_PLS_Predict.exe " + data_folder + r"/file_path.json"
    infile.write(batcode)#Writes the desired contents to the file
    infile.close()#Closes the file    
    
    input_path = read_path(data_folder)
    #input_file_path = os.path.join(data_folder, "file_path.json")
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])  
    config = read_config(input_path["config_path"], mylog)
    delcol = config["IDX"] 
    
    df_x_test_del=df_x_test.drop(delcol, axis=1)
    #df_x_test_del.to_csv(input_path["x_test_path_del"], index=False)
    df_x_test_del.to_csv(input_path["x_train_path_del"], index=False)
    
    subprocess.call("AVM_PLS_Predict_x_abnormal.bat", cwd=r"D:/SVM/Modules")
    
    model_pls_predict_path = os.path.join(data_folder, "trainPredResult.csv")
    print(model_pls_predict_path)
    def timer(n):
        while True:
            if os.path.isfile(model_pls_predict_path):
                time.sleep(5) #20190708 for testPredResult.csv has exist
                break
            else:
                time.sleep(n)
    timer(5)

def pls_online_predict_x_many(data_folder, df_x_test):   
    data_folder = os.path.abspath(data_folder)
    infile=open(r"D:/SVM/Modules/AVM_PLS_Predict_x_many.bat", "w")#Opens the file
    batcode = r"start " + r"D:/SVM/Modules/AVM_PLS_Predict/AVM_PLS_Predict.exe " + data_folder + r"/file_path.json"
    infile.write(batcode)#Writes the desired contents to the file
    infile.close()#Closes the file    
    
    input_path = read_path(data_folder)
    #input_file_path = os.path.join(data_folder, "file_path.json")
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])  
    config = read_config(input_path["config_path"], mylog)
    delcol = config["IDX"] 
    
    df_x_test_del=df_x_test.drop(delcol, axis=1)
    #df_x_test_del.to_csv(input_path["x_test_path_del"], index=False)
    df_x_test_del.to_csv(input_path["x_train_path_del"], index=False)
    
    subprocess.call("AVM_PLS_Predict_x_many.bat", cwd=r"D:/SVM/Modules")
    
    model_pls_predict_path = os.path.join(data_folder, "trainPredResult.csv")
    print(model_pls_predict_path)
    def timer(n):
        while True:
            if os.path.isfile(model_pls_predict_path):
                time.sleep(5) #20190708 for testPredResult.csv has exist
                break
            else:
                time.sleep(n)
    timer(5)

def pls_online_predict_x_many_abnormal(data_folder, df_x_test):   
    data_folder = os.path.abspath(data_folder)
    infile=open(r"D:/SVM/Modules/AVM_PLS_Predict_x_many_abnormal.bat", "w")#Opens the file
    batcode = r"start " + r"D:/SVM/Modules/AVM_PLS_Predict/AVM_PLS_Predict.exe " + data_folder + r"/file_path.json"
    infile.write(batcode)#Writes the desired contents to the file
    infile.close()#Closes the file    
    
    input_path = read_path(data_folder)
    #input_file_path = os.path.join(data_folder, "file_path.json")
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])  
    config = read_config(input_path["config_path"], mylog)
    delcol = config["IDX"] 
    
    df_x_test_del=df_x_test.drop(delcol, axis=1)
    #df_x_test_del.to_csv(input_path["x_test_path_del"], index=False)
    df_x_test_del.to_csv(input_path["x_train_path_del"], index=False)
    
    subprocess.call("AVM_PLS_Predict_x_many_abnormal.bat", cwd=r"D:/SVM/Modules")
    
    model_pls_predict_path = os.path.join(data_folder, "trainPredResult.csv")
    print(model_pls_predict_path)
    def timer(n):
        while True:
            if os.path.isfile(model_pls_predict_path):
                time.sleep(5) #20190708 for testPredResult.csv has exist
                break
            else:
                time.sleep(n)
    timer(5)

def pls_online_predict_y(data_folder, df_x_test):   
    data_folder = os.path.abspath(data_folder)
    infile=open(r"D:/SVM/Modules/AVM_PLS_Predict_y.bat", "w")#Opens the file
    batcode = r"start " + r"D:/SVM/Modules/AVM_PLS_Predict/AVM_PLS_Predict.exe " + data_folder + r"/file_path.json"
    infile.write(batcode)#Writes the desired contents to the file
    infile.close()#Closes the file    
    
    input_path = read_path(data_folder)
    #input_file_path = os.path.join(data_folder, "file_path.json")
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])  
    config = read_config(input_path["config_path"], mylog)
    delcol = config["IDX"] 
    
    df_x_test_del=df_x_test.drop(delcol, axis=1)
    #df_x_test_del.to_csv(input_path["x_test_path_del"], index=False)
    df_x_test_del.to_csv(input_path["x_train_path_del"], index=False)
    
    subprocess.call("AVM_PLS_Predict_y.bat", cwd=r"D:/SVM/Modules")
    
    model_pls_predict_path = os.path.join(data_folder, "trainPredResult.csv")
    print(model_pls_predict_path)
    def timer(n):
        while True:
            if os.path.isfile(model_pls_predict_path):
                time.sleep(5) #20190708 for testPredResult.csv has exist
                break
            else:
                time.sleep(n)
    timer(5)

def pls_online_predict_y_many(data_folder, df_x_test):   
    data_folder = os.path.abspath(data_folder)
    infile=open(r"D:/SVM/Modules/AVM_PLS_Predict_y_many.bat", "w")#Opens the file
    batcode = r"start " + r"D:/SVM/Modules/AVM_PLS_Predict/AVM_PLS_Predict.exe " + data_folder + r"/file_path.json"
    infile.write(batcode)#Writes the desired contents to the file
    infile.close()#Closes the file    
    
    input_path = read_path(data_folder)
    #input_file_path = os.path.join(data_folder, "file_path.json")
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])  
    config = read_config(input_path["config_path"], mylog)
    delcol = config["IDX"] 
    
    df_x_test_del=df_x_test.drop(delcol, axis=1)
    #df_x_test_del.to_csv(input_path["x_test_path_del"], index=False)
    df_x_test_del.to_csv(input_path["x_train_path_del"], index=False)
    
    subprocess.call("AVM_PLS_Predict_y_many.bat", cwd=r"D:/SVM/Modules")
    
    model_pls_predict_path = os.path.join(data_folder, "trainPredResult.csv")
    print(model_pls_predict_path)
    def timer(n):
        while True:
            if os.path.isfile(model_pls_predict_path):
                time.sleep(5) #20190708 for testPredResult.csv has exist
                break
            else:
                time.sleep(n)
    timer(5)
    
#---------------------------------------------------------------------------------
#Read.path.py
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

#------------------------------------------------------------------
#SmartVM_Constructor_dispatching.py
# -*- coding: utf-8 -*-
import time
import os
from DB_operation import select_project_creating, select_project_waitcreate, select_project_model_confirmok_o2m, select_project_model_confirmok_m2m 

if __name__ == '__main__': 
    def timer(n):
        while True:     
            #20190730 control process number, no execute while no model need training 
            SVM_PROJECT_RUN = select_project_creating()
            if len(SVM_PROJECT_RUN) < 17:
                
                SVM_PROJECT_WAITCREATE = select_project_waitcreate()
                SVM_PROJECT_CONFIRMOK_O2M = select_project_model_confirmok_o2m()
                SVM_PROJECT_CONFIRMOK_M2M = select_project_model_confirmok_m2m()
                
                if len(SVM_PROJECT_WAITCREATE) != 0:
                    Execute = r"start SmartVM_Constructor.bat"
                    os.system(Execute)
                    print("open_finish_for_WAITCREATE")
                    time.sleep(n)
                    
                elif len(SVM_PROJECT_CONFIRMOK_O2M) != 0:
                    Execute = r"start SmartVM_Constructor.bat"
                    os.system(Execute)
                    print("open_finish_for_confirmok_o2m")
                    time.sleep(n)
                
                elif len(SVM_PROJECT_CONFIRMOK_M2M) != 0:
                    Execute = r"start SmartVM_Constructor.bat"
                    os.system(Execute)
                    print("open_finish_for_confirmok_m2m")
                    time.sleep(n)
                
                else:
                    print("no model need train")
                    time.sleep(n)
                                                                     
            else:
                print("train model full")
                time.sleep(n*6)
    timer(20)  
    
#-------------------------------------------------------------------
#SmartVM_Constructor_mp_v3.py
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
from DB_operation import DB_Connection, select_project_by_status, select_project_model_by_modelid, select_project_model_by_modelname, select_project_maxmodel_by_projectid, select_project_model_confirmok_o2m, select_project_model_confirmok_m2m, select_project_with_model_by_projectid, update_project_STATUS_by_projectid, update_project_model_modelstatus_by_modelid, update_project_model_modelstatus_modelstep_waitconfirm_by_modelid, update_project_model_mae_mape_by_modelid

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
import zipfile
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
        """
        self.model_dict_pause = {}
        self.model_dict_pause["1"] = {}
        self.model_dict_pause["1"] = Data_Preview
        Data_Preview(path_dict['2']["main"])
        Data_PreProcess_Train(path_dict['3'][train_flag])
        Data_PreProcess_Test(path_dict['3'][test_flag])
        """
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
        #retrain_num = self.get_max_retrain_num(retrain_num)
        #batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)
        retrain_num = 0
        batch_num = 0
        path_dict = self.path_config[self.base_name][str(retrain_num)][str(batch_num)]
        
        cnxn2 = DB_Connection()
        
        SVM_PP_MODEL = select_project_with_model_by_projectid(project_id)
        
        train_flag = '0'
        test_flag = '1'
        Data_Preview_status = Data_Preview(path_dict['2']["main"])
        
        input_path = read_path(path_dict['2']["main"])
        mylog = WriteLog(input_path["log_path"], input_path["error_path"])  
        
        json2csv(input_path["config_path"]) #20190702 update and output config.csv
        dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="DataPreview",
                                         retrain_num=retrain_num, batch_num=batch_num)       
        
        # FeatureExclude(path_dict['2']["main"])
        print(Data_Preview_status)
        if Data_Preview_status == "NG":
                
            cursor1 = cnxn2.cursor()
            cursor1.execute("UPDATE SVM_PROJECT_MODEL SET MODEL_SEQ = ?, MODEL_STATUS = ?, BATCH_SEQ = ?, IS_ONLINE = ?, MODEL_STEP = ?, WAIT_CONFIRM = ? WHERE MODEL_ID = ?", str(retrain_num), '1', str(batch_num), 0 , '1', 1 , int(SVM_PP_MODEL.MODEL_ID[0]))
            cnxn2.commit()
            
            time.sleep(10)
            update_project_STATUS_by_projectid("CREATING_PAUSE", project_id) #20190729 Because NG, change project status, return function and not execute down   
            
            mylog.info("Data_Preview_status NG")
            
            return 1        
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
            
            update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)
            
            mylog.info("YDI_off_line_report_status NG")
            
            return 1
            """    
            def timer(n):
                while True:
                    return_confirm = select_project_model_by_modelid(SVM_PP_MODEL.MODEL_ID[0])
                    if str(return_confirm.WAIT_CONFIRM[0]) == "False":
                        break
                    else:
                        time.sleep(n)
            timer(10)
            """
        else:
            update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('3', '3', 0, SVM_PP_MODEL.MODEL_ID[0])
        
        dataremove_xdi_status, dataremove_xdi_msg = DataRemove(path_dict['4']["1"], "XDI")  #20190703 remove after step3
        dataremove_ydi_status, dataremove_ydi_msg = DataRemove(path_dict['5']["1"], "YDI")  #20190703 remove after step3
                 
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
            
            update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)
            
            mylog.info("msg_Model NG")
            
            return 1
            """    
            def timer(n):
                while True:

                    return_confirm = select_project_model_by_modelid(SVM_PP_MODEL.MODEL_ID[0])
                    if str(return_confirm.WAIT_CONFIRM[0]) == "False":
                        break
                    else:
                        time.sleep(n)
            timer(10)
            """
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
        
        update_project_model_modelstatus_by_modelid('6', SVM_PP_MODEL.MODEL_ID[0])
        
        mxci_myci_offline_status, mxci_myci_offline_msg = MXCI_MYCI_offline(path_dict['8'][test_flag], mode="Train")
        json2csv(input_path["config_path"])
        dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="CI",
                                         retrain_num=retrain_num, batch_num=batch_num)
        
        update_project_model_modelstatus_by_modelid('7', SVM_PP_MODEL.MODEL_ID[0])
        return None
    
    def training_phase_pause(self, project_id, dc_instance, retrain_num, batch_num=None):
        #retrain_num = self.get_max_retrain_num(retrain_num)
        #batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)
        retrain_num = 0
        batch_num = 0
        
        path_dict = self.path_config[self.base_name][str(retrain_num)][str(batch_num)]
        
        #cnxn2 = DB_Connection()
        
        SVM_PP_MODEL = select_project_with_model_by_projectid(project_id)
        
        train_flag = '0'
        test_flag = '1'
        input_path = read_path(path_dict['2']["main"])
        mylog = WriteLog(input_path["log_path"], input_path["error_path"])
        
        #20190731 check step1 3 5, update creating_run and test
        
        #Data_Preview_status = Data_Preview(path_dict['2']["main"])
                
        #json2csv(input_path["config_path"]) #20190702 update and output config.csv
        #dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="DataPreview",
        #                                 retrain_num=retrain_num, batch_num=batch_num)       
        
        # FeatureExclude(path_dict['2']["main"])
        if SVM_PP_MODEL.MODEL_STEP[0] == '1':
            if str(SVM_PP_MODEL.WAIT_CONFIRM[0]) == 'True':
                time.sleep(10) #20190802 avoid execute the same project
                update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)                                
                return 1
            else:
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
                    
                    update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)
                    
                    mylog.info("YDI_off_line_report_status pause NG1")
                    
                    return 1
                else:
                    update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('3', '3', 0, SVM_PP_MODEL.MODEL_ID[0])
                
                dataremove_xdi_status, dataremove_xdi_msg = DataRemove(path_dict['4']["1"], "XDI")  #20190703 remove after step3
                dataremove_ydi_status, dataremove_ydi_msg = DataRemove(path_dict['5']["1"], "YDI")  #20190703 remove after step3
                         
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
                    
                    update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)
                    
                    mylog.info("msg_Model pause NG1")
                    
                    return 1
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
                
                update_project_model_modelstatus_by_modelid('6', SVM_PP_MODEL.MODEL_ID[0])
                
                mxci_myci_offline_status, mxci_myci_offline_msg = MXCI_MYCI_offline(path_dict['8'][test_flag], mode="Train")
                json2csv(input_path["config_path"])
                dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="CI",
                                                 retrain_num=retrain_num, batch_num=batch_num)
                
                update_project_model_modelstatus_by_modelid('7', SVM_PP_MODEL.MODEL_ID[0])
                
                            
        elif SVM_PP_MODEL.MODEL_STEP[0] == '3':
            if str(SVM_PP_MODEL.WAIT_CONFIRM[0]) == 'True':
                time.sleep(10)
                update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)
                return 1
            else:
                dataremove_xdi_status, dataremove_xdi_msg = DataRemove(path_dict['4']["1"], "XDI")  #20190703 remove after step3
                dataremove_ydi_status, dataremove_ydi_msg = DataRemove(path_dict['5']["1"], "YDI")  #20190703 remove after step3
                         
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
                    
                    update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)
                    
                    mylog.info("msg_Model pause NG2")
                    
                    return 1
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
                
                update_project_model_modelstatus_by_modelid('6', SVM_PP_MODEL.MODEL_ID[0])
                
                mxci_myci_offline_status, mxci_myci_offline_msg = MXCI_MYCI_offline(path_dict['8'][test_flag], mode="Train")
                json2csv(input_path["config_path"])
                dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="CI",
                                                 retrain_num=retrain_num, batch_num=batch_num)
                
                update_project_model_modelstatus_by_modelid('7', SVM_PP_MODEL.MODEL_ID[0])
            
        
        elif SVM_PP_MODEL.MODEL_STEP[0] == '5':
            if str(SVM_PP_MODEL.WAIT_CONFIRM[0]) == 'True':
                time.sleep(10)
                update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)
                return 1
            else:
                """
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
                """
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
                
                update_project_model_modelstatus_by_modelid('6', SVM_PP_MODEL.MODEL_ID[0])
                
                mxci_myci_offline_status, mxci_myci_offline_msg = MXCI_MYCI_offline(path_dict['8'][test_flag], mode="Train")
                json2csv(input_path["config_path"])

                dc_instance.one_to_all_collector(path_config=self.path_config, base_name=self.base_name, situation="CI",
                                                 retrain_num=retrain_num, batch_num=batch_num)
                
                update_project_model_modelstatus_by_modelid('7', SVM_PP_MODEL.MODEL_ID[0])
                            
        return None
        
    def merge_phase(self, base_name, retrain_num, batch_num=None):
        #retrain_num = self.get_max_retrain_num(retrain_num)
        #batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)
        retrain_num = 0
        batch_num = 0
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
            project_id = SVM_PROJECT_MODEL.PROJECT_ID[0]
            
            path_dict = self.path_config[self.filter_dir_name[feature]][str(retrain_num)][str(batch_num)]            
            input_path = read_path(path_dict[str(3)]['0'])
            mylog = WriteLog(input_path["log_path"], input_path["error_path"]) 
                        
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
                        
                       update_project_model_modelstatus_modelstep_waitconfirm_by_modelid('3', '3', 1, SVM_PROJECT_MODEL.MODEL_ID[0])
                       
                       update_project_STATUS_by_projectid("CREATING_PAUSE", project_id) #20190729 Because NG, change project status, return function and not execute down 
                       
                       mylog.info("Wait user check YDI NG!!!")
                       
                       return 1
                       """     
                       def timer(n):
                           while True:
                               return_confirm = select_project_model_by_modelname(self.filter_dir_name[feature])
                               if str(return_confirm.WAIT_CONFIRM[0]) == "False":
                                   break
                               else:
                                   time.sleep(n)
                       timer(10) 
                       """
                       dataremove_xdi_status, dataremove_xdi_msg = DataRemove(path_dict['4']["1"], "XDI")  #20190716 if YDI NG, wait user check and remove after step3
                       dataremove_ydi_status, dataremove_ydi_msg = DataRemove(path_dict['5']["1"], "YDI")  #20190703 if YDI NG, wait user check and remove after step3
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
            project_id = SVM_PROJECT_MODEL.PROJECT_ID[0]
            
            input_path = read_path(path_dict[str(7)]['0'])
            mylog = WriteLog(input_path["log_path"], input_path["error_path"])
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
                   
                   update_project_STATUS_by_projectid("CREATING_PAUSE", project_id) #20190729 Because NG, change project status, return function and not execute down 
                   
                   mylog.info("Wait user select model!!!")
                   
                   return 1
                   """
                   def timer(n):
                       while True:
                           return_confirm = select_project_model_by_modelname(self.filter_dir_name[feature])
                           if str(return_confirm.WAIT_CONFIRM[0]) == "False":
                               break
                           else:
                               time.sleep(n)
                   timer(10)
                   """
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
                                         
        #retrain_num = self.get_max_retrain_num(retrain_num=self.current_retrain_number)
        #batch_num = self.get_max_batch_num(retrain_num=self.current_retrain_number, batch_num=None)
        retrain_num = 0
        batch_num = 0
        
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
        step3_result = self.module_for_loop(model_num=5, retrain_num=retrain_num, batch_num=batch_num, dc=dc)
        if step3_result == 1:
            return 1
            
        self.many_model_phase(retrain_num=retrain_num, batch_num=batch_num)           
        dc.Model_collector(path_config=self.path_config, filter_dir_name=self.filter_dir_name,
                           retrain_num=retrain_num, batch_num=batch_num, feature_name=self.filter_feature,
                           feature_lists=self.feature_lists, merge_flag=None)
        
        for x in self.filter_dir_name.keys():

            SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[x])            
            update_project_model_modelstatus_by_modelid('4', SVM_PROJECT_MODEL.MODEL_ID[0])
            
        step5_result = self.model_selection_for_loop(retrain_num=retrain_num, batch_num=batch_num, dc=dc)
        if step5_result == 1:
            return 1
            
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
        
        cursor1 = cnxn1.cursor()
        cursor1.execute("UPDATE SVM_PROJECT SET STATUS = ? WHERE PROJECT_ID = ?", "CREATED_OK", int(project_id))
        cnxn1.commit() 
        
        self.zip_project(project_id)
        
        return None
    

    def many_to_many_pause(self, project_id):
        dc = Data_Collector(self.base_path, self.train_data, self.config_file)  #create Data_Collector folder, base.path=../Cases/PSH_Demo 
        #dc.create_file_path() # for DataPreview

        #Data_Preview_status = Data_Preview(dc.data_preview_path)
        
        self.get_filter_feature()
        self.get_feature_content(split_flag=True)
        #self.create_many_path(training_data_dict=self.filter_feature_dict)
                                         
        #retrain_num = self.get_max_retrain_num(retrain_num=self.current_retrain_number)
        #batch_num = self.get_max_batch_num(retrain_num=self.current_retrain_number, batch_num=None)
        retrain_num = 0
        batch_num = 0
        
        #20190618 judge Data_Preview_status in future
        cnxn1 = DB_Connection()
        """
        for x in self.filter_dir_name.keys():

            SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[x])
            
            cursor1 = cnxn1.cursor()
            cursor1.execute("UPDATE SVM_PROJECT_MODEL SET MODEL_SEQ = ?, MODEL_STATUS = ?, BATCH_SEQ = ?, IS_ONLINE = ?, MODEL_STEP = ?, WAIT_CONFIRM = ? WHERE MODEL_ID = ?", str(retrain_num), '1', str(batch_num), 0, '1', 0, int(SVM_PROJECT_MODEL.MODEL_ID[0]))
            cnxn1.commit() 
        """        
        dc.init_collector_dir(retrain_num, batch_num)
        print("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num))
        """
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
        """
        SVM_MAXMODEL = select_project_maxmodel_by_projectid(project_id)
        if SVM_MAXMODEL.MODEL_STEP[0] == '3':
            if str(SVM_MAXMODEL.WAIT_CONFIRM[0]) == 'True':
                time.sleep(10)
                update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)
                return 1
            else:
                self.many_model_phase(retrain_num=retrain_num, batch_num=batch_num)           
                dc.Model_collector(path_config=self.path_config, filter_dir_name=self.filter_dir_name,
                                   retrain_num=retrain_num, batch_num=batch_num, feature_name=self.filter_feature,
                                   feature_lists=self.feature_lists, merge_flag=None)
                
                for x in self.filter_dir_name.keys():
        
                    SVM_PROJECT_MODEL = select_project_model_by_modelname(self.filter_dir_name[x])            
                    update_project_model_modelstatus_by_modelid('4', SVM_PROJECT_MODEL.MODEL_ID[0])
                    
                step5_result = self.model_selection_for_loop(retrain_num=retrain_num, batch_num=batch_num, dc=dc)
                if step5_result == 1:
                    return 1
                    
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
                
                cursor1 = cnxn1.cursor()
                cursor1.execute("UPDATE SVM_PROJECT SET STATUS = ? WHERE PROJECT_ID = ?", "CREATED_OK", int(project_id))
                cnxn1.commit()
                
                self.zip_project(project_id)
        
        elif SVM_MAXMODEL.MODEL_STEP[0] == '5':
            if str(SVM_MAXMODEL.WAIT_CONFIRM[0]) == 'True':
                time.sleep(10)
                update_project_STATUS_by_projectid("CREATING_PAUSE", project_id)
                return 1
            else:
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
                
                cursor1 = cnxn1.cursor()
                cursor1.execute("UPDATE SVM_PROJECT SET STATUS = ? WHERE PROJECT_ID = ?", "CREATED_OK", int(project_id))
                cnxn1.commit()
                
                self.zip_project(project_id)
             
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
        
        #retrain_num = self.get_max_retrain_num(retrain_num=self.current_retrain_number)
        #batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=None)
        retrain_num = 0
        batch_num = 0
        
        print("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num))
        dc.init_collector_dir(retrain_num, batch_num)
        training_result = self.training_phase(project_id, retrain_num=self.current_retrain_number, dc_instance=dc)
        if training_result == 1:
            return 1
        
        self.merge_phase(base_name=self.base_name, retrain_num=retrain_num)
        dc.data_porter(path_config=self.path_config, base_name=self.base_name,
                       retrain_num=retrain_num, batch_num=batch_num, mode="Merge")
        
        cnxn1 = DB_Connection()
        cursor1 = cnxn1.cursor()
        cursor1.execute("UPDATE SVM_PROJECT SET STATUS = ? WHERE PROJECT_ID = ?", "CREATED_OK", int(project_id))
        cnxn1.commit() 
        
        self.zip_project(project_id) #20190805 add zip file to D:\SVM\model_out for AI_365
        
        return None
    
    def one_to_all_pause(self, project_id):
        #path = os.path.join(self.base_path, self.base_name)
        dc = Data_Collector(self.base_path, self.train_data, self.config_file)
        #self.path_config = self.create_path(path)
        self.get_saved_path_config()
        #self.save_path_config()
        
        #retrain_num = self.get_max_retrain_num(retrain_num=self.current_retrain_number)
        #batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=None)
        retrain_num = 0
        batch_num = 0
        print("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num))
        dc.init_collector_dir(retrain_num, batch_num) #20190802 must execute for data collect
        training_result = self.training_phase_pause(project_id, retrain_num=self.current_retrain_number, dc_instance=dc)
        if training_result == 1:
            return 1
        
        self.merge_phase(base_name=self.base_name, retrain_num=retrain_num)
        dc.data_porter(path_config=self.path_config, base_name=self.base_name,
                       retrain_num=retrain_num, batch_num=batch_num, mode="Merge")
        
        cnxn1 = DB_Connection()
        cursor1 = cnxn1.cursor()
        cursor1.execute("UPDATE SVM_PROJECT SET STATUS = ? WHERE PROJECT_ID = ?", "CREATED_OK", int(project_id))
        cnxn1.commit() 
        
        self.zip_project(project_id)
        
        return None
        
if __name__ == '__main__':   
    SVM_PP_MODEL = select_project_by_status("CREATED")
    
    if len(SVM_PP_MODEL) != 0:        

        #pool=mp.Pool(16)
        
        #for a in range(len(SVM_PP_MODEL)):
        project_id = SVM_PP_MODEL.PROJECT_ID[0]
        #20190805 SVN error update status, avoid execute recycle
        try:                
            SVM = SuperVM(str(SVM_PP_MODEL.PROJECT_NAME[0]), str(SVM_PP_MODEL.UPLOAD_FILE[0]), r"Config.json")
        except Exception as e:
            print("SVM error 1")
            update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                               
        update_project_STATUS_by_projectid("CREATING", project_id)
        
        print("------------")
        print(project_id)
        print("------------")
        
        if (str(SVM_PP_MODEL.MODEL_TYPE[0]) == '1') or (str(SVM_PP_MODEL.MODEL_TYPE[0]) == '2'):
            
            #pool.apply_async(SVM.one_to_all, args=(project_id,))  
            SVM.one_to_all(project_id)                                
           
        elif str(SVM_PP_MODEL.MODEL_TYPE[0]) == '3':

            #pool.apply_async(SVM.many_to_many, args=(project_id,))
            SVM.many_to_many(project_id)

        #pool.close()
        #pool.join()
    else:
        SVM_PROJECT_CONFIRMOK_O2M = select_project_model_confirmok_o2m()
        
        #SVM_PROJECT_NG = select_project_by_status("CREATING_PAUSE")
        
        if len(SVM_PROJECT_CONFIRMOK_O2M) != 0:
        
            #pool=mp.Pool(16)
            
            #for a in range(len(SVM_PROJECT_NG)): 
            SVM_MODEL = select_project_model_by_modelid(SVM_PROJECT_CONFIRMOK_O2M.MODEL_ID[0])
                          
            project_id = SVM_MODEL.PROJECT_ID[0]
            
            print("------------")
            print(project_id)
            print("------------")
            
            try:
                SVM = SuperVM(str(SVM_PROJECT_CONFIRMOK_O2M.PROJECT_NAME[0]), str(SVM_PROJECT_CONFIRMOK_O2M.UPLOAD_FILE[0]), r"Config.json")                
            except Exception as e:
                print("SVM error 2")
                update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                        
            update_project_STATUS_by_projectid("CREATING_RUN", project_id)
            
            #if (str(SVM_PROJECT_CONFIRMOK_O2M.MODEL_TYPE[0]) == '1') or (str(SVM_PROJECT_CONFIRMOK_O2M.MODEL_TYPE[0]) == '2'):
                
                #pool.apply_async(SVM.one_to_all_pause, args=(project_id,))
            SVM.one_to_all_pause(project_id)                                  
               
            #elif str(SVM_PROJECT_CONFIRMOK_O2M.MODEL_TYPE[0]) == '3':

                #pool.apply_async(SVM.many_to_many_pause, args=(project_id,))
                #SVM.many_to_many_pause(project_id)

            #pool.close()
            #pool.join()  
        else:
            SVM_PROJECT_CONFIRMOK_M2M = select_project_model_confirmok_m2m()
            
            if len(SVM_PROJECT_CONFIRMOK_M2M) != 0:
                
                SVM_MODEL = select_project_model_by_modelid(SVM_PROJECT_CONFIRMOK_M2M.MODEL_ID[0])
                          
                project_id = SVM_MODEL.PROJECT_ID[0]
                
                print("------------")
                print(project_id)
                print("------------")
                
                try:
                    SVM = SuperVM(str(SVM_PROJECT_CONFIRMOK_M2M.PROJECT_NAME[0]), str(SVM_PROJECT_CONFIRMOK_M2M.UPLOAD_FILE[0]), r"Config.json")             
                except Exception as e:
                    print("SVM error 3")
                    update_project_STATUS_by_projectid("CREATING_ERROR", project_id)
                            
                update_project_STATUS_by_projectid("CREATING_RUN", project_id)
                
                SVM.many_to_many_pause(project_id)
            
#----------------------------------------------------------------------------------------------------------------------------------------
#SmartVM_Constructor_online_x.py
from Data_PreProcess import Data_PreProcess_Train, Data_PreProcess_Test, OnLine_Data_PreProcess_no_y
from XDI import XDI_off_line_report, Build_XDI_Model, OnLine_XDI
from YDI import YDI_off_line_report, Build_YDI_Model
from MXCI_MYCI_pre import pre_MXCI_MYCI
from MXCI_MYCI import MXCI_MYCI_offline, CI_online
from XGB_model import xgb_online_predict
from PLS_model import pls_online_predict_x
from Read_path import read_path
from config import read_config
from CreateLog import WriteLog
from DB_operation import select_project_model_by_projectid, select_project_model_by_projectid_status, select_project_online_scantime_x, select_project_online_scantime_by_projectid_datatype, select_online_parameter_by_projectid_datatype, select_online_status_by_projectid_runindex, select_projectx_run_by_itime, select_projectx_parameter_data_by_runindex_parameter, select_projectx_predict_data_by_runindex_parameter_modelid, select_projectx_contribution_data_by_runindex_parameter_model, update_online_scantime_lastscantime_by_projectid_datatype, update_online_status_xdialarm_by_projectid_runindex, update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid, update_projectx_run_signal_by_runindex, update_projectx_run_xdialarm_by_runindex, update_projectx_contribution_data_contribution_by_runindex_parameter_model, insert_online_status_projectid_runindex_xdialarm_itime, insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict, insert_projectx_contribution_data_runindex_model_parameter_contribution

import os
import json
import traceback
# from Data_Check import Data_Check
from shutil import copyfile
import pandas as pd
import datetime
import time


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

########################################################################################################################
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
    
    def model_predict_online_x(self, df_online_X, path_dict):
        xgb_y_pred = xgb_online_predict(path_dict['6']["XGB"]["5"], df_online_X) #20190702 eat 05_Prediction  
        #20190702 mark temporarily
        pls_online_predict_x(path_dict['6']["PLS"]["5"], df_online_X)
        
        input_path = read_path(path_dict['6']["PLS"]["5"])
        mylog = WriteLog(input_path["log_path"], input_path["error_path"])    
        config = read_config(input_path["config_path"], mylog)
        config_y = config["Y"][0]
        Baseline_Model = config["Model_Selection"][config_y]["Baseline_Model"] 
        pls_y_name = config["Y"][0]+"_pred_PLS"
        
        #20190702 mark temporarily
        pls_predict_test_path = os.path.join(path_dict['6']["PLS"]["5"], "trainPredResult.csv")
        pls_test=pd.read_csv(pls_predict_test_path)
        pls_test.rename({'Y_pred': pls_y_name}, axis=1, inplace=True) #20190722 after pls predict, must rename
        #pls_y_pred = pls_test['Y_pred']
        pls_y_pred = pls_test[pls_y_name]
        #pls_y_pred = xgb_y_pred * 1.2
        #20190709 for pls_y_pred null
        if len(pls_y_pred) == 0:
            pls_y_predc = 0
        else:
            pls_y_predc = pls_y_pred[0] #20190621 because return series, must assign location

        if Baseline_Model == "PLS":
            #20190702 mark temporarily
            return xgb_y_pred, pls_y_predc 
            #return xgb_y_pred, pls_y_pred
        else:
            #20190702 mark temporarily
            return pls_y_predc, xgb_y_pred
            #return pls_y_pred, xgb_y_pred
    
    def online_x_phase(self, projectid, datatype, base_name, retrain_num, batch_num=None):
        retrain_num = self.get_max_retrain_num(retrain_num)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)

        path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]
        SVM_PROJECT_ONLINE_SCANTIME = select_project_online_scantime_by_projectid_datatype(projectid, datatype)
        
        #20190620 get server name
        server_name_1 = str(SVM_PROJECT_ONLINE_SCANTIME.SERVER_NAME[0])
        #cnxn2 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server_name_1 + ';DATABASE=' + db_name + ';UID=' + user + ';PWD=' + password)
        
        project_id = "SVM_PROJECT" + str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0])
        
        if len(SVM_PROJECT_ONLINE_SCANTIME) == 0:
            print('online_x_phase no scantime')                                                                      
        else:
            MAX_TIME = "9998-12-31 23:59:59.999"
                       
            LAST_SCANTIME = datetime.datetime.strftime(SVM_PROJECT_ONLINE_SCANTIME.LAST_SCANTIME[0], '%Y-%m-%d %H:%M:%S.%f')[:-3] #20190704 get millisecond 3
            SVM_PROJECT_RUN = select_projectx_run_by_itime(project_id, LAST_SCANTIME, MAX_TIME, server_name_1)
            
            if len(SVM_PROJECT_RUN) == 0:
                print('SVM_PROJECT_RUN null')              
            else:
                update_online_scantime_lastscantime_by_projectid_datatype(SVM_PROJECT_RUN.ITIME[0], SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], 'X', server_name_1)                   
                SVM_ONLINE_PARAMETER = select_online_parameter_by_projectid_datatype(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], 'X', server_name_1)
                
                for k in range(len(SVM_ONLINE_PARAMETER)):
                                          
                    if str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'TOOLID':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.TOOLID[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'LOTNAME':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.LOTNAME[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'FOUPNAME':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.FOUPNAME[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'OPID':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.OPID[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'RECIPE':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.RECIPE[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'ABBRID':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.ABBRID[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'PRODUCT':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.PRODUCT[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'MODELNO':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.MODELNO[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'CHAMBER':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.CHAMBER[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'SHEETID':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.SHEETID[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'SLOTNO':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.SLOTNO[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'POINT':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.POINT[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'FILTER_FEATURE':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.FILTER_FEATURE[0])],axis=1)
                    else:
                        SVM_PARAMETER_DATA_tmp = select_projectx_parameter_data_by_runindex_parameter(project_id, SVM_PROJECT_RUN.RUNINDEX[0], SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k], server_name_1)
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PARAMETER_DATA_tmp.PARAM_VALUE[0])],axis=1)
                    
                    if k == 0:
                        SVM_PARAMETER_DATA = SVM_PARAMETER_DATA_part
                    else:
                        SVM_PARAMETER_DATA = pd.concat([SVM_PARAMETER_DATA, SVM_PARAMETER_DATA_part], axis=0, ignore_index=True)
                    
                    SVM_PARAMETER_DATA_part = SVM_PARAMETER_DATA_part.iloc[0:0]
                    
                                                                                                                                                                                        
                if len(SVM_PARAMETER_DATA) == 0:
                    print('SVM_PARAMETER_DATA null')  #20190619 add inert sql in the future
                else:
                    #df_SVM_PARAMETER_DATA_lite=SVM_PARAMETER_DATA.drop(['RUNINDEX','ITIME'], axis=1)
                    df_SVM_PARAMETER_DATA_lite_T = SVM_PARAMETER_DATA.T #20190619 行列互轉
                    headers = df_SVM_PARAMETER_DATA_lite_T.iloc[0] #20190619 first row set header
                    df_SVM_PARAMETER_DATA_lite_T_H  = pd.DataFrame(df_SVM_PARAMETER_DATA_lite_T.values[1:], columns=headers)
                    
                    print("online x phase[ns]")
                    df_online_X = OnLine_Data_PreProcess_no_y(df_SVM_PARAMETER_DATA_lite_T_H, path_dict['3']["3"]) #20190702 eat 03_test
                    XDI_online_value, XDI_Threshold, XDI_Check, df_contri = OnLine_XDI(df_online_X, path_dict['4']["3"])
                    y_pred, y_base = self.model_predict_online_x(df_online_X, path_dict)
                    MXCI_value, MXCI_T, MYCI_value, MYCI_T, light = CI_online(path_dict['8']["3"], df_online_X, y_pred, y_base)
                                           
                    SVM_PROJECT_MODEL = select_project_model_by_projectid_status(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], "ONLINE", server_name_1)
                    
                    if XDI_online_value is not None:
                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PROJECT_RUN.RUNINDEX[0], 'XDI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PROJECT_RUN.RUNINDEX[0], 'XDI', XDI_online_value, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name_1)                                
                        else:  
                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, XDI_online_value, 0, SVM_PROJECT_RUN.RUNINDEX[0], 'XDI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                                              
                    if XDI_Threshold is not None:
                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PROJECT_RUN.RUNINDEX[0], 'XDI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PROJECT_RUN.RUNINDEX[0], 'XDI_Threshold', XDI_Threshold, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name_1)  
                        else: 
                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, XDI_Threshold, 0, SVM_PROJECT_RUN.RUNINDEX[0], 'XDI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)                                             
                    
                    if XDI_Check is not None:
                        if int(XDI_Check) == 0:
                            SVM_ONLINE_STATUS = select_online_status_by_projectid_runindex(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], SVM_PROJECT_RUN.RUNINDEX[0], server_name_1)
                            if len(SVM_ONLINE_STATUS) == 0:
                                insert_online_status_projectid_runindex_xdialarm_itime(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], SVM_PROJECT_RUN.RUNINDEX[0], 1, datetime.datetime.now(), server_name_1) #20190710 xdi_alarm 0->1                                
                            else: 
                                update_online_status_xdialarm_by_projectid_runindex(1, SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], SVM_PROJECT_RUN.RUNINDEX[0], server_name_1)  
                            update_projectx_run_xdialarm_by_runindex(project_id, 1, SVM_PROJECT_RUN.RUNINDEX[0], server_name_1)                                                   
                    
                    if y_pred is not None:
                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PROJECT_RUN.RUNINDEX[0], 'Predict_Y', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                        if len(SVM_PROJECT_PREDICT_DATA) == 0: 
                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PROJECT_RUN.RUNINDEX[0], 'Predict_Y', y_pred, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name_1)                                  
                        else: 
                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, y_pred, 0, SVM_PROJECT_RUN.RUNINDEX[0], 'Predict_Y', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                    
                    if MXCI_value is not None:
                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PROJECT_RUN.RUNINDEX[0], 'MXCI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                        if len(SVM_PROJECT_PREDICT_DATA) == 0:   
                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PROJECT_RUN.RUNINDEX[0], 'MXCI', MXCI_value, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name_1)  
                        else: 
                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MXCI_value, 0, SVM_PROJECT_RUN.RUNINDEX[0], 'MXCI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                        
                    if MXCI_T is not None:
                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PROJECT_RUN.RUNINDEX[0], 'MXCI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PROJECT_RUN.RUNINDEX[0], 'MXCI_Threshold', MXCI_T, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name_1)                                    
                        else: 
                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MXCI_T, 0, SVM_PROJECT_RUN.RUNINDEX[0], 'MXCI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                        
                    if MYCI_value is not None:
                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PROJECT_RUN.RUNINDEX[0], 'MYCI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                        MYCI_value = float(str(MYCI_value)[0:10])
                        if len(SVM_PROJECT_PREDICT_DATA) == 0:

                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PROJECT_RUN.RUNINDEX[0], 'MYCI', MYCI_value, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name_1)                                   
                        else: 
                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MYCI_value, 0, SVM_PROJECT_RUN.RUNINDEX[0], 'MYCI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                        
                    if MYCI_T is not None:
                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PROJECT_RUN.RUNINDEX[0], 'MYCI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PROJECT_RUN.RUNINDEX[0], 'MYCI_Threshold', MYCI_T, SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name_1)                                    
                        else: 
                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MYCI_T, 0, SVM_PROJECT_RUN.RUNINDEX[0], 'MYCI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                    
                    if light is not None:
                        update_projectx_run_signal_by_runindex(project_id, light, SVM_PROJECT_RUN.RUNINDEX[0], server_name_1)
                    
                    if df_contri is not None:
                        for j in range(df_contri.shape[0]):
                            SVM_PROJECT_CONTRIBUTION_DATA = select_projectx_contribution_data_by_runindex_parameter_model(project_id, SVM_PROJECT_RUN.RUNINDEX[0], df_contri.Col_Name[j], 'XDI', server_name_1)
                            if len(SVM_PROJECT_CONTRIBUTION_DATA) == 0:  
                                insert_projectx_contribution_data_runindex_model_parameter_contribution(project_id, SVM_PROJECT_RUN.RUNINDEX[0], 'XDI', df_contri.Col_Name[j], df_contri.Contribution[j], server_name_1)
                            else: 
                                update_projectx_contribution_data_contribution_by_runindex_parameter_model(project_id, df_contri.Contribution[j], SVM_PROJECT_RUN.RUNINDEX[0], df_contri.Col_Name[j], 'XDI', server_name_1)                                                                                       
        return None
    
    def one_to_all_online_x(self, projectid, datatype, retrain_num):           
        retrain_num = self.get_max_retrain_num(retrain_num=retrain_num)
        self.get_saved_path_config()
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=None)
        print("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num))
        self.online_x_phase(projectid, datatype, base_name=self.base_name, retrain_num=retrain_num, batch_num=batch_num)
        
        return None
 
if __name__ == '__main__':        
    #20190627 online data must go 10.96.18.199, get SERVER_NAME to the server
    def timer(n):
        while True: 
            SVM_PROJECT_ONLINE_SCANTIME = select_project_online_scantime_x()
                                       
            if len(SVM_PROJECT_ONLINE_SCANTIME) != 0:
                
                for a in range(len(SVM_PROJECT_ONLINE_SCANTIME)):
                    
                    projectid = SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[a]
                    SVM_MODEL_ONLINE_X = select_project_model_by_projectid_status(projectid, "ONLINE")
                    
                    if len(SVM_MODEL_ONLINE_X) != 0:
                        
                        SVM = SuperVM(str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_NAME[a]), str(SVM_PROJECT_ONLINE_SCANTIME.UPLOAD_FILE[a]), r"Config.json")                
                        datatype = SVM_PROJECT_ONLINE_SCANTIME.DATATYPE[a].upper()    
                        retrain_num = int(SVM_MODEL_ONLINE_X.MODEL_SEQ[0]) 
                        
                        print("--------------")
                        print(projectid)
                        print(datatype)
                        print(retrain_num)
                        print("--------------")
                        
                        SVM.one_to_all_online_x(projectid, datatype, retrain_num)
                    
                    else:
                        print("SVM_MODEL_ONLINE_X None")
                              
            time.sleep(n)
    timer(5) 

#----------------------------------------------------------------------------------------------------------------------------------------
#SmartVM_Constructor_online_x_many.py
from Data_Preview import Data_Preview
from Data_PreProcess import Data_PreProcess_Train, Data_PreProcess_Test, OnLine_Data_PreProcess, OnLine_Data_PreProcess_no_y
from XDI import XDI_off_line_report, Build_XDI_Model, OnLine_XDI
from YDI import YDI_off_line_report, Build_YDI_Model, OnLine_YDI
from MXCI_MYCI_pre import pre_MXCI_MYCI
from MXCI_MYCI import MXCI_MYCI_offline, CI_online
from XGB_model import xgb_tuning, xgb_build, xgb_predict, xgb_merge_tuning, xgb_merge_predict, xgb_batch_predict, xgb_online_predict
from PLS_model import pls_build, pls_predict, pls_merge_predict, pls_batch_predict, pls_online_predict_x_many
from Read_path import read_path
from config import read_config, save_config
from CreateLog import WriteLog
from DB_operation import DB_Connection, select_project_config_by_modelid_parameter3RD, select_project_model_by_projectid, select_project_model_by_projectid_status, select_project_model_by_projectid_status_filterfeature, select_project_model_by_projectid_predictresult, select_project_online_scantime_x_many, select_project_online_scantime_by_projectid_datatype, select_online_parameter_by_projectid, select_online_parameter_by_projectid_datatype, select_online_status_by_projectid_runindex, select_projectx_run_by_itime, select_projectx_parameter_data_by_runindex_parameter, select_projectx_predict_data_by_runindex_parameter_modelid, select_projectx_contribution_data_by_runindex_parameter_model, update_project_model_predictresult_by_modelid, update_online_scantime_lastscantime_by_projectid_datatype, update_online_status_xdialarm_by_projectid_runindex, update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid, update_projectx_run_xdialarm_by_runindex, update_projectx_run_ydialarm_by_runindex, update_projectx_contribution_data_contribution_by_runindex_parameter_model, insert_online_status_projectid_runindex_xdialarm_itime, insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict, insert_projectx_contribution_data_runindex_model_parameter_contribution

import os
import json
import traceback
from Path import All_path
from Data_Collector import Data_Collector
# from Data_Check import Data_Check
from Exclusion import DataRemove, FeatureExclude

from shutil import copyfile
import numpy as np
import pandas as pd
import pyodbc
import datetime
import time


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
    
    def model_predict_online_x(self, df_online_X, path_dict):
        xgb_y_pred = xgb_online_predict(path_dict['6']["XGB"]["5"], df_online_X) #20190702 eat 05_Prediction  
        #20190702 mark temporarily
        pls_online_predict_x_many(path_dict['6']["PLS"]["5"], df_online_X)
        
        input_path = read_path(path_dict['6']["PLS"]["5"])
        mylog = WriteLog(input_path["log_path"], input_path["error_path"])    
        config = read_config(input_path["config_path"], mylog)
        config_y = config["Y"][0]
        Baseline_Model = config["Model_Selection"][config_y]["Baseline_Model"]  
        pls_y_name = config["Y"][0]+"_pred_PLS"
        
        #20190702 mark temporarily
        pls_predict_test_path = os.path.join(path_dict['6']["PLS"]["5"], "trainPredResult.csv")
        pls_test=pd.read_csv(pls_predict_test_path)
        pls_test.rename({'Y_pred': pls_y_name}, axis=1, inplace=True) #20190722 after pls predict, must rename
        #pls_y_pred = pls_test['Y_pred']
        pls_y_pred = pls_test[pls_y_name]
        #pls_y_pred = xgb_y_pred * 1.2

        if Baseline_Model == "PLS":
            #20190702 mark temporarily
            return xgb_y_pred, pls_y_pred[0] #20190621 because return series, must assign location
            #return xgb_y_pred, pls_y_pred
        else:
            #20190702 mark temporarily
            return pls_y_pred[0], xgb_y_pred
            #return pls_y_pred, xgb_y_pred
    
    def online_x_many_phase(self, projectid, model_id, datatype, base_name, retrain_num, batch_num=None):
        retrain_num = self.get_max_retrain_num(retrain_num)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)

        path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]
        
        #server_name = "10.96.18.199"
        db_name = "APC"
        user = "YL"
        password = "YL$212"                  
        #cnxn1 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server_name + ';DATABASE=' + db_name + ';UID=' + user + ';PWD=' + password)
                
        #sql = "select * from SVM_PROJECT where PROJECT_NAME = \'" + self.base_name + "\'"
        #SVM_PROJECT = pd.read_sql(sql, cnxn1)
        
        SVM_PROJECT_ONLINE_SCANTIME = select_project_online_scantime_by_projectid_datatype(projectid, datatype)
        #20190620 get server name
        server_name_1 = str(SVM_PROJECT_ONLINE_SCANTIME.SERVER_NAME[0])
        cnxn2 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server_name_1 + ';DATABASE=' + db_name + ';UID=' + user + ';PWD=' + password)
                
        project_id = "SVM_PROJECT" + str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0])
        #project_id = "SVM_PROJECT" + str(1) #20190618 Temporary use
        
        #sql = "select * from SVM_ONLINE_SCANTIME where PROJECT_ID = \'" + str(SVM_PROJECT.PROJECT_ID[0]) + "\' and DATATYPE = \'X\'"  
        #SVM_ONLINE_SCANTIME = pd.read_sql(sql, cnxn2)
        if len(SVM_PROJECT_ONLINE_SCANTIME) == 0:
            print('online_x_many_phase no scantime')                                                                                                                 
        else:
            SVM_PROJECT_CONFIG = select_project_config_by_modelid_parameter3RD(model_id, "Filter_Feature", server_name_1)
            MAX_TIME = "9998-12-31 23:59:59.999"                        
            LAST_SCANTIME = datetime.datetime.strftime(SVM_PROJECT_ONLINE_SCANTIME.LAST_SCANTIME[0], '%Y-%m-%d %H:%M:%S.%f')[:-3] #20190704 get millisecond 3
            
            FILTER_FEATURE = SVM_PROJECT_CONFIG.PARAM_VALUE[0]
            SVM_PROJECT_RUN = select_projectx_run_by_itime(project_id, LAST_SCANTIME, MAX_TIME, server_name_1)
        
            if len(SVM_PROJECT_RUN) != 0:   
                
                index = base_name.rfind("_")
                current_p = base_name[index+1:]
                
                try:
                    db_p = SVM_PROJECT_RUN[FILTER_FEATURE][0]
                except:
                    db_p = SVM_PROJECT_RUN['FILTER_FEATURE'][0]
                """
                print("---------------")
                print(current_p)
                print(db_p)
                print("---------------")
                """          
                if current_p == str(db_p):
                                               
                    cursor1 = cnxn2.cursor()
                    cursor1.execute("UPDATE SVM_ONLINE_SCANTIME SET LAST_SCANTIME = ? WHERE PROJECT_ID = ? AND DATATYPE = ?", SVM_PROJECT_RUN.ITIME[0], int(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0]), 'X')
                    cnxn2.commit()
                    
                    #sql = r"select * from SVM_ONLINE_PARAMETER where PROJECT_ID = '" + str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0]) + r"' order by SEQ"
                    #SVM_ONLINE_PARAMETER = pd.read_sql(sql, cnxn1)
                    SVM_ONLINE_PARAMETER = select_online_parameter_by_projectid_datatype(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], 'X', server_name_1)
            
                    for k in range(len(SVM_ONLINE_PARAMETER)):
                                              
                        if str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'TOOLID':                           
                            SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.TOOLID[0])],axis=1)
                        elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'LOTNAME':                           
                            SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.LOTNAME[0])],axis=1)
                        elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'FOUPNAME':                           
                            SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.FOUPNAME[0])],axis=1)
                        elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'OPID':                           
                            SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.OPID[0])],axis=1)
                        elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'RECIPE':                           
                            SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.RECIPE[0])],axis=1)
                        elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'ABBRID':                           
                            SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.ABBRID[0])],axis=1)
                        elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'PRODUCT':                           
                            SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.PRODUCT[0])],axis=1)
                        elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'MODELNO':                           
                            SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.MODELNO[0])],axis=1)
                        elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'CHAMBER':                           
                            SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.CHAMBER[0])],axis=1)
                        elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'SHEETID':                           
                            SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.SHEETID[0])],axis=1)
                        elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'SLOTNO':                           
                            SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.SLOTNO[0])],axis=1)
                        elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'POINT':                           
                            SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.POINT[0])],axis=1)
                        elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'FILTER_FEATURE':                           
                            SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.FILTER_FEATURE[0])],axis=1)
                        else:
                            #sql = "select * from " + str(project_id) + "_PARAMETER_DATA where RUNINDEX = " + str(SVM_PROJECT_RUN.RUNINDEX[i]) + r" and UPPER(PARAMETER) = '" + str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() + r"'"
                            #SVM_PARAMETER_DATA_tmp = pd.read_sql(sql, cnxn2)
                            SVM_PARAMETER_DATA_tmp = select_projectx_parameter_data_by_runindex_parameter(project_id, SVM_PROJECT_RUN.RUNINDEX[0], SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k], server_name_1)
                            SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PARAMETER_DATA_tmp.PARAM_VALUE[0])],axis=1)
                        
                        if k == 0:
                            SVM_PARAMETER_DATA = SVM_PARAMETER_DATA_part
                        else:
                            SVM_PARAMETER_DATA = pd.concat([SVM_PARAMETER_DATA, SVM_PARAMETER_DATA_part], axis=0, ignore_index=True)
                        
                        SVM_PARAMETER_DATA_part = SVM_PARAMETER_DATA_part.iloc[0:0]
                
                    #sql = "select * from " + str(project_id) + "_PARAMETER_DATA where RUNINDEX = " + str(SVM_PROJECT_RUN.RUNINDEX[0])
                    #SVM_PARAMETER_DATA = pd.read_sql(sql, cnxn2)
                        
                    if len(SVM_PARAMETER_DATA) == 0:
                        print('SVM_PARAMETER_DATA null')  #20190619 add inert sql in the future
                    else:
                        #df_SVM_PARAMETER_DATA_lite=SVM_PARAMETER_DATA.drop(['RUNINDEX','ITIME'], axis=1)
                        df_SVM_PARAMETER_DATA_lite_T = SVM_PARAMETER_DATA.T #20190619 行列互轉
                        headers = df_SVM_PARAMETER_DATA_lite_T.iloc[0] #20190619 first row set header
                        df_SVM_PARAMETER_DATA_lite_T_H  = pd.DataFrame(df_SVM_PARAMETER_DATA_lite_T.values[1:], columns=headers)
                            
                        print("online x phase[ns]")
                        df_online_X = OnLine_Data_PreProcess_no_y(df_SVM_PARAMETER_DATA_lite_T_H, path_dict['3']["3"])
                        XDI_online_value, XDI_Threshold, XDI_Check, df_contri = OnLine_XDI(df_online_X, path_dict['4']["3"])
                        y_pred, y_base = self.model_predict_online_x(df_online_X, path_dict)
                        MXCI_value, MXCI_T, MYCI_value, MYCI_T, light = CI_online(path_dict['8']["3"], df_online_X, y_pred, y_base)
                        
                        try:
                            SVM_PROJECT_MODEL = select_project_model_by_projectid_status_filterfeature(projectid, "ONLINE", SVM_PROJECT_RUN[FILTER_FEATURE][0], server_name_1)
                        except:
                            SVM_PROJECT_MODEL = select_project_model_by_projectid_status_filterfeature(projectid, "ONLINE", SVM_PROJECT_RUN['FILTER_FEATURE'][0], server_name_1)
                        """                          
                        if SVM_PROJECT_RUN.FILTER_FEATURE[0] is not None:
                            if SVM_PROJECT_RUN.FILTER_FEATURE[0] == 'Point_ID':
                                sql = "select * from SVM_PROJECT_MODEL where MODEL_NAME = \'" + self.base_name + "_POINT_" + SVM_PROJECT_RUN.POINT[0] + r"' AND UPPER(STATUS) = 'ONLINE'"
                            elif SVM_PROJECT_RUN.FILTER_FEATURE[0] == 'Sheet_ID':
                                sql = "select * from SVM_PROJECT_MODEL where MODEL_NAME = \'" + self.base_name + "_SHEET_" + SVM_PROJECT_RUN.SHEETID[0] + r"' AND UPPER(STATUS) = 'ONLINE'"                                
                            SVM_PROJECT_MODEL = pd.read_sql(sql, cnxn2)
                        """
                        if XDI_online_value is not None:
                            sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PROJECT_RUN.RUNINDEX[0]) + r" and PARAMETER = 'XDI' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                            SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                            if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                cursor1 = cnxn2.cursor()
                                sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},getdate())'.format(project_id+"_PREDICT_DATA", int(SVM_PROJECT_RUN.RUNINDEX[0]), 'XDI', XDI_online_value, int(SVM_PROJECT_MODEL.MODEL_ID[0]), 0)
                                cursor1.execute(sql)       
                                cnxn2.commit()                                 
                            else: 
                                now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                cursor1 = cnxn2.cursor()
                                sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", XDI_online_value, 0, int(SVM_PROJECT_RUN.RUNINDEX[0]), 'XDI', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                cursor1.execute(sql)
                                cnxn2.commit()  
                                                  
                        if XDI_Threshold is not None:
                            sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PROJECT_RUN.RUNINDEX[0]) + r" and PARAMETER = 'XDI_Threshold' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                            SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                            if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                cursor1 = cnxn2.cursor()
                                sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},getdate())'.format(project_id+"_PREDICT_DATA", int(SVM_PROJECT_RUN.RUNINDEX[0]), 'XDI_Threshold', XDI_Threshold, int(SVM_PROJECT_MODEL.MODEL_ID[0]), 0)
                                cursor1.execute(sql)       
                                cnxn2.commit()                                 
                            else: 
                                now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                cursor1 = cnxn2.cursor()
                                sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", XDI_Threshold, 0, int(SVM_PROJECT_RUN.RUNINDEX[0]), 'XDI_Threshold', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                cursor1.execute(sql)
                                cnxn2.commit()                                             
                        
                        if XDI_Check is not None:
                            if int(XDI_Check) == 0:
                                sql = "select * from SVM_ONLINE_STATUS where PROJECT_ID = \'" + str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0]) + "\' and RUNINDEX = \'" + str(SVM_PROJECT_RUN.RUNINDEX[0]) + "\'"
                                SVM_ONLINE_STATUS = pd.read_sql(sql, cnxn2)
                                if len(SVM_ONLINE_STATUS) == 0:
                                    cursor1 = cnxn2.cursor()  
                                    cursor1.execute("insert into SVM_ONLINE_STATUS(PROJECT_ID, RUNINDEX, XDI_ALARM, ITIME) values (?,?,?,?)", int(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0]), int(SVM_PROJECT_RUN.RUNINDEX[0]), 1, datetime.datetime.now())
                                    cnxn2.commit()                                    
                                else:
                                    cursor1 = cnxn2.cursor()
                                    cursor1.execute("UPDATE SVM_ONLINE_STATUS SET XDI_ALARM = ? WHERE PROJECT_ID = ? AND RUNINDEX = ?", 1, int(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0]), int(SVM_PROJECT_RUN.RUNINDEX[0]))
                                    cnxn2.commit() 
                                update_projectx_run_xdialarm_by_runindex(project_id, 1, SVM_PROJECT_RUN.RUNINDEX[0], server_name_1)                                                         
                        
                        if y_pred is not None:
                            sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PROJECT_RUN.RUNINDEX[0]) + r" and PARAMETER = 'Predict_Y' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                            SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                            if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                cursor1 = cnxn2.cursor()
                                sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},\'{}\')'.format(project_id+"_PREDICT_DATA", int(SVM_PROJECT_RUN.RUNINDEX[0]), 'Predict_Y', y_pred, int(SVM_PROJECT_MODEL.MODEL_ID[0]), 0, now)
                                cursor1.execute(sql)       
                                cnxn2.commit()                                   
                            else: 
                                now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                cursor1 = cnxn2.cursor()
                                sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", y_pred, 0, int(SVM_PROJECT_RUN.RUNINDEX[0]), 'Predict_Y', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                cursor1.execute(sql)
                                cnxn2.commit()     
                        
                        if y_base is not None:
                            sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PROJECT_RUN.RUNINDEX[0]) + r" and PARAMETER = 'y_base' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                            SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                            if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                cursor1 = cnxn2.cursor()
                                sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},\'{}\')'.format(project_id+"_PREDICT_DATA", int(SVM_PROJECT_RUN.RUNINDEX[0]), 'y_base', y_base, int(SVM_PROJECT_MODEL.MODEL_ID[0]), 0, now)
                                cursor1.execute(sql)       
                                cnxn2.commit()                                   
                            else: 
                                now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                cursor1 = cnxn2.cursor()
                                sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", y_base, 0, int(SVM_PROJECT_RUN.RUNINDEX[0]), 'y_base', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                cursor1.execute(sql)
                                cnxn2.commit() 
                        
                        if MXCI_value is not None:
                            sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PROJECT_RUN.RUNINDEX[0]) + r" and PARAMETER = 'MXCI' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                            SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                            if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                cursor1 = cnxn2.cursor()
                                sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},\'{}\')'.format(project_id+"_PREDICT_DATA", int(SVM_PROJECT_RUN.RUNINDEX[0]), 'MXCI', MXCI_value, int(SVM_PROJECT_MODEL.MODEL_ID[0]), 0, now)
                                cursor1.execute(sql)       
                                cnxn2.commit()                                   
                            else: 
                                now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                cursor1 = cnxn2.cursor()
                                sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", MXCI_value, 0, int(SVM_PROJECT_RUN.RUNINDEX[0]), 'MXCI', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                cursor1.execute(sql)
                                cnxn2.commit()
                            
                        if MXCI_T is not None:
                            sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PROJECT_RUN.RUNINDEX[0]) + r" and PARAMETER = 'MXCI_Threshold' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                            SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                            if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                cursor1 = cnxn2.cursor()
                                sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},\'{}\')'.format(project_id+"_PREDICT_DATA", int(SVM_PROJECT_RUN.RUNINDEX[0]), 'MXCI_Threshold', MXCI_T, int(SVM_PROJECT_MODEL.MODEL_ID[0]), 0, now)
                                cursor1.execute(sql)       
                                cnxn2.commit()                                  
                            else: 
                                now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                cursor1 = cnxn2.cursor()
                                sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", MXCI_T, 0, int(SVM_PROJECT_RUN.RUNINDEX[0]), 'MXCI_Threshold', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                cursor1.execute(sql)
                                cnxn2.commit()
                            
                        if MYCI_value is not None:
                            sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PROJECT_RUN.RUNINDEX[0]) + r" and PARAMETER = 'MYCI' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                            SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                            MYCI_value = float(str(MYCI_value)[0:10])
                            if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                cursor1 = cnxn2.cursor()
                                sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},\'{}\')'.format(project_id+"_PREDICT_DATA", int(SVM_PROJECT_RUN.RUNINDEX[0]), 'MYCI', MYCI_value, int(SVM_PROJECT_MODEL.MODEL_ID[0]), 0, now)
                                cursor1.execute(sql)       
                                cnxn2.commit()                                  
                            else: 
                                now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                cursor1 = cnxn2.cursor()
                                sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", MYCI_value, 0, int(SVM_PROJECT_RUN.RUNINDEX[0]), 'MYCI', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                cursor1.execute(sql)
                                cnxn2.commit()
                            
                        if MYCI_T is not None:
                            sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PROJECT_RUN.RUNINDEX[0]) + r" and PARAMETER = 'MYCI_Threshold' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                            SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                            if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                cursor1 = cnxn2.cursor()
                                sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},\'{}\')'.format(project_id+"_PREDICT_DATA", int(SVM_PROJECT_RUN.RUNINDEX[0]), 'MYCI_Threshold', MYCI_T, int(SVM_PROJECT_MODEL.MODEL_ID[0]), 0, now)
                                cursor1.execute(sql)       
                                cnxn2.commit()                                  
                            else: 
                                now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                cursor1 = cnxn2.cursor()
                                sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", MYCI_T, 0, int(SVM_PROJECT_RUN.RUNINDEX[0]), 'MYCI_Threshold', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                cursor1.execute(sql)
                                cnxn2.commit()
                            
                        if light is not None:
                            now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                            cursor1 = cnxn2.cursor()
                            sql = 'UPDATE {} SET SIGNAL = {} WHERE RUNINDEX = {}'.format(project_id+"_RUN", light, int(SVM_PROJECT_RUN.RUNINDEX[0]))
                            cursor1.execute(sql)
                            cnxn2.commit()
                        
                        if df_contri is not None:
                            for j in range(df_contri.shape[0]):
                                sql = "select * from " + str(project_id) + "_CONTRIBUTION_DATA where RUNINDEX = " + str(SVM_PROJECT_RUN.RUNINDEX[0]) + r" and PARAMETER = '" + str(df_contri.Col_Name[j]) + r"' and MODEL = 'XDI'"
                                SVM_PROJECT_CONTRIBUTION_DATA = pd.read_sql(sql, cnxn2)
                                if len(SVM_PROJECT_CONTRIBUTION_DATA) == 0:
                                    now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                    cursor1 = cnxn2.cursor()
                                    sql = 'INSERT INTO {} (RUNINDEX,MODEL,PARAMETER,CONTRIBUTION,ITIME) VALUES ({},\'{}\',\'{}\',{},\'{}\')'.format(project_id+"_CONTRIBUTION_DATA", int(SVM_PROJECT_RUN.RUNINDEX[0]), 'XDI', str(df_contri.Col_Name[j]), float(df_contri.Contribution[j]), now)
                                    cursor1.execute(sql)       
                                    cnxn2.commit()                                  
                                else: 
                                    now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                    cursor1 = cnxn2.cursor()
                                    sql = 'UPDATE {} SET CONTRIBUTION = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL = \'{}\''.format(project_id+"_CONTRIBUTION_DATA", float(df_contri.Contribution[j]), int(SVM_PROJECT_RUN.RUNINDEX[0]), str(df_contri.Col_Name[j]), 'XDI')
                                    cursor1.execute(sql)
                                    cnxn2.commit()               
                                
        return None
    
    def many_online_x_phase(self, projectid, model_id, datatype, retrain_num, batch_num):
        for feature in self.feature_lists:
            self.online_x_many_phase(projectid, model_id, datatype, base_name=self.filter_dir_name[feature], retrain_num=retrain_num, batch_num=batch_num)
        return None
    
    def many_to_many_online_x(self, projectid, model_id, datatype, retrain_num):
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
                
        self.get_saved_path_config()
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=None)
        print("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num))
        self.many_online_x_phase(projectid, model_id, datatype, retrain_num=retrain_num, batch_num=batch_num)

        return None

if __name__ == '__main__':        
    #20190627 online data must go 10.96.18.199, get SERVER_NAME to the server
    def timer(n):
        #i = 1
        while True:
            #print(i)                 
            SVM_PROJECT_ONLINE_SCANTIME = select_project_online_scantime_x_many() #20190718 tell care 48 point error, modify sql
            
            if len(SVM_PROJECT_ONLINE_SCANTIME) != 0:
                
                for a in range(len(SVM_PROJECT_ONLINE_SCANTIME)):
                                   
                    projectid = SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[a]
                    SVM_MODEL_ONLINE_X_MANY = select_project_model_by_projectid_status(projectid, "ONLINE")
                    
                    if len(SVM_MODEL_ONLINE_X_MANY) != 0:
                    
                        SVM = SuperVM(str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_NAME[a]), str(SVM_PROJECT_ONLINE_SCANTIME.UPLOAD_FILE[a]), r"Config.json")                
                        datatype = SVM_PROJECT_ONLINE_SCANTIME.DATATYPE[a].upper()
                        retrain_num = int(SVM_MODEL_ONLINE_X_MANY.MODEL_SEQ[0]) 
                        model_id = SVM_MODEL_ONLINE_X_MANY.MODEL_ID[0]
                        """
                        print("--------------")
                        print(projectid)
                        print(model_id)
                        print(datatype)
                        print(retrain_num)
                        print("--------------")
                        """
                        SVM.many_to_many_online_x(projectid, model_id, datatype, retrain_num)
                    
                    else:
                        print("SVM_MODEL_ONLINE_X_MANY None")

            #i = i+1                 
            time.sleep(n)
    timer(5) 

#-----------------------------------------------------------------------------------------------------
#SmartVM_Constructor_online_xy_abnormal.py
from Data_PreProcess import Data_PreProcess_Train, Data_PreProcess_Test, OnLine_Data_PreProcess
from XDI import XDI_off_line_report, Build_XDI_Model, OnLine_XDI
from YDI import YDI_off_line_report, Build_YDI_Model, OnLine_YDI
from MXCI_MYCI_pre import pre_MXCI_MYCI
from MXCI_MYCI import MXCI_MYCI_offline, CI_online
from XGB_model import xgb_online_predict
from PLS_model import pls_online_predict_x_abnormal
from Read_path import read_path
from config import read_config
from CreateLog import WriteLog
from DB_operation import select_project_otm, select_project_by_projectid, select_project_config_by_modelid_parameter3RD, select_project_model_by_projectid_predictresult, select_online_parameter_by_projectid, select_projectx_parameter_data_by_runindex_parameter, select_projectx_predict_data_by_runindex_parameter_modelid, update_project_model_predictresult_by_modelid, update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid, insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict

import os
import json
import traceback
# from Data_Check import Data_Check
from shutil import copyfile
import pandas as pd
import datetime
import time
import pyodbc


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

########################################################################################################################
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
    
    def model_predict_online_x(self, df_online_X, path_dict):
        xgb_y_pred = xgb_online_predict(path_dict['6']["XGB"]["5"], df_online_X) #20190702 eat 05_Prediction  
        #20190702 mark temporarily
        pls_online_predict_x_abnormal(path_dict['6']["PLS"]["5"], df_online_X)
        
        input_path = read_path(path_dict['6']["PLS"]["5"])
        mylog = WriteLog(input_path["log_path"], input_path["error_path"])    
        config = read_config(input_path["config_path"], mylog)
        config_y = config["Y"][0]
        Baseline_Model = config["Model_Selection"][config_y]["Baseline_Model"]  
        pls_y_name = config["Y"][0]+"_pred_PLS"
        
        #20190702 mark temporarily
        pls_predict_test_path = os.path.join(path_dict['6']["PLS"]["5"], "trainPredResult.csv")
        pls_test=pd.read_csv(pls_predict_test_path)
        pls_test.rename({'Y_pred': pls_y_name}, axis=1, inplace=True) #20190722 after pls predict, must rename
        #pls_y_pred = pls_test['Y_pred']
        pls_y_pred = pls_test[pls_y_name]
        #pls_y_pred = xgb_y_pred * 1.2
        #20190709 for pls_y_pred null
        if len(pls_y_pred) == 0:
            pls_y_predc = 0
        else:
            pls_y_predc = pls_y_pred[0] #20190621 because return series, must assign location

        if Baseline_Model == "PLS":
            #20190702 mark temporarily
            return xgb_y_pred, pls_y_predc 
            #return xgb_y_pred, pls_y_pred
        else:
            #20190702 mark temporarily
            return pls_y_predc, xgb_y_pred
            #return pls_y_pred, xgb_y_pred
    
    def online_xy_phase(self, projectid, base_name, retrain_num, batch_num=None):
        retrain_num = self.get_max_retrain_num(retrain_num)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)
        """
        SVM_PROJECT_FORRETRAIN = select_project_model_by_projectid(projectid) #20190708 avoid retrain cause predict error[find not file]
        if SVM_PROJECT_FORRETRAIN.STATUS[0].upper() == "ONLINE":
            path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]
        else:
            path_dict = self.path_config[base_name][str(retrain_num-1)][str(batch_num)]
        """
        path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]
        SVM_PROJECT_OTM = select_project_by_projectid(projectid)
        
        #20190620 get server name
        server_name_1 = str(SVM_PROJECT_OTM.SERVER_NAME[0])
        
        db_name = "APC"
        user = "YL"
        password = "YL$212" 
        cnxn2 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server_name_1 + ';DATABASE=' + db_name + ';UID=' + user + ';PWD=' + password)
        
        project_id = "SVM_PROJECT" + str(SVM_PROJECT_OTM.PROJECT_ID[0])
        
        SVM_PROJECT_MODEL_ABNORMAL = select_project_model_by_projectid_predictresult(SVM_PROJECT_OTM.PROJECT_ID[0], "False", server_name_1)
        
        if len(SVM_PROJECT_MODEL_ABNORMAL) == 0:
            print("no model in abnormal")
        else:                    
            SVM_PROJECT_CONFIG = select_project_config_by_modelid_parameter3RD(SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], "Y", server_name_1)
            
            if len(SVM_PROJECT_CONFIG) == 0:
                print('config no data')                                                                                                                                                       
            else:       
                PREDICT_START_TIME = datetime.datetime.strftime(SVM_PROJECT_MODEL_ABNORMAL.PREDICT_START_TIME[0], '%Y-%m-%d %H:%M:%S.%f')[:-3]
                RETRAIN_END_TIME = datetime.datetime.strftime(SVM_PROJECT_MODEL_ABNORMAL.RETRAIN_END_TIME[0], '%Y-%m-%d %H:%M:%S.%f')[:-3]
                
                sql = "select * from " + str(project_id) + r"_PARAMETER_DATA where PARAMETER = '" + str(SVM_PROJECT_CONFIG.PARAM_VALUE[0]) + r"' and ITIME > '" + PREDICT_START_TIME + r"' and ITIME < '" + RETRAIN_END_TIME + r"' order by ITIME" 
                SVM_PARAMETER_DATA_y = pd.read_sql(sql, cnxn2)
                
                if len(SVM_PARAMETER_DATA_y) == 0:
                    print('SVM_PARAMETER_DATA_y null') 
                else:   
                    for m in range(len(SVM_PARAMETER_DATA_y)): 
                                                
                        sql = "select * from " + str(project_id) + "_RUN where RUNINDEX = \'" + str(SVM_PARAMETER_DATA_y.RUNINDEX[m]) + "\'"
                        SVM_PROJECT_RUN = pd.read_sql(sql, cnxn2)
                                              
                        SVM_ONLINE_PARAMETER = select_online_parameter_by_projectid(SVM_PROJECT_OTM.PROJECT_ID[0], server_name_1)
                        
                        for k in range(len(SVM_ONLINE_PARAMETER)):
                                                  
                            if str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'TOOLID':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.TOOLID[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'LOTNAME':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.LOTNAME[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'FOUPNAME':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.FOUPNAME[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'OPID':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.OPID[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'RECIPE':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.RECIPE[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'ABBRID':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.ABBRID[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'PRODUCT':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.PRODUCT[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'MODELNO':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.MODELNO[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'CHAMBER':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.CHAMBER[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'SHEETID':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.SHEETID[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'SLOTNO':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.SLOTNO[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'POINT':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.POINT[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'FILTER_FEATURE':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.FILTER_FEATURE[0])],axis=1)
                            else:
                                SVM_PARAMETER_DATA_tmp = select_projectx_parameter_data_by_runindex_parameter(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k], server_name_1)
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PARAMETER_DATA_tmp.PARAM_VALUE[0])],axis=1)
                            
                            if k == 0:
                                SVM_PARAMETER_DATA = SVM_PARAMETER_DATA_part
                            else:
                                SVM_PARAMETER_DATA = pd.concat([SVM_PARAMETER_DATA, SVM_PARAMETER_DATA_part], axis=0, ignore_index=True)
                            
                            SVM_PARAMETER_DATA_part = SVM_PARAMETER_DATA_part.iloc[0:0] #20190710 clear dataframe data
                                                                                                                                                                                                                           
                        if len(SVM_PARAMETER_DATA) == 0:
                            print('SVM_PARAMETER_DATA null')  #20190619 add inert sql in the future
                        else:
                            #df_SVM_PARAMETER_DATA_lite=SVM_PARAMETER_DATA.drop(['RUNINDEX','ITIME'], axis=1)
                            df_SVM_PARAMETER_DATA_lite_T = SVM_PARAMETER_DATA.T #20190619 行列互轉
                            headers = df_SVM_PARAMETER_DATA_lite_T.iloc[0] #20190619 first row set header
                            df_SVM_PARAMETER_DATA_lite_T_H  = pd.DataFrame(df_SVM_PARAMETER_DATA_lite_T.values[1:], columns=headers)
                            
                            df_online_X, df_online_y = OnLine_Data_PreProcess(df_SVM_PARAMETER_DATA_lite_T_H, path_dict['3']["3"])
                            XDI_online_value, XDI_Threshold, XDI_Check, df_contri = OnLine_XDI(df_online_X, path_dict['4']["3"])
                            df_YDI_online = OnLine_YDI(df_online_X, df_online_y, path_dict['5']["3"])
                            y_pred, y_base = self.model_predict_online_x(df_online_X, path_dict)
                            MXCI_value, MXCI_T, MYCI_value, MYCI_T, light = CI_online(path_dict['8']["3"], df_online_X, y_pred, y_base)
    
                            if XDI_online_value is not None:
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'XDI', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'XDI', XDI_online_value, SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name_1)                                
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, XDI_online_value, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'XDI', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                                      
                            if XDI_Threshold is not None:
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'XDI_Threshold', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:  
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'XDI_Threshold', XDI_Threshold, SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name_1)  
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, XDI_Threshold, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'XDI_Threshold', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)                                             
                                                                                                           
                            if y_pred is not None:
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Predict_Y', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Predict_Y', y_pred, SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name_1)                                  
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, y_pred, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'Predict_Y', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                            
                            if y_base is not None:
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'y_base', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'y_base', y_base, SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name_1)                                  
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, y_base, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'y_base', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                            
                            if MXCI_value is not None:
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MXCI', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:   
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MXCI', MXCI_value, SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name_1)  
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MXCI_value, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MXCI', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                
                            if MXCI_T is not None:
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MXCI_Threshold', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MXCI_Threshold', MXCI_T, SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name_1)                                    
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MXCI_T, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MXCI_Threshold', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                
                            if MYCI_value is not None:
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MYCI', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                MYCI_value = float(str(MYCI_value)[0:10])
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MYCI', MYCI_value, SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name_1)                                   
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MYCI_value, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MYCI', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                
                            if MYCI_T is not None:
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MYCI_Threshold', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MYCI_Threshold', MYCI_T, SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 1, server_name_1)                                    
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, MYCI_T, 1, SVM_PARAMETER_DATA_y.RUNINDEX[m], 'MYCI_Threshold', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                            
                            if df_YDI_online is not None:
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Interval', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:  
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Interval', df_YDI_online.Interval[0], SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 0, server_name_1)                              
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Interval[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Interval', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI', df_YDI_online.YDI[0], SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 0, server_name_1)                                           
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.YDI[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI_Threshold', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:   
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI_Threshold', df_YDI_online.YDI_Threshold[0], SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 0, server_name_1)                                   
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.YDI_Threshold[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI_Threshold', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Y_avg', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Y_avg', df_YDI_online.Y_avg[0], SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 0, server_name_1)                                                  
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Y_avg[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Y_avg', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                
                                SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Cluster_idx', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Cluster_idx', df_YDI_online.Cluster_idx[0], SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], 0, server_name_1)                                                  
                                else: 
                                    update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Cluster_idx[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Cluster_idx', SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)
                              
                    update_project_model_predictresult_by_modelid("True", SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], server_name_1)                                                                                        
        return None
    
    def one_to_all_online_xy(self, projectid, retrain_num):          
        retrain_num = self.get_max_retrain_num(retrain_num=retrain_num)
        self.get_saved_path_config() #20190715 get path_config
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=None)
        print("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num))
        self.online_xy_phase(projectid, base_name=self.base_name, retrain_num=retrain_num, batch_num=batch_num)
        
        return None
 
if __name__ == '__main__':        
    #20190627 online data must go 10.96.18.199, get SERVER_NAME to the server
    def timer(n):
        while True: 
            SVM_PROJECT_OTM = select_project_otm()
            
            if len(SVM_PROJECT_OTM) != 0:
                
                for a in range(len(SVM_PROJECT_OTM)):
                    
                    projectid = SVM_PROJECT_OTM.PROJECT_ID[a]
                    SVM_PROJECT_MODEL_OTM = select_project_model_by_projectid_predictresult(projectid, "False") 
                    
                    if len(SVM_PROJECT_MODEL_OTM) != 0:
                                            
                        SVM = SuperVM(str(SVM_PROJECT_OTM.PROJECT_NAME[a]), str(SVM_PROJECT_OTM.UPLOAD_FILE[a]), r"Config.json")                                                                           
                        retrain_num = int(SVM_PROJECT_MODEL_OTM.MODEL_SEQ[0])
                        SVM.one_to_all_online_xy(projectid, retrain_num)
                        
                    else:
                        print("SVM_PROJECT_MODEL_OTM None")
                                     
            time.sleep(n)
    timer(5) 
    
#------------------------------------------------------------------------------
#SmartVM_Constructor_online_xy_many_abnormal.py
from Data_PreProcess import Data_PreProcess_Train, Data_PreProcess_Test, OnLine_Data_PreProcess
from XDI import XDI_off_line_report, Build_XDI_Model, OnLine_XDI
from YDI import YDI_off_line_report, Build_YDI_Model, OnLine_YDI
from MXCI_MYCI_pre import pre_MXCI_MYCI
from MXCI_MYCI import MXCI_MYCI_offline, CI_online
from XGB_model import xgb_online_predict
from PLS_model import pls_online_predict_x_many_abnormal
from Read_path import read_path
from config import read_config
from CreateLog import WriteLog
from DB_operation import select_project_mtm, select_project_by_projectid, select_project_model_by_projectid_predictresult, select_project_model_by_projectid_status2_filterfeature, select_project_config_by_modelid_parameter3RD, select_online_parameter_by_projectid, select_projectx_parameter_data_by_runindex_parameter, select_projectx_parameter_data_by_parameter_itime_itime, select_projectx_predict_data_by_runindex_parameter_modelid, update_project_model_predictresult_by_modelid, update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid, insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict

import os
import json
import traceback
# from Data_Check import Data_Check

from shutil import copyfile
import pandas as pd
import pyodbc
import datetime
import time


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
    
    def model_predict_online_x(self, df_online_X, path_dict):
        xgb_y_pred = xgb_online_predict(path_dict['6']["XGB"]["5"], df_online_X) #20190702 eat 05_Prediction  
        #20190702 mark temporarily
        pls_online_predict_x_many_abnormal(path_dict['6']["PLS"]["5"], df_online_X)
        
        input_path = read_path(path_dict['6']["PLS"]["5"])
        mylog = WriteLog(input_path["log_path"], input_path["error_path"])    
        config = read_config(input_path["config_path"], mylog)
        config_y = config["Y"][0]
        Baseline_Model = config["Model_Selection"][config_y]["Baseline_Model"]    
        pls_y_name = config["Y"][0]+"_pred_PLS"
        
        #20190702 mark temporarily
        pls_predict_test_path = os.path.join(path_dict['6']["PLS"]["5"], "trainPredResult.csv")
        pls_test=pd.read_csv(pls_predict_test_path)
        pls_test.rename({'Y_pred': pls_y_name}, axis=1, inplace=True) #20190722 after pls predict, must rename
        #pls_y_pred = pls_test['Y_pred']
        pls_y_pred = pls_test[pls_y_name]
        #pls_y_pred = xgb_y_pred * 1.2

        if Baseline_Model == "PLS":
            #20190702 mark temporarily
            return xgb_y_pred, pls_y_pred[0] #20190621 because return series, must assign location
            #return xgb_y_pred, pls_y_pred
        else:
            #20190702 mark temporarily
            return pls_y_pred[0], xgb_y_pred
            #return pls_y_pred, xgb_y_pred    
    
    def online_xy_many_phase(self, projectid, base_name, retrain_num, batch_num=None):
        retrain_num = self.get_max_retrain_num(retrain_num)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)

        path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]
        SVM_PROJECT_MTM = select_project_by_projectid(projectid)
        
        #server_name = "10.96.18.199"
        db_name = "APC"
        user = "YL"
        password = "YL$212"                  
                
        #20190620 get server name
        server_name_1 = str(SVM_PROJECT_MTM.SERVER_NAME[0])
        cnxn2 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server_name_1 + ';DATABASE=' + db_name + ';UID=' + user + ';PWD=' + password)
                
        project_id = "SVM_PROJECT" + str(SVM_PROJECT_MTM.PROJECT_ID[0])

        SVM_PROJECT_MODEL_ABNORMAL = select_project_model_by_projectid_predictresult(SVM_PROJECT_MTM.PROJECT_ID[0], "False", server_name_1)
        
        if len(SVM_PROJECT_MODEL_ABNORMAL) == 0:
            print("no model in manay abnormal")
        else:                   
            SVM_PROJECT_CONFIG = select_project_config_by_modelid_parameter3RD(SVM_PROJECT_MODEL_ABNORMAL.MODEL_ID[0], "Y", server_name_1)
            
            if len(SVM_PROJECT_CONFIG) == 0:
                print('config no data')                                                                                                                                                       
            else:     
                PREDICT_START_TIME = datetime.datetime.strftime(SVM_PROJECT_MODEL_ABNORMAL.PREDICT_START_TIME[0], '%Y-%m-%d %H:%M:%S.%f')[:-3]
                RETRAIN_END_TIME = datetime.datetime.strftime(SVM_PROJECT_MODEL_ABNORMAL.RETRAIN_END_TIME[0], '%Y-%m-%d %H:%M:%S.%f')[:-3]
                
                #sql = "select * from " + str(project_id) + r"_PARAMETER_DATA where PARAMETER = '" + str(SVM_PROJECT_CONFIG.PARAM_VALUE[0]) + r"' and ITIME > '" + PREDICT_START_TIME + r"' and ITIME < '" + RETRAIN_END_TIME + r"' order by ITIME" 
                #SVM_PARAMETER_DATA_y = pd.read_sql(sql, cnxn2)
                SVM_PARAMETER_DATA_y = select_projectx_parameter_data_by_parameter_itime_itime(project_id, SVM_PROJECT_CONFIG.PARAM_VALUE[0], PREDICT_START_TIME, RETRAIN_END_TIME, server_name_1)
                
                if len(SVM_PARAMETER_DATA_y) == 0:
                    print('SVM_PARAMETER_DATA_y null') 
                else:   
                    for m in range(len(SVM_PARAMETER_DATA_y)): 
                        
                        FILTER_FEATURE = SVM_PROJECT_CONFIG.PARAM_VALUE[0]                        
                        sql = "select * from " + str(project_id) + "_RUN where RUNINDEX = \'" + str(SVM_PARAMETER_DATA_y.RUNINDEX[m]) + "\'"
                        SVM_PROJECT_RUN = pd.read_sql(sql, cnxn2)
                        
                        if len(SVM_PROJECT_RUN) != 0:
                            
                            index = base_name.rfind("_")
                            current_p = base_name[index+1:]
                            
                            try:
                                db_p = SVM_PROJECT_RUN[FILTER_FEATURE][0]
                            except:
                                db_p = SVM_PROJECT_RUN['FILTER_FEATURE'][0]
                                                                
                            if current_p == db_p:

                                SVM_ONLINE_PARAMETER = select_online_parameter_by_projectid(SVM_PROJECT_MTM.PROJECT_ID[0], server_name_1)
                        
                                for k in range(len(SVM_ONLINE_PARAMETER)):
                                                          
                                    if str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'TOOLID':                           
                                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.TOOLID[0])],axis=1)
                                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'LOTNAME':                           
                                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.LOTNAME[0])],axis=1)
                                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'FOUPNAME':                           
                                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.FOUPNAME[0])],axis=1)
                                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'OPID':                           
                                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.OPID[0])],axis=1)
                                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'RECIPE':                           
                                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.RECIPE[0])],axis=1)
                                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'ABBRID':                           
                                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.ABBRID[0])],axis=1)
                                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'PRODUCT':                           
                                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.PRODUCT[0])],axis=1)
                                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'MODELNO':                           
                                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.MODELNO[0])],axis=1)
                                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'CHAMBER':                           
                                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.CHAMBER[0])],axis=1)
                                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'SHEETID':                           
                                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.SHEETID[0])],axis=1)
                                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'SLOTNO':                           
                                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.SLOTNO[0])],axis=1)
                                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'POINT':                           
                                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.POINT[0])],axis=1)
                                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'FILTER_FEATURE':                           
                                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.FILTER_FEATURE[0])],axis=1)
                                    else:
                                        SVM_PARAMETER_DATA_tmp = select_projectx_parameter_data_by_runindex_parameter(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[m], SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k], server_name_1)
                                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PARAMETER_DATA_tmp.PARAM_VALUE[0])],axis=1)
                                    
                                    if k == 0:
                                        SVM_PARAMETER_DATA = SVM_PARAMETER_DATA_part
                                    else:
                                        SVM_PARAMETER_DATA = pd.concat([SVM_PARAMETER_DATA, SVM_PARAMETER_DATA_part], axis=0, ignore_index=True)
                                    
                                    SVM_PARAMETER_DATA_part = SVM_PARAMETER_DATA_part.iloc[0:0]
                                    
                                if len(SVM_PARAMETER_DATA) == 0:
                                    print('SVM_PARAMETER_DATA null')  #20190619 add inert sql in the future
                                else:
                                    #df_SVM_PARAMETER_DATA_lite=SVM_PARAMETER_DATA.drop(['RUNINDEX','ITIME'], axis=1)
                                    df_SVM_PARAMETER_DATA_lite_T = SVM_PARAMETER_DATA.T #20190619 行列互轉
                                    headers = df_SVM_PARAMETER_DATA_lite_T.iloc[0] #20190619 first row set header
                                    df_SVM_PARAMETER_DATA_lite_T_H  = pd.DataFrame(df_SVM_PARAMETER_DATA_lite_T.values[1:], columns=headers)
                                        
                                    print("online x phase[ns]")
                                    df_online_X, df_online_y = OnLine_Data_PreProcess(df_SVM_PARAMETER_DATA_lite_T_H, path_dict['3']["3"])
                                    XDI_online_value, XDI_Threshold, XDI_Check, df_contri = OnLine_XDI(df_online_X, path_dict['4']["3"])
                                    df_YDI_online = OnLine_YDI(df_online_X, df_online_y, path_dict['5']["3"])
                                    y_pred, y_base = self.model_predict_online_x(df_online_X, path_dict)
                                    MXCI_value, MXCI_T, MYCI_value, MYCI_T, light = CI_online(path_dict['8']["3"], df_online_X, y_pred, y_base)
                                    """
                                    if SVM_PROJECT_RUN.FILTER_FEATURE[0] is not None:
                                        if SVM_PROJECT_RUN.FILTER_FEATURE[0] == 'Point_ID':
                                            sql = "select * from SVM_PROJECT_MODEL where MODEL_NAME = \'" + self.base_name + "_POINT_" + SVM_PROJECT_RUN.POINT[0] + r"' AND UPPER(STATUS) in ('ABNORMAL','ABNORMAL->ONLINE')"
                                        elif SVM_PROJECT_RUN.FILTER_FEATURE[0] == 'Sheet_ID':
                                            sql = "select * from SVM_PROJECT_MODEL where MODEL_NAME = \'" + self.base_name + "_SHEET_" + SVM_PROJECT_RUN.SHEETID[0] + r"' AND UPPER(STATUS) in ('ABNORMAL','ABNORMAL->ONLINE')"                                
                                        SVM_PROJECT_MODEL = pd.read_sql(sql, cnxn2)
                                    """
                                    try:
                                        SVM_PROJECT_MODEL = select_project_model_by_projectid_status2_filterfeature(projectid, "ABNORMAL", "ABNORMAL->ONLINE", SVM_PROJECT_RUN[FILTER_FEATURE][0], server_name_1)
                                    except:
                                        SVM_PROJECT_MODEL = select_project_model_by_projectid_status2_filterfeature(projectid, "ABNORMAL", "ABNORMAL->ONLINE", SVM_PROJECT_RUN['FILTER_FEATURE'][0], server_name_1)
                                    
                                    if XDI_online_value is not None:
                                        sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PARAMETER_DATA_y.RUNINDEX[m]) + r" and PARAMETER = 'XDI' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                                        SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                            cursor1 = cnxn2.cursor()
                                            sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},getdate())'.format(project_id+"_PREDICT_DATA", int(SVM_PARAMETER_DATA_y.RUNINDEX[m]), 'XDI', XDI_online_value, int(SVM_PROJECT_MODEL.MODEL_ID[0]), 1)
                                            cursor1.execute(sql)       
                                            cnxn2.commit()                                 
                                        else: 
                                            now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                            cursor1 = cnxn2.cursor()
                                            sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", XDI_online_value, 1, int(SVM_PARAMETER_DATA_y.RUNINDEX[m]), 'XDI', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                            cursor1.execute(sql)
                                            cnxn2.commit()  
                                                              
                                    if XDI_Threshold is not None:
                                        sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PARAMETER_DATA_y.RUNINDEX[m]) + r" and PARAMETER = 'XDI_Threshold' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                                        SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                            cursor1 = cnxn2.cursor()
                                            sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},getdate())'.format(project_id+"_PREDICT_DATA", int(SVM_PARAMETER_DATA_y.RUNINDEX[m]), 'XDI_Threshold', XDI_Threshold, int(SVM_PROJECT_MODEL.MODEL_ID[0]), 1)
                                            cursor1.execute(sql)       
                                            cnxn2.commit()                                 
                                        else: 
                                            now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                            cursor1 = cnxn2.cursor()
                                            sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", XDI_Threshold, 1, int(SVM_PARAMETER_DATA_y.RUNINDEX[m]), 'XDI_Threshold', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                            cursor1.execute(sql)
                                            cnxn2.commit()                                                                                                  
                                    
                                    if y_pred is not None:
                                        sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PARAMETER_DATA_y.RUNINDEX[m]) + r" and PARAMETER = 'Predict_Y' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                                        SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                            now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                            cursor1 = cnxn2.cursor()
                                            sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},\'{}\')'.format(project_id+"_PREDICT_DATA", int(SVM_PARAMETER_DATA_y.RUNINDEX[m]), 'Predict_Y', y_pred, int(SVM_PROJECT_MODEL.MODEL_ID[0]), 1, now)
                                            cursor1.execute(sql)       
                                            cnxn2.commit()                                   
                                        else: 
                                            now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                            cursor1 = cnxn2.cursor()
                                            sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", y_pred, 1, int(SVM_PARAMETER_DATA_y.RUNINDEX[m]), 'Predict_Y', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                            cursor1.execute(sql)
                                            cnxn2.commit()     
                                    
                                    if y_base is not None:
                                        sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PARAMETER_DATA_y.RUNINDEX[m]) + r" and PARAMETER = 'y_base' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                                        SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                            now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                            cursor1 = cnxn2.cursor()
                                            sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},\'{}\')'.format(project_id+"_PREDICT_DATA", int(SVM_PARAMETER_DATA_y.RUNINDEX[m]), 'y_base', y_base, int(SVM_PROJECT_MODEL.MODEL_ID[0]), 1, now)
                                            cursor1.execute(sql)       
                                            cnxn2.commit()                                   
                                        else: 
                                            now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                            cursor1 = cnxn2.cursor()
                                            sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", y_base, 1, int(SVM_PARAMETER_DATA_y.RUNINDEX[m]), 'y_base', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                            cursor1.execute(sql)
                                            cnxn2.commit() 
                                    
                                    if MXCI_value is not None:
                                        sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PARAMETER_DATA_y.RUNINDEX[m]) + r" and PARAMETER = 'MXCI' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                                        SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                            now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                            cursor1 = cnxn2.cursor()
                                            sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},\'{}\')'.format(project_id+"_PREDICT_DATA", int(SVM_PARAMETER_DATA_y.RUNINDEX[m]), 'MXCI', MXCI_value, int(SVM_PROJECT_MODEL.MODEL_ID[0]), 1, now)
                                            cursor1.execute(sql)       
                                            cnxn2.commit()                                   
                                        else: 
                                            now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                            cursor1 = cnxn2.cursor()
                                            sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", MXCI_value, 1, int(SVM_PARAMETER_DATA_y.RUNINDEX[m]), 'MXCI', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                            cursor1.execute(sql)
                                            cnxn2.commit()
                                        
                                    if MXCI_T is not None:
                                        sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PARAMETER_DATA_y.RUNINDEX[m]) + r" and PARAMETER = 'MXCI_Threshold' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                                        SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                            now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                            cursor1 = cnxn2.cursor()
                                            sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},\'{}\')'.format(project_id+"_PREDICT_DATA", int(SVM_PARAMETER_DATA_y.RUNINDEX[m]), 'MXCI_Threshold', MXCI_T, int(SVM_PROJECT_MODEL.MODEL_ID[0]), 1, now)
                                            cursor1.execute(sql)       
                                            cnxn2.commit()                                  
                                        else: 
                                            now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                            cursor1 = cnxn2.cursor()
                                            sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", MXCI_T, 1, int(SVM_PARAMETER_DATA_y.RUNINDEX[m]), 'MXCI_Threshold', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                            cursor1.execute(sql)
                                            cnxn2.commit()
                                        
                                    if MYCI_value is not None:
                                        sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PARAMETER_DATA_y.RUNINDEX[m]) + r" and PARAMETER = 'MYCI' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                                        SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                                        MYCI_value = float(str(MYCI_value)[0:10])
                                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                            now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                            cursor1 = cnxn2.cursor()
                                            sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},\'{}\')'.format(project_id+"_PREDICT_DATA", int(SVM_PARAMETER_DATA_y.RUNINDEX[m]), 'MYCI', MYCI_value, int(SVM_PROJECT_MODEL.MODEL_ID[0]), 1, now)
                                            cursor1.execute(sql)       
                                            cnxn2.commit()                                  
                                        else: 
                                            now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                            cursor1 = cnxn2.cursor()
                                            sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", MYCI_value, 1, int(SVM_PARAMETER_DATA_y.RUNINDEX[m]), 'MYCI', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                            cursor1.execute(sql)
                                            cnxn2.commit()
                                        
                                    if MYCI_T is not None:
                                        sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PARAMETER_DATA_y.RUNINDEX[m]) + r" and PARAMETER = 'MYCI_Threshold' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                                        SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                            now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                            cursor1 = cnxn2.cursor()
                                            sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},\'{}\')'.format(project_id+"_PREDICT_DATA", int(SVM_PARAMETER_DATA_y.RUNINDEX[m]), 'MYCI_Threshold', MYCI_T, int(SVM_PROJECT_MODEL.MODEL_ID[0]), 1, now)
                                            cursor1.execute(sql)       
                                            cnxn2.commit()                                  
                                        else: 
                                            now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                                            cursor1 = cnxn2.cursor()
                                            sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", MYCI_T, 1, int(SVM_PARAMETER_DATA_y.RUNINDEX[m]), 'MYCI_Threshold', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                            cursor1.execute(sql)
                                            cnxn2.commit()      
                                    
                                    if df_YDI_online is not None:
                                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Interval', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                                        if len(SVM_PROJECT_PREDICT_DATA) == 0:  
                                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Interval', df_YDI_online.Interval[0], SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name_1)                              
                                        else: 
                                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Interval[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Interval', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                                        
                                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI', df_YDI_online.YDI[0], SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name_1)                                           
                                        else: 
                                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.YDI[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                                        
                                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                                        if len(SVM_PROJECT_PREDICT_DATA) == 0:   
                                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI_Threshold', df_YDI_online.YDI_Threshold[0], SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name_1)                                   
                                        else: 
                                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.YDI_Threshold[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                                        
                                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Y_avg', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Y_avg', df_YDI_online.Y_avg[0], SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name_1)                                                  
                                        else: 
                                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Y_avg[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Y_avg', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                                        
                                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Cluster_idx', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Cluster_idx', df_YDI_online.Cluster_idx[0], SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name_1)                                                  
                                        else: 
                                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Cluster_idx[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Cluster_idx', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                                                    
                    update_project_model_predictresult_by_modelid("True", SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)               
        return None
    
    def many_online_xy_phase(self, projectid, retrain_num, batch_num):
        for feature in self.feature_lists:
            self.online_xy_many_phase(projectid, base_name=self.filter_dir_name[feature], retrain_num=retrain_num, batch_num=batch_num)
        return None

        
    def many_to_many_online_xy(self, projectid, retrain_num):
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
        self.many_online_xy_phase(projectid, retrain_num=retrain_num, batch_num=batch_num)

        return None
  
if __name__ == '__main__':        
    #20190627 online data must go 10.96.18.199, get SERVER_NAME to the server
    def timer(n):
        while True:
            SVM_PROJECT_MTM = select_project_mtm()
                                    
            if len(SVM_PROJECT_MTM) != 0:
                
                for a in range(len(SVM_PROJECT_MTM)):
                    
                    projectid = SVM_PROJECT_MTM.PROJECT_ID[a]  
                    SVM_PROJECT_MODEL_MTM = select_project_model_by_projectid_predictresult(projectid, "False") 
                    
                    if len(SVM_PROJECT_MODEL_MTM) != 0:
                                          
                        SVM = SuperVM(str(SVM_PROJECT_MTM.PROJECT_NAME[a]), str(SVM_PROJECT_MTM.UPLOAD_FILE[a]), r"Config.json")                                                              
                        retrain_num = int(SVM_PROJECT_MODEL_MTM.MODEL_SEQ[0])                  
                        SVM.many_to_many_online_xy(projectid, retrain_num) 
                    
                    else:
                        print("SVM_PROJECT_MODEL_MTM None")
                                   
            time.sleep(n)
    timer(5) 
    
#------------------------------------------------------------------------------
#SmartVM_Constructor_online_y.py
from Data_PreProcess import Data_PreProcess_Train, Data_PreProcess_Test, OnLine_Data_PreProcess
from XDI import XDI_off_line_report, Build_XDI_Model
from YDI import YDI_off_line_report, Build_YDI_Model, OnLine_YDI
from MXCI_MYCI_pre import pre_MXCI_MYCI
from MXCI_MYCI import MXCI_MYCI_offline
from DB_operation import select_project_config_by_modelid_parameter3RD, select_project_model_by_projectid, select_project_model_by_projectid_status, select_project_online_scantime_y, select_project_online_scantime_by_projectid_datatype, select_online_parameter_by_projectid, select_online_status_by_projectid_runindex, select_projectx_run_by_runindex, select_projectx_parameter_data_by_runindex_parameter, select_projectx_parameter_data_by_parameter_itime, select_projectx_predict_data_by_runindex_parameter_modelid, update_online_scantime_lastscantime_by_projectid_datatype, update_online_status_ydialarm_by_projectid_runindex, update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid, update_projectx_run_ydialarm_by_runindex, insert_online_status_projectid_runindex_ydialarm_itime, insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict

import os
import json
import traceback
# from Data_Check import Data_Check
from shutil import copyfile
import pandas as pd
import datetime
import time


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

########################################################################################################################
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
    
    #20190710 Predict YDI never enter abnormal mode
    def online_y_phase(self, projectid, datatype, base_name, retrain_num, batch_num=None):
        retrain_num = self.get_max_retrain_num(retrain_num)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)
        """
        SVM_PROJECT_FORRETRAIN = select_project_model_by_projectid(projectid)
        if SVM_PROJECT_FORRETRAIN.STATUS[0].upper() == "RETRAIN":
            path_dict = self.path_config[base_name][str(retrain_num-1)][str(batch_num)]
        elif SVM_PROJECT_FORRETRAIN.STATUS[0].upper() == "ONLINE":
            path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]
        """
        path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]                        
        SVM_PROJECT_ONLINE_SCANTIME = select_project_online_scantime_by_projectid_datatype(projectid, datatype)
        #20190620 get server name
        server_name_1 = str(SVM_PROJECT_ONLINE_SCANTIME.SERVER_NAME[0])
             
        project_id = "SVM_PROJECT" + str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0])
        
        SVM_PROJECT_MODEL = select_project_model_by_projectid_status(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], "ONLINE", server_name_1)
        
        SVM_PROJECT_CONFIG = select_project_config_by_modelid_parameter3RD(SVM_PROJECT_MODEL.MODEL_ID[0], "Y", server_name_1)
        
        if len(SVM_PROJECT_CONFIG) == 0:
            print('config no data')                                                                                                                                                       
        else:                      
            LAST_SCANTIME = datetime.datetime.strftime(SVM_PROJECT_ONLINE_SCANTIME.LAST_SCANTIME[0], '%Y-%m-%d %H:%M:%S.%f')[:-3]
            #sql = "select * from " + str(project_id) + r"_PARAMETER_DATA where PARAMETER = '" + str(SVM_PROJECT_CONFIG.PARAM_VALUE[0].upper()) + r"' and ITIME > '" + LAST_SCANTIME + r"' order by ITIME"                
            #SVM_PARAMETER_DATA_y = pd.read_sql(sql, cnxn2)
            SVM_PARAMETER_DATA_y = select_projectx_parameter_data_by_parameter_itime(project_id, SVM_PROJECT_CONFIG.PARAM_VALUE[0], LAST_SCANTIME, server_name_1)
            
            if len(SVM_PARAMETER_DATA_y) == 0:
                print('SVM_PARAMETER_DATA_y null') 
            else:                                                   
                #sql = "select * from " + str(project_id) + "_RUN where RUNINDEX = \'" + str(SVM_PARAMETER_DATA_y.RUNINDEX[0]) + "\'"
                #SVM_PROJECT_RUN = pd.read_sql(sql, cnxn2)
                SVM_PROJECT_RUN = select_projectx_run_by_runindex(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], server_name_1)
                update_online_scantime_lastscantime_by_projectid_datatype(SVM_PARAMETER_DATA_y.ITIME[0], SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], 'Y', server_name_1)

                SVM_ONLINE_PARAMETER = select_online_parameter_by_projectid(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], server_name_1)
                
                for k in range(len(SVM_ONLINE_PARAMETER)):
                                          
                    if str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'TOOLID':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.TOOLID[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'LOTNAME':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.LOTNAME[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'FOUPNAME':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.FOUPNAME[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'OPID':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.OPID[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'RECIPE':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.RECIPE[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'ABBRID':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.ABBRID[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'PRODUCT':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.PRODUCT[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'MODELNO':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.MODELNO[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'CHAMBER':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.CHAMBER[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'SHEETID':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.SHEETID[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'SLOTNO':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.SLOTNO[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'POINT':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.POINT[0])],axis=1)
                    elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'FILTER_FEATURE':                           
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.FILTER_FEATURE[0])],axis=1)
                    else:
                        SVM_PARAMETER_DATA_tmp = select_projectx_parameter_data_by_runindex_parameter(project_id, SVM_PROJECT_RUN.RUNINDEX[0], SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k], server_name_1)
                        SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PARAMETER_DATA_tmp.PARAM_VALUE[0])],axis=1)
                    
                    if k == 0:
                        SVM_PARAMETER_DATA = SVM_PARAMETER_DATA_part
                    else:
                        SVM_PARAMETER_DATA = pd.concat([SVM_PARAMETER_DATA, SVM_PARAMETER_DATA_part], axis=0, ignore_index=True)
                    
                    SVM_PARAMETER_DATA_part = SVM_PARAMETER_DATA_part.iloc[0:0]
                
                if len(SVM_PARAMETER_DATA) == 0:
                    print('SVM_PARAMETER_DATA null')  #20190619 add inert sql in the future
                else:
                    #df_SVM_PARAMETER_DATA_lite=SVM_PARAMETER_DATA.drop(['RUNINDEX','ITIME'], axis=1)
                    df_SVM_PARAMETER_DATA_lite_T = SVM_PARAMETER_DATA.T #20190619 行列互轉
                    headers = df_SVM_PARAMETER_DATA_lite_T.iloc[0] #20190619 first row set header
                    df_SVM_PARAMETER_DATA_lite_T_H  = pd.DataFrame(df_SVM_PARAMETER_DATA_lite_T.values[1:], columns=headers)
                    
                    print("online y phase[ns]")
                    df_online_X, df_online_y = OnLine_Data_PreProcess(df_SVM_PARAMETER_DATA_lite_T_H, path_dict['3']["3"])
                    df_YDI_online = OnLine_YDI(df_online_X, df_online_y, path_dict['5']["3"])
                                       
                    if df_YDI_online is not None:
                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Interval', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                        if len(SVM_PROJECT_PREDICT_DATA) == 0:  
                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Interval', df_YDI_online.Interval[0], SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name_1)                              
                        else: 
                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Interval[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Interval', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                        
                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI', df_YDI_online.YDI[0], SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name_1)                                           
                        else: 
                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.YDI[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                        
                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                        if len(SVM_PROJECT_PREDICT_DATA) == 0:   
                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI_Threshold', df_YDI_online.YDI_Threshold[0], SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name_1)                                   
                        else: 
                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.YDI_Threshold[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'YDI_Threshold', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                        
                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Y_avg', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Y_avg', df_YDI_online.Y_avg[0], SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name_1)                                                  
                        else: 
                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Y_avg[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Y_avg', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                        
                        SVM_PROJECT_PREDICT_DATA = select_projectx_predict_data_by_runindex_parameter_modelid(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Cluster_idx', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                        if len(SVM_PROJECT_PREDICT_DATA) == 0:
                            insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict(project_id, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Cluster_idx', df_YDI_online.Cluster_idx[0], SVM_PROJECT_MODEL.MODEL_ID[0], 0, server_name_1)                                                  
                        else: 
                            update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid(project_id, df_YDI_online.Cluster_idx[0], 0, SVM_PARAMETER_DATA_y.RUNINDEX[0], 'Cluster_idx', SVM_PROJECT_MODEL.MODEL_ID[0], server_name_1)
                        
                        if int(df_YDI_online.YDI_Check[0]) == 0:
                            SVM_ONLINE_STATUS = select_online_status_by_projectid_runindex(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], SVM_PARAMETER_DATA_y.RUNINDEX[0], server_name_1)
                            if len(SVM_ONLINE_STATUS) == 0:      
                                insert_online_status_projectid_runindex_ydialarm_itime(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], SVM_PARAMETER_DATA_y.RUNINDEX[0], 1, datetime.datetime.now(), server_name_1)
                            else:
                                update_online_status_ydialarm_by_projectid_runindex(1, SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], SVM_PARAMETER_DATA_y.RUNINDEX[0], server_name_1)
                            update_projectx_run_ydialarm_by_runindex(project_id, 1, SVM_PARAMETER_DATA_y.RUNINDEX[0], server_name_1)   
        return None
    
    def one_to_all_online_y(self, projectid, datatype, retrain_num):           
        retrain_num = self.get_max_retrain_num(retrain_num=retrain_num)
        self.get_saved_path_config()
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=None)
        print("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num))
        self.online_y_phase(projectid, datatype, base_name=self.base_name, retrain_num=retrain_num, batch_num=batch_num)
        
        return None
  
if __name__ == '__main__':        
    #20190627 online data must go 10.96.18.199, get SERVER_NAME to the server
    def timer(n):
        while True:
            
            SVM_PROJECT_ONLINE_SCANTIME = select_project_online_scantime_y()                        
            
            if len(SVM_PROJECT_ONLINE_SCANTIME) != 0:
                
                for a in range(len(SVM_PROJECT_ONLINE_SCANTIME)):
                                        
                    projectid = SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[a]
                    SVM_MODEL_ONLINE_Y = select_project_model_by_projectid_status(projectid, "ONLINE")
                    
                    if len(SVM_MODEL_ONLINE_Y) != 0:
                        
                        SVM = SuperVM(str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_NAME[a]), str(SVM_PROJECT_ONLINE_SCANTIME.UPLOAD_FILE[a]), r"Config.json")                
                        datatype = SVM_PROJECT_ONLINE_SCANTIME.DATATYPE[a].upper()
                        retrain_num = int(SVM_MODEL_ONLINE_Y.MODEL_SEQ[0]) 
                        
                        print("--------------")
                        print(projectid)
                        print(datatype)
                        print(retrain_num)
                        print("--------------")
                        SVM.one_to_all_online_y(projectid, datatype, retrain_num)
                    
                    else:
                        print("SVM_MODEL_ONLINE_Y None")
                                 
            time.sleep(n)
    timer(5) 
    
#--------------------------------------------------------------------------------------
#SmartVM_Constructor_online_y_many.py
from Data_Preview import Data_Preview
from Data_PreProcess import Data_PreProcess_Train, Data_PreProcess_Test, OnLine_Data_PreProcess, OnLine_Data_PreProcess_no_y
from XDI import XDI_off_line_report, Build_XDI_Model, OnLine_XDI
from YDI import YDI_off_line_report, Build_YDI_Model, OnLine_YDI
from MXCI_MYCI_pre import pre_MXCI_MYCI
from MXCI_MYCI import MXCI_MYCI_offline, CI_online
from XGB_model import xgb_tuning, xgb_build, xgb_predict, xgb_merge_tuning, xgb_merge_predict, xgb_batch_predict, xgb_online_predict
from PLS_model import pls_build, pls_predict, pls_merge_predict, pls_batch_predict, pls_online_predict_y_many
from Read_path import read_path
from config import read_config, save_config
from CreateLog import WriteLog
from DB_operation import DB_Connection, select_project_config_by_modelid_parameter3RD, select_project_model_by_projectid, select_project_model_by_projectid_status, select_project_model_by_projectid_predictresult, select_project_model_by_projectid_status_filterfeature, select_project_online_scantime_y_many, select_project_online_scantime_by_projectid_datatype, select_online_parameter_by_projectid, select_online_parameter_by_projectid_datatype, select_online_status_by_projectid_runindex, select_projectx_run_by_itime, select_projectx_parameter_data_by_runindex_parameter, select_projectx_predict_data_by_runindex_parameter_modelid, select_projectx_contribution_data_by_runindex_parameter_model, update_project_model_predictresult_by_modelid, update_online_scantime_lastscantime_by_projectid_datatype, update_online_status_xdialarm_by_projectid_runindex, update_projectx_predict_data_paramvalue_isretrainpredict_by_runindex_parameter_modelid, update_projectx_run_xdialarm_by_runindex, update_projectx_run_ydialarm_by_runindex, update_projectx_contribution_data_contribution_by_runindex_parameter_model, insert_online_status_projectid_runindex_xdialarm_itime, insert_projectx_predict_data_runindex_parameter_paramvalue_modelid_isretrainpredict, insert_projectx_contribution_data_runindex_model_parameter_contribution

import os
import json
import traceback
from Path import All_path
from Data_Collector import Data_Collector
# from Data_Check import Data_Check
from Exclusion import DataRemove, FeatureExclude

from shutil import copyfile
import numpy as np
import pandas as pd
import pyodbc
import datetime
import time


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
    
    #20190710 Predict YDI never enter abnormal mode
    def online_y_many_phase(self, projectid, datatype, base_name, retrain_num, batch_num=None):
        retrain_num = self.get_max_retrain_num(retrain_num)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)
        """
        SVM_PROJECT_FORRETRAIN = select_project_model_by_projectid(projectid)
        if SVM_PROJECT_FORRETRAIN.STATUS[0].upper() == "RETRAIN":
            path_dict = self.path_config[base_name][str(retrain_num-1)][str(batch_num)]
        elif SVM_PROJECT_FORRETRAIN.STATUS[0].upper() == "ONLINE":
            path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]
        """
        path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]
        #server_name = "10.96.18.199"
        db_name = "APC"
        user = "YL"
        password = "YL$212"                  
        #cnxn1 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server_name + ';DATABASE=' + db_name + ';UID=' + user + ';PWD=' + password)
                
        #sql = "select * from SVM_PROJECT where PROJECT_NAME = \'" + self.base_name + "\'"
        #SVM_PROJECT = pd.read_sql(sql, cnxn1)
        
        SVM_PROJECT_ONLINE_SCANTIME = select_project_online_scantime_by_projectid_datatype(projectid, datatype)
        #20190620 get server name
        server_name_1 = str(SVM_PROJECT_ONLINE_SCANTIME.SERVER_NAME[0])
        cnxn2 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server_name_1 + ';DATABASE=' + db_name + ';UID=' + user + ';PWD=' + password)
        
        project_id = "SVM_PROJECT" + str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0])
        
        #sql = "select * from SVM_ONLINE_SCANTIME where PROJECT_ID = \'" + str(SVM_PROJECT.PROJECT_ID[0]) + "\' and DATATYPE = \'Y\'"  
        #SVM_ONLINE_SCANTIME = pd.read_sql(sql, cnxn2)
        
        sql = "select * from SVM_PROJECT_MODEL where PROJECT_ID = \'" + str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0]) + "\'"
        SVM_PROJECT_MODEL = pd.read_sql(sql, cnxn2)
        
        sql = "select * from SVM_PROJECT_CONFIG where MODEL_ID = \'" + str(SVM_PROJECT_MODEL.MODEL_ID[0]) + "\' and PARAMETER_3RD = \'Y\'"  
        SVM_PROJECT_CONFIG = pd.read_sql(sql, cnxn2)
        
        if len(SVM_PROJECT_CONFIG) == 0:
            print('config no data')                                                                                                                                
        else:            
            LAST_SCANTIME = datetime.datetime.strftime(SVM_PROJECT_ONLINE_SCANTIME.LAST_SCANTIME[0], '%Y-%m-%d %H:%M:%S.%f')[:-3]
            sql = "select * from " + str(project_id) + r"_PARAMETER_DATA where PARAMETER = '" + str(SVM_PROJECT_CONFIG.PARAM_VALUE[0]) + r"' and ITIME > '" + LAST_SCANTIME + r"'" 
            SVM_PARAMETER_DATA_y = pd.read_sql(sql, cnxn2)
            
            if len(SVM_PARAMETER_DATA_y) == 0:
                print('SVM_PARAMETER_DATA_y null ns') 
            else:
                FILTER_FEATURE = SVM_PROJECT_CONFIG.PARAM_VALUE[0] 
                sql = "select * from " + str(project_id) + "_RUN where RUNINDEX = " + str(SVM_PARAMETER_DATA_y.RUNINDEX[0])
                SVM_PROJECT_RUN = pd.read_sql(sql, cnxn2)
                        
                if len(SVM_PROJECT_RUN) != 0:
                    
                    index = base_name.rfind("_")
                    current_p = base_name[index+1:]
                    
                    try:
                        db_p = SVM_PROJECT_RUN[FILTER_FEATURE][0]
                    except:
                        db_p = SVM_PROJECT_RUN['FILTER_FEATURE'][0]                   
                            
                    if current_p == db_p:
                 
                        #cursor1 = cnxn2.cursor()
                        #cursor1.execute("UPDATE SVM_ONLINE_SCANTIME SET LAST_SCANTIME = ? WHERE PROJECT_ID = ? AND DATATYPE = ?", SVM_PARAMETER_DATA_y.ITIME[0], int(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0]), 'Y')
                        #cnxn2.commit()
                        update_online_scantime_lastscantime_by_projectid_datatype(SVM_PARAMETER_DATA_y.ITIME[0], SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], 'Y', server_name_1)
                        
                        #sql = r"select * from SVM_ONLINE_PARAMETER where PROJECT_ID = '" + str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0]) + r"' order by SEQ"
                        #SVM_ONLINE_PARAMETER = pd.read_sql(sql, cnxn1)
                        SVM_ONLINE_PARAMETER = select_online_parameter_by_projectid(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0], server_name_1)
            
                        for k in range(len(SVM_ONLINE_PARAMETER)):
                                                  
                            if str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'TOOLID':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.TOOLID[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'LOTNAME':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.LOTNAME[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'FOUPNAME':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.FOUPNAME[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'OPID':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.OPID[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'RECIPE':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.RECIPE[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'ABBRID':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.ABBRID[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'PRODUCT':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.PRODUCT[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'MODELNO':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.MODELNO[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'CHAMBER':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.CHAMBER[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'SHEETID':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.SHEETID[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'SLOTNO':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.SLOTNO[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'POINT':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.POINT[0])],axis=1)
                            elif str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() == 'FILTER_FEATURE':                           
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PROJECT_RUN.FILTER_FEATURE[0])],axis=1)
                            else:
                                #sql = "select * from " + str(project_id) + "_PARAMETER_DATA where RUNINDEX = " + str(SVM_PROJECT_RUN.RUNINDEX[i]) + r" and UPPER(PARAMETER) = '" + str(SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k]).upper() + r"'"
                                #SVM_PARAMETER_DATA_tmp = pd.read_sql(sql, cnxn2)
                                SVM_PARAMETER_DATA_tmp = select_projectx_parameter_data_by_runindex_parameter(project_id, SVM_PROJECT_RUN.RUNINDEX[0], SVM_ONLINE_PARAMETER.ONLINE_PARAMETER[k], server_name_1)
                                SVM_PARAMETER_DATA_part = pd.concat([pd.Series(SVM_ONLINE_PARAMETER.OFFLINE_PARAMETER[k]),pd.Series(SVM_PARAMETER_DATA_tmp.PARAM_VALUE[0])],axis=1)
                            
                            if k == 0:
                                SVM_PARAMETER_DATA = SVM_PARAMETER_DATA_part
                            else:
                                SVM_PARAMETER_DATA = pd.concat([SVM_PARAMETER_DATA, SVM_PARAMETER_DATA_part], axis=0, ignore_index=True)
                            
                            SVM_PARAMETER_DATA_part = SVM_PARAMETER_DATA_part.iloc[0:0]                                
                        
                        #sql = "select * from " + str(project_id) + "_PARAMETER_DATA where RUNINDEX = " + str(SVM_PARAMETER_DATA_y.RUNINDEX[0])
                        #SVM_PARAMETER_DATA = pd.read_sql(sql, cnxn2)
                        
                        if len(SVM_PARAMETER_DATA) == 0:
                            print('SVM_PARAMETER_DATA null')  #20190619 add inert sql in the future
                        else:
                            #df_SVM_PARAMETER_DATA_lite=SVM_PARAMETER_DATA.drop(['RUNINDEX','ITIME'], axis=1)
                            df_SVM_PARAMETER_DATA_lite_T = SVM_PARAMETER_DATA.T #20190619 行列互轉
                            headers = df_SVM_PARAMETER_DATA_lite_T.iloc[0] #20190619 first row set header
                            df_SVM_PARAMETER_DATA_lite_T_H  = pd.DataFrame(df_SVM_PARAMETER_DATA_lite_T.values[1:], columns=headers)
                                
                            print("online y phase[ns]")
                            df_online_X, df_online_y = OnLine_Data_PreProcess(df_SVM_PARAMETER_DATA_lite_T_H, path_dict['3']["3"])
                            df_YDI_online = OnLine_YDI(df_online_X, df_online_y, path_dict['5']["3"])
                            """
                            if SVM_PROJECT_RUN.FILTER_FEATURE[0] is not None:
                                if SVM_PROJECT_RUN.FILTER_FEATURE[0] == 'Point_ID':
                                    sql = "select * from SVM_PROJECT_MODEL where MODEL_NAME = \'" + self.base_name + "_POINT_" + SVM_PROJECT_RUN.POINT[0] + r"' AND UPPER(STATUS) = 'ONLINE'"
                                elif SVM_PROJECT_RUN.FILTER_FEATURE[0] == 'Sheet_ID':
                                    sql = "select * from SVM_PROJECT_MODEL where MODEL_NAME = \'" + self.base_name + "_SHEET_" + SVM_PROJECT_RUN.SHEETID[0] + r"' AND UPPER(STATUS) = 'ONLINE'"                                
                                SVM_PROJECT_MODEL = pd.read_sql(sql, cnxn2)
                            """
                            try:
                                SVM_PROJECT_MODEL = select_project_model_by_projectid_status_filterfeature(projectid, "ONLINE", SVM_PROJECT_RUN[FILTER_FEATURE][0], server_name_1)
                            except:
                                SVM_PROJECT_MODEL = select_project_model_by_projectid_status_filterfeature(projectid, "ONLINE", SVM_PROJECT_RUN['FILTER_FEATURE'][0], server_name_1)
                            
                            if df_YDI_online is not None:
                                sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PARAMETER_DATA_y.RUNINDEX[0]) + r" and PARAMETER = 'Interval' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                                SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    cursor1 = cnxn2.cursor()
                                    sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},getdate())'.format(project_id+"_PREDICT_DATA", int(SVM_PARAMETER_DATA_y.RUNINDEX[0]), 'Interval', df_YDI_online.Interval[0], int(SVM_PROJECT_MODEL.MODEL_ID[0]), 0) 
                                    cursor1.execute(sql)       
                                    cnxn2.commit()                                  
                                else: 
                                    cursor1 = cnxn2.cursor()
                                    sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", df_YDI_online.Interval[0], 0, int(SVM_PARAMETER_DATA_y.RUNINDEX[0]), 'Interval', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                    cursor1.execute(sql)
                                    cnxn2.commit()
                                
                                sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PARAMETER_DATA_y.RUNINDEX[0]) + r" and PARAMETER = 'YDI' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                                SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    cursor1 = cnxn2.cursor()
                                    sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},getdate())'.format(project_id+"_PREDICT_DATA", int(SVM_PARAMETER_DATA_y.RUNINDEX[0]), 'YDI', df_YDI_online.YDI[0], int(SVM_PROJECT_MODEL.MODEL_ID[0]), 0) 
                                    cursor1.execute(sql)       
                                    cnxn2.commit()                                             
                                else: 
                                    cursor1 = cnxn2.cursor()
                                    sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", df_YDI_online.YDI[0], 0, int(SVM_PARAMETER_DATA_y.RUNINDEX[0]), 'YDI', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                    cursor1.execute(sql)
                                    cnxn2.commit()
                                
                                sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PARAMETER_DATA_y.RUNINDEX[0]) + r" and PARAMETER = 'YDI_Threshold' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                                SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    cursor1 = cnxn2.cursor()
                                    sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},getdate())'.format(project_id+"_PREDICT_DATA", int(SVM_PARAMETER_DATA_y.RUNINDEX[0]), 'YDI_Threshold', df_YDI_online.YDI_Threshold[0], int(SVM_PROJECT_MODEL.MODEL_ID[0]), 0) 
                                    cursor1.execute(sql)       
                                    cnxn2.commit()                                       
                                else: 
                                    cursor1 = cnxn2.cursor()
                                    sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", df_YDI_online.YDI_Threshold[0], 0, int(SVM_PARAMETER_DATA_y.RUNINDEX[0]), 'YDI_Threshold', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                    cursor1.execute(sql)
                                    cnxn2.commit()
                                
                                sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PARAMETER_DATA_y.RUNINDEX[0]) + r" and PARAMETER = 'Y_avg' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                                SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    cursor1 = cnxn2.cursor()
                                    sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},getdate())'.format(project_id+"_PREDICT_DATA", int(SVM_PARAMETER_DATA_y.RUNINDEX[0]), 'Y_avg', df_YDI_online.Y_avg[0], int(SVM_PROJECT_MODEL.MODEL_ID[0]), 0) 
                                    cursor1.execute(sql)       
                                    cnxn2.commit()                                                  
                                else: 
                                    cursor1 = cnxn2.cursor()
                                    sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", df_YDI_online.Y_avg[0], 0, int(SVM_PARAMETER_DATA_y.RUNINDEX[0]), 'Y_avg', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                    cursor1.execute(sql)
                                    cnxn2.commit()
                                
                                sql = "select * from " + str(project_id) + "_PREDICT_DATA where RUNINDEX = " + str(SVM_PARAMETER_DATA_y.RUNINDEX[0]) + r" and PARAMETER = 'Cluster_idx' and MODEL_ID = " + str(SVM_PROJECT_MODEL.MODEL_ID[0])
                                SVM_PROJECT_PREDICT_DATA = pd.read_sql(sql, cnxn2)
                                if len(SVM_PROJECT_PREDICT_DATA) == 0:
                                    cursor1 = cnxn2.cursor()
                                    sql = 'INSERT INTO {} (RUNINDEX,PARAMETER,PARAM_VALUE,MODEL_ID,IS_RETRAIN_PREDICT,ITIME) VALUES ({},\'{}\',{},{},{},getdate())'.format(project_id+"_PREDICT_DATA", int(SVM_PARAMETER_DATA_y.RUNINDEX[0]), 'Cluster_idx', df_YDI_online.Cluster_idx[0], int(SVM_PROJECT_MODEL.MODEL_ID[0]), 0) 
                                    cursor1.execute(sql)       
                                    cnxn2.commit()                                                   
                                else: 
                                    cursor1 = cnxn2.cursor()
                                    sql = 'UPDATE {} SET PARAM_VALUE = {}, IS_RETRAIN_PREDICT = {} WHERE RUNINDEX = {} AND PARAMETER = \'{}\' AND MODEL_ID = {}'.format(project_id+"_PREDICT_DATA", df_YDI_online.Cluster_idx[0], 0, int(SVM_PARAMETER_DATA_y.RUNINDEX[0]), 'Cluster_idx', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
                                    cursor1.execute(sql)
                                    cnxn2.commit()
                                
                                if int(df_YDI_online.YDI_Check[0]) == 0:
                                    sql = "select * from SVM_ONLINE_STATUS where PROJECT_ID = \'" + str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0]) + "\' and RUNINDEX = \'" + str(SVM_PARAMETER_DATA_y.RUNINDEX[0]) + "\'"
                                    SVM_ONLINE_STATUS = pd.read_sql(sql, cnxn2)
                                    if len(SVM_ONLINE_STATUS) == 0:
                                        cursor1 = cnxn2.cursor()  
                                        cursor1.execute("insert into SVM_ONLINE_STATUS(PROJECT_ID, RUNINDEX, YDI_ALARM, ITIME) values (?,?,?,?)", int(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0]), int(SVM_PARAMETER_DATA_y.RUNINDEX[0]), 1, datetime.datetime.now())
                                        cnxn2.commit()                                         
                                    else:
                                        cursor1 = cnxn2.cursor()
                                        cursor1.execute("UPDATE SVM_ONLINE_STATUS SET YDI_ALARM = ? WHERE PROJECT_ID = ? AND RUNINDEX = ?", 1, int(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[0]), int(SVM_PARAMETER_DATA_y.RUNINDEX[0]))
                                        cnxn2.commit() 
                                    update_projectx_run_ydialarm_by_runindex(project_id, 1, SVM_PARAMETER_DATA_y.RUNINDEX[0], server_name_1)                                                                                   
        return None
    
    def many_online_y_phase(self, projectid, datatype, retrain_num, batch_num):
        for feature in self.feature_lists:
            self.online_y_many_phase(projectid, datatype, base_name=self.filter_dir_name[feature], retrain_num=retrain_num, batch_num=batch_num)
        return None
    
    def many_to_many_online_y(self, projectid, datatype, retrain_num):
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
        
        self.get_saved_path_config()
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=None)
        self.many_online_y_phase(projectid, datatype, retrain_num=retrain_num, batch_num=batch_num)

        return None

if __name__ == '__main__':        
    #20190627 online data must go 10.96.18.199, get SERVER_NAME to the server
    def timer(n):
        #i = 1
        while True:
            #print(i)                  
            SVM_PROJECT_ONLINE_SCANTIME = select_project_online_scantime_y_many()
            
            if len(SVM_PROJECT_ONLINE_SCANTIME) != 0:
                
                for a in range(len(SVM_PROJECT_ONLINE_SCANTIME)):
                
                    projectid = SVM_PROJECT_ONLINE_SCANTIME.PROJECT_ID[a]
                    SVM_MODEL_ONLINE_Y_MANY = select_project_model_by_projectid_status(projectid, "ONLINE")
                    
                    if len(SVM_MODEL_ONLINE_Y_MANY) != 0:
                        
                        SVM = SuperVM(str(SVM_PROJECT_ONLINE_SCANTIME.PROJECT_NAME[a]), str(SVM_PROJECT_ONLINE_SCANTIME.UPLOAD_FILE[a]), r"Config.json")                
                        datatype = SVM_PROJECT_ONLINE_SCANTIME.DATATYPE[a].upper()
                        retrain_num = int(SVM_MODEL_ONLINE_Y_MANY.MODEL_SEQ[0]) 
                        SVM.many_to_many_online_y(projectid, datatype, retrain_num)   
                    
                    else:
                        print("SVM_MODEL_ONLINE_Y_MANY None")                      
            #i = i+1                 
            time.sleep(n)
    timer(5) 

#-----------------------------------------------------------------------------------------
#SmartVM_Constructor_retrain.py
from Data_Preview import Data_Preview
from Data_PreProcess import Data_PreProcess_Train, Data_PreProcess_Test, OnLine_Data_PreProcess
from XDI import XDI_off_line_report, Build_XDI_Model, OnLine_XDI
from YDI import YDI_off_line_report, Build_YDI_Model, OnLine_YDI
from MXCI_MYCI_pre import pre_MXCI_MYCI
from MXCI_MYCI import MXCI_MYCI_offline, CI_online
from Model_Selection import Model_Selection
from XGB_model import xgb_tuning, xgb_build, xgb_predict, xgb_merge_tuning, xgb_merge_predict, xgb_batch_predict, xgb_online_predict
from PLS_model import pls_build, pls_predict, pls_merge_predict, pls_batch_predict, pls_online_predict_x
from Read_path import read_path
from config import read_config, save_config
from CreateLog import WriteLog
from DB_operation import DB_Connection, select_project_model_by_modelid, select_project_model_by_modelname_status, update_project_model_modelstatus_by_modelid, update_project_model_retrainstarttime_by_modelid, update_project_model_modelstatus_modelstep_waitconfirm_by_modelid, update_project_model_mae_mape_by_modelid

import os
import json
import traceback
from Path import All_path
from Data_Collector import Data_Collector
# from Data_Check import Data_Check
from Exclusion import DataRemove, FeatureExclude

from shutil import copyfile
import numpy as np
import pandas as pd
import pyodbc
import datetime
import time
from json2csv import json2csv


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
            pls_y_name = config["Y"][0]+"_pred_PLS"
            
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
    
    def retrain_phase(self, modelid, base_name, retrain_num, batch_num=None):
        retrain_num = self.get_max_retrain_num(retrain_num)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)
        path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]

        print("RETRAIN phase")
        merge_flag = '2'
        test_flag = '3'
        
        server_name = "10.96.18.199"
        db_name = "APC"
        user = "YL"
        password = "YL$212"
        cnxn2 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server_name + ';DATABASE=' + db_name + ';UID=' + user + ';PWD=' + password)
        
        cursor1 = cnxn2.cursor()
        cursor1.execute("UPDATE SVM_PROJECT_MODEL SET RETRAIN_START_TIME = ? WHERE MODEL_ID = ?", datetime.datetime.now(), int(modelid))
        cnxn2.commit()
        
        Data_PreProcess_Train(path_dict['3'][merge_flag], merge_flag=True)
        Build_XDI_Model(path_dict['4'][merge_flag])
        XDI_off_line_report(path_dict['4'][test_flag], mode="Merge")
        Build_YDI_Model(path_dict['5'][merge_flag])
        YDI_off_line_report(path_dict['5'][test_flag], mode="Merge")
        self.model(path_dict, merge_flag=True)
        msg_Model, win = Model_Selection(path_dict['7']['1'], mode="Merge")
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
            
        pre_MXCI_MYCI(path_dict['8'][merge_flag])
        MXCI_MYCI_offline(path_dict['8'][test_flag],  mode="Merge")
        
        cursor1 = cnxn2.cursor()
        cursor1.execute("UPDATE SVM_PROJECT_MODEL SET RETRAIN_END_TIME = ?, RETRAIN_RESULT = ? WHERE MODEL_ID = ?", datetime.datetime.now(), 'OK', int(modelid))
        cnxn2.commit()

        return None
    
    def retrain_many_phase(self, base_name, retrain_num, batch_num=None):
        retrain_num = self.get_max_retrain_num(retrain_num)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=batch_num)
        path_dict = self.path_config[base_name][str(retrain_num)][str(batch_num)]

        print("RETRAIN many phase")
        merge_flag = '2'
        test_flag = '3'
        
        #base_name = MODEL_NAME(m2m)       
        server_name = "10.96.18.199"
        db_name = "APC"
        user = "YL"
        password = "YL$212"
        cnxn2 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server_name + ';DATABASE=' + db_name + ';UID=' + user + ';PWD=' + password)
        
        SVM_PROJECT_MODEL = select_project_model_by_modelname_status(base_name, "RETRAIN")
        
        update_project_model_retrainstarttime_by_modelid(datetime.datetime.now(), SVM_PROJECT_MODEL.MODEL_ID[0])

        Data_PreProcess_Train(path_dict['3'][merge_flag], merge_flag=True)
        Build_XDI_Model(path_dict['4'][merge_flag])
        XDI_off_line_report(path_dict['4'][test_flag], mode="Merge")
        Build_YDI_Model(path_dict['5'][merge_flag])
        YDI_off_line_report(path_dict['5'][test_flag], mode="Merge")
        self.model(path_dict, merge_flag=True)
        msg_Model, win = Model_Selection(path_dict['7']['1'], mode="Merge")
        
        output_paths = {}
        if str(win).upper() == 'XGB':           
            output_paths["Model_Selection_Report"] = os.path.join(path_dict['7']['1'], "Model_Selection_Report.csv")
            select_df = pd.read_csv(output_paths["Model_Selection_Report"])
			# 2019079 change Test to train By Peter Lin
            out_df = select_df[(select_df["Model"] == 'XGB') & (select_df["Data"] == 'Train')]            
            update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PROJECT_MODEL.MODEL_ID[0])
        elif str(win).upper() == 'PLS':
            output_paths["Model_Selection_Report"] = os.path.join(path_dict['7']['1'], "Model_Selection_Report.csv")
            select_df = pd.read_csv(output_paths["Model_Selection_Report"])
			# 2019079 change Test to train By Peter Lin			
            out_df = select_df[(select_df["Model"] == 'PLS') & (select_df["Data"] == 'Train')]            
            update_project_model_mae_mape_by_modelid(out_df["MAE"], out_df["MAPE"], SVM_PROJECT_MODEL.MODEL_ID[0])
            
        pre_MXCI_MYCI(path_dict['8'][merge_flag])
        MXCI_MYCI_offline(path_dict['8'][test_flag],  mode="Merge")
        
        cursor1 = cnxn2.cursor()
        cursor1.execute("UPDATE SVM_PROJECT_MODEL SET RETRAIN_END_TIME = ?, RETRAIN_RESULT = ? WHERE MODEL_ID = ?", datetime.datetime.now(), 'OK', int(SVM_PROJECT_MODEL.MODEL_ID[0]))
        cnxn2.commit()

        return None
    
    def many_retrain_phase(self, retrain_num, batch_num):
        for feature in self.feature_lists:
            self.retrain_many_phase(base_name=self.filter_dir_name[feature], retrain_num=retrain_num, batch_num=batch_num)
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

    def one_to_all_retrain(self, modelid):
        path = os.path.join(self.base_path, self.base_name)
        dc = Data_Collector(self.base_path, self.train_data, self.config_file)
        self.path_config = self.create_path(path)
        self.save_path_config()

        retrain_num = self.get_max_retrain_num(retrain_num=self.current_retrain_number)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=None)
        print("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num))
        dc.init_collector_dir(retrain_num, batch_num)
        self.retrain_phase(modelid, base_name=self.base_name, retrain_num=retrain_num)
        dc.data_porter(path_config=self.path_config, base_name=self.base_name,
                       retrain_num=retrain_num, batch_num=batch_num, mode="Merge")
        return None


    def many_to_many_retrain(self):
        dc = Data_Collector(self.base_path, self.train_data, self.config_file)

        self.get_filter_feature()
        self.get_feature_content(split_flag=True)
        self.create_many_path(training_data_dict=self.filter_feature_dict)

        retrain_num = self.get_max_retrain_num(retrain_num=self.current_retrain_number)
        batch_num = self.get_max_batch_num(retrain_num=retrain_num, batch_num=None)
        dc.init_collector_dir(retrain_num, batch_num)
        print("We're at retrain_num=" + str(retrain_num) + " and batch_num=" + str(batch_num))

        self.many_retrain_phase(retrain_num=retrain_num, batch_num=batch_num)
        self.many_data_porter(retrain_num=retrain_num, batch_num=batch_num, mode="Merge")  

if __name__ == '__main__':
    def timer(n):
        #i = 1
        while True:
            #print(i)            
            server_name = "10.96.18.199"
            db_name = "APC"
            user = "YL"
            password = "YL$212"        
            cnxn1 = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server_name + ';DATABASE=' + db_name + ';UID=' + user + ';PWD=' + password)
            
            sql = r"select * from SVM_PROJECT A, SVM_PROJECT_MODEL B where A.PROJECT_ID = B.PROJECT_ID AND UPPER(A.STATUS) = 'RETRAIN' AND UPPER(B.STATUS) = 'RETRAIN' AND RETRAIN_START_TIME is null"
            SVM_PP_MODEL = pd.read_sql(sql, cnxn1)  
            
            if len(SVM_PP_MODEL) != 0:
                
                SVM = SuperVM(str(SVM_PP_MODEL.PROJECT_NAME[0]), str(SVM_PP_MODEL.UPLOAD_FILE[0]), r"Config.json")
                modelid = SVM_PP_MODEL.MODEL_ID[0]
                
                if (str(SVM_PP_MODEL.MODEL_TYPE[0]) == '1') or (str(SVM_PP_MODEL.MODEL_TYPE[0]) == '2'):
                    
                    SVM.one_to_all_retrain(modelid)                                    
                
                elif str(SVM_PP_MODEL.MODEL_TYPE[0]) == '3':

                    SVM.many_to_many_retrain()           
            #i = i+1                    
            time.sleep(n)
    timer(10)    
    
#-------------------------------------------------------------
#XDI.py
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
            msg = "XYDI(Pre-process): There is no col named" + str(col) + "already."
            mylogs.warning(msg)
            mylogs.warning_trace(e)

    return data, df_idx


def XDI_PreWork_Train(data_path, configs, XDI_PreWork_DataTrans_paths, mylogs):

    data, df_idx = XDI_Pre_Process(data_path, configs["IDX"], mylogs)
    print("Pre-work")
    print(type(data))

    # check if Data Transform was executed in Data Pre-Process
    if "DataTrans" not in configs["Data_Preprocess_Steps"]:
        configs["XDI"]["Data_Transform_Check"] = 0
        scaler_raw_Z_score = preprocessing.StandardScaler().fit(data)
        col_ = data.columns
        data = scaler_raw_Z_score.transform(data)
        data = pd.DataFrame(data=data, columns=col_)
        joblib.dump(scaler_raw_Z_score, XDI_PreWork_DataTrans_paths)
        print("Pre-work")
        print(type(data))
        return data, configs

    elif "DataTrans" in configs["Data_Preprocess_Steps"]:
        configs["XDI"]["Data_Transform_Check"] = 1
        return data, configs


def XDI_PreWork_Test(data_paths, input_paths, index_cols, check_prework_DataTrans, mylogs):

    data, df_idx = XDI_Pre_Process(data_paths, index_cols, mylogs)
    print("Pre-work Test")
    print(type(data))
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
        plt.barh(range(contri_amount, 0, -1), df_contri["Contribution"].iloc[:contri_amount])
        plt.yticks(range(contri_amount, 0, -1), df_contri["Col_Name"].iloc[:contri_amount])
        ax.set_title("Top 20 Contribution")
        plt.legend()
        plt.tight_layout()
        plt.savefig(output_path_)
        plt.clf()
    return None


def XDI_off_line_report(data_folder, mode):
    input_path, output_path, mylog, config = XDI_prepare(data_folder, Mode="Test")

    mylog.info("XDI\tOffline Report Start")
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
        raise KeyError("Key: mode is Error: only Train, Merge, Batch")

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
        raise KeyError("Key: mode is Error: only Train, Merge, Batch")

    msg_XDI = "OK"
    if "NG" in df_XDI["Result"].values:
        XDI_Alarm_report(df_raw_train, df_raw_test, df_XDI, config["Index_Columns"], output_path, mylog)
        msg_XDI = "NG"

    df_XDI["Index_Columns"] = df_XDI[config["Index_Columns"]].apply(lambda x: '_'.join(str(e) for e in x), axis=1)
    if config["Filter_Feature"] is not None:
        df_XDI["Filter_Feature"] = df_XDI[config["Filter_Feature"]]
    else:
        df_XDI["Filter_Feature"] = df_XDI[config["Index_Columns"]].apply(lambda x: '_'.join(str(e) for e in x), axis=1)


    fig, ax = plt.subplots(figsize=(10, 6))
    plt.plot(X_XDI, label='XDI',  c='b')
    if df_XDI["Result"].isin(["NG"]).any():
        idx = df_XDI["Result"].loc[df_XDI["Result"] == "NG"].index
        plt.scatter(idx, df_XDI["XDI"].iloc[idx], c="r", label="NG")
    ax.axhline(y=config["XDI"]["XDI_Threshold"], c='r', label='XDI threshold')
    ax.axvline(x=X_train.shape[0], c='g', label='Train/Test Split Line')
    ax.set_title("XDI")
    plt.legend()
    plt.savefig(output_path["XDI_offline_pic_path"])
    plt.clf()

    # df_XDI["Index_Columns"] = df_XDI[config["Index_Columns"]].apply(lambda x: '_'.join(str(e) for e in x), axis=1)
    # if config["Filter_Feature"] is not None:
    #     df_XDI["Filter_Feature"] = df_XDI[config["Filter_Feature"]]
    df_XDI.to_csv(output_path["XDI_offline_path"], index=False)
    

    mylog.info("XDI\tOffline Report is stored at: " + output_path["XDI_offline_path"])
    mylog.info("XDI\tOffline Report Finished")

    return msg_XDI


def Build_XDI_Model(data_folder):
    input_path, output_path, mylog, config = XDI_prepare(data_folder, Mode="Train")
    mylog.info("XDI\tModel Building Start")

    X_train, config = XDI_PreWork_Train(input_path["x_train_path"],
                                        config,
                                        output_path["XDI_PreWork_DataTrans_path"],
                                        mylog)

    setting_XDI_threshold = config["XDI"]["XDI_threshold_ratio"]
    setting_pca_threshold = config["XDI"]["filter_pca_threshold"]
    mylog.info("XDI\tThreshold Calculate Start")
    config["XDI"]["XDI_Threshold"] = XDI_threshold_calculator(X_train, setting_XDI_threshold, setting_pca_threshold)
    mylog.info("XDI\tThreshold: " + str(config["XDI"]["XDI_Threshold"]))
    mylog.info("XDI\tThreshold Calculate Finished")
    # mylog.info("----------LOO End--------------")

    # PCA Module
    module_PCA = PCA(n_components=setting_pca_threshold).fit(X_train)
    X_train_pca = module_PCA.transform(X_train)
    joblib.dump(module_PCA, output_path["XDI_PCA_path"])

    # PCA again after PCA
    scaler_pca_Z_score = preprocessing.StandardScaler().fit(X_train_pca)
    joblib.dump(scaler_pca_Z_score, output_path["XDI_DataTrans_path"])

    save_config(input_path["config_path"], config, mylog)
    mylog.info("XDI\tBuilding Finished")
        
    return None


def OnLine_XDI(df_online, data_folder):

    input_path, mylog, config = XDI_prepare(data_folder, Mode="OnLine")

    for col in config["IDX"]:
        try:
            df_online = df_online.drop(col, axis=1)
        except Exception as e:
            msg = "XDI(Pre-process): There is no col named" + str(col) + "already."
            mylog.warning(msg)
            mylog.warning_trace(e)


    # print(df_online.columns)
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

#------------------------------------------------------------------------------------------------
#XGB_model.py
# coding: utf-8
from Read_path import read_path
from config import read_config, save_config
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
def xgb_tuning(data_folder):#輸入()，輸出()
                
    input_path = read_path(data_folder) #read Cases\CVD2E_Split1_Test\06_Model\XGB\file_path.json   
    # init log
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    #mylog.init_logger() #設定info存在System.log,error存在error.log
    # read config
    config = read_config(input_path["config_path"], mylog) #read Cases\CVD2E_Split1_Test\00_config\config.json
    
    part=10 #每個範圍切10段
    nround=1000 #最多跑1000顆
    ES=100
    nthread=4 #20190523 新增thread限制，避免吃完，default=4
    #設定調參初始值
    Originalp=[[6, 1], 0, [1, 1], 0, 1, 0.1, 500]
    #依序輸入所有超參數的最小值與最大值。深、葉重、gamma、subsample、colsample_bytree、L1(懲罰係數)、L2(懲罰係數)、eta(學習率)
    #float('inf')為無窮大，float('-inf')為負無窮
    p=[['max_depth','min_child_weight'],'gamma',['subsample','colsample_bytree'],'alpha','lambda','eta','nround']
    minPa=[[2, 1], 0, [0.0000001, 0.0000001], 0, 0, 0.00001]
    maxPa=[[float('inf'), float('inf')], float('inf'), [1, 1], float('inf'), float('inf'), 1.00001]
    #依序輸入所有超參數的首次測試極值
    topall=[[2, 1], 0 ,[0.5, 0.3], 0, 0.9, 0]
    tailall=[[10, 10], 3, [1, 1], 0.1, 1.1, 0.3]     
    
    xgb_parameter_path = os.path.join(data_folder, "xgb_parameter.json") #20190613 save xgb parameter to xgb_parameter.json
    xgb_parameter = {}
    xgb_parameter["part"] = part
    xgb_parameter["nround"] = nround
    xgb_parameter["ES"] = ES
    xgb_parameter["nthread"] = nthread
    xgb_parameter["Originalp"] = Originalp
    xgb_parameter["p"] = p
    xgb_parameter["minPa"] = minPa
    xgb_parameter["maxPa"] = maxPa
    xgb_parameter["topall"] = topall
    xgb_parameter["tailall"] = tailall
    #config["XGB_Parameter"] = xgb_parameter
    #save_config(input_path["config_path"],config, mylog)
    save_config(xgb_parameter_path,xgb_parameter, mylog)
    mylog.info("-----XGB tuning-----")
    
    output_path={}
    output_path["Param_aftertuning_path"] = os.path.join(data_folder, "Parameter_aftertuning.csv")
    Param_aftertuning_path = output_path["Param_aftertuning_path"]
             
    x_Trainpath=input_path["x_train_path"] #訓練資料集路徑
    x_Testpath=input_path["x_test_path"] #訓練資料集路徑
    y_Trainpath=input_path["y_train_path"] #訓練資料集路徑
    #y_Testpath=input_path["y_test_path"] #訓練資料集路徑
    delcol = config["IDX"]  #20190613 Index_Columns->IDX, avoid y_train error[2 column]
    
    try:
        train=pd.read_csv(x_Trainpath)
        test=pd.read_csv(x_Testpath)      
        y_train=pd.read_csv(y_Trainpath).drop(delcol, axis=1)   #20190418 只留train的y      
    except Exception as e:
        mylog.error("Read raw data error")
        mylog.error_trace(e) 
    else:
        X_train=train.drop(delcol, axis=1)                      #20190418 只留train的x
        X_test=test.drop(delcol, axis=1)                        #20190418 只留test的x
        X = pd.concat([X_train, X_test], axis=0, ignore_index=True) #20190329 垂直合併train及test的x 自動產生index
    
    AllParam=pd.DataFrame() #畫好參數調整總表的表格圖
    ETALossv=[]  #紀錄ETA loss curve
    ETALossp=[]  #紀錄ETA之測試值
    ETALosst=[]
    
    ######這邊為設定驗證機制的部分########
    XGTime=0 #設定迴圈初始值
    die=0
    seed=7 #在CV中，ran之defult值為0;在valid中seed=7
       
    while die==0:
    ######以上為設定驗證機制的部分########
        X_train1, X_val, y_train1, y_val = train_test_split(X_train, y_train, test_size=0.3, random_state=seed)
        dtrain = xgb.DMatrix(X_train1, y_train1)
        dval = xgb.DMatrix(X_val, y_val) #xgboost內部使用的資料結構
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
                        area=(tail-top)/10 #area=(3-0)/10
                        L1=np.append(np.arange(top, tail, area),[tail]) #別忘了最右邊的值，((0,3,0.3),3)
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
                                'lambda': Originalp[4],
                                'nthread': nthread
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
                                    'lambda': Originalp[4],
                                    'nthread': nthread
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
                                    'lambda': Originalp[4],
                                    'nthread': nthread
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
                    
        bestP['nthread'] = nthread #20190523 新增thread限制，避免吃完
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
            mylog.error("We trained XGboost 10 times. But still false, please give us more data and check the correction of data.")
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
        
#----------------------------------------------------------------------------------------------------------------
def xgb_merge_tuning(data_folder):#輸入()，輸出()
                
    input_path = read_path(data_folder) #read Cases\CVD2E_Split1_Test\06_Model\XGB\file_path.json   
    # init log
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    #mylog.init_logger() #設定info存在System.log,error存在error.log
    # read config
    config = read_config(input_path["config_path"], mylog) #read Cases\CVD2E_Split1_Test\00_config\config.json
    
    part=10 #每個範圍切10段
    nround=1000 #最多跑1000顆
    ES=100
    nthread=4 #20190523 新增thread限制，避免吃完, default=4
    #設定調參初始值
    Originalp=[[6, 1], 0, [1, 1], 0, 1, 0.1, 500]
    #依序輸入所有超參數的最小值與最大值。深、葉重、gamma、subsample、colsample_bytree、L1(懲罰係數)、L2(懲罰係數)、eta(學習率)
    #float('inf')為無窮大，float('-inf')為負無窮
    p=[['max_depth','min_child_weight'],'gamma',['subsample','colsample_bytree'],'alpha','lambda','eta','nround']
    minPa=[[2, 1], 0, [0.0000001, 0.0000001], 0, 0, 0.00001]
    maxPa=[[float('inf'), float('inf')], float('inf'), [1, 1], float('inf'), float('inf'), 1.00001]
    #依序輸入所有超參數的首次測試極值
    topall=[[2, 1], 0 ,[0.5, 0.3], 0, 0.9, 0]
    tailall=[[10, 10], 3, [1, 1], 0.1, 1.1, 0.3]     
    
    xgb_parameter_path = os.path.join(data_folder, "xgb_parameter.json")
    xgb_parameter = {}
    xgb_parameter["part"] = part
    xgb_parameter["nround"] = nround
    xgb_parameter["ES"] = ES
    xgb_parameter["nthread"] = nthread
    xgb_parameter["Originalp"] = Originalp
    xgb_parameter["p"] = p
    xgb_parameter["minPa"] = minPa
    xgb_parameter["maxPa"] = maxPa
    xgb_parameter["topall"] = topall
    xgb_parameter["tailall"] = tailall
    #config["XGB_Parameter"] = xgb_parameter
    #save_config(input_path["config_path"],config, mylog)
    save_config(xgb_parameter_path,xgb_parameter, mylog)
    
    mylog.info("-----XGB merge tuning-----")
    
    output_path={}
    output_path["Param_aftertuning_path"] = os.path.join(data_folder, "Parameter_aftertuning.csv")
    Param_aftertuning_path = output_path["Param_aftertuning_path"]
             
    x_Trainpath=input_path["x_train_path"] #訓練資料集路徑
    y_Trainpath=input_path["y_train_path"] #訓練資料集路徑
    delcol = config["IDX"]   
    
    try:
        train=pd.read_csv(x_Trainpath)    
        y_train=pd.read_csv(y_Trainpath).drop(delcol, axis=1)   #20190418 只留train的y       
    except Exception as e:
        mylog.error("Read raw data error")
        mylog.error_trace(e) 
    else:
        X_train=train.drop(delcol, axis=1)                      #20190418 只留train的x
    
    AllParam=pd.DataFrame() #畫好參數調整總表的表格圖
    ETALossv=[]  #紀錄ETA loss curve
    ETALossp=[]  #紀錄ETA之測試值
    ETALosst=[]
    
    ######這邊為設定驗證機制的部分########
    XGTime=0 #設定迴圈初始值
    die=0
    seed=7 #在CV中，ran之defult值為0;在valid中seed=7
       
    while die==0:
    ######以上為設定驗證機制的部分########
        X_train1, X_val, y_train1, y_val = train_test_split(X_train, y_train, test_size=0.3, random_state=seed)
        dtrain = xgb.DMatrix(X_train1, y_train1)
        dval = xgb.DMatrix(X_val, y_val) #xgboost內部使用的資料結構
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
                        area=(tail-top)/10 #area=(3-0)/10
                        L1=np.append(np.arange(top, tail, area),[tail]) #別忘了最右邊的值，((0,3,0.3),3)
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
                                'lambda': Originalp[4],
                                'nthread': nthread
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
                                    'lambda': Originalp[4],
                                    'nthread': nthread
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
                                    'lambda': Originalp[4],
                                    'nthread': nthread
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
                    
        bestP['nthread'] = nthread #20190523 新增thread限制，避免吃完
        dtrain = xgb.DMatrix(X_train, y_train) 
        watchlist = [(dtrain,'train')]
        evals_result = {}
        num_rounds = int(bestP['nround'])
    #     print('begin')
        try:
            model = xgb.train(bestP, dtrain, num_rounds, watchlist, evals_result=evals_result, verbose_eval=False)                        
        except Exception as e:
            mylog.error("train error before predict merge_X_train")
            mylog.error_trace(e)
        else:
            pred = model.predict(xgb.DMatrix(X_train))
            arr=stats.mode(pred)                  
         
        if arr[1][0] < (len(X_train)/2):  #arr[1][0]為眾數出現次數
            die=die+1
        elif XGTime == 10:
            die=die+1
            mylog.error("We trained XGboost 10 times. But still false, please give us more data and check the correction of data.")
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
        
#----------------------------------------------------------------------------------------------------------------

def xgb_build(data_folder):#輸入()，輸出()
    
    output_path={}
    output_path["ModelSave_path"] = os.path.join(data_folder, "XGB.model")   
    output_path["FeatureScore_path"] = os.path.join(data_folder, "FeatureScore.csv")    #20190612 output FeatureScore
    ModelSave_path = output_path["ModelSave_path"]
        
    input_path = read_path(data_folder) #read Cases\CVD2E_Split1_Test\06_Model\XGB\file_path.json   
    # init log
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    #mylog.init_logger() #設定info存在System.log,error存在error.log    
    mylog.info("-----XGB building-----")
    # read config
    config = read_config(input_path["config_path"], mylog) #read Cases\CVD2E_Split1_Test\00_config\config.json
    xgb_parameter = read_config(input_path["xgb_parameter_path"], mylog) #20190614 read xgb_parameter.json
    
    x_Trainpath=input_path["x_train_path"] #訓練資料集路徑
    #x_Testpath=input_path["x_test_path"] #訓練資料集路徑
    y_Trainpath=input_path["y_train_path"] #訓練資料集路徑
    #y_Testpath=input_path["y_test_path"] #訓練資料集路徑
    Param_aftertuning_path = input_path["xgb_tuning"] #20190523 改抓input_path
    FeatureScore_path = output_path["FeatureScore_path"]
    delcol = config["IDX"]  
    nthread = xgb_parameter["nthread"] 
    
    try:
        train=pd.read_csv(x_Trainpath)
        #test=pd.read_csv(x_Testpath)      
        y_train=pd.read_csv(y_Trainpath).drop(delcol, axis=1)   #20190418 只留train的y    
        #y_test=pd.read_csv(y_Testpath).drop(delcol, axis=1)     #20190418 只留test的y      
    except Exception as e:
        mylog.error("Read raw data error")
        mylog.error_trace(e) 
    else:
        X_train=train.drop(delcol, axis=1)                      #20190418 只留train的x
        #X_test=test.drop(delcol, axis=1)                        #20190418 只留test的x
        #X = pd.concat([X_train, X_test], axis=0, ignore_index=True) #20190329 垂直合併train及test的x 自動產生index
    
    Param_read = pd.read_csv(Param_aftertuning_path)    
    Param = pd.DataFrame(Param_read)
    """
    print(len(Param['Best Value'][2]))
    print(Param['Best Value'][2])
    a = Param['Best Value'][2].replace("[", "")
    b = a.replace("]", "")
    c = b.split(', ', 1 )
    print(c)
    print(type(c))
    print(len(c))
    """
    # # 把剛剛存起來的Param叫出來訓練    
    bestP={}
    for i in range(len(Param)):
        if i in [1,3,4,5,6]:
            bestP[Param['Params'][i]]=Param['Best Value'][i]
        else:
            #20190528 transfer string to list from csv
            a = Param['Best Value'][i].replace("[", "")
            b = a.replace("]", "")
            c = b.split(', ', 1 )
            for j in range(len(c)):
                bestP[Param['Params'][i][j]]=c[j]
    
    bestP['nthread'] = nthread
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
        importance = model.get_fscore()
        importance = sorted(importance.items(), key=operator.itemgetter(1))  
        df = pd.DataFrame(importance, columns=['feature', 'fscore'])
        df.to_csv(FeatureScore_path, index=False) 
        model.save_model(ModelSave_path)    #20190426 save model for reuse          

def xgb_predict(data_folder):##: 輸入()，輸出(啟動主程式，目前包成副函式，以後可在其他物件中，呼叫此副函式，減少物件的浪費)    
    #20190422 從hotcode->讀取config==================================================
    output_path={}    
    #output_path["FeatureScore_path"] = os.path.join(data_folder, "FeatureScore.csv") 
    output_path["Importance10_path"] = os.path.join(data_folder, "Importance10.jpg") 
    output_path["Importance30_path"] = os.path.join(data_folder, "Importance30.jpg") 
    output_path["testPredResult_path"] = os.path.join(data_folder, "testPredResult.csv")
    output_path["trainPredResult_path"] = os.path.join(data_folder, "trainPredResult.csv") 
    #output_path["ModelSave_path"] = os.path.join(data_folder, "XGB.model")       

    #FeatureScore_path = output_path["FeatureScore_path"]
    Importance10_path = output_path["Importance10_path"]
    Importance30_path = output_path["Importance30_path"]
    testPredResult_path = output_path["testPredResult_path"]
    trainPredResult_path = output_path["trainPredResult_path"]    

    # init log
    input_path = read_path(data_folder) #read Cases\CVD2E_Split1_Test\06_Model\XGB\file_path.json
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    #mylog.init_logger() #設定info存在System.log,error存在error.log       
    config = read_config(input_path["config_path"], mylog) #read Cases\CVD2E_Split1_Test\00_config\config.json
        
    # read config        
    x_Trainpath=input_path["x_train_path"] #訓練資料集路徑
    x_Testpath=input_path["x_test_path"] #訓練資料集路徑
    y_Trainpath=input_path["y_train_path"] #訓練資料集路徑
    y_Testpath=input_path["y_test_path"] #訓練資料集路徑
    ModelSave_path = input_path["xgb_model"] #20190523 改抓input_path
    delcol = config["IDX"] 
    y_config = config["Y"]
    y_pred = y_config[0] + "_pred_XGB"
    #=================================================================================   
    
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
        #X = pd.concat([X_train, X_test], axis=0, ignore_index=True) #20190329 垂直合併train及test的x 自動產生index
        
    mylog.info("-----XGB predict start-----")
        
    try:
        model1=xgb.Booster(model_file=ModelSave_path)
        dtest = xgb.DMatrix(X_test)              
    except Exception as e:
        mylog.error("train error before predict test")
        mylog.error_trace(e)
    else:
        pre = model1.predict(dtest)                 
    """
    try:
        importance = model1.get_fscore()
        importance = sorted(importance.items(), key=operator.itemgetter(1))  
        df = pd.DataFrame(importance, columns=['feature', 'fscore'])
    except Exception as e:
        mylog.error("FeatureScore to csv error")
        mylog.error_trace(e)        
    else:       
        df.to_csv(FeatureScore_path, index=False)            
    """   
    try:
        plot_importance(model1,max_num_features=30)       
    except Exception as e:
        mylog.error("Importance30 save error")
        mylog.error_trace(e)        
    else:
        plt.savefig(Importance30_path, bbox_inches='tight') #20190507 因應ylabel cut off          

    try:
        plot_importance(model1,max_num_features=10)       
    except Exception as e:
        mylog.error("Importance10 save error")
        mylog.error_trace(e)      
    else:
        plt.savefig(Importance10_path, bbox_inches='tight')        

    try:
        sheet_test = test[delcol] 
        sheet_train = train[delcol]
        ans = pd.DataFrame(pre,columns=[y_pred])  #20190430 Y_pred->config[Y]
        output = pd.concat([y_test,ans],axis=1)
        output = pd.concat([sheet_test,output],axis=1)
        output['differ']=np.abs(output[y_config[0]]-output[y_pred])
    except Exception as e:
        mylog.error("testPredResult to csv error")
        mylog.error_trace(e)
    else:       
        output.to_csv(testPredResult_path, index=False)        
    
    try:
        dtrain = xgb.DMatrix(X_train)
        pre_train = model1.predict(dtrain)
        ans_train = pd.DataFrame(pre_train,columns=[y_pred]) 
        output_train = pd.concat([y_train,ans_train],axis=1)
        output_train = pd.concat([sheet_train,output_train],axis=1)
        output_train['differ']=np.abs(output_train[y_config[0]]-output_train[y_pred])
    except Exception as e:
        mylog.error("trainPredResult to csv error")
        mylog.error_trace(e)
    else:          
        output_train.to_csv(trainPredResult_path, index=False)  
     
    mylog.info("-----XGB predict Done-----") 
    
#------------------------------------------------------------------------------------------------------------------

def xgb_merge_predict(data_folder):##: 輸入()，輸出(啟動主程式，目前包成副函式，以後可在其他物件中，呼叫此副函式，減少物件的浪費)    
    #20190422 從hotcode->讀取config==================================================
    output_path={}    
    #output_path["FeatureScore_path"] = os.path.join(data_folder, "FeatureScore.csv") 
    output_path["Importance10_path"] = os.path.join(data_folder, "Importance10.jpg") 
    output_path["Importance30_path"] = os.path.join(data_folder, "Importance30.jpg") 
    output_path["trainPredResult_path"] = os.path.join(data_folder, "trainPredResult.csv")     

    #FeatureScore_path = output_path["FeatureScore_path"]
    Importance10_path = output_path["Importance10_path"]
    Importance30_path = output_path["Importance30_path"]
    trainPredResult_path = output_path["trainPredResult_path"]    

    # init log
    input_path = read_path(data_folder) #read Cases\CVD2E_Split1_Test\06_Model\XGB\file_path.json
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    #mylog.init_logger() #設定info存在System.log,error存在error.log       
    config = read_config(input_path["config_path"], mylog) #read Cases\CVD2E_Split1_Test\00_config\config.json
        
    # read config        
    x_Trainpath=input_path["x_train_path"] #訓練資料集路徑
    y_Trainpath=input_path["y_train_path"] #訓練資料集路徑
    ModelSave_path = input_path["xgb_model"] #20190523 改抓input_path
    delcol = config["IDX"] 
    y_config = config["Y"]
    y_pred = y_config[0] + "_pred_XGB"
    #=================================================================================   
        
    try:
        train=pd.read_csv(x_Trainpath)  
        y_train=pd.read_csv(y_Trainpath).drop(delcol, axis=1)   #20190418 只留train的y         
    except Exception as e:
        mylog.error("Read raw data error")
        mylog.error_trace(e) 
    else:
        X_train=train.drop(delcol, axis=1)                      #20190418 只留train的x
        
    mylog.info("-----XGB merge predict start-----")
        
    try:
        model1=xgb.Booster(model_file=ModelSave_path)
        dtrain = xgb.DMatrix(X_train)              
    except Exception as e:
        mylog.error("train error before predict test")
        mylog.error_trace(e)
    else:
        pre = model1.predict(dtrain)                 
    """
    try:
        importance = model1.get_fscore()
        importance = sorted(importance.items(), key=operator.itemgetter(1))  
        df = pd.DataFrame(importance, columns=['feature', 'fscore'])
    except Exception as e:
        mylog.error("FeatureScore to csv error")
        mylog.error_trace(e)        
    else:       
        df.to_csv(FeatureScore_path, index=False)            
    """  
    try:
        plot_importance(model1,max_num_features=30)       
    except Exception as e:
        mylog.error("Importance30 save error")
        mylog.error_trace(e)        
    else:
        plt.savefig(Importance30_path, bbox_inches='tight') #20190507 因應ylabel cut off          

    try:
        plot_importance(model1,max_num_features=10)       
    except Exception as e:
        mylog.error("Importance10 save error")
        mylog.error_trace(e)      
    else:
        plt.savefig(Importance10_path, bbox_inches='tight')        

    try:
        sheet_train = train[delcol]
        ans = pd.DataFrame(pre,columns=[y_pred])
        output = pd.concat([y_train,ans],axis=1)
        output = pd.concat([sheet_train,output],axis=1)
        output['differ']=np.abs(output[y_config[0]]-output[y_pred])
    except Exception as e:
        mylog.error("trainPredResult to csv error")
        mylog.error_trace(e)
    else:       
        output.to_csv(trainPredResult_path, index=False)            
       
    mylog.info("-----XGB merge predict Done-----")   
#------------------------------------------------------------------------------------------------------------------
    
def xgb_batch_predict(data_folder):##: 輸入()，輸出(啟動主程式，目前包成副函式，以後可在其他物件中，呼叫此副函式，減少物件的浪費)    
    #20190422 從hotcode->讀取config==================================================
    output_path={}    
    #output_path["FeatureScore_path"] = os.path.join(data_folder, "FeatureScore.csv") 
    output_path["Importance10_path"] = os.path.join(data_folder, "Importance10.jpg") 
    output_path["Importance30_path"] = os.path.join(data_folder, "Importance30.jpg") 
    output_path["testPredResult_path"] = os.path.join(data_folder, "testPredResult.csv")   

    #FeatureScore_path = output_path["FeatureScore_path"]
    Importance10_path = output_path["Importance10_path"]
    Importance30_path = output_path["Importance30_path"]
    testPredResult_path = output_path["testPredResult_path"]  

    # init log
    input_path = read_path(data_folder) #read Cases\CVD2E_Split1_Test\06_Model\XGB\file_path.json
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])
    #mylog.init_logger() #設定info存在System.log,error存在error.log       
    config = read_config(input_path["config_path"], mylog) #read Cases\CVD2E_Split1_Test\00_config\config.json
        
    # read config        
    x_Testpath=input_path["x_test_path"] #訓練資料集路徑
    y_Testpath=input_path["y_test_path"] #訓練資料集路徑
    ModelSave_path = input_path["xgb_model"] #20190523 改抓input_path
    delcol = config["IDX"] 
    y_config = config["Y"]
    y_pred = y_config[0] + "_pred_XGB"
    #=================================================================================   
    if os.path.isfile(y_Testpath):
        try:
            test=pd.read_csv(x_Testpath)        
            y_test=pd.read_csv(y_Testpath).drop(delcol, axis=1)     #20190418 只留test的y      
        except Exception as e:
            mylog.error("Read raw data error")
            mylog.error_trace(e) 
        else:
            X_test=test.drop(delcol, axis=1)                        #20190418 只留test的x
            
        mylog.info("-----XGB batch predict start-----")
            
        try:
            model1=xgb.Booster(model_file=ModelSave_path)
            dtest = xgb.DMatrix(X_test)              
        except Exception as e:
            mylog.error("train error before predict test")
            mylog.error_trace(e)
        else:
            pre = model1.predict(dtest)                 

        try:
            plot_importance(model1,max_num_features=30)       
        except Exception as e:
            mylog.error("Importance30 save error")
            mylog.error_trace(e)        
        else:
            plt.savefig(Importance30_path, bbox_inches='tight') #20190507 因應ylabel cut off          
    
        try:
            plot_importance(model1,max_num_features=10)       
        except Exception as e:
            mylog.error("Importance10 save error")
            mylog.error_trace(e)      
        else:
            plt.savefig(Importance10_path, bbox_inches='tight')            
    
        try:
            sheet_test = test[delcol] 
            ans = pd.DataFrame(pre,columns=[y_pred])
            output = pd.concat([y_test,ans],axis=1)
            output = pd.concat([sheet_test,output],axis=1)
            output['differ']=np.abs(output[y_config[0]]-output[y_pred])
        except Exception as e:
            mylog.error("testPredResult to csv error")
            mylog.error_trace(e)
        else:       
            output.to_csv(testPredResult_path, index=False)  
            
    else:
        try:
            test=pd.read_csv(x_Testpath)           
        except Exception as e:
            mylog.error("Read raw data error")
            mylog.error_trace(e) 
        else:
            X_test=test.drop(delcol, axis=1)                        #20190418 只留test的x
            
        mylog.info("-----XGB batch predict start-----")
            
        try:
            model1=xgb.Booster(model_file=ModelSave_path)
            dtest = xgb.DMatrix(X_test)              
        except Exception as e:
            mylog.error("train error before predict test")
            mylog.error_trace(e)
        else:
            pre = model1.predict(dtest)                 

        try:
            plot_importance(model1,max_num_features=30)       
        except Exception as e:
            mylog.error("Importance30 save error")
            mylog.error_trace(e)        
        else:
            plt.savefig(Importance30_path, bbox_inches='tight') #20190507 因應ylabel cut off          
    
        try:
            plot_importance(model1,max_num_features=10)       
        except Exception as e:
            mylog.error("Importance10 save error")
            mylog.error_trace(e)      
        else:
            plt.savefig(Importance10_path, bbox_inches='tight')            
    
        try:
            sheet_test = test[delcol] 
            ans = pd.DataFrame(pre,columns=[y_pred])
            output = pd.concat([sheet_test,ans],axis=1)
        except Exception as e:
            mylog.error("testPredResult to csv error")
            mylog.error_trace(e)
        else:       
            output.to_csv(testPredResult_path, index=False) 
        
    mylog.info("-----XGB batch predict Done-----")  

def xgb_online_predict(data_folder, df_x_test, df_y_test=None):##: 輸入()，輸出(啟動主程式，目前包成副函式，以後可在其他物件中，呼叫此副函式，減少物件的浪費)    
    # init log
    input_path = read_path(data_folder) #read Cases\CVD2E_Split1_Test\06_Model\XGB\file_path.json
    mylog = WriteLog(input_path["log_path"], input_path["error_path"])    
    config = read_config(input_path["config_path"], mylog) #read Cases\CVD2E_Split1_Test\00_config\config.json
        
    # read config        
    ModelSave_path = input_path["xgb_model"] #20190523 改抓input_path
    #delcol = db_data['IDX']    
    delcol = config["IDX"]
    y_config = config["Y"]
    y_pred = y_config[0] + "_pred_XGB"
    """
    if delcol.find(',') != -1:
        delcol_list = delcol.split(",")
    """

    if df_y_test is not None:
        #X_test=x_test_df.drop(delcol_list, axis=1)                       
        #y_test=y_test_df.drop(delcol_list, axis=1)  
        X_test=df_x_test.drop(delcol, axis=1)                       
        y_test=df_y_test.drop(delcol, axis=1) 
            
        mylog.info("-----XGB online predict Y start-----")
            
        try:
            model1=xgb.Booster(model_file=ModelSave_path)
            dtest = xgb.DMatrix(X_test)              
        except Exception as e:
            mylog.error("train error before predict test")
            mylog.error_trace(e)
        else:
            pre = model1.predict(dtest)                     
    
        try:
            ans = pd.DataFrame(pre,columns=[y_pred])  #20190430 pred->Y_pred
        except Exception as e:
            mylog.error("testPredResult to csv error")
            mylog.error_trace(e)
        else:       
            return ans[y_pred][0], y_test[y_config[0]][0]
            
    else:
        #X_test=x_test_df.drop(delcol_list, axis=1)  
        X_test=df_x_test.drop(delcol, axis=1)                     
            
        mylog.info("-----XGB online predict X start-----")
            
        try:
            model1=xgb.Booster(model_file=ModelSave_path)
            dtest = xgb.DMatrix(X_test)              
        except Exception as e:
            mylog.error("train error before predict test")
            mylog.error_trace(e)
        else:
            pre = model1.predict(dtest)                    
    
        try:
            ans = pd.DataFrame(pre,columns=[y_pred])  #20190430 pred->Y_pred
        except Exception as e:
            mylog.error("testPredResult to csv error")
            mylog.error_trace(e)
        else:       
            return ans[y_pred][0]   #20190621 because return series, must assign location
    mylog.info("-----XGB online predict Done-----")  
     
#呼叫主函式    
#data_folder = "D:/AVM/Cases/CVD2E_Split1_T75R4_batch_test/CVD2E_Split1_T75R4_batch_test_00/00/06_Model/XGB" 
#xgb_tuning(data_folder)
#xgb_build(data_folder)
#xgb_predict(data_folder)

#-------------------------------------------------------------------------------------------------------------
#YDI.py
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



# def YDI_off_line_report(data_folder):
#
#     input_path, output_path, mylog, config = YDI_prepare(data_folder, Mode="Test")
#     mylog.info("-----YDI Offline-----")
#     # print(config["YDI"])
#     X_train = YDI_PreWork_Test(input_path["x_train_path"],
#                                config["IDX"],
#                                config["YDI"]["Data_Transform_Check"],
#                                input_path["YDI_PreWork_DataTrans"],
#                                mylog)
#
#     X_test = YDI_PreWork_Test(input_path["x_test_path"],
#                               config["IDX"],
#                               config["YDI"]["Data_Transform_Check"],
#                               input_path["YDI_PreWork_DataTrans"],
#                               mylog)
#
#     df_train_idx = X_train[config["IDX"]]
#     df_test_idx = X_test[config["IDX"]]
#     y_train = read_data(input_path["y_train_path"], mylog)
#     y_train = y_train[config["Y"]]
#     y_test = read_data(input_path["y_test_path"], mylog)
#     y_test = y_test[config["Y"]]
#
#
#     if config["filter_feature"] is not None:
#         group_col = config["filter_feature"]
#         # group_col = config["filter_feature"]
#         # print(group_col)
#         YDI_Off_line_table = pd.DataFrame()
#         for groups in np.unique(X_train[group_col]):
#             group_idx_train = X_train.loc[X_train[group_col] == groups].index
#             group_idx_test = X_test.loc[X_test[group_col] == groups].index
#
#             X_train_data = X_train.iloc[group_idx_train].reset_index(drop=True)
#             X_train_data = X_train_data.drop(config["IDX"], axis=1)
#             y_train_data = y_train.iloc[group_idx_train].reset_index(drop=True)
#
#             X_test_data = X_test.iloc[group_idx_test].reset_index(drop=True)
#             X_test_data = X_test_data.drop(config["IDX"], axis=1)
#             y_test_data = y_test.iloc[group_idx_test].reset_index(drop=True)
#
#             train_idx_data = df_train_idx.iloc[group_idx_train].reset_index(drop=True)
#             test_idx_data = df_test_idx.iloc[group_idx_test].reset_index(drop=True)
#
#
#             # print(groups)
#             # print(X_train_data.shape, y_train_data.shape, X_test_data.shape, y_test_data.shape)
#
#             YDI_Off_line_group_table = YDI_report_generator(X_train_data,
#                                                             X_test_data,
#                                                             y_train_data,
#                                                             y_test_data,
#                                                             train_idx_data,
#                                                             test_idx_data,
#                                                             config["Y"],
#                                                             group_col,
#                                                             groups,
#                                                             input_path["YDI_Group_path"],
#                                                             output_path["YDI_Group_path"])
#
#
#             YDI_Off_line_group_table["filter_feature"] = groups
#
#             YDI_Off_line_table = pd.concat([YDI_Off_line_table, YDI_Off_line_group_table], axis=0)
#             # if config["filter_feature"] in config["Index_Columns"]:
#             #     YDI_Off_line_table = pd.concat([YDI_Off_line_table, YDI_Off_line_group_table], axis=0)
#             #
#             #
#             # if config["filter_feature"] not in config["Index_Columns"]:
#             #     YDI_Off_line_group_table[config["filter_feature"]] = groups
#             # YDI_Off_line_table = pd.concat([YDI_Off_line_table, YDI_Off_line_group_table], axis=0)
#
#
#     elif config["filter_feature"] is None:
#         X_train = X_train.drop(config["Index_Columns"], axis=1)
#         X_test = X_test.drop(config["Index_Columns"], axis=1)
#         YDI_Off_line_table = YDI_report_generator(X_train,
#                                                   X_test,
#                                                   y_train,
#                                                   y_test,
#                                                   df_train_idx,
#                                                   df_test_idx,
#                                                   config["Y"],
#                                                   None,
#                                                   None,
#                                                   input_path["YDI_Group_path"],
#                                                   output_path["YDI_Group_path"])
#
#     YDI_Off_line_table["Index_Columns"] = \
#         YDI_Off_line_table[config["Index_Columns"]].apply(lambda x: '_'.join(str(e) for e in x), axis=1)
#     YDI_Off_line_table.to_csv(output_path["YDI_Offline_report_path"],index=False)
#
#     mylog.info("-----YDI Offline Done-----")
#     return None


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

    mylog.info("YDI\tOffline Report Start")
    # print(config["YDI"])
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


    if config["Filter_Feature"] is not None:
        group_col = config["Filter_Feature"]
        # group_col = config["filter_feature"]
        # print(group_col)
        YDI_Off_line_table = pd.DataFrame()
        for groups in np.unique(X_train[group_col]):
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


    elif config["Filter_Feature"] is None:
        X_train = X_train.drop(config["Index_Columns"], axis=1)
        X_test = X_test.drop(config["Index_Columns"], axis=1)
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

    YDI_Off_line_table["Index_Columns"] = \
        YDI_Off_line_table[config["Index_Columns"]].apply(lambda x: '_'.join(str(e) for e in x), axis=1)
    YDI_Off_line_table.to_csv(output_path["YDI_Offline_report_path"],index=False)

    msg_YDI = "OK"
    if "NG" in YDI_Off_line_table["Result"].values:
        msg_YDI = "NG"

    mylog.info("YDI\tOffline Report Finished")
    return msg_YDI


def Build_YDI_Model(data_folder):

    input_path, output_path, mylog, config = YDI_prepare(data_folder, Mode="Train")

    mylog.info("YDI\tModel Building Start")
    X_train, config = YDI_PreWork_Train(input_path["x_train_path"],
                                        config,
                                        output_path["YDI_PreWork_DataTrans"],
                                        mylog)

    y_train = read_data(input_path["y_train_path"], mylog)
    y_train = y_train[config["Y"]]

    if config["Filter_Feature"] is not None:
        group_col = config["Filter_Feature"]
        YDI_threshold_table = pd.DataFrame()
        # print(group_col)
        for groups in np.unique(X_train[group_col]):
            # print(X_train)
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
            # break

    else:
        X_train = X_train.drop(config["IDX"], axis=1)
        YDI_threshold_table = YDI_threshold_generator(X_train,
                                                      y_train,
                                                      config["Y"],
                                                      config["YDI"]["filter_pca_threshold"],
                                                      None,
                                                      output_path["YDI_Group_path"])

    YDI_threshold_table.to_csv(output_path["YDI_threshold_table_path"], index=False)
    mylog.info("YDI\tModel Building Finished")
    save_config(input_path["config_path"], config, mylog)
    return None


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
