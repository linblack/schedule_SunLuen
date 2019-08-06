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

