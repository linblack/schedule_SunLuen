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
