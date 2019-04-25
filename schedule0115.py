# XGB_0416
# coding: utf-8
# In[1]:
import xgboost as xgb
from xgboost import plot_importance
import operator
from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
import os #新增前處理資料夾分類
from sklearn.model_selection import train_test_split
from scipy import stats ######找眾數######
import sys
import datetime
# # 我們來調參吧
def parameter_setting():#輸入()，輸出()
    global Param
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
                            except:
                                temp_tuple = ()
                                temp_tuple+=(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),)
                                temp_tuple+=(sys.exc_info()[0],)
                                temp_tuple+=(sys.exc_info()[1],)
                                error_log.append(temp_tuple) 
                                df_error_log=pd.DataFrame(columns=error_name,data=error_log)
                                df_error_log.to_csv('../../Output/{}/error_log.csv'.format(vset), index=False)
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
                                except:
                                    temp_tuple = ()
                                    temp_tuple+=(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),)
                                    temp_tuple+=(sys.exc_info()[0],)
                                    temp_tuple+=(sys.exc_info()[1],)
                                    error_log.append(temp_tuple)
                                    df_error_log=pd.DataFrame(columns=error_name,data=error_log)
                                    df_error_log.to_csv('../../Output/{}/error_log.csv'.format(vset), index=False)
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
                                except:
                                    temp_tuple = ()
                                    temp_tuple+=(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),)
                                    temp_tuple+=(sys.exc_info()[0],)
                                    temp_tuple+=(sys.exc_info()[1],)
                                    error_log.append(temp_tuple)
                                    df_error_log=pd.DataFrame(columns=error_name,data=error_log)
                                    df_error_log.to_csv('../../Output/{}/error_log.csv'.format(vset), index=False)
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
        except:
            temp_tuple = ()
            temp_tuple+=(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),)
            temp_tuple+=(sys.exc_info()[0],)
            temp_tuple+=(sys.exc_info()[1],)
            error_log.append(temp_tuple)
            df_error_log=pd.DataFrame(columns=error_name,data=error_log)
            df_error_log.to_csv('../../Output/{}/error_log.csv'.format(vset), index=False)
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
        Param.to_csv('../../Output/{}/Param_aftertuning.csv'.format(vset), index=False) #把變數檔存起來        
    except:
        temp_tuple = ()
        temp_tuple+=(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),)
        temp_tuple+=(sys.exc_info()[0],)
        temp_tuple+=(sys.exc_info()[1],)
        error_log.append(temp_tuple)
        df_error_log=pd.DataFrame(columns=error_name,data=error_log)
        df_error_log.to_csv('../../Output/{}/error_log.csv'.format(vset), index=False)
        

def xgb_execution():##: 輸入()，輸出(啟動主程式，目前包成副函式，以後可在其他物件中，呼叫此副函式，減少物件的浪費)
    # 調整之前要先改位置的變數
    global vset
    global X_train
    global y_train
    global X
    global error_log
    global error_name
    path='sp10402'
    vset='ACD6K_aftertuning{}'.format(path)
    p='../../Output/{}'.format(vset)
    if not os.path.isdir(p):
        os.mkdir(p)
    
    #需要輸入的資料  train:174/test:20
    x_Trainpath='../../DataSet/OriginalData/x_Train{}.csv'.format(path) #訓練資料集路徑
    x_Testpath='../../DataSet/OriginalData/x_Test{}.csv'.format(path) #訓練資料集路徑
    y_Trainpath='../../DataSet/OriginalData/y_Train{}.csv'.format(path) #訓練資料集路徑
    y_Testpath='../../DataSet/OriginalData/y_Test{}.csv'.format(path) #訓練資料集路徑
    delcol=['SHEET_ID'] #刪除之欄名list
    
    error_log = []
    temp_tuple=()
    error_name=['error_time','error_type','error_message']
    
    try:
        train=pd.read_csv(x_Trainpath)
        test=pd.read_csv(x_Testpath)      
        y_train=pd.read_csv(y_Trainpath).drop(delcol, axis=1)   #20190418 只留train的y    
        y_test=pd.read_csv(y_Testpath).drop(delcol, axis=1)     #20190418 只留test的y                   
    except:
        temp_tuple+=(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),)
        temp_tuple+=(sys.exc_info()[0],)
        temp_tuple+=(sys.exc_info()[1],)
        error_log.append(temp_tuple)  
        df_error_log=pd.DataFrame(columns=error_name,data=error_log)
        df_error_log.to_csv('../../Output/{}/error_log.csv'.format(vset), index=False)
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
    # # 把剛剛存起來的Param叫出來訓練
    bestP={}
    for i in range(len(Param)):
        if i in [1,3,4,5,6]:
            bestP[Param['Params'][i]]=Param['Best Value'][i]
        else:
            for j in range(len(Param['Best Value'][i])):
                bestP[Param['Params'][i][j]]=Param['Best Value'][i][j]
    
    
    # In[12]:
    dtrain = xgb.DMatrix(X_train, y_train)
    watchlist = [(dtrain,'train')]
    evals_result = {}
    num_rounds = int(bestP['nround'])
    try:
        model = xgb.train(bestP, dtrain, num_rounds, watchlist, evals_result=evals_result, verbose_eval=False)
        dtest = xgb.DMatrix(X_test)              
    except:
        temp_tuple = ()
        temp_tuple+=(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),)
        temp_tuple+=(sys.exc_info()[0],)
        temp_tuple+=(sys.exc_info()[1],)
        error_log.append(temp_tuple)
        df_error_log=pd.DataFrame(columns=error_name,data=error_log)
        df_error_log.to_csv('../../Output/{}/error_log.csv'.format(vset), index=False)
    else:
        pre = model.predict(dtest)                 
    # In[13]:  
    try:
        importance = model.get_fscore()
        importance = sorted(importance.items(), key=operator.itemgetter(1))  
        df = pd.DataFrame(importance, columns=['feature', 'fscore'])
    except:
        temp_tuple = ()
        temp_tuple+=(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),)
        temp_tuple+=(sys.exc_info()[0],)
        temp_tuple+=(sys.exc_info()[1],)
        error_log.append(temp_tuple)
        df_error_log=pd.DataFrame(columns=error_name,data=error_log)
        df_error_log.to_csv('../../Output/{}/error_log.csv'.format(vset), index=False)        
    else:       
        df.to_csv('../../Output/{}/FeatureScore.csv'.format(vset), index=False)            
    
    try:
        plot_importance(model,max_num_features=30)       
    except:
        temp_tuple = ()
        temp_tuple+=(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),)
        temp_tuple+=(sys.exc_info()[0],)
        temp_tuple+=(sys.exc_info()[1],)
        error_log.append(temp_tuple)
        df_error_log=pd.DataFrame(columns=error_name,data=error_log)
        df_error_log.to_csv('../../Output/{}/error_log.csv'.format(vset), index=False)         
    else:
        plt.savefig('../../Output/{}/Importance30.jpg'.format(vset))               
        # In[14]:
    try:
        plot_importance(model,max_num_features=10)       
    except:
        temp_tuple = ()
        temp_tuple+=(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),)
        temp_tuple+=(sys.exc_info()[0],)
        temp_tuple+=(sys.exc_info()[1],)
        error_log.append(temp_tuple)
        df_error_log=pd.DataFrame(columns=error_name,data=error_log)
        df_error_log.to_csv('../../Output/{}/error_log.csv'.format(vset), index=False)         
    else:
        plt.savefig('../../Output/{}/Importance10.jpg'.format(vset))        
        # In[15]:
    try:
        sheet_test = test[delcol] 
        sheet_train = train[delcol]
        ans = pd.DataFrame(pre,columns=['pred'])
        output = pd.concat([y_test,ans],axis=1)
        output = pd.concat([sheet_test,output],axis=1)
        output['differ']=np.abs(output['Value']-output['pred'])
    except:
        temp_tuple = ()
        temp_tuple+=(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),)
        temp_tuple+=(sys.exc_info()[0],)
        temp_tuple+=(sys.exc_info()[1],)
        error_log.append(temp_tuple)
        df_error_log=pd.DataFrame(columns=error_name,data=error_log)
        df_error_log.to_csv('../../Output/{}/error_log.csv'.format(vset), index=False)
    else:       
        output.to_csv('../../Output/{}/testPredResult.csv'.format(vset), index=False)        
        # In[16]:     
    try:
        dtrain = xgb.DMatrix(X_train)
        pre_train = model.predict(dtrain)
        ans_train = pd.DataFrame(pre_train,columns=['pred'])
        output_train = pd.concat([y_train,ans_train],axis=1)
        output_train = pd.concat([sheet_train,output_train],axis=1)
        output_train['differ']=np.abs(output_train['Value']-output_train['pred'])
    except:
        temp_tuple = ()
        temp_tuple+=(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),)
        temp_tuple+=(sys.exc_info()[0],)
        temp_tuple+=(sys.exc_info()[1],)
        error_log.append(temp_tuple)
        df_error_log=pd.DataFrame(columns=error_name,data=error_log)
        df_error_log.to_csv('../../Output/{}/error_log.csv'.format(vset), index=False)
    else:          
        output_train.to_csv('../../Output/{}/trainPredResult.csv'.format(vset), index=False)           
#呼叫主函式    
xgb_execution()


#AVM_sample_code============================================================================
from Read_path import Read_in_out
from config import read_config
from CreateLog import WriteLog

import pandas as pd

def sum_col(folder_path):
    # get input, output path
    IO_path = Read_in_out(folder_path)
    input_path, output_path = IO_path.Read_in_out_path()
    
    # init log
    mylog = WriteLog(output_path["log_path"], output_path["error_path"])
    mylog.init_logger()
    
    # read config
    config = read_config(input_path["config_path"], mylog)
    
    # read input file
    try:
        data = pd.read_csv(input_path["raw_path"])
    except Exception as e:
        mylog.error("Read raw data error")
        mylog.error_trace(e)        
    
    # calculate sum
    col = config["col"]
    sum_ = data[col].sum()
    
    # record to log
    mylog.info("the sum is "+str(sum_))
    
    return sum_

if __name__ == "__main__":
    path = "Cases/AVM_sample_dir/02_sum"
    sum_col(path)

    
#Read_path===========================================================================
# -*- coding: utf-8 -*-
import json
import os
import traceback

class Read_in_out:
    def __init__(self, folder_path):
        self.folder_path = folder_path
        self.input_file_name = "input_file_path.json"
        self.output_file_name = "output_file_path.json"
        self.input_file_path = os.path.join(self.folder_path, self.input_file_name)
        self.output_file_path = os.path.join(self.folder_path, self.output_file_name)
    
    def Read_json(self, path):
        try:
            with open(path) as json_data:
                file = json.load(json_data)
            return file
    
        except Exception as e:
            print('load file error:\n', e)
            path, _ = os.path.split(path)
            err_path = os.path.join(path, "error.log")
            with open(err_path, 'a') as file:
                file.write('load file error:\n')
                file.write(str(e))
                file.write(traceback.format_exc())
            raise
            
    def Read_in_out_path(self):
        input_path = self.Read_json(self.input_file_path)
        output_path = self.Read_json(self.output_file_path)
        return input_path, output_path
    
    
#CreateLog============================================================================
# -*- coding: utf-8 -*-
import logging
import getpass

class WriteLog(object):
    def __init__(self, normal_log_path, error_log_path):
        self.normal_log_path = normal_log_path
        self.error_log_path = error_log_path
        self.user=getpass.getuser()	#取得user[帳號]
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
        
    def error_trace(self, msg):
        self.log.error(msg,exc_info=True)
        self.err_log.error(msg,exc_info=True)
    
    def warning_trace(self, msg):
        self.log.warning(msg,exc_info=True)
        self.err_log.warning(msg,exc_info=True)
        
        
        
if __name__ == "__main__":
    mylog = WriteLog("./log.log", "./error.log")
    mylog.init_logger()
#    mylog.logger.info("GGGG")
#    mylog.logger_error.error("NOOOOO")
    mylog.error("Holy Shit")



    
