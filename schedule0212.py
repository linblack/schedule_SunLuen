#-*- coding:utf-8 -*-

import re #regular expression(正規表達式)，處理掉讀者不想閱讀的部份, 或是過濾掉不必要的訊息, 只清楚地顯示你所需要的那一部份
import csv
import os
import sys
import time
import xlrd #xls/xlsx read
import copy
import logbook
import logging
import pytz #python time zone
import schedule#引用相關排程Lib來進行排程工作: 目前測試OK，但必須寫為同一個函式內完成所有相關工作(設定參數、匯出CSV紀錄檔案、自動排缸)[也就是把每個按鈕動作都排進來，每個動作都要time.sleep(秒數)，可看停個1~5秒)
from tkinter import *
import tkinter
import tkinter.ttk as ttk
import pandas as pd #資料前處理，可以擷取 JSON, CSV, Excel, HTML 等等格式的資料。資料型態為Series(一維陣列)及 DataFrame(二維陣列) 格式，可以使用 Index (row) 或 Column 存取資料
#from getDBData import FetchDataFromSqlDB
from getDBData import TestDBconnection
from getDBData import fetch_dyeing_data_without_2weeks#目前沒有抓取兩周內的資料
from getDBData import fetch_dyeing_data_with_Totolcon#0105:去抓取目前總濃度的相關資料，再與工單資料合併
from Queue import queue#新增排缸queue物件
from cylinder import cylinder#新增缸物件
from jobtask import jobtask#新增工單物件
from config import config#新增config物件去讀取資料庫參數
#from loadExcelFile import LoadExcelFile#載入相關檔案
from logbook import  Logger, StreamHandler, FileHandler#1211新增logbook紀錄相關程式狀況
from datetime import datetime #時間
from tkinter import filedialog#顯示選取檔案畫面
from tkinter.messagebox import showinfo#顯示視窗畫面
import operator
#=============================================
global after_sort_list
after_sort_list=[]
global temp_list
temp_list=[]
#@@@@@@@@@@@@@@@@@@@@@@@副函式區域@@@@@@@@@@@@@
def compare(_reclist, tree):#進行與現在時間排程比較: 輸入(資料List, 顯示格式)，輸出(排序後結果)
    clear_tree_content(tree)#清除treeview裡面顯示文字
    global return_list
    return_list=[]
    temp=[]
    temp=_reclist #_reclist=temp_tuple[100筆]
    d = datetime.now()#取得目前時間
    t = d.time()
    current_year=0
    current_month=0
    current_day=0
    current_hour=0
    current_minute=0
    current_second=0.0
    current_year=d.year
    current_month=d.month
    current_day=d.day
    current_hour=t.hour
    current_minute=t.minute
    current_second=t.second
    #這邊要去抓2week時間的工單近來
    #用下面的delta=time1-time2,delta<=14來進行判斷
#-------------------------------------10/20時間比較---------
    #TIMEFORMAT="%Y/%m/%d %H:%M:%S:%f"
    TIMEFORMAT_WITHOUT_MS="%Y-%m-%d %H:%M:%S"#1218 mark: 因為有的資料庫欄位沒有milisecond
    #=========以上避免沒有microsecond秒數的資料格式互換，產生錯誤=====
    j=0
    i=0
    for i in range(0,len(temp)):
        for j in range(0,len(temp)-1-i):
            _temp=()
            time1=datetime.strptime(temp[j][1], TIMEFORMAT_WITHOUT_MS)
            time2=datetime.strptime(temp[j+1][1], TIMEFORMAT_WITHOUT_MS)
            #print(time1)
            #print(time2)
            #0212:轉換為time1，time2去計算時間差距為何，提供後續排序使用
            
            if time1>time2 and j<=len(temp):
                _temp=temp[j]
                temp[j]=temp[j+1]
                temp[j+1]=_temp
                #print(_temp)
    #後續為顯示相關排序結果
    #回存gloabl變數return_list中，等候輸出使用
    first=0
    index=0
    size1=len(temp)
    for i  in range(len(temp)):
            tree.insert("" , 0,    text=str(size1), values=(temp[i][0],temp[i][1],temp[i][2],temp[i][3],temp[i][4],temp[i][5],temp[i][6],temp[i][7],temp[i][8],temp[i][9],temp[i][10],temp[i][11]))
            #temp[i][12] 新增總濃度(1206)
            #1214 新增總濃度
            size1-=1
    return_list=temp
    print("排序後結果為")
    print(return_list)
#============================================================
def outputCsv(_output_list, button, _job_list):#輸出CSV檔案: 輸入(缸List, 按鈕，工單資料List)，輸出(CSV檔案)

    enable_button(btn_db_connection)#開啟資料庫連線按鈕
    #=======================
    global file_to_output
    file_to_output=[]
    file_to_output=_output_list
    pathProg=os.path.abspath('.')#更改路徑為執行程式的路徑，避免對方電腦沒有相關路徑名稱，產生錯誤
    print("目前CSV檔案輸出路徑為 "+os.path.abspath('.'))
    if not os.path.exists(pathProg):
          os.makedirs(pathProg)
          #print("CSV資料目錄已經建立")
          text4.insert(tkinter.INSERT,"CSV資料目錄已經建立")
    else:
          #print("CSV資料目錄已經存在")
          text4.insert(tkinter.INSERT,"CSV資料目錄已經存在")
    os.chdir(pathProg)
    if os.getcwd() != pathProg:
        #print ("EEROR:.檔案存檔路徑錯誤")
        text4.insert(tkinter.END,"EEROR:.檔案存檔路徑錯誤")
        sys.exit()
    else:
            filename=""
            filename=datetime.now().strftime("%Y%m%d-%H%M%S")
            myfile='\\'+filename+'.csv'
            if os.path.isfile(pathProg+myfile):
                os.remove(pathProg+myfile)
                text4.insert(tkinter.INSERT,"檔案產生中")
            else:    ## Show an error ##
                text4.insert(tkinter.END,"\nError: %s 無找到相關輸出CSV檔案" % myfile)
            #--------/*檢查檔案是否存在，存在就刪除 重新產生*/
            file = open(pathProg + myfile, 'w', newline='')
            writer =csv.writer(file, delimiter=',', quoting=csv.QUOTE_NONE)
            csvHeader = ['排程單號']
            writer.writerow(csvHeader)
            csvHeader1 = ['機台號','缸號','工單','開卡日時間', '投胚日時間']
            writer.writerow(csvHeader1)
            i=0#loop index
            for i in range(len(file_to_output)):
                #==================列印每缸表頭資料區============
                if i!=len(file_to_output)-1:
                    #加上[]後，變成物件，便不會變成各自逗點分隔出來
                    writer.writerow(["#====第"+str(file_to_output[i].get_cylinder_no())+"缸排程工單==start==#"])
                else:
                    writer.writerow(["#====暫時缸未排程工單==start==#"])
                #==================列印每缸工單資料區
                j=0
                size=0
                #0111 修改: 改為get_len_of_cylinder_con_list輸出
                #size=file_to_output[i].queue.size()
                size=file_to_output[i].get_len_of_cylinder_con_list()
                
                #0111:取得相關get_len_of_cylinder_con_list
                temp_list=[]
                temp_list=file_to_output[i].get_cylinder_con_list()
                
                global temp_tuple
                global _temp_tuple
               
                #格式為[機台號,缸號,工單,開卡日, 投胚日]
                _tmp_cyno=str(file_to_output[i].get_cylinder_no())#缸號
                #-========以上要設定相關機台號、缸號、工單清單、開卡日、投胚日
                for j in range(size):#根據工單號
                    _temp_tuple=()
                    _temp_tuple=temp_list[j]
                    _tmp_mach=""#機台號
                    _tmp_cist_no=""#工單號
                    _tmp_pr_date=""#開卡日
                    _tmp_emb_qty_date=""#投胚日
                    _tmp_cist_no=temp_list[j][0]#0111:取得排序過後的第一筆資料
                    if _tmp_cist_no!=None:
                        temp_tuple=search_tuple_from_joblist(_tmp_cist_no,_job_list)#從工單號去找出相對應工單資訊tuple(xxx,xxx,xxx)
                        if temp_tuple!=None:
                        #==/這邊是要填入機台號，但是目前的機台號編碼不曉得，故還要看倍斯特這邊定義/
                            #0208:修改機台號為缸號(1~9缸則是加上0，變成兩位數，超過則是不加上0)
                            #if len(_tmp_cyno)==1:#表示由1~9缸，則是多加0至前面缸號
                            #   _tmp_machid=''
                            #   _tmp_machid='0'+_tmp_cyno
                            #   _tmp_mach=str(_tmp_machid)#機台號
                            #else:
                            #   _tmp_mach=str(_tmp_cyno)#機台號
                        #===================================
                            _tmp_mach=str(temp_tuple[7])
                            #%%%0122:這邊可能要換成上面的D+缸號
                            _tmp_cist_no=str(temp_tuple[0])#工單號
                            _tmp_pr_date=str(temp_tuple[1])#開卡日
                            _tmp_emb_qty_date=str(temp_tuple[10])#投胚日
                            #最後寫出一整行相關排缸資料(機台號，缸號，工單，開卡日，投胚日)
                            writer.writerow([_tmp_mach]+[_tmp_cyno]+[_tmp_cist_no]+[_tmp_pr_date]+[_tmp_emb_qty_date])
                            
                        temp_tuple=None
                #==========================================
                if i!=len(file_to_output)-1:
                    writer.writerow(["#====第"+str(file_to_output[i].get_cylinder_no())+"缸排程工單==end==#"])
                else:
                    writer.writerow(["#====暫時缸未排程工單==start==#"])
                writer.writerow(["%%"])
            file.close
            #0122:暫時先不清理相關顯示文字，避免未排缸的工單被忽略
            #clear_text_content(text4)#清除text4顯示文字
            text4.insert(tkinter.END,"檔案輸出成功")
            log.info("CSV檔案已經輸出")
#======/*從工單號去找出相對應工單資訊tuple(xxx,xxx,xxx)資訊: 輸入(工單號碼，缸List)，輸出(工單資料tuple)*/
def search_tuple_from_joblist(_target_jobno, _rec_list):
    global found_tuple
    found_tuple=[]
    size=0
    size=len(_rec_list)
    index=0
    for index in range(0,size):
        tmp_no=""
        tmp_no=_rec_list[index][0]
        if _target_jobno==tmp_no:
            found_tuple.append(_rec_list[index][0])#工單號
            found_tuple.append(_rec_list[index][1])#開卡日
            found_tuple.append(_rec_list[index][2])#定單號
            found_tuple.append(_rec_list[index][3])#投胚量
            found_tuple.append(_rec_list[index][4])#重量(kg)
            found_tuple.append(_rec_list[index][5])#色號
            found_tuple.append(_rec_list[index][6])#碼長[3]/[4]
            found_tuple.append(_rec_list[index][7])#機台號
            found_tuple.append(_rec_list[index][8])#濃度
            found_tuple.append(_rec_list[index][9])#領料量
            found_tuple.append(_rec_list[index][10])#投胚日
            found_tuple.append(_rec_list[index][11])#總濃度
            break
        else:
            continue
    return found_tuple
#===========#讀取測試檔案到tree顯示區域: 輸入(資料list，顯示格式)，輸出(讀入總濃度資料List)
def readfile_to_tree(_rec_showlist, tree):
    #從DB載入相關工單號碼與資料欄位: 目前測試OK
    global DBList
    DBList=[]
    #=========================
    global db_co_object
    db_co_object=None
    #建立DB物件，取得目前DB_config.ini目前檔案值
    db_co_object=config()
    #===========之前測試連線用，看倍思特的API再來打開此部分程式碼===
    #DBList=FetchDataFromSqlDB(db_co_object.get_server(),db_co_object.get_dbname(),db_co_object.get_user(),db_co_object.get_password())
    #index=0
    #dblist_size=0
    #dblist_size=len(DBList)
    #for index in range(0,dblist_size):
    #    tmp_jobname=""
    #    tmp_jobname=DBList[index][0]
    #    print(tmp_jobname)
    #-----以上測試DB連線用
    #===========================================================   
    
    global _to_jobtask_list
    _to_jobtask_list=[]
    #清除目前相關tree內容，避免重復顯示
    clear_tree_content(tree)
    file_to_tree=[]
    file_to_tree=_rec_showlist
    #測試看看是否資料庫這邊可以撈取相關資料出來
    #file_to_tree=DBList
    
    #計算碼長 並填入相關list中
    size=len(file_to_tree)
    #目前會傳送的list
    global jobtask_list
    jobtask_list=[]
    
    temp_tuple=()
    j=0
    size=len(_rec_showlist)
    for j in range(size):
        temp=0
        temp_emb_qty=float(file_to_tree[j][3])#投胚量
        temp_weight_kg=float(file_to_tree[j][4])#換算成kg才能計算碼長
        temp_yard=float(file_to_tree[j][6])#計算出碼長,目前不知道長度單位為何(幾尺)
        #if file_to_tree[j][8]!=None:
        #    temp_pi_qty=float(file_to_tree[j][8])#領料量
        if file_to_tree[j][8]=='':
            temp_pi_qty=0.0
        else:
            temp_pi_qty=0.0
        #如果無投胚量或是碼長，則是都設定濃度為0.0(因為無法算出相關濃度值)
        if temp_emb_qty==0.0 or temp_pi_qty==0.0:
            #濃度設定為0.0
            temp_con=0.0
        else:
            #工單各自濃度
            temp_con=temp_pi_qty/(temp_emb_qty*1000)
            temp_con=round(temp_con,3)
           

        if  file_to_tree[j][11]=="0.0" or file_to_tree[j][11]=="":
            file_to_tree[j][11]=0.0
        #==========機台號要看跟貝斯特談得如何，再來打開此程式碼
        #0207:機台號填入空白(清空機台號)
        #file_to_tree[j][7]=""
        #======================================================
        
        _jobtask=(file_to_tree[j][0],file_to_tree[j][1],file_to_tree[j][2],file_to_tree[j][3],file_to_tree[j][4],file_to_tree[j][5],temp_yard,
        file_to_tree[j][7],file_to_tree[j][8],temp_con,file_to_tree[j][10],file_to_tree[j][11])
        #file_to_tree[j][11] 1214: 新增總濃度
        
        tree.insert("" , 0, text=str(size), values=(file_to_tree[j][0],file_to_tree[j][1],file_to_tree[j][2],file_to_tree[j][3],file_to_tree[j][4],file_to_tree[j][5],temp_yard,
        file_to_tree[j][7],file_to_tree[j][8],temp_con,file_to_tree[j][10], file_to_tree[j][11]))
        #file_to_tree[j][11] 1214: 新增總濃度
        
        temp_tuple+=(file_to_tree[j][0],)
        temp_tuple+=(file_to_tree[j][1],)
        temp_tuple+=(file_to_tree[j][2],)
        temp_tuple+=(file_to_tree[j][3],)
        temp_tuple+=(file_to_tree[j][4],)
        temp_tuple+=(file_to_tree[j][5],)
        temp_tuple+=(temp_yard,)
        temp_tuple+=(file_to_tree[j][7],)#機台號碼
        temp_tuple+=(file_to_tree[j][8],)
        temp_tuple+=(temp_con,)
        temp_tuple+=(file_to_tree[j][10],)
        temp_tuple+=(file_to_tree[j][11],)#1214 新增總濃度        
        
        jobtask_list.append(temp_tuple)
        _to_jobtask_list.append(_jobtask)
        #填入jobtask_list中  這裡要處理_jobtask要填入最後的temp值再存入List中
        _jobtask=None
        temp_tuple=()
        size-=1
#---------#1101進度設定每缸參數: 輸入(文字顯示區域)，輸出(每缸設定完成相關參數)
def setup_cylinder_params(text):
     #==================清除文字內容==
    clear_listbox_content(cy1_status)
    clear_listbox_content(cy3_status)
    clear_listbox_content(cy5_status)
    clear_listbox_content(cy6_status)
    clear_text_content(text4)
    #================0111:新增相關工單與濃度List(做後續濃度排序與工單顯示使用)
    global cylinder_list
    cylinder_list=[]
    
    global _cy1_list,_cy2_list, _cy3_list
    _cy1_list,_cy2_list, _cy3_list=[],[],[]
   
    global _cy5_list,_cy6_list,_cy7_list
    _cy5_list,_cy6_list,_cy7_list=[],[],[]
 
    global _cy8_list,_cy9_list,_cy10_list
    _cy8_list,_cy9_list,_cy10_list=[],[],[]
    
    
    global _cy11_list,_cy12_list,_cy13_list
    _cy11_list,_cy12_list,_cy13_list=[],[],[]

    global _cy14_list,_cy15_list,_cy16_list
    _cy14_list,_cy15_list,_cy16_list=[],[],[]
    
    
    global _cy17_list,_cy18_list,_cy19_list
    _cy17_list,_cy18_list,_cy19_list=[],[],[]
    
    
    global _cy20_list,_cy21_list,_cy22_list
    _cy20_list,_cy21_list,_cy22_list=[],[],[]
    
    global _cy23_list,_cy24_list,_cy25_list
    _cy23_list,_cy24_list,_cy25_list=[],[],[]

    global _cy26_list,_cy32_list,_cy100_list
    _cy26_list,_cy32_list,_cy100_list=[],[],[]


    #0111:每缸的設定，各參數 1:缸號, 'B':固定染色號, 500:投胚量, 3200:碼長, _cy1_list:用來儲存(工單,總濃度) tuple的list，用來作之後顯示與排序使用。
    temp_cylinder1=cylinder(1, 'B', 500, 3200,_cy1_list,False)
    setup_cyparams_to_listbox(cy1_status,temp_cylinder1)
    #----------------------------------------
    temp_cylinder2=cylinder(2, '', 20, 80,_cy2_list,False)
    setup_cyparams_to_listbox(cy2_status,temp_cylinder2)
    #--------------------------------------
    temp_cylinder3=cylinder(3, '', 150, 500,_cy3_list,False)
    setup_cyparams_to_listbox(cy3_status,temp_cylinder3)
    #-----------------------------------------
    temp_cylinder5=cylinder(5, '', 500, 3200,_cy5_list,False)
    setup_cyparams_to_listbox(cy5_status,temp_cylinder5)
    
    temp_cylinder6=cylinder(6, '', 500, 3200,_cy6_list,False)
    setup_cyparams_to_listbox(cy6_status,temp_cylinder6)
    
    temp_cylinder7=cylinder(7, 'B', 1000, 6400,_cy7_list,False)
    setup_cyparams_to_listbox(cy7_status,temp_cylinder7)
    
    temp_cylinder8=cylinder(8, '', 150, 500,_cy8_list,False)
    setup_cyparams_to_listbox(cy8_status,temp_cylinder8)
    
    temp_cylinder9=cylinder(9, 'W', 1000, 6400,_cy9_list,False)
    setup_cyparams_to_listbox(cy9_status,temp_cylinder9)
    
    temp_cylinder10=cylinder(10, 'B', 500, 3200,_cy10_list,False)
    setup_cyparams_to_listbox(cy10_status,temp_cylinder10)
    
    temp_cylinder11=cylinder(11, '', 500, 3200,_cy11_list,False)
    setup_cyparams_to_listbox(cy11_status,temp_cylinder11)
    
    temp_cylinder12=cylinder(12, '', 500, 3200,_cy12_list,False)
    setup_cyparams_to_listbox(cy12_status,temp_cylinder12)
    
    
    temp_cylinder13=cylinder(13, 'W', 500, 3200,_cy13_list,False)
    setup_cyparams_to_listbox(cy13_status,temp_cylinder13)
    
    temp_cylinder14=cylinder(14, '', 100, 500,_cy14_list,False)
    setup_cyparams_to_listbox(cy14_status,temp_cylinder14)
    
    temp_cylinder15=cylinder(15, '', 500, 3200,_cy15_list,False)
    setup_cyparams_to_listbox(cy15_status,temp_cylinder15)
    
    temp_cylinder16=cylinder(16, '', 20, 80,_cy16_list,False)
    setup_cyparams_to_listbox(cy16_status,temp_cylinder16)
    
    temp_cylinder17=cylinder(17, '', 20, 80,_cy17_list,False)
    setup_cyparams_to_listbox(cy17_status,temp_cylinder17)
    
    temp_cylinder18=cylinder(18, '', 20, 80,_cy18_list,False)
    setup_cyparams_to_listbox(cy18_status,temp_cylinder18)
    
    temp_cylinder19=cylinder(19, 'B', 500, 3200,_cy19_list,False)
    setup_cyparams_to_listbox(cy19_status,temp_cylinder19)
    
    temp_cylinder20=cylinder(20, '', 300, 1600,_cy20_list,False)
    setup_cyparams_to_listbox(cy20_status,temp_cylinder20)
    
    
    temp_cylinder21=cylinder(21, '', 300, 1600,_cy21_list,False)
    setup_cyparams_to_listbox(cy21_status,temp_cylinder21)
    
    temp_cylinder22=cylinder(22, '', 300, 1600,_cy22_list,False)
    setup_cyparams_to_listbox(cy22_status,temp_cylinder22)
    
    temp_cylinder23=cylinder(23, '', 300, 1600,_cy23_list,False)
    setup_cyparams_to_listbox(cy23_status,temp_cylinder23)
    
    
    temp_cylinder24=cylinder(24, '', 200, 500,_cy24_list,False)
    setup_cyparams_to_listbox(cy24_status,temp_cylinder24)
    
    temp_cylinder25=cylinder(25, '', 150, 500,_cy25_list,False)
    setup_cyparams_to_listbox(cy25_status,temp_cylinder25)
    
    temp_cylinder26=cylinder(26, '', 500, 3200,_cy26_list,False)
    setup_cyparams_to_listbox(cy26_status,temp_cylinder26)
    
    temp_cylinder32=cylinder(32, '', 100, 500,_cy32_list,False)
    setup_cyparams_to_listbox(cy32_status,temp_cylinder32)
    
    #暫時先設立一個100號缸:處理相關未排進去的工單號碼
    temp_cylinder100=cylinder(100, '', 0, 0,_cy100_list,False)
    #setup_cyparams_to_listbox(listbox_temp,temp_cylinder100)
    
    cylinder_list.append(temp_cylinder1)
    cylinder_list.append(temp_cylinder2)
    cylinder_list.append(temp_cylinder3)
    cylinder_list.append(temp_cylinder5)
    cylinder_list.append(temp_cylinder6)
    cylinder_list.append(temp_cylinder7)
    cylinder_list.append(temp_cylinder8)
    cylinder_list.append(temp_cylinder9)
    cylinder_list.append(temp_cylinder10)
    cylinder_list.append(temp_cylinder11)
    cylinder_list.append(temp_cylinder12)
    cylinder_list.append(temp_cylinder13)
    cylinder_list.append(temp_cylinder14)
    cylinder_list.append(temp_cylinder15)
    cylinder_list.append(temp_cylinder16)
    cylinder_list.append(temp_cylinder17)
    cylinder_list.append(temp_cylinder18)
    cylinder_list.append(temp_cylinder19)
    cylinder_list.append(temp_cylinder20)
    cylinder_list.append(temp_cylinder21)
    cylinder_list.append(temp_cylinder22)
    cylinder_list.append(temp_cylinder23)
    cylinder_list.append(temp_cylinder24)
    cylinder_list.append(temp_cylinder25)
    cylinder_list.append(temp_cylinder26)
    cylinder_list.append(temp_cylinder32)
    #設立第100號缸，處理未排入上述缸號的相關工單號碼
    cylinder_list.append(temp_cylinder100)
    text4.insert(END,"各缸參數已設定完成")
#--------------#回傳每缸參數:輸入(顯示格式，缸資料物件)，輸出(每缸設定好參數文字)
def setup_cyparams_to_listbox(listbox, _temp_cylinder):#進行設定每缸參數
    listbox.insert(1, "缸號: "+str(_temp_cylinder.get_cylinder_no()))
    listbox.insert(2,"染色號: "+str(_temp_cylinder.get_dying_color_name()))
    listbox.insert(3,"缸容量: "+str(_temp_cylinder.get_weight())+"Kg")
    listbox.insert(4,"碼長: "+str(_temp_cylinder.get_yard_length())+"")
    listbox.insert(5,"缸容量(80%): "+str(_temp_cylinder.get_weight()*0.8)+"Kg")
#---#進行工單負荷平均排缸派遣動作:輸入(缸list，工單資料list)，輸出(目前已經排好第一輪的工單資料，尚未有最小清缸化資料)
def ProcessSchedulingOnCylinders(_rec_cylinderlists, _schedule_list):
    #目前各缸資訊物件list
    global processing_cylinder_list
    processing_cylinder_list=[]
    processing_cylinder_list=_rec_cylinderlists
    #目前各job工單資料物件list
    global _rec_jobtask_list
    _rec_jobtask_list=[]
    _rec_jobtask_list=_schedule_list
  
    global temp_date
    TIMEFORMAT="%Y/%m/%d %H:%M:%S:%f"
    
    global job_counter#0117:紀錄工單筆數counter
    job_counter=0
    
    
    i=0
    j=0
    global Unable_Schedule_List
    Unable_Schedule_List=[]#儲存投胚量為0的工單編號list
    for i,item in enumerate(_rec_jobtask_list): #enumerate=>index運用需求
        temp_yard=0.0
        temp_qty=0.0
        temp_total_com=0.0
        
        temp_no=_rec_jobtask_list[i][0]#工單號
        temp_date=_rec_jobtask_list[i][1]#工單時間-用來比對放入缸中(一天最多4筆)
        temp_od_no=_rec_jobtask_list[i][2]#訂單號:未來可做缸號工單比對(可把相同工單排在一起)
        temp_qty=float(_rec_jobtask_list[i][3])#投胚量
        temp_yard=float(_rec_jobtask_list[i][6])#碼長
        temp_total_com=_rec_jobtask_list[i][11]#取得最後總濃度
      
        if temp_qty==0.0 or temp_yard>6400  :#0116:小於0  或是大6400的碼長，通通抓近來
            Unable_Schedule_List.append(temp_no)#紀錄有幾筆工單的投胚量為0或是碼常大於6400的工單，排入此無法安排的List中，之後自動排缸玩會顯示出來相關工單。
            continue
        else:
            print('------------------------------') 
            print('工單號%s' %temp_no)
            cyfound_index=0
            #0110進行排缸: 傳入(工單號，總濃度，碼長，投胚量，處理的總缸List，暫存區List)
            cyfound_index=Find_Best_Cylinder_To_Schedule(temp_no,temp_total_com,temp_yard, temp_qty,processing_cylinder_list,recyce_list)
            job_counter+=1
            print('目前所排的工單筆數為第%s筆' %job_counter)
    #-===============
    global load_balance_list#0118:紀錄需要啟動負載平衡的工單List
    load_balance_list=[]
    _final_result=False
    _final_result=Find_the_temp_job_to_schedule(recyce_list,processing_cylinder_list,load_balance_list)
    if _final_result:
       print('暫存區工單已經排入完畢')
       recyce_list.clear()
    #原來顯示function為下面，但目前已經不使用。故註解起來
    #print_each_cylinder_queue(processing_cylinder_list[0].queue,cy1_schedule)
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[0].get_cylinder_con_list(),cy1_schedule)
    
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[1].get_cylinder_con_list(),cy2_schedule)
    
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[2].get_cylinder_con_list(),cy3_schedule)
    
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[3].get_cylinder_con_list(),cy5_schedule)
    
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[4].get_cylinder_con_list(),cy6_schedule)
   
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[5].get_cylinder_con_list(),cy7_schedule)
   
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[6].get_cylinder_con_list(),cy8_schedule)
  
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[7].get_cylinder_con_list(),cy9_schedule)

    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[8].get_cylinder_con_list(),cy10_schedule)

    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[9].get_cylinder_con_list(),cy11_schedule)

    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[10].get_cylinder_con_list(),cy12_schedule)

    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[11].get_cylinder_con_list(),cy13_schedule)
    
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[12].get_cylinder_con_list(),cy14_schedule)
    
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[13].get_cylinder_con_list(),cy15_schedule)
    

    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[14].get_cylinder_con_list(),cy16_schedule)
    
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[15].get_cylinder_con_list(),cy17_schedule)
    
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[16].get_cylinder_con_list(),cy18_schedule)
    
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[17].get_cylinder_con_list(),cy19_schedule)
    
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[18].get_cylinder_con_list(),cy20_schedule)
    
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[19].get_cylinder_con_list(),cy21_schedule)
    
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[20].get_cylinder_con_list(),cy22_schedule)
    
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[21].get_cylinder_con_list(),cy23_schedule)
    
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[22].get_cylinder_con_list(),cy24_schedule)
   
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[23].get_cylinder_con_list(),cy25_schedule)
    
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[24].get_cylinder_con_list(),cy26_schedule)
    
    print_scheduled_jobno_for_each_cylinder(processing_cylinder_list[25].get_cylinder_con_list(),cy32_schedule)

    #0111:這邊顯示戰存區相關工單與濃度資料
    print_scheduled_jobno_for_each_cylinder(recyce_list,listbox_temp)
    
   #===============以下顯示目前工單投胚量為0的部分===
    clear_text_content(text4)
    text4.insert(tkinter.INSERT,"目前投胚量為0的工單共有"+str(len(Unable_Schedule_List))+"筆，如下\n")
    qty_zero_size=0
    qty_zero_size=len(Unable_Schedule_List)
    index=0
    for index in range(qty_zero_size):
        text4.insert(tkinter.INSERT,str(Unable_Schedule_List[index])+"\n")
    log.info("自動排缸流程完畢")
#=============0116:去找入相關暫存區待排工單，輸入(工單資料list，缸list， 需要待排的資料List)，輸出(有找到為True，沒有找到為False)
def Find_the_temp_job_to_schedule(_recy_list, _rec_pro_list,load_balance_list):
    global _temp_recy_list,final_result,counter_temp_recy_list
    _temp_recy_list=[]
    final_result=False
    counter_temp_recy_list=0
    
    _temp_recy_list=_recy_list
    size_temp_recy_list=0
    size_temp_recy_list=len(_recy_list)
    
    size_process_list=len(_rec_pro_list)
    index=0
    for index in range(size_process_list):
        _temp_list=[]
        _temp_list=_rec_pro_list[index].get_cylinder_con_list()#取得目前排入list
        result=False
        
        result=List_tuple_compare(_recy_list,_temp_list, _rec_pro_list[index].get_cylinder_no())
        
        if result>0:
          counter_temp_recy_list+=result
        else:
          continue
    if  counter_temp_recy_list==len(_recy_list):
       final_result=True
    
    return final_result
#============0116List_tuple_compare: list中的資料比較: 輸入(排序的List1，被找尋List2，被找尋的工單號碼)  
def List_tuple_compare(_from_list, _to_list,_rec_pro_cylist_index):
    global counter
    counter=0
    size_from_list=0
    size_from_list=len(_from_list)
    size_to_list=0
    size_to_list=len(_to_list)
    i=0
    j=0
    for i in range(size_from_list):
        _temp_to_schedule_jobno=''
        _temp_to_schedule_jobno=_from_list[i][0]
        for j in range(size_to_list):
            _temp_scheduled_jobno=''
            _temp_scheduled_jobno=_to_list[j][0]
            if _temp_to_schedule_jobno==_temp_scheduled_jobno:
                counter+=1
                _tuple=()
                _tuple+=(_temp_to_schedule_jobno,)
                _tuple+=(_rec_pro_cylist_index,)
                load_balance_list.append(_tuple)
                break
            else:
                continue

    return counter    
#============0110 去找出最符合的缸號，並排入缸號與回傳缸號號碼: 輸入(被找尋工單號，被找尋濃度，被找尋碼長，
#被找尋投胚量，缸list，回收List)，輸出(相關缸號物件)
def Find_Best_Cylinder_To_Schedule(_test_no, _test_con, _test_yard, _test_qty,_processing_cylinder_list,_recyce_list):    
    global _test_cy_yard
    global _test_cy_qty_weight
    global _test_cy_color
    
    global _diff_list
    _diff_list=[]
    global _temp_tuple
    
    global _diff_yard
    global _diff_weight
    
    global _original_list
    _original_list=[]
    
    #0112 added:新增分類list，分群list進行分類演算法
    #===============1. 先去找屬於那種(投胚量，碼長)分類，找到分類後，去找下面的各缸random去填入缸中，如果缸滿了，則式進行負載平衡，去找其他缸，或是往下一層去找
    cy_category_list_size=len(cy_category_list)
    j=0
    category_index=0
    for j in range(cy_category_list_size):
        _temp_tuple=()
        
        _diff_yard=0
        _diff_qty=0
        _cate_cy_no=0
        
        _each_cy_qty=cy_category_list[j][0]#取出qty投胚量
        _each_cy_yard=cy_category_list[j][1]#取出碼長
        
        _diff_qty=_each_cy_qty-_test_qty#計算每一個投胚量差距
        _diff_yard=_each_cy_yard-_test_yard#計算每一個碼長差距
        _cate_cy_no=category_index#分類編號
        
        
        _temp_tuple+=(_diff_qty,)
        _temp_tuple+=(_diff_yard,)
        _temp_tuple+=(_cate_cy_no,)
            
        _diff_list.append(_temp_tuple)
        _original_list.append(_temp_tuple)
        category_index+=1
    #_diff_list.sort(key=lambda tup: (tup[0], tup[1]))#依照yard 跟weight 去排序
    
    #先過濾有負號(小於0)的元素[0]，再過濾有負號(小於0)的元素[1]
    global _diff_list_minus_0_list
    _diff_list_minus_0_list = [i for i in _diff_list if i[0] >= 0]
   
    global _diff_list_minus_1_list
    _diff_list_minus_1_list = [i for i in _diff_list_minus_0_list if i[1] >= 0]
    _diff_list_minus_1_list=sorted(_diff_list_minus_1_list, key=operator.itemgetter(1, 2))
    #0131: 經過上面的過濾，可過濾出最相近的缸號是哪一缸，便可依照此缸號排入缸號list中。
    #_diff_list=sorted(_diff_list, key=operator.itemgetter(1, 2))#依照yard 跟weight 去排序
    
    global _result_class
    _result_class=0
    i=0
    j=0
    
    for j in range(0, len(_diff_list_minus_1_list)):
        _testing_qty=_diff_list_minus_1_list[j][0]
        if _testing_qty>=0:
            if len(_diff_list_minus_1_list)==1:
                _result_class=_diff_list_minus_1_list[j][2]
            elif j+1<len(_diff_list_minus_1_list):
                if _testing_qty<=_diff_list_minus_1_list[j+1][0]:
                    _result_class=_diff_list_minus_1_list[j][2] #1
                    break
        else:
            continue 
    #=================================================        
        
    #==================2. 找出分類中的第幾號缸，排入缸中
    global _dispatched_group,_dispatched_ptr,_dispatched_cylinder        
    _dispatched_group=()
    _dispatched_group=cy_group_list[_result_class] #(14,32)
    _dispatched_ptr=cy_ptr_list[_result_class] #0

    if _dispatched_ptr<len(_dispatched_group):
        _dispatched_cylinder=_dispatched_group[_dispatched_ptr]
    else:
        _dispatched_ptr=_dispatched_ptr % len(cy_group_list[_result_class])
        _dispatched_cylinder=_dispatched_group[_dispatched_ptr]
   
    print('選擇到的缸分類為第%s類第%s缸' %(_result_class,_dispatched_cylinder))      
    #0112:這邊去更新ptr指標
    _dispatched_ptr+=1
    cy_ptr_list[_result_class]=_dispatched_ptr#寫回PTR目前缸指標記錄

    #=========================3.去排入缸中，如果缸已滿，則是往下一層去找未滿的缸
    i=0
    k=0 

    for i in range(0, len(_processing_cylinder_list)):
       _temp_cy_no=_processing_cylinder_list[i].get_cylinder_no()
       if _dispatched_cylinder==_temp_cy_no:
           
            _temp_jobno_con_tuple=()#0111:新增工單與濃度tuple
            _temp_jobno_con_list=[]#0111:新增工單與濃度tuple_list
            
            queue_flag=False
            queue_flag=_processing_cylinder_list[i].get_queue_full_flag()
   
            if not queue_flag: #0115:如果queue不滿(小於8張工單)
                #0115:這邊要去設定queue flag
                queue_size=0
                queue_size=_processing_cylinder_list[i].get_dying_queue().size()
                if queue_size>=8:#如果大於8張工單，則式進行過載平衡
                    print(u'預排入缸容量已滿')
                    print('=========過載平衡啟動=====================') 
                    _processing_cylinder_list[i].set_queue_full_flag(True)
                    #0116:測試放入回收暫時區
                    _tmp_tuple=()
                    _tmp_tuple+=(_test_no,)
                    _tmp_tuple+=(_test_con,)
                    recyce_list.append(_tmp_tuple)
                    #================================
                    #print(u'第 %s分類 %s 工單號 執行load負載平衡' %(_result_class,_test_no))
                    print('No. %s class %s work_id doing load balancing' %(_result_class,_test_no))
                    load_balance(_test_con,_dispatched_cylinder,_result_class,_test_no,_processing_cylinder_list,cy_group_list,cy_ptr_list)
                    print(u'=========過載平衡執行完畢==================') 
                    break
                else:#排入缸中的queue與list，通常list會放入tuple(工單號，濃度)，此樣想法為之後，移動工單或是排序工單可以使用，而非放入queue中(因queue中只會放入工單號，並不會放入濃度，因此為了後續的排缸與濃度排序使用，我新增了set_cylinder_con_list與get_cylinder_con_lis來放入tuple(工單號，濃度)，方便後續寫程式的快速性。而非用啥理論的queue，queue資料結構是好用，但是要搭配實際情況。
                    # 你也可以把工單整個物件放進來queue中，這樣方式也可以，但是我覺得光是要比較濃度後去移動物件，可能也要花不少時間，看你之後的需求為何                     
                
                    _processing_cylinder_list[i].queue.enq(_test_no)
                    #0111:紀錄工單與濃度list中
                    _temp_jobno_con_tuple+=(_test_no,)
                    _temp_jobno_con_tuple+=(_test_con,)
                    _temp_jobno_con_list=_processing_cylinder_list[i].get_cylinder_con_list()
                    _temp_jobno_con_list.append(_temp_jobno_con_tuple)
                    _processing_cylinder_list[i].set_cylinder_con_list(_temp_jobno_con_list)
                    #0116:設定到每缸的list中，方便後續濃度排序與調整使用
                    print(u"工單號 %s 已排入第%s分類 第 %s 缸中" %(_test_no,_result_class, processing_cylinder_list[i].get_cylinder_no()))
                    break
            else:   #要排的缸已經滿了，則是啟動過載平衡機制，去找下一層缸排入
                    print('預排入缸容量已滿')
                    print('==========過載平衡啟動==============') 
                    _processing_cylinder_list[i].set_queue_full_flag(True)
                    #0116:測試放入回收暫時區
                    _tmp_tuple=()
                    _tmp_tuple+=(_test_no,)
                    _tmp_tuple+=(_test_con,)
                    recyce_list.append(_tmp_tuple)
                    #==========================
                    print('第 %s分類 %s 工單號 執行load負載平衡' %(_result_class,_test_no))
                    load_balance(_test_con,_dispatched_cylinder,_result_class,_test_no,_processing_cylinder_list,cy_group_list,cy_ptr_list)
                    print('==========過載平衡執行完畢===========') 
                    break
       else:
           continue
    return _dispatched_cylinder
#====0115: 負載平衡相關的工單: 輸入(分類，工單，缸list，分群list, 指標list)，輸出(已經排入的缸號或是回收區)=======
def load_balance(_rec_test_con, _rec_target_cylinder,_rec_class, _rec_no, _rec_cy_list, _rec_group_list, _rec_ptr_list):
    #0115:去找出同類的class中的其他queue，看是否能排入不行的話，就直接往上一階去排
    same_group_len=0
    same_group_len=len(_rec_group_list[_rec_class])#計算出同一類的class中的數量
    global _target_cylinder_group
    _target_cylinder_group=()
    _target_cylinder_group=_rec_group_list[_rec_class]#取出同一類的tuple
    _target_cylinder_group_list=[]
    _target_cylinder_group_list=list(_target_cylinder_group)#換成list後，取出對應的tuple元素
    
    index=0
    global counter#紀錄每一次的分類找尋數目
    counter=0
    for index in range(same_group_len):
        _temp_same_class_cy_no=''
        _temp_same_class_cy_no=_target_cylinder_group_list[index]
        if _temp_same_class_cy_no==_rec_target_cylinder:#同為一個，則是跳過
            counter+=1
            continue
        else:
            #不同的話，則是去看同類的缸是否queue已滿: (全部缸list，排缸號)
            if check_queue_is_fulled_or_not(_rec_cy_list,_temp_same_class_cy_no):
                counter+=1
                if counter<=same_group_len:#全滿則是跳出
                    break
            else:#沒有的話，則是塞入同類的缸中
                print('排入同類其他缸號中........')
                if check_queue_is_fulled_or_not(_rec_cy_list,_temp_same_class_cy_no):
                
                    if put_the_target_class_cylinder(_rec_no,_rec_test_con,_temp_same_class_cy_no, _rec_cy_list):
                        print('已排入同類缸 %s中' %_temp_same_class_cy_no)
                else:

                    print('%s 號缸已滿'%_temp_same_class_cy_no)
                    print('%s 工單未排入同類缸%s 號缸中' %(_rec_no,_temp_same_class_cy_no))
    #========================================================
    #回傳同類缸也是滿的，則是往下跳到cy_group_list，去找出下一層的缸，去進行排缸
    global up_level_class_ptr,up_level_class_list,_target_ptr
    up_level_class_ptr=_rec_class
    up_level_class_ptr=_rec_class+1#往下一層
    print('同類缸已滿，往下一層找尋......')
    
    #找出目前下一層的ptr出來
    up_level_group_size=len(_rec_group_list[up_level_class_ptr])
    _target_ptr=_rec_ptr_list[up_level_class_ptr]
    if _target_ptr>=up_level_group_size:
        _target_ptr=_target_ptr % up_level_group_size
        _rec_ptr_list[up_level_class_ptr]=_target_ptr
    
   
    if up_level_class_ptr<=len(_rec_group_list):
        #去挑出一個cylinder出來
        up_level_group=()
        up_level_group=_rec_group_list[up_level_class_ptr]
        up_level_class_size=0
        up_level_class_size=len(up_level_group)#算出裡面的個數
        up_level_class_list=list(up_level_group)#換成List比較好操作
       
        _target_cylinder_no=''
        _target_cylinder_no=up_level_class_list[_target_ptr]#找出下一層缸號碼
        if not check_queue_is_fulled_or_not(_rec_cy_list,_target_cylinder_no):#檢查是否此下一層缸號已滿
            print('%s 工單塞入下一層, 第%s層第%s號缸' %(_rec_no,up_level_class_ptr,_target_cylinder_no))
            put_the_target_class_cylinder(_rec_no,_rec_test_con,_target_cylinder_no, _rec_cy_list)#放入相對應的缸中
            #0115: 更新PTR，且放回PTR list中，給下一次排缸使用
            _target_ptr+=1
            _target_ptr=_target_ptr % len(up_level_class_list)
            _rec_ptr_list[up_level_class_ptr]=_target_ptr
    else:#都沒有，則是放入到暫時區(要依靠手工去排缸決定)
        _tuple=()
        _tuple+=(_rec_no,)
        _tuple+=(_rec_test_con,)
        recyce_list.append(_tuple)
#============0115:排入同類缸中: 輸入(工單號，濃度值，缸號，缸list)，輸出(排入為True，沒有排入為False)
def put_the_target_class_cylinder(_no, _con, _cy_no, _pro_list):
    result=False
    cysize=0
    cysize=len(_pro_list)
    index=0
    for index in range(cysize):
        _temp_no=''
        _temp_no=_pro_list[index].get_cylinder_no()
        if _temp_no==_cy_no:
            _temp_tuple=()
            _temp_tuple+=(_no,)
            _temp_tuple+=(_con,)
            _temp_jobnocon_list=_pro_list[index].get_cylinder_con_list()
            _temp_jobnocon_list.append(_temp_tuple)
            _pro_list[index].set_cylinder_con_list(_temp_jobnocon_list)
            result=True
            break
        else:
            continue
    return result
#============檢查是否同類的缸號也是被排滿: 輸入(缸List，工單號)，輸出(是否有排入queue中，有為True，沒有為False)
def check_queue_is_fulled_or_not(_cy_list, _rec_cy_no):    
    check_result=False
    list_size=0
    list_size=len(_cy_list)
    index=0
    for index in range(list_size):
        _temp_cy_no=''
        _temp_cy_no=_cy_list[index].get_cylinder_no()
        if _temp_cy_no==_rec_cy_no:
            _temp_queue_full_or_not=False
            _temp_queue_full_or_not=_cy_list[index].get_queue_full_flag()
            if _temp_queue_full_or_not==True:
                check_result=True
                break
        else:
            continue
    return check_result
#==================0131: 按鈕動作: 出對應的類別List中，所有工單資料排序濃度後，依照淡  中  濃去 進行分類與分缸，如果缸號數量不夠分三缸，則是把淡  跟  中 混合成一缸，輸入(缸list，缸指標List，缸分類list)，輸出(已經排好的第二次 淡 中  深缸號)
def groupList_to_clear_cylinder_jobtask(_pro_job_list, _cylinder_ptr_list, _cy_gp_list):
    index=0
    global _rec_gp_list#分類cy_group_list集合
    _rec_gp_list=[]
    _rec_gp_list=_cy_gp_list
    
    group_list_size=0
    group_list_size=len(_rec_gp_list)

    index=0
    for index in range(len(_pro_job_list)):
        _pro_job_list[index].set_queue_full_flag(False)
        
    for index in range(group_list_size):
        start_to_schedule_clear_cylinder_jobtask(_pro_job_list, _cylinder_ptr_list,_rec_gp_list[index])
#=========0131: 啟動第二次排序，進行最小清缸合併動作: 輸入(缸list，缸指標List，缸分類list)，輸出(已經排好的第二次 淡 中  深缸號)    
def start_to_schedule_clear_cylinder_jobtask(_pro_joblist,_cy_ptr_list, _cy_group_list):
    print('最小清缸排序動作啟動......................')
    global _cy_group_index_size
    global _total_jobtasks_list
    _total_jobtasks_list=[]
    _cy_group_index_size=0
    _cy_group_index_size=len(_cy_group_list)#去找出分類中的長度有多少: ex[(14,32)] 長度為2

    index,j,k=0,0,0
    
    for index in range(_cy_group_index_size):
        _temp_list_no=''
        _temp_list_no=_cy_group_list[index]
        #0212:測試code
        #_temp_list_no=_cy_group_list[4][index]
        
        for j in range(len(_pro_joblist)):
            #找到相關缸號
            if _temp_list_no==_pro_joblist[j].get_cylinder_no():
               _temp_list=[]
               #取得已經排入的list_job工單   
               _temp_list=_pro_joblist[j].get_cylinder_con_list()        
               for k in range(len(_temp_list)):
                   _temp_tuple=()
                   _temp_tuple=_temp_list[k]
                   _total_jobtasks_list.append(_temp_tuple)
            else:
                continue
    #=================進行濃度: 淺 中 深 排序=====
    global sorted_scheduled_list
    sorted_scheduled_list=[]
    sorted_scheduled_list=sorted(_total_jobtasks_list, key=operator.itemgetter(1))
    #=================排序完 依照濃度0~0.5, 0.5~1.0, 1.1~up
    #=================取出相關濃度值，填入對應List中
    index,j,k=0,0,0
    for index in range(_cy_group_index_size):
        _temp_list_no=''
        _temp_list_no=_cy_group_list[index]
        #0212:測試code
        #_temp_list_no=_cy_group_list[4][index]
    
        for j in range(len(_pro_joblist)):
            if _temp_list_no==_pro_joblist[j].get_cylinder_no():#找到相關缸號
               _temp_list=[]
               if _cy_group_index_size<3:#小於淺中深色三種，則是把淺跟中排一起，深色獨立一缸
                  if _cy_group_index_size==2:#剛好兩缸，則是分為淺中唯一缸，深色一缸
                  
                    print('目前個數為....%s個' %str(_cy_group_index_size))
                    if index==0:#淺與中色為一缸
                        _tmp_list=[]
                        _tmp_list=[i for  i in sorted_scheduled_list if i[1]<1.1]#把淺與中色抓出來為同一List
                        _pro_joblist[j].set_cylinder_con_list(_tmp_list)
                        if len(_pro_joblist[j].get_cylinder_con_list())==8:
                            _pro_joblist[j].set_queue_full_flag(True)
                    else:#排入深色一缸
                        _tmp_list=[]
                        _tmp_list=[i for  i in sorted_scheduled_list if i[1]>1.1]#把深色抓出來為同一List
                        _pro_joblist[j].set_cylinder_con_list(_tmp_list)
                        if len(_pro_joblist[j].get_cylinder_con_list())==8:
                            _pro_joblist[j].set_queue_full_flag(True)
                  else:#只有一缸 則是全部塞入
                        print('目前個數為....%s個' %str(_cy_group_index_size))
                        _tmp_list=[]
                        _tmp_list=[i for  i in sorted_scheduled_list]
                        _pro_joblist[j].set_cylinder_con_list(_tmp_list)
                        if len(_pro_joblist[j].get_cylinder_con_list())==8:
                            _pro_joblist[j].set_queue_full_flag(True)
    
               elif _cy_group_index_size>3: #大於三缸#0125: thinking..............
                    #pass
                    #目前想法，各自去算出濃度的資料筆數，選擇相關缸數去填入。
                    #0129:如果大於三缸(如4, 5, ....以上)，則是依照濃度比數，去計算所需要的缸數，算出後
                    #填入相關缸中，如果滿缸，則是設定相關旗標，如果全滿，則是填入recylist中。
                    print('目前個數為....%s個' %str(_cy_group_index_size))
                    global _light_color_list, _mid_color_list, _deep_color_list
                    global _light_len,_mid_len, _deep_len
                    _light_color_list, _mid_color_list, _deep_color_list=[],[],[]
                    _light_len,_mid_len, _deep_len=0,0,0
                    
                    _light_color_list=[i for  i in sorted_scheduled_list if  i[1]<0.5]
                    _mid_color_list=[i for  i in sorted_scheduled_list if 1.0>i[1]>=0.5]
                    _deep_color_list=[i for  i in sorted_scheduled_list if i[1]>1.1]
                    
                    _light_len=len(_light_color_list)
                    _mid_len=len(_mid_color_list)
                    _deep_len=len(_deep_color_list)
                    
                    global _light_mod, _mid_mod, _deep_mod
                    _light_mod, _mid_mod, _deep_mod=0,0,0
                    _l_index, _m_index, _d_index=0,0,0
                    
                    
                    global _l_quot, _m_quot, _d_quot,_l_remin, _m_remin, _d_remin
                    _l_quot, _m_quot, _d_quot, _l_remin, _m_remin, _d_remin=0,0,0,0,0,0
                   
                    
                    _l_remin=_light_len % 8  
                    _m_remin=_mid_len % 8
                    _d_remin=_deep_len % 8
                    
                    _l_quot=_light_len/8
                    _m_quot=_mid_len/8
                    _d_quot=_deep_len/8
                    
                    
                    
                    
                    if _l_remin>0:
                        _light_mod=int(_l_quot)+1
                    else:
                        _light_mod=int(_l_quot)
                        
                    if _m_remin>0:
                        _mid_mod=int(_m_quot)+1
                    else:
                        _mid_mod=int(_m_quot)
                        
                    if _d_remin>0:
                        _deep_mod=int(_d_quot)+1
                    else:
                        _deep_mod=int(_d_quot)    
                            
                    
                    if index<_light_mod:
                        
                        if _pro_joblist[j].get_queue_full_flag()!=True:#淺色缸範圍
                            _tmp_list=[]
                            _tmp_list=[h for  h in sorted_scheduled_list[(index)*8:(index+1)*8]]
                            #去抓出[0:8],[8:16] 等相關的元素至list中
                            if len(_tmp_list)==8:
                                    _pro_joblist[j].set_cylinder_con_list(_tmp_list)
                                    _pro_joblist[j].set_queue_full_flag(True)
                            else:
                                    _pro_joblist[j].set_cylinder_con_list(_tmp_list)
                    elif index>=_light_mod and index<(_light_mod+_deep_mod-1):#中色缸範圍
                         
                        if _pro_joblist[j].get_queue_full_flag()!=True:
                            _tmp_list=[]
                            _tmp_list=[q for  q in sorted_scheduled_list[(index)*8:(index+1)*8]]
                            #去抓出[0:8],[8:16]，相關的元素至list中
                            if len(_tmp_list)==8:
                                    _pro_joblist[j].set_cylinder_con_list(_tmp_list)
                                    _pro_joblist[j].set_queue_full_flag(True)
                            else:
                                    _pro_joblist[j].set_cylinder_con_list(_tmp_list)
                    else:
                             
                        if _pro_joblist[j].get_queue_full_flag()!=True:
                            _tmp_list=[]
                            _tmp_list=[p for  p in sorted_scheduled_list[(index)*8:(index+1)*8]]
                            #去抓出[0:8],[8:16]，相關的元素至list中
                            if len(_tmp_list)==8:
                                    _pro_joblist[j].set_cylinder_con_list(_tmp_list)
                                    _pro_joblist[j].set_queue_full_flag(True)
                            else:
                                    _pro_joblist[j].set_cylinder_con_list(_tmp_list)
                    
               else: #剛好三缸，利用不同index值，去抓去各三缸所需要的工單號與濃度，填入List中 
                    #pass
                    if index==0:#淺色為一缸
                        _tmp_list=[]
                        _tmp_list=[i for  i in sorted_scheduled_list if i[1]<0.5]#把色抓出來為同一List
                        _pro_joblist[j].set_cylinder_con_list(_tmp_list)
                        if len(_pro_joblist[j].get_cylinder_con_list())==8:
                            _pro_joblist[j].set_queue_full_flag(True)
                    elif index==1:#排入中色一缸
                        _tmp_list=[]
                        _tmp_list=[i for  i in sorted_scheduled_list if 1.0>i[1]>=0.5]#把中色抓出來為同一List
                        _pro_joblist[j].set_cylinder_con_list(_tmp_list)
                        if len(_pro_joblist[j].get_cylinder_con_list())==8:
                            _pro_joblist[j].set_queue_full_flag(True)
                    else:
                        _tmp_list=[]
                        _tmp_list=[i for  i in sorted_scheduled_list if i[1]>1.1]#把深色抓出來為同一List
                        _pro_joblist[j].set_cylinder_con_list(_tmp_list)
                        if len(_pro_joblist[j].get_cylinder_con_list())==8:
                            _pro_joblist[j].set_queue_full_flag(True)

            else:
                continue
    #=======================
    #0124: 這邊再重新顯視一次     
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[0].get_cylinder_con_list(),cy1_schedule)
    
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[1].get_cylinder_con_list(),cy2_schedule)
    
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[2].get_cylinder_con_list(),cy3_schedule)
    
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[3].get_cylinder_con_list(),cy5_schedule)
    
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[4].get_cylinder_con_list(),cy6_schedule)
   
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[5].get_cylinder_con_list(),cy7_schedule)
   
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[6].get_cylinder_con_list(),cy8_schedule)
  
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[7].get_cylinder_con_list(),cy9_schedule)

    print_scheduled_jobno_for_each_cylinder(_pro_joblist[8].get_cylinder_con_list(),cy10_schedule)

    print_scheduled_jobno_for_each_cylinder(_pro_joblist[9].get_cylinder_con_list(),cy11_schedule)

    print_scheduled_jobno_for_each_cylinder(_pro_joblist[10].get_cylinder_con_list(),cy12_schedule)

    print_scheduled_jobno_for_each_cylinder(_pro_joblist[11].get_cylinder_con_list(),cy13_schedule)
    
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[12].get_cylinder_con_list(),cy14_schedule)
    
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[13].get_cylinder_con_list(),cy15_schedule)
    
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[14].get_cylinder_con_list(),cy16_schedule)
    
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[15].get_cylinder_con_list(),cy17_schedule)
    
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[16].get_cylinder_con_list(),cy18_schedule)
    
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[17].get_cylinder_con_list(),cy19_schedule)
    
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[18].get_cylinder_con_list(),cy20_schedule)
    
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[19].get_cylinder_con_list(),cy21_schedule)
    
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[20].get_cylinder_con_list(),cy22_schedule)
    
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[21].get_cylinder_con_list(),cy23_schedule)
    
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[22].get_cylinder_con_list(),cy24_schedule)
   
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[23].get_cylinder_con_list(),cy25_schedule)
    
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[24].get_cylinder_con_list(),cy26_schedule)
    
    print_scheduled_jobno_for_each_cylinder(_pro_joblist[25].get_cylinder_con_list(),cy32_schedule)           
    print('最小清缸排序動作已完成......................')
#================0131:去queue中，找尋相同工單，找到相同工單後回傳true，否則回傳False，目前已經停用此函式
def find_the_same_jobno_from_queue(_rec_queue, _rec_no):
    _result=FALSE
    size_queue=0
    size_queue=_rec_queue.size()
    index=0
    for index in range(0, size_queue):
        _desired_no=''
        _desired_no=_rec_queue.deq()
        if _rec_no==_desired_no:
            _result=True
            break
        else:
            _rec_queue.enq(_desired_no)
            continue
    return _result
#===============0131:去list中，找尋相同工單，找到相同工單後回傳true，否則回傳False: 輸入(工單List，工單號)，輸出(找到為True，找不到為False)
def find_the_same_jobno_in_queue(_recycle_list, _rec_job_no):
     global found
     found=False
     size=0
     size=len(_recycle_list)
     index=0
     for index in range(size):
         temp_no=""
         temp_no=_recycle_list[index]
         if _rec_job_no==temp_no:
             found=True
             break
         else:
             continue
     return found
#=========0131:檢查目前未排入缸號的工單號碼 並顯示於listbox_temp區域中: 輸入(工單List，顯示區域)，輸出(已經顯示的文字)
def show_no_schedule_jobno(_r_list, listbox):
    clear_listbox_content(listbox)
    size=len(_r_list)
    index=0
    i=0
    if size==0:
        listbox.insert(i,"")
    else:
        for i in range(0,size):
                temp_no_str=""
                temp_no_str=_r_list[i]
                listbox.insert(i,temp_no_str)
#============檢查缸中同一天內工單數目L :輸入(工單list，缸中queue，工單號)，輸出(輸出數字為找到同一天的工單數目)
def check_eachday_jobtask_num(_r_list, _r_cylist_queue,_tmp_no):
   global tmp_list
   #queue物件取出轉成list去進行運算
   tmp_list=[]
   i=0
   for i in range(_r_cylist_queue.size()):
       _tmp_object=""
       _tmp_object=_r_cylist_queue.deq()
       tmp_list.append(_tmp_object)
       _r_cylist_queue.enq(_tmp_object)
   _tmp_date=""
   _tmp_date=search_opendate_from_jobist(_r_list,_tmp_no)
   _tmp_cmp_date=""
   j=0
   _tmp_cmp_result=0
   _tmp_cmp_counter=0
   for j in range(len(tmp_list)):
        _tmp_cmp_no=""
        _tmp_cmp_no=tmp_list[j]
        _tmp_cmp_date=search_opendate_from_jobist(_r_list,_tmp_cmp_no)
        _tmp_cmp_result=compute_day_diff(_tmp_date,_tmp_cmp_date)
        if _tmp_cmp_result<=1:
            _tmp_cmp_counter+=1
        else:
            continue
   return _tmp_cmp_counter
#=============比對時間差距 回傳幾天: 輸入(比較天1，比較天2)，輸出(差距天數)============
def compute_day_diff(_rec_date1,_rec_date2):
    #TIMEFORMAT="%Y-%m-%d %H:%M:%S:%f"
    TIMEFORMAT_WITHOUT_MS="%Y-%m-%d %H:%M:%S"#0105 mark: 因為有的資料庫欄位沒有milisecon
    time1=datetime.strptime(_rec_date1, TIMEFORMAT_WITHOUT_MS)
    time2=datetime.strptime(_rec_date2, TIMEFORMAT_WITHOUT_MS)
    diff_time=time1-time2
    _diff_days=int(diff_time.total_seconds()/86400)#相差幾天(目前是一天內)
    return _diff_days
#=============從list中找出對應工單號碼的開卡日期: 輸入(工單List，工單號碼)，輸出(開卡日)============
def search_opendate_from_jobist(_target_list, _target_jobno):
    global _rec_date
    _rec_date=""
    list_size=0
    list_size=len(_target_list)
    index=0
    for index in range(list_size):
        _find_no=""
        _find_no=_target_list[index][0]
        if _find_no==_target_jobno:
            _rec_date=_target_list[index][1]
            break
    return _rec_date
#=============#0111:根據濃度值來進行排序，依序由淡--->濃列印出來相關工單號: 輸入(工單List，顯示區域)，輸出(顯示文字或是結果)
def print_scheduled_jobno_for_each_cylinder(_rec_list, listbox):
    clear_listbox_content(listbox)
    if len(_rec_list)>0:
        size=len(_rec_list)
        i=0
        j=0
        listbox_list=[]
        if size==0:
            listbox.insert(i,"目前無暫時代排工單")
        else:
            _rec_list.sort(key=lambda tup: tup[1])
            for i in range(0,size):
                temp_no_str=""
                temp_no_str=_rec_list[i]
                listbox.insert(i,temp_no_str)
#===========列印出來每個缸裡面的queue所有工單號碼: 輸入(要顯示的queue，顯示區域)，輸出(顯示文字或是結果)
def print_each_cylinder_queue(_rec_queue, listbox):
    clear_listbox_content(listbox)
    temp_queue=queue()
    temp_queue=_rec_queue
    size=temp_queue.size()
    i=0
    j=0
    listbox_list=[]
    if size==0:
        listbox.insert(i,"")
    else:
        for i in range(0,size):
            temp_no_str=""
            temp_no_str=temp_queue.deq()#先從queue取出
            listbox.insert(i,temp_no_str)
            #listbox.itemconfig(i, bg='green', fg='white')#字體可變換顏色
            i+=1
            temp_queue.enq(temp_no_str)#填回原本queue中
#===========cy1選擇到的event函式: 輸入(輸入的事件)，輸出(所點選的物件為何(工單號))
def cy1_select_trigger(evt):
    try:
        w = evt.widget
        if w.curselection():
            index = int(w.curselection()[0])
            value = w.get(index)
            print('You selected item %d: "%s"' % (index, value))
    except IndexError:
        print("IndexError")
#========listbox_temp暫存區去點選: 輸入(輸入的事件)，輸出(所點選的物件為何(工單號))
def listbox_temp_select(evt):
    try:
        temp_work = evt.widget
        if temp_work.curselection():
            index = int(temp_work.curselection()[0])
            value = temp_work.get(index)
            print("You selected item %d: %s" % (index, value))
    except IndexError:
        print("選擇"%value+"時的Index錯誤")
#========這邊為相關缸號來進行手動刪除排程工單: 輸入(被刪除物件的顯示區域，被新增物件的顯示區域，缸號碼)，輸出(重新顯示被刪除物件的QUEUE內容)
def cy_btn_delete(listbox,_rec_listbox,_rec_cy_name):
    global temp_list
    temp_list=[]
    global tuple
    tuple=()
    _found_cy_index=0
    _rec_cy_number=int(_rec_cy_name[-1:])#取得目前每缸編號
    #帶入編號，找出對應index number:提供後續queue
    _found_cy_index=find_cylinder_index_from_cyno(_rec_cy_number,processing_cylinder_list)
    try:
        if listbox.curselection():
            index=int(listbox.curselection()[0])
            value=listbox.get(index)
            print("%s 已經被刪除" % value)
    except IndexError:
            print("刪除" % value+"時的Index錯誤")
    #------------刪除後跑至顯示於listbox_temp視窗中------
    tuple+=(value,)
    temp_list.append(tuple)
    i=0
    for i in range(len(temp_list)):
        _rec_listbox.insert(i,temp_list[i][0])
    j=0
    #-------------從queue中移除-------------------------
    size=0
    size=processing_cylinder_list[_found_cy_index].queue.size()
    
    for j in range(0,size):
            temp_cmp_str=""
            temp_cmp_str=processing_cylinder_list[_found_cy_index].queue.deq()
            if value==temp_cmp_str:
                break
            else:
                processing_cylinder_list[_found_cy_index].queue.enq(temp_cmp_str)
            j+=1
    #-----重新顯示queue內容---------------
    print_each_cylinder_queue(processing_cylinder_list[_found_cy_index].queue,listbox)
#======================輸入相關缸號碼號，去找出對應的index為何: 輸入(找尋的缸號碼，缸list)，輸出(缸索引編號)
def find_cylinder_index_from_cyno(_rec_number, _rec_cylinder_list):
    found_number=0
    size=0
    size=len(_rec_cylinder_list)
    i=0
    for i in range(size):
        temp_index=0
        temp_index=int(_rec_cylinder_list[i].get_cylinder_no())
        if _rec_number==temp_index:
            found_number=i
        else:
            continue
    return found_number
#===========================按鈕動作: 這邊去手動新增排程工單: 輸入(被刪除物件的顯示區域，被新增物件的顯示區域，缸號碼)，輸出(重新顯示被新增物件的QUEUE內容)
def cy_btn_add(listbox,_rec_listbox,_rec_cy_name):
    global temp_add_list
    temp_add_list=[]
    global tuple
    tuple=()
    _rec_cy_number=int(_rec_cy_name[-1:])#取得目前每缸編號
    _found_cy_index=0
    #帶入編號，找出對應index number
    _found_cy_index=find_cylinder_index_from_cyno(_rec_cy_number,processing_cylinder_list)
    index=int(_rec_listbox.curselection()[0])
    value=_rec_listbox.get(index)
    _rec_listbox.delete(index)
    print("%s 已經被新增" % value)
    #----------------新增回原來的queue---------
    processing_cylinder_list[_found_cy_index].queue.enq(value)
    #寫回目前對應的cylinder中的queue內
    print_each_cylinder_queue(processing_cylinder_list[_found_cy_index].queue,listbox)
#==========================更新treeview的內容: 輸入(tree顯示區域)，輸出(顯示文字)
def clear_tree_content(tree):
    x =tree.get_children() 
    for item in x: 
        tree.delete(item)
#========================啟動button功能: 輸入(按鈕名稱)，輸出(啟用該按鈕功能)
def enable_button(button):
    button.config(state=tkinter.NORMAL)
#========================關閉button功能: 輸入(按鈕名稱)，輸出(關閉該按鈕功能)
def disable_button(button):
    button.config(state=tkinter.DISABLED)
#========================程式離開: 輸入(tkinter父物件)，輸出(關閉視窗或是母程式)
def exit(tk):
    tk.destroy()
#========================清除text內顯示內容: 輸入(顯示區域)，輸出(清除顯示區域內文字)
def clear_text_content(text):
    text.delete(1.0, tkinter.END)
#========================清除listbox內容: 輸入(顯示區域)，輸出(清除顯示區域內文字)
def clear_listbox_content(listbox):
    listbox.delete(0, tkinter.END)
#===============================
#點選optionMenu後，可以選擇相關缸號，去載入相關排程工單資訊進入
#且去判斷目前的缸號狀態，建議可以排入其他缸號
#===============缸號選擇menu變動時，載入相關缸號內部的內容: 輸入(點選項目，工單List，顯示區域，pop視窗區域)，輸出(顯示目前pop視窗內的清單內容)
def cy_selection_changed(_rec_select,_rec_tmp_list,_show_listbox, _pop_rec_info_listbox):
    clear_listbox_content(_show_listbox)
    clear_listbox_content(_pop_rec_info_listbox)
    print("第"+_rec_select.get()+"缸已點選")
    
    size=0
    size=len(_rec_tmp_list)
    global _rec_number
    _rec_number=str(_rec_select.get())

    #0207:修正手排點選載入錯誤資料
    global _tmp_list
    _tmp_list=[]

    i=0
    for i in range(size):
        temp_index=0
        temp_index=str(_rec_tmp_list[i].get_cylinder_no())
        if _rec_number==temp_index:
            _tmp_list=_rec_tmp_list[i].get_cylinder_con_list()#取得list資料
            break

    print_scheduled_jobno_for_each_cylinder(_tmp_list,_show_listbox)
    #======================================
    #_pop_rec_info_listbox.insert(1,)#清除文字
    pop_rec_info_list=[]
    pop_index=0
    for pop_index, listbox_entry in enumerate(_show_listbox.get(0, END)):
         pop_rec_info_list.append(_show_listbox.get(pop_index))
    size=len(pop_rec_info_list)
    if size>7:
        _pop_rec_info_listbox.insert(1,"此缸排入太多工單，請用手動排缸調整工單，謝謝")
        #global blink_id
        # blink_id=blink(_pop_rec_info_listbox, "green", "red")
        
    else:
        #blink(_pop_rec_info_listbox, "white", "white",False)
        #blink(_pop_rec_info_listbox, "green", "red")
        #cancel_blink()
       # cancel_blink(blink_id)
       # cancel_blink(_pop_rec_info_listbox,blink_id)
        _pop_rec_info_listbox.insert(1,"目前此缸狀態正常。")  
        
       # blink(_pop_rec_info_listbox, "white", "white")
#==============================原本想做閃動提示:可看你時間去研究開發
 # def flash(self,count):
 #        bg = self.cget('background')
 #        fg = self.cget('foreground')
 #        self.configure(background=fg,foreground=bg)
 #        count +=1
 #        if (count < 31):
 #             self.after(1000,self.flash, count) 
#=============文字閃動效果==========原本想做閃動提示:可看你時間去研究開發
def blink(l,f1, f2):
    #if _rec_bool:
    _rec_blink_id=''
    
    if l["bg"] == f1:
        l["bg"] = f2
    else:
        l["bg"] = f1
        
    _rec_blink_id=l.after(1000, blink, l, f1, f2)
    #_rec_blink_id=_rec_blink_id[-2:]
    print('blink id  %s' %_rec_blink_id) 
    #else:
    #l.after_cancel(blink_id)
    return _rec_blink_id
def cancel_blink(l,_rec_id):
   l.after_cancel(_rec_id)
#======popup跳出視窗顯示相關區域, 輸入(_cur_cy_listbox:排程區域，_rec_cyno: 排程缸號，_rec_pro_list:排程cylinder list)，輸出(顯示出POP目前的缸號內容與工單、濃度值排序結果)
def Popup(_cur_cy_listbox,_rec_cyno,_rec_pro_list):
    global pop_tk
    
    pop_tk = tkinter.Tk()
    pop_tk.title('工作排程')

    label1=ttk.Label(pop_tk, text="工單顯示區",font="24")
    label1.pack()

    frame1 = tkinter.Frame(pop_tk, width=950, height=400)
    frame1.pack()
    
    pop_label= ttk.Label(pop_tk, text="排程工單區域(目前缸號)")
    pop_label.place(x=25, y=20,width=200, height=30)
    
    pop_left_status=Listbox(pop_tk)
    pop_left_status.place(x=20, y=50, width=200,height=220)
    
    
    pop_label2= ttk.Label(pop_tk, text="欲轉換的缸號(來源缸號)")
    pop_label2.place(x=550, y=20,width=200, height=30)
    
    pop_right_status=Listbox(pop_tk)
    pop_right_status.place(x=550, y=50, width=200,height=220)
    
    #===================================
    pop_recommdation_info=Listbox(pop_tk)
    pop_recommdation_info.place(x=500, y=280, width=420,height=60) 
    pop_recommdation_info.insert(1,"尚未有相關此缸號狀態")
    #==================================放在POP-uP訊息槽下面，用來推薦相關目前缸狀態
    global move_to_right_select
    move_to_right_select=""
    
    #這邊要去抓list_temp暫存區這邊未排入的資料
    cylinder_options = ["1","2","3","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","32","100"]
    variable = StringVar(pop_tk)
    variable.set(cylinder_options[0]) # default value
    #1117:這邊必須要把自己當成參數傳過去(variable)，才能觸發cy_selection_changed 去抓目前所點選的選項
    variable.trace("w", lambda *args: cy_selection_changed(variable,_rec_pro_list,pop_right_status, pop_recommdation_info))
    #cy_selection_changed("1",processing_cylinder_list,pop_right_status)

    w = OptionMenu(pop_tk, variable, *cylinder_options)
    w.place(x=450, y=50, width=100,height=40)
    
    #搬移到右邊: 可看你開發時間去COPY此方式開發
    #move_to_right_queue
    #move_to_right_queue= ttk.Button(pop_tk, text=">>",command = lambda *args:  move_to_left(variable,_cur_cy_listbox, pop_left_status,pop_right_status,_rec_cyno,_rec_pro_list))
    #move_to_right_queue.place(x=280, y=90, width=150,height=40)
    #搬移到左邊
    #Move_to_left_queue
    move_to_left_queue= ttk.Button(pop_tk, text="<<搬移至左邊缸號",command =lambda *args:  move_to_left(variable,_cur_cy_listbox, pop_left_status,pop_right_status,_rec_cyno,_rec_pro_list))
    move_to_left_queue.place(x=280, y=150, width=180,height=40)
    
    #---------左手邊內容區: 主要用來顯示相關排程相關資料 方便手動調整
    global _listbox_list
    _listbox_list=[]
    j=0
    #去抓右手邊區域cyX-schedule裏面的顯示資料
    for j, listbox_entry in enumerate(_cur_cy_listbox.get(0, END)):
        _listbox_list.append(_cur_cy_listbox.get(j))
    #----------------------------------------------------
    #顯示出來在左邊pop_left_status中
    i=0
    for i in range(len(_listbox_list)):
        pop_left_status.insert(i, _listbox_list[i])
    #----------------------------------------------------
    button9 = ttk.Button(pop_tk, text="離開",command=pop_tk.destroy)
    button9.place(x=300, y=350, width=150,height=40)
    
    pop_tk.mainloop()
#=====================去從接收到的cyno缸號，去找出相關內部的queue出來: 輸入(缸號碼，缸list)，輸出(找到對應缸號碼的QUEUE內容)
def findqueue_by_cylinderno(_rec_cyno, _tar_list):
    found_queue=None
    found_queue=queue()
    tmep_no=0
    index=0
    size=len(_tar_list)
    for index in range(0, size):
        tmep_no=_tar_list[index].get_cylinder_no()
        if tmep_no==_rec_cyno:
            found_queue=_tar_list[index].get_dying_queue()
            break
        else:
            continue
    
    return found_queue
#==============從jobno工單號去找尋目前排入的缸號名稱: 輸入(工單號)，輸出(顯示區域中的缸號碼)
def search_cylinder_name_from_jobno(_target_jobno):
    cy_name=None
    global cyname_list
    if _target_jobno=="1":
        cy_name=cy1_schedule
    elif _target_jobno=="2":
        cy_name=cy2_schedule
    elif _target_jobno=="3":
        cy_name=cy3_schedule
    elif _target_jobno=="5":
        cy_name=cy5_schedule
    elif _target_jobno=="6":
        cy_name=cy6_schedule
    elif _target_jobno=="7":
        cy_name=cy7_schedule 
    elif _target_jobno=="8":
        cy_name=cy8_schedule 
    elif _target_jobno=="9":
        cy_name=cy9_schedule 
    elif _target_jobno=="10":
        cy_name=cy10_schedule
    elif _target_jobno=="11":
        cy_name=cy11_schedule
    elif _target_jobno=="12":
        cy_name=cy12_schedule
    elif _target_jobno=="13":
        cy_name=cy13_schedule
    elif _target_jobno=="14":
        cy_name=cy14_schedule
    elif _target_jobno=="15":
        cy_name=cy15_schedule
    elif _target_jobno=="16":
        cy_name=cy16_schedule
    elif _target_jobno=="17":
        cy_name=cy17_schedule
    elif _target_jobno=="18":
        cy_name=cy18_schedule
    elif _target_jobno=="19":
        cy_name=cy19_schedule
    elif _target_jobno=="20":
        cy_name=cy20_schedule
    elif _target_jobno=="21":
        cy_name=cy21_schedule
    elif _target_jobno=="22":
        cy_name=cy22_schedule
    elif _target_jobno=="23":
        cy_name=cy23_schedule
    elif _target_jobno=="24":
        cy_name=cy24_schedule
    elif _target_jobno=="25":
        cy_name=cy25_schedule
    elif _target_jobno=="26":
        cy_name=cy26_schedule
    elif _target_jobno=="32":
        cy_name=cy32_schedule
    elif _target_jobno=="100":#設定為暫時缸
        cy_name=listbox_temp
    else:
        cy_name=None
    return cy_name
#============從右邊已排缸號內容，把工單號碼搬移到左邊: 輸入(選擇到的工單項目，新缸號顯示區域位置，pop視窗左手邊顯示listbox，pop視窗右手邊顯示listbox，目前缸號號碼，全部缸list)，輸出(移動到新缸後的結果)
def move_to_left(_rec_cy_select,_cy_schedule_listbox, _left_listbox, _right_listbox,_rec_cyno,_rec_list):

    global _get_right_select
    _get_right_select=_rec_cy_select.get()#0207: 取得POP視窗中，右手邊視窗(舊缸)所點選的缸號號碼
    
    if _get_right_select==str(_rec_cyno):#點選缸號與目前缸號比對
        showinfo("警告視窗","您已選擇到相同缸號，請重新選擇其他缸號，以利進行手動排程，感謝您!!")
        log.warn("選擇到相同缸號進行手動排缸")
    else:
        try:
            if  _right_listbox.curselection():
                index=int(_right_listbox.curselection()[0])
                value=_right_listbox.get(index)#0207: 取得缸號內所點選的資料值(工單號，濃度)
                _right_listbox.delete(index)
                print(value[0]+" 號工單 \n")
                #0207:原為value
                print("%s 已經被搬移至第 %s 缸\n" % (value[0], str(_rec_cyno)))
        except IndexError:
            print("IndexError 選擇")
            log.warn("IndexError: index 選擇錯誤")
        
        #------0207: 點選後跑至顯示於左邊_left_listbox視窗中------
        #------新增點選工單至到後來的缸list中: ex: 9缸List中-----------
        _rec_cy_number=int(_rec_cyno)
        _added_cy_index=0
        _added_cy_index=find_cylinder_index_from_cyno(_rec_cy_number,_rec_list)
        #0207: 新增工單號與濃度至點選缸號list中
        _moved_list=[]
        _moved_list=_rec_list[_added_cy_index].get_cylinder_con_list()
        _moved_list.append(value)
        _rec_list[_added_cy_index].set_cylinder_con_list(_moved_list)
        print("%s 已經被新增至第 %s 缸\n " % (value[0],str(_rec_cyno)))
        #==================================以上是點選入新缸List中
        print_scheduled_jobno_for_each_cylinder(_rec_list[_added_cy_index].get_cylinder_con_list(),_left_listbox)#0207:POP視窗左手邊區塊顯示移動後的工單資料與濃度
        print_scheduled_jobno_for_each_cylinder(_rec_list[_added_cy_index].get_cylinder_con_list(),_cy_schedule_listbox)#0207:顯示移動工單後新缸的Listbox值:如9缸--->13缸，這裡為13缸的listbox

        #-------------0207: 從原本的缸號中移除點選的工單號與濃度資料-------------------------
        _remove_cy_index=0
        _rec_cy_number=int(_get_right_select)#取得右手邊缸號
        _remove_cy_index=find_cylinder_index_from_cyno(_rec_cy_number,_rec_list)
        #移除點選工單後的剩下排缸工單List集合與濃度放入_swap_list中
        global _swap_list
        _swap_list=[]
        _swap_list=_rec_list[_remove_cy_index].get_cylinder_con_list()
        _swap_list=[i for i in _swap_list if i[0] !=value[0]]
    
        #0207:設定回原來的右手邊缸list中。  
        _rec_list[_remove_cy_index].set_cylinder_con_list(_swap_list)
        #0207:列印原來移動的缸listbox顯示區域
        print_scheduled_jobno_for_each_cylinder(_rec_list[_remove_cy_index].get_cylinder_con_list(),_right_listbox)
        
        #0207:取得最後移動缸號listbox名稱(也就是新缸的Listbox顯示區域)
        return_cy_name=""
        return_cy_name=search_cylinder_name_from_jobno(_get_right_select)
        if return_cy_name==None:
            print("無法取得原點選移除缸號")
            log.warn("無法取得原點選移除缸號")
        else:
            #0207:顯示相關移動後的缸號
            print_scheduled_jobno_for_each_cylinder(_rec_list[_remove_cy_index].get_cylinder_con_list(),return_cy_name)
#===========0111:根據工單號去找出對應的總濃度值: 輸入(工單號，工單list)，輸出(濃度值)
def find_the_con_from_jobtask(_rec_jobno,_jobtask_list):
    global _found_con
    _found_con=0.0
    index=0
    listsize=0
    listsize=len(_jobtask_list)
    for index in range(listsize):
        _temp_jobno=''
        _temp_con=0.0
        _temp_jobno=_jobtask_list[index][0]
        if _rec_jobno==_temp_jobno:
            _temp_con=_jobtask_list[index][11]
            _found_con=_temp_con
            break
        else:
            continue
            
    return _found_con
#===============#載入excel檔案(目前只有單一檔案): 輸入(點選的EXCEL檔案)，輸出(計算後的濃度值)，目前暫時先停用[尚未和貝斯特討論]
def open_excel_file():
    global excel_dist
    excel_dist=[]
    data_list=[]  
    filename = filedialog.askopenfilename()
    print("檔案名稱為 "+filename)
    
    global split_filename
    split_filename=""
    #===================
    xl = pd.ExcelFile(filename)
    print(xl.sheet_names)
    
    split_filename=xl.sheet_names
    global df1
    df1 = xl.parse(split_filename[0])
    #print(df1)
    excel_dist=[tuple(x) for x in df1.values]#轉換資料為tuple形態
    
    #1212去抓出濃度的部分變成tuple(工號，濃度)
    global con_list
    con_list=[]
    
    size=len(excel_dist)
    i=0
    for i in range(size):
         if excel_dist[i][5]=="染料":
             con_list.append(excel_dist[i])
    #=========================上述過濾濃度的tuple    
    global total_list
    total_list=[]
    sum_tuple=()
    temp_con=0.0
    j=0
    for j in range(0,len(con_list)):
            temp_no=""
            temp_no=con_list[j][0]
            temp_con+=con_list[j][7]
            
            if j+1<=len(con_list)-1:
                if temp_no!=con_list[j+1][0]:
                    sum_tuple+=(temp_no,)
                    sum_tuple+=(temp_con,)
                    total_list.append(sum_tuple)
                    sum_tuple=()
                    temp_con=0.0
                else:
                    continue
            else:#去算最後一個tuple(因j+1會>len(con_list)長度
                    sum_tuple+=(temp_no,)
                    sum_tuple+=(temp_con,)
                    total_list.append(sum_tuple)
                    sum_tuple=()
                    temp_con=0.0
        
#==============算出濃度總合且組成為(工單號，濃度) tuple
#====去合併從資料庫讀出來的染色工單值與總濃度工單值: 輸入(從DB中撈出的資料list，檔案List)，輸出(合併過後總濃度List)
def merge_db_and_conf_file(_rec_db_file_list, _rec_con_list_file):
    global total_con_list
    total_con_list=[]
    size_db_list=0
    size_con_list=0
    size_db_list=len(_rec_db_file_list)
    size_con_list=len(_rec_con_list_file)
    index_db=0
    index_con=0
    temp_tuple=()
    
    for index_db in range(0, size_db_list):
        temp_db_no=""
        temp_db_no=_rec_db_file_list[index_db][0]
        temp_con=0.0
        temp_tuple+=(_rec_db_file_list[index_db][0],)
        temp_tuple+=(_rec_db_file_list[index_db][1],)
        temp_tuple+=(_rec_db_file_list[index_db][2],)
        temp_tuple+=(_rec_db_file_list[index_db][3],)
        temp_tuple+=(_rec_db_file_list[index_db][4],)
        temp_tuple+=(_rec_db_file_list[index_db][5],)
        temp_tuple+=(_rec_db_file_list[index_db][6],)
        temp_tuple+=(_rec_db_file_list[index_db][7],)
        temp_tuple+=(_rec_db_file_list[index_db][8],)
        temp_tuple+=(_rec_db_file_list[index_db][9],)
        temp_tuple+=(_rec_db_file_list[index_db][10],)
      
        for index_con in range(0, size_con_list):
            temp_con_no=""
            temp_con_no=_rec_con_list_file[index_con][0]
            if temp_db_no==temp_con_no:
                temp_con=_rec_con_list_file[index_con][1]
                break
            else:
                continue
        temp_tuple+=(temp_con,)
        total_con_list.append(temp_tuple)
        temp_tuple=()
    
    showinfo("濃度檔案合併狀態","目前已合併成功")
#=====================查詢且目前儲存資料庫參數狀態與連線測試功能: 輸入(DB資料庫參數設定)，輸出(顯示目前資料庫狀態，可依照上崙資料庫IP位置去修改，此修改後的值，會存入到DB.config檔案中)
def setup_db_params_and_testing():
    global db_object
    db_object=None
    db_object=config()#建立DB物件，取得目前DB_config.ini目前檔案值
    global pop_db_tk
    pop_db_tk = tkinter.Tk()
    pop_db_tk.title('資料庫參數')

    label_db=ttk.Label(pop_db_tk, text="資料庫參數顯示區",font="24")
    label_db.pack()

    frame1 = tkinter.Frame(pop_db_tk, width=900, height=300)
    frame1.pack()
    
    pop_server_label= ttk.Label(pop_db_tk, text="server IP")
    pop_server_label.place(x=25, y=30,width=200, height=30)
    server_input = Entry(pop_db_tk)
    server_input.place(x=175, y=30,width=200, height=30)
    
    server_input.delete(0, END)
    server_input.insert(0, db_object.get_server())#載入目前最新serveIP位置
    
    pop_dbname_label= ttk.Label(pop_db_tk, text="DB名稱")
    pop_dbname_label.place(x=25, y=70,width=200, height=30)
    dbname_input = Entry(pop_db_tk)
    dbname_input.place(x=175, y=70,width=200, height=30)
    
    dbname_input.delete(0, END)
    dbname_input.insert(0, db_object.get_dbname())#載入目前最新dbname
    
    pop_user_label= ttk.Label(pop_db_tk, text="User名稱")
    pop_user_label.place(x=25, y=110,width=200, height=30)
    user_input = Entry(pop_db_tk)
    user_input.place(x=175, y=110,width=200, height=30)
    
    user_input.delete(0, END)
    user_input.insert(0, db_object.get_user())#載入目前最新user
    
    pop_pass_label= ttk.Label(pop_db_tk, text="Password名稱")
    pop_pass_label.place(x=25, y=150,width=200, height=30)
    pass_input = Entry(pop_db_tk)
    pass_input.place(x=175, y=150,width=200, height=30)
    
    pass_input.delete(0, END)
    pass_input.insert(0, db_object.get_password())#載入目前最新password
    
    
    pop_index_label= ttk.Label(pop_db_tk, text="資料庫Index序號")
    pop_index_label.place(x=25, y=190,width=200, height=30)
    index_input = Entry(pop_db_tk)#關閉編輯功能,state='disabled'
    #index_input.config(state='disabled')
    index_input.place(x=175, y=220,width=200, height=30)
    
    index_input.delete(0, END)
    index_input.insert(0, db_object.get_dbrecordindex())#1212載入目前最新資料庫筆數index
    index_input.config(state='disabled')
    #=================按鈕區===============================
    test_btn = ttk.Button(pop_db_tk, text="測試連線",command=lambda: test_db_connection(server_input.get(),dbname_input.get(),user_input.get(),pass_input.get(),test_db_output))
    #抓取到更新值後，直接呼叫物件的modify更新程式，來進行參數更新，方便去進行資料庫帳密修改作業
    test_btn.place(x=400, y=25, width=100,height=40)
    
    test_db_output = Entry(pop_db_tk)
    test_db_output.place(x=510, y=25,width=200, height=30)
    #執行第一次
    #test_db_connection(server_input.get(),dbname_input.get(),user_input.get(),pass_input.get(),test_db_output)
    
 #抓取到更新值後，直接呼叫物件的modify更新程式，來進行參數更新，方便去進行資料庫帳密修改作業    
    save_btn = ttk.Button(pop_db_tk, text="存檔",command=lambda: db_object.modify( server_input.get(),dbname_input.get(),user_input.get(),pass_input.get()))
    save_btn.place(x=250, y=280, width=150,height=40)
    
    
    exit_btn = ttk.Button(pop_db_tk, text="離開",command=pop_db_tk.destroy)
    exit_btn.place(x=450, y=280, width=150,height=40)
    
    pop_db_tk.mainloop()
#=======================#目前用來測試資料庫連線狀態使用: 輸入(伺服器名稱IP，資料庫名稱，使用者帳號，使用者密碼，顯示位置)，輸出(資料庫是否已經連線狀態)
def test_db_connection(_rec_server, _rec_dbname, _rec_user, _rec_pass,_rec_output):
     global _db_list
     global _conn_or_not
     _db_list=[]
     
     _rec_output.delete(0, END)#清除連線顯示entry
     
     showinfo("測試DB連線中","請稍候，資料庫連線測試中....")
     #_db_list=FetchDataFromSqlDB(_rec_server, _rec_dbname, _rec_user, _rec_pass)
     _conn_or_not=TestDBconnection(_rec_server, _rec_dbname, _rec_user, _rec_pass)
    # time.sleep(10)#暫停10秒
    #len(_db_list)!=0
     if _conn_or_not==True:
         _rec_output.insert(0, "目前已連線上")
         log.info("資料庫已連線")
     else:
         _rec_output.insert(0, "未連線")
         log.info("資料庫未連線")
#---------------------------------------------------------
def shutdown_manual_schedule_btns():#關閉手動排程按鈕: 輸入()，輸出(關閉各按鈕功能)
    disable_button(cy1_sta_mod_btn)
    disable_button(cy2_sta_mod_btn)
    disable_button(cy3_sta_mod_btn)
    disable_button(cy5_sta_mod_btn)
    disable_button(cy6_sta_mod_btn)
    disable_button(cy7_sta_mod_btn)
    disable_button(cy8_sta_mod_btn)
    disable_button(cy9_sta_mod_btn)
    disable_button(cy10_sta_mod_btn)
    disable_button(cy11_sta_mod_btn)
    
    disable_button(cy12_sta_mod_btn)
    disable_button(cy13_sta_mod_btn)
    disable_button(cy14_sta_mod_btn)
    disable_button(cy15_sta_mod_btn)
    disable_button(cy16_sta_mod_btn)
    disable_button(cy17_sta_mod_btn)
    disable_button(cy18_sta_mod_btn)
    disable_button(cy19_sta_mod_btn)
    disable_button(cy20_sta_mod_btn)
    disable_button(cy21_sta_mod_btn)
    
    disable_button(cy22_sta_mod_btn)
    disable_button(cy23_sta_mod_btn)
    disable_button(cy24_sta_mod_btn)
    disable_button(cy25_sta_mod_btn)
    disable_button(cy26_sta_mod_btn)
    disable_button(cy32_sta_mod_btn)
    disable_button(cy100_sta_mod_btn)
    
    
    #0118:先關手動排缸與輸出CSV按鈕
    #0305:關閉最小清缸按鈕
    disable_button(btn4_1_auto_schedule)
    disable_button(btn4_2_hand_schedule)
    disable_button(btn4_3_bad_schedule)
    disable_button(btn5_output_csv)
#========================================
def up_manual_schedule_btns():#開啟手動排程按鈕: 輸入()，輸出(開啟各按鈕功能)
    enable_button(cy1_sta_mod_btn)
    enable_button(cy2_sta_mod_btn)
    enable_button(cy3_sta_mod_btn)
    enable_button(cy5_sta_mod_btn)
    enable_button(cy6_sta_mod_btn)
    enable_button(cy7_sta_mod_btn)
    enable_button(cy8_sta_mod_btn)
    enable_button(cy9_sta_mod_btn)
    enable_button(cy10_sta_mod_btn)
    enable_button(cy11_sta_mod_btn)
    
    enable_button(cy12_sta_mod_btn)
    enable_button(cy13_sta_mod_btn)
    enable_button(cy14_sta_mod_btn)
    enable_button(cy15_sta_mod_btn)
    enable_button(cy16_sta_mod_btn)
    enable_button(cy17_sta_mod_btn)
    enable_button(cy18_sta_mod_btn)
    enable_button(cy19_sta_mod_btn)
    enable_button(cy20_sta_mod_btn)
    enable_button(cy21_sta_mod_btn)
    
    enable_button(cy22_sta_mod_btn)
    enable_button(cy23_sta_mod_btn)
    enable_button(cy24_sta_mod_btn)
    enable_button(cy25_sta_mod_btn)
    enable_button(cy26_sta_mod_btn)
    enable_button(cy32_sta_mod_btn)
    enable_button(cy100_sta_mod_btn)
#=======================================
def exeute_manual_schedule_task():#點選手動排程: 輸入()，輸出(關閉各按鈕功能與開啟手動排程)
   disable_button(btn1_read_file)#關閉相關載入與自動排程按鈕
   #disable_button(btn2_choose_file)#關閉相關載入與自動排程按鈕
   #disable_button(btn3_load_param)#關閉相關載入與自動排程按鈕
   disable_button(btn4_1_auto_schedule)#關閉相關載入與自動排程按鈕
   disable_button(btn_db_connection)#關閉相關載入與自動排程按鈕
   up_manual_schedule_btns()#開啟相關手動排程按鈕
#==========去切換各個按鈕(依序去執行相關程式流程)
def Switch_buttons(_from_btn, _to_btn):#: 輸入(來源按鈕，目的按鈕)，輸出(切換按鈕功能)
    disable_button(_from_btn)
    enable_button(_to_btn)
#1222:重新載入排程程式
def restart_scheduler():#: 輸入()，輸出(重新啟動程式)
    tk.destroy()
    scheduler_to_dyeing()
def logginf_restart(_log):#: 輸入(_log物件)，輸出(紀錄排程程式log)
    _log.info("排程程式重新啟動")
#=========================以下為主程式與介面設定==========
#-=========================frame設定區域=================
#=====0118:主程式區域，目前寫成副函式，以後可以變成副含式呼叫此主函式
def scheduler_to_dyeing():##: 輸入()，輸出(啟動主程式，目前包成副函式，以後可在其他物件中，呼叫此副函式，減少物件的浪費)
    global cy_category_list#0115:去根據碼長與缸重分類
    cy_category_list=[(20,80),(100,500),(150,500),(200,500),(300,1600),(500,3200),(1000,6400)]

    global cy_group_list#0115:目前相同碼長與缸容的分類
    cy_group_list=[(2,16,17,18),(14,32),(3,8,25),(24,),(20,21,22,23),(5,6,11,12,15,26,13,1,19,10),(9,7)]
    
    global cy_ptr_list#0115:去記錄相關目前排到缸的ptr(紀錄目前排到的cy_group_list中的index為何)
    cy_ptr_list=[0,0,0,0,0,0,0]
    
    global recyce_list#0118:為回收工單list，回收沒有排入的工單號碼與濃度值
    recyce_list=[]
    
    
    global tk
    tk = tkinter.Tk()
    tk.title('染色排缸')
    label=ttk.Label(tk, text="工單顯示區",font="20")
    label.pack()
    
    print("目前解析度為: "+str(tk.winfo_screenwidth()),str(tk.winfo_screenheight()))
   
    
    frame = tkinter.Frame(tk, width=tk.winfo_screenwidth(), height=tk.winfo_screenheight())
    frame.pack()
    #-[工單,開卡日, 訂單號,投胚量,碼重, 染色號,碼長(投胚量/碼重),機台號, 領料量,濃度(領料量/投胚量*100%), 投胚日, 總濃度]-
    #1214 新增配方表總濃度欄位
    global db_file_object
    db_file_object=None
    db_file_object=config()#建立DB物件，取得目前DB_config.ini目前檔案值
    server_name=""
    db_name=""
    user=""
    password=""
    dbindex=""
    server_name=db_file_object.get_server()
    db_name=db_file_object.get_dbname()
    user=db_file_object.get_user()
    password=db_file_object.get_password()
    #1221:新增dbindex，紀錄目前讀到第幾筆資料
    dbindex=db_file_object.get_dbrecordindex()
    global dist#資料撈取集合
    dist=[]
    #0105: 重新抓取DB資料庫值
    #這邊會去更新目前的dbindex為何，每次都會讀取30筆資料去進行排缸，避免排缸過程delay。
    #這邊可以用最下面的測試資料測試(無總濃度版本資料)
    dist=fetch_dyeing_data_with_Totolcon(server_name,db_name,user, password,dbindex,db_file_object)
    print('共抓入%s筆資料' %str(len(dist)) )
    #=====================原始資料區域(未排序)
    tree = ttk.Treeview(tk)
    #tree['show'] = 'headings'#去除第一行空白
    tree.column("#0", width=55)
    tree["columns"]=("1","2","3","4","5","6","7","8","9","10","11","12")
    tree.column("1", width=120,anchor = 'w')
    tree.column("2", width=180,anchor = 'w')
    tree.column("3", width=100)
    tree.column("4", width=60)
    tree.column("5", width=50)
    tree.column("6", width=100)
    tree.column("7", width=100)
    tree.column("8", width=40)#-60
    tree.column("9", width=100)
    tree.column("10", width=100)
    tree.column("11", width=100)
    tree.column("12", width=60)
    #==================================
    tree.heading("1", text="工單號")
    tree.heading("2", text="完成時間")
    tree.heading("3", text="訂單號碼")
    tree.heading("4", text="投胚量")
    tree.heading("5", text="碼重")
    tree.heading("6", text="色號")
    tree.heading("7", text="碼長")
    tree.heading("8", text="機台號")
    tree.heading("9", text="領料量")
    tree.heading("10", text="濃度")
    tree.heading("11", text="投胚日")
    tree.heading("12", text="總濃度")
    tree.place(x=170, y=25,width=1230, height=190)
    #=======================排序完後顯示區
    tree1 = ttk.Treeview(tk)
    #tree1['show'] = 'headings'#去除第一行空白
    tree1.column("#0", width=55)
    vsb = ttk.Scrollbar(orient="vertical",command=tree1.yview)
    tree1.configure(yscrollcommand=vsb.set)
    tree1["columns"]=("1","2","3","4","5","6","7","8","9","10","11","12")
    tree1.column("1", width=120,anchor = 'w')
    tree1.column("2", width=180,anchor = 'w')
    tree1.column("3", width=100)
    tree1.column("4", width=60)
    tree1.column("5", width=50)
    tree1.column("6", width=100)
    tree1.column("7", width=100)
    tree1.column("8", width=40)#-60
    tree1.column("9", width=100)
    tree1.column("10", width=100)
    tree1.column("11", width=100)
    tree1.column("12", width=60)
    #===================================
    tree1.heading("1", text="工單號")
    tree1.heading("2", text="完成時間")
    tree1.heading("3", text="訂單號碼")
    tree1.heading("4", text="投胚量")
    tree1.heading("5", text="碼重")
    tree1.heading("6", text="色號")
    tree1.heading("7", text="碼長")
    tree1.heading("8", text="機台號")
    tree1.heading("9", text="領料量")
    tree1.heading("10", text="濃度")
    tree1.heading("11", text="投胚日")
    tree1.heading("12", text="總濃度")
    tree1.place(x=170, y=160,width=1230, height=130)
    #==================================================
    
    global btn1_read_file
    global btn2_choose_file
    global btn2_merge_file
    global btn3_load_param
    
    global btn4_1_auto_schedule
    global btn4_2_hand_schedule
    global btn4_3_bad_schedule
    global btn_db_connection
    global btn_clean
    global btn5_output_csv
    
    btn1_read_file = ttk.Button(frame, text="#讀入測試檔案", command=lambda : [readfile_to_tree(dist,tree),Switch_buttons(btn1_read_file, btn4_1_auto_schedule)])
    btn1_read_file.place(x=5, y=0, width=150, height=40)
    
    #btn2_choose_file = ttk.Button(frame, text="2.選檔",command=lambda :open_excel_file())
    #btn2_choose_file.place(x=5, y=45, width=75,height=40)
    
    #btn2_merge_file = ttk.Button(frame, text="2.合併", command=lambda :merge_db_and_conf_file(_to_jobtask_list, total_list))
    # btn2_merge_file.place(x=80, y=45, width=75,height=40)
    #disable_button(btn2_choose_file)#1204暫時關閉選擇檔案功能(目前測試OK)
    
    #btn3_load_param = ttk.Button(frame, text="3. 載入參數",command=lambda: setup_cylinder_params(text4))
    #btn3_load_param.place(x=5, y=90, width=150,height=40)
    
    
    #1110 此部分已經改為利用button4點一下執行排序跟排缸動作
    btn4_1_auto_schedule = ttk.Button(frame, text="#自動排缸", command=lambda: [compare(jobtask_list, tree1), ProcessSchedulingOnCylinders(cylinder_list, jobtask_list),Switch_buttons(btn4_1_auto_schedule,btn4_3_bad_schedule)])
    btn4_1_auto_schedule.place(x=5, y=45, width=150,height=40)
    #1214原為jobtask_list, 後改total_con_list
    #0105改為jobtask_list
    
    
    
    #1110 此部分已經改為利用button4點一下執行排序跟排缸動作
    # btn4_1_1_loadbalance_schedule = ttk.Button(frame, text="排缸平衡", command=lambda: load_schedule_balance(cy_ptr_list, cylinder_list,jobtask_list))
    # btn4_1_1_loadbalance_schedule.place(x=95, y=135, width=75,height=40)
    #1214原為_to_jobtask_list,後來為total_con_list
    #0105 改為 _to_jobtask_list
    
    btn4_2_hand_schedule = ttk.Button(frame, text="開啟手排",command=lambda:  [exeute_manual_schedule_task(),Switch_buttons(btn4_2_hand_schedule,btn5_output_csv)])
    btn4_2_hand_schedule .place(x=85, y=90, width=75,height=40)
    
   
   #start_to_schedule_clear_cylinder_jobtask(_pro_joblist,_cy_ptr_list, _cy_group_list)
   
    btn4_3_bad_schedule = ttk.Button(frame, text="最小清缸",command=lambda:  [groupList_to_clear_cylinder_jobtask(processing_cylinder_list,cy_ptr_list, cy_group_list),Switch_buttons(btn4_3_bad_schedule,btn4_2_hand_schedule)])
    btn4_3_bad_schedule .place(x=5, y=90, width=75,height=40)
    
   
   
   
    btn5_output_csv = ttk.Button(frame, text="#輸出排程CSV",command=lambda: [outputCsv(processing_cylinder_list, btn5_output_csv, _to_jobtask_list)])
   # btn5_output_csv.place(x=1410, y=0, width=150,height=40)
   #0118:原來為右手邊位置，後來至換至左手邊
    btn5_output_csv.place(x=5, y=140, width=150,height=40)
    
    
    btn_db_connection = ttk.Button(frame,  text="@設定DB", command=lambda: setup_db_params_and_testing())
    btn_db_connection.place(x=5, y=185, width=150,height=40)
    #button7.configure(bg = "red")
    
    #1222:新增重新啟動按鈕
    btn_clean = ttk.Button(frame,  text="重啟!!", command=lambda: [logginf_restart(log), restart_scheduler()])
    btn_clean.place(x=5, y=225, width=150,height=40)
    #button7.configure(bg = "red")
    
    
    
    #------------------工單變數宣告區---------------
    global cy1_status
    global cy2_status
    global cy3_status
    global cy5_status
    global cy6_status
    global cy7_status
    global cy8_status
    global cy9_status
    global cy10_status
    global cy11_status
    
    global cy12_status
    global cy13_status
    global cy14_status
    global cy15_status
    global cy16_status
    global cy17_status
    global cy18_status
    global cy19_status
    global cy20_status
    global cy21_status
    
    global cy22_status
    global cy23_status
    global cy24_status
    global cy25_status
    global cy26_status
    global cy32_status
    
    global listbox_temp
    global text4
    
    
    global cy1_schedule
    global cy2_schedule
    global cy3_schedule
    global cy5_schedule
    global cy6_schedule
    global cy7_schedule
    global cy8_schedule
    global cy9_schedule
    global cy10_schedule
    global cy11_schedule
    global cy12_schedule
    global cy13_schedule
    
    global cy14_schedule
    global cy15_schedule
    global cy16_schedule
    global cy17_schedule
    global cy18_schedule
    global cy19_schedule
    global cy20_schedule
    global cy21_schedule
    global cy22_schedule
    global cy23_schedule
    global cy24_schedule
    global cy25_schedule
    global cy26_schedule
    global cy32_schedule
    
    
    
    
    #-----------------------染缸UI區-1號缸---------------
    cy1_lb1=ttk.Label(frame,text='1號缸', background='yellow')
    cy1_lb1.place(x=5, y=270, width=150,height=40) 
    
    cy1_status=Listbox(tk)
    cy1_status.place(x=5, y=330, width=150,height=100)
    
    cy1_lb2=ttk.Label(frame,text='1號缸排程區', background='orange')
    cy1_lb2.place(x=160, y=270, width=150,height=40)
    
    cy1_schedule=Listbox(tk)
    cy1_schedule.place(x=160, y=330, width=150,height=100)
    
    #1215 added 
    global cy1_sta_mod_btn
    
    cy1_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy1_schedule, "1",processing_cylinder_list))
    cy1_sta_mod_btn.place(x=190, y=410, width=100,height=30)
    #--------------------------2號缸------------------------------
    cy2_lb1=ttk.Label(frame,text='2號缸', background='yellow')
    cy2_lb1.place(x=330, y=270, width=150,height=40) 
    
    cy2_status=Listbox(tk)
    cy2_status.place(x=330, y=330, width=150,height=100)
    
    cy2_lb2=ttk.Label(frame,text='2號缸排程區', background='orange')
    cy2_lb2.place(x=490, y=270, width=150,height=40)
    
    cy2_schedule=Listbox(tk)
    cy2_schedule.place(x=490, y=330, width=150,height=100)
    
    #1215 added 
    global cy2_sta_mod_btn
    
    
    cy2_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy2_schedule, "2",processing_cylinder_list))
    cy2_sta_mod_btn.place(x=520, y=410, width=100,height=30)
    #---------------------------3號缸----------------------
    cy3_lb1=ttk.Label(frame,text='3號缸', background='yellow')
    cy3_lb1.place(x=650, y=270, width=150,height=40) 
    
    cy3_status=Listbox(tk)
    cy3_status.place(x=650, y=330, width=150,height=100)
    
    cy3_lb2=ttk.Label(frame,text='3號缸排程區', background='orange')
    cy3_lb2.place(x=810, y=270, width=150,height=40)
    
    cy3_schedule=Listbox(tk)
    cy3_schedule.place(x=810, y=330, width=150,height=100)
    
    #1215 added 
    global cy3_sta_mod_btn
    
    
    cy3_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy3_schedule, "3",processing_cylinder_list))
    cy3_sta_mod_btn.place(x=840, y=410, width=100,height=30)
    #---------------------------5號缸-------------------------
    cy5_lb1=ttk.Label(frame,text='5號缸', background='yellow')
    cy5_lb1.place(x=970, y=270, width=150,height=40) 
    
    cy5_status=Listbox(tk)
    cy5_status.place(x=970, y=330, width=150,height=100)
    
    cy5_lb2=ttk.Label(frame,text='5號缸排程區', background='orange')
    cy5_lb2.place(x=1130, y=270, width=150,height=40)
    
    cy5_schedule=Listbox(tk)
    cy5_schedule.place(x=1130, y=330, width=150,height=100)
    
    
    global cy5_sta_mod_btn
    
    cy5_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy5_schedule, "5",processing_cylinder_list))
    cy5_sta_mod_btn.place(x=1160, y=410, width=100,height=30)
    #------------------------7號缸-----------------------------------
    cy6_lb1=ttk.Label(frame,text='6號缸', background='yellow')
    cy6_lb1.place(x=1290, y=270, width=150,height=40) 
    
    cy6_status=Listbox(tk)
    cy6_status.place(x=1290, y=330, width=150,height=100)
    
    cy6_lb2=ttk.Label(frame,text='6號缸排程區', background='orange')
    cy6_lb2.place(x=1450, y=270, width=150,height=40)
    
    cy6_schedule=Listbox(tk)
    cy6_schedule.place(x=1450, y=330, width=150,height=100)
    
   
    global cy6_sta_mod_btn
    
    cy6_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy6_schedule, "6",processing_cylinder_list))
    cy6_sta_mod_btn.place(x=1480, y=410, width=100,height=30)
    #-------------------7號缸-----------------------------------
    cy7_lb1=ttk.Label(frame,text='7號缸(黑)', background='yellow')
    cy7_lb1.place(x=1610, y=270, width=150,height=40) 
    
    cy7_status=Listbox(tk)
    cy7_status.place(x=1610, y=330, width=150,height=100)
    
    cy7_lb2=ttk.Label(frame,text='7號缸排程區', background='orange')
    cy7_lb2.place(x=1770, y=270, width=150,height=40)
    
    cy7_schedule=Listbox(tk)
    cy7_schedule.place(x=1770, y=330, width=150,height=100)
    
   
    global cy7_sta_mod_btn
    
    cy7_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy7_schedule, "7",processing_cylinder_list))
    cy7_sta_mod_btn.place(x=1800, y=410, width=100,height=30)
    #---------------------------8號缸--第二列-----------------------
    cy8_lb1=ttk.Label(frame,text='8號缸', background='yellow')
    cy8_lb1.place(x=10, y=440, width=150,height=40) 
    
    cy8_status=Listbox(tk)
    cy8_status.place(x=10, y=500, width=150,height=100)
    
    cy8_lb2=ttk.Label(frame,text='8號缸排程區', background='orange')
    cy8_lb2.place(x=170, y=440, width=150,height=40)
    
    cy8_schedule=Listbox(tk)
    cy8_schedule.place(x=170, y=500, width=150,height=100)
    
    
     
    global cy8_sta_mod_btn
    
    cy8_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy8_schedule, "8",processing_cylinder_list))
    cy8_sta_mod_btn.place(x=190, y=580, width=100,height=30)
    #---------------------------9號缸-------------------------
    cy9_lb1=ttk.Label(frame,text='9號缸(白)', background='yellow')
    cy9_lb1.place(x=340, y=440, width=150,height=40) 
    
    cy9_status=Listbox(tk)
    cy9_status.place(x=340, y=500, width=150,height=100)
    
    cy9_lb2=ttk.Label(frame,text='9號缸排程區', background='orange')
    cy9_lb2.place(x=500, y=440, width=150,height=40)
    
    cy9_schedule=Listbox(tk)
    cy9_schedule.place(x=500, y=500, width=150,height=100)
    
    
    global cy9_sta_mod_btn
    
    cy9_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy9_schedule, "9",processing_cylinder_list))
    cy9_sta_mod_btn.place(x=520, y=580, width=100,height=30)
    #---------------------------10號缸-------------------------
    cy10_lb1=ttk.Label(frame,text='10號缸(黑)', background='yellow')
    cy10_lb1.place(x=660, y=440, width=150,height=40) 
    
    cy10_status=Listbox(tk)
    cy10_status.place(x=660, y=500, width=150,height=100)
    
    cy10_lb2=ttk.Label(frame,text='10號缸排程區', background='orange')
    cy10_lb2.place(x=820, y=440, width=150,height=40)
    
    cy10_schedule=Listbox(tk)
    cy10_schedule.place(x=820, y=500, width=150,height=100)
    
    
    global cy10_sta_mod_btn
    
    cy10_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy10_schedule, "10",processing_cylinder_list))
    cy10_sta_mod_btn.place(x=840, y=580, width=100,height=30)
    #----------------------------11號缸---------------------
    cy11_lb1=ttk.Label(frame,text='11號缸狀態', background='yellow')
    cy11_lb1.place(x=980, y=440, width=150,height=40) 
    
    cy11_status=Listbox(tk)
    cy11_status.place(x=980, y=500, width=150,height=100)
    
    cy11_lb2=ttk.Label(frame,text='11號缸排程區', background='orange')
    cy11_lb2.place(x=1140, y=440, width=150,height=40)
    
    cy11_schedule=Listbox(tk)
    cy11_schedule.place(x=1140, y=500, width=150,height=100)
    
    
    global cy11_sta_mod_btn
    
    cy11_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy11_schedule, "11",processing_cylinder_list))
    cy11_sta_mod_btn.place(x=1160, y=580, width=100,height=30)
    #----------------------------12號缸---------------------
    cy12_lb1=ttk.Label(frame,text='12號缸狀態', background='yellow')
    cy12_lb1.place(x=1300, y=440, width=150,height=40) 
    
    cy12_status=Listbox(tk)
    cy12_status.place(x=1300, y=500, width=150,height=100)
    
    cy12_lb2=ttk.Label(frame,text='12號缸排程區', background='orange')
    cy12_lb2.place(x=1460, y=440, width=150,height=40)
    
    cy12_schedule=Listbox(tk)
    cy12_schedule.place(x=1460, y=500, width=150,height=100)
    
     
    global cy12_sta_mod_btn
    
    cy12_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy12_schedule, "12",processing_cylinder_list))
    cy12_sta_mod_btn.place(x=1480, y=580, width=100,height=30)
    #----------------------------13號缸---------------------
    cy13_lb1=ttk.Label(frame,text='13號缸(白)', background='yellow')
    cy13_lb1.place(x=1620, y=440, width=150,height=40) 
    
    cy13_status=Listbox(tk)
    cy13_status.place(x=1620, y=500, width=150,height=100)
    
    cy13_lb2=ttk.Label(frame,text='13號缸排程區', background='orange')
    cy13_lb2.place(x=1780, y=440, width=150,height=40)
    
    cy13_schedule=Listbox(tk)
    cy13_schedule.place(x=1780, y=500, width=150,height=100)
    
   
    global cy13_sta_mod_btn
    
    cy13_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy13_schedule, "13",processing_cylinder_list))
    cy13_sta_mod_btn.place(x=1800, y=580, width=100,height=30)
    #-------------------14號缸--第三列-----------------------
    cy14_lb1=ttk.Label(frame,text='14號缸狀態', background='yellow')
    cy14_lb1.place(x=10, y=610, width=150,height=40) 
    
    cy14_status=Listbox(tk)
    cy14_status.place(x=10, y=670, width=150,height=100)
    
    cy14_lb2=ttk.Label(frame,text='14號缸排程區', background='orange')
    cy14_lb2.place(x=170, y=610, width=150,height=40)
    
    cy14_schedule=Listbox(tk)
    cy14_schedule.place(x=170, y=670, width=150,height=100)
    
    
    global cy14_sta_mod_btn
    
    cy14_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy14_schedule, "14",processing_cylinder_list))
    cy14_sta_mod_btn.place(x=190, y=750, width=100,height=30)
    #---------------------------15號缸-------------------------
    cy15_lb1=ttk.Label(frame,text='15號缸狀態', background='yellow')
    cy15_lb1.place(x=340, y=610, width=150,height=40) 
    
    cy15_status=Listbox(tk)
    cy15_status.place(x=340, y=670, width=150,height=100)
    
    cy15_lb2=ttk.Label(frame,text='15號缸排程區', background='orange')
    cy15_lb2.place(x=500, y=610, width=150,height=40)
    
    cy15_schedule=Listbox(tk)
    cy15_schedule.place(x=500, y=670, width=150,height=100)
    
    
    global cy15_sta_mod_btn
    
    cy15_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy15_schedule, "15",processing_cylinder_list))
    cy15_sta_mod_btn.place(x=520, y=750, width=100,height=30)
    #---------------------------16號缸-------------------------
    cy16_lb1=ttk.Label(frame,text='16號缸狀態', background='yellow')
    cy16_lb1.place(x=660, y=610, width=150,height=40) 
    
    cy16_status=Listbox(tk)
    cy16_status.place(x=660, y=670, width=150,height=100)
    
    cy16_lb2=ttk.Label(frame,text='16號缸排程區', background='orange')
    cy16_lb2.place(x=820, y=610, width=150,height=40)
    
    cy16_schedule=Listbox(tk)
    cy16_schedule.place(x=820, y=670, width=150,height=100)
    
    
   
    global cy16_sta_mod_btn
    cy16_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy16_schedule, "16",processing_cylinder_list))
    cy16_sta_mod_btn.place(x=840, y=750, width=100,height=30)
    #---------------------------17號缸-------------------------
    cy17_lb1=ttk.Label(frame,text='17號缸狀態', background='yellow')
    cy17_lb1.place(x=980, y=610, width=150,height=40) 
    
    cy17_status=Listbox(tk)
    cy17_status.place(x=980, y=670, width=150,height=100)
    
    cy17_lb2=ttk.Label(frame,text='17號缸排程區', background='orange')
    cy17_lb2.place(x=1140, y=610, width=150,height=40)
    
    cy17_schedule=Listbox(tk)
    cy17_schedule.place(x=1140, y=670, width=150,height=100)
    
   
    global cy17_sta_mod_btn
    
    cy17_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy17_schedule, "17",processing_cylinder_list))
    cy17_sta_mod_btn.place(x=1160, y=750, width=100,height=30)
    #---------------------------18號缸-------------------------
    cy18_lb1=ttk.Label(frame,text='18號缸狀態', background='yellow')
    cy18_lb1.place(x=1300, y=610, width=150,height=40) 
    
    cy18_status=Listbox(tk)
    cy18_status.place(x=1300, y=670, width=150,height=100)
    
    cy18_lb2=ttk.Label(frame,text='18號缸排程區', background='orange')
    cy18_lb2.place(x=1460, y=610, width=150,height=40)
    
    cy18_schedule=Listbox(tk)
    cy18_schedule.place(x=1460, y=670, width=150,height=100)
    
   
    global cy18_sta_mod_btn
    
    cy18_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy18_schedule, "18",processing_cylinder_list))
    cy18_sta_mod_btn.place(x=1480, y=750, width=100,height=30)
    #---------------------------19號缸-------------------------
    cy19_lb1=ttk.Label(frame,text='19號缸(黑)', background='yellow')
    cy19_lb1.place(x=1620, y=610, width=150,height=40) 
    
    cy19_status=Listbox(tk)
    cy19_status.place(x=1620, y=670, width=150,height=100)
    
    cy19_lb2=ttk.Label(frame,text='19號缸排程區', background='orange')
    cy19_lb2.place(x=1780, y=610, width=150,height=40)
    
    cy19_schedule=Listbox(tk)
    cy19_schedule.place(x=1780, y=670, width=150,height=100)
    
   
    global cy19_sta_mod_btn
    
    cy19_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy19_schedule, "19",processing_cylinder_list))
    cy19_sta_mod_btn.place(x=1800, y=750, width=100,height=30)
    #---------------------------20號缸--第4列-----------------------
    cy20_lb1=ttk.Label(frame,text='20號缸', background='yellow')
    cy20_lb1.place(x=10, y=780, width=150,height=40) 
    
    cy20_status=Listbox(tk)
    cy20_status.place(x=10, y=840, width=150,height=100)
    
    cy20_lb2=ttk.Label(frame,text='20號缸排程區', background='orange')
    cy20_lb2.place(x=180, y=780, width=150,height=40)
    
    cy20_schedule=Listbox(tk)
    cy20_schedule.place(x=180, y=840, width=150,height=100)
    
  
    global cy20_sta_mod_btn
    
    cy20_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy20_schedule, "20",processing_cylinder_list))
    cy20_sta_mod_btn.place(x=190, y=920, width=100,height=30)
    #---------------------------21號缸------------------------
    cy21_lb1=ttk.Label(frame,text='21號缸', background='yellow')
    cy21_lb1.place(x=340, y=780, width=150,height=40) 
    
    cy21_status=Listbox(tk)
    cy21_status.place(x=340, y=840, width=150,height=100)
    
    cy21_lb2=ttk.Label(frame,text='21號缸排程區', background='orange')
    cy21_lb2.place(x=500, y=780, width=150,height=40)
    
    cy21_schedule=Listbox(tk)
    cy21_schedule.place(x=500, y=840, width=150,height=100)
    
   
    global cy21_sta_mod_btn
    
    cy21_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy21_schedule, "21",processing_cylinder_list))
    cy21_sta_mod_btn.place(x=510, y=920, width=100,height=30)
    #---------------------------22號缸------------------------
    cy22_lb1=ttk.Label(frame,text='22號缸', background='yellow')
    cy22_lb1.place(x=670, y=780, width=150,height=40) 
    
    cy22_status=Listbox(tk)
    cy22_status.place(x=670, y=840, width=150,height=100)
    
    cy22_lb2=ttk.Label(frame,text='22號缸排程區', background='orange')
    cy22_lb2.place(x=830, y=780, width=150,height=40)
    
    cy22_schedule=Listbox(tk)
    cy22_schedule.place(x=830, y=840, width=150,height=100)
    
 
    global cy22_sta_mod_btn
    
    cy22_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy22_schedule, "22",processing_cylinder_list))
    cy22_sta_mod_btn.place(x=840, y=920, width=100,height=30)
    #---------------------------23號缸------------------------
    cy23_lb1=ttk.Label(frame,text='23號缸', background='yellow')
    cy23_lb1.place(x=990, y=780, width=150,height=40) 
    
    cy23_status=Listbox(tk)
    cy23_status.place(x=990, y=840, width=150,height=100)
    
    cy23_lb2=ttk.Label(frame,text='23號缸排程區', background='orange')
    cy23_lb2.place(x=1150, y=780, width=150,height=40)
    
    cy23_schedule=Listbox(tk)
    cy23_schedule.place(x=1150, y=840, width=150,height=100)
    
  
    global cy23_sta_mod_btn
    
    cy23_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy23_schedule, "23",processing_cylinder_list))
    cy23_sta_mod_btn.place(x=1160, y=920, width=100,height=30)
    #---------------------------24號缸------------------------
    cy24_lb1=ttk.Label(frame,text='24號缸', background='yellow')
    cy24_lb1.place(x=1310, y=780, width=150,height=40) 
    
    cy24_status=Listbox(tk)
    cy24_status.place(x=1310, y=840, width=150,height=100)
    
    cy24_lb2=ttk.Label(frame,text='24號缸排程區', background='orange')
    cy24_lb2.place(x=1470, y=780, width=150,height=40)
    
    cy24_schedule=Listbox(tk)
    cy24_schedule.place(x=1470, y=840, width=150,height=100)
    
   
    global cy24_sta_mod_btn
    
    cy24_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy24_schedule, "24",processing_cylinder_list))
    cy24_sta_mod_btn.place(x=1480, y=920, width=100,height=30)
    #---------------------------25號缸------------------------
    cy25_lb1=ttk.Label(frame,text='25號缸', background='yellow')
    cy25_lb1.place(x=1630, y=780, width=150,height=40) 
    
    cy25_status=Listbox(tk)
    cy25_status.place(x=1630, y=840, width=150,height=100)
    
    cy25_lb2=ttk.Label(frame,text='25號缸排程區', background='orange')
    cy25_lb2.place(x=1790, y=780, width=150,height=40)
    
    cy25_schedule=Listbox(tk)
    cy25_schedule.place(x=1790, y=840, width=150,height=100)
    
   
    global cy25_sta_mod_btn
    
    cy25_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy25_schedule, "25",processing_cylinder_list))
    cy25_sta_mod_btn.place(x=1800, y=920, width=100,height=30)
    #--------------------------26號缸-------------------------------
    cy26_lb1=ttk.Label(frame,text='26號缸', background='yellow')
    cy26_lb1.place(x=1600, y=0, width=150,height=40) 
    
    cy26_status=Listbox(tk)
    cy26_status.place(x=1600, y=60, width=150,height=80)
    
    cy26_lb2=ttk.Label(frame,text='26號缸排程區', background='orange')
    cy26_lb2.place(x=1770, y=0, width=150,height=40)
    
    cy26_schedule=Listbox(tk)
    cy26_schedule.place(x=1770, y=60, width=150,height=50)
    
   
    global cy26_sta_mod_btn
    cy26_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy26_schedule, "26",processing_cylinder_list))
    cy26_sta_mod_btn.place(x=1800, y=90, width=100,height=30)
    #---------------------32號缸---------------------------------
    cy32_lb1=ttk.Label(frame,text='32號缸', background='yellow')
    cy32_lb1.place(x=1600, y=120, width=150,height=40) 
    
    cy32_status=Listbox(tk)
    cy32_status.place(x=1600, y=180, width=150,height=80)
    
    cy32_lb2=ttk.Label(frame,text='32號缸排程區', background='orange')
    cy32_lb2.place(x=1770, y=120, width=150,height=40)
    
    cy32_schedule=Listbox(tk)
    cy32_schedule.place(x=1770, y=180, width=150,height=80)
    
    global cy32_sta_mod_btn
    cy32_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(cy32_schedule, "32",processing_cylinder_list))
    cy32_sta_mod_btn.place(x=1800, y=240, width=100,height=30)
    
    
    global cy100_sta_mod_btn
    cy100_sta_mod_btn = ttk.Button(frame, text="手排",command=lambda: Popup(listbox_temp, "100",processing_cylinder_list))
    cy100_sta_mod_btn.place(x=1440, y=240, width=100,height=30)

    #0118: 暫時區介面設定與控制區=============================
    cy100_lb1=ttk.Label(frame,text='暫時區 100', background='yellow')
    cy100_lb1.place(x=1420, y=100, width=150,height=40) 
    listbox_temp=Listbox(tk)
    listbox_temp.place(x=1410, y=160, width=180,height=100)
    listboxt_select=listbox_temp.bind('<<ListboxSelect>>', listbox_temp_select)
    #------------------ CSV訊息區-----------------------
    text4=tkinter.Text(tk)
    text4.place(x=1410, y=25,width=180, height=100)
    #======================================================


    #=========================設定每缸參數區===================
    setup_cylinder_params(text4)#載入每缸參數值與相關碼重與 投胚量
    shutdown_manual_schedule_btns()#關閉手動排程按鈕
    #==========================log紀錄器宣告與啟動===================
    global log
    #這邊要設定logbook物件的時間去抓取本電腦的時間local，否則會有少8小時現象產生
    logbook.set_datetime_format("local")
    StreamHandler(sys.stdout).push_application()
    log = Logger('Logbook')
    log_handler = FileHandler('schedule_dyeing.log')
    log_handler.push_application()
    log.info("排程程式啟動")
    tk.mainloop()
    
    
#呼叫主函式    
scheduler_to_dyeing()    




#====================0118: 測試資料
 #dist=[('100178001A', '2017/8/8 16:54:20:316','X100170719','0','1','X101700013',0,"外包",0,0,'',0),      ('A00175059A', '2017/6/1 07:55:38:57','EHA75059','481.1','190','A10150004',0,"D09",4775.5,0,'',0), ('A00175060A','2017/6/1 07:55:17:913','EHA75059','474','190','A10150004',0,'D09',2370,0,'',0), ('A00175061A','2017/6/8 01:29:14:88','EHA75035','480.6','190','A10150002',0,'D19',1441.8,0,'',0),('A00175062A','2017/6/8 06:34:29:06','EHA75035','479.3','190','A10150002',0,'D19',1437.9,0,'',0),('A00175063A','2017/6/8 11:30:31:913','EHA75035','485.4','190','A10150002',0,'D19',1456.2,0,'',0),('A00175064A','2017/6/8 15:32:39:746','EHA75035','469.8','190','A10150002',0,'D19',1409.4,0,'',0),('A00175065A','2017/6/8 21:00:14:563','EHA75035','478.6','190','A10150002',0,'D19',14435.8,0,'',0),('A00176007A','2017/6/12 22:31:00:306','20170602001','0','1','A001700015',0,'D17',210,0,'',0),('A00176064A','2017/6/27 11:51:57:97','A00170616','14.7','1','A621600053',0,'D08',102.9,0,'',0),('A00176065A','2017/6/27 15:51:35:933','A00170616','16.9','1','A621600053',0,'D08',118.3,0,'',0),('A00177004A','2017/7/14 13:31:24:38','A5320170704','53.5','280','A531500868',0,'D14',0,0,'',0),('A00177022A','2017/7/9 06:08:19:59','EHA75035','477.7','190','A101500002',0,'D05',0,0,'',0),('A00177023A','2017/7/10 15:00:42:46','EHA75035','471.8','190','A101500002',0,'D05',0,0,'',0),('A00177024A','2017/7/11 21:50:50:8','EHA75035','180.2','190','A101500002',0,'D20',0,0,'',0),('A00177059A','2017/7/25 04:56:47:246','EHA75035','491.9','190','A101500002',0,'D05',0,0,'',0),('A00177060A','2017/7/25 10:31:38:276','EHA75035','486.7','190','A101500002',0,'D05',0,0,'',0),('A00177061A','2017/7/25 15:57:13:096','EHA75035','488.4','190','A101500002',0,'D05',0,0,'',0),('A00177061B','2017/7/27 03:33:50:25','EHA75035','488.4','190','A101500002',0,'D05',0,0,'',0),('A00177061C','2017/7/28 03:55:29:78','EHA75035','488.4','190','A101500002',0,'D05',0,0,'',0)]