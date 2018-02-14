import xlrd
#打开excel文件
from tkinter import *
import tkinter
from tkinter.messagebox import showinfo
from tkinter.filedialog import askopenfilename
import re
import pandas as pd
#===============================================
def LoadExcelFile():
    
    #filename=""
    #這邊會去開啟一個視窗，點選檔案後，回傳相關檔案位置
    #data_list用来存放第一行数据
    global excel_dist
    excel_dist=[]
    data_list=[]  
    filename = askopenfilename()
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
    #print(excel_dist)
    
    # i=0
    #for i in range(len(excel_dist)):
        #if i==0:
          #print(excel_dist[0][0])
    
    #global str_tmp
    #attached_filename=""
    #str_tmp=[]
    #str_tmp=re.split(r'\.(?!\d)', filename)
    #attached_filename=str_tmp[1]
    
    #filename_add_file_atttr=""
    #filename_add_file_atttr=filename[]
    
    #if filename!=None:
    #        showinfo("警告視窗","您尚未選擇相關載入檔案，請重新選擇檔案，感謝您!!")
    #else:
    
    #print(filename)
    #data=xlrd.open_workbook(filnal_filename)
    #table=data.sheets()[0]
    #抓取第一行標頭放到data_list理
    #data_list.extend(table.row_values(0))
    #nrows = table.nrows
    #ncols = table.ncols
    #count=0
    #tmp_tuple=()
    #for i in range(1,nrows):
        #tmp_tuple=tuple(table.row_values(i))# 把row資料轉換為tuple型別
        #excel_dist.append(tmp_tuple)
        #tmp_tuple=None  
     
  
  
  
    return excel_dist
#temp_dist=[]
#temp_dist=LoadExcelFile()

#=====註解區
#列印出每列資料
#for i in range(nrows):
#    print(table.row_values(i))

#打印出第一行的全部数据
#for item in data_list:
#    print (item)
        #print(dist)
        #print("#########################")
        #print("共"+str(count)+"筆資料")
                