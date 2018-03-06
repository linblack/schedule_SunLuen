import pyodbc
import tkinter
from tkinter.messagebox import showinfo
import sys
import logbook
from logbook import Logger, StreamHandler, FileHandler#1211新增logbook紀錄相關程式狀況



#0105L這邊加入RECIPE2017濃度表，去抓出總濃度出來，不用去合併檔案
def fetch_dyeing_data_with_Totolcon(_server, _dbname, _user, _pass,_dbindex,_dbobject):
    global result_set
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+_server+';DATABASE='+_dbname+';UID='+_user+';PWD='+_pass)
    cursor2 = cnxn.cursor() #server 位置: 172.16.41.23
    sql_state=""
    dbindex=0
    dbindex=int(_dbindex)
    #if dbindex==0:#
    sql_state="select top 100 t.cist_no,CONVERT(DATETIME2(0), t.ok_date) as ok_date,t.od_no, t.emb_qty, (t.weight/1000) as weight, t.clr_id, round((t.emb_qty/t.weight)*1000,2) as yard, t.mach_id, '' as pi_qty, '' as con_percentage, '' as emb_qty_date, h.total_con from SL_DYEING t left join (select g.cist_no, sum(g.con_percentage) as total_con from (select  a.cist_no,c.clr_id,a.emb_qty,c.bomc_qty as con_percentage from SL_DYEING a left join  SL_RECIPE2017  c on a.clr_id=c.clr_id  where a.emb_qty!=0 and c.dypd_type='1' and c.un_type='2') g group by g.cist_no) h on t.cist_no=h.cist_no where t.emb_qty>0 and h.total_con is not null and LEN(mach_id)<=3 order by ok_date desc"
    #0105: 去除毫秒:CONVERT(DATETIME2(0), t.ok_date)
   #     dbindex+=100
        
        
   # elif dbindex>0:#每次都會累加100筆
  #      sql_state="select top "+str(dbindex)+" * from (select top "+str(dbindex+100)+" a.cist_no,a.ok_date,a.od_no, a.emb_qty, (a.weight/1000) as weight, a.clr_id, round((a.emb_qty/a.weight)*1000,2) as yard, a.mach_id, round(c.pi_qty/1000,3) as pi_qty,  round(((c.pi_qty/(a.emb_qty*1000))*100),3) as con_percentage, '' as emb_qty_date, '' as total_con from SL_DYEING a left join  SL_RECIPE c on a.cist_no=c.cist_no where a.emb_qty!=0 order by ok_date) t order by ok_date desc"
   #     dbindex+=100
    #1221:更新資料庫筆數index使用=========
    #_dbobject.update_record(str(dbindex))
    #log.info("")
    #===================================
    cursor2.execute(sql_state)#cnxn.commit()
    result_set=cursor2.fetchall()
    if len(result_set)!=0:
        cnxn.close() #print (result_set)
        return result_set






#===========#計算出各自濃度(同一筆工單會有多筆濃度資料值出現，無法計算何是染料 何是助劑)
#====1218 更新:目前新增總濃度欄位
def fetch_dyeing_data_without_2weeks(_server, _dbname, _user, _pass,_dbindex,_dbobject):
    global result_set
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+_server+';DATABASE='+_dbname+';UID='+_user+';PWD='+_pass)
    cursor2 = cnxn.cursor() #server 位置: 172.16.41.23
    sql_state=""
    dbindex=0
    dbindex=int(_dbindex)
    if dbindex==0:#第一次為選擇前30筆
        sql_state="select top 30 a.cist_no,a.ok_date,a.od_no, a.emb_qty, (a.weight/1000) as weight, a.clr_id, round((a.emb_qty/a.weight)*1000,2) as yard, a.mach_id, round(c.pi_qty/1000,3) as pi_qty,  round(((c.pi_qty/(a.emb_qty*1000))*100),3) as con_percentage, '' as emb_qty_date, '' as total_con from SL_DYEING a left join  SL_RECIPE c on a.cist_no=c.cist_no where a.emb_qty!=0 order by ok_date desc"
        dbindex+=30
    elif dbindex>0:#每次都會累加30筆
        sql_state="select top "+str(dbindex)+" * from (select top "+str(dbindex+30)+" a.cist_no,a.ok_date,a.od_no, a.emb_qty, (a.weight/1000) as weight, a.clr_id, round((a.emb_qty/a.weight)*1000,2) as yard, a.mach_id, round(c.pi_qty/1000,3) as pi_qty,  round(((c.pi_qty/(a.emb_qty*1000))*100),3) as con_percentage, '' as emb_qty_date, '' as total_con from SL_DYEING a left join  SL_RECIPE c on a.cist_no=c.cist_no where a.emb_qty!=0 order by ok_date) t order by ok_date desc"
        dbindex+=30
    #1221:更新資料庫筆數index使用=========
    _dbobject.update_record(str(dbindex))
    #log.info("")
    #===================================
    cursor2.execute(sql_state)#cnxn.commit()
    result_set=cursor2.fetchall()
    if len(result_set)!=0:
        cnxn.close() #print (result_set)
        return result_set
#=================#計算出總濃度
#此方式不適合(因會有助劑濃度計算進去，故暫時先不用)
def fetch_dyeing_data_without_2weeks_withTotalCon(_server, _dbname, _user, _pass):
    global result_set_with_total_con
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+_server+';DATABASE='+_dbname+';UID='+_user+';PWD='+_pass)
    cursor2 = cnxn.cursor()
    #server 位置: 172.16.41.23
    cursor2.execute("select  t.cist_no,t.ok_date,t.od_no, t.emb_qty, (t.weight/1000) as weight, t.clr_id, round((t.emb_qty/t.weight)*1000,2) as yard, t.mach_id, '' as pi_qty, '' as con_percentage, '' as emb_qty_date, h.total_con from SL_DYEING t left join (select g.cist_no, sum(g.con_percentage) as total_con from (select  a.cist_no,a.emb_qty,round(c.pi_qty/1000,3) as pi_qty,round(((c.pi_qty/(a.emb_qty*1000))*100),3) as con_percentage from SL_DYEING a left join  SL_RECIPE c on a.cist_no=c.cist_no where a.emb_qty!=0 ) g group by g.cist_no) h on t.cist_no=h.cist_no where t.emb_qty>0 order by ok_date desc")
    #cnxn.commit()
    result_set_with_total_con=cursor2.fetchall()
   
    if len(result_set_with_total_con)!=0:
        #print (result_set_with_total_con)
        cnxn.close()
        return result_set_with_total_con        
#========#抓取距離目前兩周內的工單號碼
def fetch_dyeing_data_with_2eeks(_server, _dbname, _user, _pass):
    global result_set_2_weeks
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+_server+';DATABASE='+_dbname+';UID='+_user+';PWD='+_pass)
    cursor2 = cnxn.cursor()
    #server 位置: 172.16.41.23
    cursor2.execute("select a.cist_no,a.ok_date,a.od_no, a.emb_qty, (a.weight/1000) as weight, a.clr_id, round((a.emb_qty/a.weight)*1000,2) as yard, a.mach_id, round(c.pi_qty/1000,3) as pi_qty,  round(((c.pi_qty/(a.emb_qty*1000))*100),3) as con, '' as emb_qty_date,'' as total_con  from SL_DYEING a left join  SL_RECIPE c on a.cist_no=c.cist_no where a.emb_qty!=0 and DATEDIFF(day, a.ok_date, getdate())<=14 order by ok_date desc")
    #cnxn.commit()
    result_set_2_weeks=cursor2.fetchall()
    if len(result_set_2_weeks)!=0:
        #print (result_set_2_weeks)
        cnxn.close()
        return result_set_2_weeks
#====================================================
def TestDBconnection(_server, _dbname, _user, _pass):#測試DB連線狀態
    global dblog
    logbook.set_datetime_format('local')#使用目前電腦時間，否則會有錯誤出現(少8小時)
    StreamHandler(sys.stdout).push_application()
    dblog = Logger('Logbook')
    dblog_handler = FileHandler('application.log')
    dblog_handler.push_application()
    dblog.info("資料庫連線程式啟動")

    global test_set
    test_set=[]
    connect_success=None
    
    SQL_ATTR_CONNECTION_TIMEOUT = 113
    login_timeout = 5
    connection_timeout = 20
    
    connstr=""
    connstr='DRIVER={SQL Server};SERVER='+_server+';DATABASE='+_dbname+';UID='+_user+';PWD='+_pass
    
    try: 
        global cnxn1
        #cnxn1 = pyodbc.connect(connstr, timeout=login_timeout, attrs_before={SQL_ATTR_CONNECTION_TIMEOUT : connection_timeout})
        
        #cnxn1 = pyodbc.connect(connstr)
        #cnxn1.timeout=5
        #cursor1 = cnxn1.cursor()
        #cursor1.execute("select * from SL_DYEING")
        #cnxn1.commit()
        #test_set=cursor1.fetchall()

      
        with pyodbc.connect(connstr, timeout=1) as cnxn1:
            cursor1 = cnxn1.cursor()
            test_set = cursor1.execute("select * from SL_DYEING").fetchall() #fetchall():接收全部的返回结果行
        
        if len(test_set)!=0:
            connect_success=True
            dblog.info("db連線成功")
            cnxn1.close()
        else:
            cnxn1.close()
    except pyodbc.Error as e:
        sqlstate = e.args[0]#cnxn1.close()
        if sqlstate=='08001':
            connect_success=False
            dblog.warn("DB連線狀態錯誤，可能為不存在連線或是拒絕存取，請檢查帳號密碼或是相關連線IP狀態")
            showinfo("連線狀態","不存在連線或是拒絕存取，連線資訊有誤。")
    return connect_success
    
#_yes_or_no=TestDBconnection('199.0.0.1', 'InfoCenter', 'mtchen', '1368')  #
#list=[]
#list=fetch_dyeing_data_without_2weeks('172.16.41.22', 'InfoCenter', 'mtchen', '1368')
#if len(list)!=0:
#   print("已連線上")