import pyodbc
#20181116   因應xp,import pytds
import pytds
import tkinter
from tkinter.messagebox import showinfo
import sys
import logbook
from logbook import Logger, StreamHandler, FileHandler#1211新增logbook紀錄相關程式狀況

#20181206   抓取三個月內未完成工單，依分派日 交期 最大濃度排序
def fetch_dyeing_data_with_Totolcon(_server, _dbname, _user, _pass,_dbindex,_dbobject):
    global result_set
    # 20181116   因應xp,import pytds
    #cnxn2 = pyodbc.connect('DRIVER={SQL Server};SERVER='+_server+';DATABASE='+_dbname+';UID='+_user+';PWD='+_pass)
    cnxn2 = pytds.connect(server='127.0.0.1', user='ttri', password='01891206', database='bl')
    cursor2 = cnxn2.cursor() #server 位置: 172.16.41.23
    sql_state=""
    dbindex=0
    dbindex=int(_dbindex)
    #if dbindex==0:#
    #20180703 抓取正式DB[三個月內未完成工單]
    #sql_state="select top 200 t.cist_no, t.od_no, t.clr_id, t.emb_qty, round((t.emb_qty/t.weight)*1000,2) as yard, CONVERT(DATETIME2(0), t.ci_date) as ci_date,CONVERT(DATETIME2(0), t.assign_date) as assign_date,CONVERT(DATETIME2(0), t.pred_date) as pred_date,h.total_con from v_pcist t left join (select g.cist_no, sum(g.con_percentage) as total_con from (select  a.cist_no,c.clr_id,a.emb_qty,c.bomc_qty as con_percentage from v_pcist a left join  v_bom  c on a.clr_id=c.clr_id  where a.emb_qty!=0 and c.dypd_type='1' and c.un_type='2') g group by g.cist_no) h on t.cist_no=h.cist_no where t.emb_qty<1001 and round((t.emb_qty/t.weight)*1000,2)<6401 and h.total_con is not null and ci_date >= DATEADD(MONTH, -3, GETDATE()) and ci_date < '2080-01-01 00:00:00.000' and ok_date is null order by CONVERT(char(10), assign_date, 20),pred_date"
    #20181206   新抓取dypd_id(最大濃度配方代號)
    sql_state = "select e.*,d.dypd_id from (select f.clr_id,f.dypd_id,f.dypd_name,f.unit_id,f.bomc_qty from (SELECT *, ROW_NUMBER() OVER (PARTITION BY clr_id Order By bomc_qty DESC) As sn FROM v_bom where dypd_type='1' and un_type='2') f where f.sn=1) d right join (select t.cist_no, t.od_no, t.clr_id, t.emb_qty, round((t.emb_qty/t.weight)*1000,2) as yard, CONVERT(DATETIME2(0), t.ci_date) as ci_date, CONVERT(DATETIME2(0), t.assign_date) as assign_date, CONVERT(DATETIME2(0), t.pred_date) as pred_date,h.total_con,h.max_con from v_pcist t left join (select g.cist_no, sum(g.con_percentage) as total_con, max(g.con_percentage) as max_con from (select  a.cist_no,c.clr_id,c.bomc_qty as con_percentage from v_pcist a left join v_bom c on a.clr_id=c.clr_id  where a.emb_qty!=0 and c.dypd_type='1' and c.un_type='2') g group by g.cist_no) h on t.cist_no=h.cist_no where t.emb_qty<1001 and round((t.emb_qty/t.weight)*1000,2)<6401 and h.total_con is not null and ci_date >= DATEADD(MONTH, -3, GETDATE()) and ci_date < '2080-01-01 00:00:00.000' and ok_date is null ) e on d.clr_id = e.clr_id order by CONVERT(char(10), e.assign_date, 20),e.pred_date,e.max_con"
    #0105: 去除毫秒:CONVERT(DATETIME2(0), t.ok_date)
   #     dbindex+=100
        
        
   # elif dbindex>0:#每次都會累加100筆
  #      sql_state="select top "+str(dbindex)+" * from (select top "+str(dbindex+100)+" a.cist_no,a.ok_date,a.od_no, a.emb_qty, (a.weight/1000) as weight, a.clr_id, round((a.emb_qty/a.weight)*1000,2) as yard, a.mach_id, round(c.pi_qty/1000,3) as pi_qty,  round(((c.pi_qty/(a.emb_qty*1000))*100),3) as con_percentage, '' as emb_qty_date, '' as total_con from SL_DYEING a left join  SL_RECIPE c on a.cist_no=c.cist_no where a.emb_qty!=0 order by ok_date) t order by ok_date desc"
   #     dbindex+=100
    #1221:更新資料庫筆數index使用=========
    #_dbobject.update_record(str(dbindex))
    #log.info("")
    #===================================
    cursor2.execute(sql_state)#cnxn2.commit()
    result_set=cursor2.fetchall()
    if len(result_set)!=0:
        cnxn2.close() #print (result_set)
        return result_set

# 20181206   抓取一天內各染缸最後一筆工單，總濃度及最大濃度
def fetch_dyeing_data_with_Totalcon_BF3D(_server, _dbname, _user, _pass, _dbindex, _dbobject):
    global result_set1
    cnxn3 = pytds.connect(server='127.0.0.1', user='ttri', password='01891206', database='bl')
    cursor3 = cnxn3.cursor()  # server 位置: 172.16.41.23
    sql_state = ""
    dbindex = 0
    dbindex = int(_dbindex)
    # 20181204   新抓取dypd_id(最大濃度配方號)   20181219 調整欄位與待排工單一致，新抓取mach_id
    sql_state = "select e.cist_no,e.od_no,e.clr_id,e.emb_qty,round((e.emb_qty/e.weight)*1000,2) as yard,e.ci_date,e.assign_date,e.pred_date,e.total_con,e.max_con,d.dypd_id,e.mach_id from (select f.clr_id,f.dypd_id,f.dypd_name,f.unit_id,f.bomc_qty from (SELECT *, ROW_NUMBER() OVER (PARTITION BY clr_id Order By bomc_qty DESC) As sn FROM v_bom where dypd_type='1' and un_type='2') f where f.sn=1) d right join (select t.*,h.total_con, h.max_con from (select a.* from [bl].[dbo].[v_pcist] a,(select mach_id, max(ok_date) as maxtime from [bl].[dbo].[v_pcist] where ok_date >= DATEADD(DAY, -1, GETDATE()) and ok_date < GETDATE() group by mach_id) as b WHERE (a.mach_id = b.mach_id) and (a.ok_date = b.maxtime)) t left join (select g.cist_no, sum(g.con_percentage) as total_con, max(g.con_percentage) as max_con from (select  a.cist_no,c.clr_id,c.bomc_qty as con_percentage from v_pcist a left join v_bom c on a.clr_id=c.clr_id  where a.emb_qty!=0 and c.dypd_type='1' and c.un_type='2') g group by g.cist_no) h on t.cist_no = h.cist_no) e on d.clr_id=e.clr_id where e.total_con != 0"
    # ===================================
    cursor3.execute(sql_state)  # cnxn3.commit()
    result_set1 = cursor3.fetchall()
    if len(result_set1) != 0:
        cnxn3.close()  # print (result_set1)
        return result_set1

#20181226   抓取即時前缸資料
def fetch_dyeing_data_with_Before(_server, _dbname, _user, _pass, _dbindex, _dbobject):
    global result_set2
    cnxn4 = pytds.connect(server='127.0.0.1', user='ttri', password='01891206', database='bl')
    cursor4 = cnxn4.cursor()  # server 位置: 172.16.41.23
    sql_state = ""
    dbindex = 0
    dbindex = int(_dbindex)
    # 20181204   新抓取dypd_id(最大濃度配方號)   20181219 調整欄位與待排工單一致，新抓取mach_id
    sql_state = "select * from schedule_before"
    # ===================================
    cursor4.execute(sql_state)  # cnxn4.commit()
    result_set2 = cursor4.fetchall()
    if len(result_set2) != 0:
        cnxn4.close()  # print (result_set2)
        return result_set2

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

        # 20181116   因應xp,import pytds
        #with pyodbc.connect(connstr, timeout=1) as cnxn1:
        with pytds.connect(connstr, timeout=1) as cnxn1:
            cursor1 = cnxn1.cursor()
            test_set = cursor1.execute("select * from SL_DYEING").fetchall()
        
        if len(test_set)!=0:
            connect_success=True
            dblog.info("db連線成功")
            cnxn1.close()
        else:
            cnxn1.close()
    # 20181116   因應xp,import pytds
    #except pyodbc.Error as e:
    except pytds.Error as e:
        sqlstate = e.args[0]#cnxn1.close()
        if sqlstate=='08001':
            connect_success=False
            dblog.warn("DB連線狀態錯誤，可能為不存在連線或是拒絕存取，請檢查帳號密碼或是相關連線IP狀態")
            showinfo("連線狀態","不存在連線或是拒絕存取，連線資訊有誤。")
    return connect_success