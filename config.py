import tkinter
import configparser
from tkinter.messagebox import showinfo
class config:
    def __init__(self):
        config_reader = configparser.ConfigParser()
        config_reader.read('DB_config.ini')
        self.sever = config_reader.get('DB_connection', 'Server')
        self.database = config_reader.get('DB_connection', 'DataBase')
        self.user = config_reader.get('DB_connection', 'User')
        self.password = config_reader.get('DB_connection', 'Password')
        #1212: 新增紀錄目前上一次排缸讀到第幾筆資料庫資料
        self.dbrecord_num=config_reader.get('DB_record', 'Index')
        
        
        print ("sever = %s" % self.sever) 
        print ("database = %s" % self.database) 
        print ("user = %s" % self.user)
        print ("password  = %s" % self.password )
        print ("上次資料庫序號為第  = %s" % self.dbrecord_num )
    
    def modify(self,_rec_server, _rec_database, _rec_user, _rec_password):  
        config_writer= configparser.ConfigParser()
        config_writer.optionxform = str
        config_writer.read('DB_config.ini')
        config_writer.set('DB_connection','Server',_rec_server)
        config_writer.set('DB_connection','DataBase',_rec_database)
        config_writer.set('DB_connection','User',_rec_user)
        config_writer.set('DB_connection','Password',_rec_password)
        with open('DB_config.ini', 'w') as configfile:
           config_writer.write(configfile)
           showinfo("檔案狀態","儲存成功")
    #新增更新資料庫資料index筆數，讓下次排缸可以接續讀取       
    def update_record(self, _rec_record):
        config_writer= configparser.ConfigParser()
        config_writer.optionxform = str
        config_writer.read('DB_config.ini')
        config_writer.set('DB_record','Index',_rec_record)
        with open('DB_config.ini', 'w') as configfile:
           config_writer.write(configfile)
           showinfo("資料庫序號更新","儲存成功")
           
           
           
           
    
    def get_server(self):
        return self.sever 
    def get_dbname(self):
        return self.database
    def get_user(self):
        return  self.user 
    def get_password(self):
        return self.password
    def get_dbrecordindex(self):
        return self.dbrecord_num 
    #===========================
        
#test=config()
#test.modify('199.0.0.1', '123', 'ggggg', 'tttttt')
#print ("sever = %s" % test.get_server()) 
