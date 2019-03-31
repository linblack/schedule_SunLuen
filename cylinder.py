from Queue import queue
class cylinder:
    def __init__(self, _no, _dying_name,_rec_weight,_length,_jobno_con_list,_flag_value):
        self.cylinder_no=_no#缸編號
        self.queue=queue()#工單排序queue
        self.dying_color_name=_dying_name#染色名稱
        self.weight=_rec_weight#缸容重量
       # self.qty_weight=_rec_weight*0.8#缸容80%重量
        self.yard_length=_length#碼長
        #self.priority_bit=_bit#設定優先順序 1最高~10最低
        #0111:這邊要去紀錄工單跟濃度tuple，方便後續排序使用
        cylinder_con_list=[]
        self.cylinder_con_list=_jobno_con_list#工單與濃度tuple_list
        #0115:用來做queue容量檢查，如果滿了，則是設定full_flag為true
        self.queue_full_flag=_flag_value   
    def get_dying_color_name(self):
        return self.dying_color_name
    def get_weight(self):
        return self.weight
    def get_dying_queue(self):
        return self.queue
    def get_cylinder_no(self):
        return self.cylinder_no
    #def get_qty_weight(self):
       # return self.qty_weight
    def get_yard_length(self):
        return self.yard_length
    #def get_priority_bit(self):
       #return self.priority_bit
    #0111:新增工單與濃度List:可依照濃度排序後，作為工單顯示用
    def get_cylinder_con_list(self):
        return self.cylinder_con_list
    #0111:回傳目前的(工單，濃度)list值，提供後續排序與手動使用
    def set_cylinder_con_list(self, _rec_list):
        self.cylinder_con_list=_rec_list
    #0111:設定目前的(工單，濃度)list值，提供後續排序與手動使用
    def get_len_of_cylinder_con_list(self):
        return  len(self.cylinder_con_list)
    def get_queue_full_flag(self):#回傳目前queue是否為滿
        return  self.queue_full_flag
    def set_queue_full_flag(self, _set_flag_value):#設定目前的flag值
        self.queue_full_flag=_set_flag_value
    #0207:清除目前所排的缸list清單內容
    def clear_cylinder_con_list(self):
        _tmp=[]
        self.cylinder_con_list=_tmp