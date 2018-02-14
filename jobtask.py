#from cylinder import cylinder
class jobtask:
    def __init__(self,_cist_no,_od_time,_od_no,_emb_qty,_qty_weight,_yard_length,_dying_color):
        self.job_cist_no=_cist_no#工單號
        self.job_od_time=_od_time#完成時間
        self.job_od_no=_od_no#訂單號碼
        self.job_emb_qty=_emb_qty#投胚量
        self.job_qty_weight=_qty_weight#碼重(g)要換成kg
        self.job_dying_color=_dying_color#染色號
        self.job_yard_length=_yard_length#碼長
    def get_cist_no(self):
        return self.job_cist_no
    def get_od_time(self):
        return self.job_od_time
    def get_od_no(self):
        return self.job_od_no
    def get_emb_qty(self):
        return self.job_emb_qty
    def get_qty_weight(self):
        return self.job_qty_weight
    def get_dying_color(self):
        return self.job_dying_color
    def get_yard_length(self):
        return self.job_yard_length
   