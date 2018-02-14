class material:
    def __init__(self,_cist_no,_name,_con_name_list, _con_list):
        self.mat_cist_no=_cist_no#工單號(cist_no)
        self.mat_name=_name#染料色號(clr_id)
        mat_con_name_list=[]
        self.mat_con_name_list=_con_name_list#染料名稱list
        mat_con_list=[]
        self.mat_con_list=_con_list#染料濃度list
    def get_mat_cist_no(self):
        return self.mat_cist_no
    def get_mat_name(self):
        return self.mat_name
    def get_len_of_mat_con_name_list(self):
        return len(mat_con_name_list)
    def get_len_of_mat_con_list(self):
        return len(mat_con_list)
