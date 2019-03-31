class queue:#佇列檢查與連動調整模組開發
    def __init__(self):
        self.items = []
    def is_empty(self):   
        return self.items == []
    def enq(self, item):
        #if len(self.items)==size():
        #    print("the size is")
        self.items.insert(0,item)
    def deq(self):
        if len(self.items)==0:
            print("The set is empty")
        else:
            return self.items.pop()
    def size(self):
        return len(self.items)