# https://www.itread01.com/content/1521659196.html

import multiprocessing as mp
import os
from time import sleep
import time

def worker(msg):
    print(os.getpid())
    sleep(2)
    print(msg)
    return msg

if __name__ == '__main__':
    def timer(n):
        while True:
            # 創建進程池對象
            p1 = mp.Pool(processes=16)  # 創建4條進程
            p2 = mp.Pool(processes=16)

            pool_result = []
            for i in range(10):
                msg = 'hello - % d' % i
                r = p1.apply_async(worker, (msg,))  # 向進程池中添加事件
                pool_result.append(r)

            for j in range(10):
                msg = 'bye - % d' % j
                r = p2.apply_async(worker, (msg,))  # 向進程池中添加事件
                pool_result.append(r)

            # 獲取事件函數的返回值
            #for r in pool_result:
            #    print('return:', r)

            p1.close()
            p1.join()  # 等待進程池中的事件執行完畢，回收進程池
            p2.close()
            p2.join()

            time.sleep(n)
    timer(3)
