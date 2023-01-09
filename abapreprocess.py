import pandas as pd
import time
import os
import shutil
import gc

filepath = input("文件夹路径:")
os.chdir(filepath)
st = time.time()
for fd in os.listdir(filepath):
    # print(fd)
    if ".csv" in fd:
        path = os.path.abspath(fd)
        print(path)
        processresult = os.path.dirname(path) + "\\转换\\"
        processdone = os.path.dirname(path) + "\\已处理\\"
        processfile = processresult + os.path.basename(path)
        if not os.path.isdir(processresult):
            os.mkdir(processresult)
        if not os.path.isdir(processdone):
            os.mkdir(processdone)
        # print(path1)
        df = pd.read_csv(path, header=1)
        # print(df.columns)
        # df = df.fillna('null')
        df.fillna('null', inplace=True)
        df["搜索频率排名"] = df["搜索频率排名"].str.replace(",", "").astype('int32')
        # df["#1 点击共享"] = df["#1 点击共享"].str.replace("%", "").astype('float')
        # df["#1 转化共享"] = df["#1 转化共享"].str.replace("%", "").astype('float')
        # df["#2 点击共享"] = df["#2 点击共享"].str.replace("%", "").astype('float')
        # df["#2 转化共享"] = df["#2 转化共享"].str.replace("%", "").astype('float')
        # df["#3 点击共享"] = df["#3 点击共享"].str.replace("%", "").astype('float')
        # df["#3 转化共享"] = df["#3 转化共享"].str.replace("%", "").astype('float')
        # print(df["#1 点击共享"])
        df.to_csv(processfile, index=False)
        shutil.move(path, processdone)
        gc.collect()
print(time.time()-st)
