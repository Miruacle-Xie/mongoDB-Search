import os
import gc
import re
import sys
import time
import random
import pymongo
import threading
import webbrowser
import numpy as np
import pandas as pd
import plotly.offline
import plotly.graph_objects as go
import requestgtrend

from datetime import datetime
from plotly.subplots import make_subplots

DEBUG = False
MongoDBargs = {
    'MongoDBhost': "58.22.71.130",
    'MongoDBport': 27017,
    'MongoDBusername': "aba_r",
    'MongoDBpassword': "aba_r",
    'MongoDBdatabase': "ABAweekly"
}


class myThread(threading.Thread):
    def __init__(self, threadID, name, processFunc, args):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.processFunc = processFunc
        self.processFuncArgs = args
        self.result = None

    def run(self):
        if DEBUG:
            print("开启线程： " + self.name)
        self.result = self.processFunc(*self.processFuncArgs)


def connectMongoDB(MongoDBargs):
    # connect MongoDB
    try:
        myclient = pymongo.MongoClient(host=MongoDBargs['MongoDBhost'], port=MongoDBargs['MongoDBport'], authSource=MongoDBargs['MongoDBdatabase'], username=MongoDBargs['MongoDBusername'], password=MongoDBargs['MongoDBpassword'])
        print("连接服务器成功")
        return myclient
    except Exception as e:
        print(repr(e))
        print("连接服务器异常")
        return False


def switchDB(mongoClient, dbName):
    try:
        mydb = mongoClient[dbName]
        return mydb
    except Exception as e:
        print(repr(e))
        print("切换{}数据库异常".format(dbName))
        return False


def findresult(usedb, collectionNames, searchQuery, limitNum=1):
    df = pd.DataFrame(
        columns=["搜索词", "搜索频率排名", "#1 已点击的 ASIN", "#1 商品名称", "#1 点击共享", "#1 转化共享", "#2 已点击的 ASIN", "#2 商品名称", "#2 点击共享",
                 "#2 转化共享", "#3 已点击的 ASIN", "#3 商品名称", "#3 点击共享", "#3 转化共享"])
    if DEBUG:
        time_start = time.time()
    for collectionName, cnt in zip(collectionNames, range(1, len(collectionNames) + 1)):
        if DEBUG:
            time_tmp1 = time.time()
            print(cnt)
            print(collectionName)
            print(datetime.strptime(collectionName[:6], '%y%m%d').strftime("%Y/%m/%d"))
            time_tmp = time.time()
        tmpcol = usedb[collectionName]
        if limitNum == 'all':
            if DEBUG:
                print('find-all')
            # xfind = tmpcol.find(searchQuery, {"_id": 0, "部门": 0}).sort([("搜索频率排名", 1)])
            xfind = tmpcol.find(searchQuery, {"_id": 0, "部门": 0})
        else:
            xfind = tmpcol.find(searchQuery, {"_id": 0, "部门": 0}).sort([("搜索频率排名", 1)]).limit(limitNum)
        if DEBUG:
            print("1-find耗时: {}s".format(time.time() - time_tmp))
        if DEBUG:
            time_tmp = time.time()
        xfindlist = list(xfind)
        if DEBUG:
            print("2-转list耗时: {}s".format(time.time() - time_tmp))
        if len(xfindlist) > 0:
            xfindlist = [{**i, **{"week": collectionName[:6]}} for i in xfindlist]
            if DEBUG:
                time_tmp = time.time()
            df = df.append(xfindlist, ignore_index=True)
            if DEBUG:
                print("3-加入df耗时: {}s".format(time.time() - time_tmp))
        else:
            tmpresult = {"week": collectionName[:6], "搜索词": str(searchQuery["搜索词"]) if type(searchQuery["搜索词"]) != type("string") else searchQuery["搜索词"], "搜索频率排名": 1000000}
            df = df.append(tmpresult, ignore_index=True)
        if DEBUG:
            print("4-{} 耗时: {}s".format(collectionName, time.time() - time_tmp1))
            print("----------------------\n")
    if DEBUG:
        print(time.time() - time_start)
    return df


def plotlytrace(data1, data2, filename, auto_open=False):
    # print(data)
    # validnum = len(data) - data["#1 已点击的 ASIN"].isnull().sum()
    averagenum = data1["搜索频率排名"].sum() / len(data1)
    ranknum = len(data1) - data1["#1 已点击的 ASIN"].isnull().sum()
    # fig = go.Figure()
    fig = make_subplots(rows=3, cols=1, subplot_titles=("ABA趋势图", "ABA逆向趋势图", "Google趋势图"))
    fig.add_trace(go.Scatter(x=data1["week"], y=data1["搜索频率排名"], text=data1["搜索频率排名"], mode="markers+lines+text", name="每周排名", textposition="top center"), row=1, col=1)
    fig.add_trace(go.Scatter(x=data1["week"], y=[averagenum for i in range(len(data1))], mode="lines", line=dict(dash="longdashdot", width=3), name="ABA平均值"), row=1, col=1)
    fig.update_layout({'title': data1["搜索词"][1]+"平均排名: {:.2f}, 在榜周数: {}".format(averagenum, ranknum)})

    re_averagenum = 1000000-data1["搜索频率排名"].sum() / len(data1)
    fig.add_trace(
        go.Scatter(x=data1["week"], y=1000000-data1["搜索频率排名"], text=1000000-data1["搜索频率排名"], mode="markers+lines+text",
                   name="每周排名", textposition="top center"), row=2, col=1)
    fig.add_trace(
        go.Scatter(x=data1["week"], y=[re_averagenum for i in range(len(data1))], mode="lines", line=dict(dash="longdashdot", width=3), name="ABA平均值"),
        row=2, col=1)

    if (data2 is not None) and (data2 is not False):
        fig.add_trace(go.Scatter(x=data2["date"], y=data2[data2.columns.values[1]], text=data2["date"], mode="markers+lines", name="5year", xaxis="x2", yaxis="y2"), row=3, col=1)
        xloc = np.where(data2["date"] == data1["week"][0])
        yloc = np.where(data2["date"] == data1["week"][len(data1)-1])
        xloc = xloc[0].tolist()
        yloc = yloc[0].tolist()
        fig.add_trace(go.Scatter(x=data2["date"][xloc[0]: yloc[0]+1], y=data2[data2.columns.values[1]][xloc[0]: yloc[0]+1], mode="markers+lines", name="ABA同周期", xaxis="x2", yaxis="y2"), row=3, col=1)
    if filename != "":
        filename = filename + ".html"
        plotly.offline.plot(fig, filename=filename, auto_open=False)
    else:
        plotly.offline.plot(fig, auto_open=False)
        filename = os.getcwd() + "\\" + "temp-plot.html"
        # input(filename)
    if auto_open:
        webbrowser.open(filename)


def findallcollections(mydb, myquery, limitNum, fileName="", customlist=[], threadNum=5, savexlsx=False):
    df = pd.DataFrame(
        columns=["搜索词", "搜索频率排名", "#1 已点击的 ASIN", "#1 商品名称", "#1 点击共享", "#1 转化共享", "#2 已点击的 ASIN", "#2 商品名称", "#2 点击共享",
                 "#2 转化共享", "#3 已点击的 ASIN", "#3 商品名称", "#3 点击共享", "#3 转化共享"])

    collist = mydb.list_collection_names()
    if customlist:
        collist = [tmp for tmp in customlist if tmp in collist]
    # input(collist)
    if not collist:
        print("无对应数据表")
        return False

    # 创建新线程
    time_start = time.time()
    cnt = len(collist) // threadNum
    cnt = cnt + 1 if (len(collist) % threadNum) != 0 else 0
    threads = [myThread(i+1, "Thread-"+str(i+1), findresult, (mydb, collist[cnt*i:cnt*(1+i)], myquery, limitNum)) if i != (threadNum-1) else myThread(i, "Thread-"+str(i), findresult, (mydb, collist[cnt*i:], myquery, limitNum)) for i in range(threadNum)]

    # 开启新线程
    print("正在查询请稍等...")
    for t in threads:
        t.start()

    # 等待所有线程完成
    for t in threads:
        t.join()

    for t in threads:
        df = df.append(t.result, ignore_index=True)
    if DEBUG:
        print("退出主线程")
    print("{}-查询耗时:{}".format(myquery, time.time() - time_start))
    # print(df)
    df["week"] = pd.to_datetime(df["week"], format="%y%m%d")
    df.sort_values(by=["week", "搜索频率排名"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    columns = list(df.columns)
    df = df[[columns[-1], *columns[:-1]]]
    if DEBUG:
        print(df)
    # df.to_excel("exact-{}.xlsx".format(myquery['搜索词']), index=None)
    if savexlsx:
        # df.to_excel("{}.xlsx".format(fileName), index=None)
        df.sort_values(by=["搜索词", "week", "搜索频率排名"], inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.to_excel("{}.xlsx".format(fileName), index=None)
    gc.collect()
    return df


def searchABAGtrend():
    exec_path = os.path.splitext(sys.executable)
    resultpath = exec_path[0] + " datafile" + "\\"
    if not os.path.isdir(resultpath):
        os.mkdir(resultpath)
    myclient = connectMongoDB(MongoDBargs)
    mydb = switchDB(myclient, "ABAweekly")
    config = requestgtrend.configfunc()
    os.environ["https_proxy"] = config["https_proxy"][random.randint(0, len(config["https_proxy"]) - 1)]
    print(os.environ["https_proxy"])
    headers = {key: config[key] for key in config.keys() if
               key == "user-agent" or key == "authority" or key == "cookie"}
    trends = requestgtrend.GtrendReq(hl=config["hl"], tz=config["tz"], retries=config["retries"])
    trends.headers = headers
    while True:
        search_word = input("输入要搜索的词组，按回车查询；退出请输入$q\n")
        search_word = search_word.lower()
        if search_word == "$q":
            print("退出...")
            break
        if re.findall(r"^[^\\/:*?\"<>|]*$", search_word):
            if search_word == "":
                continue
            myquery = {"搜索词": search_word}
            filename = search_word
            df_aba = findallcollections(mydb, myquery=myquery, limitNum=1, fileName=resultpath + filename, threadNum=5)
            df_gtrend = getGoogleTrend(trends, config, search_word, resultpath + filename)
            plotlytrace(df_aba, df_gtrend, resultpath + filename, True)
        else:
            print("含有非法字符：\\/:*?\"<>|")


def searchABAGtrendByFile():
    filepath = input("文件路径:\n")
    filepath = filepath.replace("\"", "")
    df = pd.read_excel(filepath)
    df.fillna('$null', inplace=True)
    search_word = df.iloc[:, 0].tolist()
    file_name = df.iloc[:, 1].tolist()
    exec_path = os.path.splitext(sys.executable)
    resultpath = exec_path[0] + " datafile" + "\\"
    if not os.path.isdir(resultpath):
        os.mkdir(resultpath)
    myclient = connectMongoDB(MongoDBargs)
    mydb = switchDB(myclient, "ABAweekly")
    config = requestgtrend.configfunc()
    os.environ["https_proxy"] = config["https_proxy"][random.randint(0, len(config["https_proxy"]) - 1)]
    print(os.environ["https_proxy"])
    headers = {key: config[key] for key in config.keys() if
               key == "user-agent" or key == "authority" or key == "cookie"}
    trends = requestgtrend.GtrendReq(hl=config["hl"], tz=config["tz"], retries=config["retries"])
    trends.headers = headers
    resultset = []
    time_start = time.time()
    for word, filename, cnt in zip(search_word, file_name, range(1, len(file_name)+1)):
        word = word.lower()
        if filename != '$null':
            if re.findall(r"^[^\\/:*?\"<>|]*$", filename):
                pass
            else:
                filename = str(cnt)
        else:
            filename = str(cnt)
        myquery = {"搜索词": word}
        df_aba = findallcollections(mydb, myquery=myquery, limitNum=1, fileName=resultpath + filename, threadNum=5)
        df_gtrend = getGoogleTrend(trends, config, word, resultpath + filename)
        plotlytrace(df_aba, df_gtrend, resultpath + filename, False)
        ranknum = len(df_aba) - df_aba["#1 已点击的 ASIN"].isnull().sum()
        excel_hyperlink = "=HYPERLINK(\"" + resultpath + filename + ".html\")"
        if df_gtrend is False:
            gtrendflag = '查询异常'
        elif df_gtrend is None:
            gtrendflag = '无谷歌趋势'
        else:
            gtrendflag = '有谷歌趋势'
        resultset.append((word, gtrendflag, ranknum, excel_hyperlink))
    df_resultset = pd.DataFrame(resultset, columns=["搜索词", "谷歌趋势", "ABA趋势在榜周数", "趋势图链接"])
    df_resultset.to_excel(resultpath+os.path.splitext(os.path.basename(filepath))[0]+"-汇总报告.xlsx", index=None)
    time_end = time.time()
    input("已生成报告, 耗时时间:{:.2f}, 平均耗时:{:.2f}, 按回车键结束".format(time_end - time_start, (time_end - time_start) / len(df)))


def getGoogleTrend(trends, config, searchword, filename):
    try:
        #  五年趋势
        print("{}-请求5年POST...".format(searchword))
        postflag = trends.build_payload([searchword], timeframe="today 5-y")
        if not postflag:
            raise NameError("{}:POST请求异常".format(searchword))
    except NameError as e:
        # print(repr(e))
        print("{}:请求5年POST异常".format(searchword))
        print("#=============================================================================\n")
        os.environ["https_proxy"] = config["https_proxy"][random.randint(0, len(config["https_proxy"]) - 1)]
        print("切换至:" + os.environ["https_proxy"] + "\n")
        time.sleep(random.uniform(1, 3))
        return False

    print("\n请求5年趋势...")
    df = trends.interest_over_time()
    if df is None:
        print("{}-请求5年趋势超时异常".format(searchword))
        os.environ["https_proxy"] = config["https_proxy"][random.randint(0, len(config["https_proxy"]) - 1)]
        print("切换至:" + os.environ["https_proxy"] + "\n")
        time.sleep(random.uniform(1, 3))
        return None

    if df.empty:
        print("{}-主题无趋势".format(searchword))
        return None
    else:
        # 绘制图形
        df = df.reset_index()
        df.to_excel(filename+"-gtrend.xlsx")
        os.environ["https_proxy"] = config["https_proxy"][random.randint(0, len(config["https_proxy"]) - 1)]
        print("切换至:" + os.environ["https_proxy"] + "\n")
        time.sleep(random.uniform(1, 3))
        return df


def main():
    select_mode = input("输入【1】：按输入查询， 输入【2】：按文件查询：\n")
    if select_mode == "":
        print("选择模式【1】")
        searchABAGtrend()
    else:
        select_mode = eval(select_mode)
        if isinstance(select_mode, int) and select_mode == 2:
            print("选择模式【2】")
            searchABAGtrendByFile()
        else:
            print("选择模式【1】")
            searchABAGtrend()


if __name__ == "__main__":
    main()
