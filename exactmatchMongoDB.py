import os
import gc
import re
import sys
import time
import pymongo
import threading
import pandas as pd
import plotly.offline
import plotly.graph_objects as go

from datetime import datetime

DEBUG = False
# threadLock = threading.Lock()
MongoDBargs = {
    'MongoDBhost': "localhost",
    'MongoDBport': 27017,
    'MongoDBusername': "***",
    'MongoDBpassword': "***",
    'MongoDBdatabase': "***"
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
        # 获取锁，用于线程同步
        # print(self.processFunc)
        # threadLock.acquire()
        self.result = self.processFunc(*self.processFuncArgs)
        # 释放锁，开启下一个线程
        # threadLock.release()


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
            # xfind = tmpcol.find(searchQuery, {"_id": 0, "部门": 0}).sort([("搜索频率排名", 1)]).limit(limitNum)
            xfind = tmpcol.find(searchQuery, {"_id": 0, "部门": 0}).limit(limitNum)
        # print(type(xfind))
        # print(xfind)
        if DEBUG:
            print("1-find耗时: {}s".format(time.time() - time_tmp))
        # print(df)
        if DEBUG:
            time_tmp = time.time()
        xfindlist = list(xfind)
        if DEBUG:
            print("2-转list耗时: {}s".format(time.time() - time_tmp))
        if len(xfindlist) > 0:
            # time_tmp = time.time()
            xfindlist = [{**i, **{"week": collectionName[:6]}} for i in xfindlist]
            # print("3-增加week耗时: {}s".format(time.time() - time_tmp))
            # print(xfindlist)
            if DEBUG:
                time_tmp = time.time()
            df = df.append(xfindlist, ignore_index=True)
            if DEBUG:
                print("3-加入df耗时: {}s".format(time.time() - time_tmp))
        else:
            tmpresult = {"week": collectionName[:6], "搜索词": searchQuery["搜索词"], "搜索频率排名": 1000000}
            df = df.append(tmpresult, ignore_index=True)
            tmpresult = {}
        if DEBUG:
            print("4-{} 耗时: {}s".format(collectionName, time.time() - time_tmp1))
            print("----------------------\n")
    if DEBUG:
        print(time.time() - time_start)
    return df


def plotlytrace(data, filename, auto_open=False):
    # print(data)
    # validnum = len(data) - data["#1 已点击的 ASIN"].isnull().sum()
    averagenum = data["搜索频率排名"].sum() / len(data)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=data["week"], y=data["搜索频率排名"], text=data["搜索频率排名"], mode="markers+lines+text", name="每周排名"))
    fig.add_trace(go.Scatter(x=data["week"], y=[averagenum for i in range(len(data))], mode="markers+lines", name="平均"))
    # fig.update_layout({'title': data["搜索词"]}, yaxis_range=[0, 100])
    # fig.update_traces(textposition='top center')
    fig.update_layout({'title': data["搜索词"][1]+"平均排名: {:.2f}".format(averagenum)})
    # print(filename + ".html")
    if filename != "":
        plotly.offline.plot(fig, filename=filename + ".html", auto_open=auto_open)
    else:
        plotly.offline.plot(fig, auto_open=auto_open)


def findallcollections(mydb, myquery, limitNum, fileName="", customlist=[], threadNum=5, savexlsx=False, auto_openhtml=False):
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
    if savexlsx:
        df.sort_values(by=["搜索词", "week", "搜索频率排名"], inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.to_excel("{}.xlsx".format(fileName), index=None)
    plotlytrace(df, fileName, auto_openhtml)
    gc.collect()
    return len(df) - df["#1 已点击的 ASIN"].isnull().sum()


def mode2():
    modeselet = input("直接回车: 返回1个结果, 输入[all]: 返回全部搜索结果, 输入数字: 返回指定数量\n")
    if modeselet == "":
        limit_num = 1
        print("已设置返回结果数量为1")
    elif modeselet == 'all':
        limit_num = modeselet
        print("已设置返回全部搜索结果")
    elif isinstance(eval(modeselet), int):
        limit_num = eval(modeselet)
    else:
        print("输入错误, 大侠请重新来过：）")
        return
    thread_num = input("输入线程数(回车默认线程为5):")
    if thread_num == "":
        thread_num = 5
        print("默认5")
    else:
        thread_num = eval(thread_num)
        if not isinstance(thread_num, int):
            print("输入错误, 大侠请重新来过：）")
        else:
            print("设置线程数量为{}".format(thread_num))
    filepath = input("文件路径:\n")
    filepath = filepath.replace("\"", "")
    resultpath = os.path.dirname(filepath) + "\\" + os.path.splitext(os.path.basename(filepath))[0] + "-报告" + "\\"
    if not os.path.isdir(resultpath):
        os.mkdir(resultpath)
    search_word = pd.read_excel(filepath, header=None)
    search_word = search_word.iloc[:, 0].tolist()
    myclient = connectMongoDB(MongoDBargs)
    mydb = switchDB(myclient, "ABAweekly")
    resultset = []
    for word in search_word:
        myquery = {"搜索词": word}
        result = findallcollections(mydb, myquery=myquery, limitNum=limit_num, fileName=resultpath + myquery["搜索词"],
                                    threadNum=thread_num, savexlsx=True)
        resultset.append((word, result))
    resultset = pd.DataFrame(resultset, columns=["搜索词", "在周榜次数"])
    resultset.to_excel(resultpath+os.path.splitext(os.path.basename(filepath))[0]+"-汇总报告.xlsx", index=None)


def mode1():
    exec_path = os.path.splitext(sys.executable)
    # resultpath = exec_path[0] + "临时文件" + "\\"
    # if not os.path.isdir(resultpath):
    #     os.mkdir(resultpath)
    myclient = connectMongoDB(MongoDBargs)
    mydb = switchDB(myclient, "ABAweekly")
    while True:
        search_word = input("输入要搜索的词组，按回车查询；退出请输入quit\n")
        search_word = search_word.lower()
        if search_word == "quit":
            print("退出...")
            break
        # for tmp in ["?",",","╲","/","*",'"',"<",">","|"]:
        if re.findall(r"^[^\\/:*?\"<>|]*$", search_word):
            if search_word == "":
                continue
            myquery = {"搜索词": search_word}
            findallcollections(mydb, myquery=myquery, limitNum=1, threadNum=5, auto_openhtml=True)
            # findallcollections(mydb, myquery=myquery, limitNum=1, fileName=resultpath+search_word, threadNum=5, auto_openhtml=True)
        else:
            print("含有非法字符：\\/:*?\"<>|")


def main():
    # username = input("请输入用户名:\n")
    # if username != "":
    #     MongoDBargs["MongoDBusername"] = username
    # password = input("请输入密码\n")
    # if password != "":
    #     MongoDBargs["MongoDBpassword"] = password
    # database = input("请输入数据库名称\n:")
    # if database != "":
    #     MongoDBargs["MongoDBdatabase"] = database
    select_mode = input("输入【1】：按输入查询， 输入【2】：按文件查询：\n")
    if select_mode == "":
        print("选择模式【1】")
        mode1()
    else:
        select_mode = eval(select_mode)
        if isinstance(select_mode, int) and select_mode == 2:
            print("选择模式【2】")
            mode2()
        else:
            print("选择模式【1】")
            mode1()


if __name__ == "__main__":
    args = sys.argv
    if len(args) >= 2:
        if args[1].lower() == "debug":
            DEBUG = True
    main()
    # test = []
    # for i in range(2):
    #     test.append((i, i+1))
    # df = pd.DataFrame(test)
    # print(df)
    # df = pd.read_excel(r"F:\JetBrains\officeTools\abaMongoDB\1-报告\its corn socks.xlsx")
    # print(df)
    # print(len(df))
    # print(df["#1 已点击的 ASIN"].isnull().sum())
