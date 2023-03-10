import os
import gc
import re
import sys
import time
import pymongo
import threading
import webbrowser
import pandas as pd
import plotly.offline
import plotly.graph_objects as go

from datetime import datetime

DEBUG = False
# threadLock = threading.Lock()
MongoDBargs = {
    'MongoDBhost': "58.22.71.130",
    'MongoDBport': 27017,
    'MongoDBusername': "aba_r",
    'MongoDBpassword': "aba_r",
    'MongoDBdatabase': "ABAweekly"
}


# df = pd.DataFrame(
#     columns=["搜索词", "搜索频率排名", "#1 已点击的 ASIN", "#1 商品名称", "#1 点击共享", "#1 转化共享", "#2 已点击的 ASIN", "#2 商品名称", "#2 点击共享",
#              "#2 转化共享", "#3 已点击的 ASIN", "#3 商品名称", "#3 点击共享", "#3 转化共享"])


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
        myclient = pymongo.MongoClient(host=MongoDBargs['MongoDBhost'], port=MongoDBargs['MongoDBport'],
                                       authSource=MongoDBargs['MongoDBdatabase'],
                                       username=MongoDBargs['MongoDBusername'], password=MongoDBargs['MongoDBpassword'])
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
        columns=["搜索词", "搜索频率排名", "#1 已点击的 ASIN", "#1 商品名称", "#1 点击共享", "#1 转化共享",
                 "#2 已点击的 ASIN", "#2 商品名称", "#2 点击共享",
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
            # print(searchQuery)
            # xfind = tmpcol.find(searchQuery, {"_id": 0, "部门": 0}).sort([("搜索频率排名", 1)])
            xfind = tmpcol.find(searchQuery, {"_id": 0, "部门": 0})
        else:
            xfind = tmpcol.find(searchQuery, {"_id": 0, "部门": 0}).sort([("搜索频率排名", 1)]).limit(limitNum)
            # xfind = tmpcol.find(searchQuery, {"_id": 0, "部门": 0}).limit(limitNum)
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
            if "搜索词" in searchQuery.keys():
                tmpresult = {"week": collectionName[:6],
                             "搜索词": str(searchQuery["搜索词"]) if not isinstance(searchQuery["搜索词"], str) else
                             searchQuery["搜索词"], "搜索频率排名": 1000000}
            else:
                tmpresult = {"week": collectionName[:6],
                             "搜索词": str(searchQuery), "搜索频率排名": 1000000}
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
    ranknum = len(data) - data["#1 已点击的 ASIN"].isnull().sum()
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(x=data["week"], y=data["搜索频率排名"],
                   hovertext=data["week"].astype("string") + ": " + data["搜索频率排名"].astype("string"),
                   text=data["搜索频率排名"], mode="markers+lines+text",
                   name="每周排名", textposition="top center"))
    fig.add_trace(
        go.Scatter(x=data["week"], y=[averagenum for i in range(len(data))], mode="lines",
                   line=dict(dash="longdashdot", width=3), name="平均"))
    # fig.update_layout({'title': data["搜索词"]}, yaxis_range=[0, 100])
    # fig.update_traces(textposition='top center')
    fig.update_layout({'title': data["搜索词"][1] + "平均排名: {:.2f}, 在榜周数: {}".format(averagenum, ranknum)})
    # print(filename + ".html")
    if filename != "":
        filename = filename + ".html"
        plotly.offline.plot(fig, filename=filename, auto_open=False)
    else:
        plotly.offline.plot(fig, auto_open=False)
        filename = os.getcwd() + "\\" + "temp-plot.html"
        # input(filename)
    if auto_open:
        webbrowser.open(filename)


def findallcollections(mydb, myquery, limitNum, fileName="", customlist=[], threadNum=5, savexlsx=False,
                       auto_openhtml=False):
    df = pd.DataFrame(
        columns=["搜索词", "搜索频率排名", "#1 已点击的 ASIN", "#1 商品名称", "#1 点击共享", "#1 转化共享",
                 "#2 已点击的 ASIN", "#2 商品名称", "#2 点击共享",
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
    # thread1 = myThread(1, "Thread-1", findresult, (mydb, ["211128-211204"], myquery))
    # thread2 = myThread(2, "Thread-2", findresult, (mydb, ["211205-211211"], myquery))
    # thread1 = myThread(1, "Thread-1", findresult, (mydb, collist[:len(collist)//2], myquery))
    # thread2 = myThread(2, "Thread-2", findresult, (mydb, collist[len(collist)//2:], myquery))
    cnt = len(collist) // threadNum
    cnt = cnt + 1 if (len(collist) % threadNum) != 0 else 0
    threads = [myThread(i + 1, "Thread-" + str(i + 1), findresult,
                        (mydb, collist[cnt * i:cnt * (1 + i)], myquery, limitNum)) if i != (
            threadNum - 1) else myThread(i, "Thread-" + str(i), findresult,
                                         (mydb, collist[cnt * i:], myquery, limitNum)) for i in range(threadNum)]

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
    if limitNum == 1:
        plotlytrace(df, fileName, auto_openhtml)
    gc.collect()
    return len(df) - df["#1 已点击的 ASIN"].isnull().sum(), df["搜索频率排名"].sum() / len(df)


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
    # input(resultpath)
    if not os.path.isdir(resultpath):
        os.mkdir(resultpath)
    # search_word = pd.read_excel(filepath, header=None)
    df = pd.read_excel(filepath)
    df.fillna('$null', inplace=True)
    search_word = df.iloc[:, 0].tolist()
    file_name = df.iloc[:, 1].tolist()
    # print(file_name)
    # print(search_word)
    myclient = connectMongoDB(MongoDBargs)
    mydb = switchDB(myclient, "ABAweekly")
    time_start = time.time()
    resultset = []
    for word, filename, cnt in zip(search_word, file_name, range(1, len(file_name) + 1)):
        if re.findall("{.*}", word):
            # print(type(word))
            word = eval(word)
            # input(type(word))
        if filename != '$null':
            if re.findall(r"^[^\\/:*?\"<>|]*$", filename):
                pass
            else:
                filename = str(cnt)
        else:
            filename = str(cnt)
        myquery = {"搜索词": word}
        # input(myquery)
        # input(myquery["搜索词"])
        # input(type(myquery["搜索词"]))
        result = findallcollections(mydb, myquery=myquery, limitNum=limit_num, fileName=resultpath + filename,
                                    threadNum=thread_num, savexlsx=True)
        # resultset.append((word, result,resultpath+myquery["搜索词"]+".xlsx"))
        resultset.append((word, result[0], result[1]))
    # resultset = pd.DataFrame(resultset, columns=["搜索词", "在周榜次数", "文件路径"])
    resultset = pd.DataFrame(resultset, columns=["搜索词", "在周榜次数", "ABA平均排名"])
    resultset.to_excel(resultpath + os.path.splitext(os.path.basename(filepath))[0] + "-汇总报告.xlsx", index=None)
    time_end = time.time()
    input("已生成报告, 耗时时间:{:.2f}, 平均耗时:{:.2f}, 按回车键结束".format(time_end - time_start,
                                                                              (time_end - time_start) / len(df)))
    # myquery = {"搜索词": {"$regex": "embroid.* hat"}}
    # findallcollections(mydb, myquery=myquery, limitNum=1 if modeselet == 'one' else modeselet, fileName="1", customlist=["211128-211204"], threadNum=thread_num)


def mode1():
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
    exec_path = os.path.splitext(sys.executable)
    resultpath = exec_path[0] + " datafileABA" + "\\"
    if not os.path.isdir(resultpath):
        os.mkdir(resultpath)
    myclient = connectMongoDB(MongoDBargs)
    mydb = switchDB(myclient, "ABAweekly")
    while True:
        search_word = input("输入要搜索的词组，按回车查询；退出请输入$q\n")
        # search_word = search_word.lower()
        if search_word.lower() == "$q":
            print("退出...")
            break
        # for tmp in ["?",",","╲","/","*",'"',"<",">","|"]:
        if re.findall(r"^[^\\/:*?\"<>|]*$", search_word):
            if search_word == "":
                continue
            search_word = search_word.lower()
            myquery = {"搜索词": search_word}
            filename = resultpath + search_word
            findallcollections(mydb, myquery=myquery, limitNum=1, fileName=filename, threadNum=thread_num, auto_openhtml=True)
        elif re.findall("{.*}", search_word) and isinstance(search_word, str):
            # print(type(word))
            search_word = eval(search_word)
            myquery = search_word
            filename = resultpath + "temp-plot1"
            findallcollections(mydb, myquery=myquery, limitNum="all", fileName=filename, threadNum=5, savexlsx=True,
                               auto_openhtml=True)
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
    # print(df["week"][0])
    # validnum = len(df) - df["#1 已点击的 ASIN"].isnull().sum()
    # print(df["搜索频率排名"].sum()/len(df))

    # print(df)
    # print(len(df))
    # print(df["#1 已点击的 ASIN"].isnull().sum())
    # plotly.offline.plot(fig, filename=filename + ".html", auto_open=auto_open)
