import re

import pandas as pd
import time
import os


def main():
    while True:
        filepath = input("输入文件路径:\n退出请输入$q\n")
        if filepath.lower() == "$q":
            print("退出...")
            break
        # filepath = r"F:\JetBrains\officeTools\get redbubble etsy image\ETSY\1.xlsx"
        filepath = filepath.replace("\"", "")
        df = pd.read_excel(filepath)
        df.drop_duplicates("搜索词", inplace=True)
        df = df["搜索词"]
        tmp_filepath = os.path.splitext(filepath)[0] + "-去重" + os.path.splitext(filepath)[1]
        df.to_excel(tmp_filepath)


if __name__ == "__main__":
    main()
    # a = os.listdir(r"D:\amazon\百店独立站运营\百店运营\主题采集\采集文档\常规\230220-230226\golf\230221 Etsy golf 原始数据-imgURl")
    # print(a)
    # a = [int(i.replace(".jpg", "")) for i in a]
    # df = pd.read_excel(r"D:\amazon\百店独立站运营\百店运营\主题采集\采集文档\常规\230220-230226\golf\230221 Etsy golf 原始数据-imgURl.xlsx")
    # b = pd.DataFrame({"a": a})
    # df1 = pd.merge(df, b, left_on="名称", right_on="a")
    # print(df)
    # print(df1)
    # df1.to_excel("1.xlsx")