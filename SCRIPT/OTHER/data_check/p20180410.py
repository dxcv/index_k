from utils.database import config as cfg
import pandas as pd
import re
engine_base = cfg.load_engine()["2Gb"]
engine_test = 11


sub_wrong_to_none = lambda x: re.sub("\s|-|--|---", "", x) if type(x) is str else x


def check_dataframe(dataframe, columns_to_check):
    dataframe[columns_to_check] = dataframe[columns_to_check].applymap(lambda x: sub_wrong_to_none(x))
    return dataframe


def to_list(df):
    a = np.array(df)#np.ndarray()
    vv=a.tolist()#list
    return vv


def diff(listA, listB):
    retA = [i for i in listA if i in listB]
    return retA


def pk(T1, T2):
    table = pd.read_sql("show columns from {}".format(T1), engine_base)
    t1 = table["Field"]
    t11 = to_list(t1)

    table2 = pd.read_sql("show columns from {}".format(T2), engine_test)
    t2 = table2["Field"]
    t22 = to_list(t2)

    x1 = diff(t11, t22)
    x = x1[:-3]
    str4 = ",".join(x)

    info = pd.read_sql("select {} from {}".format(str4, T1), engine_base)
    info2 = pd.read_sql("select {} from {}".format(str4, T2), engine_test)
    df = check_dataframe(info, x)
    df2 = check_dataframe(info2, x)
    # del info["fund_consultant"]
    # df.drop('fund_consultant',axis=1, inplace=True)
    # info["currency_type"]=info["currency"]
    # info.rename(columns={"fund_manager":"fund_consultant"},inplace=True)
    #
    # info.rename(columns={"fund_member":"fund_manager","locked_time_limit":"limit_time"},inplace=True)
    # info["limit_date"]=info["limit_time"]
    c = df.count()
    y = len(info)
    df_a = c.reset_index()
    df_a.columns = ["字段名字", "非空字段数量"]
    df_a["空字段数量"] = y - df_a["非空字段数量"]
    df_a["非空百分比"] = df_a["非空字段数量"] / y
    c2 = df2.count()
    df_b = c2.reset_index()
    u = len(info2)
    df_b.columns = ["字段名字", "old非空字段数量"]
    df_b["old空字段"] = u - df_b["old非空字段数量"]
    df_b["old非空百分比"] = df_b["old非空字段数量"] / u
    df_all = df_b.merge(df_a, on="字段名字", how="left")
    df_all["改进后"] = df_all["非空百分比"] - df_all["old非空百分比"]
    df_all["改进后"] = df_all["改进后"].apply(lambda x: '%.2f' % (x * 100))
    df_all["改进后"] = df_all["改进后"].apply(lambda x: str(x) + '%')
    df_all["非空字段数量"] = df_all["非空字段数量"].apply(lambda x: float('%.0f' % x))
    df_all["空字段数量"] = df_all["空字段数量"].apply(lambda x: '%.0f' % x)
    df_all["old非空百分比"] = df_all["old非空百分比"].apply(lambda x: '%.2f' % (x * 100))
    df_all["old非空百分比"] = df_all["old非空百分比"].apply(lambda x: str(x) + '%')
    df_all["非空百分比"] = df_all["非空百分比"].apply(lambda x: '%.2f' % (x * 100))
    df_all["非空百分比"] = df_all["非空百分比"].apply(lambda x: str(x) + '%')
    return df_all


T1 = 'org_info'
T2 = 'org_info_20180327'
# dict={"locked_time_limit":"limit_time"}

df_all = pk(T1, T2)
# to_table(df_all)


new = df_all.iloc[:, [0, 3, 6]]

new2 = new[new.iloc[:, 1] != new.iloc[:, 2]]

new2.iloc[:, 1] = new2.iloc[:, 1].apply(lambda x: float(re.sub("%", "", x)))
new2.iloc[:, 2] = new2.iloc[:, 2].apply(lambda x: float(re.sub("%", "", x)))

new3 = new2[(new2.iloc[:, 2] - new2.iloc[:, 1]) > 2]

x1 = new3.iloc[:, 0]
xs = to_list(x1)

y1 = new3.iloc[:, 1]
ys = to_list(y1)

y2 = new3.iloc[:, 2]
ys2 = to_list(y2)

# ------------------------------------------------------3D图

import random
import matplotlib as mpl
import matplotlib.pyplot as plt

mpl.rcParams['font.size'] = 6.5
from mpl_toolkits.mplot3d import Axes3D

# fig = plt.figure()
fig = plt.figure(figsize=(8, 6))
ax = fig.add_subplot(111, projection='3d')

ax = fig.add_subplot(111, projection='3d')
z = 1
color = plt.cm.Set2(random.choice(range(plt.cm.Set2.N)))
color = "lightblue"
ax.bar(xs, ys, zs=z, zdir='y', color=color, alpha=0.8)
ax.yaxis.set_major_locator(mpl.ticker.FixedLocator(xs))
ax.yaxis.set_major_locator(mpl.ticker.FixedLocator(ys))
z1 = 2
color = plt.cm.Set2(random.choice(range(plt.cm.Set2.N)))
color = "Khaki"
ax.bar(xs, ys2, zs=z1, zdir='y', color=color, alpha=0.8)
ax.yaxis.set_major_locator(mpl.ticker.FixedLocator(xs))
ax.yaxis.set_major_locator(mpl.ticker.FixedLocator(ys2))
ax.set_xlabel('percentage', fontsize=11)
ax.set_ylabel('old,new', fontsize=14)
ax.set_zlabel(T1 + ' for improvement', fontsize=12)
plt.show()
# ------------------------------------------------------------3图




# ----------------------------------条状图
import numpy as np
import matplotlib.pyplot as plt

N = len(xs)
# menMeans = (20, 35, 30, 35, 27)
# menStd = (2, 3, 4, 1, 2)

ind = np.arange(N)  # the x locations for the groups
width = 0.4  # the width of the bars
size = 5
fig, ax = plt.subplots()
color2 = "lightblue"
rects1 = ax.bar(ind, ys, width, color=color2)  # yerr=menStd)

# womenMeans = (25, 32, 34, 20, 25)
# womenStd = (3, 5, 2, 3, 3)

color = "Khaki"
rects2 = ax.bar(ind + width, ys2, width, color=color)

# add some text for labels, title and axes ticks
ax.set_ylabel('percentage', fontsize=10)
ax.set_title('tables for improvement', fontsize=10)
ax.set_xticks(ind + width)
ax.set_xticklabels(xs)

ax.legend((rects1[0], rects2[0]), ('new', 'old'))

plt.xticks(fontsize=7)
plt.yticks(fontsize=7)


def autolabel(rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() / 2., 1.005 * height,
                '%d' % int(height),
                ha='center', va='bottom')


autolabel(rects1)
autolabel(rects2)

# plt.legend(handles=[blue_line,red_line],loc='upper left')
plt.show()
