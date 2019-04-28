


print("*"*100)

import time
time.sleep(3)

for i in range(20):
    print(i)


print('over')

from openpyxl import Workbook

# 实例化工作簿
work_book = Workbook()
# 激活工作表 / 添加工作表
sheet = work_book.active
# 修改工作表名称
sheet.title = "new"
data = [
    [1, "张三", "男"],
    [2, "李四", "女"],
    [3, "王五", "女"],
]
# 写入数据
for each in data:
    sheet.append(each)

# 保存为本地文件
work_book.save("E:\\py\\index\\testing\\utiltest\\algorithm\\result.xlsx")
# 关闭工作簿
work_book.close()

