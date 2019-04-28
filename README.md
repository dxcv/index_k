# IndexCalculation
指标计算库

# 目录结构
一. SCRIPT: 脚本文件夹
  #存放其中的脚本应提供main函数作为入口启动;
  1) FACTOR: 因子定时计算脚本目录;
  2) FINANCE: 金融数据清洗脚本目录;
  3) MUTUAL: 公募清洗, 定时计算脚本目录;
  4) PRIVATE: 私募清洗, 定时计算脚本目录;
  5) SYNC: 同步脚本目录;

二. utils: 组件, 中间件
  1) algorithm: 内部算法库;
  2) decofactory: 装饰器工具类库;
  3) etlkit: 清洗工具库;
  4) sqlfactory: SQL工具类库;
  5) synckit: 同步工具库;

三. test: 测试文件夹
