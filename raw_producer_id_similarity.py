import pandas as pd
import pymysql
import jieba
from difflib import SequenceMatcher

# 创建数据库连接
conn = pymysql.connect(host="192.168.101.24",port=3306,user="root",passwd="jinsubao@999",db="python_ceshi" )

# 编写 SQL 查询语句
query = "SELECT * FROM raw_producer"
# 读取数据
data = pd.read_sql(query, conn)
print(data)

# 引用ratio方法，返回序列相似性的度量
def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()

# 编写 SQL 查询语句
query1 = "SELECT * FROM raw_producer_id_similarity"
# 读取数据
df1 = pd.read_sql(query1, conn)
b = df1['id'].max()+1
if pd.isna(b):
    b = 0
print(b)

# 线程池中执行的函数
def compute_similarity(i, j, data, conn):
    global b
    m = list(jieba.cut(str(data["name"][i])))
    for a in range(len(m) - 1, -1, -1):
        if m[a] in {'/', '(', ')', '（', '）'}:
            m.remove(m[a])

    n = list(jieba.cut(str(data["name"][j])))
    for a in range(len(n) - 1, -1, -1):
        if n[a] in {'/','(',')','（','）'}:
            n.remove(n[a])

    if similarity(m, n) > 0.5:
        cursor = conn.cursor()
        sql_select = "select * from raw_producer_id_similarity where raw_producer_id_Comparative_elements=%s and raw_producer_id_source=%s"
        param_select = (int(data['id'][j]), int(data['id'][i]))
        cursor.execute(sql_select, param_select)
        result = cursor.fetchone()

        if not result:  # 如果查询结果为空，则插入数据
            sql_insert = "insert into raw_producer_id_similarity values(%s,%s,%s,%s)"
            param_insert = (b,int(data['id'][j]),int(data['id'][i]),similarity(m,n))
            cursor.execute(sql_insert, param_insert)
            conn.commit()
            print((b,int(data['id'][j]),int(data['id'][i]),similarity(m,n)))
            b += 1
        else:
            print('数据已存在，无需插入')

        cursor.close()


# 提交任务到线程池中
for i in range(len(data["name"])-1):
    for j in range(i+1,len(data["name"])):
        compute_similarity(i, j, data, conn)


# 关闭连接
conn.close()
