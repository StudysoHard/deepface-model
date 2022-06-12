# connect_db：连接数据库，并操作数据库
 
import pymysql
from deepface.milvus import snowFlow as snowflow

 
 
class OperationMysql:
    """
    数据库SQL相关操作
    import pymysql
# 打开数据库连接
db = pymysql.connect("localhost","testuser","test123","TESTDB" )
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()
# 使用 execute()  方法执行 SQL 查询
cursor.execute("SELECT VERSION()")
    """
 
    def __init__(self):
        # 创建一个连接数据库的对象
        self.conn = pymysql.connect(
            host='101.43.42.54',  # 连接的数据库服务器主机名
            port=3306,  # 数据库端口号
            user='root',  # 数据库登录用户名
            passwd='root',
            db='face-data',  # 数据库名称
            charset='utf8',  # 连接编码
            cursorclass=pymysql.cursors.DictCursor
        )
        # 使用cursor()方法创建一个游标对象，用于操作数据库
        self.cur = self.conn.cursor()
 
    # 查询一条数据
    def search_one(self, sql):
        self.cur.execute(sql)
        result = self.cur.fetchone()  # 使用 fetchone()方法获取单条数据.只显示一行结果
        # result = self.cur.fetchall()  # 显示所有结果
        return result
 
    # 更新SQL
    def updata_one(self, sql):
        try:
            self.cur.execute(sql)  # 执行sql
            self.conn.commit()  # 增删改操作完数据库后，需要执行提交操作
        except:
            # 发生错误时回滚
            self.conn.rollback()
        self.conn.close()  # 记得关闭数据库连接
 
    # 插入SQL
    def insert_one(self,milvus_id,camera_id,insert_time):
        sql = "insert into captured_face(id,milvus_id,camera_id,insert_time) values(%s,%s,%s,%s)"
        # 雪花算法的id
        snowId = snowflow.IdWorker(1, 2, 0)
        try:
            self.cur.execute(sql,(snowId.get_id(),milvus_id,camera_id,insert_time))  # 执行sql
            self.conn.commit()  # 增删改操作完数据库后，需要执行提交操作
        except:
            # 发生错误时回滚
            self.conn.rollback()
        self.conn.close()
 
    # 删除sql
    def delete_one(self, sql):
        try:
            self.cur.execute(sql)  # 执行sql
            self.conn.commit()  # 增删改操作完数据库后，需要执行提交操作
        except:
            # 发生错误时回滚
            self.conn.rollback()
        self.conn.close()
 
 
# if __name__ == '__main__':
#     op_mysql = OperationMysql()
#     res = op_mysql.search_one("SELECT *  from camera")
#     sql = "insert into captured_face(id,milvus_id,camera_id,insert_time) values(%s,%s,%s,%s)"
#     res = op_mysql.insert_one(sql)
#     print(res)