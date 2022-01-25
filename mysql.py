import pymysql.cursors
import setting

def get_conn():
    connection = pymysql.connect(host=setting.HOST,
                             user=setting.USER,
                             password=setting.PWD,
                             database= setting.DB,
                             cursorclass=pymysql.cursors.DictCursor)
    return connection

def get_binlog_info():
    conn = get_conn()
   
    with conn.cursor() as cursor:
        sql = 'show master status;'
        cursor.execute(sql)
        conn.commit()
        t = cursor.fetchone()
        return t['File'], t['Position']
    
    
def fetch_one(sql: str):
    conn = get_conn()
   
    with conn.cursor() as cursor:
        cursor.execute(sql)
        conn.commit()
        t = cursor.fetchone()
        return t