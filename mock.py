from faker import Faker
from random import random
import time
import json
import pymysql.cursors
from pysqler import Insert
from datetime import datetime, timedelta
import setting
from mysql import get_conn


fake = Faker()


conn = get_conn()

while True:
    try:
        interval = fake.random_int(min=1, max=5) * 0.1
        time.sleep(interval)
        item = dict()
        item["user_mail"] = fake.safe_email()
        status = fake.random_choices(elements=('unpaid', 'paid', 'cancel','shipping', 'finished'), length=1)[0]
        item["status"] = status
        item["good_count"] = fake.random_int(min=1, max=10)
        item["city"] = fake.city()
        item["amount"] = round(fake.random_int(min=1000, max=100000)*0.01, 2)
        
        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        if status == 'unpaid':
            item["create_time"]= now_str
            item["update_time"]= now_str
        else:
            day = fake.random_int(min=1, max=10)
            item["create_time"]= (now - timedelta(days=day)).strftime("%Y-%m-%d %H:%M:%S")
            item["update_time"]= now_str
            
        
        command = Insert("`{0}`".format(setting.Focus_TB))
        for key in item:
            command.put(key, item[key])
                
            
        
        print("mock: {0}".format(item))
        with conn.cursor() as cursor:
            sql = str(command)
            cursor.execute(sql)
            conn.commit()
            
    except Exception as e:
        print(e)
        time.sleep(60)
        conn.close()
        conn = get_conn()

