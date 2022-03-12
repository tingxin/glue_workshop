from faker import Faker
from random import random
import time
from pysqler import Insert
from datetime import datetime, timedelta


fake = Faker()


def gen(interval_min=1000, interval_max=3000):
    """
    间隔随机毫秒数生成模拟订单数据
    :param interval_min: 最小毫秒数
    :param interval_max: 最大毫秒数
    :return:
    """
    while True:
        item = dict()
        # item["user_mail"] = fake.safe_email()
        item["user_mail"] = fake.random_choices(
            elements=('barry.xu@163.com', 'dandan@qq.com', 'pony@qq.com', 'focus@qq.com'), length=1)[0]
        status = fake.random_choices(
            elements=('unpaid', 'paid', 'cancel', 'shipping', 'finished'), length=1)[0]
        item["status"] = status
        item["good_count"] = fake.random_int(min=1, max=10)
        item["city"] = fake.city()
        item["amount"] = round(fake.random_int(min=1000, max=100000)*0.01, 2)

        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        if status == 'unpaid':
            item["create_time"] = now_str
            item["update_time"] = now_str
        else:
            m = fake.random_int(min=1, max=10)
            item["create_time"] = (
                now - timedelta(minutes=m)).strftime("%Y-%m-%d %H:%M:%S")
            item["update_time"] = now_str

        yield item
        interval = fake.random_int(min=interval_min, max=interval_max) * 0.001
        time.sleep(interval)
