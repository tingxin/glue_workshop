from itsdangerous import json
from kafka import KafkaProducer
from time import sleep
from mock import gen
import json
import sys
from setting import BootStrap_Servers


creator = gen(increment_id="creator_id")


def send_success(self, *args, **kwargs):
    """异步发送成功回调函数"""
    print('save success')
    return


def send_error(self, *args, **kwargs):
    """异步发送错误回调函数"""
    print('error => {0}'.format(*args))
    return


def start_producer(topic):
    producer = KafkaProducer(bootstrap_servers=BootStrap_Servers)
    for item in creator:
        doc = json.dumps(item).encode('utf-8')
        producer.send(topic, doc).add_callback(
            send_success).add_errback(send_error)
        producer.flush()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        topic_name = sys.argv[1]
    else:
        raise ValueError("need param for topic name")
    # read your csv
    start_producer(topic_name)
