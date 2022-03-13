from kafka import KafkaConsumer
from setting import BootStrap_Servers
import time
import sys


def start_consumer(topic):
    consumer = KafkaConsumer(topic, bootstrap_servers=BootStrap_Servers)
    print("Begin...")
    for msg in consumer:
        print(msg)
        # print("topic = %s" % msg.topic)  # topic default is string
        # print("partition = %d" % msg.offset)
        # print("value = %s" % msg.value.decode())  # bytes to string
        # print("timestamp = %d" % msg.timestamp)
        # print("time = ", time.strftime("%Y-%m-%d %H:%M:%S",
        #       time.localtime(msg.timestamp/1000)))


if __name__ == '__main__':
    if len(sys.argv) > 1:
        topic_name = sys.argv[1]
    else:
        raise ValueError("need param for topic name")
    start_consumer(topic_name)
