from confluent_kafka import Producer
import json
import time
from faker import Faker

fake = Faker()
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'names_group',
    'auto.offset.reset': 'earliest',
    'max.partition.fetch.bytes': 10 * 1024 * 1024  # 10 MB
}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

while True:
    name = fake.name()
    message = {'name': name, 'timestamp': time.time()}
    print(f"Generated name: {name}")

    producer.produce('realtime_messaging', key=None, value=json.dumps(message), callback=delivery_report)
    producer.flush()
    time.sleep(1)