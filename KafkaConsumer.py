from confluent_kafka import Consumer, KafkaError
import json

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'names_group',
    'auto.offset.reset': 'earliest',
    'max.partition.fetch.bytes': 10 * 1024 * 1024,
    'fetch.message.max.bytes': 10 * 1024 * 1024,  # 10 MB
 

}

consumer = Consumer(conf)
consumer.subscribe(['realtime_messaging'])

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
    else:
        message_value = json.loads(msg.value().decode('utf-8'))
        name = message_value['name']
        print(f"Received name: {name}")