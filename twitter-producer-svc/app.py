
from kafka import KafkaProducer, KafkaConsumer
import json
import random
import time


KAFKA_SVC = 'kafka:9092'
TOPIC = 'home-sensehat-temperature'
producer = KafkaProducer(bootstrap_servers=[KAFKA_SVC], 
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'), 
                         api_version=(7,1,0))
def on_send_success(record_metadata):
    print("success--->",record_metadata.topic, record_metadata.partition,record_metadata.offset)

def on_send_error(excp):
    print('Error --->', exc_info=excp)


text = "a,b,c,d,e,f".split(",")
while True:

    producer.send(TOPIC,{random.choice(text): "2020-08-12 23:31:19.102347", "temperature":random.choice(text) }
                  ).add_callback(on_send_success).add_errback(on_send_error)
    time.sleep(2)


producer.flush()
