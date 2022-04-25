
from kafka import KafkaProducer, KafkaConsumer
import json

KAFKA_SVC = 'kafka:9092'
TOPIC = 'home-sensehat-temperature'
producer = KafkaProducer(bootstrap_servers=[KAFKA_SVC], 
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'), 
                         api_version=(7,1,0))
def on_send_success(record_metadata):
    print("success--->",record_metadata.topic, record_metadata.partition,record_metadata.offset)

def on_send_error(excp):
    print('Error --->', exc_info=excp)


for i in range(10):
    producer.send(TOPIC,{"timestamp": "2020-08-12 23:31:19.102347", "temperature": i}
                  ).add_callback(on_send_success).add_errback(on_send_error)

# # To consume latest messages and auto-commit offsets
consumer = KafkaConsumer(TOPIC,
                         group_id='my-group',
                         bootstrap_servers=KAFKA_SVC,
                         api_version=(7, 1, 0),
                         consumer_timeout_ms=1000,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('ascii')))

for message in consumer:
    print("here")
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                         message.offset, message.key,
                                         message.value))

# # consume earliest available messages, don't commit offsets
# KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

# # consume json messages
# KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))

# # consume msgpack
# KafkaConsumer(value_deserializer=msgpack.unpackb)

# # StopIteration if no message after 1sec
# KafkaConsumer(consumer_timeout_ms=1000)

# # Subscribe to a regex topic pattern
# consumer = KafkaConsumer()
# consumer.subscribe(pattern='^awesome.*')

# # Use multiple consumers in parallel w/ 0.9 kafka brokers
# # typically you would run each on a different server / process / CPU
# consumer1 = KafkaConsumer('my-topic',
#                           group_id='my-group',
#                           bootstrap_servers='my.server.com')
# consumer2 = KafkaConsumer('my-topic',
#                           group_id='my-group',
#                           bootstrap_servers='my.server.com')
