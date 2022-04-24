
from kafka import KafkaProducer, KafkaConsumer
from json import dumps

KAFKA_SVC = 'kafka:29092'
producer = KafkaProducer(bootstrap_servers=[KAFKA_SVC], 
                         value_serializer=lambda x: dumps(x).encode('utf-8'), 
                         api_version=(7,1,0))
producer.send('home-sensehat-temperature',{"timestamp": "2020-08-12 23:31:19.102347", "temperature": 127.6969})
print("here")
producer.flush()
consumer=KafkaConsumer(bootstrap_servers=[KAFKA_SVC], api_version=(7,1,0))
print(consumer.topics())



# producer = KafkaProducer(bootstrap_servers=[KAFKA_SVC], 
#                          value_serializer=lambda x: dumps(x).encode('utf-8'), 
#                          api_version=(7,1,0))
# for i in range(10):
#     producer.send('home-sensehat-temperature',{"timestamp": "2020-08-12 23:31:19.102347", "temperature": i})

# consumer=KafkaConsumer(bootstrap_servers=[KAFKA_SVC], api_version=(7,1,0))
# for msg in consumer:
#     print(msg)
# print("consumener done")
# print(consumer.topics())



#
#  from kafka import KafkaConsumer
# from kafka import KafkaProducer
# from kafka.errors import KafkaError
# from pprint import pprint



# producer = KafkaProducer(bootstrap_servers=[KAFKA_SVC],
#               api_version=(7,1,0),)




# # producer.send('quickstart-events', b'mmmmmm')

# future = producer.send('quickstart-events', b'kkkkkkkkkk')

# # Block for 'synchronous' sends
# try:
#     record_metadata = future.get(timeout=10)
# except KafkaError:
#     # Decide what to do if produce request failed...
#     # log.exception()
#     raise

# # Successful result returns assigned partition and offset
# print (record_metadata.topic)
# print (record_metadata.partition)
# print (record_metadata.offset)

# # produce keyed messages to enable hashed partitioning
# producer.send('my-topic', key=b'foo', value=b'bar')

# # encode objects via msgpack
# producer = KafkaProducer(value_serializer=msgpack.dumps)
# producer.send('msgpack-topic', {'key': 'value'})

# # produce json messages
# producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
# producer.send('json-topic', {'key': 'value'})


# def on_send_success(record_metadata):
#     print(record_metadata.topic)
#     print(record_metadata.partition)
#     print(record_metadata.offset)

# def on_send_error(excp):
#     print('I am an errback', exc_info=excp)
#     # handle exception


# # produce asynchronously with callbacks

# producer.send('quickstart-events', b'mmmmmm')#.add_callback(on_send_success).add_errback(on_send_error)
# producer.send('my-topic', b'raw_bytes').add_callback(on_send_success).add_errback(on_send_error)
# # produce asynchronously
# for i in range(10):
#     msg = f"message:{i}"
#     print(f"sending {msg}")
#     producer.send('my-topic', str.encode(msg)).add_callback(on_send_success).add_errback(on_send_error)

# # # block until all async messages are sent
# # producer.flush()
# print("done...")
# # configure multiple retries
# # producer = KafkaProducer(retries=5)


# # To consume latest messages and auto-commit offsets
# consumer = KafkaConsumer('my-topic',
#                          group_id='my-group',
#                          bootstrap_servers=[KAFKA_SVC],
#                          api_version=(7, 1, 0),
#                          consumer_timeout_ms=1000)
# print(consumer.topics())

# print(producer.metrics())
# print(consumer.metrics())
# for message in consumer:
#     print("here")
#     # message value and key are raw bytes -- decode if necessary!
#     # e.g., for unicode: `message.value.decode('utf-8')`
#     print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
#                                          message.offset, message.key,
#                                          message.value))

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
