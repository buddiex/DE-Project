#from pyspark.sql import SparkSession
from datetime import datetime, date
#from pyspark.sql import Row
from kafka import KafkaConsumer
import json
import time


KAFKA_SVC = 'kafka:9092'
TOPIC = 'home-sensehat-temperature'

# spark = SparkSession.builder.getOrCreate()

# df = spark.createDataFrame([
#     Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
#     Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
#     Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
# ])

# df.show()


# print("consumming data")
#consumer.subscribe(TOPIC)
consumer = KafkaConsumer(TOPIC,
                         group_id='my-group2',
                         bootstrap_servers=KAFKA_SVC,
                         api_version=(7, 1, 0),
                         consumer_timeout_ms=1000,
                         #  enable_auto_commit=False,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('ascii')))
consumer.subscribe(TOPIC)
#print(consumer.metrics())

print("consumming data")
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    time.sleep(2)
    print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                         message.offset, message.key,
                                         message.value))




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