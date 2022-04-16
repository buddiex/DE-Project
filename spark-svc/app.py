from pyspark.sql import SparkSession
from datetime import datetime, date
from pyspark.sql import Row
from kafka import KafkaConsumer


KAFKA_SVC = 'kafka:29092'

spark = SparkSession.builder.getOrCreate()


df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])

df.show()


print("consumming data")
# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('my-topic',
                         group_id='my-group',
                         bootstrap_servers=[KAFKA_SVC],
                         api_version=(7, 1, 0),
                         consumer_timeout_ms=1000)
for message in consumer:
    # print("here")
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                         message.offset, message.key,
                                         message.value))