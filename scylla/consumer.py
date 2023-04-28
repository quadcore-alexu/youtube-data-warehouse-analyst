# from cassandra.cluster import Cluster
# from kafka import KafkaConsumer

import params
from confluent_kafka import Consumer

conf = {'bootstrap.servers': params.kafka_listeners,
        'group.id': 'test-consumer',
        'auto.offset.reset': 'earliest'}
consumer = Consumer(conf)
consumer.subscribe(['deltaTopic'])

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print('Error while consuming message: {}'.format(msg.error()))
        else:
            print('Received message: {}'.format(msg.value().decode('utf-8')))
except KeyboardInterrupt:
    pass
finally:
    consumer.close()

# cluster = Cluster(['0.0.0.0'])
# session = cluster.connect()
#
# session.execute('USE Test')
# result = session.execute("SELECT * FROM User;").one()
# print(result.name, result.age)
#

# Configure Kafka consumer
# consumer = KafkaConsumer('scyllaTopic',
#                          auto_offset_reset='earliest',
#                          bootstrap_servers=['kafka3:9092']
#                          )
# Configure Cassandra connection
# cluster = Cluster(['localhost'])
# session = cluster.connect('Test')

# Consume messages and insert into ScyllaDB
# for message in consumer:
# Extract data from Kafka message
# key = message.key.decode('utf-8')
# value = message.value.decode('utf-8')
# print("HELLOOOOO")
# print(key)
# print(value)
# Insert data into ScyllaDB
# session.execute(
#     """
#     INSERT INTO test_table (key, value)
#     VALUES (%s, %s)
#     """,
#     (key, value)
# )
