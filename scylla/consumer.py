from kafka import KafkaConsumer
from cassandra.cluster import Cluster

consumer = KafkaConsumer('deltaTopic',
                         bootstrap_servers=['localhost:9001'],
                         auto_offset_reset='earliest',
                         group_id='test-consumer',
                         value_deserializer=lambda x: x.decode('utf-8'))

# cluster = Cluster(['0.0.0.0'])
# session = cluster.connect()
# session.execute('USE Test')
#
# result = session.execute("SELECT * FROM User;").one()
# print(result.name, result.age)

for message in consumer:
    print(message.value)
