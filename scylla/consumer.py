from cassandra.cluster import Cluster
from confluent_kafka import Consumer
import params

conf = {'bootstrap.servers': params.kafka_listeners,
        'group.id': 'test-consumer',
        'auto.offset.reset': 'earliest'}
consumer = Consumer(conf)
consumer.subscribe(['deltaTopic'])

cluster = Cluster(['scylla'])

session = cluster.connect()

session.execute("CREATE KEYSPACE IF NOT EXISTS scyllakeyspace WITH replication = {'class': 'SimpleStrategy', "
                "'replication_factor': 1}")
session.execute("USE scyllakeyspace")
session.execute("CREATE TABLE IF NOT EXISTS mytable (id INT PRIMARY KEY, message TEXT)")
counter = 0
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print('Error while consuming message: {}'.format(msg.error()))
        else:
            try:
                # print('Received message: {}'.format(msg.value().decode('utf-8')))
                # Insert consumed message into ScyllaDB
                query = "INSERT INTO mytable (id, message) VALUES ({}, '{}')".format(counter, msg.value().decode('utf-8'))
                print(query)
                session.execute(query)
                print('Insert successful')
                counter += 1
            except Exception as e:
                print('Error while inserting into ScyllaDB: {}'.format(str(e)))

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
