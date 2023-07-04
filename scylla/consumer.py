from cassandra.cluster import Cluster
from confluent_kafka import Consumer
import threading
import params
import json


def consume_topic(topic):
    # kafka configuration
    conf = {'bootstrap.servers': params.kafka_listeners,
            'group.id': 'test-consumer',
            'auto.offset.reset': 'earliest'}
    consumer = Consumer(conf)
    consumer.subscribe([topic])

    # scylla connection
    cluster = Cluster(['scylla'])
    session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS scyllakeyspace WITH replication = {'class': 'SimpleStrategy', "
                    "'replication_factor': 1}")
    session.execute("USE scyllakeyspace")

    # tables creation
    ddl_file = "schema.cql"
    with open(ddl_file, "r") as file:
        ddl_queries = file.read().split(";")

    # Execute the queries
    for query in ddl_queries:
        if query.strip():
            session.execute(query)

    table_name = topic
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print('Error while consuming message from topic {}: {}'.format(topic, msg.error()))
            else:
                try:
                    message_json = json.loads(msg.value().decode('utf-8'))

                    # Extract the fields from the JSON message
                    timestamp = message_json.get('timestamp')
                    user_id = message_json.get('user_id')
                    user_country = message_json.get('user_country')
                    user_age = message_json.get('user_age')
                    video_id = message_json.get('video_id')
                    channel_id = message_json.get('channel_id')
                    seconds_offset = message_json.get('seconds_offset')
                    comment = message_json.get('comment')

                    if seconds_offset is None or comment is None:
                        query = "INSERT INTO {} (timestamp, user_id, user_country, user_age, video_id, channel_id) VALUES (%s, %s, %s, %s, %s, %s)".format(
                            table_name)
                        session.execute(query, (
                            timestamp, user_id, user_country, user_age, video_id, channel_id))

                    elif seconds_offset is not None:
                        query = "INSERT INTO {} (timestamp, user_id, user_country, user_age, video_id, channel_id, seconds_offset) VALUES (%s, %s, %s, %s, %s, %s, %s)".format(
                            table_name)
                        session.execute(query,
                                        (timestamp, user_id, user_country, user_age, video_id, channel_id,
                                         seconds_offset))

                    elif comment is not None:
                        query = "INSERT INTO {} (timestamp, user_id, user_country, user_age, video_id, channel_id, comment) VALUES (%s, %s, %s, %s, %s, %s, %s)".format(
                            table_name)
                        session.execute(query,
                                        (timestamp, user_id, user_country, user_age, video_id, channel_id,
                                         comment))
                    print(query)
                    print('Insert successful for topic {}'.format(topic))

                except Exception as e:
                    print('Error while inserting into ScyllaDB for topic {}: {}'.format(topic, str(e)))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


# Define the topics to consume from
topics = params.topics

# Create a separate thread for each topic
threads = []
for topic in topics:
    thread = threading.Thread(target=consume_topic, args=(topic,))
    thread.start()
    threads.append(thread)

# Wait for all threads to finish
for thread in threads:
    thread.join()
