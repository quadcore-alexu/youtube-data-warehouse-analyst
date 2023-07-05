from threading import Thread
from confluent_kafka import Producer
import socket
import time
import params
import json
from random import normalvariate
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement


# scylla connection
cluster = Cluster(['scylla'])
session = cluster.connect()
session.execute("USE scyllakeyspace")
tables_suffix = ["video", "age", "country"]

def normal_int(low, high):
    mean = (high + low) / 2
    stddev = (high - low) / 6
    while True:
        num = int(normalvariate(mean, stddev))
        if low <= num < high:
            return num


def normal_float(low, high):
    mean = (high + low) / 2
    stddev = (high - low) / 6
    while True:
        num = normalvariate(mean, stddev)
        if low <= num < high:
            return num


def normal_choice(lst):
    mean = (len(lst) - 1) / 2
    stddev = len(lst) / 6
    while True:
        index = int(normalvariate(mean, stddev))
        if 0 <= index < len(lst):
            return lst[index]


def gen_message(schema):
    args = {}
    for k, v in schema.items():
        if v['type'] == 'int-range':
            args[k] = normal_int(v['low'], v['high'])
        elif v['type'] == 'float-range':
            args[k] = normal_float(v['low'], v['high'])
        elif v['type'] == 'cat':
            args[k] = normal_choice(v['values'])
        elif v['type'] == 'object':
            args[k] = gen_message(v['schema'])
        elif v['type'] == 'function':
            args[k] = v['function'](args)
        else:
            raise RuntimeError(f'{k} field type not specified or not supported')
    return args


def start_action(args):
    # Send data
    for i in range(1000):
        print("batch number ", str(i))
        insert_in_table(args['schema'], args['topic'])
        time.sleep(2)
    print("############################################################################")

def insert_in_table(schema, table_name):
    message = json.dumps(gen_message(schema))
    topic = table_name
    message_json = json.loads(message)
    # Extract the fields from the JSON message
    timestamp = message_json.get('timestamp')
    user_country = message_json.get('user_country')
    user_age = message_json.get('user_age')
    video_id = message_json.get('video_id')
    channel_id = message_json.get('channel_id')
    seconds_offset = message_json.get('seconds_offset')
    comment = message_json.get('comment')

    if table_name == 'views':
        batch = BatchStatement()
        for i in range(1000):
            message = json.dumps(gen_message(schema))
            message_json = json.loads(message)
            # Extract the fields from the JSON message
            timestamp = message_json.get('timestamp')
            user_country = message_json.get('user_country')
            user_age = message_json.get('user_age')
            video_id = message_json.get('video_id')
            channel_id = message_json.get('channel_id')
            seconds_offset = message_json.get('seconds_offset')
            comment = message_json.get('comment')
            for suffix in tables_suffix:
                insert_query = session.prepare(f"INSERT INTO {table_name}_{suffix} (timestamp, user_country, user_age, video_id, channel_id, seconds_offset) VALUES (?, ?, ?, ?, ?, ?)")
                batch.add(insert_query, (timestamp, user_country, user_age, video_id, channel_id, seconds_offset))
        session.execute(batch)
    elif table_name == 'first_views':
        batch = BatchStatement()
        for i in range(1000):
            message = json.dumps(gen_message(schema))
            message_json = json.loads(message)
            # Extract the fields from the JSON message
            timestamp = message_json.get('timestamp')
            user_country = message_json.get('user_country')
            user_age = message_json.get('user_age')
            video_id = message_json.get('video_id')
            channel_id = message_json.get('channel_id')
            seconds_offset = message_json.get('seconds_offset')
            comment = message_json.get('comment')
            for suffix in tables_suffix:
                insert_query = session.prepare(f"INSERT INTO {table_name}_{suffix} (timestamp, user_country, user_age, video_id, channel_id) VALUES (?, ?, ?, ?, ?)")
                batch.add(insert_query, (timestamp, user_country, user_age, video_id, channel_id))
        session.execute(batch)
    elif table_name == 'likes':
        batch = BatchStatement()
        for i in range(1000):
            message = json.dumps(gen_message(schema))
            message_json = json.loads(message)
            # Extract the fields from the JSON message
            timestamp = message_json.get('timestamp')
            user_country = message_json.get('user_country')
            user_age = message_json.get('user_age')
            video_id = message_json.get('video_id')
            channel_id = message_json.get('channel_id')
            seconds_offset = message_json.get('seconds_offset')
            comment = message_json.get('comment')
            for suffix in tables_suffix:
                insert_query = session.prepare(f"INSERT INTO {table_name}_{suffix} (timestamp, user_country, user_age, video_id, channel_id, seconds_offset) VALUES (?, ?, ?, ?, ?, ?)")
                batch.add(insert_query, (timestamp, user_country, user_age, video_id, channel_id, seconds_offset))
        session.execute(batch)
    elif table_name == 'subscribes':
        batch = BatchStatement()
        for i in range(1000):
            message = json.dumps(gen_message(schema))
            message_json = json.loads(message)
            # Extract the fields from the JSON message
            timestamp = message_json.get('timestamp')
            user_country = message_json.get('user_country')
            user_age = message_json.get('user_age')
            video_id = message_json.get('video_id')
            channel_id = message_json.get('channel_id')
            seconds_offset = message_json.get('seconds_offset')
            comment = message_json.get('comment')
            for suffix in tables_suffix:
                insert_query = session.prepare(f"INSERT INTO {table_name}_{suffix} (timestamp, user_country, user_age, video_id, channel_id) VALUES (?, ?, ?, ?, ?)")
                batch.add(insert_query, (timestamp, user_country, user_age, video_id, channel_id))
        session.execute(batch)
 
    print('Insert successful 3000 for topic {}'.format(topic))


def run_client():
    for action in params.actions:
        thread = Thread(target=start_action, args=(action,))
        thread.start()
