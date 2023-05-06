from confluent_kafka import Producer
import socket
import time
import schema
import params
import random


def gen_message():
    fields = schema.schema
    args = {}
    for k, v in fields.items():
        if v['type'] == 'int-range':
            args[k] = random.randint(v['low'], v['high'])
        elif v['type'] == 'float-range':
            args[k] = random.uniform(v['low'], v['high'])
        elif v['type'] == 'cat':
            args[k] = random.choice(v['values'])
        else:
            raise f'{k} field type not specified or not supported'
    return str(args)


def run_client():
    delay = params.client_delay_time
    conf = {'bootstrap.servers': params.kafka_listeners,
            'client.id': socket.gethostname()}
    producer = Producer(conf)
    topic = params.topic_name
    # Send data
    while True:
        message = gen_message()
        producer.produce(topic, value=message.encode("utf-8"))
        time.sleep(delay)
