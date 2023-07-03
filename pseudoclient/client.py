from threading import Thread
from confluent_kafka import Producer
import socket
import time
import params
import json
from random import normalvariate


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
    conf = {'bootstrap.servers': params.kafka_listeners,
            'client.id': socket.gethostname()}
    producer = Producer(conf)
    # Send data
    while True:
        message = json.dumps(gen_message(args['schema']))
        producer.produce(args['topic'], value=message.encode("utf-8"))
        time.sleep(args['delay'])


def run_client():
    for action in params.actions:
        thread = Thread(target=start_action, args=(action,))
        thread.start()
