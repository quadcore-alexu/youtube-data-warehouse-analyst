from confluent_kafka import Producer
import socket
import sys
import time
import params

def run_client():
    server_address = params.server_address
    msg_size = params.msg_size
    delay = params.client_delay_time
    schema = params.schema
    conf = {'bootstrap.servers': params.kafka_listeners,
            'client.id': socket.gethostname()}
    producer = Producer(conf)
    topic = params.topic_name
    try:
        # Send data
        while True:
            message = schema.format(video_id=1050, country='Egypt')
            producer.produce(topic, value=message.encode("utf-8"))
            time.sleep(delay)

    finally:
        print(sys.stderr, 'closing socket')
        sock.close()