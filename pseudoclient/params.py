import schemas

number_of_client_threads = 5
kafka_listeners = 'kafka1:9092,kafka2:9092,kafka3:9092'

actions = [
    {
        'topic': 'deltaTopic',
        'delay': 1,
        'schema': schemas.view_action
    },
    {
        'topic': 'deltaTopic',
        'delay': 1,
        'schema': schemas.log_action
    }
]
