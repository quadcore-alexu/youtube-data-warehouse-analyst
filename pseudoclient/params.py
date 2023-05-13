import schemas

number_of_client_threads = 5
kafka_listeners = 'kafka1:9092,kafka2:9092,kafka3:9092'

actions = [
    {
        'topic': 'views',
        'delay': 1,
        'schema': schemas.view_action
    },
    {
        'topic': 'first_view',
        'delay': 1,
        'schema': schemas.log_action.get('type').get('values')[0]
    },
    {
        'topic': 'subscribes',
        'delay': 1,
        'schema': schemas.log_action.get('type').get('values')[1]
    },
    {
        'topic': 'likes',
        'delay': 1,
        'schema': schemas.log_action.get('type').get('values')[2]
    }
]
