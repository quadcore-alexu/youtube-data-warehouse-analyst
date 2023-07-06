import schemas

number_of_client_threads = 5
kafka_listeners = 'kafka1:9092,kafka2:9092,kafka3:9092'

actions = [
    {
        'topic': 'views',
        'delay': 0.2,
        'schema': schemas.view_action,
        'number_of_batches': 700
    },
    {
        'topic': 'first_views',
        'delay': 1,
        'schema': schemas.first_views,
        'number_of_batches': 140
    },
    {
        'topic': 'subscribes',
        'delay': 2,
        'schema': schemas.subscribe,
        'number_of_batches': 70
    },
    {
        'topic': 'likes',
        'delay': 2,
        'schema': schemas.like,
        'number_of_batches': 70
    },

    # {
    #     'topic': 'comments',
    #     'delay': 5,
    #     'schema': schemas.comment,
    #     'number_of_batches': 35
    # }
]
