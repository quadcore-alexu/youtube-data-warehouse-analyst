import schemas

number_of_client_threads = 5
kafka_listeners = 'kafka1:9092,kafka2:9092,kafka3:9092'

actions = [
    {
        'topic': 'views',
        'delay': 0.2,
        'schema': schemas.view_action
    },
    {
        'topic': 'first_view',
        'delay': 1,
        'schema': schemas.first_view
    },
    {
        'topic': 'subscribes',
        'delay': 2,
        'schema': schemas.subscribe
    },
    {
        'topic': 'likes',
        'delay': 2,
        'schema': schemas.like
    },

    {
        'topic': 'comments',
        'delay': 5,
        'schema': schemas.comment
    }
]
