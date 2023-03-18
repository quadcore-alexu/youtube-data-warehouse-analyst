server_address = ('localhost', 10000)
msg_size = 4096
client_delay_time = 1
fields = ['video_id', 'country']

# create schema string
schema = '{{'
for f in fields:
    schema += f + ':{' + f + '},'
schema = schema[:-1]
schema += '}}'
