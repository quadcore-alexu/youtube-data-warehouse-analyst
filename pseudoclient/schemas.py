import random
from datetime import datetime, timedelta

# fields definition


_timestamp = {
    'type': 'int-range',
    'low': int(datetime.timestamp(datetime.now() - timedelta(days=60))),
    'high': int(datetime.timestamp(datetime.now()))
}

_user_id = {
    'type': 'int-range',
    'low': 100_000,
    'high': 999_999
}

_country = {
    'type': 'cat',
    'values': ['Egypt', 'KSA', 'USA', 'Germany']
}

_age = {
    'type': 'int-range',
    'low': 10,
    'high': 80
}

_categories = {
    'type': 'cat',
    'values': ['Comedy', 'Action', 'Horror', 'Educational', 'Music']
}

_creation_date = {
    'type': 'int-range',
    'low': int(datetime.timestamp(datetime.now() - timedelta(days=10 * 365))),
    'high': int(datetime.timestamp(datetime.now()))
}

_duration = {
    'type': 'int-range',
    'low': int(timedelta(seconds=1).total_seconds()),
    'high': int(timedelta(hours=10).total_seconds())
}

_video_id = {
    'type': 'int-range',
    'low': 1_000_000,
    'high': 1_000_005
}

_channel_id = {
    'type': 'int-range',
    'low': 10_000,
    'high': 99_999
}

_video = {
    'type': 'object',
    'schema': {
        'channel_id': _channel_id,
        'creation_date': _creation_date,
        'category': _categories,
        'duration': _duration
    }
}

_log_type = {
    'type': 'cat',
    'values': ['first_view', 'subscribe', 'like']
}

_seconds_offset = {
    'type': 'function',
    'function': lambda obj: random.randint(0, obj['video_object']['duration'])
}

# schemas definition

# view action
view_action = {
    'timestamp': _timestamp,
    'user_id': _user_id,
    'user_country': _country,
    'user_age': _age,
    'video_id': _video_id,
    'channel_id': _channel_id,
    'video_object': _video,
    'seconds_offset': _seconds_offset
}

# first vew action
first_view = {
    'timestamp': _timestamp,
    'user_id': _user_id,
    'user_country': _country,
    'user_age': _age,
    'video_id': _video_id,
    'channel_id': _channel_id,
    'video_object': _video,
}

# subscribe action
subscribe = {
    'timestamp': _timestamp,
    'user_id': _user_id,
    'user_country': _country,
    'user_age': _age,
    'video_id': _video_id,
    'channel_id': _channel_id,
    'video_object': _video,
}

# like action
like = {
    'timestamp': _timestamp,
    'user_id': _user_id,
    'user_country': _country,
    'user_age': _age,
    'video_id': _video_id,
    'channel_id': _channel_id,
    'video_object': _video,
}
