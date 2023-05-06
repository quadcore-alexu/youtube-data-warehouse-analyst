# categorical fields
countries = ['Egypt', 'KSA', 'USA', 'Germany']
days = ['Sat', 'Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri']

# schema definition
schema = {
    'video_id': {
        'type': 'int-range',
        'low': 10_000,
        'high': 99_999
    },
    'day': {
        'type': 'cat',
        'values': days
    },
    'timestamp': {
        'type': 'float-range',
        'low': 0,
        'high': 24*60
    },
    'country': {
        'type': 'cat',
        'values': countries
    }
}
