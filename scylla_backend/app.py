from datetime import datetime
from flask import Flask, jsonify, request
from cassandra.cluster import Cluster

app = Flask(__name__)
cluster = Cluster(['scylla'])
session = cluster.connect('scyllakeyspace')


@app.route('/scylla/top_watched_videos')
def get_top_watched_videos():
    level = request.args.get("level")
    query = "SELECT video_id, COUNT(*) AS views_count FROM first_views WHERE timestamp >= {} GROUP BY video_id ORDER BY views_count DESC LIMIT 10;".format(get_time_window(level))

    rows = session.execute(query)
    result = [
        {
            'video_id': row.video_id,
            'views_count': row.views_count
        }
        for row in rows
    ]
    return jsonify(result)


@app.route('/delta/top_watched_channels')
def get_top_watched_channels():
    level = request.args.get("level")
    query = "SELECT channel_id, COUNT(*) AS views_count FROM first_views WHERE timestamp >= {} GROUP BY channel_id ORDER BY views_count DESC LIMIT 10;".format(get_time_window(level))
    rows = session.execute(query)
    result = [
        {
            'channel_id': row.channel_id,
            'views_count': row.views_count
        }
        for row in rows
    ]
    return jsonify(result)


@app.route('/delta/top_liked_videos')
def get_top_liked_videos():
    level = request.args.get("level")
    query = "SELECT video_id, COUNT(*) AS likes_count FROM likes WHERE timestamp >= {} \
            GROUP BY video_id ORDER BY likes_count DESC LIMIT 10;".format(get_time_window(level))
    rows = session.execute(query)
    result = [
        {
            'video_id': row.video_id,
            'likes_count': row.likes_count
        }
        for row in rows
    ]
    return jsonify(result)


@app.route('/delta/top_liked_channels')
def get_top_liked_channels():
    level = request.args.get("level")
    query = "SELECT channel_id, COUNT(*) AS likes_count FROM likes WHERE timestamp >= {} \
                GROUP BY channel_id ORDER BY likes_count DESC LIMIT 10;".format(get_time_window(level))
    rows = session.execute(query)
    result = [
        {
            'channel_id': row.channel_id,
            'likes_count': row.likes_count
        }
        for row in rows
    ]
    return jsonify(result)



@app.route('/scylla/video_history')
def get_video_history():
    id = request.args.get("video_id")
    hour_views, day_views, week_views, month_views, all_views = get_history('views', 'video_id', id)
    hour_likes, day_likes, week_likes, month_likes, all_likes = get_history('likes', 'video_id', id)

    response = {"last_hour": {"views_count": hour_views[0].views_count, "likes_count": hour_likes[0].views_count},
                "last_day": {"views_count": day_views[0].views_count, "likes_count": day_likes[0].views_count},
                "last_week": {"views_count": week_views[0].views_count, "likes_count": week_likes[0].views_count},
                "last_month": {"views_count": month_views[0].views_count, "likes_count": month_likes[0].views_count},
                "all": {"views_count": all_views[0].views_count, "likes_count": all_likes[0].views_count}}
    return jsonify(response)


@app.route('/scylla/channel_history')
def get_channel_history():
    id = request.args.get("channel_id")
    hour_views, day_views, week_views, month_views, all_views = get_history('views', 'channel_id', id)
    hour_likes, day_likes, week_likes, month_likes, all_likes = get_history('likes', 'channel_id', id)

    response = {"last_hour": {"views_count": hour_views[0].views_count, "likes_count": hour_likes[0].views_count},
                "last_day": {"views_count": day_views[0].views_count, "likes_count": day_likes[0].views_count},
                "last_week": {"views_count": week_views[0].views_count, "likes_count": week_likes[0].views_count},
                "last_month": {"views_count": month_views[0].views_count, "likes_count": month_likes[0].views_count},
                "all": {"views_count": all_views[0].views_count, "likes_count": all_likes[0].views_count}}
    return jsonify(response)


def get_history(table_name, id_type, id):
    hour = session.execute(f"""
            SELECT {id_type}, COUNT(*) AS views_count
            FROM {table_name} 
            where {id_type} = {id} and timestamp >= {get_time_window('hour')}
            GROUP BY {id_type};
        """)
    day = session.execute(f"""
            SELECT {id_type}, COUNT(*) AS views_count
            FROM {table_name} 
            where {id_type} = {id} and timestamp >= {get_time_window('day')}
            GROUP BY {id_type};
        """)
    week = session.execute(f"""
                SELECT {id_type}, COUNT(*) AS views_count
                FROM {table_name} 
                where {id_type} = {id} and timestamp >= {get_time_window('week')}
                GROUP BY {id_type};
            """)
    month = session.execute(f"""
                SELECT {id_type}, COUNT(*) AS views_count
                FROM {table_name} 
                where {id_type} = {id} and timestamp >= {get_time_window('month')}
                GROUP BY {id_type};
            """)
    all = session.execute(f"""
                SELECT {id_type}, COUNT(*) AS views_count
                FROM {table_name} 
                where {id_type} = {id}
                GROUP BY {id_type};
            """)
    return hour, day, week, month, all


@app.route('/scylla/interaction')
def get_interaction():
    channel_id = request.args.get("channel_id")
    query = session.prepare("SELECT MOD(timestamp, 86400) / 3600 AS interaction_hour, user_country, COUNT(*) \
    AS interaction_count FROM first_views where channel_id = ? GROUP BY interaction_hour, user_country \
    HAVING MAX(interaction_count)")

    # query = f"""
    # SELECT MOD(timestamp, 86400) / 3600 AS interaction_hour, user_country, COUNT(*) AS interaction_count
    # FROM first_views
    # where channel_id = {channel_id}
    # GROUP BY interaction_hour, user_country
    # HAVING MAX(interaction_count)
    # """

    rows = session.execute(query, [channel_id])
    result = [
        {
            "country": row.user_country,
            "peak_interaction_time": row.interaction_hour
        }
        for row in rows
    ]
    return jsonify(result)


@app.route('/scylla/countries')
def get_countries_dist():
    channel_id = request.args.get("channel_id")

    country_views = session.execute("SELECT channel_id, user_country, COUNT(*) as views_count from first_views WHERE channel_id = {} GROUP BY channel_id, user_country".format(channel_id))
    total_channel_views = session.execute("SELECT channel_id, COUNT(*) as total_views from first_views WHERE channel_id = {} GROUP BY channel_id".format(channel_id))

    country_likes = session.execute(session.execute("SELECT channel_id, user_country, COUNT(*) as likes_count from likes WHERE channel_id = {} GROUP BY channel_id, user_country".format(channel_id)))
    total_channel_likes = session.execute("SELECT channel_id, COUNT(*) as total_likes from likes WHERE channel_id = {} GROUP BY channel_id".format(channel_id))

    country_mins = session.execute(session.execute("SELECT channel_id, user_country, COUNT(*) as mins_count from views WHERE channel_id = {} GROUP BY channel_id, user_country".format(channel_id)))
    total_channel_mins = session.execute("SELECT channel_id, COUNT(*) as total_mins_views from views WHERE channel_id = {} GROUP BY channel_id".format(channel_id))


    response = [
        {
            "country": country_views.user_country,
            "views_count": country_views.views_count / total_channel_views.total_views,
            "likes_count": country_likes.likes_count / total_channel_likes.total_likes,
            "minutes_watched": country_mins.mins_count / total_channel_mins.total_mins_views
        }
    ]

    return jsonify(response)


@app.route('/scylla/ages')
def get_ages_dist():
    channel_id = request.args.get("channel_id")
    country_views = session.execute(
        "SELECT channel_id, user_country, COUNT(*) as views_count from first_views WHERE channel_id = {} GROUP BY channel_id, user_country".format(
            channel_id))
    total_channel_views = session.execute(
        "SELECT channel_id, COUNT(*) as total_views from first_views WHERE channel_id = {} GROUP BY channel_id".format(
            channel_id))

    country_likes = session.execute(session.execute(
        "SELECT channel_id, user_country, COUNT(*) as likes_count from likes WHERE channel_id = {} GROUP BY channel_id, user_country".format(
            channel_id)))
    total_channel_likes = session.execute(
        "SELECT channel_id, COUNT(*) as total_likes from likes WHERE channel_id = {} GROUP BY channel_id".format(
            channel_id))

    country_mins = session.execute(session.execute(
        "SELECT channel_id, user_country, COUNT(*) as mins_count from views WHERE channel_id = {} GROUP BY channel_id, user_country".format(
            channel_id)))
    total_channel_mins = session.execute(
        "SELECT channel_id, COUNT(*) as total_mins_views from views WHERE channel_id = {} GROUP BY channel_id".format(
            channel_id))

    response = [
        {
            "age": country_views.user_country,
            "views_count": country_views.views_count / total_channel_views.total_views,
            "likes_count": country_likes.likes_count / total_channel_likes.total_likes,
            "minutes_watched": country_mins.mins_count / total_channel_mins.total_mins_views
        }
    ]

    return jsonify(response)


def get_time_window(filter_level):
    current_time = int(datetime.timestamp(datetime.now()))
    current_hour = current_time - (current_time % 3600)
    current_day = current_time - (current_time % (3600 * 24))
    current_week = current_time - (current_time % (3600 * 24 * 7))
    current_month = current_time - (current_time % (3600 * 24 * 30))

    if filter_level == 'hour':
        return current_hour
    elif filter_level == 'day':
        return current_day
    elif filter_level == 'week':
        return current_week
    elif filter_level == 'month':
        return current_month
    else:
        return 0


if __name__ == '__main__':
    app.run()
