from datetime import datetime
from flask import Flask, jsonify, request
from cassandra.cluster import Cluster
import pandas as pd
import sys

app = Flask(__name__)
cluster = Cluster(['scylla'])
session = cluster.connect('scyllakeyspace')


@app.route('/scylla/show')
def show():
    # query = "SELECT * FROM first_views_videos"
    # rows = session.execute(query)
    # rows_df = pd.DataFrame(list(rows)).head(50)
    # result = [
    #     {
    #         'video_id': row.__getattribute__("video_id"),
    #     }
    #     for row in rows_df.itertuples()
    # ]
    # return jsonify(result)
    level = request.args.get("level")
    query = "SELECT video_id,channel_id, COUNT(*) AS views_count FROM first_views_video GROUP BY video_id ".format(
        get_time_window(level))
    rows = session.execute(query)
    rows_df = pd.DataFrame(list(rows))
    # haha = rows_df.groupby(by="channel_id").size()
    print(query, file=sys.stderr)
    # for row in haha:
    #     print(row, file=sys.stderr)

    # sorted_df = rows_df.sort_values(by='views_count', ascending=False).head(10)
    result = [
        {
            'video_id': row.__getattribute__("video_id"),
            'views_count': row.__getattribute__("views_count"),
            'channel_id': row.__getattribute__("channel_id"),
        }
        for row in rows_df.itertuples()
    ]
    return jsonify(result)


@app.route('/scylla/top_watched_videos')
def get_top_watched_videos():
    level = request.args.get("level")
    query = "SELECT video_id, COUNT(*) AS views_count FROM first_views_video WHERE timestamp >= {} GROUP BY video_id ALLOW FILTERING;".format(
        get_time_window(level))
    rows = session.execute(query)
    rows_df = pd.DataFrame(list(rows))
    sorted_df = rows_df.sort_values(by='views_count', ascending=False).head(10)
    result = [
        {
            'video_id': row.__getattribute__("video_id"),
            'views_count': row.__getattribute__("views_count")
        }
        for row in sorted_df.itertuples()
    ]
    return jsonify(result)


@app.route('/scylla/top_watched_channels')
def get_top_watched_channels():
    level = request.args.get("level")
    query = "SELECT channel_id, COUNT(*) AS views_count FROM first_views_age WHERE timestamp >= {} GROUP BY channel_id LIMIT 10 ALLOW FILTERING;".format(
        get_time_window(level))
    rows = session.execute(query)
    rows_df = pd.DataFrame(list(rows))
    sorted_df = rows_df.sort_values(by='views_count', ascending=False).head(10)
    result = [
        {
            'channel_id': row.__getattribute__("channel_id"),
            'views_count': row.__getattribute__("views_count")
        }
        for row in sorted_df.itertuples()
    ]
    return jsonify(result)


@app.route('/scylla/top_liked_videos')
def get_top_liked_videos():
    level = request.args.get("level")
    query = "SELECT video_id, COUNT(*) AS likes_count FROM likes_video WHERE timestamp >= {} \
            GROUP BY video_id LIMIT 10 ALLOW FILTERING;".format(get_time_window(level))
    rows = session.execute(query)
    rows_df = pd.DataFrame(list(rows))
    sorted_df = rows_df.sort_values(by='likes_count', ascending=False).head(10)
    result = [
        {
            'video_id': row.__getattribute__("video_id"),
            'likes_count': row.__getattribute__("likes_count")
        }
        for row in sorted_df.itertuples()
    ]
    return jsonify(result)


@app.route('/scylla/top_liked_channels')
def get_top_liked_channels():
    level = request.args.get("level")
    query = "SELECT channel_id, COUNT(*) AS likes_count FROM likes_age WHERE timestamp >= {} \
                GROUP BY channel_id LIMIT 10 ALLOW FILTERING;".format(get_time_window(level))
    rows = session.execute(query)
    rows_df = pd.DataFrame(list(rows))
    sorted_df = rows_df.sort_values(by='likes_count', ascending=False).head(10)
    result = [
        {
            'channel_id': row.__getattribute__("channel_id"),
            'likes_count': row.__getattribute__("likes_count")
        }
        for row in sorted_df.itertuples()
    ]
    return jsonify(result)


@app.route('/scylla/video_history')
def get_video_history():
    id = request.args.get("video_id")
    hour_views, day_views, week_views, month_views, all_views = get_history('views_video', 'video_id', id)
    hour_likes, day_likes, week_likes, month_likes, all_likes = get_history('likes_video', 'video_id', id)

    response = {"last_hour": {"views_count": hour_views[0].__getattribute__("views_count"),
                              "likes_count": hour_likes[0].__getattribute__("views_count")},
                "last_day": {"views_count": day_views[0].__getattribute__("views_count"),
                             "likes_count": day_likes[0].__getattribute__("views_count")},
                "last_week": {"views_count": week_views[0].__getattribute__("views_count"),
                              "likes_count": week_likes[0].__getattribute__("views_count")},
                "last_month": {"views_count": month_views[0].__getattribute__("views_count"),
                               "likes_count": month_likes[0].__getattribute__("views_count")},
                "all": {"views_count": all_views[0].__getattribute__("views_count"),
                        "likes_count": all_likes[0].__getattribute__("views_count")}}
    return jsonify(response)


@app.route('/scylla/channel_history')
def get_channel_history():
    id = request.args.get("channel_id")
    hour_views, day_views, week_views, month_views, all_views = get_history('views_age', 'channel_id', id)
    hour_likes, day_likes, week_likes, month_likes, all_likes = get_history('likes_age', 'channel_id', id)

    response = {"last_hour": {"views_count": hour_views[0].__getattribute__("views_count"),
                              "likes_count": hour_likes[0].__getattribute__("views_count")},
                "last_day": {"views_count": day_views[0].__getattribute__("views_count"),
                             "likes_count": day_likes[0].__getattribute__("views_count")},
                "last_week": {"views_count": week_views[0].__getattribute__("views_count"),
                              "likes_count": week_likes[0].__getattribute__("views_count")},
                "last_month": {"views_count": month_views[0].__getattribute__("views_count"),
                               "likes_count": month_likes[0].__getattribute__("views_count")},
                "all": {"views_count": all_views[0].__getattribute__("views_count"),
                        "likes_count": all_likes[0].__getattribute__("views_count")}}
    return jsonify(response)


def get_history(table_name, id_type, id):
    hour = session.execute(f"""
            SELECT {id_type}, COUNT(*) AS views_count
            FROM {table_name} 
            where {id_type} = {id} and timestamp >= {get_time_window('hour')}
            GROUP BY {id_type}
            ALLOW FILTERING;
        """)
    day = session.execute(f"""
            SELECT {id_type}, COUNT(*) AS views_count
            FROM {table_name} 
            where {id_type} = {id} and timestamp >= {get_time_window('day')}
            GROUP BY {id_type}
            ALLOW FILTERING;
        """)
    week = session.execute(f"""
                SELECT {id_type}, COUNT(*) AS views_count
                FROM {table_name} 
                where {id_type} = {id} and timestamp >= {get_time_window('week')}
                GROUP BY {id_type}
                ALLOW FILTERING;
            """)
    month = session.execute(f"""
                SELECT {id_type}, COUNT(*) AS views_count
                FROM {table_name} 
                where {id_type} = {id} and timestamp >= {get_time_window('month')}
                GROUP BY {id_type}
                ALLOW FILTERING;
            """)
    all = session.execute(f"""
                SELECT {id_type}, COUNT(*) AS views_count
                FROM {table_name} 
                where {id_type} = {id}
                GROUP BY {id_type}
                ALLOW FILTERING;
            """)
    return hour, day, week, month, all


@app.route('/scylla/interaction')
def get_interaction():
    channel_id = request.args.get("channel_id")

    query = "SELECT timestamp AS interaction_hour, user_country, COUNT(*) AS interaction_count FROM first_views_country WHERE channel_id = {} ALLOW FILTERING;".format(channel_id)

    rows = session.execute(query)

    rows = rows.groupby('user_country', sort=False)['interaction_count'].idxmax()
    result = [
        {
            "country": row.__getattribute__("user_country"),
            "peak_interaction_time": row.__getattribute__("interaction_hour")
        }
        for row in rows.itertuples()
    ]
    return jsonify(result)


@app.route('/scylla/countries')
def get_countries_dist():
    channel_id = request.args.get("channel_id")

    country_views = session.execute(
        "SELECT channel_id, user_country, COUNT(*) as views_count from first_views_country WHERE channel_id = {} GROUP BY channel_id, user_country".format(
            channel_id))

    country_likes = session.execute(
        "SELECT channel_id, user_country, COUNT(*) as likes_count from likes_country WHERE channel_id = {} GROUP BY channel_id, user_country".format(
            channel_id))

    country_mins = session.execute(
        "SELECT channel_id, user_country, COUNT(*) as mins_count from views_country WHERE channel_id = {} GROUP BY channel_id, user_country".format(
            channel_id))

    country_views_df = pd.DataFrame(country_views)
    country_likes_df = pd.DataFrame(country_likes)
    country_mins_df = pd.DataFrame(country_mins)

    merged_df = pd.merge(country_views_df, country_likes_df, on=['channel_id', 'user_country']).merge(country_mins_df,
                                                                                                      on=['channel_id',
                                                                                                          'user_country'])
    response = [
        {
            "country": row.__getattribute__("user_country"),
            "views_count": row.__getattribute__("views_count"),
            "likes_count": row.__getattribute__("likes_count"),
            "minutes_watched": row.__getattribute__("mins_count")
        }
        for row in merged_df.itertuples()
    ]

    return jsonify(response)


@app.route('/scylla/ages')
def get_ages_dist():
    channel_id = request.args.get("channel_id")

    ages_views = session.execute(
        "SELECT channel_id, user_age, COUNT(*) as views_count from first_views_age WHERE channel_id = {} GROUP BY channel_id, user_age".format(
            channel_id))

    ages_likes = session.execute(
        "SELECT channel_id, user_age, COUNT(*) as likes_count from likes_age WHERE channel_id = {} GROUP BY channel_id, user_age".format(
            channel_id))

    ages_mins = session.execute(
        "SELECT channel_id, user_age, COUNT(*) as mins_count from views_age WHERE channel_id = {} GROUP BY channel_id, user_age".format(
            channel_id))

    ages_views_df = pd.DataFrame(ages_views)
    ages_likes_df = pd.DataFrame(ages_likes)
    ages_mins_df = pd.DataFrame(ages_mins)

    merged_df = pd.merge(ages_views_df, ages_likes_df, on=['channel_id', 'user_age']).merge(ages_mins_df,
                                                                                            on=['channel_id',
                                                                                                'user_age'])
    response = [
        {
            "age": row.__getattribute__("user_age"),
            "views_count": row.__getattribute__("views_count"),
            "likes_count": row.__getattribute__("likes_count"),
            "minutes_watched": row.__getattribute__("mins_count")
        }
        for row in merged_df.itertuples()
    ]

    return jsonify(response)


@app.route('/scylla/comments')
def comments():
    positive = session.execute("SELECT video_id, COUNT(*) AS positive_count FROM comments WHERE comment_score = 1 \
            GROUP BY video_id LIMIT 10 ALLOW FILTERING;")
    count = session.execute("SELECT video_id, COUNT(*) AS negative_count FROM comments WHERE comment_score = -1 \
            GROUP BY video_id LIMIT 10 ALLOW FILTERING;")

    positive_df = pd.DataFrame(positive)
    count_df = pd.DataFrame(count)
    merged_df = pd.merge(positive_df, count_df, on=['video_id'])

    sorted_df = merged_df.sort_values(by='positive_count', ascending=False).head(10)
    result = [
        {
            'video_id': row.__getattribute__("video_id"),
            'positive_count': row.__getattribute__("positive_count"),
            'negative_count': row.__getattribute__("negative_count")
        }
        for row in sorted_df.itertuples()
    ]
    return jsonify(result)


@app.route('/scylla/histogram')
def get_video_histogram():
    video_id = request.args.get("video_id")
    views_query = "SELECT video_id, seconds_offset, COUNT(*) AS views_count FROM views_video \
    WHERE video_id = {} ALLOW FILTERING;".format(video_id)
    likes_query = "SELECT video_id, seconds_offset, COUNT(*) AS likes_count FROM likes_video \
    WHERE video_id = {} ALLOW FILTERING;".format(video_id)

    views_df = pd.DataFrame(session.execute(views_query))
    likes_df = pd.DataFrame(session.execute(likes_query))

    views_df = views_df.drop(['video_id'], axis=1)
    likes_df = likes_df.drop(['video_id'], axis=1)

    merged_df = pd.merge(views_df.groupby(['seconds_offset'], sort=False).sum(),
                         likes_df.groupby(['seconds_offset'], sort=False).sum(), on=['seconds_offset'], how="outer")
    sorted_df = merged_df.sort_values(by='seconds_offset')

    result = [
        {
            'views_count': row.__getattribute__("views_count"),
            'likes_count': row.__getattribute__("likes_count")
        }
        for row in sorted_df.itertuples()
    ]
    return jsonify(result)


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
    app.run(host="0.0.0.0", port=8000)
