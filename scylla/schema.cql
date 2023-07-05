CREATE TABLE IF NOT EXISTS channels (
  id INT PRIMARY KEY,
  owner_id INT,
  creation_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS videos (
  id INT PRIMARY KEY,
  channel_id INT,
  creation_date TIMESTAMP,
  category TEXT,
  duration_minutes INT
);



CREATE TABLE IF NOT EXISTS views_video (
    timestamp TIMESTAMP,
    user_country TEXT,
    user_age INT,
    video_id INT,
    channel_id INT,
    seconds_offset INT,
    PRIMARY KEY (video_id, channel_id, timestamp)
);
CREATE TABLE IF NOT EXISTS views_age (
    timestamp TIMESTAMP,
    user_country TEXT,
    user_age INT,
    video_id INT,
    channel_id INT,
    seconds_offset INT,
    PRIMARY KEY (channel_id,user_age, video_id, timestamp)
);
CREATE TABLE IF NOT EXISTS views_country (
    timestamp TIMESTAMP,
    user_country TEXT,
    user_age INT,
    video_id INT,
    channel_id INT,
    seconds_offset INT,
    PRIMARY KEY (channel_id,user_country, video_id, timestamp)
);


CREATE TABLE IF NOT EXISTS first_views_video (
    timestamp TIMESTAMP,
    user_country TEXT,
    user_age INT,
    video_id INT,
    channel_id INT,
    PRIMARY KEY (video_id, channel_id, timestamp)
);

CREATE TABLE IF NOT EXISTS first_views_age (
    timestamp TIMESTAMP,
    user_country TEXT,
    user_age INT,
    video_id INT,
    channel_id INT,
    PRIMARY KEY (channel_id, user_age,video_id, timestamp)
);

CREATE TABLE IF NOT EXISTS first_views_country (
    timestamp TIMESTAMP,
    user_country TEXT,
    user_age INT,
    video_id INT,
    channel_id INT,
    PRIMARY KEY (channel_id, user_country,video_id, timestamp)
);

CREATE TABLE IF NOT EXISTS subscribes (
    timestamp TIMESTAMP,
    user_country TEXT,
    user_age INT,
    video_id INT,
    channel_id INT,
    PRIMARY KEY (channel_id, video_id, timestamp)
);

CREATE TABLE IF NOT EXISTS likes_video (
    timestamp TIMESTAMP,
    user_country TEXT,
    user_age INT,
    video_id INT,
    channel_id INT,
    seconds_offset INT,
    PRIMARY KEY (video_id, channel_id, timestamp)
);

CREATE TABLE IF NOT EXISTS likes_age (
    timestamp TIMESTAMP,
    user_country TEXT,
    user_age INT,
    video_id INT,
    channel_id INT,
    seconds_offset INT,
    PRIMARY KEY (channel_id, user_age, video_id, timestamp)
);

CREATE TABLE IF NOT EXISTS likes_country (
    timestamp TIMESTAMP,
    user_country TEXT,
    user_age INT,
    video_id INT,
    channel_id INT,
    seconds_offset INT,
    PRIMARY KEY (channel_id, user_country, video_id, timestamp)
);

CREATE TABLE IF NOT EXISTS comments (
    timestamp TIMESTAMP,
    user_country TEXT,
    user_age INT,
    video_id INT,
    channel_id INT,
    comment_score INT,
    PRIMARY KEY (video_id, channel_id, timestamp)
);