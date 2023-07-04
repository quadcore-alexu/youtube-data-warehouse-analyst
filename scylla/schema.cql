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

CREATE TABLE IF NOT EXISTS views (
    timestamp TIMESTAMP,
    user_id INT,
    user_country TEXT,
    user_age INT,
    video_id INT,
    channel_id INT,
    seconds_offset INT,
    PRIMARY KEY (timestamp, user_id)
);
CREATE TABLE IF NOT EXISTS first_views (
    timestamp TIMESTAMP,
    user_id INT,
    user_country TEXT,
    user_age INT,
    video_id INT,
    channel_id INT,
    PRIMARY KEY (timestamp, user_id)
);
CREATE TABLE IF NOT EXISTS subscribes (
    timestamp TIMESTAMP,
    user_id INT,
    user_country TEXT,
    user_age INT,
    video_id INT,
    channel_id INT,
    PRIMARY KEY (timestamp, user_id)
);
CREATE TABLE IF NOT EXISTS likes (
    timestamp TIMESTAMP,
    user_id INT,
    user_country TEXT,
    user_age INT,
    video_id INT,
    channel_id INT,
    seconds_offset INT,
    PRIMARY KEY (timestamp, user_id)
);
CREATE TABLE IF NOT EXISTS comments (
    timestamp TIMESTAMP,
    user_id INT,
    user_country TEXT,
    user_age INT,
    video_id INT,
    channel_id INT,
    comment VARCHAR,
    PRIMARY KEY (timestamp, user_id)
);