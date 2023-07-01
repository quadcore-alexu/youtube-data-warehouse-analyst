SELECT video_id, seconds_offset, COUNT(*) AS views_count
FROM views
GROUP BY video_id, seconds_offset;

SELECT video_id, seconds_offset, COUNT(*) AS likes_count
FROM likes
GROUP BY video_id, seconds_offset;

SELECT channel_id, user_country, COUNT(*) AS channel_views_count_country
FROM first_views
GROUP BY channel_id, user_country;

SELECT channel_id, user_age, COUNT(*) AS channel_views_count_age
FROM first_views
GROUP BY channel_id, user_age;

SELECT channel_id, COUNT(*) AS channel_views
FROM first_views
GROUP BY channel_id;

SELECT channel_id, seconds_offset, COUNT(*) AS weighted_channel_views
FROM views
GROUP BY channel_id, seconds_offset;


SELECT MOD(timestamp, 86400) / 3600 AS interaction_hour, user_country, COUNT(*) AS interaction_count
FROM (
    SELECT timestamp, user_country
    FROM first_views
    UNION ALL
    SELECT timestamp, user_country
    FROM likes
    UNION ALL
    SELECT timestamp, user_country
    FROM subscribes
) interactions
GROUP BY interaction_hour, user_country
ORDER BY interaction_count DESC
LIMIT 1;


SELECT video_id, COUNT(*) AS view_count
FROM first_views
WHERE timestamp >= toUnixTimestamp(now()) - 3600
GROUP BY video_id
ORDER BY view_count DESC
LIMIT 10;

SELECT video_id, COUNT(*) AS view_count
FROM first_views
WHERE timestamp >= toUnixTimestamp(now()) - 86400
GROUP BY video_id
ORDER BY view_count DESC
LIMIT 10;

SELECT video_id, COUNT(*) AS view_count
FROM first_views
WHERE timestamp >= toUnixTimestamp(now()) - 604800
GROUP BY video_id
ORDER BY view_count DESC
LIMIT 10;

SELECT category, video_id, COUNT(*) AS view_count
FROM first_views
WHERE timestamp >= toUnixTimestamp(now()) - 3600
GROUP BY category, video_id
ORDER BY view_count DESC
LIMIT 10 PER PARTITION;

SELECT category, video_id, COUNT(*) AS view_count
FROM first_views
WHERE timestamp >= toUnixTimestamp(now()) - 86400
GROUP BY category, video_id
ORDER BY view_count DESC
LIMIT 10 PER PARTITION;

SELECT category, video_id, COUNT(*) AS view_count
FROM first_views
WHERE timestamp >= toUnixTimestamp(now()) - 604800
GROUP BY category, video_id
ORDER BY view_count DESC
LIMIT 10 PER PARTITION;

SELECT category, channel_id, COUNT(*) AS view_count
FROM first_views
WHERE timestamp >= toUnixTimestamp(now()) - 3600
GROUP BY category, channel_id
ORDER BY view_count DESC
LIMIT 10 PER PARTITION;

SELECT category, channel_id, COUNT(*) AS view_count
FROM first_views
WHERE timestamp >= toUnixTimestamp(now()) - 86400
GROUP BY category, channel_id
ORDER BY view_count DESC
LIMIT 10 PER PARTITION;

SELECT category, channel_id, COUNT(*) AS view_count
FROM first_views
WHERE timestamp >= toUnixTimestamp(now()) - 604800
GROUP BY category, channel_id
ORDER BY view_count DESC
LIMIT 10 PER PARTITION;