WITH users AS (
    SELECT *
    FROM {{ source('dbo', 'Users') }}
),

listening_history AS (
    SELECT *
    FROM {{ source('dbo', 'ListeningHistory') }}
),

songs AS (
    SELECT *
    FROM {{ source('dbo', 'Songs') }}
),

artists AS (
    SELECT *
    FROM {{ source('dbo', 'Artists') }}
),

user_preferences AS (
    SELECT *
    FROM {{ source('dbo', 'UserPreferences') }}
)

SELECT
  u.user_id,
  u.username,
  u.email,
  u.birthdate,
  u.country,
  u.join_date,
  a.artist_id,
  a.name AS artist_name,
  a.genre,
  a.popularity AS artist_popularity,
  a.followers,
  s.song_id,
  s.title,
  s.album,
  s.release_date,
  s.duration_ms,
  s.popularity AS song_popularity,
  lh.history_id,
  lh.timestamp,
  lh.listening_duration,
  up.preference_id,
  up.preference_score
FROM
  users u
JOIN
  listening_history lh ON u.user_id = lh.user_id
JOIN
  songs s ON lh.song_id = s.song_id
JOIN
  artists a ON s.artist_id = a.artist_id
JOIN
  user_preferences up ON u.user_id = up.user_id AND a.artist_id = up.artist_id;
