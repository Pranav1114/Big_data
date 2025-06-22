"""
Music Data Analysis and Recommendation System

Author: Pranav Sri Vasthav Tenali Gnana
Roll No: G24AI1114
Platform: SQLite using DBeaver on Parrot OS
"""

import pymysql

connection = pymysql.connect(
    host="127.0.0.1",
    user="root",
    password="1234",
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)
cursor = connection.cursor()

# Create and use the database
cursor.execute("CREATE DATABASE IF NOT EXISTS music_streaming_DB;")
cursor.execute("USE music_streaming_DB;")

# Table Creation
cursor.execute("""CREATE TABLE IF NOT EXISTS Users (
  user_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE
);""")

cursor.execute("""CREATE TABLE IF NOT EXISTS Songs (
  song_id INTEGER PRIMARY KEY,
  title TEXT NOT NULL,
  artist TEXT NOT NULL,
  genre TEXT
);""")

cursor.execute("""CREATE TABLE IF NOT EXISTS Listens (
  listen_id INTEGER PRIMARY KEY,
  user_id INTEGER NOT NULL,
  song_id INTEGER NOT NULL,
  rating FLOAT,
  listen_time TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES Users(user_id),
  FOREIGN KEY (song_id) REFERENCES Songs(song_id)
);""")

cursor.execute("""CREATE TABLE IF NOT EXISTS Recommendations (
  recommendation_id INTEGER PRIMARY KEY,
  recommendation_time TIMESTAMP,
  user_id INTEGER NOT NULL,
  song_id INTEGER NOT NULL,
  FOREIGN KEY (user_id) REFERENCES Users(user_id),
  FOREIGN KEY (song_id) REFERENCES Songs(song_id)
);""")

# Insert Users
cursor.execute("""INSERT INTO Users VALUES
(1, 'Mickey', 'mickey@example.com'),
(2, 'Minnie', 'minnie@example.com'),
(3, 'Daffy', 'daffy@example.com'),
(4, 'Donald', 'donald@example.com');""")

# Insert Songs
cursor.execute("""INSERT INTO Songs VALUES
(1, 'Evermore', 'Taylor Swift', 'Pop'),
(2, 'Willow', 'Taylor Swift', 'Pop'),
(3, 'Shape of You', 'Ed Sheeran', 'Pop'),
(4, 'Perfect', 'Ed Sheeran', 'Pop'),
(5, 'Yesterday', 'The Beatles', 'Rock'),
(6, 'Let It Be', 'The Beatles', 'Rock'),
(7, 'Yellow Submarine', 'The Beatles', 'Rock'),
(8, 'Hey Jude', 'The Beatles', 'Rock'),
(9, 'Bad Blood', 'Taylor Swift', 'Pop');""")

# Insert Listens
cursor.execute("""INSERT INTO Listens VALUES
(1, 1, 1, 4.5, '2024-08-25 10:00:00'),
(2, 2, 2, NULL, NULL),
(3, 2, 7, 4.7, '2024-08-28 09:20:00'),
(4, 2, 8, 4.8, '2024-08-27 16:45:00'),
(5, 1, 5, 4.4, '2024-08-26 11:00:00'),
(6, 3, 2, 4.3, '2024-08-26 14:20:00'),
(7, 3, 3, 4.6, '2024-08-28 15:00:00');""")
connection.commit()

# --------- QUERIES ---------

print("\nAll Pop Songs:")
cursor.execute("SELECT * FROM Songs WHERE genre = 'Pop';")
print(cursor.fetchall())

print("\nSongs with rating > 4.6:")
cursor.execute("SELECT * FROM Listens WHERE rating > 4.6;")
print(cursor.fetchall())

print("\nAverage rating per song:")
cursor.execute("""
SELECT s.title, AVG(l.rating) AS avg_rating
FROM Songs s JOIN Listens l ON s.song_id = l.song_id
GROUP BY s.title;
""")
print(cursor.fetchall())

print("\nSongs never listened to:")
cursor.execute("""
SELECT * FROM Songs
WHERE song_id NOT IN (SELECT song_id FROM Listens);
""")
print(cursor.fetchall())

print("\nClassic genre songs:")
cursor.execute("""
SELECT title, artist FROM Songs WHERE genre = 'Rock';
""")
print(cursor.fetchall())

print("\nClassic songs starting with 'Ye':")
cursor.execute("""
SELECT title, artist FROM Songs 
WHERE genre = 'Rock' AND title LIKE 'Ye%';
""")
print(cursor.fetchall())

print("\nAll genres:")
cursor.execute("SELECT genre FROM Songs;")
print(cursor.fetchall())

print("\nDistinct genres:")
cursor.execute("SELECT DISTINCT genre FROM Songs;")
print(cursor.fetchall())

print("\nCount of Taylor Swift songs by genre:")
cursor.execute("""
SELECT artist, genre, COUNT(*) as num_songs
FROM Songs
WHERE artist = 'Taylor Swift'
GROUP BY artist, genre;
""")
print(cursor.fetchall())

print("\nCount of songs by all artists and genres:")
cursor.execute("""
SELECT artist, genre, COUNT(*) as num_songs
FROM Songs
GROUP BY artist, genre;
""")
print(cursor.fetchall())

print("\nFull join of Users, Listens, and Songs:")
cursor.execute("""
SELECT * FROM Songs
LEFT JOIN Listens ON Songs.song_id = Listens.song_id
LEFT JOIN Users ON Listens.user_id = Users.user_id;
""")
print(cursor.fetchall())

print("\nSongs by Ed Sheeran and Taylor Swift:")
cursor.execute("""
SELECT title, artist FROM Songs 
WHERE artist IN ('Ed Sheeran', 'Taylor Swift');
""")
print(cursor.fetchall())

print("\nSongs from both Pop and Rock genres:")
cursor.execute("""
SELECT title, artist FROM Songs WHERE genre='Pop'
UNION
SELECT title, artist FROM Songs WHERE genre='Rock';
""")
print(cursor.fetchall())

print("\nSongs with NULL listen_time (not recently listened):")
cursor.execute("""
SELECT title, artist FROM Songs 
WHERE song_id IN (
  SELECT song_id FROM Listens WHERE listen_time IS NULL
);
""")
print(cursor.fetchall())

# --------- COLLABORATIVE FILTERING ---------

print("\nShared song pairs by multiple users:")
cursor.execute("""
WITH song_similarity AS (
  SELECT u1.song_id AS song1, u2.song_id AS song2, COUNT(*) AS common_users
  FROM Listens u1
  JOIN Listens u2 ON u1.user_id = u2.user_id AND u1.song_id != u2.song_id
  GROUP BY u1.song_id, u2.song_id
  HAVING COUNT(*) > 1
),
recs AS (
  SELECT L.user_id, song2 AS song_id
  FROM song_similarity
  JOIN Listens L ON L.song_id = song_similarity.song1
  WHERE song_similarity.song2 NOT IN (
    SELECT song_id FROM Listens temp WHERE temp.user_id = L.user_id
  )
)
SELECT * FROM recs;
""")
print(cursor.fetchall())

print("\nInsert recommendations into Recommendations table:")
cursor.execute("""
INSERT INTO Recommendations (recommendation_id, recommendation_time, user_id, song_id)
SELECT 
  ROW_NUMBER() OVER () AS recommendation_id,
  CURRENT_TIMESTAMP() AS recommendation_time,
  user_id,
  song_id
FROM (
  WITH song_similarity AS (
    SELECT u1.song_id AS song1, u2.song_id AS song2, COUNT(*) AS common_users
    FROM Listens u1
    JOIN Listens u2 ON u1.user_id = u2.user_id AND u1.song_id != u2.song_id
    GROUP BY u1.song_id, u2.song_id
    HAVING COUNT(*) > 1
  ),
  recs AS (
    SELECT L.user_id, song2 AS song_id
    FROM song_similarity
    JOIN Listens L ON L.song_id = song_similarity.song1
    WHERE song_similarity.song2 NOT IN (
      SELECT song_id FROM Listens temp WHERE temp.user_id = L.user_id
    )
  )
  SELECT * FROM recs
) AS final;
""")
connection.commit()

print("\nRecommendations for Minnie (user_id = 2):")
cursor.execute("""
WITH song_similarity AS (
  SELECT u1.song_id AS song1, u2.song_id AS song2, COUNT(*) AS common_users
  FROM Listens u1
  JOIN Listens u2 ON u1.user_id = u2.user_id AND u1.song_id != u2.song_id
  GROUP BY u1.song_id, u2.song_id
  HAVING COUNT(*) > 1
),
recs AS (
  SELECT L.user_id, song2 AS song_id
  FROM song_similarity
  JOIN Listens L ON L.song_id = song_similarity.song1
  WHERE song_similarity.song2 NOT IN (
    SELECT song_id FROM Listens temp WHERE temp.user_id = L.user_id
  )
)
SELECT s.title, s.artist
FROM recs JOIN Songs s ON recs.song_id = s.song_id
WHERE recs.user_id = 2;
""")
print(cursor.fetchall())

print("\nTime-sensitive recommendations for Minnie:")
cursor.execute("""
WITH recent_listens AS (
  SELECT * FROM Listens WHERE listen_time IS NOT NULL
),
song_similarity AS (
  SELECT l1.song_id AS song1, l2.song_id AS song2, COUNT(*) AS common_users
  FROM recent_listens l1
  JOIN recent_listens l2 ON l1.user_id = l2.user_id AND l1.song_id != l2.song_id
  GROUP BY l1.song_id, l2.song_id
  HAVING COUNT(*) > 1
),
recs AS (
  SELECT rl.user_id, song2 AS song_id
  FROM song_similarity
  JOIN recent_listens rl ON rl.song_id = song_similarity.song1
  WHERE song_similarity.song2 NOT IN (
    SELECT song_id FROM Listens WHERE user_id = rl.user_id
  )
)
SELECT DISTINCT s.title, s.artist, r.user_id
FROM recs r JOIN Songs s ON r.song_id = s.song_id
WHERE r.user_id = 2;
""")
print(cursor.fetchall())

connection.close()
