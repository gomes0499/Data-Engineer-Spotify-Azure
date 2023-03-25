from faker import Faker
import pyodbc
import random
import configparser


# Get the full path to the config.ini file
config = configparser.ConfigParser()
config.read("/Users/gomes/Desktop/Projects/Data Engineer/3-Project/config/config.ini")


# Azure SQL Server variables for connection
server = config.get("sqlserver", "server")
database = config.get("sqlserver", "database")
username = config.get("sqlserver", "username")
password = config.get("sqlserver", "password")
driver = 'ODBC Driver 18 for SQL Server'

# Connection string
connection_string = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'

# Connect to the Azure SQL Server
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Instantiate the class to generate dummy data
fake = Faker()

# Generate fake user data
def create_fake_user():
    user = {
        "user_id": fake.uuid4(),
        "username": fake.user_name(),
        "email": fake.email(),
        "birthdate": str(fake.date_of_birth(tzinfo = None, minimum_age = 13, maximum_age = 70)),
        "country": fake.country_code(),
        "join_date": str(fake.date_between(start_date = "-10y", end_date = "today"))
    }   
    return user

# Generate fake artist data
def create_fake_artist():
    music_genres = [
        'Pop', 'Rock', 'Hip-Hop', 'Electronic', 'Jazz', 'Classical', 'R&B', 'Reggae',
        'Country', 'Blues', 'Folk', 'Latin', 'Metal', 'Punk', 'Funk', 'Soul',
    ]

    artist = {
        "artist_id": fake.uuid4(),
        "name": fake.name(),
        "genre": random.choice(music_genres),
        "popularity": random.randint(1, 100),
        "followers": random.randint(1, 1000000),
    }
    return artist

# Generate fake song date
def create_fake_song(artist_id):
    song = {
        "song_id": fake.uuid4(),
        "title": fake.sentence(nb_words = 4),
        "artist_id": artist_id,
        "album": fake.sentence(nb_words = 3),
        "release_date": str(fake.date_between(start_date = "-30y", end_date = "today")),
        "duration_ms": random.randint(100000, 600000),
        "popularity": random.randint(1, 100)
    }
    return song

# Generate fake listening history data
def create_fake_listening_history(user_id, song_id):
    listening_history = {
        "history_id": fake.uuid4(),
        "user_id": user_id,
        "song_id": song_id,
        "timestamp": str(fake.date_time_between(start_date = "-1y", end_date = "now")),
        "listening_duration": random.randint(10000, 600000)
    }
    return listening_history

# Generate fake user preferences data
def create_fake_user_preferences(user_id, artist_id):
    user_preferences = {
        "preference_id": fake.uuid4(),
        "user_id": user_id,
        "artist_id": artist_id,
        "preference_score": random.uniform(0, 1)
    }
    return user_preferences

# Generate sample data
num_user = 100
num_artist = 50
num_song_per_artist = 10
num_histories_per_user = 5
num_preferences_per_user = 5

users = [create_fake_user() for _ in range(num_user)]
artists = [create_fake_artist() for _ in range(num_artist)]
songs = [create_fake_song(artist["artist_id"]) for artist in artists for _ in range(num_song_per_artist)]

listening_histories = []
for user in users:
    for _ in range(num_histories_per_user):
        song = random.choice(songs)
        listening_histories.append(create_fake_listening_history(user["user_id"], song["song_id"]))

user_preferences = []
for user in users:
    for _ in range(num_preferences_per_user):
        artist = random.choice(artists)
        user_preferences.append(create_fake_user_preferences(user["user_id"], artist["artist_id"]))        


# Create Users table
cursor.execute("""
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Users')
BEGIN
    CREATE TABLE [Users] (
        user_id UNIQUEIDENTIFIER PRIMARY KEY,
        username NVARCHAR(255) NOT NULL,
        email NVARCHAR(255) NOT NULL,
        birthdate DATE NOT NULL,
        country CHAR(2) NOT NULL,
        join_date DATE NOT NULL
    );
END
""")

# Create Artists table
cursor.execute("""
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Artists')
BEGIN
    CREATE TABLE Artists (
        Artist_id UNIQUEIDENTIFIER PRIMARY KEY,
        name NVARCHAR(255) NOT NULL,
        genre NVARCHAR(255) NOT NULL,
        popularity INT NOT NULL,
        followers INT NOT NULL
    );
END
""")

# Create Songs table
cursor.execute("""
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Songs')
BEGIN
    CREATE TABLE Songs (
        song_id UNIQUEIDENTIFIER PRIMARY KEY,
        title NVARCHAR(255) NOT NULL,
        artist_id UNIQUEIDENTIFIER FOREIGN KEY REFERENCES Artists(artist_id),
        album NVARCHAR(255) NOT NULL,
        release_date DATE NOT NULL,
        duration_ms INT NOT NULL,
        popularity INT NOT NULL
    );
END
""")

# Create Listening History table
cursor.execute("""
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ListeningHistory')
BEGIN
    CREATE TABLE ListeningHistory (
        history_id UNIQUEIDENTIFIER PRIMARY KEY,
        user_id UNIQUEIDENTIFIER FOREIGN KEY REFERENCES [Users] (user_id),
        song_id UNIQUEIDENTIFIER FOREIGN KEY REFERENCES Songs(song_id),
        timestamp DATETIME NOT NULL,
        listening_duration INT NOT NULL
    );
END
""")

# Create User Preferences table
cursor.execute("""
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'UserPreferences')
BEGIN
    CREATE TABLE UserPreferences (
        preference_id UNIQUEIDENTIFIER PRIMARY KEY,
        user_id UNIQUEIDENTIFIER FOREIGN KEY REFERENCES Users(user_id),
        artist_id UNIQUEIDENTIFIER FOREIGN KEY REFERENCES Artists(artist_id),
        preference_score FLOAT NOT NULL
    );
END
""")

conn.commit()

# Insert users data
for user in users:
    cursor.execute("""
        INSERT INTO [Users] (user_id, username, email, birthdate, country, join_date)
        VALUES (?, ?, ?, ?, ?, ?)
    """, user["user_id"], user["username"], user["email"], user["birthdate"], user["country"], user["join_date"])
    print(f"Inserted user: {user['username']}")

# Insert artists data
for artist in artists:
    cursor.execute("""
        INSERT INTO Artists (artist_id, name, genre, popularity, followers)
        VALUES (?, ?, ?, ?, ?)
    """, artist["artist_id"], artist["name"], artist["genre"], artist["popularity"], artist["followers"])
    print(f"Inserted artist: {artist['name']}")

# Insert songs data
for song in songs:
    cursor.execute("""
        INSERT INTO Songs (song_id, title, artist_id, album,
        release_date, duration_ms, popularity)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, song["song_id"], song["title"], song["artist_id"], song["album"], song["release_date"], song["duration_ms"], song["popularity"])
    print(f"Inserted song: {song['title']}")

# Insert listening history data
for history in listening_histories:
    cursor.execute("""
        INSERT INTO ListeningHistory (history_id, user_id, song_id, timestamp, listening_duration)
        VALUES (?, ?, ?, ?, ?)
    """, history["history_id"], history["user_id"], history["song_id"], history["timestamp"], history["listening_duration"])
    print(f"Inserted listening history: {history['history_id']}")

# Insert user preferences data
for preference in user_preferences:
    cursor.execute("""
        INSERT INTO UserPreferences (preference_id, user_id, artist_id, preference_score)
        VALUES (?, ?, ?, ?)
    """, preference["preference_id"], preference["user_id"], preference["artist_id"], preference["preference_score"])
    print(f"Inserted user preference: {preference['preference_id']}")


# Commit the changes and close the connection
conn.commit()
conn.close()