import os
import pandas as pd
from azure.storage.blob import ContainerClient
import pyodbc
import configparser

# Get the full path to the config.ini file
config = configparser.ConfigParser()
config.read("/Users/gomes/Desktop/Projects/Data Engineer/3-Project/config/config.ini")

# Set up your credentials and configurations
storage_account_name = config.get("synapses", "storage_account_name")
storage_account_key = config.get("synapses", "storage_account_key")
container_name = config.get("blob", "container_name")
connection_string = "DefaultEndpointsProtocol=https;AccountName={};AccountKey={};EndpointSuffix=core.windows.net".format(storage_account_name, storage_account_key)

# Connect to Blob Storage
container_client = ContainerClient.from_connection_string(connection_string, container_name)

# Initialize the list of Parquet files
parquet_files = {
    "processedArtists.parquet": None,
    "processedListeningHistory.parquet": None,
    "processedSongs.parquet": None,
    "processedUserPreferences.parquet": None,
    "processedUsers.parquet": None
}


# Retrieve the list of Parquet files in the 'process' folder
for blob in container_client.list_blobs(name_starts_with="process/"):
    if blob.name.endswith('.parquet') and blob.name.split('/')[-1] in parquet_files:
        parquet_files[blob.name.split('/')[-1]] = blob

# Define Azure Synapse Analytics connection string
synapse_conn_str = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:wu3synapseworkspace.sql.azuresynapse.net,1433;Database=wu3pool;Uid=wu3user;Pwd=Wu3password;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

# Connect to Azure Synapse Analytics
cnxn = pyodbc.connect(synapse_conn_str, autocommit=True)
cursor = cnxn.cursor()

# Create Users table
cursor.execute("""
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Users')
BEGIN
    CREATE TABLE [Users] (
        user_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY NONCLUSTERED NOT ENFORCED,
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
        artist_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY NONCLUSTERED NOT ENFORCED,
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
        song_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY NONCLUSTERED NOT ENFORCED,
        title NVARCHAR(255) NOT NULL,
        artist_id UNIQUEIDENTIFIER NOT NULL,
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
        history_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY NONCLUSTERED NOT ENFORCED,
        user_id UNIQUEIDENTIFIER NOT NULL,
        song_id UNIQUEIDENTIFIER NOT NULL,
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
        preference_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY NONCLUSTERED NOT ENFORCED,
        user_id UNIQUEIDENTIFIER NOT NULL,
        artist_id UNIQUEIDENTIFIER NOT NULL,
        preference_score FLOAT NOT NULL
    );
END
""")
# Insert data into the tables
cnxn.autocommit = False


# Read the data from the Parquet files into dictionaries
data = {}
for parquet_file, blob in parquet_files.items():
    if blob is None:
        continue

    # Download the Parquet file to a temporary local file
    with open(parquet_file, 'wb') as f:
        blob_data = container_client.download_blob(blob.name).readall()
        f.write(blob_data)

    # Read the Parquet file into a Pandas DataFrame
    df = pd.read_parquet(parquet_file)

    # Convert the DataFrame to a list of dictionaries
    data[os.path.splitext(parquet_file)[0]] = df.to_dict('records')

    # Delete the temporary local file
    os.remove(parquet_file)


# Insert users data
for user in data["processedUsers"]:
    cursor.execute("""
        INSERT INTO [Users] (user_id, username, email, birthdate, country, join_date)
        VALUES (?, ?, ?, ?, ?, ?)
    """, user["user_id"], user["username"], user["email"], user["birthdate"], user["country"], user["join_date"])
    print(f"Inserted user: {user['username']}")

# Insert artists data
for artist in data["processedArtists"]:
    cursor.execute("""
        INSERT INTO Artists (artist_id, name, genre, popularity, followers)
        VALUES (?, ?, ?, ?, ?)
    """, artist["Artist_id"], artist["name"], artist["genre"], artist["popularity"], artist["followers"])
    print(f"Inserted artist: {artist['name']}")

# Insert songs data
for song in data["processedSongs"]:
    cursor.execute("""
        INSERT INTO Songs (song_id, title, artist_id, album,
        release_date, duration_ms, popularity)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, song["song_id"], song["title"], song["artist_id"], song["album"], song["release_date"], song["duration_ms"], song["popularity"])
    print(f"Inserted song: {song['title']}")

# Insert listening history data
for history in data["processedListeningHistory"]:
    cursor.execute("""
        INSERT INTO ListeningHistory (history_id, user_id, song_id, timestamp, listening_duration)
        VALUES (?, ?, ?, ?, ?)
    """, history["history_id"], history["user_id"], history["song_id"], history["timestamp"], history["listening_duration"])
    print(f"Inserted listening history: {history['history_id']}")

# Insert user preferences data
for preference in data["processedUserPreferences"]:
    cursor.execute("""
        INSERT INTO UserPreferences (preference_id, user_id, artist_id, preference_score)
        VALUES (?, ?, ?, ?)
    """, preference["preference_id"], preference["user_id"], preference["artist_id"], preference["preference_score"])
    print(f"Inserted user preference: {preference['preference_id']}")


cnxn.commit()
cnxn.autocommit = True

cursor.close()
cnxn.close()
