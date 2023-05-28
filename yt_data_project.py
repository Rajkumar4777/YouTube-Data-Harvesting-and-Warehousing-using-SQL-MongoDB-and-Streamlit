from googleapiclient.discovery import build
import pymongo

# connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["database"]
collectionss = db["collectionss"]


# youTube API key
api_key='AIzaSyA5tUkia4O-J4eXEaeHX0C8e_05a0akMbo'
# Set the channel ID and playlist ID
channel_id = 'UChL9x-Q75LCuYHIFRDT_T1A'
playlist_id = 'UUhL9x-Q75LCuYHIFRDT_T1A'

# connection between YouTube API client
youtube = build('youtube', 
                'v3', 
                developerKey=api_key)

#function to get channel information
def get_channel_info(youtube, channel_id):
    request = youtube.channels().list( part="snippet,contentDetails,statistics",id=channel_id)
    response = request.execute()
    channel_info = {
        'Channel_Name': response['items'][0]['snippet']['title'],
        'Channel_id': response['items'][0]['id'],
        'Subscription_Count': response['items'][0]['statistics']['subscriberCount'],
        'Channel_Views': response['items'][0]['statistics']['viewCount'],
        'Channel_Description': response['items'][0]['snippet']['description'],
        'Playlist_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }
    return channel_info

#function to get videos in a playlist
def get_playlist_videos(youtube, playlist_id):
    request = youtube.playlistItems().list(part="snippet",playlistId=playlist_id,maxResults=30)
    response = request.execute()
    videos = []
    for item in response['items']:
        video_id = item['snippet']['resourceId']['videoId']
        video_info = {
            'Video_id':video_id,
            'Channel_id': channel_id,
            'Video_Name':item['snippet']['title'],
            'Video_Description': item['snippet']['description'],
            'PublishedAt': item['snippet']['publishedAt'],
            'Thumbnail': item['snippet']['thumbnails']['high']['url']
        }

        # Get video details
        stats_request =youtube.videos().list(part="statistics,contentDetails", id=video_id)
        stats_response =stats_request.execute()
        stats_data = {
            'View_Count':int(stats_response['items'][0]['statistics']['viewCount']),
            'Comment_Count':int(stats_response['items'][0]['statistics']['commentCount']),
            'Favarite_Count': int(stats_response['items'][0]['statistics']['favoriteCount']),
            'like_Count':int(stats_response['items'][0]['statistics'].get('likeCount',0)),
            'Dislike_Count': int(stats_response['items'][0]['statistics'].get('dislikeCount', 0)),
            'Duration': stats_response['items'][0]['contentDetails']['duration']
        }
        video_info.update(stats_data)

        # Get video comments details
        comments_request = youtube.commentThreads().list(part="snippet",maxResults=10,videoId=video_id)
        comments_response = comments_request.execute()
        comments = []
        for comment_item in comments_response['items']:
            comment = {
                'Comment_id': comment_item['id'],
                'Author': comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'Comment': comment_item['snippet']['topLevelComment']['snippet']['textOriginal'],
                'Like_Count': comment_item['snippet']['topLevelComment']['snippet']['likeCount'],
                'PublishedAt': comment_item['snippet']['topLevelComment']['snippet']['publishedAt']
            }
            comments.append(comment)
                # inserting the comments dict into video info
        video_info['Comments'] = comments

        #append video info
        videos.append(video_info)
    return videos
    
#Get channel info and playlist videos
channel_info = get_channel_info(youtube, channel_id)  # firts 
playlist_videos = get_playlist_videos(youtube, playlist_id)

# Insert the channel info and videos into the MongoDB collection
document = {
    'Channel_Info': channel_info,
    'Playlist_Videos': playlist_videos
}
collectionss.insert_one(document)
print("mongodb_Test_Case_1_passed")



import mysql.connector

# Connect to MySQL
mysql_db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="12345",  # string
    database="raj4777"
  )
mysql_cursor = mysql_db.cursor()

mysql_cursor.execute("""CREATE TABLE IF NOT EXISTS channel (
        Channel_id VARCHAR(255) PRIMARY KEY,
        Channel_Name VARCHAR(255),
        Channel_Description VARCHAR(5500),
        Subscription_Count INT,
        Channel_Views BIGINT,
        Playlist_id VARCHAR(255))""")

mysql_db.commit()

# Convert Channel_Info to MySQL 
mysql_data = (
    channel_info['Channel_id'],
    channel_info['Channel_Name'],
    channel_info['Channel_Description'],
    int(channel_info['Subscription_Count']),
    int(channel_info['Channel_Views']),
    channel_info['Playlist_id']
)

# Insert Channel_Info into MySQL
mysql_query = "INSERT INTO channel (Channel_id, Channel_Name, Channel_Description, Subscription_Count, Channel_Views, Playlist_id) VALUES (%s, %s, %s, %s, %s, %s)"
mysql_cursor.execute(mysql_query, mysql_data)
mysql_db.commit()

# create the 'video' table in MySQL
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS Video (
        Video_id VARCHAR(255) PRIMARY KEY,
        Channel_id VARCHAR(255),
        Video_Name VARCHAR(5500),
        Video_Description VARCHAR(5500),
        PublishedAt VARCHAR(255),
        Thumbnail VARCHAR(255),
        View_Count INT,
        Comment_Count INT,
        Favarite_Count INT,
        like_Count INT,
        Dislike_Count INT,
        Duration VARCHAR(255))""")


# Commit the table creation query
mysql_db.commit()

# Insert Playlist_Videos into MySQL
for video in playlist_videos:
    mysql_data = (
        video['Video_id'],
        video['Channel_id'],
        video['Video_Name'],
        video['Video_Description'],
        video['PublishedAt'],
        video['Thumbnail'],
        video['View_Count'],
        video['Comment_Count'],
        video['Favarite_Count'],
        video['like_Count'],
        video['Dislike_Count'],
        video['Duration']
    )
    mysql_query = "INSERT INTO Video (Video_id, Channel_id, Video_Name, Video_Description, PublishedAt, Thumbnail, View_Count, Comment_Count,Favarite_Count,like_Count,Dislike_Count,Duration) VALUES (%s,%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mysql_cursor.execute(mysql_query, mysql_data)

# Commit the insert queries
mysql_db.commit()

# Create the 'comment' table in MySQL
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS comment (
        Comment_id VARCHAR(255) PRIMARY KEY,
        Author VARCHAR(255),
        Like_Count INT,
        PublishedAt VARCHAR(255))""")

# Commit the comment creation query
mysql_db.commit()


# Insert comments into MySQL
for video in playlist_videos:
    for comment in video['Comments']:
        mysql_data = (
            comment['Comment_id'],
            comment['Author'],
            comment['Like_Count'],
            comment['PublishedAt']
        )
        mysql_query = "INSERT INTO comment (Comment_id, Author, Like_Count, PublishedAt) VALUES (%s, %s, %s, %s)"
        mysql_cursor.execute(mysql_query, mysql_data)

# Commit the insert queries
mysql_db.commit()


print("mysql_text_case_1_passed")




channel_id = 'UCduIoIMfD8tT3KoU0-zBRgQ'
playlist_id = 'UUduIoIMfD8tT3KoU0-zBRgQ'

# Define function to get channel information
def get_channel_info(youtube, channel_id):
    request = youtube.channels().list( part="snippet,contentDetails,statistics",id=channel_id)
    response = request.execute()
    channel_info = {
        'Channel_Name': response['items'][0]['snippet']['title'],
        'Channel_id': response['items'][0]['id'],
        'Subscription_Count': response['items'][0]['statistics']['subscriberCount'],
        'Channel_Views': response['items'][0]['statistics']['viewCount'],
        'Channel_Description': response['items'][0]['snippet']['description'],
        'Playlist_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }
    return channel_info

# Define function to get videos in a playlist
def get_playlist_videos(youtube, playlist_id):
    request = youtube.playlistItems().list(part="snippet",playlistId=playlist_id,maxResults=30)
    response = request.execute()
    videos = []
    for item in response['items']:
        video_id = item['snippet']['resourceId']['videoId']
        video_info = {
            'Video_id':video_id,
            'Channel_id': channel_id,
            'Video_Name':item['snippet']['title'],
            'Video_Description': item['snippet']['description'],
            'PublishedAt': item['snippet']['publishedAt'],
            'Thumbnail': item['snippet']['thumbnails']['high']['url']
        }

        # Get video details
        stats_request =youtube.videos().list(part="statistics,contentDetails", id=video_id)
        stats_response =stats_request.execute()
        stats_data = {
            'View_Count':int(stats_response['items'][0]['statistics']['viewCount']),
            'Comment_Count':int(stats_response['items'][0]['statistics']['commentCount']),
            'Favarite_Count': int(stats_response['items'][0]['statistics']['favoriteCount']),
            'like_Count':int(stats_response['items'][0]['statistics'].get('likeCount',0)),
            'Dislike_Count': int(stats_response['items'][0]['statistics'].get('dislikeCount', 0)),
            'Duration': stats_response['items'][0]['contentDetails']['duration']
        }
        video_info.update(stats_data)

        # Get video comments details
        comments_request = youtube.commentThreads().list(part="snippet",maxResults=10,videoId=video_id)
        comments_response = comments_request.execute()
        comments = []
        for comment_item in comments_response['items']:
            comment = {
                'Comment_id': comment_item['id'],
                'Author': comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'Comment': comment_item['snippet']['topLevelComment']['snippet']['textOriginal'],
                'Like_Count': comment_item['snippet']['topLevelComment']['snippet']['likeCount'],
                'PublishedAt': comment_item['snippet']['topLevelComment']['snippet']['publishedAt']
            }
            comments.append(comment)
                # inserting the comments dict into video info
        video_info['Comments'] = comments

        #append video info
        videos.append(video_info)
    return videos
    
# Get the channel info and playlist videos
channel_info = get_channel_info(youtube, channel_id)
playlist_videos = get_playlist_videos(youtube, playlist_id)

# Insert the channel info and videos into the MongoDB collection
document = {
    'Channel_Info': channel_info,
    'Playlist_Videos': playlist_videos
}
collectionss.insert_one(document)
print("mongodb_Test_Case_2_passed")




mysql_cursor.execute("""CREATE TABLE IF NOT EXISTS channel (
        Channel_id VARCHAR(255) PRIMARY KEY,
        Channel_Name VARCHAR(255),
        Channel_Description VARCHAR(5500),
        Subscription_Count INT,
        Channel_Views BIGINT,
        Playlist_id VARCHAR(255))""")

mysql_db.commit()

# Convert Channel_Info to MySQL 
mysql_data = (
    channel_info['Channel_id'],
    channel_info['Channel_Name'],
    channel_info['Channel_Description'],
    int(channel_info['Subscription_Count']),
    int(channel_info['Channel_Views']),
    channel_info['Playlist_id']
)

# Insert Channel_Info into MySQL
mysql_query = "INSERT INTO channel (Channel_id, Channel_Name, Channel_Description, Subscription_Count, Channel_Views, Playlist_id) VALUES (%s, %s, %s, %s, %s, %s)"
mysql_cursor.execute(mysql_query, mysql_data)
mysql_db.commit()

# Create the 'video' table in MySQL
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS Video (
        Video_id VARCHAR(255) PRIMARY KEY,
        Channel_id VARCHAR(255),
        Video_Name VARCHAR(5500),
        Video_Description VARCHAR(5500),
        PublishedAt VARCHAR(255),
        Thumbnail VARCHAR(255),
        View_Count INT,
        Comment_Count INT,
        Favarite_Count INT,
        like_Count INT,
        Dislike_Count INT,
        Duration VARCHAR(255))""")


# Commit the table creation query
mysql_db.commit()

# Insert Playlist_Videos into MySQL
for video in playlist_videos:
    mysql_data = (
        video['Video_id'],
        video['Channel_id'],
        video['Video_Name'],
        video['Video_Description'],
        video['PublishedAt'],
        video['Thumbnail'],
        video['View_Count'],
        video['Comment_Count'],
        video['Favarite_Count'],
        video['like_Count'],
        video['Dislike_Count'],
        video['Duration']
    )
    mysql_query = "INSERT INTO Video (Video_id, Channel_id, Video_Name, Video_Description, PublishedAt, Thumbnail, View_Count, Comment_Count,Favarite_Count,like_Count,Dislike_Count,Duration) VALUES (%s,%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mysql_cursor.execute(mysql_query, mysql_data)

# Commit the insert queries
mysql_db.commit()

# Create the 'comment' table in MySQL
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS comment (
        Comment_id VARCHAR(255) PRIMARY KEY,
        Author VARCHAR(255),
        Like_Count INT,
        PublishedAt VARCHAR(255))""")

# Commit the comment creation query
mysql_db.commit()


# Insert comments into MySQL
for video in playlist_videos:
    for comment in video['Comments']:
        mysql_data = (
            comment['Comment_id'],
            comment['Author'],
            comment['Like_Count'],
            comment['PublishedAt']
        )
        mysql_query = "INSERT INTO comment (Comment_id, Author, Like_Count, PublishedAt) VALUES (%s, %s, %s, %s)"
        mysql_cursor.execute(mysql_query, mysql_data)

# Commit the insert queries
mysql_db.commit()


print("mysql_text_case_2_passed")


channel_id = 'UC7beWKhXaZuRpD-oKQj852g'
playlist_id = 'UU7beWKhXaZuRpD-oKQj852g'

# function to get channel information
def get_channel_info(youtube, channel_id):
    request = youtube.channels().list( part="snippet,contentDetails,statistics",id=channel_id)
    response = request.execute()
    channel_info = {
        'Channel_Name': response['items'][0]['snippet']['title'],
        'Channel_id': response['items'][0]['id'],
        'Subscription_Count': response['items'][0]['statistics']['subscriberCount'],
        'Channel_Views': response['items'][0]['statistics']['viewCount'],
        'Channel_Description': response['items'][0]['snippet']['description'],
        'Playlist_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }
    return channel_info

#  function to get videos in a playlist
def get_playlist_videos(youtube, playlist_id):
    request = youtube.playlistItems().list(part="snippet",playlistId=playlist_id,maxResults=30)
    response = request.execute()
    videos = []
    for item in response['items']:
        video_id = item['snippet']['resourceId']['videoId']
        video_info = {
            'Video_id':video_id,
            'Channel_id': channel_id,
            'Video_Name':item['snippet']['title'],
            'Video_Description': item['snippet']['description'],
            'PublishedAt': item['snippet']['publishedAt'],
            'Thumbnail': item['snippet']['thumbnails']['high']['url']
        }

        # Get video details
        stats_request =youtube.videos().list(part="statistics,contentDetails", id=video_id)
        stats_response =stats_request.execute()
        stats_data = {
            'View_Count':int(stats_response['items'][0]['statistics']['viewCount']),
            'Comment_Count':int(stats_response['items'][0]['statistics']['commentCount']),
            'Favarite_Count': int(stats_response['items'][0]['statistics']['favoriteCount']),
            'like_Count':int(stats_response['items'][0]['statistics'].get('likeCount',0)),
            'Dislike_Count': int(stats_response['items'][0]['statistics'].get('dislikeCount', 0)),
            'Duration': stats_response['items'][0]['contentDetails']['duration']
        }
        video_info.update(stats_data)

        # Get video comments details
        comments_request = youtube.commentThreads().list(part="snippet",maxResults=10,videoId=video_id)
        comments_response = comments_request.execute()
        comments = []
        for comment_item in comments_response['items']:
            comment = {
                'Comment_id': comment_item['id'],
                'Author': comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'Comment': comment_item['snippet']['topLevelComment']['snippet']['textOriginal'],
                'Like_Count': comment_item['snippet']['topLevelComment']['snippet']['likeCount'],
                'PublishedAt': comment_item['snippet']['topLevelComment']['snippet']['publishedAt']
            }
            comments.append(comment)
                # inserting the comments dict into video info
        video_info['Comments'] = comments

        #append video info
        videos.append(video_info)
    return videos
    
# Get the channel info and playlist videos
channel_info = get_channel_info(youtube, channel_id)
playlist_videos = get_playlist_videos(youtube, playlist_id)

# Insert the channel info and videos into the MongoDB collection
document = {
    'Channel_Info': channel_info,
    'Playlist_Videos': playlist_videos
}
collectionss.insert_one(document)
print("mongodb_Test_Case_3_passed")


mysql_cursor.execute("""CREATE TABLE IF NOT EXISTS channel (
        Channel_id VARCHAR(255) PRIMARY KEY,
        Channel_Name VARCHAR(255),
        Channel_Description VARCHAR(5500),
        Subscription_Count INT,
        Channel_Views BIGINT,
        Playlist_id VARCHAR(255))""")

mysql_db.commit()

# Convert Channel_Info to MySQL 
mysql_data = (
    channel_info['Channel_id'],
    channel_info['Channel_Name'],
    channel_info['Channel_Description'],
    int(channel_info['Subscription_Count']),
    int(channel_info['Channel_Views']),
    channel_info['Playlist_id']
)

# Insert Channel_Info into MySQL
mysql_query = "INSERT INTO channel (Channel_id, Channel_Name, Channel_Description, Subscription_Count, Channel_Views, Playlist_id) VALUES (%s, %s, %s, %s, %s, %s)"
mysql_cursor.execute(mysql_query, mysql_data)
mysql_db.commit()

# Create the 'video' table in MySQL
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS Video (
        Video_id VARCHAR(255) PRIMARY KEY,
        Channel_id VARCHAR(255),
        Video_Name VARCHAR(5500),
        Video_Description VARCHAR(5500),
        PublishedAt VARCHAR(255),
        Thumbnail VARCHAR(255),
        View_Count INT,
        Comment_Count INT,
        Favarite_Count INT,
        like_Count INT,
        Dislike_Count INT,
        Duration VARCHAR(255))""")


# Commit the table creation query
mysql_db.commit()

# Insert Playlist_Videos into MySQL
for video in playlist_videos:
    mysql_data = (
        video['Video_id'],
        video['Channel_id'],
        video['Video_Name'],
        video['Video_Description'],
        video['PublishedAt'],
        video['Thumbnail'],
        video['View_Count'],
        video['Comment_Count'],
        video['Favarite_Count'],
        video['like_Count'],
        video['Dislike_Count'],
        video['Duration']
    )
    mysql_query = "INSERT INTO Video (Video_id, Channel_id, Video_Name, Video_Description, PublishedAt, Thumbnail, View_Count, Comment_Count,Favarite_Count,like_Count,Dislike_Count,Duration) VALUES (%s,%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mysql_cursor.execute(mysql_query, mysql_data)

# Commit the insert queries
mysql_db.commit()

# create the 'comment' table in MySQL
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS comment (
        Comment_id VARCHAR(255) PRIMARY KEY,
        Author VARCHAR(255),
        Like_Count INT,
        PublishedAt VARCHAR(255))""")

# commit the comment creation query
mysql_db.commit()


# insert comments into MySQL
for video in playlist_videos:
    for comment in video['Comments']:
        mysql_data = (
            comment['Comment_id'],
            comment['Author'],
            comment['Like_Count'],
            comment['PublishedAt']
        )
        mysql_query = "INSERT INTO comment (Comment_id, Author, Like_Count, PublishedAt) VALUES (%s, %s, %s, %s)"
        mysql_cursor.execute(mysql_query, mysql_data)

# Commit the insert queries
mysql_db.commit()


print("mysql_text_case_3_passed")



channel_id = 'UC2MYZHG8a56u4REI2RedtRA'
playlist_id = 'UU2MYZHG8a56u4REI2RedtRA'

# Define function to get channel information
def get_channel_info(youtube, channel_id):
    request = youtube.channels().list( part="snippet,contentDetails,statistics",id=channel_id)
    response = request.execute()
    channel_info = {
        'Channel_Name': response['items'][0]['snippet']['title'],
        'Channel_id': response['items'][0]['id'],
        'Subscription_Count': response['items'][0]['statistics']['subscriberCount'],
        'Channel_Views': response['items'][0]['statistics']['viewCount'],
        'Channel_Description': response['items'][0]['snippet']['description'],
        'Playlist_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }
    return channel_info

# Define function to get videos in a playlist
def get_playlist_videos(youtube, playlist_id):
    request = youtube.playlistItems().list(part="snippet",playlistId=playlist_id,maxResults=30)
    response = request.execute()
    videos = []
    for item in response['items']:
        video_id = item['snippet']['resourceId']['videoId']
        video_info = {
            'Video_id':video_id,
            'Channel_id': channel_id,
            'Video_Name':item['snippet']['title'],
            'Video_Description': item['snippet']['description'],
            'PublishedAt': item['snippet']['publishedAt'],
            'Thumbnail': item['snippet']['thumbnails']['high']['url']
        }

        # Get video details
        stats_request =youtube.videos().list(part="statistics,contentDetails", id=video_id)
        stats_response =stats_request.execute()
        stats_data = {
            'View_Count':int(stats_response['items'][0]['statistics']['viewCount']),
            'Comment_Count':int(stats_response['items'][0]['statistics']['commentCount']),
            'Favarite_Count': int(stats_response['items'][0]['statistics']['favoriteCount']),
            'like_Count':int(stats_response['items'][0]['statistics'].get('likeCount',0)),
            'Dislike_Count': int(stats_response['items'][0]['statistics'].get('dislikeCount', 0)),
            'Duration': stats_response['items'][0]['contentDetails']['duration']
        }
        video_info.update(stats_data)

        # Get video comments details
        comments_request = youtube.commentThreads().list(part="snippet",maxResults=10,videoId=video_id)
        comments_response = comments_request.execute()
        comments = []
        for comment_item in comments_response['items']:
            comment = {
                'Comment_id': comment_item['id'],
                'Author': comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'Comment': comment_item['snippet']['topLevelComment']['snippet']['textOriginal'],
                'Like_Count': comment_item['snippet']['topLevelComment']['snippet']['likeCount'],
                'PublishedAt': comment_item['snippet']['topLevelComment']['snippet']['publishedAt']
            }
            comments.append(comment)
                # inserting the comments dict into video info
        video_info['Comments'] = comments

        #append video info
        videos.append(video_info)
    return videos
    
# Get the channel info and playlist videos
channel_info = get_channel_info(youtube, channel_id)
playlist_videos = get_playlist_videos(youtube, playlist_id)

# Insert the channel info and videos into the MongoDB collection
document = {
    'Channel_Info': channel_info,
    'Playlist_Videos': playlist_videos
}
collectionss.insert_one(document)
print("mongodb_Test_Case_4_passed")

mysql_cursor.execute("""CREATE TABLE IF NOT EXISTS channel (
        Channel_id VARCHAR(255) PRIMARY KEY,
        Channel_Name VARCHAR(255),
        Channel_Description VARCHAR(5500),
        Subscription_Count INT,
        Channel_Views BIGINT,
        Playlist_id VARCHAR(255))""")

mysql_db.commit()

# Convert Channel_Info to MySQL 
mysql_data = (
    channel_info['Channel_id'],
    channel_info['Channel_Name'],
    channel_info['Channel_Description'],
    int(channel_info['Subscription_Count']),
    int(channel_info['Channel_Views']),
    channel_info['Playlist_id']
)

# Insert Channel_Info into MySQL
mysql_query = "INSERT INTO channel (Channel_id, Channel_Name, Channel_Description, Subscription_Count, Channel_Views, Playlist_id) VALUES (%s, %s, %s, %s, %s, %s)"
mysql_cursor.execute(mysql_query, mysql_data)
mysql_db.commit()

# Create the 'video' table in MySQL
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS Video (
        Video_id VARCHAR(255) PRIMARY KEY,
        Channel_id VARCHAR(255),
        Video_Name VARCHAR(5500),
        Video_Description VARCHAR(5500),
        PublishedAt VARCHAR(255),
        Thumbnail VARCHAR(255),
        View_Count INT,
        Comment_Count INT,
        Favarite_Count INT,
        like_Count INT,
        Dislike_Count INT,
        Duration VARCHAR(255))""")


# Commit the table creation query
mysql_db.commit()

# Insert Playlist_Videos into MySQL
for video in playlist_videos:
    mysql_data = (
        video['Video_id'],
        video['Channel_id'],
        video['Video_Name'],
        video['Video_Description'],
        video['PublishedAt'],
        video['Thumbnail'],
        video['View_Count'],
        video['Comment_Count'],
        video['Favarite_Count'],
        video['like_Count'],
        video['Dislike_Count'],
        video['Duration']
    )
    mysql_query = "INSERT INTO Video (Video_id, Channel_id, Video_Name, Video_Description, PublishedAt, Thumbnail, View_Count, Comment_Count,Favarite_Count,like_Count,Dislike_Count,Duration) VALUES (%s,%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mysql_cursor.execute(mysql_query, mysql_data)

# Commit the insert queries
mysql_db.commit()

# Create the 'comment' table in MySQL
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS comment (
        Comment_id VARCHAR(255) PRIMARY KEY,
        Author VARCHAR(255),
        Like_Count INT,
        PublishedAt VARCHAR(255))""")

# Commit the comment creation query
mysql_db.commit()


# Insert comments into MySQL
for video in playlist_videos:
    for comment in video['Comments']:
        mysql_data = (
            comment['Comment_id'],
            comment['Author'],
            comment['Like_Count'],
            comment['PublishedAt']
        )
        mysql_query = "INSERT INTO comment (Comment_id, Author, Like_Count, PublishedAt) VALUES (%s, %s, %s, %s)"
        mysql_cursor.execute(mysql_query, mysql_data)

# Commit the insert queries
mysql_db.commit()


print("mysql_text_case_4_passed")


channel_id = 'UCcHQONN73kcbAvdewXOmTIA'
playlist_id = 'UUcHQONN73kcbAvdewXOmTIA'

# Define function to get channel information
def get_channel_info(youtube, channel_id):
    request = youtube.channels().list( part="snippet,contentDetails,statistics",id=channel_id)
    response = request.execute()
    channel_info = {
        'Channel_Name': response['items'][0]['snippet']['title'],
        'Channel_id': response['items'][0]['id'],
        'Subscription_Count': response['items'][0]['statistics']['subscriberCount'],
        'Channel_Views': response['items'][0]['statistics']['viewCount'],
        'Channel_Description': response['items'][0]['snippet']['description'],
        'Playlist_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }
    return channel_info

# Define function to get videos in a playlist
def get_playlist_videos(youtube, playlist_id):
    request = youtube.playlistItems().list(part="snippet",playlistId=playlist_id,maxResults=30)
    response = request.execute()
    videos = []
    for item in response['items']:
        video_id = item['snippet']['resourceId']['videoId']
        video_info = {
            'Video_id':video_id,
            'Channel_id': channel_id,
            'Video_Name':item['snippet']['title'],
            'Video_Description': item['snippet']['description'],
            'PublishedAt': item['snippet']['publishedAt'],
            'Thumbnail': item['snippet']['thumbnails']['high']['url']
        }

        # Get video details
        stats_request =youtube.videos().list(part="statistics,contentDetails", id=video_id)
        stats_response =stats_request.execute()
        stats_data = {
            'View_Count':int(stats_response['items'][0]['statistics']['viewCount']),
            'Comment_Count':int(stats_response['items'][0]['statistics']['commentCount']),
            'Favarite_Count': int(stats_response['items'][0]['statistics']['favoriteCount']),
            'like_Count':int(stats_response['items'][0]['statistics'].get('likeCount',0)),
            'Dislike_Count': int(stats_response['items'][0]['statistics'].get('dislikeCount', 0)),
            'Duration': stats_response['items'][0]['contentDetails']['duration']
        }
        video_info.update(stats_data)

        # Get video comments details
        comments_request = youtube.commentThreads().list(part="snippet",maxResults=10,videoId=video_id)
        comments_response = comments_request.execute()
        comments = []
        for comment_item in comments_response['items']:
            comment = {
                'Comment_id': comment_item['id'],
                'Author': comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'Comment': comment_item['snippet']['topLevelComment']['snippet']['textOriginal'],
                'Like_Count': comment_item['snippet']['topLevelComment']['snippet']['likeCount'],
                'PublishedAt': comment_item['snippet']['topLevelComment']['snippet']['publishedAt']
            }
            comments.append(comment)
                # inserting the comments dict into video info
        video_info['Comments'] = comments

        #append video info
        videos.append(video_info)
    return videos
    
# Get the channel info and playlist videos
channel_info = get_channel_info(youtube, channel_id)
playlist_videos = get_playlist_videos(youtube, playlist_id)

# Insert the channel info and videos into the MongoDB collection
document = {
    'Channel_Info': channel_info,
    'Playlist_Videos': playlist_videos
}
collectionss.insert_one(document)
print("mongodb_Test_Case_5_passed")


mysql_cursor.execute("""CREATE TABLE IF NOT EXISTS channel (
        Channel_id VARCHAR(255) PRIMARY KEY,
        Channel_Name VARCHAR(255),
        Channel_Description VARCHAR(5500),
        Subscription_Count INT,
        Channel_Views BIGINT,
        Playlist_id VARCHAR(255))""")

mysql_db.commit()

# Convert Channel_Info to MySQL 
mysql_data = (
    channel_info['Channel_id'],
    channel_info['Channel_Name'],
    channel_info['Channel_Description'],
    int(channel_info['Subscription_Count']),
    int(channel_info['Channel_Views']),
    channel_info['Playlist_id']
)

# Insert Channel_Info into MySQL
mysql_query = "INSERT INTO channel (Channel_id, Channel_Name, Channel_Description, Subscription_Count, Channel_Views, Playlist_id) VALUES (%s, %s, %s, %s, %s, %s)"
mysql_cursor.execute(mysql_query, mysql_data)
mysql_db.commit()

# Create the 'video' table in MySQL
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS Video (
        Video_id VARCHAR(255) PRIMARY KEY,
        Channel_id VARCHAR(255),
        Video_Name VARCHAR(5500),
        Video_Description VARCHAR(5500),
        PublishedAt VARCHAR(255),
        Thumbnail VARCHAR(255),
        View_Count INT,
        Comment_Count INT,
        Favarite_Count INT,
        like_Count INT,
        Dislike_Count INT,
        Duration VARCHAR(255))""")


# Commit the table creation query
mysql_db.commit()

# Insert Playlist_Videos into MySQL
for video in playlist_videos:
    mysql_data = (
        video['Video_id'],
        video['Channel_id'],
        video['Video_Name'],
        video['Video_Description'],
        video['PublishedAt'],
        video['Thumbnail'],
        video['View_Count'],
        video['Comment_Count'],
        video['Favarite_Count'],
        video['like_Count'],
        video['Dislike_Count'],
        video['Duration']
    )
    mysql_query = "INSERT INTO Video (Video_id, Channel_id, Video_Name, Video_Description, PublishedAt, Thumbnail, View_Count, Comment_Count,Favarite_Count,like_Count,Dislike_Count,Duration) VALUES (%s,%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mysql_cursor.execute(mysql_query, mysql_data)

# Commit the insert queries
mysql_db.commit()

# Create the 'comment' table in MySQL
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS comment (
        Comment_id VARCHAR(255) PRIMARY KEY,
        Author VARCHAR(255),
        Like_Count INT,
        PublishedAt VARCHAR(255))""")

# Commit the comment creation query
mysql_db.commit()


# Insert comments into MySQL
for video in playlist_videos:
    for comment in video['Comments']:
        mysql_data = (
            comment['Comment_id'],
            comment['Author'],
            comment['Like_Count'],
            comment['PublishedAt']
        )
        mysql_query = "INSERT INTO comment (Comment_id, Author, Like_Count, PublishedAt) VALUES (%s, %s, %s, %s)"
        mysql_cursor.execute(mysql_query, mysql_data)

# Commit the insert queries
mysql_db.commit()


print("mysql_text_case_5_passed")


channel_id = 'UCkRFwipiIqBTakN-mkZ-GcQ'
playlist_id = 'UUkRFwipiIqBTakN-mkZ-GcQ'

# Define function to get channel information
def get_channel_info(youtube, channel_id):
    request = youtube.channels().list( part="snippet,contentDetails,statistics",id=channel_id)
    response = request.execute()
    channel_info = {
        'Channel_Name': response['items'][0]['snippet']['title'],
        'Channel_id': response['items'][0]['id'],
        'Subscription_Count': response['items'][0]['statistics']['subscriberCount'],
        'Channel_Views': response['items'][0]['statistics']['viewCount'],
        'Channel_Description': response['items'][0]['snippet']['description'],
        'Playlist_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }
    return channel_info

# Define function to get videos in a playlist
def get_playlist_videos(youtube, playlist_id):
    request = youtube.playlistItems().list(part="snippet",playlistId=playlist_id,maxResults=30)
    response = request.execute()
    videos = []
    for item in response['items']:
        video_id = item['snippet']['resourceId']['videoId']
        video_info = {
            'Video_id':video_id,
            'Channel_id': channel_id,
            'Video_Name':item['snippet']['title'],
            'Video_Description': item['snippet']['description'],
            'PublishedAt': item['snippet']['publishedAt'],
            'Thumbnail': item['snippet']['thumbnails']['high']['url']
        }

        # Get video details
        stats_request =youtube.videos().list(part="statistics,contentDetails", id=video_id)
        stats_response =stats_request.execute()
        stats_data = {
            'View_Count':int(stats_response['items'][0]['statistics']['viewCount']),
            'Comment_Count':int(stats_response['items'][0]['statistics']['commentCount']),
            'Favarite_Count': int(stats_response['items'][0]['statistics']['favoriteCount']),
            'like_Count':int(stats_response['items'][0]['statistics'].get('likeCount',0)),
            'Dislike_Count': int(stats_response['items'][0]['statistics'].get('dislikeCount', 0)),
            'Duration': stats_response['items'][0]['contentDetails']['duration']
        }
        video_info.update(stats_data)

        # Get video comments details
        comments_request = youtube.commentThreads().list(part="snippet",maxResults=10,videoId=video_id)
        comments_response = comments_request.execute()
        comments = []
        for comment_item in comments_response['items']:
            comment = {
                'Comment_id': comment_item['id'],
                'Author': comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'Comment': comment_item['snippet']['topLevelComment']['snippet']['textOriginal'],
                'Like_Count': comment_item['snippet']['topLevelComment']['snippet']['likeCount'],
                'PublishedAt': comment_item['snippet']['topLevelComment']['snippet']['publishedAt']
            }
            comments.append(comment)
                # inserting the comments dict into video info
        video_info['Comments'] = comments

        #append video info
        videos.append(video_info)
    return videos
    
# Get the channel info and playlist videos
channel_info = get_channel_info(youtube, channel_id)
playlist_videos = get_playlist_videos(youtube, playlist_id)

# Insert the channel info and videos into the MongoDB collection
document = {
    'Channel_Info': channel_info,
    'Playlist_Videos': playlist_videos
}
collectionss.insert_one(document)
print("mongodb_Test_Case_6_passed")

mysql_cursor.execute("""CREATE TABLE IF NOT EXISTS channel (
        Channel_id VARCHAR(255) PRIMARY KEY,
        Channel_Name VARCHAR(255),
        Channel_Description VARCHAR(5500),
        Subscription_Count INT,
        Channel_Views BIGINT,
        Playlist_id VARCHAR(255))""")

mysql_db.commit()

# Convert Channel_Info to MySQL 
mysql_data = (
    channel_info['Channel_id'],
    channel_info['Channel_Name'],
    channel_info['Channel_Description'],
    int(channel_info['Subscription_Count']),
    int(channel_info['Channel_Views']),
    channel_info['Playlist_id']
)

# Insert Channel_Info into MySQL
mysql_query = "INSERT INTO channel (Channel_id, Channel_Name, Channel_Description, Subscription_Count, Channel_Views, Playlist_id) VALUES (%s, %s, %s, %s, %s, %s)"
mysql_cursor.execute(mysql_query, mysql_data)
mysql_db.commit()

# Create the 'video' table in MySQL
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS Video (
        Video_id VARCHAR(255) PRIMARY KEY,
        Channel_id VARCHAR(255),
        Video_Name VARCHAR(5500),
        Video_Description VARCHAR(5500),
        PublishedAt VARCHAR(255),
        Thumbnail VARCHAR(255),
        View_Count INT,
        Comment_Count INT,
        Favarite_Count INT,
        like_Count INT,
        Dislike_Count INT,
        Duration VARCHAR(255))""")


# Commit the table creation query
mysql_db.commit()

# Insert Playlist_Videos into MySQL
for video in playlist_videos:
    mysql_data = (
        video['Video_id'],
        video['Channel_id'],
        video['Video_Name'],
        video['Video_Description'],
        video['PublishedAt'],
        video['Thumbnail'],
        video['View_Count'],
        video['Comment_Count'],
        video['Favarite_Count'],
        video['like_Count'],
        video['Dislike_Count'],
        video['Duration']
    )
    mysql_query = "INSERT INTO Video (Video_id, Channel_id, Video_Name, Video_Description, PublishedAt, Thumbnail, View_Count, Comment_Count,Favarite_Count,like_Count,Dislike_Count,Duration) VALUES (%s,%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mysql_cursor.execute(mysql_query, mysql_data)

# Commit the insert queries
mysql_db.commit()

# Create the 'comment' table in MySQL
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS comment (
        Comment_id VARCHAR(255) PRIMARY KEY,
        Author VARCHAR(255),
        Like_Count INT,
        PublishedAt VARCHAR(255))""")

# Commit the comment creation query
mysql_db.commit()


# Insert comments into MySQL
for video in playlist_videos:
    for comment in video['Comments']:
        mysql_data = (
            comment['Comment_id'],
            comment['Author'],
            comment['Like_Count'],
            comment['PublishedAt']
        )
        mysql_query = "INSERT INTO comment (Comment_id, Author, Like_Count, PublishedAt) VALUES (%s, %s, %s, %s)"
        mysql_cursor.execute(mysql_query, mysql_data)

# Commit the insert queries
mysql_db.commit()


print("mysql_text_case_6_passed")


channel_id = 'UCBeNeNLrf5C4u04LYNPAvxA'
playlist_id = 'UUBeNeNLrf5C4u04LYNPAvxA'

# Define function to get channel information
def get_channel_info(youtube, channel_id):
    request = youtube.channels().list( part="snippet,contentDetails,statistics",id=channel_id)
    response = request.execute()
    channel_info = {
        'Channel_Name': response['items'][0]['snippet']['title'],
        'Channel_id': response['items'][0]['id'],
        'Subscription_Count': response['items'][0]['statistics']['subscriberCount'],
        'Channel_Views': response['items'][0]['statistics']['viewCount'],
        'Channel_Description': response['items'][0]['snippet']['description'],
        'Playlist_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }
    return channel_info

# Define function to get videos in a playlist
def get_playlist_videos(youtube, playlist_id):
    request = youtube.playlistItems().list(part="snippet",playlistId=playlist_id,maxResults=30)
    response = request.execute()
    videos = []
    for item in response['items']:
        video_id = item['snippet']['resourceId']['videoId']
        video_info = {
            'Video_id':video_id,
            'Channel_id': channel_id,
            'Video_Name':item['snippet']['title'],
            'Video_Description': item['snippet']['description'],
            'PublishedAt': item['snippet']['publishedAt'],
            'Thumbnail': item['snippet']['thumbnails']['high']['url']
        }

        # Get video details
        stats_request =youtube.videos().list(part="statistics,contentDetails", id=video_id)
        stats_response =stats_request.execute()
        stats_data = {
            'View_Count':int(stats_response['items'][0]['statistics']['viewCount']),
            'Comment_Count':int(stats_response['items'][0]['statistics']['commentCount']),
            'Favarite_Count': int(stats_response['items'][0]['statistics']['favoriteCount']),
            'like_Count':int(stats_response['items'][0]['statistics'].get('likeCount',0)),
            'Dislike_Count': int(stats_response['items'][0]['statistics'].get('dislikeCount', 0)),
            'Duration': stats_response['items'][0]['contentDetails']['duration']
        }
        video_info.update(stats_data)

        # Get video comments details
        comments_request = youtube.commentThreads().list(part="snippet",maxResults=10,videoId=video_id)
        comments_response = comments_request.execute()
        comments = []
        for comment_item in comments_response['items']:
            comment = {
                'Comment_id': comment_item['id'],
                'Author': comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'Comment': comment_item['snippet']['topLevelComment']['snippet']['textOriginal'],
                'Like_Count': comment_item['snippet']['topLevelComment']['snippet']['likeCount'],
                'PublishedAt': comment_item['snippet']['topLevelComment']['snippet']['publishedAt']
            }
            comments.append(comment)
                # inserting the comments dict into video info
        video_info['Comments'] = comments

        #append video info
        videos.append(video_info)
    return videos
    
# Get the channel info and playlist videos
channel_info = get_channel_info(youtube, channel_id)
playlist_videos = get_playlist_videos(youtube, playlist_id)

# Insert the channel info and videos into the MongoDB collection
document = {
    'Channel_Info': channel_info,
    'Playlist_Videos': playlist_videos
}
collectionss.insert_one(document)
print("mongodb_Test_Case_7_passed")


mysql_cursor.execute("""CREATE TABLE IF NOT EXISTS channel (
        Channel_id VARCHAR(255) PRIMARY KEY,
        Channel_Name VARCHAR(255),
        Channel_Description VARCHAR(5500),
        Subscription_Count INT,
        Channel_Views BIGINT,
        Playlist_id VARCHAR(255))""")

mysql_db.commit()

# Convert Channel_Info to MySQL 
mysql_data = (
    channel_info['Channel_id'],
    channel_info['Channel_Name'],
    channel_info['Channel_Description'],
    int(channel_info['Subscription_Count']),
    int(channel_info['Channel_Views']),
    channel_info['Playlist_id']
)

# Insert Channel_Info into MySQL
mysql_query = "INSERT INTO channel (Channel_id, Channel_Name, Channel_Description, Subscription_Count, Channel_Views, Playlist_id) VALUES (%s, %s, %s, %s, %s, %s)"
mysql_cursor.execute(mysql_query, mysql_data)
mysql_db.commit()

# Create the 'video' table in MySQL
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS Video (
        Video_id VARCHAR(255) PRIMARY KEY,
        Channel_id VARCHAR(255),
        Video_Name VARCHAR(5500),
        Video_Description VARCHAR(5500),
        PublishedAt VARCHAR(255),
        Thumbnail VARCHAR(255),
        View_Count INT,
        Comment_Count INT,
        Favarite_Count INT,
        like_Count INT,
        Dislike_Count INT,
        Duration VARCHAR(255))""")


# Commit the table creation query
mysql_db.commit()

# Insert Playlist_Videos into MySQL
for video in playlist_videos:
    mysql_data = (
        video['Video_id'],
        video['Channel_id'],
        video['Video_Name'],
        video['Video_Description'],
        video['PublishedAt'],
        video['Thumbnail'],
        video['View_Count'],
        video['Comment_Count'],
        video['Favarite_Count'],
        video['like_Count'],
        video['Dislike_Count'],
        video['Duration']
    )
    mysql_query = "INSERT INTO Video (Video_id, Channel_id, Video_Name, Video_Description, PublishedAt, Thumbnail, View_Count, Comment_Count,Favarite_Count,like_Count,Dislike_Count,Duration) VALUES (%s,%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mysql_cursor.execute(mysql_query, mysql_data)

# Commit the insert queries
mysql_db.commit()

# Create the 'comment' table in MySQL
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS comment (
        Comment_id VARCHAR(255) PRIMARY KEY,
        Author VARCHAR(255),
        Like_Count INT,
        PublishedAt VARCHAR(255))""")

# Commit the comment creation query
mysql_db.commit()


# Insert comments into MySQL
for video in playlist_videos:
    for comment in video['Comments']:
        mysql_data = (
            comment['Comment_id'],
            comment['Author'],
            comment['Like_Count'],
            comment['PublishedAt']
        )
        mysql_query = "INSERT INTO comment (Comment_id, Author, Like_Count, PublishedAt) VALUES (%s, %s, %s, %s)"
        mysql_cursor.execute(mysql_query, mysql_data)

# Commit the insert queries
mysql_db.commit()


print("mysql_text_case_7_passed")


channel_id = 'UCXhbCCZAG4GlaBLm80ZL-iA'
playlist_id = 'UUXhbCCZAG4GlaBLm80ZL-iA'

# Define function to get channel information
def get_channel_info(youtube, channel_id):
    request = youtube.channels().list( part="snippet,contentDetails,statistics",id=channel_id)
    response = request.execute()
    channel_info = {
        'Channel_Name': response['items'][0]['snippet']['title'],
        'Channel_id': response['items'][0]['id'],
        'Subscription_Count': response['items'][0]['statistics']['subscriberCount'],
        'Channel_Views': response['items'][0]['statistics']['viewCount'],
        'Channel_Description': response['items'][0]['snippet']['description'],
        'Playlist_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }
    return channel_info

# Define function to get videos in a playlist
def get_playlist_videos(youtube, playlist_id):
    request = youtube.playlistItems().list(part="snippet",playlistId=playlist_id,maxResults=30)
    response = request.execute()
    videos = []
    for item in response['items']:
        video_id = item['snippet']['resourceId']['videoId']
        video_info = {
            'Video_id':video_id,
            'Channel_id': channel_id,
            'Video_Name':item['snippet']['title'],
            'Video_Description': item['snippet']['description'],
            'PublishedAt': item['snippet']['publishedAt'],
            'Thumbnail': item['snippet']['thumbnails']['high']['url']
        }

        # Get video details
        stats_request =youtube.videos().list(part="statistics,contentDetails", id=video_id)
        stats_response =stats_request.execute()
        stats_data = {
            'View_Count':int(stats_response['items'][0]['statistics']['viewCount']),
            'Comment_Count':int(stats_response['items'][0]['statistics']['commentCount']),
            'Favarite_Count': int(stats_response['items'][0]['statistics']['favoriteCount']),
            'like_Count':int(stats_response['items'][0]['statistics'].get('likeCount',0)),
            'Dislike_Count': int(stats_response['items'][0]['statistics'].get('dislikeCount', 0)),
            'Duration': stats_response['items'][0]['contentDetails']['duration']
        }
        video_info.update(stats_data)

        # Get video comments details
        comments_request = youtube.commentThreads().list(part="snippet",maxResults=10,videoId=video_id)
        comments_response = comments_request.execute()
        comments = []
        for comment_item in comments_response['items']:
            comment = {
                'Comment_id': comment_item['id'],
                'Author': comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'Comment': comment_item['snippet']['topLevelComment']['snippet']['textOriginal'],
                'Like_Count': comment_item['snippet']['topLevelComment']['snippet']['likeCount'],
                'PublishedAt': comment_item['snippet']['topLevelComment']['snippet']['publishedAt']
            }
            comments.append(comment)
                # inserting the comments dict into video info
        video_info['Comments'] = comments

        #append video info
        videos.append(video_info)
    return videos
    
# Get the channel info and playlist videos
channel_info = get_channel_info(youtube, channel_id)
playlist_videos = get_playlist_videos(youtube, playlist_id)

# Insert the channel info and videos into the MongoDB collection
document = {
    'Channel_Info': channel_info,
    'Playlist_Videos': playlist_videos
}
collectionss.insert_one(document)
print("mongodb_Test_Case_8_passed")


mysql_cursor.execute("""CREATE TABLE IF NOT EXISTS channel (
        Channel_id VARCHAR(255) PRIMARY KEY,
        Channel_Name VARCHAR(255),
        Channel_Description VARCHAR(5500),
        Subscription_Count INT,
        Channel_Views BIGINT,
        Playlist_id VARCHAR(255))""")

mysql_db.commit()

# Convert Channel_Info to MySQL 
mysql_data = (
    channel_info['Channel_id'],
    channel_info['Channel_Name'],
    channel_info['Channel_Description'],
    int(channel_info['Subscription_Count']),
    int(channel_info['Channel_Views']),
    channel_info['Playlist_id']
)

# Insert Channel_Info into MySQL
mysql_query = "INSERT INTO channel (Channel_id, Channel_Name, Channel_Description, Subscription_Count, Channel_Views, Playlist_id) VALUES (%s, %s, %s, %s, %s, %s)"
mysql_cursor.execute(mysql_query, mysql_data)
mysql_db.commit()

# Create the 'video' table in MySQL
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS Video (
        Video_id VARCHAR(255) PRIMARY KEY,
        Channel_id VARCHAR(255),
        Video_Name VARCHAR(5500),
        Video_Description VARCHAR(5500),
        PublishedAt VARCHAR(255),
        Thumbnail VARCHAR(255),
        View_Count INT,
        Comment_Count INT,
        Favarite_Count INT,
        like_Count INT,
        Dislike_Count INT,
        Duration VARCHAR(255))""")


# Commit the table creation query
mysql_db.commit()

# Insert Playlist_Videos into MySQL
for video in playlist_videos:
    mysql_data = (
        video['Video_id'],
        video['Channel_id'],
        video['Video_Name'],
        video['Video_Description'],
        video['PublishedAt'],
        video['Thumbnail'],
        video['View_Count'],
        video['Comment_Count'],
        video['Favarite_Count'],
        video['like_Count'],
        video['Dislike_Count'],
        video['Duration']
    )
    mysql_query = "INSERT INTO Video (Video_id, Channel_id, Video_Name, Video_Description, PublishedAt, Thumbnail, View_Count, Comment_Count,Favarite_Count,like_Count,Dislike_Count,Duration) VALUES (%s,%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mysql_cursor.execute(mysql_query, mysql_data)

# Commit the insert queries
mysql_db.commit()

# Create the 'comment' table in MySQL
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS comment (
        Comment_id VARCHAR(255) PRIMARY KEY,
        Author VARCHAR(255),
        Like_Count INT,
        PublishedAt VARCHAR(255))""")

# Commit the comment creation query
mysql_db.commit()


# Insert comments into MySQL
for video in playlist_videos:
    for comment in video['Comments']:
        mysql_data = (
            comment['Comment_id'],
            comment['Author'],
            comment['Like_Count'],
            comment['PublishedAt']
        )
        mysql_query = "INSERT INTO comment (Comment_id, Author, Like_Count, PublishedAt) VALUES (%s, %s, %s, %s)"
        mysql_cursor.execute(mysql_query, mysql_data)

# Commit the insert queries
mysql_db.commit()


print("mysql_text_case_8_passed")


channel_id = 'UCzB2vC0kH-PPbrGmqeII9hQ'
playlist_id = 'UUzB2vC0kH-PPbrGmqeII9hQ'


# Define function to get channel information
def get_channel_info(youtube, channel_id):
    request = youtube.channels().list( part="snippet,contentDetails,statistics",id=channel_id)
    response = request.execute()
    channel_info = {
        'Channel_Name': response['items'][0]['snippet']['title'],
        'Channel_id': response['items'][0]['id'],
        'Subscription_Count': response['items'][0]['statistics']['subscriberCount'],
        'Channel_Views': response['items'][0]['statistics']['viewCount'],
        'Channel_Description': response['items'][0]['snippet']['description'],
        'Playlist_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }
    return channel_info

# Define function to get videos in a playlist
def get_playlist_videos(youtube, playlist_id):
    request = youtube.playlistItems().list(part="snippet",playlistId=playlist_id,maxResults=30)
    response = request.execute()
    videos = []
    for item in response['items']:
        video_id = item['snippet']['resourceId']['videoId']
        video_info = {
            'Video_id':video_id,
            'Channel_id': channel_id,
            'Video_Name':item['snippet']['title'],
            'Video_Description': item['snippet']['description'],
            'PublishedAt': item['snippet']['publishedAt'],
            'Thumbnail': item['snippet']['thumbnails']['high']['url']
        }

        # Get video details
        stats_request =youtube.videos().list(part="statistics,contentDetails", id=video_id)
        stats_response =stats_request.execute()
        stats_data = {
            'View_Count':int(stats_response['items'][0]['statistics']['viewCount']),
            'Comment_Count':int(stats_response['items'][0]['statistics']['commentCount']),
            'Favarite_Count': int(stats_response['items'][0]['statistics']['favoriteCount']),
            'like_Count':int(stats_response['items'][0]['statistics'].get('likeCount',0)),
            'Dislike_Count': int(stats_response['items'][0]['statistics'].get('dislikeCount', 0)),
            'Duration': stats_response['items'][0]['contentDetails']['duration']
        }
        video_info.update(stats_data)

        # Get video comments details
        comments_request = youtube.commentThreads().list(part="snippet",maxResults=10,videoId=video_id)
        comments_response = comments_request.execute()
        comments = []
        for comment_item in comments_response['items']:
            comment = {
                'Comment_id': comment_item['id'],
                'Author': comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'Comment': comment_item['snippet']['topLevelComment']['snippet']['textOriginal'],
                'Like_Count': comment_item['snippet']['topLevelComment']['snippet']['likeCount'],
                'PublishedAt': comment_item['snippet']['topLevelComment']['snippet']['publishedAt']
            }
            comments.append(comment)
                # inserting the comments dict into video info
        video_info['Comments'] = comments

        #append video info
        videos.append(video_info)
    return videos
    
# Get the channel info and playlist videos
channel_info = get_channel_info(youtube, channel_id)
playlist_videos = get_playlist_videos(youtube, playlist_id)

# Insert the channel info and videos into the MongoDB collection
document = {
    'Channel_Info': channel_info,
    'Playlist_Videos': playlist_videos
}
collectionss.insert_one(document)
print("mongodb_Test_Case_9_passed")


mysql_cursor.execute("""CREATE TABLE IF NOT EXISTS channel (
        Channel_id VARCHAR(255) PRIMARY KEY,
        Channel_Name VARCHAR(255),
        Channel_Description VARCHAR(5500),
        Subscription_Count INT,
        Channel_Views BIGINT,
        Playlist_id VARCHAR(255))""")

mysql_db.commit()

# Convert Channel_Info to MySQL 
mysql_data = (
    channel_info['Channel_id'],
    channel_info['Channel_Name'],
    channel_info['Channel_Description'],
    int(channel_info['Subscription_Count']),
    int(channel_info['Channel_Views']),
    channel_info['Playlist_id']
)

# Insert Channel_Info into MySQL
mysql_query = "INSERT INTO channel (Channel_id, Channel_Name, Channel_Description, Subscription_Count, Channel_Views, Playlist_id) VALUES (%s, %s, %s, %s, %s, %s)"
mysql_cursor.execute(mysql_query, mysql_data)
mysql_db.commit()

# Create the 'video' table in MySQL
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS Video (
        Video_id VARCHAR(255) PRIMARY KEY,
        Channel_id VARCHAR(255),
        Video_Name VARCHAR(5500),
        Video_Description VARCHAR(5500),
        PublishedAt VARCHAR(255),
        Thumbnail VARCHAR(255),
        View_Count INT,
        Comment_Count INT,
        Favarite_Count INT,
        like_Count INT,
        Dislike_Count INT,
        Duration VARCHAR(255))""")


# Commit the table creation query
mysql_db.commit()

# Insert Playlist_Videos into MySQL
for video in playlist_videos:
    mysql_data = (
        video['Video_id'],
        video['Channel_id'],
        video['Video_Name'],
        video['Video_Description'],
        video['PublishedAt'],
        video['Thumbnail'],
        video['View_Count'],
        video['Comment_Count'],
        video['Favarite_Count'],
        video['like_Count'],
        video['Dislike_Count'],
        video['Duration']
    )
    mysql_query = "INSERT INTO Video (Video_id, Channel_id, Video_Name, Video_Description, PublishedAt, Thumbnail, View_Count, Comment_Count,Favarite_Count,like_Count,Dislike_Count,Duration) VALUES (%s,%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mysql_cursor.execute(mysql_query, mysql_data)

# Commit the insert queries
mysql_db.commit()

# Create the 'comment' table in MySQL
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS comment (
        Comment_id VARCHAR(255) PRIMARY KEY,
        Author VARCHAR(255),
        Like_Count INT,
        PublishedAt VARCHAR(255))""")

# Commit the comment creation query
mysql_db.commit()


# Insert comments into MySQL
for video in playlist_videos:
    for comment in video['Comments']:
        mysql_data = (
            comment['Comment_id'],
            comment['Author'],
            comment['Like_Count'],
            comment['PublishedAt']
        )
        mysql_query = "INSERT INTO comment (Comment_id, Author, Like_Count, PublishedAt) VALUES (%s, %s, %s, %s)"
        mysql_cursor.execute(mysql_query, mysql_data)

# Commit the insert queries
mysql_db.commit()


print("mysql_text_case_9_passed")



channel_id = 'UC5UL61DkApcfM4msKTDmBTQ'
playlist_id = 'UU5UL61DkApcfM4msKTDmBTQ'

# Define function to get channel information
def get_channel_info(youtube, channel_id):
    request = youtube.channels().list( part="snippet,contentDetails,statistics",id=channel_id)
    response = request.execute()
    channel_info = {
        'Channel_Name': response['items'][0]['snippet']['title'],
        'Channel_id': response['items'][0]['id'],
        'Subscription_Count': response['items'][0]['statistics']['subscriberCount'],
        'Channel_Views': response['items'][0]['statistics']['viewCount'],
        'Channel_Description': response['items'][0]['snippet']['description'],
        'Playlist_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }
    return channel_info

# Define function to get videos in a playlist
def get_playlist_videos(youtube, playlist_id):
    request = youtube.playlistItems().list(part="snippet",playlistId=playlist_id,maxResults=30)
    response = request.execute()
    videos = []
    for item in response['items']:
        video_id = item['snippet']['resourceId']['videoId']
        video_info = {
            'Video_id':video_id,
            'Channel_id': channel_id,
            'Video_Name':item['snippet']['title'],
            'Video_Description': item['snippet']['description'],
            'PublishedAt': item['snippet']['publishedAt'],
            'Thumbnail': item['snippet']['thumbnails']['high']['url']
        }

        # Get video details
        stats_request =youtube.videos().list(part="statistics,contentDetails", id=video_id)
        stats_response =stats_request.execute()
        stats_data = {
            'View_Count':int(stats_response['items'][0]['statistics']['viewCount']),
            'Comment_Count':int(stats_response['items'][0]['statistics']['commentCount']),
            'Favarite_Count': int(stats_response['items'][0]['statistics']['favoriteCount']),
            'like_Count':int(stats_response['items'][0]['statistics'].get('likeCount',0)),
            'Dislike_Count': int(stats_response['items'][0]['statistics'].get('dislikeCount', 0)),
            'Duration': stats_response['items'][0]['contentDetails']['duration']
        }
        video_info.update(stats_data)

        # Get video comments details
        comments_request = youtube.commentThreads().list(part="snippet",maxResults=10,videoId=video_id)
        comments_response = comments_request.execute()
        comments = []
        for comment_item in comments_response['items']:
            comment = {
                'Comment_id': comment_item['id'],
                'Author': comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'Comment': comment_item['snippet']['topLevelComment']['snippet']['textOriginal'],
                'Like_Count': comment_item['snippet']['topLevelComment']['snippet']['likeCount'],
                'PublishedAt': comment_item['snippet']['topLevelComment']['snippet']['publishedAt']
            }
            comments.append(comment)
                # inserting the comments dict into video info
        video_info['Comments'] = comments

        #append video info
        videos.append(video_info)
    return videos
    
# Get the channel info and playlist videos
channel_info = get_channel_info(youtube, channel_id)
playlist_videos = get_playlist_videos(youtube, playlist_id)

# Insert the channel info and videos into the MongoDB collection
document = {
    'Channel_Info': channel_info,
    'Playlist_Videos': playlist_videos
}
collectionss.insert_one(document)
print("mongodb_Test_Case_10_passed")

mysql_cursor.execute("""CREATE TABLE IF NOT EXISTS channel (
        Channel_id VARCHAR(255) PRIMARY KEY,
        Channel_Name VARCHAR(255),
        Channel_Description VARCHAR(5500),
        Subscription_Count INT,
        Channel_Views BIGINT,
        Playlist_id VARCHAR(255))""")

mysql_db.commit()

# Convert Channel_Info to MySQL 
mysql_data = (
    channel_info['Channel_id'],
    channel_info['Channel_Name'],
    channel_info['Channel_Description'],
    int(channel_info['Subscription_Count']),
    int(channel_info['Channel_Views']),
    channel_info['Playlist_id']
)

# Insert Channel_Info into MySQL
mysql_query = "INSERT INTO channel (Channel_id, Channel_Name, Channel_Description, Subscription_Count, Channel_Views, Playlist_id) VALUES (%s, %s, %s, %s, %s, %s)"
mysql_cursor.execute(mysql_query, mysql_data)
mysql_db.commit()

# Create the 'video' table in MySQL
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS Video (
        Video_id VARCHAR(255) PRIMARY KEY,
        Channel_id VARCHAR(255),
        Video_Name VARCHAR(5500),
        Video_Description VARCHAR(5500),
        PublishedAt VARCHAR(255),
        Thumbnail VARCHAR(255),
        View_Count INT,
        Comment_Count INT,
        Favarite_Count INT,
        like_Count INT,
        Dislike_Count INT,
        Duration VARCHAR(255))""")


# Commit the table creation query
mysql_db.commit()

# Insert Playlist_Videos into MySQL
for video in playlist_videos:
    mysql_data = (
        video['Video_id'],
        video['Channel_id'],
        video['Video_Name'],
        video['Video_Description'],
        video['PublishedAt'],
        video['Thumbnail'],
        video['View_Count'],
        video['Comment_Count'],
        video['Favarite_Count'],
        video['like_Count'],
        video['Dislike_Count'],
        video['Duration']
    )
    mysql_query = "INSERT INTO Video (Video_id, Channel_id, Video_Name, Video_Description, PublishedAt, Thumbnail, View_Count, Comment_Count,Favarite_Count,like_Count,Dislike_Count,Duration) VALUES (%s,%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mysql_cursor.execute(mysql_query, mysql_data)

# Commit the insert queries
mysql_db.commit()

# Create the 'comment' table in MySQL
mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS comment (
        Comment_id VARCHAR(255) PRIMARY KEY,
        Author VARCHAR(255),
        Like_Count INT,
        PublishedAt VARCHAR(255))""")

# Commit the comment creation query
mysql_db.commit()


# Insert comments into MySQL
for video in playlist_videos:
    for comment in video['Comments']:
        mysql_data = (
            comment['Comment_id'],
            comment['Author'],
            comment['Like_Count'],
            comment['PublishedAt']
        )
        mysql_query = "INSERT INTO comment (Comment_id, Author, Like_Count, PublishedAt) VALUES (%s, %s, %s, %s)"
        mysql_cursor.execute(mysql_query, mysql_data)

# Commit the insert queries
mysql_db.commit()


print("mysql_text_case_10_passed")


# converting mysql data into streamlit ----------------- in test.py file checkit