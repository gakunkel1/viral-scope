from youtube.auth import Config
from googleapiclient.discovery import build
from datetime import datetime, UTC
from pprint import pprint
import json

from models.youtube import Channel

def get_uploads_playlist_by_channel_id(
    id: str = "UC_x5XG1OV2P6uZZ5FSM9Ttw"
) -> str:
    cfg = Config()
    youtube = build('youtube', 'v3', developerKey=cfg.youtube_api_key)
    
    # list channels
    request = youtube.channels().list(
        part="snipper,contentDetails,statistics",
        id=id
    )
    
    # execute request
    response = request.execute()
    pprint(response)
    
    # get uploads from response
    uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    return uploads_playlist_id

def get_uploads_playlist_by_handle(
    handle: str
) -> str:
    cfg = Config()
    youtube = build('youtube', 'v3', developerKey=cfg.youtube_api_key)
    
    # list channels
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        forHandle=handle
    )
    
    # execute request
    response = request.execute()

    # save response to file as JSON
    with open('channel.json', 'w') as f:
        json.dump(response, f)
    
    # get uploads from response
    uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    return uploads_playlist_id

def get_channel_details_by_handle(
    handle: str
) -> Channel:
    cfg = Config()
    youtube = build('youtube', 'v3', developerKey=cfg.youtube_api_key)
    
    # list channels
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        forHandle=handle
    )
    
    # execute request
    response = request.execute()
    
    # save ingestion timestamp
    ingested_at = datetime.now(UTC)
    
    pprint(response)

    # parse response
    id = response['items'][0]['id']
    title = response['items'][0]['snippet']['title']
    description = response['items'][0]['snippet']['description']
    custom_url = response['items'][0]['snippet']['customUrl']
    published_at = response['items'][0]['snippet']['publishedAt']
    thumbnail_url = response['items'][0]['snippet']['thumbnails']['default']['url']
    view_count = response['items'][0]['statistics']['viewCount']
    subscriber_count = response['items'][0]['statistics']['subscriberCount']
    hidden_subscriber_count = response['items'][0]['statistics']['hiddenSubscriberCount']
    video_count = response['items'][0]['statistics']['videoCount']
    uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    return Channel(
        id=id,
        handle=handle,
        title=title,
        description=description,
        custom_url=custom_url,
        published_at=published_at,
        thumbnail_url=thumbnail_url,
        view_count=view_count,
        subscriber_count=subscriber_count,
        hidden_subscriber_count=hidden_subscriber_count,
        video_count=video_count,
        uploads_playlist_id=uploads_playlist_id,
        ingested_at=ingested_at
    )