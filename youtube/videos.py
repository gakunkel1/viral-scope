from youtube.auth import Config
from models.youtube import Video
from googleapiclient.discovery import build
from pprint import pprint

def get_videos_by_playlist_id(
    id: str = "UU_x5XG1OV2P6uZZ5FSM9Ttw",
    max_results: int = 50
) -> list[Video]:
    cfg = Config()
    youtube = build('youtube', 'v3', developerKey=cfg.youtube_api_key)
    
    # List channels
    request = youtube.playlistItems().list(
        part="id,snippet,status,contentDetails",
        playlistId=id,
        maxResults=max_results
    )
    
    videos: list[Video] = []
    while request:
        # Execute the request
        response = request.execute()
        
        for item in response['items']:
            video_id = item['contentDetails']['videoId']
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            title = item['snippet']['title']
            videos.append(Video(title=title, url=video_url))
            
        request = youtube.playlistItems().list_next(request, response)
        
        if len(videos) >= max_results:
            break
        
    return videos