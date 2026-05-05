from prefect import flow, get_run_logger
from prefect.concurrency.sync import concurrency
from pprint import pprint
from datetime import datetime, UTC

from youtube.channels import get_channel_details_by_handle
from youtube.videos import get_videos_by_playlist_id
from models.youtube import Channel
from ingestion.downloader import VideoDownloader, VideoMetadata

from ingestion.tasks import get_channels_to_process, save_channels_to_db, get_upload_playlists, save_video_metadata_to_db

@flow(log_prints=True, timeout_seconds=600)
def download_videos():
    """Ingest channel details into PostgreSQL."""
    with concurrency("video-ingestion", occupy=1):
        logger = get_run_logger()
        logger.info("Starting ingestion pipeline...")
        
        # Get the list of channel handles to process
        channel_handles = get_channels_to_process()
        logger.info(f'channels: {channel_handles}')
        
        # Get the channel details for each channel
        channel_detail_list: list[Channel] = []
        for handle in channel_handles:
            channel_detail_list.append(get_channel_details_by_handle(handle))
        
        # Save the channel details to the database
        save_channels_to_db(channel_detail_list)
        
        # Fetch each channel's videos using the upload_playlist_id
        upload_playlists = get_upload_playlists()
        logger.info(upload_playlists)
        
        # Download videos from each uploads playlist
        dl = VideoDownloader()
        for up in upload_playlists:
            # Get list of videos by the default upload playlist ID
            videos = get_videos_by_playlist_id(up.uploads_playlist_id, max_results=5)
            logger.info(videos)
            
            # Download the videos
            processed_video_metadata: list[VideoMetadata] = []
            for v in videos:
                # Download the video
                storage_uri, video_metadata = dl.download(v.url)
                video_metadata.ingested_at = datetime.now(UTC)
                processed_video_metadata.append(video_metadata)
                
            # Load metadata from videos into Postgres
            save_video_metadata_to_db(processed_video_metadata)
            logger.info(processed_video_metadata)
            
        logger.info('Ingestion pipeline complete.')
        
