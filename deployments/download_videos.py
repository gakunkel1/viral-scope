from prefect import flow, get_run_logger
from prefect.concurrency.sync import concurrency
from pprint import pprint
from datetime import datetime, UTC
import os

from youtube.channels import get_channel_details_by_handle
from youtube.videos import get_videos_by_playlist_id
from models.youtube import Channel
from ingestion.downloader import VideoDownloader, VideoMetadata

from ingestion.tasks import (
    get_channels_to_process, save_channels_to_db,
    get_upload_playlists_to_process, save_video_metadata_to_db,
    video_previously_processed, update_channel_videos_last_processed,
    create_db_tables
)

MAX_VIDEO_DURATION = int(os.getenv("MAX_VIDEO_DURATION", "120"))

@flow(log_prints=True, timeout_seconds=600)
def download_videos():
    """Ingest channel details into PostgreSQL."""
    with concurrency("video-ingestion", occupy=1):
        logger = get_run_logger()
        logger.info("Starting ingestion pipeline...")
        
        # Create 'youtube' schema tables if they don't yet exist
        create_db_tables()
        
        # Get the list of channel handles to process
        channel_handles = get_channels_to_process()
        logger.info(f'channels: {channel_handles}')
        
        # Get the channel details for each channel
        channel_detail_list: list[Channel] = []
        for handle in channel_handles:
            channel_detail_list.append(get_channel_details_by_handle(handle))
        
        # Save the channel details to the database
        save_channels_to_db(channel_detail_list)
        
        # Fetch each channel's upload playlist ID
        upload_playlists = get_upload_playlists_to_process(update_threshold_days=7)
        logger.info(upload_playlists)
        
        # Download videos from each uploads playlist
        dl = VideoDownloader(max_duration=MAX_VIDEO_DURATION)
        
        for up in upload_playlists:
            # Get list of videos by the default upload playlist ID
            videos = get_videos_by_playlist_id(up.uploads_playlist_id, max_results=5)
            logger.info(videos)
            
            processed_video_metadata: list[VideoMetadata] = []
            
            for v in videos:
                
                # Check if we have already fully processed the video
                if video_previously_processed(webpage_url=v.url):
                    video_metadata = dl.extract_metadata_only(v.url)
                
                # Download video if it hasn't been processed (and we don't have it in storage)
                else:
                    video_metadata = dl.download(v.url)
                
                if video_metadata is None:
                    continue
                
                video_metadata.ingested_at = datetime.now(UTC)
                processed_video_metadata.append(video_metadata)
                
            # Load metadata from videos into Postgres
            save_video_metadata_to_db(processed_video_metadata)
            logger.info(processed_video_metadata)
            
            # Mark channel with videos_last_processed date
            update_channel_videos_last_processed(up.channel_id)
            
        logger.info('Ingestion pipeline complete.')
        
