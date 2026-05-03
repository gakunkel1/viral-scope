import csv
import logging
import traceback
from pprint import pprint
from datetime import datetime, UTC
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor

from youtube.channels import get_uploads_playlist_by_handle, get_channel_details_by_handle
from youtube.videos import get_videos_by_playlist_id
from models.youtube import Channel, UploadPlaylist, Video
from db.pg_connect import connect_to_db
from ingestion.downloader import VideoDownloader, VideoMetadata

def run_pipeline():
    """Ingest channel details into PostgreSQL."""
    # Get the list of channel handles to process
    channel_handles = get_channels_to_process()
    pprint(f'channels: {channel_handles}')
    
    # Get the channel details for each channel
    channel_detail_list: list[Channel] = []
    for handle in channel_handles:
        channel_detail_list.append(get_channel_details_by_handle(handle))
        
    # Load the channel details to Postgres
    save_channels_to_db(channel_detail_list)
    
    # Fetch each channel's videos using the upload_playlist_id
    upload_playlists = get_upload_playlists()
    pprint(upload_playlists)
    
    # Download videos from each uploads playlist
    for up in upload_playlists:
        # Get list of videos by the default upload playlist ID
        videos = get_videos_by_playlist_id(up.uploads_playlist_id, max_results=2)
        pprint(videos)
        
        # Download the videos
        processed_video_metadata: list[VideoMetadata] = []
        for v in videos:
            dl = VideoDownloader()
            storage_uri, video_metadata = dl.download(v.url)
            video_metadata.ingested_at = datetime.now(UTC)
            processed_video_metadata.append(video_metadata)
            
        # Load video metadata to Postgres
        save_video_metadata_to_db(processed_video_metadata)
        
def get_channels_to_process() -> list[str]:
    """Get channels to process from channels.csv."""
    channels_file = 'channels.csv'
    with open(channels_file, mode='r') as file:
        reader = csv.DictReader(file)
        return [row['channel_handle'] for row in reader]   
    
def create_channel_table(conn):
    print('Ensuring youtube.channels table exists...')
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS youtube;
            CREATE TABLE IF NOT EXISTS youtube.channels (
                id TEXT PRIMARY KEY,
                handle TEXT,
                title TEXT,
                description TEXT,
                custom_url TEXT,
                published_at  TIMESTAMPTZ,
                thumbnail_url TEXT,
                view_count INTEGER,
                subscriber_count INTEGER,
                hidden_subscriber_count BOOLEAN,
                video_count INTEGER,
                uploads_playlist_id TEXT,
                ingested_at TIMESTAMPTZ DEFAULT NOW(),
                videos_last_processed TIMESTAMPTZ
            )
        """)
        conn.commit()
    except psycopg2.Error as e:
        print(f'Failed to create youtube.channels table: {str(e)}')
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
            
def create_video_metadata_table(conn):
    print('Ensuring youtube.video_metadata table exists...')
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS youtube;
            CREATE TABLE IF NOT EXISTS youtube.video_metadata (
                video_id TEXT PRIMARY KEY,
                title TEXT,
                channel TEXT,
                channel_id TEXT,
                duration_seconds INTEGER,
                view_count INTEGER,
                like_count INTEGER,
                comment_count INTEGER,
                upload_date DATE,
                description TEXT,
                tags TEXT[],
                webpage_url TEXT,
                ext TEXT,
                storage_uri TEXT,
                ingested_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        conn.commit()
    except psycopg2.Error as e:
        print(f'Failed to create youtube.video_metadata table: {str(e)}')
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
     
def upsert_video_metadata(conn, video_metadata: list[VideoMetadata]):
    """
    Insert video_metadata records into Postgres.
    """
    print("Inserting data into youtube.video_metadata...")
    
    schema_name, table_name = "youtube", "video_metadata"
    unique_id_fields = ["video_id"]
    
    # or use list(VideoMetadata.model_fields.keys())
    fields = [
        "video_id", "title", "channel", "channel_id", "duration_seconds", "view_count",
        "like_count", "comment_count", "upload_date", "description",
        "tags", "webpage_url", "ext", "storage_uri", "ingested_at"
    ]
    
    # Deduplicate by id and warn on collisions
    seen: dict[str, Channel] = {}
    for v in video_metadata:
        if v.video_id in seen:
            print(f"Warning: duplicate video_id {v.video_id} in source data.")
        else:
            seen[v.video_id] = v
            
    values = [
        tuple(v.model_dump()[f] for f in fields)
        for v in seen.values()
    ]
    
    columns = ", ".join(fields)
    updates = ", ".join(f"{f} = EXCLUDED.{f}" for f in fields if f != "id")
    on_conflict_fields = ", ".join(unique_id_fields)
    try:
        with conn.cursor() as cursor:
            execute_values(
                cursor,
                f"""
                INSERT INTO {schema_name}.{table_name} ({columns})
                VALUES %s
                ON CONFLICT ({on_conflict_fields}) DO UPDATE SET {updates}
                """,
                values,
            )
        conn.commit()
        print(f"Upserted {len(values)} channels into {schema_name}.{table_name}")
    except psycopg2.Error as e:
        print(f"Failed to insert records into {schema_name}.{table_name}: {str(e)}")
        raise
            
def upsert_channels(conn, channels: list[Channel]):
    """
    Insert channel records into Postgres.
    """
    print("Inserting data into youtube.channels...")
    
    schema_name, table_name = "youtube", "channels"
    unique_id_fields = ["id"]
    
    # or use list(Channel.model_fields.keys())
    fields = [
        "id", "handle", "title", "description", "custom_url", "published_at",
        "thumbnail_url", "view_count", "subscriber_count", "hidden_subscriber_count",
        "video_count", "uploads_playlist_id", "ingested_at",
    ]
    
    # Deduplicate by id and warn on collisions
    seen: dict[str, Channel] = {}
    for c in channels:
        if c.id in seen:
            print(f"Warning: duplicate channel ID {c.id} in source data.")
        else:
            seen[c.id] = c
            
    values = [
        tuple(c.model_dump()[f] for f in fields)
        for c in seen.values()
    ]
    
    columns = ", ".join(fields)
    updates = ", ".join(f"{f} = EXCLUDED.{f}" for f in fields if f != "id")
    on_conflict_fields = ", ".join(unique_id_fields)
    try:
        with conn.cursor() as cursor:
            execute_values(
                cursor,
                f"""
                INSERT INTO {schema_name}.{table_name} ({columns})
                VALUES %s
                ON CONFLICT ({on_conflict_fields}) DO UPDATE SET {updates}
                """,
                values,
            )
        conn.commit()
        print(f"Upserted {len(values)} channels into {schema_name}.{table_name}")
    except psycopg2.Error as e:
        print(f"Failed to insert records into {schema_name}.{table_name}: {str(e)}")
        raise
     
def save_channels_to_db(channels: list[Channel]):
    """Load channel details to PostgreSQL."""
    conn = connect_to_db()
    create_channel_table(conn)
    upsert_channels(conn, channels)
    if 'conn' in locals():
        conn.close()
        print('DB connection closed')
        
def save_video_metadata_to_db(video_metadata: list[VideoMetadata]):
    """Load video metadata to PostgreSQL."""
    conn = connect_to_db()
    create_video_metadata_table(conn)
    upsert_video_metadata(conn, video_metadata)
    if 'conn' in locals():
        conn.close()
        print('DB connection closed')
    
def get_upload_playlists(update_threshold_days: int = 7) -> list[UploadPlaylist]:
    """Query upload playlists for channels that require processing/reprocessing."""
    upload_playlists: list[UploadPlaylist] = []
    try:
        conn = connect_to_db()
        with conn.cursor('get_upload_playlists', cursor_factory=RealDictCursor) as read_cur:
            read_cur.execute(f"""
                SELECT
                    id as channel_id,
                    handle as channel_handle,
                    uploads_playlist_id
                FROM youtube.channels
                WHERE videos_last_processed IS NULL
                OR videos_last_processed < NOW() - INTERVAL '{str(update_threshold_days)} days';             
            """)
            upload_playlists.extend([
                UploadPlaylist(
                    channel_id=row['channel_id'],
                    channel_handle=row['channel_handle'],
                    uploads_playlist_id=row['uploads_playlist_id']
                )
                for row in read_cur
            ])
        return upload_playlists
    except psycopg2.Error as e:
        print(f"Error retrieving upload playlist IDs: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            print('DB connection closed')
            
    
if __name__ == '__main__':
    try:
        run_pipeline()
    except Exception as e:
        logging.error(traceback.format_exc())