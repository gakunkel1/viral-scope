from prefect import flow, task, get_run_logger
import csv
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor

from models.youtube import Channel, UploadPlaylist
from db.pg_connect import connect_to_db
from ingestion.downloader import VideoMetadata

@task
def get_channels_to_process() -> list[str]:
    """Get channels to process from channels.csv."""
    channels_file = 'channels.csv'
    with open(channels_file, mode='r') as file:
        reader = csv.DictReader(file)
        return [row['channel_handle'] for row in reader]   
    
@task
def create_channel_table():
    logger = get_run_logger()
    logger.info('Ensuring youtube.channels table exists...')
    conn = connect_to_db()
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
                view_count BIGINT,
                subscriber_count BIGINT,
                hidden_subscriber_count BOOLEAN,
                video_count INTEGER,
                uploads_playlist_id TEXT,
                ingested_at TIMESTAMPTZ DEFAULT NOW(),
                videos_last_processed TIMESTAMPTZ
            )
        """)
        conn.commit()
    except psycopg2.Error as e:
        logger.error(f'Failed to create youtube.channels table: {str(e)}')
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            logger.info('DB connection closed')
            
@task
def create_video_metadata_table():
    logger = get_run_logger()
    logger.info('Ensuring youtube.video_metadata table exists...')
    conn = connect_to_db()
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
                ingested_at TIMESTAMPTZ DEFAULT NOW(),
                transcript TEXT
            )
        """)
        conn.commit()
    except psycopg2.Error as e:
        logger.error(f'Failed to create youtube.video_metadata table: {str(e)}')
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            logger.info('DB connection closed')
     
@task
def upsert_video_metadata(video_metadata: list[VideoMetadata]):
    """
    Insert video_metadata records into Postgres.
    """
    logger = get_run_logger()
    logger.info("Inserting data into youtube.video_metadata...")
    
    conn = connect_to_db()
    
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
            logger.warning(f"Warning: duplicate video_id {v.video_id} in source data.")
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
        logger.info(f"Upserted {len(values)} channels into {schema_name}.{table_name}")
    except psycopg2.Error as e:
        logger.error(f"Failed to insert records into {schema_name}.{table_name}: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info('DB connection closed')
            
@task
def upsert_channels(channels: list[Channel]):
    """
    Insert channel records into Postgres.
    """
    logger = get_run_logger()
    logger.info("Inserting data into youtube.channels...")
    
    conn = connect_to_db()
    
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
            logger.warning(f"Warning: duplicate channel ID {c.id} in source data.")
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
        sql = f"""
                INSERT INTO {schema_name}.{table_name} ({columns})
                VALUES %s
                ON CONFLICT ({on_conflict_fields}) DO UPDATE SET {updates}
                """
        logger.info(sql)
        with conn.cursor() as cursor:
            execute_values(
                cursor,
                sql,
                values,
            )
        conn.commit()
        logger.info(f"Upserted {len(values)} channels into {schema_name}.{table_name}")
    except psycopg2.Error as e:
        logger.error(f"Failed to insert records into {schema_name}.{table_name}: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info('DB connection closed')
     
@flow
def save_channels_to_db(channels: list[Channel]):
    """Load channel details to PostgreSQL."""
    logger = get_run_logger()
    logger.info('Saving channels to database...')
    conn = connect_to_db()
    create_channel_table()
    upsert_channels(channels)
    if 'conn' in locals():
        conn.close()
        logger.info('DB connection closed')
        
@flow
def save_video_metadata_to_db(video_metadata: list[VideoMetadata]):
    """Load video metadata to PostgreSQL."""
    logger = get_run_logger()
    logger.info('Saving video metadata to database...')
    conn = connect_to_db()
    create_video_metadata_table()
    upsert_video_metadata(video_metadata)
    if 'conn' in locals():
        conn.close()
        logger.info('DB connection closed')
    
@task
def get_upload_playlists(update_threshold_days: int = 7) -> list[UploadPlaylist]:
    """Query upload playlists for channels that require processing/reprocessing."""
    logger = get_run_logger()
    logger.info('Getting upload playlists...')
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
        logger.error(f"Error retrieving upload playlist IDs: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info('DB connection closed')