import os
from prefect import task, get_run_logger
from prefect.cache_policies import NO_CACHE
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from datetime import datetime, UTC

from qdrant_client.models import PointStruct

from db.pg_connect import connect_to_db
from models.youtube import VideoMetadata
from models.embeddings import TranscriptChunk
from embedding.vector_db import make_chunk_id, VectorStore

###########################################################################
# TRANSCRIPT
###########################################################################

@task
def chunk_transcript(video_id: str, segments: list, max_tokens=200) -> list[TranscriptChunk]:
    """Fixed-length chunking to preserve timestamps."""
    logger = get_run_logger()
    logger.info(f'Chunking transcript for video {video_id}')
    chunks: list[TranscriptChunk] = []
    current_chunk = []
    current_length = 0
    
    # Recombining the segments, saving segment IDs in metadata
    # Considered semantic chunking but this was cleaner for saving timestamps
    for seg in segments:
        seg_tokens = len(seg["text"].split())
        if current_length + seg_tokens > max_tokens and current_chunk:
            chunks.append(
                TranscriptChunk(
                    id=make_chunk_id(video_id, current_chunk[0]["start"]),
                    text=" ".join(s["text"].strip() for s in current_chunk),
                    start_time=current_chunk[0]["start"],
                    end_time=current_chunk[-1]["end"],
                    segment_ids=[s["id"] for s in current_chunk],
                    video_id=video_id
                )    
            )
            current_chunk = []
            current_length = 0
        current_chunk.append(seg)
        current_length += seg_tokens

    if current_chunk:
        chunks.append(
            TranscriptChunk(
                    id=make_chunk_id(video_id, current_chunk[0]["start"]),
                    text=" ".join(s["text"].strip() for s in current_chunk),
                    start_time=current_chunk[0]["start"],
                    end_time=current_chunk[-1]["end"],
                    segment_ids=[s["id"] for s in current_chunk],
                    video_id=video_id
                )
        )
    return chunks

@task(cache_policy=NO_CACHE)
def embed_and_store_chunks(vector_store: VectorStore, chunks: list[TranscriptChunk]):
    logger = get_run_logger()
    logger.info(f'Embedding and storing transcript chunks...')
    points: list[PointStruct] = []
    for c in chunks:
        points.append(
            PointStruct(
                id=c.id,
                vector=vector_store.embed_transcript_chunk(c.text),
                payload={
                    'start_time': c.start_time,
                    'end_time': c.end_time,
                    'segment_ids': c.segment_ids,
                    'video_id': c.video_id
                }
            )
        )
    vector_store.upsert_chunks(points, 'transcript_chunks')
        
@task
def save_transcript(whisper_model: str, video_id: str, transcription_result: dict):
    """Save one video transcript to youtube.video_metadata."""
    logger = get_run_logger()
    logger.info(f'Saving transcript for video {video_id}...')
    conn = connect_to_db()
    try:
        with conn.cursor() as cur:
            insert_sql = f"""
                INSERT INTO youtube.transcripts(
                video_id, full_text, language, segments, model_name, transcribed_at)
                VALUES (%s, %s, %s, %s, %s, %s);
                """
            data = (
                video_id,
                transcription_result['text'],
                transcription_result['language'],
                Json(transcription_result['segments']),
                whisper_model,
                datetime.now(UTC)
            )
            cur.execute(insert_sql, data)
        conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Failed to insert record into youtube.transcripts: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info('DB connection closed')

@task
def create_transcripts_table():
    """Create youtube.transcripts if it doesn't exist yet."""
    logger = get_run_logger()
    logger.info('Creating table youtube.transcripts...')
    conn = connect_to_db()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS youtube;
            CREATE TABLE IF NOT EXISTS youtube.transcripts (
                video_id TEXT PRIMARY KEY REFERENCES youtube.video_metadata(video_id),
                full_text TEXT,
                language TEXT,
                segments JSONB,
                model_name TEXT,
                transcribed_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        conn.commit()
    except psycopg2.Error as e:
        logger.error(f'Failed to create youtube.transcripts table: {str(e)}')
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            logger.info('DB connection closed')

@task
def get_unprocessed_audio_videos() -> list[VideoMetadata]:
    """Retrieve unprocessed videos from youtube.video_metadata."""
    logger = get_run_logger()
    logger.info('Getting unprocessed videos for transcription...')
    videos_to_process: list[VideoMetadata] = []
    try:
        conn = connect_to_db()
        with conn.cursor('get_unprocessed_audio_videos', cursor_factory=RealDictCursor) as read_cur:
            read_cur.execute(f"""
                SELECT
                    vm.video_id,
                    vm.title,
                    vm.channel,
                    vm.channel_id,
                    vm.duration_seconds,
                    vm.view_count,
                    vm.like_count,
                    vm.comment_count,
                    vm.upload_date,
                    vm.description,
                    vm.tags,
                    vm.webpage_url,
                    vm.ext,
                    vm.storage_uri,
                    vm.ingested_at
                FROM youtube.video_metadata vm
                LEFT JOIN youtube.transcripts t ON vm.video_id = t.video_id
                WHERE t.video_id IS NULL;             
            """)
            videos_to_process.extend([
                VideoMetadata(
                    video_id=row['video_id'],
                    title=row['title'],
                    channel=row['channel'],
                    channel_id=row['channel_id'],
                    duration_seconds=row['duration_seconds'],
                    view_count=row['view_count'],
                    like_count=row['like_count'],
                    comment_count=row['comment_count'],
                    upload_date=row['upload_date'],
                    description=row['description'],
                    tags=row['tags'],
                    webpage_url=row['webpage_url'],
                    ext=row['ext'],
                    storage_uri=row['storage_uri'],
                    ingested_at=row['ingested_at']
                )
                for row in read_cur
            ])
        return videos_to_process
    except psycopg2.Error as e:
        logger.error(f"Error retrieving video metadata: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info('DB connection closed')
            
            
###########################################################################
# FRAMES
###########################################################################
@task
def get_unprocessed_frames_videos() -> list[VideoMetadata]:
    """Retrieve videos from youtube.video_metadata where frames have not yet been processed."""
    logger = get_run_logger()
    logger.info('Getting unprocessed videos for frame extraction and embedding...')
    videos_to_process: list[VideoMetadata] = []
    try:
        conn = connect_to_db()
        with conn.cursor('get_unprocessed_frames_videos', cursor_factory=RealDictCursor) as read_cur:
            read_cur.execute(f"""
                SELECT
                    vm.video_id,
                    vm.title,
                    vm.channel,
                    vm.channel_id,
                    vm.duration_seconds,
                    vm.view_count,
                    vm.like_count,
                    vm.comment_count,
                    vm.upload_date,
                    vm.description,
                    vm.tags,
                    vm.webpage_url,
                    vm.ext,
                    vm.storage_uri,
                    vm.ingested_at
                FROM youtube.video_metadata vm
                WHERE vm.frames_processed_at IS NULL;             
            """)
            videos_to_process.extend([
                VideoMetadata(
                    video_id=row['video_id'],
                    title=row['title'],
                    channel=row['channel'],
                    channel_id=row['channel_id'],
                    duration_seconds=row['duration_seconds'],
                    view_count=row['view_count'],
                    like_count=row['like_count'],
                    comment_count=row['comment_count'],
                    upload_date=row['upload_date'],
                    description=row['description'],
                    tags=row['tags'],
                    webpage_url=row['webpage_url'],
                    ext=row['ext'],
                    storage_uri=row['storage_uri'],
                    ingested_at=row['ingested_at']
                )
                for row in read_cur
            ])
        return videos_to_process
    except psycopg2.Error as e:
        logger.error(f"Error retrieving video metadata: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info('DB connection closed')
            
            
def update_frame_processing_status(video_id: str):
    """Update youtube.video_metadata with frames_processed_at timestamp, after processing/embedding."""
    logger = get_run_logger()
    logger.info(f'Marking video {video_id} as processed...')
    conn = connect_to_db()
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            UPDATE youtube.video_metadata
            SET frames_processed_at = NOW()
            WHERE video_id = '{video_id}'
            );
        """)
        conn.commit()
    except psycopg2.Error as e:
        logger.error(f'Failed to mark video {video_id} as processed: {str(e)}')
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            logger.info('DB connection closed')