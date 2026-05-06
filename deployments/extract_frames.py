from prefect import flow, task, get_run_logger
from prefect.concurrency.sync import concurrency
from prefect.cache_policies import NO_CACHE
import os
from dotenv import load_dotenv
from qdrant_client.models import PointStruct

from embedding.image_processing import FrameProcessor
from embedding.vector_db import VectorStore, make_chunk_id
from embedding.tasks import (
    get_unprocessed_frames_videos,
    update_frame_processing_status
)
from ingestion.tasks import create_db_tables

load_dotenv()

QDRANT_API_KEY = os.environ['QDRANT_API_KEY']
QDRANT_URL = os.environ['QDRANT_URL']
DELETE_AFTER_PROCESSING = os.getenv("DELETE_AFTER_PROCESSING", "false").lower() in ("true", "1", "yes", "y")
    
@flow(log_prints=True, timeout_seconds=600)
def extract_frames():
    """Extract frames from unprocessed videos. Store embeddings in Qdrant."""
    with concurrency("frame-extraction", occupy=1):
        logger = get_run_logger()
        logger.info("Starting frame extraction pipeline...")
        
        # Create 'youtube' schema tables if they don't yet exist
        create_db_tables()
        
        # Get videos that need their frames processed
        videos = get_unprocessed_frames_videos()
        logger.info(f'Processing {len(videos)} videos...')
        
        # Initialize Qdrant database
        vs = VectorStore(url=QDRANT_URL, api_key=QDRANT_API_KEY)
        
        # Create collections if they don't yet exist
        logger.info('Ensuring collections exist...')
        vs.ensure_collections()
        
        # Initialize FrameProcessor
        fp = FrameProcessor()
        
        logger.info([v.video_id for v in videos])
        for v in videos:
            logger.info(f'Extracting frames from video {v.video_id}...')
            try:
                frames, timestamps = fp.extract_keyframes(v.video_id, interval_sec=3)
                frame_embeddings = fp.get_image_features(frames)
                
                points = [
                    PointStruct(
                        id=make_chunk_id(v.video_id, ts),
                        vector=emb.cpu().tolist(),
                        payload={
                            'timestamp': ts,
                            'video_id': v.video_id,
                            'channel_id': v.channel_id
                        }
                    )
                    for emb, ts in zip(frame_embeddings, timestamps)
                ]
                vs.upsert_chunks(points, 'video_frames')
                logger.info(f'Stored {len(points)} frame embeddings for {v.video_id}')
                
                # Update frames_processed_at in youtube.video_metadata
                update_frame_processing_status(v.video_id)
                
                # Delete the file if configured to do so post-processing
                if DELETE_AFTER_PROCESSING and v.transcript_exists:
                    logger.info(f'Deleting video {v.video_id}')
                    fp.storage.delete(v.video_id, ext="mp4")
            except Exception as e:
                logger.warning(f'Skipping {v.video_id}: {e}')
        