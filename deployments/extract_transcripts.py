from prefect import flow, get_run_logger
from prefect.concurrency.sync import concurrency
import os
import whisper
from dotenv import load_dotenv

from embedding.vector_db import VectorStore
from embedding.tasks import (
    get_unprocessed_audio_videos, save_transcript,
    chunk_transcript, embed_and_store_chunks
)
from ingestion.tasks import create_db_tables

load_dotenv()

QDRANT_API_KEY = os.environ['QDRANT_API_KEY']
QDRANT_URL = os.environ['QDRANT_URL']
WHISPER_MODEL = os.environ['WHISPER_MODEL']
WHISPER_DEVICE = os.environ['WHISPER_DEVICE']

@flow(log_prints=True, timeout_seconds=600)
def extract_transcripts():
    """Transcribe unprocessed videos. Store embeddings in Qdrant."""
    with concurrency("video-transcription", occupy=1):
        logger = get_run_logger()
        logger.info("Starting transcription pipeline...")
        
        model = whisper.load_model(WHISPER_MODEL, device=WHISPER_DEVICE)
        logger.info(f'Loaded Whisper model "{WHISPER_MODEL}"...')
        
        # Create 'youtube' schema tables if they don't yet exist
        create_db_tables()
        
        # Get videos that need transcription
        videos = get_unprocessed_audio_videos()
        logger.info(f'Processing {len(videos)} videos...')
        
        # Initialize Qdrant database
        vs = VectorStore(url=QDRANT_URL, api_key=QDRANT_API_KEY)
        
        # Create collections if they don't yet exist
        logger.info('Ensuring collections exist...')
        vs.ensure_collections()
        
        logger.info([v.video_id for v in videos])
        for v in videos:
            logger.info(f'Transcribing v {v.video_id}')
            result = model.transcribe(v.storage_uri)
            save_transcript(WHISPER_MODEL, v.video_id, result)
            chunks = chunk_transcript(v.video_id, result["segments"])
            embed_and_store_chunks(vs, chunks)
            
        logger.info('Transcription pipeline complete.')