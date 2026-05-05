import os
import logging
from abc import ABC, abstractmethod
from pathlib import Path
import uuid
from datetime import datetime
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, Vector, CollectionsResponse, PointStruct, VectorParams, Batch
from langchain_huggingface import HuggingFaceEmbeddings

from models.embeddings import TranscriptChunk

logger = logging.getLogger(__name__)

def make_chunk_id(video_id: str, start_time: float) -> str:
    """UUID from video + timestamp."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{video_id}:{start_time}"))

def make_frame_id(video_id: str, frame_timestamp: float) -> str:
    """UUID from video + frame timestamp."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{video_id}:{frame_timestamp}"))

class VectorStore:
    """
    Uses Qdrant vector database to store embeddings.
    """
    
    COLLECTIONS = {
        'transcript_chunks': {
            'size': 384,
            'distance': Distance.COSINE,
            'model': 'all-MiniLM-L6-v2'
        },
        'video_frames': {
            'size': 512,
            'distance': Distance.COSINE
        }
    }
    
    def __init__(self, url: str, api_key: str):
        model_name = self.COLLECTIONS['transcript_chunks']['model']
        self.embeddings = HuggingFaceEmbeddings(model_name=model_name)
        
        self.client = QdrantClient(url=url, api_key=api_key)
        
        logger.info(f"VectorStore initialized: Qdrant at URL {url}")
        
    def ensure_collections(self):
        """Ensure collections exist at setup time."""
        existing = {c.name for c in self.client.get_collections().collections}
        for name, config in self.COLLECTIONS.items():
            if name not in existing:
                self.client.create_collection(
                    collection_name=name,
                    vectors_config=VectorParams(
                        size=config['size'],
                        distance=config['distance']
                    )
                )
                
    def embed_transcript_chunk(self, text: str) -> Vector:
        """Use configured model to embed a text chunk into vector space."""
        result = self.embeddings.embed_query(text)
        return result
        
    def upsert_transcript_chunks(self, points: list[PointStruct], ):
        """Add (or update by ID) the chunks into the Qdrant collection."""
        self.client.upsert(
            collection_name='transcript_chunks',
            points=points
        )