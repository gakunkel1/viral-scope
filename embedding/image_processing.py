from ingestion.storage import get_storage_backend
import torch
from torch import Tensor
import clip
from PIL import Image
import numpy as np
from decord import VideoReader, cpu
import cv2

class FrameProcessor:
    """
    Uses decord and CLIP to decode and normalize images into Tensors.
    """
    
    def __init__(self):
        self.storage = get_storage_backend()
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.clip_model = None
        self.preprocess = None
        
    def _ensure_model(self):
        """Initialize model if we don't have it initialized yet."""
        if self.clip_model is None or self.preprocess is None:
            self.clip_model, self.preprocess = clip.load(
                "ViT-B/32", device=self.device
            )
        
    def extract_keyframes(self, video_id: str, interval_sec: float = 5.0) -> tuple[np.ndarray, list]:
        """Extract keyframes from video metadata using decord."""
        with self.storage.scoped_local_path(video_id, ext="mp4") as local_path:
            cap = cv2.VideoCapture(local_path)
            fps = cap.get(cv2.CAP_PROP_FPS)
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            
            step = int(fps * interval_sec)
            indices = set(range(0, total_frames, step))
            
            frames = []
            timestamps = []
            frame_idx = 0
            
            while True:
                ret, frame = cap.read()
                if not ret:
                    break
                if frame_idx in indices:
                    # Transform from BGR to RGB
                    frames.append(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
                    timestamps.append(frame_idx / fps)
                frame_idx += 1
            
            cap.release()
            return np.array(frames), timestamps
        
    def _chunked(self, frames: np.ndarray, batch_size: int = 32):
        """Yield a chunk of frames of the specified batch size."""
        if len(frames) == 0:
            yield []
            return
        for i in range(0, len(frames), batch_size):
            yield frames[i : i + batch_size]
            
    def _normalize_features(self, image_features: Tensor) -> Tensor:
        image_features /= image_features.norm(dim=-1, keepdim=True)
        return image_features
            
    def get_image_features(self, frames: np.ndarray) -> Tensor:
        """Encode frames and normalize frame features in batches. Return normalized image features (the embeddings)."""
        # Make sure we have initialized the model
        self._ensure_model()
        
        # Use CLIP preprocessor to preprocess each image in the batch
        all_features = []
        for frame_batch in self._chunked(frames, 32):
            images_preprocessed = torch.stack([
                self.preprocess(Image.fromarray(img)) for img in frame_batch
            ]).to(self.device)
            
            # Encode the image with CLIP and normalize
            with torch.no_grad():
                batch_features = self.clip_model.encode_image(images_preprocessed)
                batch_features = self._normalize_features(batch_features)
                all_features.append(batch_features)
                
        return torch.cat(all_features, dim=0)