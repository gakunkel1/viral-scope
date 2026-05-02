import json
import logging
import subprocess
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

from embedding.models import CreatorVideo

logger = logging.getLogger(__name__)

_YT_DLP = [sys.executable, "-m", "yt_dlp"]

TMP_VIDEO_DIR = os.getenv("TMP_VIDEO_DIR")

def download_video(url: str) -> CreatorVideo:
    """Download YouTube video and extract metadata."""
    meta_cmd = [*_YT_DLP, "--dump-json", "--no-download", url]
    result = subprocess.run(meta_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"yt-dlp metadata fetch failed: {result.stderr}")
        result.check_returncode()
    meta = json.loads(result.stdoutd)
    
    video_id = meta["id"]
    output_path = TMP_VIDEO_DIR / f"{video_id}.%(ext)s"