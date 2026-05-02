# viral-scope

Viral Scope is an AI-powered influencer content intelligence agent that ingests raw social video, extracts multimodal data (frames, audio, and text), stores them as embeddings, and performs autonomous reasoning over the data to recommend actions for content campaigns.

## Ingestion Layer
- YouTube scraper (yt-dlp)
- Push to Amazon S3 (boto3)

## Processing Layer
- Frame extraction (FFmpeg)
- Transcription (OpenAI Whisper)
- Visual embeddings (OpenAI CLIP)

## Data Storage
- Vector database (Qdrant)
- Engagement stats and creator profiles (PostgreSQL)

## Agent Layer
- Processing loop (LangGraph, ReAct)
- LLM analysis (Claude API)
- Persistent memory (Redis)

## Tool Execution
- Virality score calculator
- Content similarity search