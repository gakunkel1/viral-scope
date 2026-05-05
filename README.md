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


# Instructions

## PostgreSQL and Prefect
0. sudo apt install ffmpeg
1. docker compose up -d
2. docker exec -it viral_postgres_container psql -U db_user -d db -c "CREATE DATABASE prefect;"
3. prefect init
4. prefect server start
5. prefect worker start --pool local-pool
6. prefect deploy --all
7. prefect gcl create video-ingestion --limit 1
8. prefect gcl create video-transcription --limit 1