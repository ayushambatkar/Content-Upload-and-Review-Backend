# Content Upload and Review Backend

FastAPI backend for large CSV movie ingestion and query APIs backed by MongoDB.


### Postman Collection: [here](./Content_Review_and_Upload.postman_collection.json)

## Features

- Streaming CSV upload endpoint (`POST /upload`) with 1GB limit
- Background async processing with job tracking (`GET /jobs/{job_id}`)
- Chunked ingestion using configurable batch size (default `5000`)
- Robust CSV parsing with strict header validation and invalid row skipping
- Deduplication using unique compound key: `original_title + release_date`
- Paginated movie query endpoint with filtering and sorting (`GET /movies`)
- MongoDB indexes for query performance and deduplication

## CSV Schema (strict order)

```text
budget
homepage
original_language
original_title
overview
release_date
revenue
runtime
status
title
vote_average
vote_count
production_company_id
genre_id
languages
```

## Tech Stack

- FastAPI
- Motor (async MongoDB driver)
- Pydantic v2

## Setup

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Copy env file and customize if needed:

```bash
copy .env.example .env
```

4. Start MongoDB locally (or set `MONGODB_URI` to your cluster).
5. Run the API:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## API Examples

### Health

```bash
curl http://localhost:8000/health
```

### Upload CSV

```bash
curl -X POST "http://localhost:8000/upload" \
  -F "file=@movies.csv"
```

Response:

```json
{
  "job_id": "a6cf4d31-8a8f-40d9-95d4-70db18c536e8",
  "status": "pending"
}
```

### Check Job Status

```bash
curl "http://localhost:8000/jobs/a6cf4d31-8a8f-40d9-95d4-70db18c536e8"
```

Response:

```json
{
  "status": "processing",
  "processed_rows": 12000,
  "total_rows": 500000,
  "failed_rows": 120
}
```

### Query Movies

```bash
curl "http://localhost:8000/movies?page=1&limit=20&release_year=2015&language=en&sort_by=vote_average&sort_order=desc"
```

Response shape:

```json
{
  "total": 1234,
  "page": 1,
  "limit": 20,
  "results": []
}
```

## Notes on Reliability and Performance

- Upload endpoint streams file to disk in chunks, avoiding full memory loading.
- Ingestion runs in background and updates progress counters.
- Invalid rows are logged and skipped.
- Processing is fault-tolerant with job status transitions to `failed` and error tracking.
- Duplicate movie rows are prevented by unique MongoDB compound index.
