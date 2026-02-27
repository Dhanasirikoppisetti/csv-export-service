# 📦 CSV Export Service

A scalable CSV export system built with FastAPI, PostgreSQL, and Docker that can handle exports of 10+ million rows efficiently without crashing or consuming excessive memory.

This project demonstrates how to design a production-style export service that supports streaming, filtering, cancellation, compression, and concurrent processing.

## 🚀 What This Project Does

This service allows users to:

Create large CSV exports from a database

Track export progress

Download files (with resume support)

Cancel running exports

Filter data dynamically

Select specific columns

Use custom delimiters

Download compressed files (gzip)

All while keeping memory usage controlled and the API responsive.

## 🧠 Why This Is Special

Instead of loading all data into memory, this service:

Streams data directly from PostgreSQL

Writes rows in batches

Supports large-scale exports (10M+ rows)

Handles multiple concurrent export jobs

Works within a 150MB memory limit

This makes it suitable for real-world SaaS environments.

## 🏗️ Tech Stack

FastAPI – API framework

PostgreSQL – Database

SQLAlchemy (Async) – Async DB layer

Docker & Docker Compose – Containerized setup

Uvicorn – ASGI server

## 📁 Project Structure
``` bash
csv-export-service/
│
├── app/
│   ├── main.py
│   ├── database.py
│   ├── models/
│   ├── routes/
│   └── services/
│
├── exports/                # Generated CSV files
├── seeds/                  # Database initialization scripts
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```
## ⚙️ How to Run the Project
### 1️⃣ Build and Start
```bash
docker-compose up --build -d
```
### 2️⃣ Check Health
``` bash
curl http://localhost:8080/health
```

Expected:
``` bash
{"status":"ok"}
```
### 📤 Create an Export
``` bash
curl -X POST http://localhost:8080/exports/csv
```

Response:
``` bash
{
  "exportId": "uuid",
  "status": "pending"
}
```
### 🔍 Create Export with Filters

Filter by country and minimum lifetime value:
``` bash
curl -X POST "http://localhost:8080/exports/csv?country_code=US&min_ltv=500"
```
🧩 Select Specific Columns

``` bash
curl -X POST "http://localhost:8080/exports/csv?columns=id,email,country_code"
```

### ➗ Custom Delimiter

``` bash
curl -X POST "http://localhost:8080/exports/csv?delimiter=|"
```
### 📊 Check Export Status

``` bash
curl http://localhost:8080/exports/<EXPORT_ID>/status
```
#### Response includes:

totalRows

processedRows

percentage

filePath

downloadUrl

### ⬇️ Download Export
``` bash
curl -OJ http://localhost:8080/exports/<EXPORT_ID>/download
```
### 🔄 Resume Download (Range Support)
```bash
curl -H "Range: bytes=0-1023" \
http://localhost:8080/exports/<EXPORT_ID>/download
```
### 🗜️ Download Compressed Version (Gzip)
```bash
curl -H "Accept-Encoding: gzip" \
http://localhost:8080/exports/<EXPORT_ID>/download \
--output export.gz
```
### ❌ Cancel an Export
```bash
curl -X DELETE http://localhost:8080/exports/<EXPORT_ID>
```
## ⚡ Performance Highlights

### Tested with:

10,000,000+ rows

3 concurrent exports

<150MB memory usage

API response time ~0.005 seconds during export

Large file downloads (800MB+) supported

Gzip reduces 800MB → ~130MB

## 🧱 Architecture Overview

Export request creates a job in the database.

Background task starts streaming data.

Data is fetched in batches from PostgreSQL.

Rows are written incrementally to disk.

Status is updated progressively.

File is served with streaming + range support.

This ensures:

No memory overflow

High performance

API remains responsive

## 🔐 Environment Configuration

#### Environment variables (example):
``` bash
DATABASE_URL=postgresql+asyncpg://exporter:secret@db:5432/exports_db
EXPORT_STORAGE_PATH=/app/exports
```
## 🐳 Docker Configuration

PostgreSQL runs in its own container

App container has 150MB memory limit

Database health checks enabled

Persistent volumes for:

PostgreSQL data

Generated CSV exports

## 📌 Design Decisions

Streaming instead of buffering → prevents memory spikes

Async SQLAlchemy → non-blocking DB access

Range requests → supports download resume

Gzip support → bandwidth optimization

Cancellation flag in DB → safe job stopping

Health checks → production readiness

## 🎯 What This Project Demonstrates

Scalable backend design

Efficient large dataset handling

Async programming

Resource optimization

Production-style Docker setup

Clean API architecture


## 🎥 Project Demo

A complete walkthrough of the architecture, scalability decisions, and live system demonstration can be viewed below:

 click here directly:
 
👉 https://youtu.be/VJyapMW0LcY

## 👨‍💻 Author

Built as a scalable CSV export system to demonstrate backend performance, concurrency handling, and efficient streaming design.
