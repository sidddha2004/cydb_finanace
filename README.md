# Sentinel AI – Project Setup & Run Guide

This guide explains how to run **Sentinel AI** locally using a **non-containerized backend** while using **Docker only for infrastructure services**.

---

## Prerequisites

Ensure the following are installed:

* Python 3.10+
* Node.js 18+
* Docker & Docker Compose
* Git

---

## 1. Clone the Repository

```bash
git clone https://github.com/<your-org>/sentinel-ai.git
cd sentinel-ai
```

---

## 2. Start Infrastructure Services (Docker)

This starts Kafka, Zookeeper, Redis, CyborgDB, and Kafka UI.

```bash
docker-compose up -d
```

Verify services:

* Kafka UI: [http://localhost:8080](http://localhost:8080)
* CyborgDB: [http://localhost:8001](http://localhost:8001)

---

## 3. Backend Setup (Local Python)

```bash
cd backend
python -m venv venv
```

Activate virtual environment:

**Windows**

```bash
venv\Scripts\activate
```

**Mac/Linux**

```bash
source venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## 4. Environment Variables

Create a `.env` file inside `backend/`:

```env
CYDB_URL=http://127.0.0.1:8001
CYDB_API_KEY=key
GOOGLE_API_KEY=your_google_gemini_api_key
KAFKA_BOOTSTRAP=localhost:9092
```

> Do not commit `.env` to GitHub.

---

## 5. Initialize Dataset & Indexes

```bash
python data_loader.py
```

This initializes CyborgDB indexes and prepares streaming + federated datasets.

---

## 6. Start Backend API

```bash
uvicorn main:app --reload
```

Backend runs at:

* [http://localhost:8000](http://localhost:8000)

---

## 7. Start Real-Time Streaming (Kafka)

Open two terminals (backend venv active).

**Terminal 1 – Consumer**

```bash
python streaming_consumer.py
```

**Terminal 2 – Producer**

```bash
python streaming_producer.py
```

---

## 8. (Optional) Start Federated Learning Simulator

```bash
python fl_trainer.py
```

---

## 9. Frontend Setup

```bash
cd frontend
npm install
npm run dev
```

Frontend runs at:

* [http://localhost:5173](http://localhost:5173)

---

## 10. Basic Testing

* Submit transaction → risk assessment shown
* Search transactions
* Run AI analysis
* Admin actions: broadcast, federated round, optimize indexes

---

## 11. Stopping the Application

Stop infrastructure services:

```bash
docker-compose down
```

---

## 12. Cleaning / Resetting the Database (Optional)

Use this when you want a **fresh start** (clean CyborgDB, Kafka topics, Redis state).

### Full infrastructure + data reset (recommended)

```bash
docker-compose down -v
```

This will:

* Stop all containers
* Remove **volumes** (CyborgDB indexes, Kafka data, Redis cache)
* Force a clean database state on next startup

After this, restart services and reinitialize data:

```bash
docker-compose up -d
python data_loader.py
```

### Backend-only reset (no Docker reset)

If you only want to clear indexes:

```bash
python data_loader.py
```

(This recreates indexes and reloads initial data.)
