# 🚨 Sentinel AI

**Federated Fraud Detection & Intelligence Platform**

Sentinel AI is a privacy-preserving, federated learning-based fraud detection system designed for real-time financial transaction monitoring across multiple banks. It enables institutions to collaborate on fraud intelligence without sharing raw data, using AI-native vector search, streaming, and explainable AI.

---

## ✨ Key Features

- 🔐 **Federated Learning** – Banks share intelligence, not raw data
- 🧠 **AI-Native Vector Search** using CyborgDB
- ⚡ **Real-Time Fraud Detection** via Kafka streaming
- 📡 **Threat Broadcasting** across banks
- 🤖 **Explainable AI (RAG)** using Gemini
- 📊 **Live Dashboards** with WebSockets
- 🧩 **Dual-Index Architecture**
  - `secure_history` (legitimate patterns)
  - `known_threats` (fraud patterns)
- 🧑‍💼 **Role-Based Access Control** (Admin / Bank Users)

---

## 🏗️ System Architecture

```
Banks (A, B, C)
   ↓ (pattern updates only)
Sentinel AI Central Aggregator
   ├─ Federated Learning Engine
   ├─ RAG Intelligence Layer
   └─ FastAPI Orchestrator
           ↓
       CyborgDB
   ├─ secure_history
   └─ known_threats
```

> **Note:** Raw transaction data never leaves the bank boundary.

---

## 🧰 Tech Stack

### Frontend
- React.js
- Vite
- Axios
- WebSockets
- CSS / Tailwind

### Backend
- Python
- FastAPI
- Uvicorn

### AI / ML
- Sentence Transformers (Bi-Encoder)
- Cross-Encoder (semantic re-ranking)
- Federated Learning (custom coordinator)
- Risk scoring & anomaly detection

### Data & Streaming
- CyborgDB (vector database)
- Kafka (Docker-based)
- Redis (real-time caching & stats)

### AI Intelligence
- RAG (Retrieval-Augmented Generation)
- Google Gemini API

### DevOps / Deployment
- Docker & Docker Compose
- Azure App Service / Container Apps
- Azure Redis Cache (cloud)

---

## 📊 Dataset Information

### Supported Dataset (Recommended)

**PaySim – Financial Fraud Dataset**

- **Source:** [Kaggle - PaySim Dataset](https://www.kaggle.com/datasets/ntnu-testimon/paysim1)
- **Format:** CSV
- **Use case:** Simulated mobile money transactions with fraud labels

### 📥 Dataset Setup (Option 1 – Manual)

1. Download the dataset from Kaggle
2. Extract and place the CSV here: `backend/data/transactions.csv`
3. Expected filename: `transactions.csv`

**Required columns (default PaySim schema):**
- `step`, `type`, `amount`
- `nameOrig`, `oldbalanceOrg`, `newbalanceOrg`
- `nameDest`, `oldbalanceDest`, `newbalanceDest`
- `isFraud`, `isFlaggedFraud`

### ⚙️ Dataset Setup (Option 2 – Automatic)

If no dataset is found, Sentinel AI will automatically generate a synthetic dataset when you run:

```bash
python data_loader.py
```

✔ Same schema as PaySim  
✔ Safe for demos & testing  
✔ No manual download required

---

## ⚙️ Project Setup & Run Guide

### Prerequisites

- Python 3.10+
- Node.js 18+
- Docker & Docker Compose
- Git

---

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/<your-org>/sentinel-ai.git
cd sentinel-ai
```

---

### 2️⃣ Start Infrastructure Services (Docker)

Starts Kafka, Zookeeper, Redis, CyborgDB, Kafka UI.

```bash
docker-compose up -d
```

**Verify:**
- Kafka UI: http://localhost:8080
- CyborgDB: http://localhost:8001

---

### 3️⃣ Backend Setup (Local Python)

```bash
cd backend
python -m venv venv
```

**Activate venv:**

- **Windows:**
  ```bash
  venv\Scripts\activate
  ```

- **Mac / Linux:**
  ```bash
  source venv/bin/activate
  ```

**Install dependencies:**

```bash
pip install -r requirements.txt
```

---

### 4️⃣ Environment Variables

Create `backend/.env`:

```env
CYDB_URL=http://127.0.0.1:8001
CYDB_API_KEY=key
GOOGLE_API_KEY=your_google_gemini_api_key
KAFKA_BOOTSTRAP=localhost:9092
REDIS_URL=redis://localhost:6379
```

⚠️ **Do not commit `.env` to GitHub**

---

### 5️⃣ Initialize Dataset & Indexes

```bash
python data_loader.py
```

This will:
- Load dataset (real or synthetic)
- Create CyborgDB indexes
- Prepare streaming & federated data

---

### 6️⃣ Start Backend API

```bash
uvicorn main:app --reload
```

**Backend runs at:** http://localhost:8000

---

### 7️⃣ Start Real-Time Streaming

Open two terminals (backend venv active).

**Consumer:**
```bash
python streaming_consumer.py
```

**Producer:**
```bash
python streaming_producer.py
```

---

### 8️⃣ (Optional) Federated Learning Simulator

```bash
python fl_trainer.py
```

---

### 9️⃣ Frontend Setup

```bash
cd frontend
npm install
npm run dev
```

**Frontend:** http://localhost:5173

---

## 🧪 Basic Testing Checklist

- [ ] Submit transaction → risk shown
- [ ] Search transactions
- [ ] Run AI analysis (RAG)
- [ ] Admin actions:
  - [ ] Broadcast threats
  - [ ] Federated round
  - [ ] Optimize indexes

---

## 🛑 Stopping the Application

```bash
docker-compose down
```

---

## ♻️ Reset / Clean Start (IMPORTANT)

### Full Reset (Recommended)

```bash
docker-compose down -v
docker-compose up -d
python data_loader.py
```

This clears:
- CyborgDB indexes
- Kafka topics
- Redis cache

---

## 📝 License

MIT

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.



---

**Built with ❤️ for secure, privacy-preserving fraud detection**
