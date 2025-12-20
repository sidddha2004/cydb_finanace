✅ Prerequisites

Make sure you have installed:

Python 3.10+

Node.js 18+

Docker & Docker Compose

Git

🚀 Step-by-Step Setup
1️⃣ Clone the Repository
git clone https://github.com/<your-username>/sentinel-ai.git
cd sentinel-ai

2️⃣ Start Infrastructure Services (Docker)

This starts:

Kafka

Zookeeper

Redis

CyborgDB

Kafka UI

docker-compose up -d


Verify:

Kafka UI → http://localhost:8080

CyborgDB → http://localhost:8001

3️⃣ Backend Setup (Local Python)
Create Virtual Environment
cd backend
python -m venv venv


Activate it:

Windows

venv\Scripts\activate


Mac/Linux

source venv/bin/activate

Install Dependencies
pip install -r requirements.txt

4️⃣ Environment Variables

Create a .env file inside backend/:

CYDB_URL=http://127.0.0.1:8001
CYDB_API_KEY=key
GOOGLE_API_KEY=your_google_gemini_api_key
KAFKA_BOOTSTRAP=localhost:9092


⚠️ Do NOT commit .env to GitHub

5️⃣ Initialize Dataset & Indexes

This step:

Loads PaySim data

Enriches transactions

Splits data for Kafka & Federated Learning

Loads initial data into CyborgDB

python data_loader.py


You should see:

secure_history index created

known_threats index created

Bank A / B / C data loaded

6️⃣ Start Backend API
uvicorn main:app --reload


Backend runs at:

http://localhost:8000

7️⃣ Start Real-Time Streaming

Open two new terminals (backend venv active).

Terminal 1 – Kafka Consumer
python streaming_consumer.py

Terminal 2 – Kafka Producer
python streaming_producer.py


You should see live transactions flowing.

8️⃣ (Optional) Start Federated Learning Simulator
python fl_trainer.py


Simulates federated learning rounds every few minutes.

9️⃣ Frontend Setup
cd frontend
npm install
npm run dev


Frontend runs at:

http://localhost:5173

🔍 What to Test
User Dashboard

Submit legitimate transaction → LOW RISK

Submit fraud transaction → BLOCKED

Search transactions

Run AI analysis (RAG)

Admin Panel (Demo Only)

Inject fraud

Trigger federated round

Broadcast threat

Optimize DB indexes

Observe Kafka live stats

⚠️ Important Notes

Admin panel exists only for demo

In real banking systems:

Federated learning is automatic

Threat broadcast is system-triggered

No human admin manually injects fraud

🧯 Troubleshooting
CyborgDB Connection Error

Make sure this is running:

docker ps


Check:

sentinel-cyborgdb → port 8001

Kafka Not Receiving Messages

Ensure Kafka container is healthy

Check Kafka UI at localhost:8080

📦 Stopping Everything
docker-compose down

🏁 Summary

This setup runs Sentinel AI exactly like a real-world system:

Local backend

Dockerized infrastructure

Real-time Kafka streaming

Federated learning simulation

AI-powered fraud analysis