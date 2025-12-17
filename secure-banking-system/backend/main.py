from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from fastapi.middleware.cors import CORSMiddleware # <--- IMPORT THIS
from celery_worker import run_fl_task
from database import CyborgDB 


app = FastAPI(title="Secure Banking Node A")

# --- ADD THIS BLOCK IMMEDIATELY AFTER app = FastAPI(...) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"], # Allow Frontend ports
    allow_credentials=True,
    allow_methods=["*"], # Allow all methods (POST, GET, etc.)
    allow_headers=["*"], # Allow all headers
)
# -----------------------------------------------------------

db = CyborgDB() 

# ... (Rest of your code remains exactly the same)

# Data Model
class Transaction(BaseModel):
    id: str
    features: list[float] # Expected length: 6

class SearchQuery(BaseModel):
    features: list[float]

@app.get("/")
def read_root():
    return {"status": "active", "system": "CyborgDB_Node_A"}

# --- 1. Federated Learning Endpoint ---
@app.post("/train-local")
async def trigger_training():
    """Starts the FL client in the background"""
    task = run_fl_task.delay()
    return {"message": "Federated Training started", "task_id": task.id}

# --- 2. CyborgDB Secure Ingestion ---
@app.post("/secure-ingest")
async def ingest_transaction(txn: Transaction):
    """Encrypts and stores transaction in Vector DB"""
    # For demo, we treat the first 3 features as the vector to encrypt
    # In reality, you'd use an Encoder model here
    vector_subset = txn.features[:3] 
    
    db.secure_storage(txn.id, vector_subset)
    return {"status": "success", "id": txn.id, "encryption": "Homomorphic_CKKS"}

# --- 3. CyborgDB Secure Search ---
@app.post("/secure-search")
async def search_network(query: SearchQuery):
    """
    Simulates a broadcast query. 
    1. Encrypts query.
    2. Compares against local encrypted DB.
    """
    vector_subset = query.features[:3]
    matches = db.secure_search(vector_subset)
    
    if matches:
        return {"match_found": True, "details": matches}
    else:
        return {"match_found": False}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)