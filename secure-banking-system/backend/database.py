import cyborgdb
from cyborgdb import Client
import time

# --- CONFIGURATION ---
CYBORG_URL = "http://127.0.0.1:8001" # Running on port 8001
API_KEY = "cyborg_4252f3ea26384623b2d77d3804550cf0"

# FIX: Use a static 32-byte key for Dev so we can always delete the old index.
# (In production, never hardcode keys!)
DEV_KEY = b"secure_dev_key_32_bytes_long_!!!" 

class CyborgDB:
    def __init__(self):
        print("🔌 Connecting to CyborgDB (Service Mode)...")
        
        self.client = Client(
            base_url=CYBORG_URL,
            api_key=API_KEY
        )

        index_name = "secure_transactions"
        self.key = DEV_KEY

        # --- CORRECT CLEANUP LOGIC FROM DOCS ---
        print(f"🧹 Checking for existing index '{index_name}'...")
        try:
            # 1. We must LOAD the index first to delete it (as per docs)
            # This works now because we are using the consistent DEV_KEY
            existing_index = self.client.load_index(
                index_name=index_name,
                index_key=self.key
            )
            
            # 2. Call delete on the INDEX object, not the client
            existing_index.delete_index()
            print("   ↳ 🗑️ Old index deleted successfully.")
            
            # Safety pause
            time.sleep(1)
        except Exception as e:
            # If index doesn't exist, load_index might fail, which is fine.
            print(f"   ↳ ℹ️ Clean start (Index didn't exist or mismatch: {e})")

        # --- CREATE NEW INDEX ---
        print(f"🔨 Creating new encrypted index '{index_name}'...")
        try:
            self.index = self.client.create_index(
                index_name=index_name,
                index_key=self.key
            )
            print("✅ CyborgDB Connected & Index Ready.")
        except Exception as e:
            print(f"❌ CRITICAL ERROR: {e}")
            raise e

    def secure_storage(self, txn_id: str, vector: list):
        try:
            # Send list directly (No 'items=' keyword)
            self.index.upsert([
                {
                    "id": txn_id,
                    "vector": vector,
                    "metadata": {
                        "risk_score": "high", 
                        "source": "secure_node"
                    }
                }
            ])
            print(f"✅ Securely ingested txn: {txn_id}")
            return True
        except Exception as e:
            print(f"❌ Storage Error: {e}")
            return False

    def secure_search(self, query_vector: list):
        try:
            # FIX 1: Use 'query_vectors=' (Plural, as per docs)
            results = self.index.query(
                query_vectors=query_vector,
                top_k=5
            )
            
            matches = []
            for res in results:
                # FIX 2: Use dictionary syntax
                # FIX 3: The docs show the key is "distance", not "score"
                matches.append({
                    "id": res.get('id'), 
                    "score": res.get('distance'), # Mapping distance to score for frontend
                    "metadata": res.get('metadata', {})
                })
            
            print(f"✅ Search found {len(matches)} matches.")
            return matches

        except Exception as e:
            print(f"❌ Search Error: {e}")
            return []