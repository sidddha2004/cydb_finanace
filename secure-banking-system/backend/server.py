import flwr as fl
import time

# Define strategy
strategy = fl.server.strategy.FedAvg(
    fraction_fit=1.0,
    fraction_evaluate=1.0,
    min_fit_clients=1,
    min_evaluate_clients=1,
    min_available_clients=1,
)

def start_aggregator():
    print("🚀 Central Aggregator (The Consortium) starting on port 8080...")
    
    # --- THE FIX: Infinite Loop ---
    while True:
        try:
            print("\n--- STARTING NEW FEDERATED SESSION (Waiting for Banks) ---")
            fl.server.start_server(
                server_address="0.0.0.0:8080",
                config=fl.server.ServerConfig(num_rounds=5), # Keep rounds low (e.g., 5) for quick demos
                strategy=strategy,
            )
        except Exception as e:
            print(f"Server error: {e}")
            
        print("Session Complete. Restarting in 5 seconds...")
        time.sleep(5) 

if __name__ == "__main__":
    start_aggregator()