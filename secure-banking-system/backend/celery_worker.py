from celery import Celery
import ml_logic

# Configure Celery to use Redis
celery_app = Celery(
    "banking_tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0"
)

@celery_app.task(name="run_fl_client")
def run_fl_task():
    result = ml_logic.start_federated_client()
    return result