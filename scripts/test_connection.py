from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env'))
engine = create_engine(f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")

try:
    conn = engine.connect()
    print("[INFO] Connection successful!")
    conn.close()
except Exception as e:
    print(f"[ERROR] Connection failed: {e}")