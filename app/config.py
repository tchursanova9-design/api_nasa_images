import os

SEARCH_URL = "https://images-api.nasa.gov/search"
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "30"))
QUERY = os.getenv("QUERY", "").strip()


LOCAL_FILENAME = "nasa_image.jpg"

KAFKA_BOOTSTRAP = (
    os.getenv("KAFKA_BOOTSTRAP")
    or os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    or "kafka:9092"
)


DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "app_db")
DB_USER = os.getenv("DB_USER", "app")
DB_PASSWORD = os.getenv("DB_PASSWORD", "app_password")

