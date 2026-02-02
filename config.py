"""
Configuration settings for the Bird Pipeline project.
"""

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "10.0.0.42:29092"
KAFKA_TOPIC = "bird-observations"
KAFKA_GROUP_ID = "bird-pipeline-consumer"

# MongoDB Configuration
MONGO_URI = "mongodb://admin:adminpassword@10.0.0.42:27017/"
MONGO_DB = "bird_pipeline"

# MinIO Configuration
MINIO_ENDPOINT = "10.0.0.42:9110"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
MINIO_SECURE = False
MINIO_BUCKET_AUDIO = "audio-files"
MINIO_BUCKET_LOGS = "classification-logs"

# API Configuration
AVES_API_BASE_URL = "https://aves.regoch.net"
AVES_CLASSIFY_ENDPOINT = f"{AVES_API_BASE_URL}/api/classify"

# Audio Processing Configuration
AUDIO_INPUT_DIR = "./audio_files"
AUDIO_EXTENSIONS = [".mp3", ".wav", ".ogg", ".flac", ".m4a"]
CLASSIFICATION_CONFIDENCE_THRESHOLD = 0.5  # Minimum confidence to create observation

# Default location for audio files (Zagreb, Croatia)
DEFAULT_LOCATION = {
    "latitude": 45.8150,
    "longitude": 15.9819
}

# Report Generation Configuration
REPORT_OUTPUT_DIR = "./reports"
DEFAULT_FUZZY_THRESHOLD = 70  # Fuzzy matching threshold (0-100) for species name filtering
