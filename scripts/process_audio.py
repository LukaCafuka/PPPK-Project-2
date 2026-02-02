"""
Process Audio Files
Scans a directory for audio files, uploads them to MinIO, classifies them using
the bird classification API, and stores results in MongoDB.

Learning Outcome 2 (20 points)
"""

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

# Add parent directory to path for imports
sys.path.insert(0, '..')
from config import (
    AVES_CLASSIFY_ENDPOINT,
    AUDIO_INPUT_DIR,
    AUDIO_EXTENSIONS,
    DEFAULT_LOCATION,
    CLASSIFICATION_CONFIDENCE_THRESHOLD,
    MINIO_BUCKET_AUDIO,
    MINIO_BUCKET_LOGS,
)
from utils.minio_client import (
    ensure_buckets,
    upload_file,
    upload_bytes,
    file_exists,
)
from utils.mongo_client import (
    insert_audio_classification,
    audio_already_processed,
    insert_observation,
    get_audio_classifications_count,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
API_TIMEOUT = 60  # seconds
API_RETRY_ATTEMPTS = 3
API_RETRY_DELAY = 2  # seconds


def get_content_type(file_path: str) -> str:
    """Get MIME type based on file extension."""
    ext = Path(file_path).suffix.lower()
    content_types = {
        '.mp3': 'audio/mpeg',
        '.wav': 'audio/wav',
        '.ogg': 'audio/ogg',
        '.flac': 'audio/flac',
        '.m4a': 'audio/mp4',
    }
    return content_types.get(ext, 'application/octet-stream')


def generate_object_key(file_name: str) -> str:
    """Generate a unique object key for MinIO storage."""
    unique_id = str(uuid.uuid4())[:8]
    return f"{unique_id}_{file_name}"


def scan_audio_files(directory: str) -> List[Path]:
    """
    Scan directory for audio files with supported extensions.
    
    Args:
        directory: Path to directory to scan
    
    Returns:
        List of Path objects for audio files found
    """
    audio_files = []
    dir_path = Path(directory)
    
    if not dir_path.exists():
        logger.warning(f"Directory does not exist: {directory}")
        return audio_files
    
    for ext in AUDIO_EXTENSIONS:
        audio_files.extend(dir_path.glob(f"*{ext}"))
        audio_files.extend(dir_path.glob(f"*{ext.upper()}"))
    
    # Deduplicate (needed for case-insensitive file systems like Windows)
    audio_files = list(set(audio_files))
    
    # Sort by name for consistent ordering
    audio_files.sort(key=lambda p: p.name)
    
    return audio_files


def call_classification_api(file_path: str, attempt: int = 1) -> Optional[Dict[str, Any]]:
    """
    Call the bird classification API with an audio file.
    
    Args:
        file_path: Path to the audio file
        attempt: Current attempt number (for retry logic)
    
    Returns:
        API response as dictionary, or None if failed
    """
    try:
        logger.debug(f"Calling classification API for: {file_path} (attempt {attempt})")
        
        with open(file_path, 'rb') as f:
            files = {'file': (Path(file_path).name, f, get_content_type(file_path))}
            response = requests.post(
                AVES_CLASSIFY_ENDPOINT,
                files=files,
                timeout=API_TIMEOUT
            )
        
        response.raise_for_status()
        return response.json()
    
    except requests.Timeout:
        if attempt < API_RETRY_ATTEMPTS:
            logger.warning(f"API timeout, retrying in {API_RETRY_DELAY}s...")
            time.sleep(API_RETRY_DELAY * attempt)
            return call_classification_api(file_path, attempt + 1)
        logger.error(f"API timeout after {API_RETRY_ATTEMPTS} attempts")
        return None
    
    except requests.RequestException as e:
        if attempt < API_RETRY_ATTEMPTS:
            logger.warning(f"API request failed, retrying... ({e})")
            time.sleep(API_RETRY_DELAY * attempt)
            return call_classification_api(file_path, attempt + 1)
        logger.error(f"API request failed after {API_RETRY_ATTEMPTS} attempts: {e}")
        return None
    
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse API response: {e}")
        return None


def create_log_entry(
    file_name: str,
    object_key: str,
    api_response: Optional[Dict[str, Any]],
    location: Dict[str, float],
    error: Optional[str] = None
) -> Dict[str, Any]:
    """Create a log entry for the API request."""
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "file_name": file_name,
        "minio_object_key": object_key,
        "api_endpoint": AVES_CLASSIFY_ENDPOINT,
        "location": location,
        "success": api_response is not None and error is None,
        "error": error,
        "response": api_response
    }


def process_single_file(
    file_path: Path,
    location: Dict[str, float]
) -> Tuple[bool, Optional[str]]:
    """
    Process a single audio file through the pipeline.
    
    Args:
        file_path: Path to the audio file
        location: Geographic location for the recording
    
    Returns:
        Tuple of (success: bool, error_message: Optional[str])
    """
    file_name = file_path.name
    object_key = generate_object_key(file_name)
    log_object_key = f"{object_key.rsplit('.', 1)[0]}.json"
    
    logger.info(f"Processing: {file_name}")
    
    # Step 1: Upload audio file to MinIO
    logger.debug(f"Uploading to MinIO: {object_key}")
    content_type = get_content_type(str(file_path))
    if not upload_file(MINIO_BUCKET_AUDIO, str(file_path), object_key, content_type):
        error = "Failed to upload file to MinIO"
        logger.error(error)
        return False, error
    
    logger.info(f"  Uploaded to MinIO: {MINIO_BUCKET_AUDIO}/{object_key}")
    
    # Step 2: Call classification API
    logger.debug("Calling classification API...")
    api_response = call_classification_api(str(file_path))
    
    if api_response is None:
        error = "Classification API call failed"
        # Still create log entry for failed attempt
        log_entry = create_log_entry(file_name, object_key, None, location, error)
        log_json = json.dumps(log_entry, indent=2, default=str)
        upload_bytes(MINIO_BUCKET_LOGS, log_json.encode('utf-8'), log_object_key, 'application/json')
        return False, error
    
    logger.info(f"  Classification complete: {len(api_response.get('classifications', []))} species detected")
    
    # Step 3: Create and upload log entry
    log_entry = create_log_entry(file_name, object_key, api_response, location)
    log_json = json.dumps(log_entry, indent=2, default=str)
    upload_bytes(MINIO_BUCKET_LOGS, log_json.encode('utf-8'), log_object_key, 'application/json')
    logger.info(f"  Log saved: {MINIO_BUCKET_LOGS}/{log_object_key}")
    
    # Step 4: Store classification result in MongoDB
    classification_data = {
        "file_name": file_name,
        "minio_object_key": object_key,
        "minio_bucket": MINIO_BUCKET_AUDIO,
        "location": location,
        "classifications": api_response.get("classifications", []),
        "api_response": api_response,
        "log_object_key": log_object_key,
        "duration_seconds": api_response.get("duration_seconds"),
    }
    
    doc_id = insert_audio_classification(classification_data)
    logger.info(f"  Saved to MongoDB: {doc_id}")
    
    # Step 5: Create observations for high-confidence classifications
    classifications = api_response.get("classifications", [])
    observations_created = 0
    
    for classification in classifications:
        confidence = classification.get("confidence", 0)
        if confidence >= CLASSIFICATION_CONFIDENCE_THRESHOLD:
            observation = {
                "taxon_key": str(classification.get("taxon_key", "")),
                "location": location,
                "source": "audio_classification",
                "biological_data": {
                    "classification_confidence": confidence,
                    "audio_file": object_key,
                    "scientific_name": classification.get("scientific_name", ""),
                },
                "observed_at": datetime.now(timezone.utc),
            }
            insert_observation(observation)
            observations_created += 1
    
    if observations_created > 0:
        logger.info(f"  Created {observations_created} observation(s)")
    
    return True, None


def process_audio_directory(
    directory: str = None,
    location: Dict[str, float] = None
) -> Dict[str, int]:
    """
    Process all audio files in a directory.
    
    Args:
        directory: Path to audio files directory (default: from config)
        location: Geographic location for all files (default: from config)
    
    Returns:
        Summary dictionary with counts
    """
    if directory is None:
        directory = AUDIO_INPUT_DIR
    
    if location is None:
        location = DEFAULT_LOCATION
    
    # Ensure MinIO buckets exist
    ensure_buckets()
    
    # Scan for audio files
    audio_files = scan_audio_files(directory)
    
    if not audio_files:
        logger.warning(f"No audio files found in: {directory}")
        return {"total": 0, "processed": 0, "skipped": 0, "failed": 0}
    
    logger.info(f"Found {len(audio_files)} audio file(s) in {directory}")
    
    summary = {
        "total": len(audio_files),
        "processed": 0,
        "skipped": 0,
        "failed": 0
    }
    
    for i, file_path in enumerate(audio_files, 1):
        logger.info(f"\n[{i}/{len(audio_files)}] {file_path.name}")
        
        # Check if already processed
        object_key = generate_object_key(file_path.name)
        # Note: We can't check exact object key since it contains UUID
        # For now, process all files (could be enhanced with hash-based dedup)
        
        success, error = process_single_file(file_path, location)
        
        if success:
            summary["processed"] += 1
        else:
            summary["failed"] += 1
            logger.error(f"  Failed: {error}")
    
    return summary


def main():
    """Main entry point for the audio processing script."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Process audio files for bird classification")
    parser.add_argument(
        "--directory", "-d",
        default=AUDIO_INPUT_DIR,
        help=f"Directory containing audio files (default: {AUDIO_INPUT_DIR})"
    )
    parser.add_argument(
        "--latitude", "-lat",
        type=float,
        default=DEFAULT_LOCATION["latitude"],
        help=f"Latitude for recordings (default: {DEFAULT_LOCATION['latitude']})"
    )
    parser.add_argument(
        "--longitude", "-lon",
        type=float,
        default=DEFAULT_LOCATION["longitude"],
        help=f"Longitude for recordings (default: {DEFAULT_LOCATION['longitude']})"
    )
    
    args = parser.parse_args()
    
    location = {
        "latitude": args.latitude,
        "longitude": args.longitude
    }
    
    logger.info("=" * 60)
    logger.info("Process Audio Files for Bird Classification")
    logger.info("=" * 60)
    logger.info(f"Directory: {args.directory}")
    logger.info(f"Location: ({location['latitude']}, {location['longitude']})")
    
    # Get initial count
    initial_count = get_audio_classifications_count()
    logger.info(f"Initial audio classifications in MongoDB: {initial_count}")
    
    # Process files
    summary = process_audio_directory(args.directory, location)
    
    # Get final count
    final_count = get_audio_classifications_count()
    
    logger.info("\n" + "=" * 60)
    logger.info("Processing Complete!")
    logger.info("=" * 60)
    logger.info(f"Total files found: {summary['total']}")
    logger.info(f"Successfully processed: {summary['processed']}")
    logger.info(f"Skipped (already processed): {summary['skipped']}")
    logger.info(f"Failed: {summary['failed']}")
    logger.info(f"Audio classifications in database: {final_count} (added {final_count - initial_count})")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
