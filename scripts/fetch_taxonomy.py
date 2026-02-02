"""
Fetch Bird Taxonomy
Fetches bird species taxonomy data from aves.regoch.net and stores in MongoDB.

Learning Outcome 3 (Minimal - 10 points)
"""

import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

# Add parent directory to path for imports
sys.path.insert(0, '..')
from config import AVES_API_BASE_URL
from utils.mongo_client import (
    upsert_species_batch,
    get_species_count,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2

# The website loads data from a JSON file
AVES_JSON_URL = f"{AVES_API_BASE_URL}/aves.json"


def fetch_species_json(attempt: int = 1) -> Optional[List[Dict[str, Any]]]:
    """
    Fetch species data from the JSON endpoint.
    
    Args:
        attempt: Current attempt number
    
    Returns:
        List of species dictionaries or None if fetch fails
    """
    try:
        logger.info(f"Fetching species data from: {AVES_JSON_URL}")
        response = requests.get(AVES_JSON_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Successfully fetched {len(data)} species from JSON")
        return data
    
    except requests.RequestException as e:
        if attempt < RETRY_ATTEMPTS:
            logger.warning(f"Request failed, retrying in {RETRY_DELAY}s... ({e})")
            time.sleep(RETRY_DELAY * attempt)  # Exponential backoff
            return fetch_species_json(attempt + 1)
        else:
            logger.error(f"Failed to fetch {AVES_JSON_URL} after {RETRY_ATTEMPTS} attempts: {e}")
            return None
    except ValueError as e:
        logger.error(f"Failed to parse JSON response: {e}")
        return None


def transform_species(raw_species: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform raw JSON species data to our schema.
    
    The JSON contains fields like:
    - key: taxon key (GBIF identifier)
    - scientificName: full scientific name with author
    - canonicalName: scientific name without author
    - rank: taxonomic rank (e.g., "SPECIES")
    - family: family name
    - order: order name
    
    Args:
        raw_species: Raw species data from JSON
    
    Returns:
        Transformed species dictionary
    """
    return {
        "taxon_key": str(raw_species.get("key", "")),
        "scientific_name": raw_species.get("scientificName", ""),
        "canonical_name": raw_species.get("canonicalName", ""),
        "rank": raw_species.get("rank", ""),
        "family": raw_species.get("family", ""),
        "order": raw_species.get("order", ""),
        "fetched_at": datetime.now(timezone.utc)
    }


def fetch_all_species() -> List[Dict[str, Any]]:
    """
    Fetch all species from the JSON endpoint and transform them.
    
    Returns:
        List of transformed species dictionaries
    """
    raw_data = fetch_species_json()
    
    if raw_data is None:
        return []
    
    # Transform each species
    species_list = []
    for raw_species in raw_data:
        species = transform_species(raw_species)
        
        # Only include if we have a valid taxon_key
        if species["taxon_key"]:
            species_list.append(species)
        else:
            logger.warning(f"Skipping species without key: {raw_species}")
    
    return species_list


def main():
    """Main entry point for the taxonomy fetch script."""
    logger.info("=" * 60)
    logger.info("Fetch Bird Taxonomy from aves.regoch.net")
    logger.info("=" * 60)
    
    # Get initial count
    initial_count = get_species_count()
    logger.info(f"Initial species count in MongoDB: {initial_count}")
    
    # Check if data already exists
    if initial_count > 0:
        logger.info("Species data already exists in database")
        logger.info("Proceeding to update/add new species (upsert mode)")
    
    try:
        # Fetch all species from JSON
        logger.info(f"Fetching data from: {AVES_API_BASE_URL}")
        species_list = fetch_all_species()
        
        if not species_list:
            logger.warning("No species data fetched!")
            return
        
        logger.info(f"Fetched {len(species_list)} species total")
        
        # Store in MongoDB with upsert
        logger.info("Storing species in MongoDB...")
        result = upsert_species_batch(species_list)
        
        # Get final count
        final_count = get_species_count()
        
        logger.info("=" * 60)
        logger.info("Taxonomy fetch complete!")
        logger.info(f"Species fetched: {len(species_list)}")
        logger.info(f"New species inserted: {result['inserted']}")
        logger.info(f"Existing species updated: {result['updated']}")
        logger.info(f"Total species in database: {final_count}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Failed to fetch taxonomy: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
