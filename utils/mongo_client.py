from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pymongo import MongoClient, ASCENDING, GEOSPHERE
from pymongo.collection import Collection
from pymongo.database import Database

import sys
sys.path.insert(0, '..')
from config import MONGO_URI, MONGO_DB


class MongoDBClient:
    """Singleton MongoDB client manager."""
    
    _instance: Optional['MongoDBClient'] = None
    _client: Optional[MongoClient] = None
    _db: Optional[Database] = None
    
    def __new__(cls) -> 'MongoDBClient':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._client is None:
            self._client = MongoClient(MONGO_URI)
            self._db = self._client[MONGO_DB]
            self._ensure_indexes()
    
    def _ensure_indexes(self) -> None:
        """Create indexes for efficient queries."""
        # Observations collection indexes
        observations = self._db["observations"]
        observations.create_index([("location", GEOSPHERE)])
        observations.create_index("taxon_key")
        observations.create_index([("source", ASCENDING), ("ingested_at", ASCENDING)])
        
        # Species collection indexes
        species = self._db["species"]
        species.create_index("taxon_key", unique=True)
        species.create_index("scientific_name")
        
        # Audio classifications collection indexes
        audio_classifications = self._db["audio_classifications"]
        audio_classifications.create_index("minio_object_key", unique=True)
        audio_classifications.create_index("classifications.taxon_key")
    
    @property
    def db(self) -> Database:
        """Get the database instance."""
        return self._db
    
    @property
    def client(self) -> MongoClient:
        """Get the MongoDB client instance."""
        return self._client
    
    def get_collection(self, name: str) -> Collection:
        """Get a collection by name."""
        return self._db[name]
    
    def close(self) -> None:
        """Close the MongoDB connection."""
        if self._client:
            self._client.close()
            MongoDBClient._client = None
            MongoDBClient._db = None
            MongoDBClient._instance = None


def get_db() -> Database:
    """Get the MongoDB database instance."""
    return MongoDBClient().db


def get_collection(name: str) -> Collection:
    """Get a MongoDB collection by name."""
    return MongoDBClient().get_collection(name)


def insert_observation(observation: Dict[str, Any]) -> str:
    collection = get_collection("observations")
    
    # Transform to GeoJSON format for geospatial queries
    doc = {
        "taxon_key": observation.get("taxon_key"),
        "location": {
            "type": "Point",
            "coordinates": [
                observation.get("location", {}).get("longitude", 0),
                observation.get("location", {}).get("latitude", 0)
            ]
        },
        "biological_data": observation.get("biological_data", {}),
        "source": observation.get("source", "unknown"),
        "observed_at": observation.get("observed_at", datetime.now(timezone.utc)),
        "ingested_at": datetime.now(timezone.utc)
    }
    
    result = collection.insert_one(doc)
    return str(result.inserted_id)


def insert_observations_batch(observations: List[Dict[str, Any]]) -> List[str]:
    if not observations:
        return []
    
    collection = get_collection("observations")
    
    docs = []
    for obs in observations:
        doc = {
            "taxon_key": obs.get("taxon_key"),
            "location": {
                "type": "Point",
                "coordinates": [
                    obs.get("location", {}).get("longitude", 0),
                    obs.get("location", {}).get("latitude", 0)
                ]
            },
            "biological_data": obs.get("biological_data", {}),
            "source": obs.get("source", "unknown"),
            "observed_at": obs.get("observed_at", datetime.now(timezone.utc)),
            "ingested_at": datetime.now(timezone.utc)
        }
        docs.append(doc)
    
    result = collection.insert_many(docs)
    return [str(id) for id in result.inserted_ids]


def get_observations_by_taxon(taxon_key: str) -> List[Dict[str, Any]]:
    """Get all observations for a specific taxon."""
    collection = get_collection("observations")
    return list(collection.find({"taxon_key": taxon_key}))


def get_observations_count() -> int:
    """Get total count of observations."""
    collection = get_collection("observations")
    return collection.count_documents({})

# Species Collection Functions
def upsert_species(species_data: Dict[str, Any]) -> bool:
    collection = get_collection("species")
    
    # Add timestamp
    species_data["updated_at"] = datetime.now(timezone.utc)
    if "fetched_at" not in species_data:
        species_data["fetched_at"] = datetime.now(timezone.utc)
    
    result = collection.update_one(
        {"taxon_key": species_data["taxon_key"]},
        {"$set": species_data},
        upsert=True
    )
    
    return result.upserted_id is not None


def upsert_species_batch(species_list: List[Dict[str, Any]]) -> Dict[str, int]:
    if not species_list:
        return {"inserted": 0, "updated": 0}
    
    from pymongo import UpdateOne
    
    collection = get_collection("species")
    now = datetime.now(timezone.utc)
    
    operations = []
    for species in species_list:
        species["updated_at"] = now
        if "fetched_at" not in species:
            species["fetched_at"] = now
        
        operations.append(
            UpdateOne(
                {"taxon_key": species["taxon_key"]},
                {"$set": species},
                upsert=True
            )
        )
    
    result = collection.bulk_write(operations, ordered=False)
    
    return {
        "inserted": result.upserted_count,
        "updated": result.modified_count
    }


def get_species_count() -> int:
    """Get total count of species in the database."""
    collection = get_collection("species")
    return collection.count_documents({})


def species_exists(taxon_key: str) -> bool:
    collection = get_collection("species")
    return collection.count_documents({"taxon_key": taxon_key}, limit=1) > 0


def get_species_by_taxon_key(taxon_key: str) -> Optional[Dict[str, Any]]:
    collection = get_collection("species")
    return collection.find_one({"taxon_key": taxon_key})


def get_species_by_scientific_name(scientific_name: str) -> Optional[Dict[str, Any]]:
    collection = get_collection("species")
    # Try exact match on scientific_name first
    result = collection.find_one({"scientific_name": scientific_name})
    if result:
        return result
    # Fall back to canonical_name (handles taxonomy synonyms)
    return collection.find_one({"canonical_name": scientific_name})


def get_all_species() -> List[Dict[str, Any]]:
    """Get all species from the database."""
    collection = get_collection("species")
    return list(collection.find({}))

# Audio Classifications Collection Functions
def insert_audio_classification(classification_data: Dict[str, Any]) -> str:
    collection = get_collection("audio_classifications")
    
    # Transform location to GeoJSON format
    location = classification_data.get("location", {})
    
    doc = {
        "file_name": classification_data.get("file_name"),
        "minio_object_key": classification_data.get("minio_object_key"),
        "minio_bucket": classification_data.get("minio_bucket"),
        "location": {
            "type": "Point",
            "coordinates": [
                location.get("longitude", 0),
                location.get("latitude", 0)
            ]
        },
        "classifications": classification_data.get("classifications", []),
        "api_response": classification_data.get("api_response", {}),
        "log_object_key": classification_data.get("log_object_key"),
        "duration_seconds": classification_data.get("duration_seconds"),
        "processed_at": datetime.now(timezone.utc)
    }
    
    result = collection.insert_one(doc)
    return str(result.inserted_id)


def get_audio_classification(object_key: str) -> Optional[Dict[str, Any]]:
    collection = get_collection("audio_classifications")
    return collection.find_one({"minio_object_key": object_key})


def audio_already_processed(object_key: str) -> bool:
    collection = get_collection("audio_classifications")
    return collection.count_documents({"minio_object_key": object_key}, limit=1) > 0


def audio_file_already_processed(file_name: str) -> bool:
    """Check if an audio file has already been processed by its original file name."""
    collection = get_collection("audio_classifications")
    return collection.count_documents({"file_name": file_name}, limit=1) > 0


def get_audio_classifications_count() -> int:
    """Get total count of audio classifications."""
    collection = get_collection("audio_classifications")
    return collection.count_documents({})


def get_audio_classifications_with_species(taxon_key: str) -> List[Dict[str, Any]]:
    collection = get_collection("audio_classifications")
    return list(collection.find({"classifications.taxon_key": taxon_key}))


# Report Generation Functions
def get_species_with_positive_classifications(min_confidence: float = 0.5) -> List[Dict[str, Any]]:
    collection = get_collection("audio_classifications")
    
    pipeline = [
        # Unwind the classifications array to process each classification separately
        {"$unwind": "$classifications"},
        
        # Filter for classifications meeting the confidence threshold
        {"$match": {"classifications.confidence": {"$gte": min_confidence}}},
        
        # Group by taxon_key and compute statistics
        {"$group": {
            "_id": "$classifications.taxon_key",
            "classification_count": {"$sum": 1},
            "avg_confidence": {"$avg": "$classifications.confidence"},
            "max_confidence": {"$max": "$classifications.confidence"},
            "min_confidence": {"$min": "$classifications.confidence"},
            "locations": {"$addToSet": "$location.coordinates"},
            "scientific_names": {"$addToSet": "$classifications.scientific_name"},
            "audio_files": {"$addToSet": "$minio_object_key"},
        }},
        
        # Lookup species information from the species collection
        {"$lookup": {
            "from": "species",
            "localField": "_id",
            "foreignField": "taxon_key",
            "as": "species_info"
        }},
        
        # Unwind species_info (will be empty array if no match)
        {"$unwind": {
            "path": "$species_info",
            "preserveNullAndEmptyArrays": True
        }},
        
        # Project final shape
        {"$project": {
            "_id": 0,
            "taxon_key": "$_id",
            "classification_count": 1,
            "avg_confidence": {"$round": ["$avg_confidence", 4]},
            "max_confidence": {"$round": ["$max_confidence", 4]},
            "min_confidence": {"$round": ["$min_confidence", 4]},
            "unique_locations": {"$size": "$locations"},
            "audio_file_count": {"$size": "$audio_files"},
            "scientific_name": {
                "$ifNull": ["$species_info.scientific_name", {"$arrayElemAt": ["$scientific_names", 0]}]
            },
            "canonical_name": {"$ifNull": ["$species_info.canonical_name", ""]},
            "family": {"$ifNull": ["$species_info.family", ""]},
            "order": {"$ifNull": ["$species_info.order", ""]},
            "rank": {"$ifNull": ["$species_info.rank", ""]},
        }},
        
        # Sort by classification count descending
        {"$sort": {"classification_count": -1}}
    ]
    
    return list(collection.aggregate(pipeline))


def get_observation_data_for_species(taxon_keys: list) -> Dict[str, Dict[str, Any]]:
    """Get aggregated observation data for given taxon keys from the observations collection."""
    collection = get_collection("observations")
    pipeline = [
        {"$match": {"taxon_key": {"$in": taxon_keys}}},
        {"$group": {
            "_id": "$taxon_key",
            "observation_count": {"$sum": 1},
            "sources": {"$addToSet": "$source"},
            "biological_data_samples": {"$push": "$biological_data"},
        }},
    ]
    results = list(collection.aggregate(pipeline))
    return {r["_id"]: r for r in results}


def get_observations_stats_by_source() -> Dict[str, int]:
    collection = get_collection("observations")
    
    pipeline = [
        {"$group": {
            "_id": "$source",
            "count": {"$sum": 1}
        }}
    ]
    
    results = list(collection.aggregate(pipeline))
    return {r["_id"]: r["count"] for r in results}


def get_all_observations_with_species(source: Optional[str] = None) -> List[Dict[str, Any]]:
    collection = get_collection("observations")
    
    match_stage = {}
    if source:
        match_stage = {"$match": {"source": source}}
    
    pipeline = [
        {"$lookup": {
            "from": "species",
            "localField": "taxon_key",
            "foreignField": "taxon_key",
            "as": "species_info"
        }},
        {"$unwind": {
            "path": "$species_info",
            "preserveNullAndEmptyArrays": True
        }}
    ]
    
    if match_stage:
        pipeline.insert(0, match_stage)
    
    return list(collection.aggregate(pipeline))
