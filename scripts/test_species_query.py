"""
Test script to verify species data in MongoDB.
Displays 100 species to confirm data is correctly stored.
"""

import sys
sys.path.insert(0, '..')

from utils.mongo_client import get_collection, get_species_count

def main():
    print("=" * 70)
    print("Species Data Verification")
    print("=" * 70)
    
    # Get count
    count = get_species_count()
    print(f"\nTotal species in database: {count}")
    
    # Fetch 100 species
    collection = get_collection("species")
    species_list = list(collection.find().limit(100))
    
    print(f"Fetched {len(species_list)} species (limit 100)")
    print("\n" + "-" * 70)
    print(f"{'#':<4} {'Taxon Key':<12} {'Scientific Name':<40} {'Family':<15}")
    print("-" * 70)
    
    for i, species in enumerate(species_list, 1):
        taxon_key = species.get('taxon_key', 'N/A')[:10]
        scientific_name = species.get('scientific_name', 'N/A')[:38]
        family = species.get('family', 'N/A')[:13]
        print(f"{i:<4} {taxon_key:<12} {scientific_name:<40} {family:<15}")
    
    print("-" * 70)
    print(f"\nDisplayed {len(species_list)} of {count} total species")
    print("=" * 70)


if __name__ == "__main__":
    main()
