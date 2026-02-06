import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from rapidfuzz import fuzz

# Add parent directory to path for imports
sys.path.insert(0, '..')
from config import (
    CLASSIFICATION_CONFIDENCE_THRESHOLD,
    REPORT_OUTPUT_DIR,
    DEFAULT_FUZZY_THRESHOLD,
)
from utils.mongo_client import (
    get_species_with_positive_classifications,
    get_observation_data_for_species,
    get_species_count,
    get_audio_classifications_count,
    get_observations_count,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fuzzy_filter_species(
    species_list: List[Dict[str, Any]], 
    query: str, 
    threshold: int = 70
) -> List[Dict[str, Any]]:
    if not query or not query.strip():
        return species_list
    
    query_lower = query.lower().strip()
    results = []
    
    for species in species_list:
        # Get all name variants to match against
        names = [
            species.get("scientific_name", ""),
            species.get("canonical_name", ""),
        ]
        
        # Filter out empty names and calculate best match score
        valid_names = [n for n in names if n]
        if not valid_names:
            continue
        
        # Use partial_ratio for substring matching (more forgiving)
        best_score = max(fuzz.partial_ratio(query_lower, n.lower()) for n in valid_names)
        
        if best_score >= threshold:
            # Add the match score for potential sorting/display
            species_with_score = species.copy()
            species_with_score["_fuzzy_score"] = best_score
            results.append(species_with_score)
    
    # Sort by fuzzy score descending, then by classification count
    results.sort(key=lambda x: (-x.get("_fuzzy_score", 0), -x.get("classification_count", 0)))
    
    return results


def enrich_with_observation_data(species_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Enrich species classification data with observation data from MongoDB (including Kafka)."""
    if not species_data:
        return species_data

    taxon_keys = [s.get("taxon_key", "") for s in species_data if s.get("taxon_key")]
    if not taxon_keys:
        return species_data

    obs_data = get_observation_data_for_species(taxon_keys)

    for species in species_data:
        tk = species.get("taxon_key", "")
        obs = obs_data.get(tk)
        if obs:
            species["total_observation_count"] = obs.get("observation_count", 0)
            species["observation_sources"] = ", ".join(sorted(obs.get("sources", [])))

            # Merge biological data fields across all observations
            bio_samples = obs.get("biological_data_samples", [])
            merged_bio: Dict[str, Any] = {}
            for sample in bio_samples:
                if not isinstance(sample, dict):
                    continue
                for key, value in sample.items():
                    # Skip internal audio-classification fields
                    if key in ("classification_confidence", "audio_file", "scientific_name"):
                        continue
                    if key not in merged_bio:
                        merged_bio[key] = set() if isinstance(value, (str, bool)) else []
                    if isinstance(value, (str, bool)):
                        merged_bio[key].add(str(value))
                    else:
                        merged_bio[key].append(value)

            # Flatten merged bio data into the species dict
            for key, values in merged_bio.items():
                if isinstance(values, set):
                    species[f"bio_{key}"] = "; ".join(sorted(values))
                else:
                    # For numeric values, take the average
                    numeric_vals = [v for v in values if isinstance(v, (int, float))]
                    if numeric_vals:
                        species[f"bio_{key}"] = round(sum(numeric_vals) / len(numeric_vals), 2)
                    else:
                        species[f"bio_{key}"] = "; ".join(str(v) for v in values)
        else:
            species["total_observation_count"] = 0
            species["observation_sources"] = ""

    return species_data


def clean_and_transform_data(raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
    if not raw_data:
        logger.warning("No data to transform")
        return pd.DataFrame()
    
    # Convert to DataFrame
    df = pd.DataFrame(raw_data)
    
    if 'taxon_key' in df.columns:
        missing_tk = df['taxon_key'].isna() | (df['taxon_key'] == '')
        if missing_tk.any():
            logger.warning(f"{missing_tk.sum()} record(s) have no taxon_key (species not found in taxonomy DB)")
            # Fill missing taxon_key with scientific_name as fallback identifier
            df.loc[missing_tk, 'taxon_key'] = df.loc[missing_tk, 'scientific_name']
    
    # Remove internal fuzzy score column if present
    if '_fuzzy_score' in df.columns:
        df = df.drop(columns=['_fuzzy_score'])
    
    # Normalize string columns (strip whitespace)
    string_columns = ['scientific_name', 'canonical_name', 'family', 'order', 'rank']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str).str.strip()
    
    # Round confidence values to 2 decimal places
    confidence_columns = ['avg_confidence', 'max_confidence', 'min_confidence']
    for col in confidence_columns:
        if col in df.columns:
            df[col] = df[col].round(2)
    
    # Ensure numeric columns have proper types
    numeric_columns = ['classification_count', 'unique_locations', 'audio_file_count', 'total_observation_count']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    # Normalize observation_sources string column
    if 'observation_sources' in df.columns:
        df['observation_sources'] = df['observation_sources'].fillna('').astype(str).str.strip()
    
    # Sort by classification count descending
    df = df.sort_values('classification_count', ascending=False)
    
    # Reorder columns for better readability
    column_order = [
        'taxon_key',
        'scientific_name',
        'canonical_name',
        'family',
        'order',
        'rank',
        'classification_count',
        'avg_confidence',
        'max_confidence',
        'min_confidence',
        'unique_locations',
        'audio_file_count',
        'total_observation_count',
        'observation_sources',
    ]
    
    # Only include columns that exist
    final_columns = [col for col in column_order if col in df.columns]
    
    # Append any bio_ columns dynamically
    bio_columns = sorted([col for col in df.columns if col.startswith('bio_')])
    final_columns.extend(bio_columns)
    
    df = df[final_columns]
    
    return df


def generate_csv_report(df: pd.DataFrame, output_path: str) -> bool:
    try:
        # Ensure output directory exists
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Export to CSV
        df.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"CSV report saved to: {output_path}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to save CSV report: {e}")
        return False


def print_summary_statistics(df: pd.DataFrame) -> None:
    """Print summary statistics for the report."""
    if df.empty:
        logger.info("No data to summarize")
        return
    
    total_species = len(df)
    total_classifications = df['classification_count'].sum()
    avg_confidence = df['avg_confidence'].mean()
    
    logger.info("\n" + "=" * 50)
    logger.info("Report Summary Statistics")
    logger.info("=" * 50)
    logger.info(f"Total species with positive classifications: {total_species}")
    logger.info(f"Total positive classifications: {total_classifications}")
    logger.info(f"Average confidence across all species: {avg_confidence:.2f}")
    
    if total_species > 0:
        # Top 5 species by classification count
        logger.info("\nTop 5 species by classification count:")
        top_5 = df.head(5)
        for _, row in top_5.iterrows():
            name = row['scientific_name'] or row['canonical_name'] or row['taxon_key']
            logger.info(f"  - {name}: {row['classification_count']} classifications (avg conf: {row['avg_confidence']:.2f})")
    
    logger.info("=" * 50)


def main():
    """Main entry point for the report generation script."""
    parser = argparse.ArgumentParser(
        description="Generate CSV report for bird species with positive classifications"
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help=f"Output CSV file path (default: {REPORT_OUTPUT_DIR}/bird_report.csv)"
    )
    parser.add_argument(
        "--species-filter", "-f",
        default=None,
        help="Fuzzy filter for species name (optional)"
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=CLASSIFICATION_CONFIDENCE_THRESHOLD,
        help=f"Minimum classification confidence (default: {CLASSIFICATION_CONFIDENCE_THRESHOLD})"
    )
    parser.add_argument(
        "--fuzzy-threshold",
        type=int,
        default=DEFAULT_FUZZY_THRESHOLD,
        help=f"Fuzzy matching threshold 0-100 (default: {DEFAULT_FUZZY_THRESHOLD})"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Determine output path
    if args.output:
        output_path = args.output
    else:
        # Generate timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(REPORT_OUTPUT_DIR, f"bird_report_{timestamp}.csv")
    
    logger.info("=" * 60)
    logger.info("Generate Bird Classification Report")
    logger.info("=" * 60)
    logger.info(f"Output path: {output_path}")
    logger.info(f"Minimum confidence: {args.min_confidence}")
    if args.species_filter:
        logger.info(f"Species filter: '{args.species_filter}' (threshold: {args.fuzzy_threshold})")
    
    # Log database statistics
    logger.info("\nDatabase Statistics:")
    logger.info(f"  Total species in database: {get_species_count()}")
    logger.info(f"  Total audio classifications: {get_audio_classifications_count()}")
    logger.info(f"  Total observations: {get_observations_count()}")
    
    # Step 1: Query species with positive classifications
    logger.info(f"\nQuerying species with classifications (confidence >= {args.min_confidence})...")
    species_data = get_species_with_positive_classifications(min_confidence=args.min_confidence)
    logger.info(f"Found {len(species_data)} species with positive classifications")

    if species_data:
        logger.info("Enriching with observation data...")
        species_data = enrich_with_observation_data(species_data)
    
    if not species_data:
        logger.warning("No species found with positive classifications.")
        # Create empty CSV with headers for Snakemake compatibility
        empty_df = pd.DataFrame(columns=[
            'taxon_key', 'scientific_name', 'canonical_name', 'family', 'order',
            'rank', 'classification_count', 'avg_confidence', 'max_confidence',
            'min_confidence', 'unique_locations', 'audio_file_count',
            'total_observation_count', 'observation_sources'
        ])
        generate_csv_report(empty_df, output_path)
        logger.info(f"Created empty report at: {output_path}")
        return
    
    # Step 2: Apply fuzzy filter if specified
    if args.species_filter:
        logger.info(f"\nApplying fuzzy filter: '{args.species_filter}'...")
        filtered_data = fuzzy_filter_species(
            species_data, 
            args.species_filter, 
            threshold=args.fuzzy_threshold
        )
        logger.info(f"Filtered to {len(filtered_data)} species matching '{args.species_filter}'")
        species_data = filtered_data
        
        if not species_data:
            logger.warning(f"No species found matching '{args.species_filter}'. Try lowering --fuzzy-threshold.")
            # Create empty CSV for Snakemake compatibility
            empty_df = pd.DataFrame(columns=[
                'taxon_key', 'scientific_name', 'canonical_name', 'family', 'order',
                'rank', 'classification_count', 'avg_confidence', 'max_confidence',
                'min_confidence', 'unique_locations', 'audio_file_count',
                'total_observation_count', 'observation_sources'
            ])
            generate_csv_report(empty_df, output_path)
            return
    
    # Step 3: Clean and transform data
    logger.info("\nCleaning and transforming data...")
    df = clean_and_transform_data(species_data)
    
    if df.empty:
        logger.warning("No data after cleaning.")
        # Create empty CSV for Snakemake compatibility
        empty_df = pd.DataFrame(columns=[
            'taxon_key', 'scientific_name', 'canonical_name', 'family', 'order',
            'rank', 'classification_count', 'avg_confidence', 'max_confidence',
            'min_confidence', 'unique_locations', 'audio_file_count',
            'total_observation_count', 'observation_sources'
        ])
        generate_csv_report(empty_df, output_path)
        return
    
    logger.info(f"Final dataset: {len(df)} species")
    
    # Step 4: Generate CSV report
    logger.info("\nGenerating CSV report...")
    success = generate_csv_report(df, output_path)
    
    if success:
        # Print summary statistics
        print_summary_statistics(df)
        
        logger.info("\n" + "=" * 60)
        logger.info("Report Generation Complete!")
        logger.info("=" * 60)
        logger.info(f"Report saved to: {output_path}")
        logger.info(f"Total rows: {len(df)}")
    else:
        logger.error("Failed to generate report")
        sys.exit(1)


if __name__ == "__main__":
    main()
