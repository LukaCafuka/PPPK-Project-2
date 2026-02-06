"""
Snakemake workflow for the PPPK project.

Usage:
    # Run full pipeline with defaults
    snakemake --cores 1

    # Dry run to see what would execute
    snakemake -n --cores 1

    # Override parameters
    snakemake --cores 1 --config audio_dir=./my_audio species_filter="sparrow"

    # Run specific rule
    snakemake fetch_taxonomy --cores 1

    # Force re-run of a rule
    snakemake --cores 1 --forcerun process_audio
"""

# Load configuration from YAML file
configfile: "config/snakemake.yaml"

# Configuration with defaults
# Audio processing parameters
AUDIO_DIR = config.get("audio_dir", "scripts/audio_files")
LATITUDE = config.get("latitude", 45.815)
LONGITUDE = config.get("longitude", 15.9819)

# Report generation parameters
REPORT_OUTPUT = config.get("report_output", "reports/bird_report.csv")
MIN_CONFIDENCE = config.get("min_confidence", 0.5)
FUZZY_THRESHOLD = config.get("fuzzy_threshold", 70)
SPECIES_FILTER = config.get("species_filter", "")

# Target rule - runs the complete pipeline
rule all:
    """Run the complete PPPK Project pipeline."""
    input:
        REPORT_OUTPUT,
        ".snakemake_checkpoints/kafka_done"


# Pipeline Rules
rule fetch_taxonomy:
    """
    Fetch bird species taxonomy from aves.regoch.net and store in MongoDB.
    """
    output:
        touch(".snakemake_checkpoints/taxonomy_done")
    log:
        "logs/fetch_taxonomy.log"
    run:
        import subprocess
        import sys
        import os
        
        # Ensure logs directory exists
        os.makedirs("logs", exist_ok=True)
        
        # Run script with current directory in PYTHONPATH
        env = os.environ.copy()
        env["PYTHONPATH"] = os.getcwd()
        
        with open(log[0], "w") as logfile:
            result = subprocess.run(
                [sys.executable, "scripts/fetch_taxonomy.py"],
                env=env,
                stdout=logfile,
                stderr=subprocess.STDOUT
            )
        
        # Print log to console as well
        with open(log[0], "r") as logfile:
            print(logfile.read())
        
        if result.returncode != 0:
            raise Exception(f"fetch_taxonomy failed with return code {result.returncode}")


rule consume_kafka:
    """
    Consume bird observation messages from Kafka and store in MongoDB.
    This step can run independently of other steps.
    """
    output:
        touch(".snakemake_checkpoints/kafka_done")
    log:
        "logs/consume_kafka.log"
    run:
        import subprocess
        import sys
        import os
        
        # Ensure logs directory exists
        os.makedirs("logs", exist_ok=True)
        
        # Run script with current directory in PYTHONPATH
        env = os.environ.copy()
        env["PYTHONPATH"] = os.getcwd()
        
        with open(log[0], "w") as logfile:
            result = subprocess.run(
                [sys.executable, "scripts/consume_kafka.py"],
                env=env,
                stdout=logfile,
                stderr=subprocess.STDOUT
            )
        
        # Print log to console as well
        with open(log[0], "r") as logfile:
            print(logfile.read())
        
        if result.returncode != 0:
            raise Exception(f"consume_kafka failed with return code {result.returncode}")


rule process_audio:
    """
    Process audio files: upload to MinIO, classify with API, store results.
    Depends on taxonomy being available for species linking.
    
    Parameters (via --config):
        audio_dir: Directory containing audio files
        latitude: Geographic latitude for recordings
        longitude: Geographic longitude for recordings
    """
    input:
        ".snakemake_checkpoints/taxonomy_done"
    output:
        touch(".snakemake_checkpoints/audio_done")
    params:
        directory=AUDIO_DIR,
        latitude=LATITUDE,
        longitude=LONGITUDE
    log:
        "logs/process_audio.log"
    run:
        import subprocess
        import sys
        import os
        
        # Ensure logs directory exists
        os.makedirs("logs", exist_ok=True)
        
        # Run script with current directory in PYTHONPATH
        env = os.environ.copy()
        env["PYTHONPATH"] = os.getcwd()
        
        cmd = [
            sys.executable, "scripts/process_audio.py",
            "--directory", str(params.directory),
            "--latitude", str(params.latitude),
            "--longitude", str(params.longitude)
        ]
        
        with open(log[0], "w") as logfile:
            result = subprocess.run(
                cmd,
                env=env,
                stdout=logfile,
                stderr=subprocess.STDOUT
            )
        
        # Print log to console as well
        with open(log[0], "r") as logfile:
            print(logfile.read())
        
        if result.returncode != 0:
            raise Exception(f"process_audio failed with return code {result.returncode}")


rule generate_report:
    """
    Generate CSV report for species with positive classifications.
    Depends on audio processing being complete.
    
    Parameters (via --config):
        report_output: Output CSV file path
        species_filter: Fuzzy filter for species name (optional)
        min_confidence: Minimum classification confidence
        fuzzy_threshold: Fuzzy matching threshold (0-100)
    """
    input:
        ".snakemake_checkpoints/audio_done",
        ".snakemake_checkpoints/kafka_done"
    output:
        REPORT_OUTPUT
    params:
        species_filter=SPECIES_FILTER,
        min_confidence=MIN_CONFIDENCE,
        fuzzy_threshold=FUZZY_THRESHOLD
    log:
        "logs/generate_report.log"
    run:
        import subprocess
        import sys
        import os
        
        # Ensure logs and reports directories exist
        os.makedirs("logs", exist_ok=True)
        os.makedirs(os.path.dirname(output[0]) or ".", exist_ok=True)
        
        # Run script with current directory in PYTHONPATH
        env = os.environ.copy()
        env["PYTHONPATH"] = os.getcwd()
        
        cmd = [
            sys.executable, "scripts/generate_report.py",
            "-o", output[0],
            "--min-confidence", str(params.min_confidence),
            "--fuzzy-threshold", str(params.fuzzy_threshold)
        ]
        
        # Add species filter if specified
        if params.species_filter:
            cmd.extend(["--species-filter", params.species_filter])
        
        with open(log[0], "w") as logfile:
            result = subprocess.run(
                cmd,
                env=env,
                stdout=logfile,
                stderr=subprocess.STDOUT
            )
        
        # Print log to console as well
        with open(log[0], "r") as logfile:
            print(logfile.read())
        
        if result.returncode != 0:
            raise Exception(f"generate_report failed with return code {result.returncode}")


# Utility Rules
rule clean:
    """Remove all generated files and checkpoints."""
    run:
        import shutil
        import os
        
        dirs_to_clean = [".snakemake_checkpoints", "reports", "logs"]
        for d in dirs_to_clean:
            if os.path.exists(d):
                shutil.rmtree(d)
                print(f"Removed {d}/")
            else:
                print(f"{d}/ does not exist, skipping")
        
        print("Cleaned checkpoints, reports, and logs")


rule clean_checkpoints:
    """Remove only checkpoint files (allows re-running pipeline)."""
    run:
        import shutil
        import os
        
        if os.path.exists(".snakemake_checkpoints"):
            shutil.rmtree(".snakemake_checkpoints")
            print("Removed .snakemake_checkpoints/")
        else:
            print(".snakemake_checkpoints/ does not exist, skipping")
        
        print("Cleaned checkpoints - pipeline will re-run from start")


rule status:
    """Show current pipeline status."""
    run:
        import os
        
        checkpoints = [
            (".snakemake_checkpoints/taxonomy_done", "Taxonomy fetched"),
            (".snakemake_checkpoints/kafka_done", "Kafka consumed"),
            (".snakemake_checkpoints/audio_done", "Audio processed"),
        ]
        
        print("\n" + "=" * 50)
        print("PPPK Project Pipeline Status")
        print("=" * 50)
        
        for checkpoint, desc in checkpoints:
            status = "DONE" if os.path.exists(checkpoint) else "PENDING"
            print(f"  [{status:7}] {desc}")
        
        report_status = "DONE" if os.path.exists(REPORT_OUTPUT) else "PENDING"
        print(f"  [{report_status:7}] Report generated: {REPORT_OUTPUT}")
        
        print("=" * 50 + "\n")
