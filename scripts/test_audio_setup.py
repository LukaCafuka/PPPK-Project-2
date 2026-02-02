"""
Test Audio Setup
Creates sample/dummy audio files for testing the audio processing pipeline.

Usage:
    python test_audio_setup.py [num_files]
"""

import os
import sys
import wave
import struct
import random
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, '..')
from config import AUDIO_INPUT_DIR


def create_dummy_wav(file_path: str, duration_seconds: float = 2.0, sample_rate: int = 44100):
    """
    Create a dummy WAV file with random noise.
    
    Args:
        file_path: Path to save the WAV file
        duration_seconds: Duration in seconds
        sample_rate: Sample rate in Hz
    """
    num_samples = int(duration_seconds * sample_rate)
    
    with wave.open(file_path, 'w') as wav_file:
        # Mono, 16-bit audio
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)  # 2 bytes = 16 bits
        wav_file.setframerate(sample_rate)
        
        # Generate random noise (simulating audio)
        for _ in range(num_samples):
            # Random value between -32768 and 32767 (16-bit range)
            # Use low amplitude to simulate quiet audio
            value = random.randint(-3000, 3000)
            data = struct.pack('<h', value)
            wav_file.writeframesraw(data)


def create_dummy_mp3_placeholder(file_path: str):
    """
    Create a placeholder MP3 file.
    Note: This creates a minimal file that won't actually play,
    but can be used to test file handling.
    
    For real testing, use actual bird audio files.
    
    Args:
        file_path: Path to save the file
    """
    # MP3 files require proper encoding which needs external libraries
    # Instead, we'll create a text note explaining this
    with open(file_path, 'wb') as f:
        # Write minimal MP3-like header (won't actually play)
        # This is just for testing file upload functionality
        f.write(b'\xff\xfb\x90\x00')  # MP3 frame header
        f.write(b'\x00' * 1000)  # Padding


def setup_test_audio_files(num_files: int = 5, output_dir: str = None):
    """
    Create test audio files in the specified directory.
    
    Args:
        num_files: Number of test files to create
        output_dir: Output directory (default: from config)
    """
    if output_dir is None:
        output_dir = AUDIO_INPUT_DIR
    
    # Create directory if it doesn't exist
    dir_path = Path(output_dir)
    dir_path.mkdir(parents=True, exist_ok=True)
    
    print(f"Creating {num_files} test audio files in: {dir_path.absolute()}")
    print("-" * 50)
    
    # Bird-related file names for realism
    bird_names = [
        "great_tit_song",
        "blue_tit_call",
        "blackbird_morning",
        "robin_garden",
        "sparrow_chirp",
        "chaffinch_melody",
        "crow_caw",
        "magpie_chatter",
        "woodpecker_drum",
        "owl_hoot",
        "nightingale_night",
        "thrush_song",
        "wren_trill",
        "goldfinch_twitter",
        "starling_mimic",
    ]
    
    created_files = []
    
    for i in range(num_files):
        # Select a bird name (cycle through if num_files > len(bird_names))
        bird_name = bird_names[i % len(bird_names)]
        
        # Alternate between WAV files (which we can create properly)
        file_name = f"{bird_name}_{i+1:03d}.wav"
        file_path = dir_path / file_name
        
        # Create dummy WAV file
        duration = random.uniform(1.5, 5.0)  # Random duration between 1.5 and 5 seconds
        create_dummy_wav(str(file_path), duration_seconds=duration)
        
        file_size = file_path.stat().st_size
        created_files.append((file_name, file_size, duration))
        
        print(f"  Created: {file_name} ({file_size / 1024:.1f} KB, {duration:.1f}s)")
    
    print("-" * 50)
    print(f"\nCreated {len(created_files)} test audio files")
    print(f"Directory: {dir_path.absolute()}")
    print("\nTo process these files, run:")
    print(f"  python process_audio.py --directory \"{dir_path}\"")
    
    return created_files


def main():
    num_files = 5  # Default
    
    if len(sys.argv) > 1:
        try:
            num_files = int(sys.argv[1])
        except ValueError:
            print(f"Invalid number of files: {sys.argv[1]}")
            print("Usage: python test_audio_setup.py [num_files]")
            sys.exit(1)
    
    print("=" * 50)
    print("Test Audio Setup")
    print("=" * 50)
    
    setup_test_audio_files(num_files)
    
    print("\n" + "=" * 50)
    print("NOTE: These are dummy WAV files with random noise.")
    print("For real testing, use actual bird audio recordings.")
    print("The classification API may return no results or errors")
    print("for these dummy files.")
    print("=" * 50)


if __name__ == "__main__":
    main()
