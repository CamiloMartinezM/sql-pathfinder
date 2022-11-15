import sys
from pathlib import Path

# Add working directory in PATH
project_dir = Path(__file__).resolve().parents[2]
file_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(file_dir))