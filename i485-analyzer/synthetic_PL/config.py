"""Top-level config for synthetic data generators."""
import os

SEED = 42
DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "synthetic_data",
)
