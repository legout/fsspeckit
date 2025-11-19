import tempfile
import os
import json


def create_sample_data_file(tmpdir):
    """Creates a sample JSON file for caching example."""
    sample_file = os.path.join(tmpdir, "sample_data.json")
    sample_data = {
        "name": "fsspeckit caching example",
        "timestamp": 1678886400.0,  # Fixed timestamp for reproducibility
        "items": [{"id": i, "value": f"item_{i}"} for i in range(100)],
    }
    with open(sample_file, "w") as f:
        json.dump(sample_data, f)
    print(f"Created sample file: {sample_file}")
    return sample_file
