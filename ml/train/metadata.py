"""Writes a metadata JSON file alongside each trained model artifact.

The metadata records exactly what produced the model -- data source, split
sizes, seed, features, and the metrics that were actually computed -- so a
model file is never just an opaque blob with no record of how it got there.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def write_metadata(
    model_path: Path,
    *,
    model_name: str,
    model_type: str,
    data_source: str,
    training_rows: int,
    test_rows: int,
    random_seed: int,
    features: list[str],
    metrics: dict[str, Any],
) -> Path:
    """Writes `<model_path stem>.metadata.json` next to model_path and
    returns the path written to."""
    metadata = {
        "model_name": model_name,
        "model_type": model_type,
        "data_source": data_source,
        "training_rows": training_rows,
        "test_rows": test_rows,
        "random_seed": random_seed,
        "features": features,
        "metrics": metrics,
        "trained_at": datetime.now(timezone.utc).isoformat(),
    }

    out_path = model_path.with_suffix(".metadata.json")
    out_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
    return out_path
