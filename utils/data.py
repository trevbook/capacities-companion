"""
This module contains utility functions related to parsing the Capacities data.
"""

# =====
# SETUP
# =====
# Below, we'll set up the rest of the file.

# General imports
import zipfile
from pathlib import Path

# Third-party imports
import yaml
import pandas as pd


# =======
# METHODS
# =======
# Next up: we'll define various data processing methods.


def parse_capacities_export_zip(zip_path: str) -> pd.DataFrame:
    """
    Parse a ZIP file containing data from a Capacities export.

    Args:
        zip_path (str): Path to the ZIP file

    Returns:
        pd.DataFrame: DataFrame with columns [object_type, object_type_label, title, properties, text_content]
    """
    data = []
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        for md_file in [f for f in zip_ref.namelist() if f.endswith(".md")]:
            content = zip_ref.read(md_file).decode("utf-8")
            parts = content.split("---", 2)

            if len(parts) >= 3:
                try:
                    properties = yaml.safe_load(parts[1])
                except:
                    properties = {}

                data.append(
                    {
                        "object_type": properties.get("type", ""),
                        "title": properties.get("title", Path(md_file).stem),
                        "properties": properties,
                        "text_content": parts[2].strip(),
                    }
                )

    return pd.DataFrame(data)
