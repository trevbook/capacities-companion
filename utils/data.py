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
                        "file_name": Path(md_file).name,
                    }
                )

    # Add the date column from `properties`, and make it a datetime
    for entry in data:
        date_str = entry["properties"].get("date", None)
        # Try to parse the date, but if it fails (e.g., "2025-01-27 11:00 - 11:30"), set as NaT
        try:
            entry["date"] = pd.to_datetime(date_str, errors="raise")
        except Exception:
            entry["date"] = pd.NaT

    return pd.DataFrame(data)


def export_notes_to_markdown(df: pd.DataFrame) -> str:
    """
    Concatenate all notes from the DataFrame into a single Markdown string, sorted by date ascending.

    Args:
        df (pd.DataFrame): DataFrame from parse_capacities_export_zip

    Returns:
        str: Concatenated Markdown string of all notes
    """
    # Sort by the 'date' column, ensuring all datetimes are tz-naive to avoid comparison errors
    df = df.copy()
    if "date" in df.columns:
        # Convert all datetimes to tz-naive (remove timezone info)
        df["date"] = pd.to_datetime(df["date"], errors="coerce").apply(
            lambda x: (
                x.tz_localize(None)
                if pd.notnull(x) and hasattr(x, "tzinfo") and x.tzinfo is not None
                else x
            )
        )
        df = df.sort_values("date", ascending=True)

    markdown_content = []
    for _, row in df.iterrows():
        title = row["title"]
        content = row["text_content"]
        markdown_content.append(f"# {title}\n\n{content}\n\n---\n\n")

    return "".join(markdown_content)
