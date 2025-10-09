"""Utility helpers for building networks from Capacities export data."""

from __future__ import annotations

import re
from collections import Counter
from pathlib import PurePosixPath
from typing import List
from urllib.parse import unquote

import networkx as nx
import pandas as pd


# Matches Markdown links, excluding obvious external protocols.
_MENTION_PATTERN = re.compile(
    r"\[[^\]]+\]\((?!https?://|mailto:|capacities://)([^)]+)\)",
    re.IGNORECASE,
)


def extract_object_mentions(text_content: str) -> List[str]:
    """Extract referenced object Markdown file names from Capacities text content.

    Parameters
    ----------
    text_content:
        The Markdown string that may contain references to other objects.

    Returns
    -------
    list of str
        Normalized Markdown file names (e.g. ``Corey Shaya.md``) referenced in
        the text. Mentions are de-duplicated while preserving order of
        appearance.
    """

    if not text_content:
        return []

    mentions: List[str] = []
    seen = set()

    for match in _MENTION_PATTERN.findall(text_content):
        cleaned = match.strip()
        if not cleaned:
            continue

        # Remove anchors or query params.
        for delimiter in ("#", "?"):
            if delimiter in cleaned:
                cleaned = cleaned.split(delimiter, 1)[0]

        # Normalise paths and decode percent-encoding.
        filename = PurePosixPath(cleaned).name
        if not filename.lower().endswith(".md"):
            continue
        filename = unquote(filename)

        if filename not in seen:
            seen.add(filename)
            mentions.append(filename)

    return mentions


import json


def build_object_graph(
    capacities_df: pd.DataFrame, normalize_dict_attrs: bool = True
) -> nx.DiGraph:
    """Construct a directed mention graph from Capacities export data.

    Nodes correspond to objects (keyed by ``file_name``) and include useful
    metadata. Directed edges connect an object to other objects it references in
    ``text_content`` and ``properties``. Multiple mentions between the same pair 
    accumulate in the ``weight`` attribute of the edge.

    Parameters
    ----------
    capacities_df : pd.DataFrame
        The dataframe containing Capacities export data.
    normalize_dict_attrs : bool, optional
        If True (default), any dict-valued node attributes (e.g. 'properties')
        are converted to JSON strings for compatibility with GraphML and other
        formats that do not support dicts as attribute values.
    """

    graph = nx.DiGraph()

    if capacities_df is None or capacities_df.empty:
        return graph

    file_lookup = {}

    # First, register all objects as nodes.
    for _, row in capacities_df.iterrows():
        file_name = row.get("file_name")
        if not isinstance(file_name, str) or not file_name:
            continue

        file_lookup.setdefault(file_name, row)

        # Prepare node attributes, omitting any with value None
        node_attrs = {
            "title": row.get("title", ""),
            "object_type": row.get("object_type", ""),
            "properties": row.get("properties", {}),
            "date": row.get("date"),
        }

        # Remove attributes with value None
        node_attrs = {k: v for k, v in node_attrs.items() if v is not None}

        # Convert pandas NaT to None or ISO string for GraphML compatibility
        if "date" in node_attrs:
            date_val = node_attrs["date"]
            if pd.isna(date_val):
                del node_attrs["date"]
            elif hasattr(date_val, "isoformat"):
                node_attrs["date"] = date_val.isoformat()
            else:
                node_attrs["date"] = str(date_val)

        if normalize_dict_attrs:
            for k, v in node_attrs.items():
                if isinstance(v, dict):
                    node_attrs[k] = json.dumps(v, ensure_ascii=False)

        graph.add_node(file_name, **node_attrs)

    # Then, add edges for mentions discovered in text content and properties.
    for _, row in capacities_df.iterrows():
        source_file = row.get("file_name")
        if not isinstance(source_file, str) or not source_file:
            continue

        # Extract mentions from text content
        text_content = row.get("text_content") or ""
        mention_counts = Counter(extract_object_mentions(text_content))

        # Extract mentions from properties
        properties = row.get("properties") or {}
        if isinstance(properties, dict):
            for prop_value in properties.values():
                if isinstance(prop_value, str):
                    property_mentions = extract_object_mentions(prop_value)
                    for mention in property_mentions:
                        mention_counts[mention] += 1

        for target_file, weight in mention_counts.items():
            if target_file not in file_lookup:
                continue
            if source_file == target_file:
                continue

            if graph.has_edge(source_file, target_file):
                graph[source_file][target_file]["weight"] += weight
            else:
                graph.add_edge(source_file, target_file, weight=weight)

    return graph
