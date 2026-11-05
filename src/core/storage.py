"""S3 and DuckDB access shared by both apps."""

import io

import boto3
import duckdb
import pandas as pd
import streamlit as st
from botocore.client import Config
from botocore.exceptions import ClientError

from config import (
    S3_ACCESS_KEY_ID,
    S3_BUCKET,
    S3_ENDPOINT,
    S3_SECRET_ACCESS_KEY,
)


@st.cache_resource
def get_s3_client():
    """Create the boto3 S3 client (shared across sessions)."""
    return boto3.client(
        "s3",
        endpoint_url=f"https://{S3_ENDPOINT}",
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
    )


@st.cache_resource
def get_duckdb_connection():
    """Create a DuckDB connection able to read s3:// paths."""
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    # SET statements instead of CREATE SECRET for better custom endpoint support
    conn.execute("SET s3_region='us-east-1';")
    conn.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY_ID}';")
    conn.execute(f"SET s3_secret_access_key='{S3_SECRET_ACCESS_KEY}';")
    conn.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
    conn.execute("SET s3_use_ssl=true;")
    conn.execute("SET s3_url_style='path';")
    return conn


def s3_url(key):
    """Build the s3:// URL of an object key in the bucket."""
    return f"s3://{S3_BUCKET}/{key}"


def split_s3_url(url):
    """Split an s3:// URL into (bucket, key)."""
    bucket, key = url.removeprefix("s3://").split("/", 1)
    return bucket, key


def list_objects(prefix):
    """List every object (dicts with Key, Size, ...) under a prefix."""
    paginator = get_s3_client().get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        objects.extend(page.get("Contents", []))
    return objects


def list_subfolders(prefix):
    """List the immediate "sub-folder" prefixes under a prefix ending in '/'."""
    paginator = get_s3_client().get_paginator("list_objects_v2")
    folders = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/"):
        folders.extend(entry["Prefix"] for entry in page.get("CommonPrefixes", []))
    return folders


def object_exists(key):
    """Return True if the key exists in the bucket."""
    try:
        get_s3_client().head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError:
        return False


def read_csv(key):
    """Read a CSV object from the bucket into a DataFrame."""
    response = get_s3_client().get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_csv(io.BytesIO(response["Body"].read()))


def read_csv_files(prefix, filename_prefix=""):
    """Read and concatenate every CSV under `prefix` whose file name starts
    with `filename_prefix`. Unreadable files are skipped.

    Returns an empty DataFrame when nothing matches.
    """
    frames = []
    for obj in list_objects(prefix):
        filename = obj["Key"].split("/")[-1]
        if not (filename.endswith(".csv") and filename.startswith(filename_prefix)):
            continue
        try:
            frames.append(read_csv(obj["Key"]))
        except Exception:  # noqa: S112
            continue
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def append_csv(df, key):
    """Append rows to a CSV object, creating it if it doesn't exist."""
    if object_exists(key):
        df = pd.concat([read_csv(key), df], ignore_index=True)
    get_s3_client().put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=df.to_csv(index=False).encode("utf-8"),
    )
