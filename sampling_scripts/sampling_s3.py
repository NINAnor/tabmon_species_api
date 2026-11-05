"""S3 operations for annotation sampling."""

import io
import os

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


def format_dataframe_for_app(df):
    """
    Format DataFrame to match the expected structure for the annotation app.

    Expected format:
    - scientific name: native list of strings
    - confidence: native list of floats
    - max uncertainty: native list of floats
    - userID: string
    """
    df = df.copy()

    # Ensure list columns contain proper Python lists
    def _to_list(x):
        if isinstance(x, list):
            return x
        if hasattr(x, "tolist"):
            return x.tolist()
        if isinstance(x, int | float):
            return [x]
        return list(x)

    for col in ["scientific name", "confidence", "max uncertainty"]:
        if col in df.columns:
            df[col] = df[col].apply(_to_list)

    # Ensure userID is string
    if "userID" in df.columns:
        df["userID"] = df["userID"].astype(str)

    return df


def upload_to_s3(df, s3_bucket, output_key):
    """Upload DataFrame as parquet to S3."""
    # Format data for app compatibility
    df = format_dataframe_for_app(df)

    # Configure S3 client
    s3_config = {}
    s3_endpoint = os.getenv("S3_ENDPOINT")
    if s3_endpoint:
        if not s3_endpoint.startswith(("http://", "https://")):
            s3_endpoint = f"https://{s3_endpoint}"
        s3_config["endpoint_url"] = s3_endpoint

    aws_access_key = os.getenv("S3_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("S3_SECRET_ACCESS_KEY")

    if aws_access_key and aws_secret_key:
        s3_config["aws_access_key_id"] = aws_access_key
        s3_config["aws_secret_access_key"] = aws_secret_key

    s3_client = boto3.client("s3", **s3_config)

    # Convert to parquet with native list types via PyArrow
    table = pa.Table.from_pandas(df)
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer)
    parquet_buffer.seek(0)

    s3_client.put_object(
        Bucket=s3_bucket, Key=output_key, Body=parquet_buffer.getvalue()
    )

    return f"s3://{s3_bucket}/{output_key}"
