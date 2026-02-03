"""
Subsample parquet files for annotation tasks.

This script:
- Reads aggregated parquet files from Listening_Lab output
- Filters by specified species and sites
- Samples detections by confidence bins (e.g., 50 per 0.1 bin)
- Assigns userIDs for annotation distribution
- Outputs to S3 for use in the annotation app

Usage:
    uv run python sampling_scripts/subsample_annotations.py \
        --species "Sylvia atricapilla" \
        --sites e9e2754 5b05fe12 \
        --samples-per-bin 2 \
        --bin-size 0.1 \
        --user-ids user001 user002 user003 \
        --output-prefix Listening_Lab/annotations_sample
"""

import argparse
import io
import os
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)

# S3 defaults
DEFAULT_S3_BUCKET = "bencretois-ns8129k-proj-tabmon"
DEFAULT_INPUT_PATH = "Listening_Lab"  # Output from merged_partitioned.py
DEFAULT_OUTPUT_PATH = "Listening_Lab/annotations_sample"


def get_duckdb_s3_connection():
    """Create a DuckDB connection configured for S3 access."""
    print("  → Connecting to DuckDB with S3 support...")
    con = duckdb.connect(database=":memory:")
    
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    
    s3_access_key = os.getenv("S3_ACCESS_KEY_ID")
    s3_secret_key = os.getenv("S3_SECRET_ACCESS_KEY")
    s3_endpoint = os.getenv("S3_ENDPOINT")
    
    s3_endpoint = s3_endpoint.replace("https://", "").replace("http://", "")
    
    con.execute("SET s3_region='us-east-1';")
    con.execute(f"SET s3_access_key_id='{s3_access_key}';")
    con.execute(f"SET s3_secret_access_key='{s3_secret_key}';")
    con.execute(f"SET s3_endpoint='{s3_endpoint}';")
    con.execute("SET s3_use_ssl=true;")
    con.execute("SET s3_url_style='path';")
    
    return con


def create_confidence_bins(confidence_values, bin_size=0.1):
    """Create confidence bin labels for given confidence values."""
    bins = np.arange(0, 1 + bin_size, bin_size)
    bin_labels = [f"{bins[i]:.1f}-{bins[i+1]:.1f}" for i in range(len(bins)-1)]
    return pd.cut(confidence_values, bins=bins, labels=bin_labels, include_lowest=True)


def subsample_segments_by_target_species_confidence(df, target_species, samples_per_bin=50, bin_size=0.1, random_seed=42):
    """
    Subsample segments by the confidence of target species detections.
    Keeps full segment data (all species) but bins/samples based on target species confidence.
    
    Args:
        df: DataFrame with aggregated segments (species arrays)
        target_species: List of target species to use for binning
        samples_per_bin: Number of samples to draw from each bin
        bin_size: Size of confidence bins (default 0.1)
        random_seed: Random seed for reproducibility
    
    Returns:
        Subsampled DataFrame with full segment data
    """
    # For each segment, find the confidence of target species
    print(f"\n  Extracting target species confidence from segments...")
    
    segments_with_conf = []
    for idx, row in df.iterrows():
        species_list = row["scientific name"]
        confidence_list = row["confidence"]
        
        # Handle case where these might be strings
        if isinstance(species_list, str):
            import ast
            species_list = ast.literal_eval(species_list)
            confidence_list = ast.literal_eval(confidence_list)
        
        # Find max confidence among target species in this segment
        target_confidences = []
        for i, species in enumerate(species_list):
            if species in target_species and i < len(confidence_list):
                target_confidences.append(float(confidence_list[i]))
        
        if target_confidences:
            # Use max confidence of target species for binning
            max_conf = max(target_confidences)
            segments_with_conf.append({
                'segment_idx': idx,
                'target_max_confidence': max_conf
            })
    
    conf_df = pd.DataFrame(segments_with_conf)
    print(f"  → Found {len(conf_df):,} segments with target species")
    
    # Create confidence bins
    conf_df["confidence_bin"] = create_confidence_bins(conf_df["target_max_confidence"], bin_size)
    
    print(f"\n  Confidence distribution (based on target species):")
    bin_counts = conf_df["confidence_bin"].value_counts().sort_index()
    for bin_label, count in bin_counts.items():
        print(f"    {bin_label}: {count:,} segments available")
    
    # Sample from each bin
    subsampled_indices = []
    print(f"\n  Sampling {samples_per_bin} segments per bin:")
    
    for bin_label in conf_df["confidence_bin"].cat.categories:
        bin_data = conf_df[conf_df["confidence_bin"] == bin_label]
        
        if len(bin_data) == 0:
            continue
        
        n_samples = min(samples_per_bin, len(bin_data))
        sampled = bin_data.sample(n=n_samples, random_state=random_seed)
        subsampled_indices.extend(sampled['segment_idx'].tolist())
        
        percentage = (n_samples / len(bin_data)) * 100
        print(f"    {bin_label}: sampled {n_samples:,} from {len(bin_data):,} segments ({percentage:.1f}%)")
    
    # Return the original segments (with full species arrays) for the sampled indices
    result = df.iloc[subsampled_indices].copy().reset_index(drop=True)
    
    return result


def assign_user_ids(df, user_ids):
    """
    Distribute clips evenly among user IDs.
    
    Args:
        df: DataFrame with subsampled detections
        user_ids: List of user ID strings
    
    Returns:
        DataFrame with userID column assigned
    """
    if not user_ids:
        print("\n  No user IDs specified, leaving userID as NaN")
        df["userID"] = "default_user"
        return df
    
    print(f"\n  Distributing {len(df):,} clips among {len(user_ids)} annotators...")
    
    # Shuffle for random distribution
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)
    
    # Assign users in round-robin fashion
    df["userID"] = [user_ids[i % len(user_ids)] for i in range(len(df))]
    
    # Print distribution
    print("\n  User ID distribution:")
    for user_id in user_ids:
        count = (df["userID"] == user_id).sum()
        percentage = (count / len(df)) * 100
        print(f"    {user_id}: {count:,} clips ({percentage:.1f}%)")
    
    return df


def load_and_filter_segments_with_duckdb(s3_bucket, input_prefix, target_species, target_sites=None):
    """
    Load parquet files and find segments containing target species using DuckDB.
    Returns segments in original aggregated format (preserving all species in each segment).
    Only includes segments that contain at least one target species.
    """
    con = get_duckdb_s3_connection()
    
    # Build species filter using efficient array function
    species_array = "['" + "', '".join(target_species) + "']"
    print(f"  → Finding segments containing {len(target_species)} target species...")
    
    # If sites are specified, read only those specific partitions (much faster!)
    if target_sites:
        print(f"  → Filtering for {len(target_sites)} deployment sites...")
        print(f"  → Optimizing: reading only specific partition paths...")
        
        # Build list of specific partition paths to scan
        # This avoids scanning the entire dataset
        partition_patterns = []
        for site in target_sites:
            # Try to extract country and device_id from deployment_id
            # Deployment IDs like: 20250611_FR_10_e9e2754
            # This maps to partitions: country=France/device_id=...
            partition_patterns.append(f"s3://{s3_bucket}/{input_prefix}/**/device_id={site}/*.parquet")
        
        # Read all specified partitions
        all_dfs = []
        for i, pattern in enumerate(partition_patterns, 1):
            try:
                print(f"    [{i}/{len(partition_patterns)}] Reading {target_sites[i-1]}...")
                query = f"""
                    SELECT 
                        filename,
                        deployment_id,
                        fullPath,
                        "start time",
                        "scientific name",
                        confidence,
                        "max uncertainty",
                        userID
                    FROM read_parquet('{pattern}', hive_partitioning=true)
                    WHERE list_has_any("scientific name", {species_array})
                """
                df = con.execute(query).fetchdf()
                if not df.empty:
                    all_dfs.append(df)
                    print(f"       ✓ Found {len(df):,} matching segments")
                else:
                    print(f"       - No matching segments")
            except Exception as e:
                print(f"       ⚠ Could not read {target_sites[i-1]}: {e}")
                continue
        
        con.close()
        
        if not all_dfs:
            print(f"  ✗ No segments found")
            return pd.DataFrame()
        
        # Combine all results
        df = pd.concat(all_dfs, ignore_index=True)
        print(f"  ✓ Total: {len(df):,} segments containing target species")
        print(f"  → Note: Each segment may contain multiple species (arrays preserved)")
        return df
    
    else:
        # No site filter - scan all files (slower)
        s3_pattern = f"s3://{s3_bucket}/{input_prefix}/**/*.parquet"
        print(f"  → Reading parquet pattern: {s3_pattern}")
        print(f"  ⚠ No site filter - this will scan ALL files (may be slow)")
        
        try:
            query = f"""
                SELECT 
                    filename,
                    deployment_id,
                    fullPath,
                    "start time",
                    "scientific name",
                    confidence,
                    "max uncertainty",
                    userID
                FROM read_parquet('{s3_pattern}', hive_partitioning=true)
                WHERE list_has_any("scientific name", {species_array})
            """
            
            print(f"  → Executing DuckDB query (finding matching segments)...")
            df = con.execute(query).fetchdf()
            con.close()
            print(f"  ✓ Found {len(df):,} segments containing target species")
            print(f"  → Note: Each segment may contain multiple species (arrays preserved)")
            return df
        except Exception as e:
            con.close()
            print(f"  ✗ Error loading data: {e}")
            print(f"  Query was: {query}")
            return pd.DataFrame()


def main():
    parser = argparse.ArgumentParser(
        description="Subsample parquet files for annotation tasks."
    )
    parser.add_argument(
        "--s3-bucket",
        default=DEFAULT_S3_BUCKET,
        help="S3 bucket name",
    )
    parser.add_argument(
        "--input-prefix",
        default=DEFAULT_INPUT_PATH,
        help="S3 prefix for input parquet files (default: Listening_Lab)",
    )
    parser.add_argument(
        "--output-prefix",
        default=DEFAULT_OUTPUT_PATH,
        help="S3 prefix for output parquet file",
    )
    parser.add_argument(
        "--species",
        nargs="+",
        required=True,
        help="List of species scientific names to include",
    )
    parser.add_argument(
        "--sites",
        nargs="*",
        default=None,
        help="List of deployment IDs to include (optional, all if not specified)",
    )
    parser.add_argument(
        "--samples-per-bin",
        type=int,
        default=50,
        help="Number of samples per confidence bin (default: 50)",
    )
    parser.add_argument(
        "--bin-size",
        type=float,
        default=0.1,
        help="Confidence bin size (default: 0.1)",
    )
    parser.add_argument(
        "--user-ids",
        nargs="+",
        default=None,
        help="List of user IDs for annotation assignment (optional)",
    )
    parser.add_argument(
        "--output-filename",
        default="annotations_sample.parquet",
        help="Output filename (default: annotations_sample.parquet)",
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    
    args = parser.parse_args()
    
    print("="*70)
    print("TABMON Annotation Subsampling Script")
    print("="*70)
    print(f"\n📂 Input:  s3://{args.s3_bucket}/{args.input_prefix}/")
    print(f"📂 Output: s3://{args.s3_bucket}/{args.output_prefix}/{args.output_filename}")
    print(f"\n🎯 Target species ({len(args.species)}):")
    for species in args.species:
        print(f"   • {species}")
    if args.sites:
        print(f"\n📍 Target sites ({len(args.sites)}): {', '.join(args.sites)}")
    else:
        print(f"\n📍 Target sites: All sites")
    print(f"\n📊 Sampling parameters:")
    print(f"   • Samples per bin: {args.samples_per_bin}")
    print(f"   • Bin size: {args.bin_size}")
    print(f"   • Random seed: {args.random_seed}")
    if args.user_ids:
        print(f"\n👥 Annotators ({len(args.user_ids)}): {', '.join(args.user_ids)}")
    print("\n" + "="*70)
    
    # Load and filter segments - keep full species arrays
    print("\n[1/4] Loading and filtering segments with DuckDB...")
    df_segments = load_and_filter_segments_with_duckdb(
        args.s3_bucket, 
        args.input_prefix,
        args.species,
        args.sites
    )
    
    if df_segments.empty:
        print("\n❌ No segments found containing target species. Exiting.")
        return
    
    # Subsample segments by target species confidence bins
    print(f"\n[2/4] Subsampling segments by target species confidence bins...")
    df_sampled = subsample_segments_by_target_species_confidence(
        df_segments,
        args.species,
        samples_per_bin=args.samples_per_bin,
        bin_size=args.bin_size,
        random_seed=args.random_seed
    )
    
    if df_sampled.empty:
        print("\n❌ No samples after binning. Exiting.")
        return
    
    print(f"\n  ✓ Total segments sampled: {len(df_sampled):,}")
    print(f"  → Each segment contains all detected species (arrays preserved)")
    
    # Assign user IDs
    print(f"\n[3/4] Assigning user IDs...")
    if args.user_ids:
        df_sampled = assign_user_ids(df_sampled, args.user_ids)
    else:
        df_sampled["userID"] = np.nan
    
    # Ensure correct column order
    column_order = [
        "filename", "deployment_id", "fullPath", "start time",
        "scientific name", "confidence", "max uncertainty", "userID"
    ]
    df_final = df_sampled[[col for col in column_order if col in df_sampled.columns]]
    
    # Save to S3
    print(f"\n[4/4] Saving results to S3...")
    print("="*70)
    print(f"\n  Location: s3://{args.s3_bucket}/{args.output_prefix}/{args.output_filename}")
    print(f"  Rows: {len(df_final):,}")
    print(f"  Columns: {', '.join(df_final.columns)}")
    
    import boto3
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
    
    # Write to parquet buffer
    parquet_buffer = io.BytesIO()
    df_final.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_buffer.seek(0)
    
    output_key = f"{args.output_prefix}/{args.output_filename}"
    s3_client.put_object(
        Bucket=args.s3_bucket,
        Key=output_key,
        Body=parquet_buffer.getvalue()
    )
    
    print(f"\n✓ Upload complete!")
    print(f"\n" + "="*70)
    print("Summary")
    print("="*70)
    
    # Count unique species from arrays
    all_unique_species = set()
    for species_array in df_final["scientific name"]:
        if isinstance(species_array, (list, np.ndarray)):
            all_unique_species.update(species_array)
        elif isinstance(species_array, str):
            import ast
            species_list = ast.literal_eval(species_array)
            all_unique_species.update(species_list)
    
    print(f"\n✅ Successfully created annotation sample")
    print(f"   • Total clips: {len(df_final):,}")
    print(f"   • Unique species across all clips: {len(all_unique_species)}")
    print(f"   • Deployments: {len(df_final['deployment_id'].unique())}")
    if args.user_ids:
        print(f"   • Annotators: {len(args.user_ids)}")
    print(f"\n📦 Output file: s3://{args.s3_bucket}/{output_key}")
    print(f"\n" + "="*70)


if __name__ == "__main__":
    main()
