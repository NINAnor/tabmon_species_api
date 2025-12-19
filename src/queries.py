import duckdb

get_species = duckdb.sql("""
    SELECT DISTINCT species
    FROM data/merged_predictions_light/*/*/*.parquet
    """)