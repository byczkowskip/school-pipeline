import json
import os
import re
from pyspark.sql import SparkSession

def load_config():
    env = os.getenv("DBX_ENV", "dev")
    with open(f"conf/{env}.json") as f:
        return json.load(f)

def detect_form(path: str) -> str:
    if 'csv' in path:
        return 'csv'
    elif 'json' in path:
        return 'json'
    return 'parquet'

def extract_table_name(path: str) -> str|None:
    match = re.search(r"school/([^/]+)", path)
    if match:
        raw = match.group(1)
        return raw.split("-")[0]
    return None

def load_stream(spark: SparkSession, input_path: str, base_checkpoint: str, base_schema: str, base_output: str):
    fmt = detect_form(input_path)
    table_name = extract_table_name(input_path)

    checkpoint = f"{base_checkpoint}/{table_name}"
    schema_loc = f"{base_schema}/{table_name}"
    output = f"{base_output}/{table_name}"

    df = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFileFormat", fmt)
            .option("cloudFiles.schemaLocation", schema_loc)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("inferSchema", "true")
            .load(input_path)
    )

    (
        df.writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint)
            .outputMode("append")
            .start(output)
    )

def main():
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    sources = config["sources"]
    base_schema = config["base_schema"]
    base_output = config["base_output"]
    base_checkpoint = config["base_checkpoint"]

    for src in sources:
        load_stream(spark, src, base_checkpoint, base_schema, base_output)

    spark.streams.awaitAnyTermination()