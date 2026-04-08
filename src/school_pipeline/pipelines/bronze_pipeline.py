import json
import os
import re
from pyspark.sql import SparkSession
from importlib.resources import files


def load_config():
    env = os.getenv("DBX_ENV", "dev")
    config_path = files("school_pipeline.conf") / f"{env}.json"

    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def extract_table_name(path: str) -> str|None:
    match = re.search(r"school/([^/]+)", path)
    if match:
        raw = match.group(1)
        return raw.split("-")[0]
    return None


def load_stream(spark: SparkSession, src: str, base_checkpoint: str, base_schema: str, base_output: str):
    input_path = src["path"]
    fmt = src["format"]
    table_name = extract_table_name(input_path)

    checkpoint = f"{base_checkpoint}/{table_name}"
    schema_loc = f"{base_schema}/{table_name}"
    output = f"{base_output}/{table_name}"

    df = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", fmt)
            .option("cloudFiles.schemaLocation", schema_loc)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(input_path)
    )

    print(f"Writing stream to {output}")
    
    (
        df.writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint)
            .outputMode("append")
            .start(output)
    )


def run():
    print("Running bronze pipeline")

    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    sources = config["sources"]
    base_schema = config["base_schema"]
    base_output = config["base_output"]
    base_checkpoint = config["base_checkpoint"]

    for src in sources:
        load_stream(spark, src, base_checkpoint, base_schema, base_output)

    spark.streams.awaitAnyTermination()