# silver_spend_from_public.py  (EMR 6.15 / Spark 3.4)
# This py will generate events_spend in delta format
from pyspark.sql import SparkSession, functions as F
import sys

def die(msg, code=1):
    print(f"[ERROR] {msg}")
    sys.exit(code)

def main():
    if len(sys.argv) != 4:
        die("Usage: silver_spend_from_public.py <public_prefix> <PROCESS_DATE|ALL> <silver_out_path>\n"
            "Example: silver_spend_from_public.py s3://betty-calendly-test/calendly_spend_data 2025-10-01 s3://dea-calendly-data/silver/calendly_spend")

    public_prefix = sys.argv[1].rstrip("/")
    process_date  = sys.argv[2]
    silver_out    = sys.argv[3].rstrip("/")

    spark = SparkSession.builder.appName("silver_spend_from_public").getOrCreate()

    # Build source path(s)
    if process_date.upper() == "ALL":
        src = f"{public_prefix}/spend_data_*.json"
    else:
        src = f"{public_prefix}/spend_data_{process_date}.json"

    print(f"[INFO] Reading source: {src}")

    # Read JSON top-level arrays
    raw = (spark.read
           .option("multiLine", True)
           .json(src))

    print("[INFO] Source schema:")
    raw.printSchema()

    # Normalize: [{date, channel, spend}] -> [dt, channel, spend]
    df = (raw
          .withColumnRenamed("date", "dt")
          .select(
              F.col("dt").cast("string").alias("dt"),
              F.col("channel").cast("string").alias("channel"),
              F.col("spend").cast("double").alias("spend")
          ))

    # Drop rows without dt or channel
    df = df.filter(F.col("dt").isNotNull()).filter(F.col("channel").isNotNull())

    if df.rdd.isEmpty():
        print("[INFO] No valid rows (after filtering). Exiting without writing.")
        spark.stop()
        return

    # De-dup within batch
    df = df.dropDuplicates(["dt", "channel"])

    dates = [r["dt"] for r in df.select("dt").distinct().collect()]
    if not dates:
        die("No distinct dt values found in file(s). Check the JSON 'date' field.")

    print(f"[INFO] Distinct dt values in this batch: {dates}")

    predicate = " OR ".join([f"dt='{d}'" for d in dates])

    print(f"[INFO] Writing Delta to: {silver_out}")
    print(f"[INFO] Overwrite partitions (replaceWhere): {predicate}")

    (df.write
       .format("delta")
       .mode("overwrite")
       .option("replaceWhere", predicate)
       .partitionBy("dt")
       .save(silver_out))

    print(f"[SUCCESS] Rows written: {df.count()} into {silver_out}")
    spark.stop()

if __name__ == "__main__":
    main()
