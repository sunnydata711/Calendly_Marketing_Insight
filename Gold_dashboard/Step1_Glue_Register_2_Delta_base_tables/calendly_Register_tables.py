# calendly_register_tables.py  (Glue 4/5, Spark job)
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# --- Glue bootstrap ---
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --- Create DB + register existing Delta tables in the Glue Data Catalog ---
spark.sql("CREATE DATABASE IF NOT EXISTS marketing_data")

spark.sql("DROP TABLE IF EXISTS marketing_data.calendly_events;")

spark.sql("""
CREATE TABLE IF NOT EXISTS marketing_data.calendly_events
USING DELTA
LOCATION 's3://dea-calendly-data/silver/calendly_events'
""")

spark.sql("DROP TABLE IF EXISTS marketing_data.events_spend;")

spark.sql("""
CREATE TABLE IF NOT EXISTS marketing_data.events_spend
USING DELTA
LOCATION 's3://dea-calendly-data/silver/events_spend'
""")

# --- Quick verification ---
df_events = spark.read.format("delta").load("s3://dea-calendly-data/silver/calendly_events")
df_spend  = spark.read.format("delta").load("s3://dea-calendly-data/silver/events_spend")
print(f"[CHECK] events rows={df_events.count()}, spend rows={df_spend.count()}")
df_events.select("booking_id","channel","invitee_email","dt").show(10, truncate=False)
df_spend.select("channel","spend","dt").show(10, truncate=False)

job.commit()
