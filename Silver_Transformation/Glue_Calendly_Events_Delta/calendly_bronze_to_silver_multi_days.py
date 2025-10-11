# calendly_bronze_to_silver_multi.py
# Glue 5 / Spark 3.5 / Python 3

import sys
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# -------------------- Args --------------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "RAW_PREFIX",         # e.g. s3://dea-calendly-data/raw/calendly_events
    "START_DATE",         # e.g. 2025-09-30
    "END_DATE"            # e.g. 2025-10-06
])

raw_prefix = args["RAW_PREFIX"].strip()
start_date = datetime.strptime(args["START_DATE"], "%Y-%m-%d").date()
end_date   = datetime.strptime(args["END_DATE"], "%Y-%m-%d").date()
silver_events_path = "s3://dea-calendly-data/silver/calendly_events"

# -------------------- Glue Context --------------------
sc = SparkContext.getOrCreate()
glue = GlueContext(sc)
spark = glue.spark_session

# enable fast S3 I/O
spark._jsc.hadoopConfiguration().set("fs.s3a.fast.upload", "true")

# -------------------- Utility: build list of dates --------------------
def daterange(start, end):
    for n in range(int((end - start).days) + 1):
        yield (start + timedelta(n)).strftime("%Y-%m-%d")

DATES = list(daterange(start_date, end_date))
print(f"[INFO] Will rebuild for dates: {DATES}")

# -------------------- Channel mapping --------------------
channel_map = {
    "https://api.calendly.com/event_types/d639ecd3-8718-4068-955a-436b10d72c78": "facebook_paid_ads",
    "https://api.calendly.com/event_types/dbb4ec50-38cd-4bcd-bbff-efb7b5a6f098": "youtube_paid_ads",
    "https://api.calendly.com/event_types/bb339e98-7a67-4af2-b584-8dbf95564312": "tiktok_paid_ads",
}
channel_map_expr = F.create_map([F.lit(x) for kv in channel_map.items() for x in kv])

# -------------------- Function to process one day --------------------
def build_day_df(dt):
    src_path = f"{raw_prefix}/dt={dt}/*"
    raw = spark.read.json(src_path)

    booking_id = F.regexp_extract(F.col("payload.uri"), r"/invitees/([^/?]+)$", 1)
    cal_event_type_url = F.col("payload.scheduled_event.event_type")
    booking_date = F.to_date(F.col("payload.scheduled_event.start_time"))
    booking_created_at = F.to_timestamp(F.col("payload.created_at"))
    start_ts = F.to_timestamp(F.col("payload.scheduled_event.start_time"))
    end_ts   = F.to_timestamp(F.col("payload.scheduled_event.end_time"))
    org_email = F.when(F.size(F.col("payload.scheduled_event.event_memberships")) > 0,
                       F.col("payload.scheduled_event.event_memberships")[0]["user_email"])
    org_name  = F.when(F.size(F.col("payload.scheduled_event.event_memberships")) > 0,
                       F.col("payload.scheduled_event.event_memberships")[0]["user_name"])
    org_uri   = F.when(F.size(F.col("payload.scheduled_event.event_memberships")) > 0,
                       F.col("payload.scheduled_event.event_memberships")[0]["user"])

    df = (
        raw
        .withColumn("webhook_event_name", F.col("event"))
        .withColumn("webhook_received_at", F.to_timestamp(F.col("created_at")))
        .withColumn("webhook_received_by", F.col("created_by"))
        .withColumn("booking_id", booking_id)
        .withColumn("scheduled_event_id",
                    F.regexp_extract(F.col("payload.scheduled_event.uri"),
                                     r"/scheduled_events/([^/?]+)$", 1))
        .withColumn("calendly_event_type_url", cal_event_type_url)
        .withColumn("channel", channel_map_expr.getItem(cal_event_type_url))
        .withColumn("booking_created_at", booking_created_at)
        .withColumn("booking_date", booking_date)
        .withColumn("scheduled_event_start", start_ts)
        .withColumn("scheduled_event_end", end_ts)
        .withColumn("scheduled_event_status", F.col("payload.scheduled_event.status"))
        .withColumn("event_name", F.col("payload.scheduled_event.name"))
        .withColumn("invitee_email", F.lower(F.col("payload.email")))
        .withColumn("invitee_name", F.col("payload.name"))
        .withColumn("invitee_status", F.col("payload.status"))
        .withColumn("invitee_uri", F.col("payload.uri"))
        .withColumn("invitee_timezone", F.col("payload.timezone"))
        .withColumn("rescheduled", F.col("payload.rescheduled"))
        .withColumn("cancel_url", F.col("payload.cancel_url"))
        .withColumn("reschedule_url", F.col("payload.reschedule_url"))
        .withColumn("text_reminder_number", F.col("payload.text_reminder_number"))
        .withColumn("location_type", F.col("payload.scheduled_event.location.type"))
        .withColumn("location_url", F.col("payload.scheduled_event.location.join_url"))
        .withColumn("organizer_email", org_email)
        .withColumn("organizer_name", org_name)
        .withColumn("organizer_uri", org_uri)
        .withColumn("utm_source",   F.col("payload.tracking.utm_source"))
        .withColumn("utm_campaign", F.col("payload.tracking.utm_campaign"))
        .withColumn("utm_medium",   F.col("payload.tracking.utm_medium"))
        .withColumn("utm_content",  F.col("payload.tracking.utm_content"))
        .withColumn("utm_term",     F.col("payload.tracking.utm_term"))
        .withColumn("dt", F.lit(dt))
    )

    cols = [
        "booking_id", "scheduled_event_id", "calendly_event_type_url", "channel",
        "event_name", "webhook_event_name",
        "booking_created_at", "booking_date", "scheduled_event_start", "scheduled_event_end",
        "invitee_email", "invitee_name", "invitee_status", "invitee_uri", "invitee_timezone",
        "rescheduled", "cancel_url", "reschedule_url", "text_reminder_number",
        "organizer_email", "organizer_name", "organizer_uri",
        "location_type", "location_url",
        "utm_source", "utm_campaign", "utm_medium", "utm_content", "utm_term",
        "webhook_received_at", "webhook_received_by",
        "dt"
    ]
    return df.select(*cols)

# -------------------- Main loop: rebuild all --------------------
# ------ initial load 9/30 - 10/6
#
# for d in DATES:
#     print(f"[INFO] Writing day {d}")
#     df_day = build_day_df(d)
#     (
#         df_day.write
#             .format("delta")
#             .mode("append")            # appends; re-running same day may duplicate
#             .partitionBy("dt")
#             .save(silver_events_path)
#     )


# --------incremental load : 10/7 - 10/8
for d in DATES:
    print(f"[INFO] Writing day {d}")
    df_day = build_day_df(d)  # ensure df_day only contains rows with dt == d
    (
        df_day.write
            .format("delta")
            .mode("overwrite")
            .option("partitionOverwriteMode", "dynamic")
            .partitionBy("dt")
            .save(silver_events_path)
    )



#---------------------------------------------------------------------------------------------------------------------------------------------
# Below code not working --
# Error: Error Category: UNCLASSIFIED_ERROR; Failed Line Number: 138; An error occurred while calling o403.save. 
# class org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable cannot be cast to class org.apache.spark.sql.delta.commands.DeleteCommand 
# (org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable and org.apache.spark.sql.delta.commands.DeleteCommand are in unnamed module of loader 'app')
####################################################################################################################################################
#
# for d in DATES:
#     print(f"[INFO] Writing day {d}")
#     df_day = build_day_df(d)  # rows must be dt == d
#     (df_day.write
#         .format("delta")
#         .mode("overwrite")
#         .option("replaceWhere", f"dt = '{d}'")  # only this day is replaced
#         .partitionBy("dt")
#         .save(silver_events_path))


print("[SUCCESS] Rebuild complete.")
