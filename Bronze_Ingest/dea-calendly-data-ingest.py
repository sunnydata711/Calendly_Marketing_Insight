# Ingest Calendly data from AWS API Gateway to s3://dea-calendly-data/bronze/calendly/webhooks/
# ########################
import json, os, hashlib, base64, datetime, uuid
import boto3

s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET_NAME"]
PREFIX = os.environ.get("PREFIX", "bronze/calendly/webhooks/")

def _now_iso():
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def _safe_event_id(body_dict):
    # Derive a stable id to avoid duplicates on retries
    try:
        p = body_dict.get("payload", {})
        uri = p.get("uri") or p.get("scheduled_event", {}).get("uri")
        if uri:
            return hashlib.sha256(uri.encode("utf-8")).hexdigest()
    except Exception:
        pass
    return str(uuid.uuid4())

def _object_key(body_dict):
    ts = _now_iso()[:10]  # YYYY-MM-DD
    event = body_dict.get("event", "unknown_event")
    eid = _safe_event_id(body_dict)
    return f"{PREFIX}dt={ts}/{event}/{eid}.json"

def lambda_handler(event, context):
    try:
        body = event.get("body") or ""
        body_bytes = body.encode("utf-8")
        if event.get("isBase64Encoded"):
            body_bytes = base64.b64decode(body)

        try:
            body_dict = json.loads(body_bytes.decode("utf-8") or "{}")
        except Exception:
            body_dict = {"raw": body_bytes.decode("utf-8", "ignore")}

        key = _object_key(body_dict)
        s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(body_dict).encode("utf-8"))

        return {"statusCode": 200, "body": "ok"}
    except Exception as e:
        print("ERROR:", repr(e))
        # Still return 200 to avoid webhook retry storms; errors visible in logs
        return {"statusCode": 200, "body": "ok"}
