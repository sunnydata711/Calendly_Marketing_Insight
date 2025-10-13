import time
import uuid
import streamlit as st
import boto3
import pandas as pd

st.set_page_config(page_title="Athena Smoke Test", layout="centered")

# ---- Load defaults from Secrets ----
REGION    = st.secrets.get("AWS_REGION", "us-east-1")
DATABASE  = st.secrets.get("ATHENA_DATABASE", "marketing_data")
WORKGROUP = st.secrets.get("ATHENA_WORKGROUP", "primary")
OUTPUT    = st.secrets.get("ATHENA_S3_OUTPUT", "s3://betty-athena-results/calendly/")  # MUST end with '/'

# ---- Simple helpers ----
def make_session():
    return boto3.Session(
        aws_access_key_id=st.secrets.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=st.secrets.get("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=st.secrets.get("AWS_SESSION_TOKEN"),
        region_name=REGION,
    )

def parse_s3_uri(uri: str):
    assert uri.startswith("s3://"), "S3 URI must start with s3://"
    bucket_key = uri[5:]
    if "/" in bucket_key:
        bucket, key = bucket_key.split("/", 1)
    else:
        bucket, key = bucket_key, ""
    return bucket, key

def athena_select_1(session):
    athena = session.client("athena", region_name=REGION)
    q = athena.start_query_execution(
        QueryString="SELECT 1 AS ok",
        QueryExecutionContext={"Database": DATABASE, "Catalog": st.secrets.get("ATHENA_CATALOG", "AwsDataCatalog")},
        ResultConfiguration={"OutputLocation": OUTPUT},
        WorkGroup=WORKGROUP,
    )
    qid = q["QueryExecutionId"]

    t0 = time.time()
    last = None
    while True:
        r = athena.get_query_execution(QueryExecutionId=qid)
        state = r["QueryExecution"]["Status"]["State"]
        if state != last:
            st.write("Athena state:", state)
            last = state
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.time() - t0 > 60:
            raise TimeoutError("Timed out waiting for Athena. Check workgroup/output/permissions.")
        time.sleep(0.5)

    if state != "SUCCEEDED":
        reason = r["QueryExecution"]["Status"].get("StateChangeReason", "No reason provided")
        raise RuntimeError(f"Athena ended in {state}: {reason}")

    res = athena.get_query_results(QueryExecutionId=qid, MaxResults=10)
    cols = [c["Label"] for c in res["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = [[d.get("VarCharValue") for d in row["Data"]] for row in res["ResultSet"]["Rows"]]
    # First row is header; keep consistent DataFrame
    if rows and rows[0] == cols:
        rows = rows[1:]
    return pd.DataFrame(rows, columns=cols)

st.title("🔎 Athena / S3 Smoke Test")

# 1) Who am I?
with st.expander("1) STS identity", expanded=True):
    try:
        sess = make_session()
        sts = sess.client("sts", region_name=REGION)
        ident = sts.get_caller_identity()
        st.success("STS call succeeded")
        st.json(ident)
    except Exception as e:
        st.error(f"STS failed: {e}")

# 2) S3 write to results path
with st.expander("2) S3 results bucket write test", expanded=True):
    try:
        assert OUTPUT.endswith("/"), "ATHENA_S3_OUTPUT must end with '/'."
        bkt, prefix = parse_s3_uri(OUTPUT)
        key = prefix + f"smoke-{uuid.uuid4()}.txt"
        s3 = sess.client("s3", region_name=REGION)
        s3.put_object(Bucket=bkt, Key=key, Body=b"ok")
        st.success(f"PutObject OK → s3://{bkt}/{key}")
    except Exception as e:
        st.error(f"S3 write failed: {e}")

# 3) Athena SELECT 1
with st.expander("3) Athena SELECT 1", expanded=True):
    if st.button("Run SELECT 1"):
        try:
            df = athena_select_1(sess)
            st.success("Athena query SUCCEEDED")
            st.dataframe(df)
        except Exception as e:
            st.error(f"Athena query failed: {e}")
