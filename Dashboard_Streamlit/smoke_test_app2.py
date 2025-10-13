# Dashboard_Streamlit/smoke_test_app2.py
import time
import uuid
import streamlit as st
import boto3
import pandas as pd

st.set_page_config(page_title="Athena / S3 Smoke Test", layout="centered")
st.title("🔎 Athena / S3 Smoke Test")

# ---- Secrets / defaults ----
REGION    = st.secrets.get("AWS_REGION", "us-east-1")
DATABASE  = st.secrets.get("ATHENA_DATABASE", "marketing_data")
WORKGROUP = st.secrets.get("ATHENA_WORKGROUP", "primary")
OUTPUT    = st.secrets.get("ATHENA_S3_OUTPUT", "s3://betty-athena-results/calendly/")  # must end with '/'

# ---- Helpers ----
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

def poll_until_done(athena, qid: str, timeout_s: int = 60):
    t0 = time.time()
    last = None
    while True:
        r = athena.get_query_execution(QueryExecutionId=qid)
        state = r["QueryExecution"]["Status"]["State"]
        if state != last:
            st.write("Athena state:", state)
            last = state
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return state, r["QueryExecution"]["Status"].get("StateChangeReason", "")
        if time.time() - t0 > timeout_s:
            return "TIMEOUT", "Timed out waiting for Athena (check workgroup/output/permissions)."
        time.sleep(0.5)

def fetch_df(athena, qid: str, max_rows: int = 50) -> pd.DataFrame:
    res = athena.get_query_results(QueryExecutionId=qid, MaxResults=max_rows)
    cols = [c["Label"] for c in res["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = [[d.get("VarCharValue") for d in row["Data"]] for row in res["ResultSet"]["Rows"]]
    if rows and rows[0] == cols:  # drop header row if present
        rows = rows[1:]
    return pd.DataFrame(rows, columns=cols)

# Create clients once
sess = make_session()
athena = sess.client("athena", region_name=REGION)
s3 = sess.client("s3", region_name=REGION)

# -------------------------------
# 0) Bucket & app region check
# -------------------------------
with st.expander("0) Bucket & App Regions", expanded=True):
    try:
        bkt, _ = parse_s3_uri(OUTPUT)
        loc = s3.get_bucket_location(Bucket=bkt).get("LocationConstraint") or "us-east-1"
        st.json({"APP_REGION": REGION, "BUCKET_REGION": loc, "WORKGROUP": WORKGROUP, "OUTPUT": OUTPUT})
    except Exception as e:
        st.error(f"Region check failed: {e}")

# -------------------------------
# 1) STS identity
# -------------------------------
with st.expander("1) STS identity", expanded=True):
    try:
        ident = sess.client("sts", region_name=REGION).get_caller_identity()
        st.success("STS call succeeded")
        st.json(ident)
    except Exception as e:
        st.error(f"STS failed: {e}")

# -------------------------------
# 2) S3 results bucket write test
# -------------------------------
with st.expander("2) S3 results bucket write test", expanded=True):
    try:
        assert OUTPUT.endswith("/"), "ATHENA_S3_OUTPUT must end with '/'."
        bkt, prefix = parse_s3_uri(OUTPUT)
        key = prefix + f"smoke-{uuid.uuid4()}.txt"
        s3.put_object(Bucket=bkt, Key=key, Body=b"ok")
        st.success(f"PutObject OK → s3://{bkt}/{key}")
    except Exception as e:
        st.error(f"S3 write failed: {e}")

# -------------------------------
# 3) Athena SELECT 1 (client OUTPUT)
# -------------------------------
with st.expander("3) Athena SELECT 1 (client-provided output)", expanded=True):
    if st.button("Run SELECT 1 (client output)"):
        try:
            q = athena.start_query_execution(
                QueryString="SELECT 1 AS ok",
                QueryExecutionContext={"Database": DATABASE, "Catalog": st.secrets.get("ATHENA_CATALOG","AwsDataCatalog")},
                ResultConfiguration={"OutputLocation": OUTPUT},  # use our OUTPUT
                WorkGroup=WORKGROUP,
            )
            qid = q["QueryExecutionId"]
            state, reason = poll_until_done(athena, qid)
            if state == "SUCCEEDED":
                df = fetch_df(athena, qid)
                st.success("Athena query SUCCEEDED")
                st.dataframe(df)
            else:
                st.error(f"Athena ended in {state}: {reason}")
        except Exception as e:
            st.error(f"Athena query failed: {e}")

# -------------------------------
# 4) Athena SELECT 1 (WG default)
# -------------------------------
with st.expander("4) Athena SELECT 1 (workgroup default output)", expanded=False):
    if st.button("Run SELECT 1 (WG default output)"):
        try:
            q = athena.start_query_execution(
                QueryString="SELECT 1 AS ok",
                QueryExecutionContext={"Database": DATABASE, "Catalog": st.secrets.get("ATHENA_CATALOG","AwsDataCatalog")},
                WorkGroup=WORKGROUP,
                # NOTE: no ResultConfiguration here -> use WG default
            )
            qid = q["QueryExecutionId"]
            state, reason = poll_until_done(athena, qid)
            if state == "SUCCEEDED":
                df = fetch_df(athena, qid)
                st.success("Athena query SUCCEEDED (WG default output)")
                st.dataframe(df)
            else:
                st.error(f"Athena ended in {state}: {reason}")
        except Exception as e:
            st.error(f"Athena query failed: {e}")

# -------------------------------
# 5) Workgroup configuration
# -------------------------------
with st.expander("5) Workgroup configuration", expanded=False):
    try:
        wg = athena.get_work_group(WorkGroup=WORKGROUP)
        cfg = wg["WorkGroup"]["Configuration"]
        st.json({
            "EnforceWorkGroupConfiguration": cfg.get("EnforceWorkGroupConfiguration"),
            "WG_OutputLocation": cfg.get("ResultConfiguration", {}).get("OutputLocation"),
            "Client_Output": OUTPUT,
        })
    except Exception as e:
        st.error(f"GetWorkGroup failed: {e}")
