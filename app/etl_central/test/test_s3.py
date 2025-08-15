import os, re, boto3, pytest

CP_RE = re.compile(r"formato_4_balance_presupuestario_-_ldf_cp(\d{4})\.xlsx", re.I)

def test_s3_list_and_match_cp_prefix():
    bucket = os.getenv("BUCKET_NAME", "centralfiles3")
    prefix = "finanzas/Balance_Presupuestario/raw/"
    s3 = boto3.client("s3")

    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    matched = []
    for page in pages:
        for obj in page.get("Contents", []):
            fname = obj["Key"].split("/")[-1]
            if CP_RE.match(fname):
                matched.append(fname)
    assert len(matched) >= 1, "No CP files found; upload at least one CP .xlsx for tests."

def test_s3_can_read_single_cp_file():
    bucket = os.getenv("BUCKET_NAME", "centralfiles3")
    prefix = "finanzas/Balance_Presupuestario/raw/"
    s3 = boto3.client("s3")
    # pick first match
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    target = None
    for page in pages:
        for obj in page.get("Contents", []):
            fname = obj["Key"].split("/")[-1]
            if CP_RE.match(fname):
                target = obj["Key"]; break
        if target: break
    assert target, "No CP file to read."

    # read object head (permissions) and a few bytes
    s3.head_object(Bucket=bucket, Key=target)
    obj = s3.get_object(Bucket=bucket, Key=target)
    peek = obj["Body"].read(64)
    assert isinstance(peek, (bytes, bytearray))
