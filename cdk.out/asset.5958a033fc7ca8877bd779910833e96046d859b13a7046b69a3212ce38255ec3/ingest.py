import json, os, urllib.request, gzip, datetime, hashlib, boto3
s3 = boto3.client("s3")

now_iso = lambda: datetime.datetime.utcnow().isoformat(timespec="seconds")+"Z"
to_bytes = lambda x: x if isinstance(x,(bytes,bytearray)) else json.dumps(x,separators=(",",":")).encode()
key_for  = lambda p: f"{p}/dt={datetime.datetime.utcnow():%Y/%m/%d}/{datetime.datetime.utcnow():%H%M%S}-{hashlib.md5(str(datetime.datetime.utcnow().timestamp()).encode()).hexdigest()}.json.gz"

def fetch(url):
    with urllib.request.urlopen(url) as r:
        code, body = r.getcode(), r.read()
    return dict(ok=200<=code<300, status=code, body=json.loads(body.decode()) if body else [])

def transform(records): return records if isinstance(records,list) else [records]

def write_gzip(bucket,key,records):
    payload = gzip.compress(to_bytes(dict(ingested_at=now_iso(), records=records)))
    s3.put_object(Bucket=bucket, Key=key, Body=payload, ContentType="application/json", ContentEncoding="gzip")
    return dict(ok=True, bucket=bucket, key=key, count=len(records))

def handler(event,_):
    env = {k: os.environ[k] for k in ("API_URL","BUCKET","PREFIX")}
    res  = fetch(env["API_URL"])
    data = transform(res["body"]) if res["ok"] else []
    put  = write_gzip(env["BUCKET"], key_for(env["PREFIX"]), data) if data else dict(ok=False, error="empty")
    return dict(fetch=res, put=put)