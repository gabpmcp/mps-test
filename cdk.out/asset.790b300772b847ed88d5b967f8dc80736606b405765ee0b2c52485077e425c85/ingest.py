import json, os, urllib.request, gzip, datetime, hashlib, boto3, itertools

s3 = boto3.client("s3")

utc = lambda: datetime.datetime.now(datetime.timezone.utc).astimezone()
key = lambda p, src: f"{p}/{src}/dt={utc():%Y/%m/%d}/{utc():%H%M%S}-{hashlib.md5(str(utc().timestamp()).encode()).hexdigest()}.ndjson.gz"

def http_json(url):
    with urllib.request.urlopen(url) as r:
        code, body = r.getcode(), r.read()
    return dict(ok=200<=code<300, status=code, body=json.loads(body.decode()) if body else None)

def to_list(x): return x if isinstance(x, list) else ([] if x is None else [x])

# --- Normalizaciones puras ----
def norm_jsonplaceholder(items):
    xs = to_list(items)
    pick = lambda u: dict(
        id=u.get("id"),
        name=u.get("name"),
        username=u.get("username"),
        email=u.get("email"),
        phone=u.get("phone"),
        website=u.get("website"),
        company=(u.get("company") or {}).get("name"),
        city=(u.get("address") or {}).get("city"),
        source="jsonplaceholder"
    )
    return list(map(pick, xs))

def norm_randomuser(payload):
    xs = (payload or {}).get("results") or []
    pick = lambda u: dict(
        id=u.get("login",{}).get("uuid"),
        name=" ".join(filter(None, [u.get("name",{}).get("first"), u.get("name",{}).get("last")])),
        username=(u.get("login") or {}).get("username"),
        email=u.get("email"),
        phone=u.get("phone"),
        website=None,
        company=None,
        city=(u.get("location") or {}).get("city"),
        source="randomuser"
    )
    return list(map(pick, xs))

def ndjson_bytes(objs):
    # 1 objeto JSON por línea, sin envolturas
    lines = "\n".join(json.dumps(o, separators=(",", ":"), ensure_ascii=False) for o in objs)
    return gzip.compress(lines.encode())

def put_ndjson(bucket, key_, objs):
    body = ndjson_bytes(objs)
    s3.put_object(
        Bucket=bucket, Key=key_, Body=body,
        ContentType="application/x-ndjson", ContentEncoding="gzip"
    )
    return dict(ok=True, bucket=bucket, key=key_, count=len(objs))

def handler(event, _ctx):
    env = {k: os.environ[k] for k in ("BUCKET","PREFIX")}
    # fuentes
    jp = http_json("https://jsonplaceholder.typicode.com/users")
    ru = http_json("https://randomuser.me/api/?results=100")

    jp_rows = norm_jsonplaceholder(jp["body"]) if jp["ok"] else []
    ru_rows = norm_randomuser(ru["body"]) if ru["ok"] else []

    put_jp = put_ndjson(env["BUCKET"], key(env["PREFIX"], "jsonplaceholder"), jp_rows) if jp_rows else dict(ok=False, error="empty", src="jsonplaceholder")
    put_ru = put_ndjson(env["BUCKET"], key(env["PREFIX"], "randomuser"), ru_rows) if ru_rows else dict(ok=False, error="empty", src="randomuser")

    return dict(fetch=dict(jsonplaceholder=jp, randomuser=ru), put=dict(jsonplaceholder=put_jp, randomuser=put_ru))