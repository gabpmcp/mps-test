# Data Pipeline (AWS CDK · Python, FP style)

Serverless pipeline: Lambda → S3 (NDJSON.GZ) → Glue Crawler → Glue Catalog → Athena.
Sources: JSONPlaceholder and RandomUser. Infra is declared with a functional CDK style (no classes).

## Architecture
```text
[JSONPlaceholder]        [RandomUser]
       │                       │
       └───(Lambda: ingest)────┘
                │ writes NDJSON.GZ
                ▼
           S3: s3://<RAW>/raw/users/
              ├─ jsonplaceholder/dt=YYYY/MM/DD/*.ndjson.gz
              ├─ randomuser/dt=YYYY/MM/DD/*.ndjson.gz
              └─ (legacy) dt=YYYY/.../*.json.gz
                │
                ▼
      Glue Crawler → Glue Database: users_db
                │
                ▼
     Athena Workgroup: wg-data-test → Queries
```

Prereqs
 * AWS CLI v2 + credentials (e.g., --profile test)
 * CDK v2 (bootstrapped in the account/region)
 * Python 3.10+
 * Region (example uses us-east-1)

## Layout
```text
.
├─ app.py
├─ stacks/
│  └─ fn_stack.py           # FP-style CDK (no classes)
└─ lambda_src/
   └─ ingest.py             # Ingests both APIs → S3 (NDJSON.GZ)
```

## Quickstart
```bash
export CDK_DEFAULT_ACCOUNT=$(aws sts get-caller-identity --query Account --output text --profile test)
export CDK_DEFAULT_REGION=us-east-1

cdk synth
cdk deploy --profile test
```

## Stack Outputs to capture

```bash
STACK=DataTest
RAW=$(aws cloudformation describe-stacks --stack-name $STACK \
  --query "Stacks[0].Outputs[?OutputKey=='RawBucket'].OutputValue" --profile test --output text)
LAMBDA=$(aws cloudformation describe-stacks --stack-name $STACK \
  --query "Stacks[0].Outputs[?OutputKey=='LambdaName'].OutputValue" --profile test --output text)
CRAWLER=$(aws cloudformation describe-stacks --stack-name $STACK \
  --query "Stacks[0].Outputs[?OutputKey=='GlueCrawler'].OutputValue" --profile test --output text)
WG=$(aws cloudformation describe-stacks --stack-name $STACK \
  --query "Stacks[0].Outputs[?OutputKey=='AthenaWG'].OutputValue" --profile test --output text)
echo RAW=$RAW; echo LAMBDA=$LAMBDA; echo CRAWLER=$CRAWLER; echo WG=$WG
```

## Ingest & Verify

```bash
# 1) trigger ingestion
aws lambda invoke --function-name "$LAMBDA" out.json --profile test >/dev/null
cat out.json

# 2) ensure S3 objects exist
aws s3 ls "s3://$RAW/raw/users/" --recursive --profile test

# 3) run crawler (creates/updates tables)
aws glue start-crawler --name "$CRAWLER" --profile test
until [ "$(aws glue get-crawler --name "$CRAWLER" --query 'Crawler.State' --profile test --output text)" = "READY" ]; do sleep 5; done

# 4) list tables
aws glue get-tables --database-name users_db --query 'TableList[].Name' --profile test
# expected:
# [ "users_dt_2025", "users_jsonplaceholder", "users_randomuser" ]

# 5) Athena count(*) per table
TABLES=$(aws glue get-tables --database-name users_db --query 'TableList[].Name' --output text --profile test)
for T in $TABLES; do
  QID=$(aws athena start-query-execution \
    --work-group "$WG" \
    --query-string "SELECT COUNT(*) AS n FROM users_db.\"$T\";" \
    --query-execution-context Database=users_db \
    --profile test --output json | jq -r .QueryExecutionId)
  until [ "$(aws athena get-query-execution --query-execution-id "$QID" --query 'QueryExecution.Status.State' --profile test --output text)" = "SUCCEEDED" ]; do sleep 2; done
  echo "Table $T →"; aws athena get-query-results --query-execution-id "$QID" --profile test --output text
done
```

## Peek rows (Athena)

```bash
# pick one table
TABLE=$(aws glue get-tables --database-name users_db --query 'TableList[0].Name' --profile test --output text)
QID=$(aws athena start-query-execution \
  --work-group "$WG" \
  --query-string "SELECT name, username, email, city, source FROM users_db.\"$TABLE\" LIMIT 5;" \
  --query-execution-context Database=users_db \
  --profile test --output json | jq -r .QueryExecutionId)
until [ "$(aws athena get-query-execution --query-execution-id "$QID" --query 'QueryExecution.Status.State' --profile test --output text)" = "SUCCEEDED" ]; do sleep 2; done
aws athena get-query-results --query-execution-id "$QID" --profile test --output table
```

## Troubleshooting
 * Crawler FAILED with logs AccessDenied
Fixed already by adding:
 * Managed policy service-role/AWSGlueServiceRole to the Crawler role
 * Inline policy for logs:CreateLogGroup/CreateLogStream/PutLogEvents on /aws-glue/*
 * No tables appear
Ensure files exist in s3: `//$RAW/raw/users/...` and rerun crawler after ingestion.
Check last crawl:
```bash
aws glue get-crawler --name "$CRAWLER" \
  --query 'Crawler.LastCrawl.{Status:Status,Summary:ErrorMessage,Start:StartTime,End:EndTime}' \
  --profile test --output table
```

 * Athena results empty
Re-run Lambda, wait a few seconds, run crawler again, then re-query.
 * Lake Formation errors
This stack is IAM‑only (no LF resources). Ensure Lake Formation Settings → Use only IAM access control is enabled.

## Notes (FP style)
 * CDK designed with function constructors and immutable data (no subclassing).
 * Lambda is pure fetch → normalize → write with NDJSON for optimal Glue inference.
 * S3 partitioning by date (dt=YYYY/MM/DD) and per‑source subfolders keeps things tidy and scalable.