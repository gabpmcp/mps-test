#!/usr/bin/env bash
set -euo pipefail

PROFILE="${PROFILE:-test}"
REGION="${REGION:-us-east-1}"
DOMAIN_NAME="${DOMAIN_NAME:-data-test-domain}"
PROJECT_NAME="${PROJECT_NAME:-data-test-project}"
ENV_NAME="${ENV_NAME:-env-analytics}"
DATASOURCE_NAME="${DATASOURCE_NAME:-users-glue-source}"
GLUE_DB="${GLUE_DB:-users_db}"
PUBLISH_ON_IMPORT="${PUBLISH_ON_IMPORT:-true}"   # auto-publicar assets al importar

awsz(){ aws --profile "$PROFILE" --region "$REGION" "$@"; }

echo ">> Looking for domain named '$DOMAIN_NAME'..."
DOMAIN_ID="$(awsz datazone list-domains --query "items[?name=='$DOMAIN_NAME'].id | [0]" --output text 2>/dev/null || true)"
if [[ -z "${DOMAIN_ID}" || "${DOMAIN_ID}" == "None" ]]; then
  echo "!! No domain found. Creating template 'domain.json' (one-time)."
  awsz datazone create-domain --generate-cli-skeleton input > domain.json

  cat <<'JSON' > patch-domain.jq
  .name = env.DOMAIN_NAME
  | .description = "Demo domain for users pipeline"
  # Si tu región/entorno exige roles explícitos, ajusta estas claves:
  # | .domainExecutionRole = "arn:aws:iam::<ACCOUNT_ID>:role/<YourDataZoneDomainExecRole>"
  # | .serviceRole = "arn:aws:iam::<ACCOUNT_ID>:role/<YourDataZoneServiceRole>"
JSON

  DOMAIN_JSON=$(jq -f patch-domain.jq domain.json)
  echo "$DOMAIN_JSON" > domain.json

  echo ">> Creating domain (this can take ~1-2 min depending on region/policies)..."
  DOMAIN_OUT="$(awsz datazone create-domain --cli-input-json file://domain.json)"
  DOMAIN_ID="$(jq -r '.id' <<<"$DOMAIN_OUT")"
else
  echo ">> Reusing domain id: $DOMAIN_ID"
fi

echo ">> Looking for project '$PROJECT_NAME'..."
PROJECT_ID="$(awsz datazone list-projects --domain-identifier "$DOMAIN_ID" --query "items[?name=='$PROJECT_NAME'].id | [0]" --output text || true)"
if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "None" ]]; then
  PROJECT_OUT="$(awsz datazone create-project \
    --domain-identifier "$DOMAIN_ID" \
    --name "$PROJECT_NAME" \
    --description "Project for DataZone integration demo")"
  PROJECT_ID="$(jq -r '.id' <<<"$PROJECT_OUT")"
  echo ">> Created project: $PROJECT_ID"
else
  echo ">> Reusing project: $PROJECT_ID"
fi

echo ">> Discovering environment blueprint..."
BP_ID="$(awsz datazone list-environment-blueprints --domain-identifier "$DOMAIN_ID" \
  --query "items[?managed==\`true\`].id" --output text | tr '\t' '\n' | head -n1)"

if [[ -z "${BP_ID}" ]]; then
  echo "!! No managed blueprint found. Listing all for debugging:"
  awsz datazone list-environment-blueprints --domain-identifier "$DOMAIN_ID" --output table
  echo "Aborting. Please pick a blueprint id and set BP_ID env var."
  exit 1
fi
echo ">> Using blueprint: $BP_ID"

echo ">> Creating environment profile (if not exists)..."
EP_ID="$(awsz datazone create-environment-profile \
  --domain-identifier "$DOMAIN_ID" \
  --name "analytics-profile" \
  --project-identifier "$PROJECT_ID" \
  --environment-blueprint-identifier "$BP_ID" \
  --query 'id' --output text 2>/dev/null || true || echo "")"

if [[ -z "$EP_ID" || "$EP_ID" == "None" ]]; then
  # Some regions returns 409 if already exists; retry to list.
  EP_ID="$(awsz datazone list-environment-blueprint-configurations \
    --domain-identifier "$DOMAIN_ID" \
    --query "items[?environmentBlueprintId=='$BP_ID'].environmentProfileId | [0]" \
    --output text 2>/dev/null || true)"
fi
[[ -z "$EP_ID" || "$EP_ID" == "None" ]] && { echo "!! Could not resolve Environment Profile ID"; exit 1; }
echo ">> Environment profile: $EP_ID"

echo ">> Creating environment '$ENV_NAME' (if not exists)..."
ENV_ID="$(awsz datazone list-environments --domain-identifier "$DOMAIN_ID" \
  --query "items[?name=='$ENV_NAME'].id | [0]" --output text || true)"
if [[ -z "$ENV_ID" || "$ENV_ID" == "None" ]]; then
  ENV_OUT="$(awsz datazone create-environment \
    --domain-identifier "$DOMAIN_ID" \
    --project-identifier "$PROJECT_ID" \
    --environment-profile-identifier "$EP_ID" \
    --name "$ENV_NAME")"
  ENV_ID="$(jq -r '.id' <<<"$ENV_OUT")"
  echo ">> Created environment: $ENV_ID"
else
  echo ">> Reusing environment: $ENV_ID"
fi

echo ">> Creating Data Source '$DATASOURCE_NAME' targeting Glue DB '$GLUE_DB'..."
DS_ID="$(awsz datazone list-data-sources --domain-identifier "$DOMAIN_ID" \
  --project-identifier "$PROJECT_ID" \
  --query "items[?name=='$DATASOURCE_NAME'].id | [0]" --output text || true)"

if [[ -z "$DS_ID" || "$DS_ID" == "None" ]]; then
  awsz datazone create-data-source --generate-cli-skeleton input > ds.json

  cat <<JSON > patch-ds.jq
  .name = env.DATASOURCE_NAME
  | .description = "Glue database $GLUE_DB"
  | .domainIdentifier = env.DOMAIN_ID
  | .projectIdentifier = env.PROJECT_ID
  | .environmentIdentifier = env.ENV_ID
  | .publishOnImport = (${PUBLISH_ON_IMPORT,,} | ascii_downcase == "true")
  | .type = "GLUE"
  | .configuration = {
      "glueRunConfiguration": {
        "relationalFilterConfigurations": [
          { "databaseName": env.GLUE_DB }
        ]
      }
    }
JSON

  jq -f patch-ds.jq ds.json > ds.final.json
  DS_OUT="$(awsz datazone create-data-source --cli-input-json file://ds.final.json)"
  DS_ID="$(jq -r '.id' <<<"$DS_OUT")"
  echo ">> Created data source: $DS_ID"
else
  echo ">> Reusing data source: $DS_ID"
fi

echo ">> Starting data source run..."
RUN_OUT="$(awsz datazone start-data-source-run \
  --domain-identifier "$DOMAIN_ID" \
  --data-source-identifier "$DS_ID")"
RUN_ID="$(jq -r '.id' <<<"$RUN_OUT")"
echo ">> Run id: $RUN_ID"

echo -n ">> Waiting run to complete"
while :; do
  STATE="$(awsz datazone get-data-source-run --domain-identifier "$DOMAIN_ID" --identifier "$RUN_ID" --query 'status' --output text)"
  [[ "$STATE" == "SUCCEEDED" ]] && { echo; echo ">> Run SUCCEEDED"; break; }
  [[ "$STATE" == "FAILED" || "$STATE" == "CANCELLED" ]] && {
    echo; echo "!! Run $STATE"
    awsz datazone get-data-source-run --domain-identifier "$DOMAIN_ID" --identifier "$RUN_ID" --output json
    exit 1
  }
  echo -n "."
  sleep 5
done

echo ">> Assets in domain (first 10):"
awsz datazone list-assets --domain-identifier "$DOMAIN_ID" --max-results 10 --output table || true

echo
echo "All done ✅"
echo "Domain:      $DOMAIN_ID"
echo "Project:     $PROJECT_ID"
echo "Environment: $ENV_ID"
echo "DataSource:  $DS_ID"
echo "Run:         $RUN_ID"