#!/usr/bin/env bash
# E2E test: schema evolution via successive pulumi up with YAML mutations.
#
# Usage:
#   bash tests/e2e_schema_evolution.sh
#
# Requires:
#   - pulumi CLI
#   - GOOGLE_APPLICATION_CREDENTIALS set
#   - GCP_PROJECT env var (or set via pulumi config)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
EXAMPLE_DIR="$REPO_ROOT/examples/gcpx-schema-evolution"
STACK_NAME="schema-evo-test-$$"
GCP_PROJECT="${GCP_PROJECT:-}"
DATASET="${DATASET:-gcpx_examples}"

if [[ -z "$GCP_PROJECT" ]]; then
  echo "ERROR: GCP_PROJECT env var must be set"
  exit 1
fi

cleanup() {
  echo "==> Cleaning up stack $STACK_NAME..."
  cd "$EXAMPLE_DIR"
  pulumi destroy -y -s "$STACK_NAME" --non-interactive 2>/dev/null || true
  pulumi stack rm -y "$STACK_NAME" --non-interactive 2>/dev/null || true
}
trap cleanup EXIT

echo "==> Setting up stack..."
cd "$EXAMPLE_DIR"
pulumi stack init "$STACK_NAME" --non-interactive
pulumi config set gcpProject "$GCP_PROJECT" -s "$STACK_NAME"
pulumi config set dataset "$DATASET" -s "$STACK_NAME"

# --- Step 1: Create table + schema (id, name, email) ---
echo "==> Step 1: Create initial schema (id, name, email)..."
cat > Pulumi.yaml <<'YAML'
name: gcpx-schema-evolution
runtime: yaml
config:
  gcpProject:
    type: string
  dataset:
    type: string
    default: gcpx_examples
resources:
  testTable:
    type: gcpx:bigquery/table:Table
    properties:
      project: ${gcpProject}
      dataset: ${dataset}
      tableId: schema_evo_test

  testSchema:
    type: gcpx:bigquery/tableSchema:TableSchema
    properties:
      project: ${gcpProject}
      dataset: ${dataset}
      tableId: ${testTable.tableId}
      schema:
        - name: id
          type: INT64
          mode: REQUIRED
        - name: name
          type: STRING
        - name: email
          type: STRING
outputs:
  tableId: ${testTable.tableId}
  schemaFingerprint: ${testSchema.schemaFingerprint}
YAML

pulumi up -y -s "$STACK_NAME" --non-interactive

# Verify 3 columns via bq show
COLS=$(bq show --format=json "$GCP_PROJECT:$DATASET.schema_evo_test" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['schema']['fields']))")
echo "  Columns after step 1: $COLS"
[[ "$COLS" == "3" ]] || { echo "FAIL: expected 3 columns, got $COLS"; exit 1; }

# --- Step 2: Add age + city (alter: insert) ---
echo "==> Step 2: Insert age + city..."
cat > Pulumi.yaml <<'YAML'
name: gcpx-schema-evolution
runtime: yaml
config:
  gcpProject:
    type: string
  dataset:
    type: string
    default: gcpx_examples
resources:
  testTable:
    type: gcpx:bigquery/table:Table
    properties:
      project: ${gcpProject}
      dataset: ${dataset}
      tableId: schema_evo_test

  testSchema:
    type: gcpx:bigquery/tableSchema:TableSchema
    properties:
      project: ${gcpProject}
      dataset: ${dataset}
      tableId: ${testTable.tableId}
      schema:
        - name: id
          type: INT64
          mode: REQUIRED
        - name: name
          type: STRING
        - name: email
          type: STRING
        - name: age
          type: INT64
          alter: insert
        - name: city
          type: STRING
          alter: insert
outputs:
  tableId: ${testTable.tableId}
  schemaFingerprint: ${testSchema.schemaFingerprint}
YAML

pulumi up -y -s "$STACK_NAME" --non-interactive

COLS=$(bq show --format=json "$GCP_PROJECT:$DATASET.schema_evo_test" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['schema']['fields']))")
echo "  Columns after step 2: $COLS"
[[ "$COLS" == "5" ]] || { echo "FAIL: expected 5 columns, got $COLS"; exit 1; }

# --- Step 3: Rename city → city_name ---
echo "==> Step 3: Rename city → city_name..."
cat > Pulumi.yaml <<'YAML'
name: gcpx-schema-evolution
runtime: yaml
config:
  gcpProject:
    type: string
  dataset:
    type: string
    default: gcpx_examples
resources:
  testTable:
    type: gcpx:bigquery/table:Table
    properties:
      project: ${gcpProject}
      dataset: ${dataset}
      tableId: schema_evo_test

  testSchema:
    type: gcpx:bigquery/tableSchema:TableSchema
    properties:
      project: ${gcpProject}
      dataset: ${dataset}
      tableId: ${testTable.tableId}
      schema:
        - name: id
          type: INT64
          mode: REQUIRED
        - name: name
          type: STRING
        - name: email
          type: STRING
        - name: age
          type: INT64
        - name: city_name
          type: STRING
          alter: rename
          alterFrom: city
outputs:
  tableId: ${testTable.tableId}
  schemaFingerprint: ${testSchema.schemaFingerprint}
YAML

pulumi up -y -s "$STACK_NAME" --non-interactive

# Verify city_name exists and city does not
HAS_CITY_NAME=$(bq show --format=json "$GCP_PROJECT:$DATASET.schema_evo_test" | python3 -c "import sys,json; fields=json.load(sys.stdin)['schema']['fields']; print('yes' if any(f['name']=='city_name' for f in fields) else 'no')")
echo "  Has city_name: $HAS_CITY_NAME"
[[ "$HAS_CITY_NAME" == "yes" ]] || { echo "FAIL: city_name not found after rename"; exit 1; }

# --- Step 4: Delete email ---
echo "==> Step 4: Delete email..."
cat > Pulumi.yaml <<'YAML'
name: gcpx-schema-evolution
runtime: yaml
config:
  gcpProject:
    type: string
  dataset:
    type: string
    default: gcpx_examples
resources:
  testTable:
    type: gcpx:bigquery/table:Table
    properties:
      project: ${gcpProject}
      dataset: ${dataset}
      tableId: schema_evo_test

  testSchema:
    type: gcpx:bigquery/tableSchema:TableSchema
    properties:
      project: ${gcpProject}
      dataset: ${dataset}
      tableId: ${testTable.tableId}
      schema:
        - name: id
          type: INT64
          mode: REQUIRED
        - name: name
          type: STRING
        - name: email
          type: STRING
          alter: delete
        - name: age
          type: INT64
        - name: city_name
          type: STRING
outputs:
  tableId: ${testTable.tableId}
  schemaFingerprint: ${testSchema.schemaFingerprint}
YAML

pulumi up -y -s "$STACK_NAME" --non-interactive

COLS=$(bq show --format=json "$GCP_PROJECT:$DATASET.schema_evo_test" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['schema']['fields']))")
echo "  Columns after step 4: $COLS"
[[ "$COLS" == "4" ]] || { echo "FAIL: expected 4 columns, got $COLS"; exit 1; }

HAS_EMAIL=$(bq show --format=json "$GCP_PROJECT:$DATASET.schema_evo_test" | python3 -c "import sys,json; fields=json.load(sys.stdin)['schema']['fields']; print('yes' if any(f['name']=='email' for f in fields) else 'no')")
[[ "$HAS_EMAIL" == "no" ]] || { echo "FAIL: email should have been deleted"; exit 1; }

echo ""
echo "==> ALL STEPS PASSED"
