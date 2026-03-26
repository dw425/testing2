#!/bin/bash
# ============================================================================
# LHO Lite — One-Command Deployment to Databricks
# ============================================================================
#
# Prerequisites:
#   1. Databricks CLI installed:  pip install databricks-cli
#   2. Authenticated:             databricks configure --token
#      OR set env vars:           DATABRICKS_HOST + DATABRICKS_TOKEN
#
# Usage:
#   ./deploy.sh                   # Deploy using default CLI profile
#   ./deploy.sh --profile PROD    # Deploy using a named profile
#
# What this does:
#   1. Uploads the LHO Lite app code to your Databricks workspace
#   2. Creates (or updates) the Databricks App
#   3. Deploys and starts it
#   4. Prints the app URL
# ============================================================================

set -e

APP_NAME="lho-lite"
PROFILE_ARG=""

# Parse args
while [[ $# -gt 0 ]]; do
  case $1 in
    --profile) PROFILE_ARG="--profile $2"; shift 2 ;;
    --name)    APP_NAME="$2"; shift 2 ;;
    -h|--help)
      echo "Usage: ./deploy.sh [--profile PROFILE_NAME] [--name APP_NAME]"
      echo ""
      echo "Options:"
      echo "  --profile   Databricks CLI profile name (default: default profile)"
      echo "  --name      App name in Databricks (default: lho-lite)"
      exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "============================================"
echo "  LHO Lite — Deploying to Databricks"
echo "============================================"
echo ""

# Check for Databricks CLI
if ! command -v databricks &> /dev/null; then
  echo "ERROR: Databricks CLI not found."
  echo "Install: pip install databricks-cli"
  exit 1
fi

# Check auth
echo "[1/3] Checking Databricks authentication..."
if ! databricks $PROFILE_ARG workspace list / &> /dev/null; then
  echo "ERROR: Cannot connect to Databricks workspace."
  echo "Run: databricks configure --token"
  exit 1
fi
echo "  ✓ Connected"

# Try DAB deploy first (preferred)
if databricks $PROFILE_ARG bundle validate &> /dev/null 2>&1; then
  echo ""
  echo "[2/3] Deploying via Databricks Asset Bundle..."
  cd "$SCRIPT_DIR"
  databricks $PROFILE_ARG bundle deploy
  echo "  ✓ Bundle deployed"

  echo ""
  echo "[3/3] Starting app..."
  databricks $PROFILE_ARG apps deploy "$APP_NAME" --source-code-path ".bundle/dev/files"
  echo ""
  echo "============================================"
  echo "  ✓ LHO Lite deployed successfully!"
  echo "  Open your Databricks workspace → Compute → Apps → $APP_NAME"
  echo "============================================"
else
  # Fallback: direct upload + app create
  WORKSPACE_PATH="/Workspace/Apps/$APP_NAME"

  echo ""
  echo "[2/3] Uploading app code to $WORKSPACE_PATH ..."
  databricks $PROFILE_ARG workspace mkdirs "$WORKSPACE_PATH" 2>/dev/null || true
  databricks $PROFILE_ARG workspace mkdirs "$WORKSPACE_PATH/app" 2>/dev/null || true

  # Upload files
  for f in app.yaml requirements.txt; do
    databricks $PROFILE_ARG workspace import "$WORKSPACE_PATH/$f" --file "$SCRIPT_DIR/$f" --overwrite --format AUTO
  done
  for f in "$SCRIPT_DIR"/app/*.py; do
    fname=$(basename "$f")
    databricks $PROFILE_ARG workspace import "$WORKSPACE_PATH/app/$fname" --file "$f" --overwrite --format AUTO
  done
  echo "  ✓ Code uploaded"

  echo ""
  echo "[3/3] Creating/updating Databricks App..."
  # Create app (ignore error if already exists)
  databricks $PROFILE_ARG apps create --json "{\"name\": \"$APP_NAME\", \"description\": \"LHO Lite — Lakehouse Optimizer\"}" 2>/dev/null || true
  # Deploy
  databricks $PROFILE_ARG apps deploy "$APP_NAME" --source-code-path "$WORKSPACE_PATH"

  echo ""
  echo "============================================"
  echo "  ✓ LHO Lite deployed successfully!"
  echo "  Open your Databricks workspace → Compute → Apps → $APP_NAME"
  echo "============================================"
fi

echo ""
echo "NEXT STEPS:"
echo "  1. Open the app in your browser"
echo "  2. The admin setup page will appear automatically"
echo "  3. Select 'Auto (SDK)' for auth — no tokens needed"
echo "  4. Grant the app's service principal workspace admin"
echo "  5. Click Save → data collection starts"
echo ""
