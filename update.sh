#!/usr/bin/env bash
# Update Invent Refactoring Engine and reinstall Cursor rules
# Run from project root: ./.invent-refactoring-engine/update.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ -d ".git" ]]; then
  echo "Updating repository..."
  git pull origin main 2>/dev/null || git pull origin master 2>/dev/null || true
else
  echo "Not a git repository. Skip update."
fi

echo "Reinstalling Cursor rules..."
"$SCRIPT_DIR/install.sh"
