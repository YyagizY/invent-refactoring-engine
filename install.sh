#!/usr/bin/env bash
# Install Invent Refactoring Engine rules into the project's .cursor/rules/
# Run from project root: ./.invent-refactoring-engine/install.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RULES_SRC="$SCRIPT_DIR/.cursor/rules"
RULES_DEST="$PROJECT_ROOT/.cursor/rules"

mkdir -p "$RULES_DEST"

if [[ -d "$RULES_SRC" ]]; then
  cp -f "$RULES_SRC"/structural_refactor.mdc "$RULES_DEST/"
  echo "Installed: .cursor/rules/structural_refactor.mdc"
else
  echo "Error: Rules not found at $RULES_SRC" >&2
  exit 1
fi

echo "Done. Open your project in Cursor and use: structural_refactor path/to/pre_etl_scope.py"
