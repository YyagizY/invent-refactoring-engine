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
  for rule in structural_refactor.mdc contextual_refactor.mdc; do
    if [[ -f "$RULES_SRC/$rule" ]]; then
      cp -f "$RULES_SRC/$rule" "$RULES_DEST/"
      echo "Installed: .cursor/rules/$rule"
    fi
  done
else
  echo "Error: Rules not found at $RULES_SRC" >&2
  exit 1
fi

echo "Done. Use in Cursor: structural_refactor path/to/script.py | contextual_refactor path/to/script.py"
