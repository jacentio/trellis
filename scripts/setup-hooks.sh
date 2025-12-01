#!/usr/bin/env bash
#
# Setup script to install git hooks
#

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOOKS_DIR="$REPO_ROOT/.git/hooks"

echo "Installing git hooks..."

# Ensure hooks directory exists
mkdir -p "$HOOKS_DIR"

# Install pre-commit hook
cp "$REPO_ROOT/scripts/pre-commit" "$HOOKS_DIR/pre-commit"
chmod +x "$HOOKS_DIR/pre-commit"

echo "  Installed pre-commit hook"
echo ""
echo "Git hooks installed successfully!"
echo ""
echo "The pre-commit hook will run CI checks before each commit."
echo "To bypass (not recommended): git commit --no-verify"
