#!/usr/bin/env bash
#
# CI script for Trellis
# Runs build, lint, test, and verification checks
#
# Usage:
#   ./scripts/ci.sh          # Run all checks
#   ./scripts/ci.sh --quick  # Skip slow checks (for pre-commit)
#

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse arguments
QUICK_MODE=false
for arg in "$@"; do
    case $arg in
        --quick)
            QUICK_MODE=true
            shift
            ;;
    esac
done

# Helper functions
info() {
    echo -e "${GREEN}==>${NC} $1"
}

warn() {
    echo -e "${YELLOW}==>${NC} $1"
}

error() {
    echo -e "${RED}==>${NC} $1"
}

check_command() {
    if ! command -v "$1" &> /dev/null; then
        error "$1 is not installed"
        return 1
    fi
}

# Track failures
FAILED=0

# Change to repo root
cd "$(dirname "$0")/.."

info "Running CI checks..."
echo ""

# 1. Check Go is installed
info "Checking Go installation..."
check_command go || exit 1
GO_VERSION=$(go version | awk '{print $3}')
echo "  Go version: $GO_VERSION"
echo ""

# 2. Check go.mod is tidy
info "Checking go.mod is tidy..."
go mod tidy
if ! git diff --quiet go.mod go.sum 2>/dev/null; then
    error "go.mod or go.sum is not tidy"
    echo "Run: go mod tidy"
    FAILED=1
else
    echo "  go.mod is tidy"
fi
echo ""

# 3. Build
info "Building..."
if ! go build ./... 2>&1; then
    error "Build failed"
    FAILED=1
else
    echo "  Build successful"
fi
echo ""

# 4. Run golangci-lint (includes gofmt, govet, staticcheck, and more)
if command -v golangci-lint &> /dev/null; then
    info "Running golangci-lint..."
    if ! golangci-lint run --timeout=5m 2>&1; then
        error "golangci-lint found issues"
        FAILED=1
    else
        echo "  No issues found"
    fi
    echo ""
else
    warn "golangci-lint not installed, falling back to basic checks"
    warn "Install: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"
    echo ""

    # Fallback: basic formatting check
    info "Checking code formatting (gofmt)..."
    UNFORMATTED=$(gofmt -l . 2>&1 | grep -v vendor || true)
    if [ -n "$UNFORMATTED" ]; then
        error "The following files are not formatted:"
        echo "$UNFORMATTED"
        echo ""
        echo "Run: gofmt -w ."
        FAILED=1
    else
        echo "  All files formatted correctly"
    fi
    echo ""

    # Fallback: go vet
    info "Running go vet..."
    if ! go vet ./... 2>&1; then
        error "go vet found issues"
        FAILED=1
    else
        echo "  No issues found"
    fi
    echo ""
fi

# 5. Run tests
if [ "$QUICK_MODE" = true ]; then
    info "Running tests (quick mode, no race detector)..."
    if ! go test ./... 2>&1; then
        error "Tests failed"
        FAILED=1
    else
        echo "  All tests passed"
    fi
else
    info "Running tests with race detector..."
    if ! go test -race -cover ./... 2>&1; then
        error "Tests failed"
        FAILED=1
    else
        echo "  All tests passed"
    fi
fi
echo ""

# 6. Check for build tags (E2E tests compile)
info "Verifying E2E tests compile..."
if ! go build -tags=e2e ./e2e/... 2>&1; then
    error "E2E tests failed to compile"
    FAILED=1
else
    echo "  E2E tests compile successfully"
fi
echo ""

# Summary
echo "----------------------------------------"
if [ $FAILED -eq 0 ]; then
    info "All checks passed!"
    exit 0
else
    error "Some checks failed"
    exit 1
fi
