#!/usr/bin/env bash
# Print the docs/CHANGELOG.md section body for a version, e.g.:
#   ./scripts/changelog-section.sh 1.0.0
# Used for GitHub release notes and the Zulip announcement.
set -euo pipefail

VERSION="${1:?usage: changelog-section.sh VERSION}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHANGELOG="${ROOT}/docs/CHANGELOG.md"

awk -v ver="$VERSION" '
  BEGIN { esc = ver; gsub(/\./, "\\.", esc) }
  # Start after the "## [VERSION]" header.
  $0 ~ "^## \\[" esc "\\]" { f = 1; next }
  # Stop at the next version header or the link-reference block.
  f && /^## \[/ { exit }
  f && /^\[[^]]+\]:[[:space:]]/ { exit }
  # Skip leading blank lines, then print the body.
  f { if (!seen && $0 ~ /^[[:space:]]*$/) next; seen = 1; print }
' "$CHANGELOG"
