#!/usr/bin/env bash
# Announce a release on Zulip, using the same releases bot as the Python lib.
#
#   ./scripts/announce-release.sh VERSION BOT_API_KEY [DEBUG]
#
# The message body is the matching docs/CHANGELOG.md section. Destination is
# overridable via ZULIP_STREAM / ZULIP_TOPIC (default: Announcements / Rayforce-Q).
set -euo pipefail

VERSION="${1:?usage: announce-release.sh VERSION BOT_API_KEY [DEBUG]}"
BOT_API_KEY="${2:-}"
DEBUG="${3:-}"

if [ -z "$BOT_API_KEY" ]; then
  echo "Error: BOT_API_KEY is required" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NOTES="$("${SCRIPT_DIR}/changelog-section.sh" "$VERSION" 2>/dev/null || true)"

STREAM="${ZULIP_STREAM:-Announcements}"
TOPIC="${ZULIP_TOPIC:-Rayforce-Q}"

CONTENT="**New Rayforce-Q version v${VERSION} is released!**"
if [ -n "$NOTES" ]; then
  CONTENT="${CONTENT}

${NOTES}"
fi

if [ "$DEBUG" = 1 ]; then
  echo "stream=${STREAM} topic=${TOPIC}"
  echo "${CONTENT}"
  exit 0
fi

curl -sS -X POST https://rayforcedb.zulipchat.com/api/v1/messages \
  -u "releases-bot@rayforcedb.zulipchat.com:${BOT_API_KEY}" \
  -d type=stream \
  --data-urlencode "to=${STREAM}" \
  --data-urlencode "topic=${TOPIC}" \
  --data-urlencode "content=${CONTENT}"

echo ""
echo "✅ Announcement sent to Zulip (${STREAM} > ${TOPIC})"
