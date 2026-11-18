#!/usr/bin/env bash
# `make pull_core RAYFORCE_REF=...` must check out a branch, a tag and a full
# commit hash of the core. Needs network access to RAYFORCE_GITHUB.
#   make check-core-ref
set -uo pipefail

MAKEFILE="$(cd "$(dirname "$0")/.." && pwd)/Makefile"
URL="${RAYFORCE_GITHUB:-https://github.com/RayforceDB/rayforce.git}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

fail=0
# pull REF WANT-SHA — check REF out into a scratch tree and compare HEAD
pull() {
  local ref="$1" want="$2" got
  rm -rf "$WORK/test"
  if (cd "$WORK" && make -s -f "$MAKEFILE" pull_core RAYFORCE_GITHUB="$URL" RAYFORCE_REF="$ref" >"$WORK/log" 2>&1); then
    got="$(git -C "$WORK/test/tmp/rayforce-c" rev-parse HEAD)"
  else
    got="(pull_core failed: $(tail -n 1 "$WORK/log"))"
  fi
  if [[ "$got" == "$want" ]]; then echo "  ok    RAYFORCE_REF=$ref"; else echo "  FAIL  RAYFORCE_REF=$ref: got $got, want $want" >&2; fail=1; fi
}

# sha_of TAG — the commit a tag points at
sha_of() { git ls-remote "$URL" "refs/tags/$1^{}" "refs/tags/$1" | tail -n 1 | cut -f1; }

branch="$(git ls-remote --symref "$URL" HEAD | awk '/^ref:/ { sub("refs/heads/", "", $2); print $2 }')"
tags="$(git ls-remote --tags --refs "$URL" 'v[0-9]*.[0-9]*.[0-9]*' | sed 's@.*refs/tags/v@@' | sort -t. -k1,1n -k2,2n -k3,3n | sed 's@^@v@')"
tag="$(echo "$tags" | tail -n 1)"
old_tag="$(echo "$tags" | tail -n 2 | head -n 1)"
[[ -n "$branch" && -n "$tag" && "$tag" != "$old_tag" ]] || { echo "cannot read a branch and two tags from $URL" >&2; exit 1; }
branch_sha="$(git ls-remote "$URL" "refs/heads/$branch" | cut -f1)"

pull "$branch" "$branch_sha"
pull "$tag" "$(sha_of "$tag")"
# a hash that is not the tip of any branch
pull "$(sha_of "$old_tag")" "$(sha_of "$old_tag")"

# An old released core lacks RAY_POLL_AUTH_API. The server must still build,
# accept a no-password listener, and refuse -q with -u/-U at startup.
echo "  old-core password behavior:"
rm -rf "$WORK/old"
if (cd "$WORK" && make -s -f "$MAKEFILE" rayforce RAYFORCE_GITHUB="$URL" RAYFORCE_REF=v3.0.2 ROOT="$WORK/old" >"$WORK/old.log" 2>&1); then
  oldcc="${CC:-cc}"
  if (cd "$WORK/old" && "$oldcc" -std=c17 -O2 -fPIC -Wall -Wextra -Werror -Wno-unused-function -I. -Itest/tmp/rayforce-c/include -Itest/tmp/rayforce-c/src -o test/driver q.c q_server.c embed/rayforce_q.c test/driver.c test/tmp/rayforce-c/librayforce.a -lm -lpthread && ./test/driver --q-auth-selftest >"$WORK/old-suite.log" 2>&1); then
    echo "  ok    old-core q authentication selftest"
  else
    echo "  FAIL  old-core q authentication selftest" >&2
    cat "$WORK/old-suite.log" >&2
    fail=1
  fi
  if (cd "$WORK/old" && make -s -f "$MAKEFILE" check-binary ROOT="$WORK/old" RAYFORCE_Q_OLD_CORE=1 >"$WORK/old-binary.log" 2>&1); then
    echo "  ok    old-core -q -u/-U refuse to start"
  else
    echo "  FAIL  old-core -q -u/-U did not fail closed" >&2
    tail -n 15 "$WORK/old-binary.log" >&2
    fail=1
  fi
else
  echo "  FAIL  old-core q build" >&2
  tail -n 15 "$WORK/old.log" >&2
  fail=1
fi
exit "$fail"
