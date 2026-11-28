#!/usr/bin/env bash
# Smoke-test the CGI binary with CGI environment variables (no lighttpd).

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
CGI="${CGI:-$SCRIPT_DIR/../cgi/csvdb.cgi}"
TMPDIR_CGI=$(mktemp -d)
trap 'rm -rf "$TMPDIR_CGI"' EXIT

if [[ ! -x "$CGI" ]]; then
    echo "CGI binary not found at $CGI (run: make cgi)" >&2
    exit 1
fi

die() {
    printf "${RED}CGI check failed:${NC} %s\n" "$1" >&2
    exit 1
}

body_of() {
    # Split CGI stdout on the blank line after headers
    awk 'BEGIN{h=1} h&&NF==0{h=0;next} !h{print}'
}

# Usage: run_cgi ENV_ASSIGNMENTS... -- [stdin via pipe]
run_cgi() {
    local out="$TMPDIR_CGI/out"
    local -a env_args=()
    while [[ $# -gt 0 ]]; do
        case "$1" in
            *=*) env_args+=("$1"); shift ;;
            --) shift; break ;;
            *) break ;;
        esac
    done
    env -i PATH="$PATH" CSVDB_DATA_DIR="$SCRIPT_DIR" "${env_args[@]}" \
        "$CGI" >"$out"
    cat "$out"
}

echo "CGI binary: $CGI"

# GET ?query=...&format=csv
out=$(run_cgi REQUEST_METHOD=GET QUERY_STRING='query=SELECT%201%20AS%20n&format=csv')
echo "$out" | grep -qi 'Content-Type: text/csv' || die "GET format=csv missing Content-Type"
body=$(printf '%s\n' "$out" | body_of)
[[ "$body" == $'n\n1' ]] || die "GET format=csv body unexpected: [$body]"

# GET with Accept: text/csv
out=$(run_cgi REQUEST_METHOD=GET QUERY_STRING='query=SELECT%202%20AS%20n' HTTP_ACCEPT='text/csv')
echo "$out" | grep -qi 'Content-Type: text/csv' || die "Accept: text/csv missing Content-Type"
body=$(printf '%s\n' "$out" | body_of)
[[ "$body" == $'n\n2' ]] || die "Accept: text/csv body unexpected: [$body]"

# POST application/x-www-form-urlencoded (HTML form)
form='query=SELECT+5+AS+n&format=csv'
out=$(printf '%s' "$form" | run_cgi \
    REQUEST_METHOD=POST \
    CONTENT_TYPE='application/x-www-form-urlencoded' \
    CONTENT_LENGTH=${#form} \
    QUERY_STRING='')
echo "$out" | grep -qi 'Content-Type: text/csv' || die "POST form missing Content-Type"
body=$(printf '%s\n' "$out" | body_of)
[[ "$body" == $'n\n5' ]] || die "POST form body unexpected: [$body]"

# POST text/sql + Accept: text/csv
sql='SELECT 3 AS n'
out=$(printf '%s' "$sql" | run_cgi \
    REQUEST_METHOD=POST \
    CONTENT_TYPE='text/sql' \
    CONTENT_LENGTH=${#sql} \
    HTTP_ACCEPT='text/csv' \
    QUERY_STRING='')
echo "$out" | grep -qi 'Content-Type: text/csv' || die "POST text/sql missing Content-Type"
body=$(printf '%s\n' "$out" | body_of)
[[ "$body" == $'n\n3' ]] || die "POST text/sql body unexpected: [$body]"

# POST text/sql; charset=utf-8 + ?format=json
sql='SELECT 4 AS n'
out=$(printf '%s' "$sql" | run_cgi \
    REQUEST_METHOD=POST \
    CONTENT_TYPE='text/sql; charset=utf-8' \
    CONTENT_LENGTH=${#sql} \
    QUERY_STRING='format=json')
echo "$out" | grep -qi 'Content-Type: application/json' || die "POST+format=json missing Content-Type"
body=$(printf '%s\n' "$out" | body_of)
echo "$body" | grep -q '"n"' || die "POST+format=json body unexpected: [$body]"

# Browser-like Accept should stay HTML (not XML)
out=$(run_cgi REQUEST_METHOD=GET QUERY_STRING='query=SELECT%201' \
    HTTP_ACCEPT='text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8')
echo "$out" | grep -qi 'Content-Type: text/html' || die "browser Accept should select HTML"

# OPTIONS CORS preflight
out=$(run_cgi REQUEST_METHOD=OPTIONS)
echo "$out" | grep -qi 'Access-Control-Allow-Methods:.*POST' || die "OPTIONS missing Allow-Methods"
echo "$out" | grep -qi 'Access-Control-Allow-Headers:.*Content-Type' || die "OPTIONS missing Allow-Headers"

# Missing query
set +e
out=$(run_cgi REQUEST_METHOD=GET QUERY_STRING='')
set -e
echo "$out" | grep -qi 'No query was provided' || die "empty GET should reject"

printf "${GREEN}CGI checks passed${NC}\n"
