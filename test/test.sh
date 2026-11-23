#!/usr/bin/env bash

RED='\033[0;31m'
GREEN='\033[0;32m'
ORANGE='\033[0;33m'
GREY='\033[0;90m'
NC='\033[0m' # No Color

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
TEST_FILE="$SCRIPT_DIR/test-cases.sql"
CSVDB="$SCRIPT_DIR/../release/csvdb"
OUTFILE=/tmp/test.out
STATFILE="$SCRIPT_DIR/test-cases-stats.csv"
SNAPSHOT_DIR="$SCRIPT_DIR/__snapshots__"
ENABLE_SNAPSHOT="1"

mkdir -p "$SNAPSHOT_DIR"

# Get array of test cases
readarray -t lines < $TEST_FILE

# cd so that csvdb is working in the correct dir to find csv files
cd $SCRIPT_DIR

stats=""
if [[ $* == "--stats" ]]; then
    stats="--stats"
fi

ci=""
if [[ $* == "--ci" ]]; then
    ci=1
fi

update=""
if [[ $* == "--update-snapshots" ]]; then
    update=1
fi

tests=0
errors=0

cli_die() {
    printf "${RED}CLI check failed:${NC} %s\n" "$1" >&2
    exit 1
}

ver_flag=$("$CSVDB" -v </dev/null)
ver_long=$("$CSVDB" --version </dev/null)
if [[ -z "$ver_flag" || "$ver_flag" != "$ver_long" ]]; then
    cli_die "-v and --version disagree: [$ver_flag] [$ver_long]"
fi

piped=$(printf 'n\n1\n' | "$CSVDB" -v)
if [[ "$piped" != "$ver_flag" ]]; then
    cli_die "standalone -v read stdin instead of printing the version"
fi

with_query=$("$CSVDB" -v --format csv --no-headers "SELECT 1" 2>/dev/null)
if [[ "$with_query" != "1" ]]; then
    cli_die "-v with a query did not run it: [$with_query]"
fi

ignored=$(printf 'n\n1\n' | "$CSVDB" --version "SELECT 1")
if [[ "$ignored" != "$ver_long" ]]; then
    cli_die "--version ran a query"
fi

space=$("$CSVDB" --format csv --no-headers "SELECT 1")
equals=$("$CSVDB" --format=csv -N "SELECT 1")
plain=$("$CSVDB" -N -F csv "SELECT 1")
if [[ "$space" != "1" || "$space" != "$equals" || "$space" != "$plain" ]]; then
    cli_die "format spellings disagree: [$space] [$equals] [$plain]"
fi

headed=$("$CSVDB" -N -H --format=csv "SELECT 1")
bare=$("$CSVDB" -H -N --format csv "SELECT 1")
if [[ "$headed" == "$bare" || "$bare" != "$plain" ]]; then
    cli_die "-H/-N did not follow flag order: [$headed] [$bare]"
fi

plan_en=$("$CSVDB" -E -N -F csv "SELECT 1")
plan_ne=$("$CSVDB" -N -E --format csv "SELECT 1")
if [[ "$plan_en" != "$plan_ne" || "$plan_en" == "$plain" ]]; then
    cli_die "-N cleared --explain"
fi

if "$CSVDB" --format >/dev/null 2>&1; then
    cli_die "--format without a value succeeded"
fi
if "$CSVDB" --input >/dev/null 2>&1; then
    cli_die "--input without a value succeeded"
fi
if "$CSVDB" --output >/dev/null 2>&1; then
    cli_die "--output without a value succeeded"
fi
if "$CSVDB" --format --no-headers "SELECT 1" >/dev/null 2>&1; then
    cli_die "--format consumed the following flag"
fi

cli_in=$(mktemp)
cli_out_space=$(mktemp)
cli_out_equals=$(mktemp)
printf 'n\n7\n' > "$cli_in"
in_space=$("$CSVDB" --input "$cli_in" --format csv --no-headers "SELECT n FROM stdin")
in_equals=$("$CSVDB" --input="$cli_in" --format=csv -N "SELECT n FROM stdin")
"$CSVDB" --output "$cli_out_space" --format csv -N "SELECT 1"
"$CSVDB" --output="$cli_out_equals" --format=csv --no-headers "SELECT 1"
if [[ "$in_space" != "7" || "$in_space" != "$in_equals" ]]; then
    cli_die "--input spellings disagree: [$in_space] [$in_equals]"
fi
if ! cmp -s "$cli_out_space" "$cli_out_equals"; then
    cli_die "--output spellings disagree"
fi
rm -f "$cli_in" "$cli_out_space" "$cli_out_equals"

# --- Config: TTY format/headers + index_dirs ---
cfg_root=$(mktemp -d)
cfg_file="$cfg_root/config"
idx_dir="$cfg_root/indexes"
mkdir -p "$idx_dir"
cat > "$cfg_file" <<EOF
[output]
format = "table"
headers = false

[paths]
index_dirs = [
  "$idx_dir",
]
EOF

# Redirected stdout must ignore config format/headers (stay csv + headers)
cfg_pipe=$(CSVDB_CONFIG="$cfg_file" "$CSVDB" "SELECT 1 AS n")
cfg_pipe_expected=$'n\n1'
if [[ "$cfg_pipe" != "$cfg_pipe_expected" ]]; then
    cli_die "config format/headers leaked into a pipe: [$cfg_pipe]"
fi

# CLI still overrides when redirected
cfg_cli=$(CSVDB_CONFIG="$cfg_file" "$CSVDB" -N -F csv "SELECT 1 AS n")
if [[ "$cfg_cli" != "1" ]]; then
    cli_die "CLI did not override config on a pipe: [$cfg_cli]"
fi

# index_dirs: index beside table is moved away; config dir should still find it
printf 'id,name\n1,alpha\n2,beta\n' > "$cfg_root/tiny.csv"
cfg_prev_cwd=$PWD
cd "$cfg_root" || cli_die "could not cd to config test dir"
CSVDB_CONFIG= "$CSVDB" "CREATE INDEX ON tiny (name)" >/dev/null
mv tiny__name.index.csv indexes/tiny__name.index.csv

plan_with=$(CSVDB_CONFIG="$cfg_file" "$CSVDB" -E -N -F csv \
    "SELECT name FROM tiny WHERE name = 'beta'")
plan_without=$(CSVDB_CONFIG= "$CSVDB" -E -N -F csv \
    "SELECT name FROM tiny WHERE name = 'beta'")
if [[ "$plan_with" != *INDEX* ]]; then
    cd "$cfg_prev_cwd" || true
    cli_die "index_dirs did not find index: [$plan_with]"
fi
if [[ "$plan_without" == *INDEX* ]]; then
    cd "$cfg_prev_cwd" || true
    cli_die "expected no index without index_dirs: [$plan_without]"
fi
hit=$(CSVDB_CONFIG="$cfg_file" "$CSVDB" -N -F csv \
    "SELECT name FROM tiny WHERE name = 'beta'")
if [[ "$hit" != "beta" ]]; then
    cd "$cfg_prev_cwd" || true
    cli_die "index_dirs query failed: [$hit]"
fi
cd "$cfg_prev_cwd" || cli_die "could not restore cwd after config test"

# TTY path (when `script` is available): config format/headers apply
if command -v script >/dev/null 2>&1; then
    tty_out=$(CSVDB_CONFIG="$cfg_file" script -qfec "$CSVDB \"SELECT 1 AS n\"" /dev/null 2>/dev/null | tr -d '\r')
    if [[ "$tty_out" != *"|"* ]]; then
        cli_die "TTY config format was not applied: [$tty_out]"
    fi
    # headers=false → single data row; -H adds a header line containing n
    tty_h=$(CSVDB_CONFIG="$cfg_file" script -qfec "$CSVDB -H \"SELECT 1 AS n\"" /dev/null 2>/dev/null | tr -d '\r')
    if [[ "$tty_h" != *"n"* ]]; then
        cli_die "TTY -H did not force headers: [$tty_h]"
    fi
    if [[ $(printf '%s\n' "$tty_out" | wc -l) -ge $(printf '%s\n' "$tty_h" | wc -l) ]]; then
        cli_die "TTY headers=false did not reduce output vs -H: [$tty_out] vs [$tty_h]"
    fi
fi

rm -rf "$cfg_root"

# Unclosed function args must abort — not print an error and still run.
bad_count_out=$(mktemp)
if "$CSVDB" -N -F csv "FROM CALENDAR SELECT COUNT(* WHERE date BETWEEN '2026-11-01' ANDX '2027-01-01'" \
        >"$bad_count_out" 2>/dev/null; then
    rm -f "$bad_count_out"
    cli_die "unclosed COUNT(* still succeeded"
fi
if [[ -s "$bad_count_out" ]]; then
    rm -f "$bad_count_out"
    cli_die "unclosed COUNT(* still produced output"
fi
rm -f "$bad_count_out"

echo "csvdb version:" "$ver_flag" 

echo

echo "duration" > $STATFILE

if [ `date +%N | grep N` ]; then
    echo "(Precise timings not supported)";
    D="date +%s"
else
    D="date +%s%N"
fi

for sql in "${lines[@]}"; do
    if [[ "$sql" == --* ]]; then
        continue
    fi

    printf "$GREY -- Test [%03d] SQL:$NC %s $GREY--$NC\n" "$tests" "$sql";

    printf "\n$GREY -- Plan: --\n"

    $CSVDB -E -F table "$sql"
    explain_result=$?

    printf "$NC"

    start=`$D`
    $CSVDB -o $OUTFILE $stats -F table "$sql"
    result=$?
    end=`$D`

    # Exit 129-254 means killed by a signal (e.g. SIGABRT from fortify).
    # CREATE and similar statements return 255 from EXPLAIN; ignore those.
    if [[ $explain_result -ge 129 && $explain_result -le 254 ]]; then
        printf "\n${RED}EXPLAIN crashed (exit %d)${NC}\n" "$explain_result"
        result=1
    fi

    if [[ $result == 0 && ! -z $ENABLE_SNAPSHOT ]]; then
        if [[ ! -f "$SNAPSHOT_DIR/$tests.no_snapshot" ]]; then
            if [[ ! -z $update ]]; then
                cp "$OUTFILE" "$SNAPSHOT_DIR/$tests"
                printf "\n${ORANGE}(Wrote new snapshot)${NC}\n"
            elif [[ -f "$SNAPSHOT_DIR/$tests" ]]; then
                if ! cmp -s "$SNAPSHOT_DIR/$tests" "$OUTFILE"; then
                    result=1

                    printf "\n$GREY -- Results: --$NC\n"
                    printf "Output does not match snapshot\n\n"
                    diff -u --color=always "$SNAPSHOT_DIR/$tests" "$OUTFILE" | tail -n +4
                fi
            elif [[ -z $ci ]]; then
                cp "$OUTFILE" "$SNAPSHOT_DIR/$tests"
                printf "\n${ORANGE}(Wrote new snapshot)${NC}\n"
            else 
                printf "${RED}Test %d does not have a snapshot${NC}\n" $tests
                exit 1
            fi
        fi
    fi

    if [[ $D == "date +%s%N" ]]; then
        runtime="$(((end-start)/1000000)) ms"
    else 
        runtime="$((end-start)) s"
    fi

    if [ $result -eq 0 ]; then
        if [ -s $OUTFILE ]; then
            if [[ ! -z $stats ]]; then
                printf "\n$GREY -- Stats: (microseconds) --\n"

                $CSVDB "TABLE stats"

                printf "$NC"
            fi

            printf "\n$GREY -- Results: --$NC\n"

            cat $OUTFILE
            printf "\n$GREY -- ${GREEN}OK${GREY} (Check output for correctness) Time: $runtime --$NC\n\n";
        else
            printf "\n$GREY -- ${ORANGE}NO OUTPUT${GREY} Time: $runtime --$NC\n\n";
        fi

        echo $runtime >> $STATFILE
    else
        printf "\n$GREY -- ${RED}ERROR${GREY} --$NC\n\n\n";
        ((errors=errors+1))
    fi
    ((tests=tests+1))

done

rm -f $OUTFILE

printf " All tests ($tests) complete: "

if [ $errors -eq 0 ]; then
    printf "${GREEN}NO ERRORS${NC} (Check output for correctness)\n\n";
elif [ $errors -eq 1 ]; then
    printf "${RED}$errors ERROR${NC}\n\n";
else
    printf "${RED}$errors ERRORS${NC}\n\n";
fi

exit $errors
