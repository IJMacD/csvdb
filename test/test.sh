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

    printf "$NC"

    start=`$D`
    $CSVDB -o $OUTFILE $stats -F table "$sql"
    result=$?
    end=`$D`

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
