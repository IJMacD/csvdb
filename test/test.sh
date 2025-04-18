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

echo "csvdb version:" `$CSVDB -v` 

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
