#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
ORANGE='\033[0;33m'
GREY='\033[0;90m'
NC='\033[0m' # No Color

readarray -t lines < ./test-cases.sql

CSVDB=../release/csvdb
OUTFILE=/tmp/test.out
STATFILE=test-cases-stats.csv

stats=""
if [[ $1 == "stats" ]]; then
    stats="--stats"
fi

errors=0

echo "duration" > $STATFILE

for sql in "${lines[@]}"; do
    if [[ "$sql" == --* ]]; then
        continue
    fi

    printf "$GREY -- SQL:$NC %s $GREY--$NC\n" "$sql";

    printf "\n$GREY -- Plan: --\n"

    $CSVDB -E -F table "$sql"

    printf "$NC"

    start=`date +%s%N`
    $CSVDB -o $OUTFILE $stats -F table "$sql"
    result=$?
    end=`date +%s%N`

    runtime=$(((end-start)/1000000))

    if [ $result -eq 0 ]; then
        if [ -s $OUTFILE ]; then
            if [[ ! -z $stats ]]; then
                printf "\n$GREY -- Stats: (microseconds) --\n"

                $CSVDB "TABLE stats"

                printf "$NC"
            fi

            printf "\n$GREY -- Results: --$NC\n"

            cat $OUTFILE
            printf "\n$GREY -- ${GREEN}OK${GREY} (Check output above) Time: $runtime ms --$NC\n\n";
        else
            printf "\n$GREY -- ${ORANGE}NO OUTPUT${GREY} Time: $runtime ms --$NC\n\n";
        fi

        echo $runtime >> $STATFILE
    else
        printf "\n$GREY -- ${RED}ERROR${GREY} --$NC\n\n\n";
        ((errors=errors+1))
    fi

done

rm -f $OUTFILE

printf " All tests complete: "

if [ $errors -eq 0 ]; then
    printf "${GREEN}NO ERRORS${NC} (Check output above)\n\n";
elif [ $errors -eq 1 ]; then
    printf "${RED}$errors ERROR${NC}\n\n";
else
    printf "${RED}$errors ERRORS${NC}\n\n";
fi

exit $errors
