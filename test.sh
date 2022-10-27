#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
ORANGE='\033[0;33m'
NC='\033[0m' # No Color

readarray -t lines < ./test-cases.sql

OUTFILE=/tmp/test.out

for sql in "${lines[@]}"; do
    if [[ "$sql" == --* ]]; then
        continue
    fi

    printf ' -- SQL: %s --\n' "$sql";

    start=`date +%s%N`
    ./release/csvdb -o $OUTFILE "$sql";
    end=`date +%s%N`

    runtime=$(((end-start)/1000000))

    if [ $? -eq 0 ]; then
        if [ -s $OUTFILE ]; then
            cat $OUTFILE
            printf " -- ${GREEN}OK${NC} (Check output above) Time: $runtime ms --\n\n";
        else
            printf " -- ${ORANGE}NO OUTPUT${NC} Time: $runtime ms --\n\n";
        fi
    else
        printf " -- ${RED}ERROR${NC} --\n\n";
    fi

done

rm -f $OUTFILE