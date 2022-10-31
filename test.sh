#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
ORANGE='\033[0;33m'
NC='\033[0m' # No Color

readarray -t lines < ./test-cases.sql

OUTFILE=/tmp/test.out

errors=0

for sql in "${lines[@]}"; do
    if [[ "$sql" == --* ]]; then
        continue
    fi

    printf ' -- SQL: %s --\n' "$sql";

    start=`date +%s%N`
    ./release/csvdb -o $OUTFILE "$sql";
    result=$?
    end=`date +%s%N`

    runtime=$(((end-start)/1000000))

    if [ $result -eq 0 ]; then
        if [ -s $OUTFILE ]; then
            cat $OUTFILE
            printf " -- ${GREEN}OK${NC} (Check output above) Time: $runtime ms --\n\n";
        else
            printf " -- ${ORANGE}NO OUTPUT${NC} Time: $runtime ms --\n\n";
        fi
    else
        printf " -- ${RED}ERROR${NC} --\n\n";
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