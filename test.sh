RED='\033[0;31m'
GREEN='\033[0;32m'
ORANGE='\033[0;33m'
NC='\033[0m' # No Color

readarray -t lines < ./test-cases.sql

OUTFILE=/tmp/test.out

for sql in "${lines[@]}"; do
    printf 'SQL: %s\n' "$sql";

    ./release/csvdb -o $OUTFILE "$sql";

    if [ $? -eq 0 ]; then
        if [ -s $OUTFILE ]; then
            printf "${GREEN}OK${NC} (Check output):\n";
            cat $OUTFILE
        else
            printf "${ORANGE}NO OUTPUT${NC}\n";
        fi
    else
        printf "${RED}ERROR${NC}\n";
    fi

done

rm -f $OUTFILE