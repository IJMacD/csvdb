RED='\033[0;31m'
GREEN='\033[0;32m'
ORANGE='\033[0;33m'
NC='\033[0m' # No Color

readarray -t lines < ./test-cases.sql

for a in "${lines[@]}"; do
    printf 'SQL: %s\n' "$a";

    # valgrind ./debug/csvdb "$a"; #> /dev/null;
    ./release/csvdb -o test.out "$a"; #> /dev/null;

    if [ $? -eq 0 ]
    then
        if [ -s test.out ]; then
            printf "${GREEN}OK${NC} (Check output):\n";
            cat test.out
        else
            printf "${ORANGE}NO OUTPUT${NC}\n";
        fi
    else
        printf "${RED}ERROR${NC}\n";
    fi

done

rm test.out