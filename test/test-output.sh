FORMATS="table box tsv csv csv:excel html json:object json:array sql:insert sql:create sql:values xml record"
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CSVDB="$SCRIPT_DIR/../release/csvdb"
TEST_FILE="$SCRIPT_DIR/nl_test.csv"
for f in $FORMATS; do echo $f; $CSVDB -F $f < $TEST_FILE; printf '\n'; done