FORMATS="table tsv csv csv:excel html json:object json:array sql:insert sql:create sql:values xml record"
CSVDB=../release/csvdb
TEST_FILE="nl_test.csv"
for f in $FORMATS; do echo $f; $CSVDB -F $f < $TEST_FILE; printf '\n'; done