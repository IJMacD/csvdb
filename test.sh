readarray -t lines < ./test-cases.sql

for a in "${lines[@]}"; do
    printf 'SQL: %s\n' "$a";

    # valgrind ./debug/csvdb "$a"; #> /dev/null;
    ./release/csvdb "$a"; #> /dev/null;

    if [ $? -eq 0 ]
    then
        echo "OK";
    else
        echo "ERROR";
    fi
done