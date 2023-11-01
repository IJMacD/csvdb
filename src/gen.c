#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "structs.h"
#include "query/query.h"

int debug_verbosity = 0;

void printUsage (const char* name) {
    printf(
        "Usage:\n\t%s n [out.csv]\n\tGenerate n records and store in out.csv or"
        " write to stdout\n",
        name
    );
}

int main (int argc, char * argv[]) {
    if (argc < 2) {
        printUsage(argv[0]);
        exit(-1);
    }

    int record_count = atoi(argv[1]);

    FILE *stream = stdout;

    if (argc > 2) {
        stream = fopen(argv[2], "w");

        if (!stream) {
            fprintf(stderr, "Couldn't open '%s'\n", argv[2]);
        }
    }

    #ifdef DETERMINISTIC
    srand(42);
    #else
    srand(time(NULL));
    #endif


    char query[64];
    sprintf(query, "FROM SAMPLE LIMIT %d", record_count);

    select_query(
        query,
        OUTPUT_FORMAT_COMMA | OUTPUT_OPTION_HEADERS,
        stream,
        NULL
    );
}
