#include <stdio.h>
#include <stdlib.h>

#include "query.h"

void printUsage (const char* name) {
    printf("Usage:\n\t%s \"SELECT <fields, ...> FROM <file> [WHERE <field> = <value>]\"\n", name);
}

int main (int argc, char * argv[]) {
    if (argc == 1) {
        printUsage(argv[0]);
        exit(-1);
    }

    query(argv[1]);
}