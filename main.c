#include <stdio.h>
#include <stdlib.h>

#include "query.h"

void printUsage (const char* name) {
    printf("Usage:\n\t%s \"SELECT <fields, ...> FROM <file> [WHERE <field> = <value>]\"\n", name);
}

int main (int argc, char * argv[]) {
    if (argc > 1) {
        query(argv[1]);
        return 0;
    }

    char buffer[1024];
    size_t count = fread(buffer, 1, 1024, stdin);
    if (count > 0) {
        buffer[count] = '\0';
        query(buffer);
        return 0;
    }
    
    printUsage(argv[0]);
    exit(-1);
}