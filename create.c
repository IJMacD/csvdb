#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "create.h"
#include "db.h"
#include "query.h"
#include "parse.h"
#include "sort.h"
#include "output.h"
#include "limits.h"

int create_index (const char *index_name, const char *table_name, const char *index_field, int unique_flag);

int create_query (const char *query) {
    int unique_flag = 0;

    size_t index;

    char index_name[TABLE_MAX_LENGTH] = {0};
    char table_name[TABLE_MAX_LENGTH] = {0};

    char index_field[FIELD_MAX_LENGTH] = {0};

    char keyword[FIELD_MAX_LENGTH];

    getToken(query, &index, keyword, FIELD_MAX_LENGTH);

    if (strcmp(keyword, "CREATE") != 0) {
        fprintf(stderr, "Expected CREATE got '%s'\n", keyword);
        return -1;
    }

    getToken(query, &index, keyword, FIELD_MAX_LENGTH);

    if (strcmp(keyword, "UNIQUE") == 0) {
        unique_flag = 1;

        getToken(query, &index, keyword, FIELD_MAX_LENGTH);
    }

    if (strcmp(keyword, "INDEX") != 0) {
        fprintf(stderr, "Expected INDEX got '%s'\n", keyword);
        return -1;
    }

    getToken(query, &index, index_name, TABLE_MAX_LENGTH);

    getToken(query, &index, keyword, FIELD_MAX_LENGTH);

    if (strcmp(keyword, "ON") != 0) {
        fprintf(stderr, "Expected ON got '%s'\n", keyword);
        return -1;
    }

    getToken(query, &index, table_name, TABLE_MAX_LENGTH);

    skipWhitespace(query, &index);

    if (query[index++] != '(') {
        fprintf(stderr, "Expected ( got '%c'\n", query[index-1]);
        return -1;
    }

    int length = getToken(query, &index, index_field, TABLE_MAX_LENGTH);

    if (index_field[length - 1] != ')') {
        fprintf(stderr, "Expected ) got '%c'\n", index_field[length - 1]);
        return -1;
    }

    index_field[length - 1] = '\0';

    create_index(index_name, table_name, index_field, unique_flag);

    return 0;
}

int create_index (const char *index_name, const char *table_name, const char *index_field, int unique_flag) {
    struct DB db;

    if (openDB(&db, table_name) != 0) {
        fprintf(stderr, "File not found: '%s'\n", table_name);
        return -1;
    }

    int index_field_index = getFieldIndex(&db, index_field);

    if (index_field_index < 0) {
        fprintf(stderr, "Field does not exist: '%s'\n", index_field);
        return -1;
    }

    char file_name[TABLE_MAX_LENGTH + 10];
    sprintf(file_name, "%s.%s.csv", index_name, unique_flag ? "unique" : "index");

    FILE *f = fopen(file_name, "w");

    if (!f) {
        fprintf(stderr, "Unable to create file for index: '%s'\n", file_name);
        return -1;
    }

    int field_indices[] = { index_field_index, FIELD_ROW_INDEX };
    int *result_rowids = malloc(sizeof (int) * db.record_count);

    sortResultRows(&db, index_field_index, ORDER_ASC, NULL, db.record_count, result_rowids);

    printHeaderLine(f, &db, field_indices, 2, OUTPUT_OPTION_COMMA);

    for (int i = 0; i < db.record_count; i++) {
        // ROW_NUMBER is offset by OFFSET from result index and is 1-index based
        printResultLine(f, &db, field_indices, 2, result_rowids[i], i + 1, OUTPUT_OPTION_COMMA);
    }

    free(result_rowids);

    return 0;
}