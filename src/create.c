#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "structs.h"
#include "token.h"
#include "db.h"
#include "result.h"
#include "sort.h"
#include "output.h"
#include "query.h"

int create_table_query (const char * query);

int create_view_query (const char * query);

int create_index_query (const char * query);

int create_index (const char *index_name, const char *table_name, const char *index_field, int unique_flag);

int create_query (const char *query) {
    size_t index = 0;

    char keyword[MAX_FIELD_LENGTH] = {0};

    getToken(query, &index, keyword, MAX_FIELD_LENGTH);

    if (strcmp(keyword, "CREATE") != 0) {
        fprintf(stderr, "Expected CREATE got '%s'\n", keyword);
        return -1;
    }

    getToken(query, &index, keyword, MAX_FIELD_LENGTH);

    if (strcmp(keyword, "TABLE") == 0) {
        return create_table_query(query);
    }

    if (strcmp(keyword, "VIEW") == 0) {
        return create_view_query(query);
    }

    if (strcmp(keyword, "INDEX") == 0 || strcmp(keyword, "UNIQUE") == 0) {
        return create_index_query(query);
    }

    fprintf(stderr, "Cannot CREATE '%s'\n", keyword);
    return -1;
}

int create_index_query (const char * query) {
    int unique_flag = 0;
    int auto_name = 0;

    size_t index = 0;

    char keyword[MAX_FIELD_LENGTH] = {0};

    getToken(query, &index, keyword, MAX_FIELD_LENGTH);

    if (strcmp(keyword, "CREATE") != 0) {
        fprintf(stderr, "Expected CREATE got '%s'\n", keyword);
        return -1;
    }

    getToken(query, &index, keyword, MAX_FIELD_LENGTH);

    char index_name[MAX_TABLE_LENGTH + MAX_FIELD_LENGTH + 2] = {0};
    char table_name[MAX_TABLE_LENGTH] = {0};

    char index_field[MAX_FIELD_LENGTH] = {0};

    if (strcmp(keyword, "UNIQUE") == 0) {
        unique_flag = 1;

        getToken(query, &index, keyword, MAX_FIELD_LENGTH);
    }

    if (strcmp(keyword, "INDEX") != 0) {
        fprintf(stderr, "Expected INDEX got '%s'\n", keyword);
        return -1;
    }

    skipWhitespace(query, &index);

    if (strncmp(&query[index], "ON ", 3) == 0) {
        // Auto generated index name
        auto_name = 1;
    } else {
        getQuotedToken(query, &index, index_name, MAX_TABLE_LENGTH);
    }

    getToken(query, &index, keyword, MAX_FIELD_LENGTH);

    if (strcmp(keyword, "ON") != 0) {
        fprintf(stderr, "Expected ON got '%s'\n", keyword);
        return -1;
    }

    getQuotedToken(query, &index, table_name, MAX_TABLE_LENGTH);

    skipWhitespace(query, &index);

    if (query[index++] != '(') {
        fprintf(stderr, "Expected ( got '%c'\n", query[index-1]);
        return -1;
    }

    getToken(query, &index, index_field, MAX_TABLE_LENGTH);

    if (query[index] != ')') {
        fprintf(stderr, "Expected ) got '%c'\n", query[index]);
        return -1;
    }

    if (auto_name) {
        sprintf(index_name, "%s__%s", table_name, index_field);
    }

    create_index(index_name, table_name, index_field, unique_flag);

    return 0;
}

int create_index (const char *index_name, const char *table_name, const char *index_field, int unique_flag) {
    struct DB db;
    struct Table table = {0};
    table.db = &db;
    strcpy(table.name, table_name);

    if (openDB(&db, table_name) != 0) {
        fprintf(stderr, "File not found: '%s'\n", table_name);
        return -1;
    }

    int index_field_index = getFieldIndex(&db, index_field);

    if (index_field_index < 0) {
        fprintf(stderr, "Field does not exist: '%s'\n", index_field);
        return -1;
    }

    char file_name[MAX_TABLE_LENGTH + MAX_FIELD_LENGTH + 12];
    sprintf(file_name, "%s.%s.csv", index_name, unique_flag ? "unique" : "index");

    FILE *f = fopen(file_name, "w");

    if (!f) {
        fprintf(stderr, "Unable to create file for index: '%s'\n", file_name);
        return -1;
    }

    struct ColumnNode columns[2] = {0};
    columns[0].fields[0].table_id = 0;
    columns[0].fields[0].index = index_field_index;
    strcpy(columns[0].alias, index_field);
    columns[1].fields[0].table_id = 0;
    columns[1].fields[0].index = FIELD_ROW_INDEX;

    int row_list = createRowList(1, db.record_count);

    // Fill row list with every sequential rowid
    fullTableAccess(&db, getRowList(row_list), -1);

    int sort_list = createRowList(getRowList(row_list)->join_count, getRowList(row_list)->row_count);

    sortResultRows(&db, 0, index_field_index, ORDER_ASC, getRowList(row_list), getRowList(sort_list));

    // Output functions assume array of DBs
    printHeaderLine(f, &table, 1, columns, 2, OUTPUT_FORMAT_COMMA);

    char values[2][MAX_VALUE_LENGTH];

    for (int i = 0; i < db.record_count; i++) {
        // Check for UNIQUE
        if (unique_flag) {
            int row_id = getRowID(getRowList(sort_list), 0, i);
            getRecordValue(&db, row_id, index_field_index, values[i % 2], MAX_VALUE_LENGTH);

            if (i > 0 && strcmp(values[0], values[1]) == 0) {
                fprintf(stderr, "UNIQUE constraint failed. Multiple values for: '%s'\n", values[0]);
                fclose(f);
                remove(file_name);
                exit(-1);
            }
        }

        printResultLine(f, &table, 1, columns, 2, i, getRowList(sort_list), OUTPUT_FORMAT_COMMA);
    }

    destroyRowList(getRowList(row_list));
    destroyRowList(getRowList(sort_list));

    return 0;
}

int create_table_query (const char * query) {

    size_t index = 0;

    char keyword[MAX_FIELD_LENGTH] = {0};

    char table_name[MAX_TABLE_LENGTH] = {0};

    getToken(query, &index, keyword, MAX_FIELD_LENGTH);

    if (strcmp(keyword, "CREATE") != 0) {
        fprintf(stderr, "Expected CREATE got '%s'\n", keyword);
        return -1;
    }

    getToken(query, &index, keyword, MAX_FIELD_LENGTH);

    if (strcmp(keyword, "TABLE") != 0) {
        fprintf(stderr, "Expected TABLE got '%s'\n", keyword);
        return -1;
    }

    skipWhitespace(query, &index);

    getQuotedToken(query, &index, table_name, MAX_TABLE_LENGTH);

    getToken(query, &index, keyword, MAX_FIELD_LENGTH);

    if (strcmp(keyword, "AS") != 0) {
        fprintf(stderr, "Expected AS got '%s'\n", keyword);
        return -1;
    }

    skipWhitespace(query, &index);

    char file_name[MAX_TABLE_LENGTH + 4];
    sprintf(file_name, "%s.csv", table_name);

    FILE *f = fopen(file_name, "w");

    if (!f) {
        fprintf(stderr, "Unable to create file for table: '%s'\n", file_name);
        return -1;
    }

    int flags = OUTPUT_FORMAT_COMMA | OUTPUT_OPTION_HEADERS;

    return select_query(query + index, flags, f);
}

int create_view_query (const char * query) {
    size_t index = 0;

    char keyword[MAX_FIELD_LENGTH] = {0};

    char view_name[MAX_TABLE_LENGTH] = {0};

    getToken(query, &index, keyword, MAX_FIELD_LENGTH);

    if (strcmp(keyword, "CREATE") != 0) {
        fprintf(stderr, "Expected CREATE got '%s'\n", keyword);
        return -1;
    }

    getToken(query, &index, keyword, MAX_FIELD_LENGTH);

    if (strcmp(keyword, "VIEW") != 0) {
        fprintf(stderr, "Expected VIEW got '%s'\n", keyword);
        return -1;
    }

    skipWhitespace(query, &index);

    getQuotedToken(query, &index, view_name, MAX_TABLE_LENGTH);

    getToken(query, &index, keyword, MAX_FIELD_LENGTH);

    if (strcmp(keyword, "AS") != 0) {
        fprintf(stderr, "Expected AS got '%s'\n", keyword);
        return -1;
    }

    skipWhitespace(query, &index);

    char file_name[MAX_TABLE_LENGTH + 4];
    sprintf(file_name, "%s.sql", view_name);

    FILE *f = fopen(file_name, "w");

    if (!f) {
        fprintf(stderr, "Unable to create file for view: '%s'\n", file_name);
        return -1;
    }

    fputs(query + index, f);

    fputc('\n', f);

    fclose(f);

    return 0;
}

int insert_query (const char * query) {
    size_t index = 0;

    char keyword[MAX_FIELD_LENGTH] = {0};

    char table_name[MAX_TABLE_LENGTH] = {0};

    if (strncmp(query, "INSERT INTO", 11) != 0) {
        fprintf(stderr, "Expected INSERT INTO got '%s'\n", keyword);
        return -1;
    }

    index += 11;

    skipWhitespace(query, &index);

    getQuotedToken(query, &index, table_name, MAX_TABLE_LENGTH);

    skipWhitespace(query, &index);

    char file_name[MAX_TABLE_LENGTH + 4];
    sprintf(file_name, "%s.csv", table_name);

    FILE *f = fopen(file_name, "a");

    if (!f) {
        fprintf(stderr, "Unable to open file for insertion: '%s'\n", file_name);
        return -1;
    }

    int flags = OUTPUT_FORMAT_COMMA;

    return select_query(query + index, flags, f);

    /**
     * TODO: Optionally rebuild any indexes found on the table
     */
}