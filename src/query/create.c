#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "../structs.h"
#include "token.h"
#include "../db/db.h"
#include "result.h"
#include "../sort/sort-quick.h"
#include "output.h"
#include "query.h"

static int create_table_query (const char * query, const char **end_ptr);

static int create_view_query (const char * query, const char **end_ptr);

static int create_index_query (const char * query, const char **end_ptr);

static int create_index (
    const char *index_name,
    const char *table_name,
    const char (*index_field)[32],
    int field_count,
    int unique_flag
);

int create_query (const char *query, const char **end_ptr) {
    size_t index = 0;

    char keyword[MAX_FIELD_LENGTH] = {0};

    getToken(query, &index, keyword, MAX_FIELD_LENGTH);

    if (strcmp(keyword, "CREATE") != 0) {
        fprintf(stderr, "Expected CREATE got '%s'\n", keyword);
        return -1;
    }

    getToken(query, &index, keyword, MAX_FIELD_LENGTH);

    if (strcmp(keyword, "TABLE") == 0) {
        return create_table_query(query, end_ptr);
    }

    if (strcmp(keyword, "VIEW") == 0) {
        return create_view_query(query, end_ptr);
    }

    if (strcmp(keyword, "INDEX") == 0 || strcmp(keyword, "UNIQUE") == 0) {
        return create_index_query(query, end_ptr);
    }

    fprintf(stderr, "Cannot CREATE '%s'\n", keyword);
    return -1;
}

int create_index_query (const char * query, const char **end_ptr) {
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

    char index_field[10][MAX_FIELD_LENGTH] = {0};
    int field_count = 0;

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

    while (query[index] != '\0') {
        skipWhitespace(query, &index);

        getQuotedToken(
            query,
            &index,
            index_field[field_count++],
            MAX_TABLE_LENGTH
        );

        skipWhitespace(query, &index);

        if (query[index] != ',') {
            break;
        }

        index++;
    }

    if (query[index++] != ')') {
        fprintf(stderr, "Expected ) got '%c'\n", query[index]);
        return -1;
    }

    if (auto_name) {
        sprintf(index_name, "%s__%s", table_name, index_field[0]);
    }

    create_index(index_name, table_name, index_field, field_count, unique_flag);

    if (query[index] == ';') {
        index++;
    }

    if (end_ptr != NULL) {
        *end_ptr = &query[index];
    }

    return 0;
}

static int create_index (
    const char *index_name,
    const char *table_name,
    const char (*index_field)[32],
    int field_count,
    int unique_flag
) {
    struct DB db;
    struct Table table = {0};
    table.db = &db;
    strcpy(table.name, table_name);

    if (openDB(&db, table_name) != 0) {
        fprintf(stderr, "File not found: '%s'\n", table_name);
        return -1;
    }

    if (unique_flag && field_count > 1) {
        fprintf(stderr, "Unable to create unique index on multiple columns.\n");
        return -1;
    }

    struct Column *columns = malloc(sizeof(*columns) * (field_count + 1));

    for (int i = 0; i < field_count; i++) {
        int index_field_index = getFieldIndex(&db, index_field[i]);

        if (index_field_index < 0) {
            fprintf(stderr, "Field does not exist: '%s'\n", index_field[i]);
            return -1;
        }

        columns[i].node.function = FUNC_UNITY;
        strcpy(columns[i].alias, index_field[i]);
        columns[i].node.field.table_id = 0;
        columns[i].node.field.index = index_field_index;
    }

    columns[field_count].node.function = FUNC_UNITY;
    strcpy(columns[field_count].alias, "rowid");
    columns[field_count].node.field.table_id = 0;
    columns[field_count].node.field.index = FIELD_ROW_INDEX;

    char file_name[MAX_TABLE_LENGTH + MAX_FIELD_LENGTH + 12];
    sprintf(
        file_name,
        "%s.%s.csv",
        index_name,
        unique_flag ? "unique" : "index"
    );

    FILE *f = fopen(file_name, "w");

    if (!f) {
        fprintf(stderr, "Unable to create file for index: '%s'\n", file_name);
        return -1;
    }

    int record_count = getRecordCount(&db);

    RowListIndex row_list = createRowList(1, record_count);

    // Fill row list with every sequential rowid
    fullTableScan(&db, getRowList(row_list), 0, -1);

    enum Order order_directions[] = {
        ORDER_ASC, ORDER_ASC, ORDER_ASC, ORDER_ASC, ORDER_ASC,
        ORDER_ASC, ORDER_ASC, ORDER_ASC, ORDER_ASC, ORDER_ASC,
    };

    struct Node **nodes = malloc(sizeof(*nodes) * field_count);

    for (int i = 0; i < field_count; i++) {
        nodes[i] = (struct Node *)&columns[i];
    }

    sortQuick(&table, *nodes, field_count, order_directions, getRowList(row_list));

    // Output functions assume array of DBs
    printHeaderLine(
        f,
        &table,
        1,
        columns,
        field_count + 1,
        OUTPUT_FORMAT_COMMA
    );

    char values[2][MAX_VALUE_LENGTH];

    for (int i = 0; i < record_count; i++) {
        // Check for UNIQUE
        if (unique_flag) {
            int row_id = getRowID(getRowList(row_list), 0, i);
            getRecordValue(
                &db,
                row_id,
                columns[0].node.field.index,
                values[i % 2],
                MAX_VALUE_LENGTH
            );

            if (i > 0 && strcmp(values[0], values[1]) == 0) {
                fprintf(
                    stderr,
                    "UNIQUE constraint failed. Multiple values for: '%s'\n",
                    values[0]
                );
                fclose(f);
                remove(file_name);
                exit(-1);
            }
        }

        printResultLine(
            f,
            &table,
            1,
            columns,
            field_count + 1,
            i,
            getRowList(row_list),
            OUTPUT_FORMAT_COMMA
        );
    }

    fclose(f);

    destroyRowList(row_list);

    return 0;
}

static int create_table_query (const char * query, const char **end_ptr) {

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

    int result = select_query(query + index, flags, f, end_ptr);

    fclose(f);

    return result;
}

static int create_view_query (const char * query, const char **end_ptr) {
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

    // Warning! can't cope with semicolons legally embedded in SQL
    char *c = strchr(query + index, ';');

    if (*c != '\0') {
        int view_len = c - (query + index);
        char *s = malloc(view_len + 1);
        strncpy(s, query + index, view_len);
        s[view_len] = '\0';

        fputs(s, f);

        if (end_ptr != NULL) {
            *end_ptr = c;
        }
    } else {
        int len = fputs(query + index, f);

        if (end_ptr != NULL) {
            *end_ptr = query + index + len;
        }
    }

    fputc('\n', f);

    fclose(f);

    return 0;
}

int insert_query (const char * query, const char **end_ptr) {
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

    int result = select_query(query + index, flags, f, end_ptr);

    fclose(f);

    return result;

    /**
     * TODO: Optionally rebuild any indexes found on the table
     */
}