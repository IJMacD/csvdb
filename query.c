#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "query.h"
#include "db.h"
#include "parse.h"
#include "predicates.h"
#include "indices.h"
#include "sort.h"
#include "output.h"
#include "limits.h"
#include "create.h"
#include "explain.h"
#include "util.h"

#define FIELD_MAX_COUNT     10

#define PLAN_INDEX_UNIQUE   1
#define PLAN_INDEX_RANGE    2
#define PLAN_FULL_TABLE     4

int select_query (const char *query, int output_flags);

int process_select_query (
    const char *table,
    const char *fields,
    int field_count,
    int flags,
    int offset_value,
    int limit_value,
    const char *predicate_field,
    char predicate_op,
    const char *predicate_value,
    const char *order_field,
    int order_direction,
    int output_flags
);

int query (const char *query, int output_flags) {
    if (strncmp(query, "CREATE ", 7) == 0) {
        return create_query(query);
    }

    return select_query(query, output_flags);
}

int select_query (const char *query, int output_flags) {
    /*********************
     * Begin Query parsing
     *********************/

    size_t index = 0;

    size_t query_length = strlen(query);

    // printf("Query length: %ld\n", query_length);

    char fields[FIELD_MAX_COUNT * FIELD_MAX_LENGTH];
    // Allow SELECT to be optional and default to SELECT *
    fields[0] = '*';
    int field_count = 1;

    // printf("Asked for %d field(s)\n", curr_index);

    char table[TABLE_MAX_LENGTH] = {0};

    char predicate_field[FIELD_MAX_LENGTH];
    char predicate_value[VALUE_MAX_LENGTH];
    char predicate_op = OPERATOR_UN;

    char order_field[FIELD_MAX_LENGTH];
    int order_direction = ORDER_ASC;

    int flags = 0;
    long offset_value = 0;
    long limit_value = -1;

    int explain_flag = 0;

    if (strncmp(query, "EXPLAIN ", 8) == 0) {
        explain_flag = 1;
        index += 8;
    }

    char keyword[FIELD_MAX_LENGTH];

    while (index < query_length) {

        int token_length = getToken(query, &index, keyword, FIELD_MAX_LENGTH);

        if (token_length <= 0) {
            break;
        }

        // printf("Token: '%s'\n", keyword);

        if (strcmp(keyword, "SELECT") == 0) {

            int curr_index = 0;
            while (index < query_length) {
                char *field_name = fields + (FIELD_MAX_LENGTH * curr_index++);
                getToken(query, &index, field_name, FIELD_MAX_LENGTH);

                // printf("Field is %s\n", field);
                if (strcmp(field_name, "COUNT(*)") == 0) {
                    flags |= FLAG_GROUP;
                }

                skipWhitespace(query, &index);

                if (query[index] != ',') {
                    break;
                }

                index++;
            }

            field_count = curr_index;
        }
        else if (strcmp(keyword, "FROM") == 0) {
            getToken(query, &index, table, TABLE_MAX_LENGTH);
        }
        else if (strcmp(keyword, "WHERE") == 0) {
            flags |= FLAG_HAVE_PREDICATE;

            getToken(query, &index, predicate_field, FIELD_MAX_LENGTH);

            if (strncmp(predicate_field, "PK(", 3) == 0) {
                flags |= FLAG_PRIMARY_KEY_SEARCH;
                size_t len = strlen(predicate_field);
                // remove trailing ')'
                for (size_t i = 0; i < len - 4; i++) {
                    predicate_field[i] = predicate_field[i+3];
                }
                predicate_field[len - 4] = '\0';
            }

            // printf("Predicate field: %s\n", predicate_field);

            char op[4];
            getToken(query, &index, op, 4);

            predicate_op = parseOperator(op);
            if (predicate_op == OPERATOR_UN) {
                fprintf(stderr, "Bad query - expected =|!=|<|<=|>|>=\n");
                return -1;
            }

            // Check for IS NOT
            if (strcmp(op, "IS") == 0) {
                size_t original_index = index;
                getToken(query, &index, op, 4);
                if (strcmp(op, "NOT") == 0) {
                    predicate_op = OPERATOR_NE;
                } else {
                    index = original_index;
                }
            }

            getToken(query, &index, predicate_value, VALUE_MAX_LENGTH);
        }
        else if (strcmp(keyword, "OFFSET") == 0) {
            offset_value = getNumericToken(query, &index);

            if (offset_value < 0) {
                fprintf(stderr, "OFFSET cannot be negative\n");
                return -1;
            }
        }
        else if (strcmp(keyword, "FETCH") == 0) {
            getToken(query, &index, keyword, FIELD_MAX_LENGTH);

            if (strcmp(keyword, "FIRST") != 0 && strcmp(keyword, "NEXT") != 0) {
                fprintf(stderr, "Bad query - expected FIRST|NEXT\n");
                return -1;
            }

            skipWhitespace(query, &index);

            if (isdigit(query[index])) {

                limit_value = getNumericToken(query, &index);

                if (limit_value < 0) {
                    fprintf(stderr, "FETCH FIRST cannot be negative\n");
                    return -1;
                }
            } else {
                limit_value = 1;
            }

            getToken(query, &index, keyword, FIELD_MAX_LENGTH);

            if (strcmp(keyword, "ROW") != 0 && strcmp(keyword, "ROWS") != 0) {
                fprintf(stderr, "Bad query - expected ROW|ROWS; Got '%s'\n", keyword);
                return -1;
            }
        }
        else if (strcmp(keyword, "LIMIT") == 0) {
            limit_value = getNumericToken(query, &index);

            if (limit_value < 0) {
                fprintf(stderr, "LIMIT cannot be negative\n");
                return -1;
            }
        }
        else if (strcmp(keyword, "ORDER") == 0) {
            getToken(query, &index, keyword, FIELD_MAX_LENGTH);

            if (strcmp(keyword, "BY") != 0) {
                fprintf(stderr, "Bad query - expected BY\n");
                return -1;
            }

            flags |= FLAG_ORDER;

            getToken(query, &index, order_field, FIELD_MAX_LENGTH);

            size_t original_index = index;

            getToken(query, &index, keyword, FIELD_MAX_LENGTH);

            if (strcmp(keyword, "ASC") == 0) {
                order_direction = ORDER_ASC;
            } else if (strcmp(keyword, "DESC") == 0) {
                order_direction = ORDER_DESC;
            } else {
                index = original_index;
            }

        }
        else {
            fprintf(stderr, "Bad query - expected WHERE|OFFSET|FETCH FIRST|LIMIT\n");
            fprintf(stderr, "Found: '%s'\n", keyword);
            return -1;
        }
    }

    if (strlen(table) == 0) {
        fprintf(stderr, "Table not specified\n");
        return -1;
    }

    if (explain_flag) {
        return explain_select_query(table, fields, field_count, flags, offset_value, limit_value, predicate_field, predicate_op, predicate_value, order_field, order_direction, output_flags);
    }

    return process_select_query(table, fields, field_count, flags, offset_value, limit_value, predicate_field, predicate_op, predicate_value, order_field, order_direction, output_flags);
}

int process_select_query (
    const char *table,
    const char *fields,
    int field_count,
    int flags,
    int offset_value,
    int limit_value,
    const char *predicate_field,
    char predicate_op,
    const char *predicate_value,
    const char *order_field,
    int order_direction,
    int output_flags
) {
    /*************************
     * Begin Query processing
     *************************/

    struct DB db;
    int result_count = RESULT_NO_INDEX;
    int group_specimen = RESULT_NO_ROWS;

    if (openDB(&db, table) != 0) {
        fprintf(stderr, "File not found: '%s'\n", table);
        return -1;
    }

    int field_indices[FIELD_MAX_COUNT];

    // Get selected column indexes
    for (int i = 0; i < field_count; i++) {
        const char *field_name = fields + (i * FIELD_MAX_LENGTH);

        if (strcmp(field_name, "COUNT(*)") == 0) {
            field_indices[i] = FIELD_COUNT_STAR;
        } else if (strcmp(field_name, "*") == 0) {
            field_indices[i] = FIELD_STAR;
        } else if (strcmp(field_name, "ROW_NUMBER()") == 0) {
            field_indices[i] = FIELD_ROW_NUMBER;
        } else if (strcmp(field_name, "rowid") == 0) {
            field_indices[i] = FIELD_ROW_INDEX;
        }
        else {
            field_indices[i] = getFieldIndex(&db, field_name);

            if (field_indices[i] == -1) {
                fprintf(stderr, "Field %s not found\n", &fields[i * FIELD_MAX_LENGTH]);
                return -1;
            }
        }
    }

    /*************************
     * Output headers
     ************************/
    if (output_flags & OUTPUT_FLAG_HEADERS) {
        printHeaderLine(stdout, &db, field_indices, field_count, 0);
    }

    /*************************
     * Special Cases
     *************************/

    /****************************
     * COUNT(*) with no predicate
     ****************************/
    // If we have COUNT(*) and there's no predicate then just early exit
    // we already know how many records there are
    if ((flags & FLAG_GROUP) && !(flags & FLAG_HAVE_PREDICATE)) {
        // We also need to provide a specimen row
        // "0 was chosen by a fair dice roll"
        // > But now we'll use offset value
        long count = db.record_count;
        if (limit_value >= 0L && limit_value < count) {
            count = limit_value;
        }
        printResultLine(stdout, &db, field_indices, field_count, offset_value, count, 0);
        return 0;
    }

    /**********************
     * Start iterating rows
     **********************/

    int *result_rowids = malloc(sizeof (int) * db.record_count);

    int plan_flags = 0;

    if (flags & FLAG_PRIMARY_KEY_SEARCH) {
        /******************
         * PRIMARY KEY
         *****************/
        result_count = primaryKeyScan(&db, predicate_field, predicate_op, predicate_value, result_rowids);

        if (result_count >= 0) {
            if (predicate_op == OPERATOR_EQ) {
                plan_flags |= PLAN_INDEX_UNIQUE;
            } else {
                plan_flags |= PLAN_INDEX_RANGE;
            }
        }
    }

    if ((flags & FLAG_HAVE_PREDICATE) && result_count == RESULT_NO_INDEX) {
        /*******************
         * UNIQUE INDEX SCAN
         *******************/
        // Try to find a unique index
        result_count = indexUniqueScan(predicate_field, predicate_op, predicate_value, result_rowids);

        if (result_count >= 0) {
            if (predicate_op == OPERATOR_EQ) {
                plan_flags |= PLAN_INDEX_UNIQUE;
            } else {
                plan_flags |= PLAN_INDEX_RANGE;
            }
        }
    }

    if (result_count == RESULT_NO_ROWS) {
        free(result_rowids);
        return 0;
    }

    if (result_count == RESULT_NO_INDEX) {
        /******************
         * INDEX RANGE SCAN
         ******************/
        result_count = indexRangeScan(result_rowids, predicate_field, predicate_op, predicate_value, flags);

        if (result_count != RESULT_NO_INDEX) {
            plan_flags |= PLAN_INDEX_RANGE;
        }
    }

    /******************
     * FULL TABLE SCAN
     ******************/
    // If INDEX RANGE SCAN failed then do FULL TABLE SCAN
    if (result_count == RESULT_NO_INDEX) {
        result_count = fullTableScan(&db, result_rowids, predicate_field, predicate_op, predicate_value, limit_value, offset_value, flags);

        plan_flags |= PLAN_FULL_TABLE;
    }

    // Early exit if there were no results
    if (result_count == 0 && !(flags & FLAG_GROUP)) {
        free(result_rowids);
        return 0;
    }

    /*******************
     * Ordering
     *******************/
    int sort_needed = flags & FLAG_ORDER;
    // If we're only outputting one row then we don't need to sort
    if (flags & FLAG_GROUP) {
        sort_needed = 0;
    }
    // If we did an index scan on the same column as the ordering then we don't need to order again;
    // the results will already be in the correct order
    if (plan_flags & (PLAN_INDEX_RANGE | PLAN_INDEX_UNIQUE) && strcmp(predicate_field, order_field) == 0) {
        sort_needed = 0;

        if (order_direction == ORDER_DESC) {
            reverse_array(result_rowids, result_count);
        }
    }
    if (sort_needed) {
        int order_index = getFieldIndex(&db, order_field);
        sortResultRows(&db, order_index, order_direction, result_rowids, result_count, result_rowids);
    }

    /********************
     * OFFSET/FETCH FIRST
     ********************/
    result_rowids += offset_value;
    result_count -= offset_value;

    if (limit_value >= 0 && limit_value < result_count) {
        result_count = limit_value;
    }

    /*******************
     * Output result set
     *******************/

    // COUNT(*) will print just one row
    if (flags & FLAG_GROUP) {
        // printf("Aggregate result:\n");
        printResultLine(stdout, &db, field_indices, field_count, group_specimen, result_count, output_flags);
    } else for (int i = 0; i < result_count; i++) {

        // ROW_NUMBER is offset by OFFSET from result index and is 1-index based
        printResultLine(stdout, &db, field_indices, field_count, result_rowids[i], offset_value + i + 1, output_flags);
    }

    free(result_rowids - offset_value);

    return 0;
}
