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

#define PLAN_INDEX_UNIQUE   1
#define PLAN_INDEX_RANGE    2
#define PLAN_FULL_TABLE     4

int select_query (const char *query, int output_flags);

int process_select_query (
    struct Query *q,
    int output_flags
);

int information_query (const char *table);

int query (const char *query, int output_flags) {
    if (strncmp(query, "CREATE ", 7) == 0) {
        return create_query(query);
    }

    return select_query(query, output_flags);
}

int select_query (const char *query, int output_flags) {
    struct Query q = {0};

    if (parseQuery(&q, query) < 0) {
        fprintf(stderr, "Error parsing query");
        return -1;
    }

    if (strlen(q.table) == 0) {
        fprintf(stderr, "Table not specified\n");
        return -1;
    }

    if (strcmp(q.table, "INFORMATION") == 0) {
        if (strlen(q.predicate_value) < 1) {
            return -1;
        }

        return information_query(q.predicate_value);
    }

    if (q.flags & FLAG_EXPLAIN) {
        return explain_select_query(&q, output_flags);
    }

    return process_select_query(&q, output_flags);
}

int process_select_query (
    struct Query *q,
    int output_flags
) {
    /*************************
     * Begin Query processing
     *************************/

    struct DB db;
    int result_count = RESULT_NO_INDEX;
    int sort_needed = q->flags & FLAG_ORDER;

    if (openDB(&db, q->table) != 0) {
        fprintf(stderr, "File not found: '%s'\n", q->table);
        return -1;
    }

    int field_indices[FIELD_MAX_COUNT];

    // Get selected column indexes
    for (int i = 0; i < q->field_count; i++) {
        const char *field_name = q->fields + (i * FIELD_MAX_LENGTH);

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
                fprintf(stderr, "Field %s not found\n", &q->fields[i * FIELD_MAX_LENGTH]);
                closeDB(&db);
                return -1;
            }
        }
    }

    /*************************
     * Output headers
     ************************/
    if (output_flags & OUTPUT_FLAG_HEADERS) {
        printHeaderLine(stdout, &db, field_indices, q->field_count, 0);
    }

    /*************************
     * Special Cases
     *************************/

    /****************************
     * COUNT(*) with no predicate
     ****************************/
    // If we have COUNT(*) and there's no predicate then just early exit
    // we already know how many records there are
    if ((q->flags & FLAG_GROUP) && !(q->flags & FLAG_HAVE_PREDICATE)) {
        // We also need to provide a specimen row
        // "0 was chosen by a fair dice roll"
        // > But now we'll use offset value
        long count = db.record_count;
        if (q->limit_value >= 0L && q->limit_value < count) {
            count = q->limit_value;
        }
        printResultLine(stdout, &db, field_indices, q->field_count, q->offset_value, count, 0);
        closeDB(&db);
        return 0;
    }

    /**********************
     * Start iterating rows
     **********************/

    int *result_rowids = malloc(sizeof (int) * db.record_count);

    int plan_flags = 0;

    if (q->flags & FLAG_PRIMARY_KEY_SEARCH) {
        /******************
         * PRIMARY KEY
         *****************/
        result_count = primaryKeyScan(&db, q->predicate_field, q->predicate_op, q->predicate_value, result_rowids);

        if (result_count >= 0) {
            if (q->predicate_op == OPERATOR_EQ) {
                plan_flags |= PLAN_INDEX_UNIQUE;
            } else {
                plan_flags |= PLAN_INDEX_RANGE;
            }
        }
    }

    if ((q->flags & FLAG_HAVE_PREDICATE) && result_count == RESULT_NO_INDEX) {
        /*******************
         * UNIQUE INDEX SCAN
         *******************/
        // Try to find a unique index
        result_count = indexUniqueScan(q->predicate_field, q->predicate_op, q->predicate_value, result_rowids);

        if (result_count >= 0) {
            if (q->predicate_op == OPERATOR_EQ) {
                plan_flags |= PLAN_INDEX_UNIQUE;
            } else {
                plan_flags |= PLAN_INDEX_RANGE;
            }
        }
    }

    if (result_count == RESULT_NO_ROWS) {
        free(result_rowids);
        closeDB(&db);
        return 0;
    }

    if ((q->flags & FLAG_HAVE_PREDICATE) && result_count == RESULT_NO_INDEX) {
        /******************
         * INDEX RANGE SCAN
         ******************/
        result_count = indexRangeScan(q->predicate_field, q->predicate_op, q->predicate_value, result_rowids);

        if (result_count != RESULT_NO_INDEX) {
            plan_flags |= PLAN_INDEX_RANGE;
        }
    }

    if (!(q->flags & FLAG_HAVE_PREDICATE) && (q->flags & FLAG_ORDER)) {
        // Before we do a full table scan... we have one more opportunity to use an index
        // To save a sort later, see if we can use an index for ordering now
        struct DB index_db;
        if (findIndex(&index_db, q->order_field, INDEX_ANY) == 0) {
            result_count = indexWalk(&index_db, 1, 0, index_db.record_count, q->order_direction, result_rowids);
            plan_flags |= PLAN_INDEX_RANGE;
            sort_needed = 0;
        }
        closeDB(&index_db);
    }

    /******************
     * FULL TABLE SCAN
     ******************/
    // If INDEX RANGE SCAN failed then do FULL TABLE SCAN
    if (result_count == RESULT_NO_INDEX) {
        result_count = fullTableScan(&db, result_rowids, q->predicate_field, q->predicate_op, q->predicate_value, q->limit_value, q->offset_value, q->flags);

        plan_flags |= PLAN_FULL_TABLE;
    }

    // Early exit if there were no results
    if (result_count == 0 && !(q->flags & FLAG_GROUP)) {
        free(result_rowids);
        closeDB(&db);
        return 0;
    }

    /*******************
     * Ordering
     *******************/
    // If we're only outputting one row then we don't need to sort
    if (q->flags & FLAG_GROUP) {
        sort_needed = 0;
    }
    // If we did an index scan on the same column as the ordering then we don't need to order again;
    // the results will already be in the correct order
    if (plan_flags & (PLAN_INDEX_RANGE | PLAN_INDEX_UNIQUE) && (strcmp(q->predicate_field, q->order_field) == 0 || strcmp(q->predicate_field, q->order_field) == 0)) {
        sort_needed = 0;

        // We know the results are already sorted but if we need them in descending order
        // we can just reverse them now
        if (q->order_direction == ORDER_DESC) {
            reverse_array(result_rowids, result_count);
        }
    }

    if (sort_needed) {
        int order_index = getFieldIndex(&db, q->order_field);
        sortResultRows(&db, order_index, q->order_direction, result_rowids, result_count, result_rowids);
    }

    /********************
     * OFFSET/FETCH FIRST
     ********************/
    result_rowids += q->offset_value;
    result_count -= q->offset_value;

    if (q->limit_value >= 0 && q->limit_value < result_count) {
        result_count = q->limit_value;
    }

    /*******************
     * Output result set
     *******************/

    // COUNT(*) will print just one row
    if (q->flags & FLAG_GROUP) {
        // printf("Aggregate result:\n");
        printResultLine(stdout, &db, field_indices, q->field_count, result_count > 0 ? result_rowids[q->offset_value] : RESULT_NO_ROWS, result_count, output_flags);
    } else for (int i = 0; i < result_count; i++) {

        // ROW_NUMBER is offset by OFFSET from result index and is 1-index based
        printResultLine(stdout, &db, field_indices, q->field_count, result_rowids[i], q->offset_value + i + 1, output_flags);
    }

    free(result_rowids - q->offset_value);

    closeDB(&db);

    return 0;
}

int information_query (const char *table) {
    struct DB db;

    if (openDB(&db, table) != 0) {
        fprintf(stderr, "File not found: '%s'\n", table);
        return -1;
    }

    printf("Table:\t%s\n", table);
    printf("Fields:\t%d\n", db.field_count);
    printf("Records:\t%d\n", db.record_count);

    printf("\n");

    char *field_name = getFieldName(&db, 10);
    size_t len = strlen(field_name);
    printf("field 10\n--------\nlength: %ld\nvalue:\n%s\nlast char: %02x\n\n", len, field_name, field_name[len-1]);

    printf("field\tindex\n");
    printf("-----\t-----\n");

    for (int i = 0; i < db.field_count; i++) {
        printf("%s\tN\n", getFieldName(&db, i));
    }

    return 0;
}