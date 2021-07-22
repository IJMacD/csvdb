#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "query.h"
#include "db.h"
#include "parse.h"
#include "predicates.h"
#include "indices.h"
#include "filter.h"
#include "sort.h"
#include "output.h"
#include "limits.h"
#include "create.h"
#include "explain.h"
#include "util.h"
#include "plan.h"

int select_query (const char *query, int output_flags);

int process_select_query (
    struct Query *q,
    struct Plan *plan,
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
        fprintf(stderr, "Error parsing query\n");
        return -1;
    }

    if (strlen(q.table) == 0) {
        fprintf(stderr, "Table not specified\n");
        return -1;
    }

    if (strcmp(q.table, "INFORMATION") == 0) {
        if (q.predicate_count < 1) {
            return -1;
        }

        int result = information_query(q.predicates[0].value);
        destroyQuery(&q);
        return result;
    }

    /**********************
     * Make Plan
     **********************/
    struct Plan plan;

    makePlan(&q, &plan);

    if (q.flags & FLAG_EXPLAIN) {
        int result =  explain_select_query(&q, &plan, output_flags);
        destroyQuery(&q);
        return result;
    }

    int result = process_select_query(&q, &plan, output_flags);
    destroyQuery(&q);
    return result;
}

int process_select_query (
    struct Query *q,
    struct Plan *plan,
    int output_flags
) {
    /*************************
     * Begin Query processing
     *************************/

    struct DB db;

    if (openDB(&db, q->table) != 0) {
        fprintf(stderr, "File not found: '%s'\n", q->table);
        return -1;
    }

    /*************************
     * Output headers
     ************************/
    if (output_flags & OUTPUT_OPTION_HEADERS) {
        printHeaderLine(stdout, &db, q->columns, q->column_count, output_flags);
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
        printResultLine(stdout, &db, q->columns, q->column_count, q->offset_value, count, 0);
        closeDB(&db);
        return 0;
    }

    // Provision enough result space for maximum of all rows
    int *result_rowids = malloc(sizeof (int) * db.record_count);

    int result_count = RESULT_NO_INDEX;

    for (int i = 0; i < plan->step_count; i++) {
        struct PlanStep s = plan->steps[i];

        if (s.type == PLAN_PK_UNIQUE || s.type == PLAN_PK_RANGE) {
            struct Predicate p = s.predicates[0];
            result_count = primaryKeyScan(&db, p.field, p.op, p.value, result_rowids);
        }
        else if (s.type == PLAN_INDEX_UNIQUE) {
            struct Predicate p = s.predicates[0];
            result_count = indexUniqueScan(q->table, p.field, p.op, p.value, result_rowids);
        }
        else if (s.type == PLAN_INDEX_RANGE) {
            struct Predicate p = s.predicates[0];
            result_count = indexRangeScan(q->table, p.field, p.op, p.value, result_rowids);
        }
        else if (s.type == PLAN_TABLE_ACCESS_FULL) {
            if (s.predicate_count > 0) {
                result_count = fullTableScan(&db, result_rowids, s.predicates, s.predicate_count, q->limit_value + q->offset_value);
            }
            else {
                result_count = fullTableAccess(&db, result_rowids, q->limit_value + q->offset_value);
            }
        }
        else if (s.type == PLAN_TABLE_ACCESS_ROWID) {
            for (int i = 0; i < s.predicate_count; i++) {
                struct Predicate p = s.predicates[i];
                result_count = filterRows(&db, result_rowids, result_count, &p, result_rowids);
            }
        }
        else if (s.type == PLAN_SORT) {
            int order_index = getFieldIndex(&db, s.predicates[0].field);
            sortResultRows(&db, order_index, s.predicates[0].op, result_rowids, result_count, result_rowids);
        }
        else if (s.type == PLAN_REVERSE) {
            reverse_array(result_rowids, result_count);
        }
        else if (s.type == PLAN_SLICE) {
            result_rowids += s.param1;
            result_count -= s.param1;

            if (s.param2 >= 0 && s.param2 < result_count) {
                result_count = s.param2;
            }
        }
        else if (s.type == PLAN_GROUP) {
            // NOP
        }
        else if (s.type == PLAN_SELECT) {
            /*******************
             * Output result set
             *******************/

            // Fill in selected column indexes
            for (int i = 0; i < q->column_count; i++) {
                struct ResultColumn *column = &(q->columns[i]);

                if (column->field == FIELD_UNKNOWN) {
                    column->field = getFieldIndex(&db, column->text);

                    if (column->field == FIELD_UNKNOWN) {
                        fprintf(stderr, "Field %s not found\n", column->text);
                        closeDB(&db);
                        return -1;
                    }
                }
            }

            // COUNT(*) will print just one row
            if (q->flags & FLAG_GROUP) {
                // printf("Aggregate result:\n");
                printResultLine(stdout, &db, q->columns, q->column_count, result_count > 0 ? result_rowids[q->offset_value] : RESULT_NO_ROWS, result_count, output_flags);
            }
            else for (int i = 0; i < result_count; i++) {

                // ROW_NUMBER is offset by OFFSET from result index and is 1-index based
                printResultLine(stdout, &db, q->columns, q->column_count, result_rowids[i], q->offset_value + i + 1, output_flags);
            }
        }
        else {
            fprintf(stderr, "Whoops. Unimplemented OP code: %d\n", s.type);
            return -1;
        }
    }

    destroyPlan(plan);

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

    printf("field\tindex\n");
    printf("-----\t-----\n");

    struct DB index_db;

    for (int i = 0; i < db.field_count; i++) {
        int have_index = 0;

        if (findIndex(&index_db, table, getFieldName(&db, i), INDEX_ANY) == 0) {
            have_index = 1;
            closeDB(&index_db);
        }

        printf("%s\t%c\n", getFieldName(&db, i), have_index ? 'Y' : 'N');
    }

    closeDB(&db);

    return 0;
}