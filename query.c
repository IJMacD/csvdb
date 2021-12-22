#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <unistd.h>

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
#include "result.h"

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
        // No table was specified.
        // However, if stdin is something more than a tty (i.e pipe or redirected file)
        // then we can default to it.
        if (!isatty(fileno(stdin))) {
            strcpy(q.table, "stdin");
        }
        else {
            fprintf(stderr, "Table not specified\n");
            return -1;
        }
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
    printPreamble(stdout, &db, q->columns, q->column_count, output_flags);

    if (output_flags & OUTPUT_OPTION_HEADERS) {
        printHeaderLine(stdout, &db, q->columns, q->column_count, output_flags);
    }

    struct ResultSet results;

    results.list_count = 1;

    struct RowList row_list;

    results.row_lists = &row_list;

    // Provision enough result space for maximum of all rows
    row_list.row_ids = malloc(sizeof (int) * db.record_count);

    row_list.join_count = 1;
    row_list.row_count = 0;

    for (int i = 0; i < plan->step_count; i++) {
        struct PlanStep s = plan->steps[i];

        if (s.type == PLAN_PK_UNIQUE || s.type == PLAN_PK_RANGE) {
            struct Predicate p = s.predicates[0];
            primaryKeyScan(&db, p.field, p.op, p.value, &row_list);
        }
        else if (s.type == PLAN_INDEX_UNIQUE) {
            struct Predicate p = s.predicates[0];
            indexUniqueScan(q->table, p.field, p.op, p.value, &row_list);
        }
        else if (s.type == PLAN_INDEX_RANGE) {
            struct Predicate p = s.predicates[0];
            indexRangeScan(q->table, p.field, p.op, p.value, &row_list);
        }
        else if (s.type == PLAN_TABLE_ACCESS_FULL) {
            if (s.predicate_count > 0) {
                fullTableScan(&db, &row_list, s.predicates, s.predicate_count, q->limit_value + q->offset_value);
            }
            else {
                fullTableAccess(&db, &row_list, q->limit_value + q->offset_value);
            }
        }
        else if (s.type == PLAN_TABLE_ACCESS_ROWID) {
            for (int i = 0; i < s.predicate_count; i++) {
                struct Predicate p = s.predicates[i];
                filterRows(&db, &row_list, &p, &row_list);
            }
        }
        else if (s.type == PLAN_SORT) {
            int order_index = getFieldIndex(&db, s.predicates[0].field);
            sortResultRows(&db, order_index, s.predicates[0].op, &row_list, &row_list);
        }
        else if (s.type == PLAN_REVERSE) {
            reverse_array(row_list.row_ids, row_list.row_count * row_list.join_count);
        }
        else if (s.type == PLAN_SLICE) {
            row_list.row_ids += s.param1 * row_list.join_count;
            row_list.row_count -= s.param1;

            if (s.param2 >= 0 && s.param2 < row_list.row_count) {
                row_list.row_count = s.param2;
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

            // Aggregate functions will print just one row
            if (q->flags & FLAG_GROUP) {
                // printf("Aggregate result:\n");
                printResultLine(stdout, &db, q->columns, q->column_count, row_list.row_count > 0 ? q->offset_value : RESULT_NO_ROWS, &row_list, output_flags);
            }
            else for (int i = 0; i < row_list.row_count; i++) {

                // ROW_NUMBER is offset by OFFSET from result index and is 1-index based
                printResultLine(stdout, &db, q->columns, q->column_count, i, &row_list, output_flags);
            }
        }
        else {
            fprintf(stderr, "Whoops. Unimplemented OP code: %d\n", s.type);
            return -1;
        }
    }

    printPostamble(stdout, &db, q->columns, q->column_count, row_list.row_count, output_flags);

    destroyPlan(plan);

    free(row_list.row_ids - q->offset_value);

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