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

int basic_select_query (struct Query *q, struct Plan *plan, int output_flags);

int information_query (const char *table);

static int populateTables (struct Query *q, struct DB * dbs);

static int populateColumns (struct Query *q);

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

    if (q.table_count == 0) {
        // No table was specified.
        // However, if stdin is something more than a tty (i.e pipe or redirected file)
        // then we can default to it.
        if (!isatty(fileno(stdin))) {
            q.tables = malloc(sizeof (struct Table));
            q.table_count = 1;
            strcpy(q.tables[0].name, "stdin");
        }
        else {
            fprintf(stderr, "Table not specified\n");
            return -1;
        }
    }

    if (strcmp(q.tables[0].name, "INFORMATION") == 0) {
        if (q.predicate_count < 1) {
            return -1;
        }

        int result = information_query(q.predicates[0].value);
        destroyQuery(&q);
        return result;
    }

    /*************************
     * Begin Query processing
     *************************/

    struct DB dbs[TABLE_MAX_COUNT];

    if (q.table_count == 0) {
        fprintf(stderr, "No tables\n");
        exit(-1);
    }

    int result = populateTables(&q, dbs);
    if (result < 0) {
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

    result = basic_select_query(&q, &plan, output_flags);
    destroyQuery(&q);
    return result;
}

int basic_select_query (
    struct Query *q,
    struct Plan *plan,
    int output_flags
) {
    // We know that the dbs storage is on the stack and starts at the db
    // pointed to by the first table.
    struct DB *dbs = q->tables[0].db;

    /*************************
     * Output headers
     ************************/
    printPreamble(stdout, NULL, q->columns, q->column_count, output_flags);

    if (output_flags & OUTPUT_OPTION_HEADERS) {
        printHeaderLine(stdout, dbs, q->table_count, q->columns, q->column_count, output_flags);
    }

    struct ResultSet results;

    results.list_count = 1;

    struct RowList row_list;

    results.row_lists = &row_list;

    // Provision enough result space for maximum of all rows in first table
    row_list.row_ids = malloc(sizeof (int) * q->tables[0].db->record_count);

    row_list.join_count = 1;
    row_list.row_count = 0;

    for (int i = 0; i < plan->step_count; i++) {
        struct PlanStep s = plan->steps[i];

        // Handy reference to first table
        struct Table table = q->tables[0];
        struct DB * db = table.db;

        if (s.type == PLAN_PK_UNIQUE || s.type == PLAN_PK_RANGE) {
            struct Predicate p = s.predicates[0];
            primaryKeyScan(db, p.field, p.op, p.value, &row_list);
        }
        else if (s.type == PLAN_INDEX_UNIQUE) {
            struct Predicate p = s.predicates[0];
            indexUniqueScan(table.name, p.field, p.op, p.value, &row_list);
        }
        else if (s.type == PLAN_INDEX_RANGE) {
            struct Predicate p = s.predicates[0];
            indexRangeScan(table.name, p.field, p.op, p.value, &row_list);
        }
        else if (s.type == PLAN_TABLE_ACCESS_FULL) {
            if (s.predicate_count > 0) {
                fullTableScan(db, &row_list, s.predicates, s.predicate_count, q->limit_value + q->offset_value);
            }
            else {
                fullTableAccess(db, &row_list, q->limit_value + q->offset_value);
            }
        }
        else if (s.type == PLAN_TABLE_ACCESS_ROWID) {
            for (int i = 0; i < s.predicate_count; i++) {
                struct Predicate p = s.predicates[i];
                filterRows(q, &row_list, &p, &row_list);
            }
        }
        else if (s.type == PLAN_CROSS_JOIN) {
            // Actually we're just doing CROSS-SELF JOIN here
            // TODO: implement more of proper join

            struct RowList new_list;

            struct DB *next_db = q->tables[row_list.join_count].db;

            int new_length = row_list.row_count * next_db->record_count;

            new_list.join_count = row_list.join_count + 1;
            new_list.row_count = 0;
            new_list.row_ids = malloc((sizeof (int *)) * new_list.join_count * new_length);

            for (int i = 0; i < row_list.row_count; i++) {
                for (int j = 0; j < next_db->record_count; j++) {
                    appendJoinedRowID(&new_list, &row_list, i, j);
                }
            }

            // Free the old list
            free(row_list.row_ids);

            // Copy updated values back
            row_list.join_count = new_list.join_count;
            row_list.row_count = new_list.row_count;
            row_list.row_ids = new_list.row_ids;
        }
        else if (s.type == PLAN_SORT) {
            int order_index = getFieldIndex(db, s.predicates[0].field);
            sortResultRows(db, order_index, s.predicates[0].op, &row_list, &row_list);
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

            int result = populateColumns(q);
            if (result < 0) {
                return result;
            }

            // Aggregate functions will print just one row
            if (q->flags & FLAG_GROUP) {
                // printf("Aggregate result:\n");
                printResultLine(stdout, dbs, q->table_count, q->columns, q->column_count, row_list.row_count > 0 ? q->offset_value : RESULT_NO_ROWS, &row_list, output_flags);
            }
            else for (int i = 0; i < row_list.row_count; i++) {

                // ROW_NUMBER is offset by OFFSET from result index and is 1-index based
                printResultLine(stdout, dbs, q->table_count, q->columns, q->column_count, i, &row_list, output_flags);
            }
        }
        else {
            fprintf(stderr, "Whoops. Unimplemented OP code: %d\n", s.type);
            return -1;
        }
    }

    printPostamble(stdout, NULL, q->columns, q->column_count, row_list.row_count, output_flags);

    destroyPlan(plan);

    free(row_list.row_ids - q->offset_value);

    for (int i = 0; i < q->table_count; i++) {
        closeDB(q->tables[i].db);
    }

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

static int populateColumns (struct Query * q) {
    // Fill in selected table id and column indexes
    for (int i = 0; i < q->column_count; i++) {
        struct ResultColumn *column = &(q->columns[i]);

        if (column->field == FIELD_UNKNOWN) {
            findColumn(q, column->text, &column->table_id, &column->field);

            if (column->field == FIELD_UNKNOWN) {
                fprintf(stderr, "Field %s not found\n", column->text);
                return -1;
            }
        }
    }

    return 0;
}

static int populateTables (struct Query *q, struct DB *dbs) {

    for (int i = 0; i < q->table_count; i++) {
        struct Table *table = &q->tables[i];

        int found = 0;

        // Try to reuse existing open table
        for (int j = 0; j < i; j++) {
            if (strcmp(q->tables[j].name, table->name) == 0) {
                // Copy pointer
                table->db = q->tables[j].db;

                // Make actual copy of DB for output functions
                memcpy(&dbs[i], &dbs[j], sizeof (dbs[i]));

                found = 1;
                break;
            }
        }

        if (found == 1) {
            continue;
        }

        if (openDB(&dbs[i], table->name) != 0) {
            fprintf(stderr, "File not found: '%s'\n", table->name);
            return -1;
        }

        table->db = &dbs[i];
    }

    return 0;
}

void findColumn (struct Query *q, const char *text, int *table_id, int *column_id) {

    int dot_index = str_find_index(text, '.');

    if (dot_index >= 0) {
        char value[FIELD_MAX_LENGTH];

        strncpy(value, text, dot_index);
        value[dot_index] = '\0';

        for (int i = 0; i < q->table_count; i++) {
            if (strcmp(q->tables[i].name, value) == 0 ||
                strcmp(q->tables[i].alias, value) == 0)
            {
                *table_id = i;

                if (text[dot_index + 1] == '*') {
                    *column_id = FIELD_STAR;
                }
                else if (strcmp(text + dot_index + 1, "rowid") == 0) {
                    *column_id = FIELD_ROW_INDEX;
                }
                else {
                    struct DB *db = q->tables[i].db;

                    *column_id = getFieldIndex(db, text + dot_index + 1);
                }

                return;
            }
        }
    }
    else {
        for (int i = 0; i < q->table_count; i++) {
            struct DB *db = q->tables[i].db;

            *column_id = getFieldIndex(db, text);

            if (*column_id != FIELD_UNKNOWN) {
                *table_id = i;

                return;
            }
        }
    }

    // Couldn't find column
    *column_id = FIELD_UNKNOWN;
}