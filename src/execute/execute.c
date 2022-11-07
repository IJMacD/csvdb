#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/time.h>
#include <unistd.h>

#include "../structs.h"
#include "../query/query.h"
#include "executeSource.h"
#include "executeJoin.h"
#include "executeFilter.h"
#include "executeProcess.h"
#include "../query/output.h"
#include "../query/result.h"
#include "../db/indices.h"
#include "../db/db.h"
#include "../evaluate/evaluate.h"
#include "../evaluate/predicates.h"
#include "../sort/sort-quick.h"
#include "../debug.h"

#ifdef DEBUG
extern int query_count;
#endif

int executeQueryPlan (
    struct Query *q,
    struct Plan *plan,
    enum OutputOption output_flags,
    FILE * output
) {
    /*************************
     * Output headers
     ************************/
    printPreamble(output, NULL, q->columns, q->column_count, output_flags);

    if (output_flags & OUTPUT_OPTION_HEADERS) {
        printHeaderLine(
            output,
            q->tables,
            q->table_count,
            q->columns,
            q->column_count,
            output_flags
        );
    }

    // struct ResultSet results;

    // results.list_count = 1;

    FILE *fstats = NULL;
    struct timeval stop, start;

    if (output_flags & OUTPUT_OPTION_STATS) {
        fstats = fopen("stats.csv", "a");

        gettimeofday(&start, NULL);
    }

    struct ResultSet *result_set = createResultSet();

    int row_count = 0;

    int result = 0;

    for (int i = 0; i < plan->step_count; i++) {
        struct PlanStep *s = &plan->steps[i];

        switch (s->type) {
            case PLAN_DUMMY_ROW:
                result = executeSourceDummyRow(q, s, result_set);
                break;
            case PLAN_PK:
            case PLAN_PK_RANGE: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d.%d: PLAN_PK\n", getpid(), query_count);
                #endif

                result = executeSourcePK(q, s, result_set);

                break;
            }

            case PLAN_UNIQUE:
            case PLAN_UNIQUE_RANGE: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d.%d: PLAN_UNIQUE\n", getpid(), query_count);
                #endif

                result = executeSourceUnique(q, s, result_set);

                break;
            }

            case PLAN_INDEX_RANGE: {
                #ifdef DEBUG
                fprintf(
                    stderr,
                    "Q%d.%d: PLAN_INDEX_SEEK\n",
                    getpid(),
                    query_count
                );
                #endif

                result = executeSourceIndexSeek(q, s, result_set);

                break;
            }

            case PLAN_INDEX_SCAN: {
                #ifdef DEBUG
                fprintf(
                    stderr,
                    "Q%d.%d: PLAN_INDEX_SCAN\n",
                    getpid(),
                    query_count
                );
                #endif

                result = executeSourceIndexScan(q, s, result_set);

                break;
            }

            case PLAN_TABLE_ACCESS_FULL:
            {
                /*************************************************************
                 * Sequentially access every row of the table applying the
                 * predicates to each row accessed.
                 *************************************************************/

                #ifdef DEBUG
                fprintf(
                    stderr,
                    "Q%d.%d: PLAN_TABLE_ACCESS_FULL\n",
                    getpid(),
                    query_count
                );
                #endif

                result = executeSourceTableFull(q, s, result_set);

                break;
            }

            case PLAN_TABLE_SCAN:
            {
                /*************************************************************
                 * Iterate a range of rowids adding each one to the RowList.
                 * Any predicates on this step must ONLY be rowid predicates.
                 *************************************************************/

                #ifdef DEBUG
                fprintf(
                    stderr,
                    "Q%d.%d: PLAN_TABLE_SCAN\n",
                    getpid(),
                    query_count
                );
                #endif

                result = executeSourceTableScan(q, s, result_set);

                break;
            }

            case PLAN_TABLE_ACCESS_ROWID: {
                /*************************************************************
                 * Every row of result set is checked against predicates and
                 * those which pass are added to the output result set.
                 *************************************************************/

                #ifdef DEBUG
                fprintf(
                    stderr,
                    "Q%d.%d: PLAN_TABLE_ACCESS_ROWID\n",
                    getpid(),
                    query_count
                );
                #endif

                result = executeTableAccessRowid(q, s, result_set);

                break;
            }

            case PLAN_CROSS_JOIN: {
                /*************************************************************
                 * Every row of left table is unconditionally joined to every
                 * row of right table.
                 *************************************************************/

                #ifdef DEBUG
                fprintf(
                    stderr,
                    "Q%d.%d: PLAN_CROSS_JOIN\n",
                    getpid(),
                    query_count
                );
                #endif

                result = executeCrossJoin(q, s, result_set);

                break;
            }

            case PLAN_CONSTANT_JOIN: {
                /*************************************************************
                 * Join type where each row on left is joined to an identical
                 * set of rows from right table. The right hand table only
                 * needs to be evaulated once.
                 *************************************************************/

                #ifdef DEBUG
                fprintf(
                    stderr,
                    "Q%d.%d: PLAN_CONSTANT_JOIN\n",
                    getpid(),
                    query_count
                );
                #endif

                result = executeConstantJoin(q, s, result_set);

                break;
            }

            case PLAN_LOOP_JOIN: {
                /*************************************************************
                 * Every row of left table is tested with predicates against
                 * every row of right table and only added to the result set
                 * if all predicates compare true.
                 *************************************************************/
                #ifdef DEBUG
                fprintf(
                    stderr,
                    "Q%d.%d: PLAN_LOOP_JOIN\n",
                    getpid(),
                    query_count
                );
                #endif

                result = executeLoopJoin(q, s, result_set);

                break;
            }

            case PLAN_UNIQUE_JOIN: {
                /*************************************************************
                 * Join type where each row on left is joined to exactly 0 or 1
                 * row on the right table using a unique index.
                 *************************************************************/

                #ifdef DEBUG
                fprintf(
                    stderr,
                    "Q%d.%d: PLAN_UNIQUE_JOIN\n",
                    getpid(),
                    query_count
                );
                #endif

                result = executeUniqueJoin(q, s, result_set);

                break;
            }


            case PLAN_INDEX_JOIN: {
                /*************************************************************
                 * Join type where each row on left is joined to 0 or more
                 * rows on the right table using an index to search for
                 * matching rows.
                 *************************************************************/

                #ifdef DEBUG
                fprintf(
                    stderr,
                    "Q%d.%d: PLAN_INDEX_JOIN\n",
                    getpid(),
                    query_count
                );
                #endif

                result = executeIndexJoin(q, s, result_set);

                break;
            }

            case PLAN_SORT: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d.%d: PLAN_SORT\n", getpid(), query_count);
                #endif

                result = executeSort(q, s, result_set);

                break;
            }

            case PLAN_REVERSE: {
                #ifdef DEBUG
                fprintf(
                    stderr,
                    "Q%d.%d: PLAN_REVERSE\n",
                    getpid(),
                    query_count
                );
                #endif

                result = executeReverse(q, s, result_set);

                break;
            }

            case PLAN_SLICE: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d.%d: PLAN_SLICE\n", getpid(), query_count);
                #endif

                result = executeSlice(q, s, result_set);

                break;
            }

            case PLAN_GROUP_SORTED: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d.%d: PLAN_GROUP_SORTED\n", getpid(), query_count);
                #endif

                result = executeGroupSorted(q, s, result_set);

                break;
            }

            case PLAN_GROUP: {
                #ifdef DEBUG
                fprintf(
                    stderr,
                    "Q%d.%d: PLAN_GROUP\n",
                    getpid(),
                    query_count
                );
                #endif

                result = executeGroupBucket(q, s, result_set);

                break;
            }

            case PLAN_SELECT: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d.%d: PLAN_SELECT\n", getpid(), query_count);
                #endif
                /*******************
                 * Output result set
                 *******************/


                RowListIndex list_id;

                while ((list_id = popRowList(result_set)) >= 0) {
                    struct RowList *row_list = getRowList(list_id);

                    // Aggregate functions will print just one row
                    if (q->flags & FLAG_GROUP) {
                        printResultLine(
                            output,
                            q->tables,
                            q->table_count,
                            q->columns,
                            q->column_count,
                            row_list->row_count > 0
                                ? q->offset_value : RESULT_NO_ROWS,
                            row_list,
                            output_flags
                        );
                        row_count++;
                    }
                    else for (
                        int i = q->offset_value;
                        i < row_list->row_count;
                        i++
                    ) {
                        printResultLine(
                            output,
                            q->tables,
                            q->table_count,
                            q->columns,
                            q->column_count,
                            i,
                            row_list,
                            output_flags
                        );
                        row_count++;
                    }

                    destroyRowList(list_id);
                }

                break;
            }
            default:
                fprintf(stderr, "Unimplemented OP code: %d\n", s->type);
                return -1;
        }

        if (result) {
            return result;
        }

        if (output_flags & OUTPUT_OPTION_STATS) {
            gettimeofday(&stop, NULL);

            fprintf(fstats, "STEP %d,%ld\n", i, dt(stop, start));

            start = stop;
        }

        #ifdef DEBUG
            // debugResultSet(result_set);

            RowListIndex row_list = popRowList(result_set);
            debugRowList(getRowList(row_list), 1);
            pushRowList(result_set, row_list);
        #endif
    }

    if (output_flags & OUTPUT_OPTION_STATS) {
        fclose(fstats);
    }

    printPostamble(
        output,
        NULL,
        q->columns,
        q->column_count,
        row_count,
        output_flags
    );

    // destroyRowListPool();

    free(result_set);

    for (int i = 0; i < q->table_count; i++) {
        closeDB(q->tables[i].db);
    }

    return 0;
}