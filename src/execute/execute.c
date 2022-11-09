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
#include "executeSelect.h"
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
    struct Table *tables,
    int table_count,
    struct Plan *plan,
    enum OutputOption output_flags,
    FILE * output
) {
    // struct ResultSet results;

    // results.list_count = 1;

    FILE *fstats = NULL;
    struct timeval stop, start;

    if (output_flags & OUTPUT_OPTION_STATS) {
        fstats = fopen("stats.csv", "a");

        gettimeofday(&start, NULL);
    }

    struct ResultSet *result_set = createResultSet();

    int result = 0;

    for (int i = 0; i < plan->step_count; i++) {
        struct PlanStep *s = &plan->steps[i];

        switch (s->type) {
            case PLAN_DUMMY_ROW:
                result = executeSourceDummyRow(tables, s, result_set);
                break;
            case PLAN_PK:
            case PLAN_PK_RANGE: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d.%d: PLAN_PK\n", getpid(), query_count);
                #endif

                result = executeSourcePK(tables, s, result_set);

                break;
            }

            case PLAN_UNIQUE:
            case PLAN_UNIQUE_RANGE: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d.%d: PLAN_UNIQUE\n", getpid(), query_count);
                #endif

                result = executeSourceUnique(tables, s, result_set);

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

                result = executeSourceIndexSeek(tables, s, result_set);

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

                result = executeSourceIndexScan(tables, s, result_set);

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

                result = executeSourceTableFull(tables, s, result_set);

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

                result = executeSourceTableScan(tables, s, result_set);

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

                result = executeTableAccessRowid(tables, s, result_set);

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

                result = executeCrossJoin(tables, s, result_set);

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

                result = executeConstantJoin(tables, s, result_set);

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

                result = executeLoopJoin(tables, s, result_set);

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

                result = executeUniqueJoin(tables, s, result_set);

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

                result = executeIndexJoin(tables, s, result_set);

                break;
            }

            case PLAN_SORT: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d.%d: PLAN_SORT\n", getpid(), query_count);
                #endif

                result = executeSort(tables, s, result_set);

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

                result = executeReverse(tables, s, result_set);

                break;
            }

            case PLAN_SLICE: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d.%d: PLAN_SLICE\n", getpid(), query_count);
                #endif

                result = executeSlice(tables, s, result_set);

                break;
            }

            case PLAN_OFFSET: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d.%d: PLAN_OFFSET\n", getpid(), query_count);
                #endif

                result = executeOffset(tables, s, result_set);

                break;
            }

            case PLAN_GROUP_SORTED: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d.%d: PLAN_GROUP_SORTED\n", getpid(), query_count);
                #endif

                result = executeGroupSorted(tables, s, result_set);

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

                result = executeGroupBucket(tables, s, result_set);

                break;
            }

            case PLAN_SELECT: {
                #ifdef DEBUG
                fprintf(stderr, "Q%d.%d: PLAN_SELECT\n", getpid(), query_count);
                #endif

                result = executeSelect(
                    output,
                    output_flags,
                    tables,
                    table_count,
                    s,
                    result_set
                );

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

    destroyRowListPool();

    free(result_set->row_list_indices);
    free(result_set);

    return 0;
}