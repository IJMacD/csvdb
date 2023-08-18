#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/time.h>

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

extern int debug_verbosity;

int executeQueryPlan (
    struct Query *query,
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

    struct Table *tables = query->tables;
    int table_count = query->table_count;

    for (int i = 0; i < plan->step_count; i++) {
        struct PlanStep *s = &plan->steps[i];

        switch (s->type) {
            case PLAN_DUMMY_ROW:
                result = executeSourceDummyRow(tables, s, result_set);
                break;
            case PLAN_PK:
            case PLAN_PK_RANGE: {
                #ifdef DEBUG
                debugLog(query, "PLAN_PK");
                #endif

                result = executeSourcePK(tables, s, result_set);

                break;
            }

            case PLAN_UNIQUE:
            case PLAN_UNIQUE_RANGE: {
                #ifdef DEBUG
                debugLog(query, "PLAN_UNIQUE");
                #endif

                result = executeSourceUnique(tables, s, result_set);

                break;
            }

            case PLAN_INDEX_RANGE: {
                #ifdef DEBUG
                debugLog(query, "PLAN_INDEX_SEEK");
                #endif

                result = executeSourceIndexSeek(tables, s, result_set);

                break;
            }

            case PLAN_INDEX_SCAN: {
                #ifdef DEBUG
                debugLog(query, "PLAN_INDEX_SCAN");
                #endif

                result = executeSourceIndexScan(tables, s, result_set);

                break;
            }

            case PLAN_COVERING_INDEX_SEEK: {
                #ifdef DEBUG
                debugLog(query, "PLAN_COVERING_INDEX_SEEK");
                #endif

                result = executeSourceCoveringIndexSeek(tables, s, result_set);

                break;
            }

            case PLAN_TABLE_ACCESS_FULL:
            {
                /*************************************************************
                 * Sequentially access every row of the table applying the
                 * predicates to each row accessed.
                 *************************************************************/

                #ifdef DEBUG
                debugLog(query, "PLAN_TABLE_ACCESS_FULL");
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
                debugLog(query, "PLAN_TABLE_SCAN");
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
                debugLog(query, "PLAN_TABLE_ACCESS_ROWID");
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
                debugLog(query, "PLAN_CROSS_JOIN");
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
                debugLog(query, "PLAN_CONSTANT_JOIN");
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
                debugLog(query, "PLAN_LOOP_JOIN");
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
                debugLog(query, "PLAN_UNIQUE_JOIN");
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
                debugLog(query, "PLAN_INDEX_JOIN");
                #endif

                result = executeIndexJoin(tables, s, result_set);

                break;
            }

            case PLAN_SORT: {
                #ifdef DEBUG
                debugLog(query, "PLAN_SORT");
                #endif

                result = executeSort(tables, s, result_set);

                break;
            }

            case PLAN_REVERSE: {
                #ifdef DEBUG
                debugLog(query, "PLAN_REVERSE");
                #endif

                result = executeReverse(tables, s, result_set);

                break;
            }

            case PLAN_SLICE: {
                #ifdef DEBUG
                debugLog(query, "PLAN_SLICE");
                #endif

                result = executeSlice(tables, s, result_set);

                break;
            }

            case PLAN_OFFSET: {
                #ifdef DEBUG
                debugLog(query, "PLAN_OFFSET");
                #endif

                result = executeOffset(tables, s, result_set);

                break;
            }

            case PLAN_GROUP_SORTED: {
                #ifdef DEBUG
                debugLog(query, "PLAN_GROUP_SORTED");
                #endif

                result = executeGroupSorted(tables, s, result_set);

                break;
            }

            case PLAN_GROUP: {
                #ifdef DEBUG
                debugLog(query, "PLAN_GROUP");
                #endif

                result = executeGroupBucket(tables, s, result_set);

                break;
            }

            case PLAN_SELECT: {
                #ifdef DEBUG
                debugLog(query, "PLAN_SELECT");
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
            if (debug_verbosity >= 2) {
                RowListIndex row_list = popRowList(result_set);
                debugRowList(getRowList(row_list), 1);
                pushRowList(result_set, row_list);
            }
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