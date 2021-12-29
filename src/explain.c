#include <string.h>

#include "explain.h"
#include "query.h"
#include "predicates.h"
#include "indices.h"
#include "limits.h"
#include "output.h"

#define COVERING_INDEX_SUPPORT 0

long log_10 (long value);

int explain_select_query (
    struct Query *q,
    struct Plan *plan,
    int output_flags,
    FILE * output
) {
    if (output_flags & OUTPUT_OPTION_HEADERS) {
        fprintf(output, "ID\tOperation\t\tName\t\tRows\tCost\n");
    }

    int row_estimate = q->tables[0].db->record_count;
    int log_rows = log_10(row_estimate);

    long rows = 0;
    long cost = 0;

    int join_count = 0;

    for (int i = 0; i < plan->step_count; i++) {
        struct PlanStep s = plan->steps[i];

        char *operation = "";
        char predicate[FIELD_MAX_LENGTH];

        if (s.predicate_count == 0) {
            predicate[0] = '\0';
        }
        else if (s.predicate_count == 1) {
            strcpy(predicate, s.predicates[0].left.text);
        }
        else {
            char *ptr = predicate;
            for (int i = 0; i < s.predicate_count; i++) {
                size_t l = strlen(s.predicates[i].left.text);

                if (ptr + l > predicate + FIELD_MAX_LENGTH) break;

                if (i > 0) {
                    *(ptr++) = ',';
                }

                strcpy(ptr, s.predicates[i].left.text);

                ptr += l;

                *ptr = '\0';
            }
        }

        if (s.type == PLAN_TABLE_ACCESS_FULL){
            operation = "TABLE ACCESS FULL";
            rows = row_estimate;
            int l = q->limit_value + q->offset_value;
            if (l >= 0 && l < rows && !(q->flags & FLAG_ORDER)) {
                rows = l;
            }
            cost = rows;

            for (int i = 0; i < s.predicate_count; i++) {
                if (s.predicates[i].op == OPERATOR_EQ) {
                    rows = rows / 1000;
                } else {
                    rows = rows / 2;
                }
            }

            if (q->predicate_count == 0) {
                strcpy(predicate, q->tables[0].name);
            }
        }
        else if (s.type == PLAN_TABLE_ACCESS_ROWID) {
            operation = "TABLE ACCESS BY ROWID";

            if (cost < rows) {
                cost = rows;
            }

            for (int i = 0; i < s.predicate_count; i++) {
                if (s.predicates[i].op == OPERATOR_EQ) {
                    rows = rows / 1000;
                } else {
                    rows = rows / 2;
                }
            }

            if (s.predicate_count == 0) {
                strcpy(predicate, q->tables[0].name);
            }
        }
        else if (s.type == PLAN_PK_UNIQUE) {
            operation = "PRIMARY KEY UNIQUE";
            rows = 1;
            cost = log_rows;
        }
        else if (s.type == PLAN_PK_RANGE) {
            operation = "PRIMARY KEY RANGE";
            rows = row_estimate / 2;
            cost = rows;
        }
        else if (s.type == PLAN_INDEX_UNIQUE) {
            operation = "INDEX UNIQUE";
            rows = 1;
            cost = log_rows;
        }
        else if (s.type == PLAN_INDEX_RANGE) {
            operation = "INDEX RANGE";
            if (s.predicate_count > 0 && s.predicates[0].op == OPERATOR_EQ) {
                rows = row_estimate / 1000;
                cost = log_rows * 2;
            } else {
                rows = row_estimate / 2;
                cost = rows;
            }
        }
        else if (s.type == PLAN_SORT) {
            operation = "SORT";
            long new_cost = rows * rows;
            if (cost < new_cost) {
                cost = new_cost;
            }
        }
        else if (s.type == PLAN_REVERSE) {
            operation = "REVERSE";
            // if (cost < rows) {
            //     cost = rows;
            // }
        }
        else if (s.type == PLAN_SLICE) {
            operation = "SLICE";
            if (s.param2 < rows) {
                rows = s.param2;
            }
        }
        else if (s.type == PLAN_GROUP) {
            operation = "GROUP";

            if (s.predicate_count > 0) {
                rows /= 10;
            } else {
                rows = 1;
            }
        }
        else if (s.type == PLAN_SELECT) {
            operation = "SELECT";
        }
        else if (s.type == PLAN_CROSS_JOIN) {
            operation = "CROSS JOIN";

            join_count++;

            struct Table *table = &q->tables[join_count];

            strcpy(predicate, table->name);

            rows *= table->db->record_count;
            if (cost < rows) {
                cost = rows;
            }
        }
        else if (s.type == PLAN_CONSTANT_JOIN) {
            operation = "CONSTANT JOIN";

            join_count++;

            struct Table *table = &q->tables[join_count];

            if (s.predicate_count > 0) {
                if (s.predicates[0].op == OPERATOR_EQ) {
                    rows += table->db->record_count / 1000;
                } else {
                    rows += table->db->record_count / 10;
                }
            } else {
                strcpy(predicate, table->name);
                rows += table->db->record_count;
            }

            if (cost < rows) {
                cost = rows;
            }
        }
        else if (s.type == PLAN_INNER_JOIN) {
            operation = "INNER JOIN";

            join_count++;

            struct Table *table = &q->tables[join_count];

            if (s.predicate_count > 0) {
                if (s.predicates[0].op == OPERATOR_EQ) {
                    rows *= table->db->record_count / 1000;
                } else {
                    rows *= table->db->record_count / 10;
                }
            } else {
                strcpy(predicate, table->name);
                rows *= table->db->record_count;
            }

            if (cost < rows) {
                cost = rows;
            }
        } else {
            operation = "Unknown OP code";
            sprintf(predicate, "%d\n", s.type);
        }

        fprintf(output, "%d\t%-23s\t%-15s\t%ld\t%ld\n", i, operation, predicate, rows, cost);
    }

    return 0;
}

long log_10 (long value) {
    long i = 0;
    while (value > 0) {
        value /= 10;
        i++;
    }
    return i;
}