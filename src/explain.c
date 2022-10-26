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
        fprintf(output, "ID\tOperation           \tTable       \tPredicate\tRows\tCost\n");
    }

    long rows = 0;
    long cost = 0;

    int join_count = 0;

    int row_estimate = 1;

    if (q->table_count > 0) {
        row_estimate = q->tables[join_count].db->record_count;
    }

    int log_rows = log_10(row_estimate);


    for (int i = 0; i < plan->step_count; i++) {
        struct PlanStep s = plan->steps[i];

        char *operation = "";
        char table[MAX_FIELD_LENGTH] = {0};
        char predicate[MAX_FIELD_LENGTH] = {0};

        if (s.predicate_count == 0) {
            predicate[0] = '\0';
        }
        else if (s.predicate_count == 1) {
            strcpy(predicate, s.predicates[0].left.fields[0].text);
        }
        else {
            char *ptr = predicate;
            for (int i = 0; i < s.predicate_count; i++) {
                size_t l = strlen(s.predicates[i].left.fields[0].text);

                if (ptr + l > predicate + MAX_FIELD_LENGTH) break;

                if (i > 0) {
                    *(ptr++) = ',';
                }

                strcpy(ptr, s.predicates[i].left.fields[0].text);

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

            strncpy(table, q->tables[join_count].name, MAX_FIELD_LENGTH);
            table[MAX_FIELD_LENGTH - 1] = '\0';
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

            if (s.predicate_count > 0) {
                int table_id = s.predicates->left.fields[0].table_id;

                strncpy(table, q->tables[table_id].name, MAX_FIELD_LENGTH);
                table[MAX_FIELD_LENGTH - 1] = '\0';
            }
        }
        else if (s.type == PLAN_PK) {
            operation = "PRIMARY KEY UNIQUE";
            strncpy(table, q->tables[join_count].name, MAX_FIELD_LENGTH);
            table[MAX_FIELD_LENGTH - 1] = '\0';
            rows = 1;
            cost = log_rows;
        }
        else if (s.type == PLAN_PK_RANGE) {
            operation = "PRIMARY KEY RANGE";
            strncpy(table, q->tables[join_count].name, MAX_FIELD_LENGTH);
            table[MAX_FIELD_LENGTH - 1] = '\0';
            rows = row_estimate / 2;
            cost = rows;
        }
        else if (s.type == PLAN_UNIQUE) {
            operation = "INDEX UNIQUE";
            strncpy(table, q->tables[join_count].name, MAX_FIELD_LENGTH);
            table[MAX_FIELD_LENGTH - 1] = '\0';
            rows = 1;
            cost = log_rows;
        }
        else if (s.type == PLAN_UNIQUE_RANGE) {
            operation = "INDEX UNIQUE RANGE";
            strncpy(table, q->tables[join_count].name, MAX_FIELD_LENGTH);
            table[MAX_FIELD_LENGTH - 1] = '\0';
            if (s.predicate_count > 0) {
                if (s.limit >= 0) {
                    rows = (s.limit < row_estimate) ? s.limit : row_estimate;
                    cost = rows;
                }
                else if (s.predicates[0].op == OPERATOR_EQ) {
                    rows = row_estimate / 1000;
                    cost = log_rows * 2;
                } else if (s.predicates[0].op == OPERATOR_UN) {
                    rows = row_estimate;
                    cost = rows;
                } else {
                    rows = row_estimate / 2;
                    cost = rows;
                }
            } else {
                rows = row_estimate;
                cost = rows;
            }
        }
        else if (s.type == PLAN_INDEX_RANGE) {
            operation = "INDEX RANGE";
            strncpy(table, q->tables[join_count].name, MAX_FIELD_LENGTH);
            table[MAX_FIELD_LENGTH - 1] = '\0';
            if (s.predicate_count > 0) {
                if (s.limit >= 0) {
                    rows = (s.limit < row_estimate) ? s.limit : row_estimate;
                    cost = rows;
                }
                else if (s.predicates[0].op == OPERATOR_EQ) {
                    rows = row_estimate / 1000;
                    cost = log_rows * 2;
                } else if (s.predicates[0].op == OPERATOR_UN) {
                    rows = row_estimate;
                    cost = rows;
                } else {
                    rows = row_estimate / 2;
                    cost = rows;
                }
            } else {
                rows = row_estimate;
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
            if (s.limit < rows) {
                rows = s.limit;
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

            rows -= q->offset_value;
        }
        else if (s.type == PLAN_CROSS_JOIN) {
            operation = "CROSS JOIN";

            join_count++;

            struct Table *t = &q->tables[join_count];

            strncpy(table, t->name, MAX_FIELD_LENGTH);
            table[MAX_FIELD_LENGTH - 1] = '\0';

            rows *= t->db->record_count;
            if (cost < rows) {
                cost = rows;
            }
        }
        else if (s.type == PLAN_CONSTANT_JOIN) {
            operation = "CONSTANT JOIN";

            join_count++;

            struct Table *t = &q->tables[join_count];

            strncpy(table, t->name, MAX_FIELD_LENGTH);
            table[MAX_FIELD_LENGTH - 1] = '\0';

            if (s.predicate_count > 0) {
                if (s.predicates[0].op == OPERATOR_EQ) {
                    rows += t->db->record_count / 1000;
                } else {
                    rows += t->db->record_count / 10;
                }

                // We might have been too hasty copying predicate name
                if (s.predicates[0].left.fields[0].table_id != join_count) {
                    strcpy(predicate, s.predicates[0].right.fields[0].text);
                }
            } else {
                rows += t->db->record_count;
            }

            if (cost < rows) {
                cost = rows;
            }
        }
        else if (s.type == PLAN_LOOP_JOIN) {
            operation = "LOOP JOIN";

            join_count++;

            struct Table *t = &q->tables[join_count];

            strncpy(table, t->name, MAX_FIELD_LENGTH);
            table[MAX_FIELD_LENGTH - 1] = '\0';

            if (s.predicate_count > 0) {
                if (s.predicates[0].op == OPERATOR_EQ) {
                    rows *= t->db->record_count / 1000;
                } else {
                    rows *= t->db->record_count / 10;
                }

                // We might have been too hasty copying predicate name
                if (s.predicates[0].left.fields[0].table_id != join_count) {
                    strcpy(predicate, s.predicates[0].right.fields[0].text);
                }
            } else {
                rows *= t->db->record_count;
            }

            if (cost < rows) {
                cost = rows;
            }
        }
        else if (s.type == PLAN_UNIQUE_JOIN) {
            operation = "UNIQUE JOIN";

            join_count++;

            struct Table *t = &q->tables[join_count];

            strncpy(table, t->name, MAX_FIELD_LENGTH);
            table[MAX_FIELD_LENGTH - 1] = '\0';

            // We might have been too hasty copying predicate name
            if (s.predicates[0].left.fields[0].table_id != join_count) {
                strcpy(predicate, s.predicates[0].right.fields[0].text);
            }

            if (cost < rows) {
                cost = rows;
            }
        }
        else {
            operation = "Unknown OP code";
            sprintf(predicate, "%d\n", s.type);
        }

        fprintf(output, "%d\t%-23s\t%-15s\t%-15s\t%ld\t%ld\n", i, operation, table, predicate, rows, cost);
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