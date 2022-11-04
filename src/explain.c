#include <string.h>

#include "structs.h"
#include "db.h"

#define COVERING_INDEX_SUPPORT 0

long log_10 (long value);

int explain_select_query (
    struct Query *q,
    struct Plan *plan,
    int output_flags,
    FILE * output
) {
    if (output_flags & OUTPUT_OPTION_HEADERS) {
        fprintf(output, "ID,Operation,Table,Predicate,Rows,Cost\n");
    }

    long rows = 0;
    long cost = 0;

    int join_count = 0;

    int row_estimate = 1;

    if (q->table_count > 0) {
        row_estimate = getRecordCount(q->tables[join_count].db);
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
        else {
            char *ptr = predicate;
            for (int i = 0; i < s.predicate_count; i++) {
                size_t remaining = MAX_FIELD_LENGTH - (ptr - predicate);

                if (i > 0) {
                    *(ptr++) = ';';
                    *(ptr++) = ' ';
                }

                if (s.predicates[i].left.function == FUNC_UNITY) {
                    ptr += snprintf(ptr, remaining, "%s", s.predicates[i].left.fields[0].text);
                }
                else {
                    ptr += snprintf(ptr, remaining, "F(%s)", s.predicates[i].left.fields[0].text);
                }

                *ptr = '\0';
            }
        }

        if (s.type == PLAN_TABLE_ACCESS_FULL){
            operation = "TABLE ACCESS FULL";
            rows = row_estimate;
            cost = rows;

            for (int i = 0; i < s.predicate_count; i++) {
                if (s.predicates[i].op == OPERATOR_EQ) {
                    rows = rows / 1000;
                } else {
                    rows = rows / 2;
                }
            }

            if (s.limit >= 0) {
                rows = (s.limit < rows) ? s.limit : rows;
            }

            strncpy(table, q->tables[join_count].alias, MAX_FIELD_LENGTH);
            table[MAX_FIELD_LENGTH - 1] = '\0';
        }
        else if (s.type == PLAN_TABLE_SCAN){
            operation = "TABLE SCAN";
            rows = row_estimate;
            cost = rows;

            for (int i = 0; i < s.predicate_count; i++) {
                if (s.predicates[i].op == OPERATOR_EQ) {
                    rows = 1;
                } else {
                    rows = rows / 2;
                }
            }

            if (s.limit >= 0) {
                rows = (s.limit < rows) ? s.limit : rows;
            }

            strncpy(table, q->tables[join_count].alias, MAX_FIELD_LENGTH);
            table[MAX_FIELD_LENGTH - 1] = '\0';
        }
        else if (s.type == PLAN_TABLE_ACCESS_ROWID) {
            operation = "ACCESS BY ROWID";

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

            if (s.limit >= 0) {
                rows = (s.limit < rows) ? s.limit : rows;
            }

            if (s.predicate_count > 0) {
                int table_id = s.predicates->left.fields[0].table_id;

                strncpy(table, q->tables[table_id].alias, MAX_FIELD_LENGTH);
                table[MAX_FIELD_LENGTH - 1] = '\0';
            }
        }
        else if (s.type == PLAN_PK) {
            operation = "PRIMARY KEY UNIQUE";
            sprintf(table, "%s__%s", q->tables[join_count].name, s.predicates[0].left.alias);
            rows = 1;
            cost = log_rows;
        }
        else if (s.type == PLAN_PK_RANGE) {
            operation = "PRIMARY KEY RANGE";
            sprintf(table, "%s__%s", q->tables[join_count].name, s.predicates[0].left.alias);
            rows = row_estimate / 2;
            cost = rows;
        }
        else if (s.type == PLAN_UNIQUE) {
            operation = "INDEX UNIQUE";
            sprintf(table, "%s__%s", q->tables[join_count].name, s.predicates[0].left.alias);
            rows = 1;
            cost = log_rows;
        }
        else if (s.type == PLAN_UNIQUE_RANGE) {
            operation = "INDEX UNIQUE RANGE";
            sprintf(table, "%s__%s", q->tables[join_count].name, s.predicates[0].left.alias);
            if (s.predicate_count > 0) {
                if (s.limit >= 0) {
                    rows = (s.limit < row_estimate) ? s.limit : row_estimate;
                    cost = rows;
                }
                else if (s.predicates[0].op == OPERATOR_EQ) {
                    rows = row_estimate / 1000;
                    cost = log_rows * 2;
                }
                else if (s.predicates[0].op == OPERATOR_UN) {
                    rows = row_estimate;
                    cost = rows;
                }
                else {
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
            sprintf(table, "%s__%s", q->tables[join_count].name, s.predicates[0].left.alias);
            if (s.predicate_count > 0) {
                if (s.limit >= 0) {
                    rows = (s.limit < row_estimate) ? s.limit : row_estimate;
                    cost = rows;
                }
                else if (s.predicates[0].op == OPERATOR_EQ) {
                    rows = row_estimate / 1000;
                    cost = log_rows * 2;
                }
                else if (s.predicates[0].op == OPERATOR_UN) {
                    rows = row_estimate;
                    cost = rows;
                }
                else {
                    rows = row_estimate / 2;
                    cost = rows;
                }
            } else {
                rows = row_estimate;
                cost = rows;
            }
        }
        else if (s.type == PLAN_INDEX_SCAN) {
            operation = "INDEX SCAN";
            sprintf(table, "%s__%s", q->tables[join_count].name, s.predicates[0].left.alias);
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

            strncpy(table, t->alias, MAX_FIELD_LENGTH);
            table[MAX_FIELD_LENGTH - 1] = '\0';

            int record_count = getRecordCount(t->db);
            rows *= record_count;
            if (cost < rows) {
                cost = rows;
            }
        }
        else if (s.type == PLAN_CONSTANT_JOIN) {
            operation = "CONSTANT JOIN";

            join_count++;

            struct Table *t = &q->tables[join_count];

            strncpy(table, t->alias, MAX_FIELD_LENGTH);
            table[MAX_FIELD_LENGTH - 1] = '\0';

            int record_count = getRecordCount(t->db);

            if (s.predicate_count > 0) {
                if (s.predicates[0].op == OPERATOR_EQ) {
                    rows += record_count / 1000;
                } else {
                    rows += record_count / 10;
                }

                // We might have been too hasty copying predicate name
                if (s.predicates[0].left.fields[0].table_id != join_count) {
                    strcpy(predicate, s.predicates[0].right.fields[0].text);
                }
            } else {
                rows += record_count;
            }

            if (cost < rows) {
                cost = rows;
            }
        }
        else if (s.type == PLAN_LOOP_JOIN) {
            operation = "LOOP JOIN";

            join_count++;

            struct Table *t = &q->tables[join_count];

            strncpy(table, t->alias, MAX_FIELD_LENGTH);
            table[MAX_FIELD_LENGTH - 1] = '\0';

            int record_count = getRecordCount(t->db);

            if (s.predicate_count > 0) {
                if (s.predicates[0].op == OPERATOR_EQ) {
                    rows *= record_count / 1000;
                } else {
                    rows *= record_count / 10;
                }

                // We might have been too hasty copying predicate name
                if (s.predicates[0].left.fields[0].table_id != join_count) {
                    strcpy(predicate, s.predicates[0].right.fields[0].text);
                }
            } else {
                rows *= record_count;
            }

            if (cost < rows) {
                cost = rows;
            }
        }
        else if (s.type == PLAN_UNIQUE_JOIN) {
            operation = "UNIQUE JOIN";

            join_count++;

            struct Table *t = &q->tables[join_count];

            strncpy(table, t->alias, MAX_FIELD_LENGTH);
            table[MAX_FIELD_LENGTH - 1] = '\0';

            if (cost < rows) {
                cost = rows;
            }
        }
        else if (s.type == PLAN_INDEX_JOIN) {
            operation = "INDEX JOIN";

            join_count++;

            struct Table *t = &q->tables[join_count];

            strncpy(table, t->alias, MAX_FIELD_LENGTH);
            table[MAX_FIELD_LENGTH - 1] = '\0';

            if (cost < rows) {
                cost = rows;
            }
        }
        else if (s.type == PLAN_DUMMY_ROW) {
            operation = "DUMMY ROW";
            rows = 1;
        }
        else {
            operation = "Unknown OP code";
            sprintf(table, "%d", s.type);
        }

        if (s.limit >= 0) {
            rows = (s.limit < rows) ? s.limit : rows;
        }

        fprintf(output, "%d,%s,%s,%s,%ld,%ld\n", i, operation, table, predicate, rows, cost);
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