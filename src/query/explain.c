#include <string.h>
#include <libgen.h>
#include <stdlib.h>

#include "../structs.h"
#include "../db/db.h"

#define COVERING_INDEX_SUPPORT 0

long log_10 (long value);

int explain_select_query (
    struct Table *tables,
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

    int log_rows = log_10(row_estimate);


    for (int i = 0; i < plan->step_count; i++) {
        struct PlanStep s = plan->steps[i];

        char *operation = "";
        char table[MAX_TABLE_LENGTH] = {0};
        char predicate[MAX_FIELD_LENGTH] = {0};

        if (s.node_count == 0) {
            predicate[0] = '\0';
        }
        else if (s.type != PLAN_SELECT) {
            char *ptr = predicate;
            for (int i = 0; i < s.node_count; i++) {
                size_t remaining = MAX_FIELD_LENGTH - (ptr - predicate);

                if (i > 0) {
                    *(ptr++) = ';';
                    *(ptr++) = ' ';
                }

                char *name = "";

                struct Node *node = &s.nodes[i];

                // SORT nodes have 1 child; PREDICATE nodes have 2
                if (node->child_count > 1) {
                    node = &node->children[0];
                }

                if (node != NULL) {
                    struct Field *field = (node->child_count <= 0)
                        ? &node->field
                        : &node->children[0].field;

                    if (field != NULL) {
                        name = field->text;

                        if (field->index == FIELD_ROW_INDEX) {
                            name = "rowid";
                        }

                        if (node->function == FUNC_UNITY) {
                            ptr += snprintf(
                                ptr,
                                remaining,
                                "%s",
                                name
                            );
                        }
                        else if (node->function != OPERATOR_ALWAYS) {
                            ptr += snprintf(
                                ptr,
                                remaining,
                                "F(%s)",
                                name
                            );
                        }
                    }
                }

                *ptr = '\0';
            }
        }

        if (s.type == PLAN_TABLE_ACCESS_FULL){
            operation = "TABLE ACCESS FULL";
            rows = getRecordCount(tables[join_count].db);
            cost = rows;

            for (int i = 0; i < s.node_count; i++) {
                if (s.nodes[i].function == OPERATOR_EQ) {
                    rows = rows / 1000;
                } else {
                    rows = rows / 2;
                }
            }

            if (s.limit >= 0) {
                rows = (s.limit < rows) ? s.limit : rows;
            }

            char *name = basename(tables[join_count].name);
            strcpy(table, name);
        }
        else if (s.type == PLAN_TABLE_SCAN){
            operation = "TABLE SCAN";
            rows = getRecordCount(tables[join_count].db);
            cost = rows;

            for (int i = 0; i < s.node_count; i++) {
                if (s.nodes[i].function == OPERATOR_EQ) {
                    rows = 1;
                } else {
                    rows = rows / 2;
                }
            }

            if (s.limit >= 0) {
                rows = (s.limit < rows) ? s.limit : rows;
            }

            char *name = basename(tables[join_count].name);
            strcpy(table, name);
        }
        else if (s.type == PLAN_TABLE_ACCESS_ROWID) {
            operation = "ACCESS BY ROWID";

            if (cost < rows) {
                cost = rows;
            }

            for (int i = 0; i < s.node_count; i++) {
                if (s.nodes[i].function == OPERATOR_EQ) {
                    rows = rows / 1000;
                } else {
                    rows = rows / 2;
                }
            }

            if (s.limit >= 0) {
                rows = (s.limit < rows) ? s.limit : rows;
            }

            if (s.node_count > 0) {
                int table_id = s.nodes[0].field.table_id;

                strncpy(table, tables[table_id].alias, MAX_FIELD_LENGTH);
                table[MAX_FIELD_LENGTH - 1] = '\0';
            }
        }
        else if (s.type == PLAN_PK) {
            operation = "PRIMARY KEY UNIQUE";

            char *t = tables[join_count].alias;

            sprintf(
                table,
                "%s__%s",
                t,
                s.nodes[0].children[0].field.text
            );
            rows = 1;
            cost = log_rows;
        }
        else if (s.type == PLAN_PK_RANGE) {
            operation = "PRIMARY KEY RANGE";

            // char *t = basename(tables[join_count].name);
            char *t = tables[join_count].alias;

            sprintf(
                table,
                "%s__%s",
                t,
                s.nodes[0].children[0].field.text
            );
            rows = getRecordCount(tables[join_count].db) / 2;
            cost = rows;
        }
        else if (s.type == PLAN_UNIQUE) {
            operation = "INDEX UNIQUE";

            char *index_filename;
            findIndex(
                NULL,
                tables[join_count].name,
                &s.nodes[0].children[0],
                INDEX_ANY,
                &index_filename
            );

            strcpy(table, basename(index_filename));
            free(index_filename);

            rows = 1;
            cost = log_rows;
        }
        else if (s.type == PLAN_UNIQUE_RANGE) {
            operation = "INDEX UNIQUE RANGE";

            char *index_filename;
            findIndex(
                NULL,
                tables[join_count].name,
                &s.nodes[0].children[0],
                INDEX_ANY,
                &index_filename
            );

            strcpy(table, basename(index_filename));
            free(index_filename);

            int row_estimate = getRecordCount(tables[join_count].db);
            if (s.node_count > 0) {
                if (s.limit >= 0) {
                    rows = (s.limit < row_estimate) ? s.limit : row_estimate;
                    cost = rows;
                }
                else if (s.nodes[0].function == OPERATOR_EQ) {
                    rows = row_estimate / 1000;
                    cost = log_rows * 2;
                }
                else if (s.nodes[0].function == FUNC_UNKNOWN) {
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
            operation = "INDEX SEEK";

            char *index_filename;
            findIndex(
                NULL,
                tables[join_count].name,
                &s.nodes[0].children[0],
                INDEX_ANY,
                &index_filename
            );

            strcpy(table, basename(index_filename));
            free(index_filename);

            int row_estimate = getRecordCount(tables[join_count].db);
            if (s.node_count > 0) {
                if (s.limit >= 0) {
                    rows = (s.limit < row_estimate) ? s.limit : row_estimate;
                    cost = rows;
                }
                else if (s.nodes[0].function == OPERATOR_EQ) {
                    rows = row_estimate / 1000;
                    cost = log_rows * 2;
                }
                else if (s.nodes[0].function == FUNC_UNKNOWN) {
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

            char *index_filename;
            findIndex(
                NULL,
                tables[join_count].name,
                &s.nodes[0],
                INDEX_ANY,
                &index_filename
            );

            strcpy(table, basename(index_filename));
            free(index_filename);
        }
        else if (s.type == PLAN_COVERING_INDEX_SEEK) {
            operation = "COVERING INDEX SEEK";

            char *index_filename;
            findIndex(
                NULL,
                tables[join_count].name,
                &s.nodes[0],
                INDEX_ANY,
                &index_filename
            );

            strcpy(table, basename(index_filename));
            free(index_filename);
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
            if (cost < rows) {
                cost = rows;
            }
        }
        else if (s.type == PLAN_SLICE) {
            operation = "SLICE";
            if (s.limit < rows) {
                rows = s.limit;
            }
        }
        else if (s.type == PLAN_GROUP_SORTED) {
            operation = "GROUP SORTED";

            if (s.node_count > 0) {
                rows /= 10;
            } else {
                rows = 1;
            }
        }
        else if (s.type == PLAN_GROUP) {
            operation = "GROUP";

            if (s.node_count > 0) {
                rows /= 10;
            } else {
                rows = 1;
            }
        }
        else if (s.type == PLAN_OFFSET) {
            operation = "OFFSET";

            rows -= s.limit;
        }
        else if (s.type == PLAN_SELECT) {
            operation = "SELECT";
        }
        else if (s.type == PLAN_CROSS_JOIN) {
            operation = "CROSS JOIN";

            join_count++;

            struct Table *t = &tables[join_count];

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

            struct Table *t = &tables[join_count];

            strncpy(table, t->alias, MAX_FIELD_LENGTH);
            table[MAX_FIELD_LENGTH - 1] = '\0';

            int record_count = getRecordCount(t->db);

            if (s.node_count > 0) {
                if (s.nodes[0].function == OPERATOR_EQ) {
                    rows += record_count / 1000;
                } else {
                    rows += record_count / 10;
                }

                // We might have been too hasty copying predicate name
                if (s.nodes[0].children[0].field.table_id != join_count) {
                    strcpy(predicate, s.nodes[0].children[1].field.text);
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

            struct Table *t = &tables[join_count];

            strncpy(table, t->alias, MAX_FIELD_LENGTH);
            table[MAX_FIELD_LENGTH - 1] = '\0';

            int record_count = getRecordCount(t->db);

            if (s.node_count > 0) {
                if (s.nodes[0].function == OPERATOR_EQ) {
                    rows *= record_count / 1000;
                } else {
                    rows *= record_count / 10;
                }

                // We might have been too hasty copying predicate name
                if (s.nodes[0].children[0].field.table_id != join_count) {
                    strcpy(predicate, s.nodes[0].children[1].field.text);
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

            struct Table *t = &tables[join_count];

            strncpy(table, t->alias, MAX_FIELD_LENGTH);
            table[MAX_FIELD_LENGTH - 1] = '\0';

            if (cost < rows) {
                cost = rows;
            }
        }
        else if (s.type == PLAN_INDEX_JOIN) {
            operation = "INDEX JOIN";

            join_count++;

            struct Table *t = &tables[join_count];

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

        if (s.type != PLAN_OFFSET && s.limit >= 0) {
            rows = (s.limit < rows) ? s.limit : rows;
        }

        fprintf(
            output,
            "%d,%s,%s,%s,%ld,%ld\n",
            i,
            operation,
            table,
            predicate,
            rows,
            cost
        );
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