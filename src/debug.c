#include <stdio.h>
#include <string.h>

#include <unistd.h>

#include "structs.h"
#include "query/result.h"
#include "debug.h"

static void debugNodeInner (struct Node * node, int depth);

const char *FUNC_NAMES[] = {
    // 0x00
    "","CHAR","TO_HEX","","","","","",
    "","","RANDOM","","","","","",
    // 0x10
    "","ADD","SUB","MUL","DIV","MOD","POW","",
    "","","","","","","","",
    // 0x20
    "","LENGTH","LEFT","RIGHT","CONCAT","","","",
    "","","","","","","","",
    // 0x30
    "","","","","","","","",
    "","","","","","","","",
    // 0x40
    "","EXTRACT<YEAR>","EXTRACT<MONTH>","EXTRACT<DAY>","EXTRACT<WEEK>","EXTRACT<WEEKDAY>","EXTRACT<WEEKYEAR>","EXTRACT<YEARDAY>",
    "EXTRACT<HEYEAR>","EXTRACT<MILLENNIUM>","EXTRACT<CENTURY>","EXTRACT<DECADE>","EXTRACT<QUARTER>","EXTRACT<HOUR>","EXTRACT<MINUTE>","EXTRACT<SECOND>",
    // 0x50
    "EXTRACT<MONTH_STRING>","EXTRACT<WEEK_STRING>","EXTRACT<YEARDAY_STRING>","","","","","",
    "","","","","EXTRACT<JULIAN>","EXTRACT<DATE>","EXTRACT<TIME>","EXTRACT<DATETIME>",
    // 0x60
    "","DATE_ADD","DATE_SUB","DATE_DIFF","","","","",
    "","","","","","","","",
    // 0x70
    "TODAY","","","","","","","",
    "","","","","","","","",
    // 0x80
    "","","","","","","","",
    "","","","","","","","",
    // 0x90
    "","","","","","","","",
    "","","","","","","","",
    // 0xA0
    "","COUNT","MIN","MAX","SUM","AVG","LISTAGG",""
    "","","","","","","","",
};

size_t FUNC_NAMES_COUNT = sizeof (FUNC_NAMES) / sizeof (FUNC_NAMES[0]);

void debugRowList (struct RowList * list, int verbosity) {
    if (list == NULL) {
        fprintf(stderr, "RowList (NULL)\n");
        return;
    }

    fprintf(
        stderr,
        "RowList (%d joins x %d rows)\n",
        list->join_count,
        list->row_count
    );

    if (verbosity > 3) {
        for (int i = 0; i < list->row_count; i++) {
            fprintf(stderr, "Index %3d, Rowids: (", i);
            for (int j = 0; j < list->join_count; j++) {
                if (j > 0) {
                    fprintf(stderr, ", ");
                }
                fprintf(stderr, "%d", getRowID(list, j, i));
            }
            fprintf(stderr, ")\n");
        }
    }
}

void debugTree (struct TreeNode * node) {
    printf(
        "{ \"key\": %d, \"value\": \"%s\", \"left\": ",
        node->key,
        node->value
    );

    if (node->left != NULL) {
        debugTree(node->left);
    }
    else printf(" null ");

    printf(", \"right\": ");

    if (node->right != NULL) {
        debugTree(node->right);
    }
    else printf(" null ");

    printf(" }");
}

void debugResultSet (struct ResultSet *results) {
    fprintf(stderr, "\tResultSet (count = %d): ", results->count);
    for (int i = 0; i < results->count; i++) {
        fprintf(stderr, "%d, ", results->row_list_indices[i]);
    }
    fprintf(stderr, "\n");
}

void debugNode (struct Node * node) {
    debugNodeInner(node, 0);
}

static void debugNodeInner (struct Node * node, int depth) {
    fprintf(
        stderr,
        "\t%*s[NODE] ", depth * 2, "");

    if (node->function != FUNC_UNITY) {
        if ((node->function & MASK_FUNC_FAMILY) == FUNC_FAM_OPERATOR) {
            char *operators[] = {
                "NEVER","EQ","LT","LE","GT","GE","NE","ALWAYS","LIKE","OR","AND"
            };
            char *s = operators[node->function & ~MASK_FUNC_FAMILY];
            fprintf(
                stderr,
                "OPERATOR_%s ",
                s
            );
        }
        else if (node->function == FUNC_PARENS) {
            fprintf(stderr, "PARENS");
        }
        else if ((node->function & (MASK_FUNC_FAMILY | 0x10)) == 0x10) {
            char *operators[] = {
                "","+","-","*","/","%","^"
            };
            char *s = operators[node->function & ~(MASK_FUNC_FAMILY | 0x10)];
            fprintf(
                stderr,
                "OPERATOR %s ",
                s
            );
        }
        else if (node->function < FUNC_NAMES_COUNT) {
            fprintf(
                stderr,
                "Function %s() ",
                FUNC_NAMES[node->function]
            );
        }
        else {
            fprintf(
                stderr,
                "Function: 0x%X ",
                node->function
            );
        }
    }

    if (node->function == FUNC_UNITY || node->child_count == -1) {
        if (node->field.index == FIELD_CONSTANT) {
            fprintf(
                stderr,
                "Field: { CONSTANT text = %s } ",
                node->field.text
            );
        }
        else if (node->field.index == FIELD_STAR) {
            fprintf(
                stderr,
                "Field: { *, table_id = %d } ",
                node->field.table_id
            );
        }
        else if (node->field.index != FIELD_UNKNOWN) {
            fprintf(
                stderr,
                "Field: { table_id = %d, index = %d, text = %s } ",
                node->field.table_id,
                node->field.index,
                node->field.text
            );
        }
        else {
            fprintf(
                stderr,
                "Field: { UNKNOWN text = %s } ",
                node->field.text
            );
        }
    }

    if (strlen(node->alias)) {
        fprintf(stderr, "[Alias: '%s']", node->alias);
    }

    fprintf(stderr, "\n");

    for (int i = 0; i < node->child_count; i++) {
        debugNodeInner(node->children + i, depth + 1);
    }
}

void debugNodes (struct Node nodes[], int node_count) {
    for (int i = 0; i < node_count; i++) {
        debugNode(&nodes[i]);
    }
}

void debugLog (struct Query *query, const char *msg) {
    #ifdef DEBUG
    if (debug_verbosity > 0) {
        fprintf(stderr, "[Q%d.%d] %s\n", getpid(), query->id, msg);
    }
    #endif
}

void debugFrom (struct Query *query) {
    if (query->table_count == 0) {
        fprintf(stderr, "      DUMMY\n");
    }
    else for(int i = 0; i < query->table_count; i++) {
        struct Table *table = &query->tables[i];
        fprintf(stderr, "      %s [Alias: '%s']", table->name, table->alias);
        if (i == 0) {
            fprintf(stderr, "\n");
        }
        else {
            if (table->join.function == OPERATOR_ALWAYS) {
                fprintf(stderr, ", CROSS\n");
            }
            else {
                const char *JOIN_TYPES[] = {"INNER","LEFT"};
                fprintf(stderr, ", %s\n", JOIN_TYPES[table->join_type]);
            }
            debugNode(&table->join);
        }
    }
}