#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "../structs.h"
#include "parseNode.h"
#include "token.h"
#include "query.h"
#include "../db/csv-mem.h"
#include "../functions/util.h"
#include "node.h"
#include "../debug.h"

#define MAX_CTES    10

static struct Table *findTable (
    const char *table_name,
    struct Table *tables,
    int table_count
);

int parseQuery (struct Query *q, const char *query, const char **end_ptr) {
    /*********************
     * Begin Query parsing
     *********************/

    size_t index = 0;

    q->table_count = 0;

    q->predicate_count = 0;

    q->order_count = 0;

    q->group_count = 0;

    q->flags = 0;
    q->offset_value = 0;
    q->limit_value = -1;

    struct Table ctes[MAX_CTES] = {0};
    int cte_count = 0;

    skipWhitespace(query, &index);

    if (
        strncmp(query + index, "EXPLAIN", 7) == 0
        && isspace(query[index + 7])
    ) {
        q->flags |= FLAG_EXPLAIN;
        index += 8;
    }

    skipWhitespace(query, &index);

    // Special treatment for VALUES only (top-level) query
    // Will behave as a query: SELECT * FROM values_mem
    // Where values_mem is a simulated DB in memory
    if (
        strncmp(query + index, "VALUES", 6) == 0
        && isspace(query[index + 6])
    ) {
        index += 7;

        q->tables = calloc(1, sizeof(q->tables[0]));
        q->table_count = 1;

        struct Table *table = &q->tables[0];

        skipWhitespace(query, &index);

        // Malloc a DB ahead of time.
        struct DB *db = calloc(1, sizeof(*db));

        const char * end = csvMem_fromValues(db, query + index, -1);

        if (end_ptr != NULL) {
            *end_ptr = end;
        }

        table->db = db;

        strcpy(table->alias, "values");

        return 0;
    }

    // Special treatment for TABLE query
    // Will behave as a query: SELECT * FROM table
    if (strncmp(query + index, "TABLE", 5) == 0 && isspace(query[index + 5])) {
        index += 6;

        char name[MAX_TABLE_LENGTH] = {0};

        getQuotedToken(query, &index, name, MAX_FIELD_LENGTH);

        if (name[0] == '\0') {
            fprintf(stderr, "expected a table name\n");
            return -1;
        }

        skipWhitespace(query, &index);

        if (query[index] != '\0' && query[index] != ';') {
            fprintf(
                stderr,
                "expected end of TABLE query. found '%s'\n",
                query + index
            );
            return -1;
        }

        if (query[index] == ';') {
            index++;
        }

        if (end_ptr != NULL) {
            *end_ptr = query + index;
        }

        char buffer[1024];
        sprintf(buffer, "FROM \"%s\"", name);

        return parseQuery(q, buffer, NULL);
    }

    char keyword[MAX_FIELD_LENGTH] = {0};

    while (query[index] != '\0' && query[index] != ';') {
        if (query[index] == ';') {
            break;
        }

        int token_length = getToken(query, &index, keyword, MAX_FIELD_LENGTH);

        if (token_length == 0) {
            // This means it consumed whitespace then found the end of the input
            break;
        }

        if (token_length < 0) {
            fprintf(
                stderr,
                "Unable to get next token but not at end of query. Remaining: "
                "'%s'\n",
                query + index
            );
            return -1;
        }

        // printf("Token: '%s'\n", keyword);

        if (strcmp(keyword, "WITH") == 0) {
            while (query[index] != '\0' && query[index] != ';') {
                int cte_index = cte_count++;

                if (cte_index >= MAX_CTES) {
                    fprintf(
                        stderr,
                        "Cannot have more than %d ctes.\n",
                        MAX_CTES
                    );
                    return -1;
                }

                struct Table *cte = ctes + cte_index;

                getQuotedToken(query, &index, cte->alias, MAX_TABLE_LENGTH);

                getToken(query, &index, keyword, MAX_FIELD_LENGTH);

                if (strcmp(keyword, "AS") != 0) {
                    fprintf(stderr, "expected AS\n");
                    return -1;
                }

                skipWhitespace(query, &index);

                if (query[index] != '(') {
                    fprintf(stderr, "Expected '('\n");
                    return -1;
                }

                int len = find_matching_parenthesis(query + index);

                if (len >= MAX_TABLE_LENGTH) {
                    fprintf(
                        stderr,
                        "CTEs longer than %d are not supported. CTE was %d "
                        "bytes.\n",
                        MAX_TABLE_LENGTH,
                        len
                    );
                    return -1;
                }

                strncpy(cte->name, query + index + 1, len - 2);

                // Indicate to query processor this is a subquery
                cte->db = DB_SUBQUERY;

                index += len;

                skipWhitespace(query, &index);

                if (query[index] != ',') {
                    break;
                }

                index++;
            }
        }
        else if (strcmp(keyword, "SELECT") == 0) {

            int curr_index = 0;
            while (query[index] != '\0' && query[index] != ';') {
                struct Node *node = allocateColumnNode(q);

                if (curr_index >= MAX_FIELD_COUNT + 1) {
                    fprintf(stderr, "Too many columns\n");
                    return -1;
                }

                skipWhitespace(query, &index);

                int col_start_index = index;

                int result = parseComplexNode(query, &index, node);

                if (result < 0) {
                    return result;
                }

                // Default alias is whole column spec (if it fits in)
                int len = index - col_start_index;

                // Uncomment if we don't want the alias to be surrounded by the
                // single quotes.
                // if (query[col_start_index] == '\'' &&
                //     query[col_start_index + len - 1] == '\'')
                // {
                //     col_start_index++;
                //     len -= 2;
                // }

                if (len < MAX_FIELD_LENGTH) {
                    whitespaceCollapse(
                        node->alias,
                        query + col_start_index,
                        len
                    );
                }

                q->flags |= result;

                skipWhitespace(query, &index);

                if (strncmp(query + index, "AS ", 3) == 0) {
                    index += 3;

                    getQuotedToken(
                        query,
                        &index,
                        node->alias,
                        MAX_FIELD_LENGTH
                    );

                    skipWhitespace(query, &index);
                }

                if (query[index] != ',') {
                    break;
                }

                index++;
            }
        }
        else if (strcmp(keyword, "FROM") == 0) {
            int next_join_flag = 0;

            while (query[index] != '\0' && query[index] != ';') {
                struct Table *table = allocateTable(q);

                table->join.function = OPERATOR_ALWAYS;
                table->join_type = next_join_flag;
                next_join_flag = 0;

                skipWhitespace(query, &index);

                if (query[index] == '(') {
                    // Subquery time!

                    int len = find_matching_parenthesis(query + index);

                    if (strncmp(query + index + 1, "VALUES", 6) == 0) {
                        // VALUES subqueries can be handled in process
                        // We'll construct a temp DB to hold data until
                        // populateTables().

                        const char *end_ptr = query + index + len;

                        index += strlen("(VALUES");

                        skipWhitespace(query, &index);

                        // Malloc a DB ahead of time. It will be copied to the
                        // stack in populateTables() then populateTables() will
                        // free this for us.
                        struct DB *db = calloc(1, sizeof(*db));

                        csvMem_fromValues(
                            db,
                            query + index,
                            end_ptr - query - index
                        );

                        // This DB needs to be free'd in populateTables()
                        table->db = db;

                        index = end_ptr - query;
                    }
                    else {
                        if (len >= MAX_TABLE_LENGTH) {
                            fprintf(
                                stderr,
                                "Subqueries longer than %d are not supported. "
                                "Subquery was %d bytes.\n",
                                MAX_TABLE_LENGTH,
                                len
                            );
                            return -1;
                        }

                        strncpy(table->name, query + index + 1, len - 2);

                        // Indicate to query processor this is a subquery
                        table->db = DB_SUBQUERY;

                        index += len;

                        strcpy(table->alias, "subquery");

                        // TODO: subqueries can't reference CTEs
                    }
                }
                else {
                    getQuotedToken(
                        query,
                        &index,
                        table->name,
                        MAX_TABLE_LENGTH
                    );

                    if (query[index] == '(') {
                        // we have a table-valued-function

                        int len = find_matching_parenthesis(query + index);
                        int table_len = strlen(table->name);

                        strncpy(table->name + table_len, query + index, len);
                        table->name[table_len + len] = '\0';

                        index += len;
                    }
                    else {
                        // We just have a regular table identifier

                        // As soon as we have a name we should search for a
                        // matching CTE
                        struct Table *cte = findTable(
                            table->name,
                            ctes,
                            MAX_CTES
                        );

                        if (cte != NULL) {
                            // `ctes` array is just a holding area on the stack.
                            // Nothing happens to any of them until they're
                            // actually referenced in the FROM clause.
                            // In which case we copy into the tables area of the
                            // query.
                            memcpy(table, cte, sizeof(*cte));

                            // TODO: CTEs can't reference CTEs
                        }
                    }
                }

                skipWhitespace(query, &index);

                if (strncmp(query + index, "AS ", 3) == 0) {
                    index += 3;

                    getQuotedToken(
                        query,
                        &index,
                        table->alias,
                        MAX_FIELD_LENGTH
                    );

                    skipWhitespace(query, &index);

                    if (query[index] == '(') {
                        int start_index = index;

                        while (
                            query[index] != '\0'
                            && query[index] != ';'
                            && query[index] != ')'
                        ) {
                            index++;
                        }

                        index++;

                        char *c = strncpy(
                            table->alias + strlen(table->alias) + 1,
                            query + start_index,
                            index - start_index
                        );

                        c[index - start_index] = '\0';
                    }
                }
                // Alias might already have been set e.g. if coming from a CTE
                else if (table->alias[0] == '\0') {
                    strcpy(table->alias, table->name);
                }

                skipWhitespace(query, &index);

                // Parse JOIN predicate
                if (strncmp(query + index, "ON ", 3) == 0) {
                    index += 3;
                    skipWhitespace(query, &index);

                    struct Node *p = &table->join;

                    parseNodeList(query, &index, p);

                    skipWhitespace(query, &index);
                } else if (strncmp(query + index, "USING ", 6) == 0) {
                    index += 6;
                    skipWhitespace(query, &index);

                    struct Node * p = &table->join;

                    p->children = malloc(sizeof(*p) * 2);
                    p->child_count = 2;

                    struct Node *left = &p->children[0];
                    struct Node *right = &p->children[1];

                    int result = parseNode(query, &index, left);
                    if (result < 0) {
                        fprintf(stderr, "Unable to parse USING node\n");
                        return result;
                    }

                    // Duplicate node from left to right side of predicate
                    copyNodeTree(right, left);

                    // One side (right) needs to be on this joined table
                    // The other side needs to be from any of the previous
                    // tables we don't which yet, but it will be filled in later
                    if (right->child_count == -1) {
                        right->field.table_id = q->table_count - 1;
                    }
                    else if (right->child_count > 0) {
                        right->children[0].field.table_id = q->table_count - 1;
                    }

                    // Set operator
                    p->function = OPERATOR_EQ;
                } else {
                    table->join.function = OPERATOR_ALWAYS;
                }

                if (query[index] == ',') {
                    index++;

                    // loop again
                }
                else {
                    int old_index = index;
                    getToken(query, &index, keyword, MAX_FIELD_LENGTH);

                    if (strcmp(keyword, "INNER") == 0) {
                        // Carry on
                        getToken(query, &index, keyword, MAX_FIELD_LENGTH);
                    }
                    else if (strcmp(keyword, "LEFT") == 0) {
                        // Mark join type and carry on
                        next_join_flag = JOIN_LEFT;
                        getToken(query, &index, keyword, MAX_FIELD_LENGTH);
                    }

                    if (strcmp(keyword, "JOIN") == 0) {
                        // loop again
                    }
                    else {
                        // Rewind then break out of FROM clause
                        index = old_index;
                        break;
                    }
                }
            }
        }
        else if (strcmp(keyword, "WHERE") == 0) {
            q->flags |= FLAG_HAVE_PREDICATE;

            while (query[index] != '\0' && query[index] != ';') {
                struct Node *p = allocatePredicateNode(q);

                int result = parseComplexNode(query, &index, p);
                if (result < 0) {
                    return result;
                }

                if ((p->function & MASK_FUNC_FAMILY) != FUNC_FAM_OPERATOR) {
                    fprintf(stderr, "WHERE node: expected =|<|<=|>|>=\n");
                    return -1;
                }

                skipWhitespace(query, &index);

                if (strncmp(query + index, "AND ", 4) == 0) {
                    index += 4;
                } else {
                    break;
                }
            }
        }
        else if (strcmp(keyword, "OFFSET") == 0) {
            q->offset_value = getNumericToken(query, &index);

            if (q->offset_value < 0) {
                fprintf(stderr, "OFFSET cannot be negative\n");
                return -1;
            }

            getToken(query, &index, keyword, MAX_FIELD_LENGTH);

            if (strcmp(keyword, "ROW") != 0 && strcmp(keyword, "ROWS") != 0) {
                fprintf(stderr, "expected ROW|ROWS; Got '%s'\n", keyword);
                return -1;
            }
        }
        else if (strcmp(keyword, "FETCH") == 0) {
            getToken(query, &index, keyword, MAX_FIELD_LENGTH);

            if (strcmp(keyword, "FIRST") != 0 && strcmp(keyword, "NEXT") != 0) {
                fprintf(stderr, "expected FIRST|NEXT\n");
                return -1;
            }

            skipWhitespace(query, &index);

            if (isdigit(query[index])) {

                q->limit_value = getNumericToken(query, &index);

                if (q->limit_value < 0) {
                    fprintf(stderr, "FETCH FIRST cannot be negative\n");
                    return -1;
                }
            } else {
                q->limit_value = 1;
            }

            getToken(query, &index, keyword, MAX_FIELD_LENGTH);

            if (strcmp(keyword, "ROW") != 0 && strcmp(keyword, "ROWS") != 0) {
                fprintf(stderr, "expected ROW|ROWS; Got '%s'\n", keyword);
                return -1;
            }

            getToken(query, &index, keyword, MAX_FIELD_LENGTH);

            if (strcmp(keyword, "ONLY") != 0) {
                fprintf(stderr, "expected ONLY; Got '%s'\n", keyword);
                return -1;
            }
        }
        else if (strcmp(keyword, "LIMIT") == 0) {
            q->limit_value = getNumericToken(query, &index);

            if (q->limit_value < 0) {
                fprintf(stderr, "LIMIT cannot be negative\n");
                return -1;
            }
        }
        else if (strcmp(keyword, "ORDER") == 0) {
            getToken(query, &index, keyword, MAX_FIELD_LENGTH);

            if (strcmp(keyword, "BY") != 0) {
                fprintf(stderr, "expected BY\n");
                return -1;
            }

            while (query[index] != '\0' && query[index] != ';') {
                int i = q->order_count++;

                skipWhitespace(query, &index);

                int result = parseNode(query, &index, &q->order_nodes[i]);
                if (result < 0) {
                    return -1;
                }

                if (
                    strcmp(q->order_nodes[0].field.text, "PK") == 0
                    && query[index] == '('
                ) {
                    // We've been asked to sort on primary key.
                    // We don't actually care which column it is so we just
                    // discard the contents of the parentheses.
                    int len = find_matching_parenthesis(query + index);
                    index += len;
                }

                size_t original_index = index;

                getToken(query, &index, keyword, MAX_FIELD_LENGTH);

                if (strcmp(keyword, "ASC") == 0) {
                    q->order_nodes[i].alias[0] = ORDER_ASC;
                } else if (strcmp(keyword, "DESC") == 0) {
                    q->order_nodes[i].alias[0] = ORDER_DESC;
                } else {
                    q->order_nodes[i].alias[0] = ORDER_ASC;
                    // backtrack
                    index = original_index;
                }

                skipWhitespace(query, &index);

                if (query[index] != ',') {
                    break;
                }

                index++;
            }

        }
        else if (strcmp(keyword, "GROUP") == 0) {
            getToken(query, &index, keyword, MAX_FIELD_LENGTH);

            if (strcmp(keyword, "BY") != 0) {
                fprintf(stderr, "expected BY\n");
                return -1;
            }

            q->flags |= FLAG_GROUP;

            while (query[index] != '\0' && query[index] != ';') {
                int i = q->group_count++;

                skipWhitespace(query, &index);

                int result = parseNode(query, &index, &q->group_nodes[i]);
                if (result < 0) {
                    return -1;
                }

                skipWhitespace(query, &index);

                if (query[index] != ',') {
                    break;
                }

                index++;
            }

        }
        else {
            fprintf(
                stderr,
                "expected WITH|SELECT|FROM|WHERE|OFFSET|FETCH FIRST|LIMIT|ORDER"
                "|GROUP\n"
            );
            fprintf(stderr, "Found '%s'\n", keyword);
            return -1;
        }
    }

    // Consume trailing semicolon
    if (query[index] == ';') {
        index++;
    }

    if (end_ptr != NULL) {
        *end_ptr = &query[index];
    }

    return 0;
}

static struct Table *findTable (
    const char *table_name,
    struct Table *tables,
    int table_count
) {
    for (int i = 0; i < table_count; i++) {
        struct Table *table = tables + i;

        if (strcmp(table->alias, table_name) == 0) {
            return table;
        }

        if (strcmp(table->name, table_name) == 0) {
            return table;
        }
    }

    return NULL;
}