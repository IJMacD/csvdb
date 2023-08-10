#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "../structs.h"
#include "token.h"
#include "../db/csv-mem.h"
#include "../functions/util.h"
#include "../evaluate/predicates.h"

#define MAX_CTES    10

static int parseNode (
    const char * query,
    size_t * index,
    struct Node *node
);

static int parseFunctionParams (
    const char * query,
    size_t * index,
    struct Node *node
);

static int checkConstantField (struct Field *field);

static int checkSimpleOperators (
    const char *query,
    size_t *index,
    struct Node *node
);

static struct Table *findTable (
    const char *table_name,
    struct Table *tables,
    int table_count
);

static struct Node *addChildNode (struct Node *node);

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
                struct Node *node = &(q->columns[curr_index++]);

                if (curr_index >= MAX_FIELD_COUNT + 1) {
                    fprintf(stderr, "Too many columns\n");
                    return -1;
                }

                skipWhitespace(query, &index);

                int col_start_index = index;

                int result = parseNode(query, &index, node);

                if (result < 0) {
                    return result;
                }

                // Default alias is whole column spec (if it fits in)
                int len = index - col_start_index;
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

            q->column_count = curr_index;
        }
        else if (strcmp(keyword, "FROM") == 0) {
            int next_join_flag = 0;

            while (query[index] != '\0' && query[index] != ';') {
                q->table_count++;

                if (q->table_count == 1) {
                    q->tables = calloc(q->table_count, sizeof (struct Table));
                } else {
                    void * ptr = realloc(
                        q->tables,
                        sizeof (struct Table) * q->table_count
                    );

                    if (ptr == NULL) {
                        fprintf(stderr, "Can't allocate memory\n");
                        return -1;
                    }

                    q->tables = ptr;

                    // Zero out realloc'd space
                    memset(
                        q->tables + (q->table_count - 1),
                        0,
                        sizeof (struct Table)
                    );
                }

                struct Table *table = &q->tables[q->table_count - 1];

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

                        index += 1 + 6; // '(' + 'VALUES'

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

                    p->children = malloc(sizeof(*p) * 2);
                    p->child_count = 2;

                    struct Node *left = &p->children[0];
                    struct Node *right = &p->children[1];

                    int result = parseNode(query, &index, left);

                    if (result < 0) {
                        return result;
                    }

                    char op[5];
                    getOperatorToken(query, &index, op, 5);

                    p->function = parseOperator(op);
                    if (p->function == FUNC_UNKNOWN) {
                        fprintf(stderr, "expected =|!=|<|<=|>|>=\n");
                        return -1;
                    }

                    // Check for IS NOT
                    if (strcmp(op, "IS") == 0) {
                        skipWhitespace(query, &index);
                        if (strncmp(query + index, "NOT ", 4) == 0) {
                            p->function = OPERATOR_NE;
                            index += 4;
                        }
                    }

                    result = parseNode(query, &index, right);
                    if (result < 0) {
                        return result;
                    }

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

                    // node from left to right side of predicate
                    memcpy(right, left, sizeof (*left));

                    // One side (right) needs to be on this joined table
                    // The other side needs to be from any of the previous
                    // tables we don't which yet, but it will be filled in later
                    right->field.table_id = q->table_count - 1;

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
                void *mem;

                if (q->predicate_count == 0) {
                    mem = malloc(sizeof (*q->predicate_nodes));
                } else {
                    mem = realloc(
                        q->predicate_nodes,
                        sizeof(*q->predicate_nodes) * (q->predicate_count + 1)
                    );
                }

                if (mem == NULL) {
                    fprintf(stderr, "Out of memory\n");
                    return -1;
                }

                q->predicate_nodes = mem;

                struct Node *p = &(q->predicate_nodes[q->predicate_count++]);

                p->children = malloc(sizeof(*p) * 2);
                p->child_count = 2;

                struct Node *left = &p->children[0];
                struct Node *right = &p->children[1];

                int result = parseNode(query, &index, left);
                if (result < 0) {
                    return result;
                }

                // printf("Predicate field: %s\n", predicate_field);

                char op[5];
                getOperatorToken(query, &index, op, 5);

                p->function = parseOperator(op);
                if (p->function == FUNC_UNKNOWN) {
                    fprintf(stderr, "expected =|!=|<|<=|>|>=\n");
                    return -1;
                }

                // Check for IS NOT
                if (strcmp(op, "IS") == 0) {
                    skipWhitespace(query, &index);
                    if (strncmp(query + index, "NOT ", 4) == 0) {
                        p->function = OPERATOR_NE;
                        index += 4;
                    }
                }

                result = parseNode(query, &index, right);
                if (result < 0) {
                    return result;
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

static int parseNode (
    const char * query,
    size_t * index,
    struct Node *node
) {
    char value[MAX_FIELD_LENGTH];
    int flags = 0;

    // Fill in defaults
    node->child_count = 0;
    node->function = FUNC_UNITY;
    node->field.index = FIELD_UNKNOWN;
    node->field.table_id = -1;
    node->field.text[0] = '\0';

    if (query[*index] == '*') {
        node->field.index = FIELD_STAR;

        // '*' will default to ALL tables
        node->field.table_id = -1;

        (*index)++;

        return flags;
    }

    int quoted_flag = getQuotedToken(query, index, value, MAX_FIELD_LENGTH);

    if (quoted_flag == 2) {
        // Field is explicit, it can't be a function or special column name
        strcpy(node->field.text, value);

        checkSimpleOperators(query, index, node);

        // Whether we found a simple operator or not, we're done here

        return flags;
    }

    if (strcmp(value, "ROW_NUMBER") == 0) {
        if (query[*index] != '(' && query[(*index)+1] != ')') {
            fprintf(stderr, "Expected () after ROW_NUMBER\n");
            return -1;
        }
        (*index) += 2;

        node->field.index = FIELD_ROW_NUMBER;

        return flags;
    }

    if (strcmp(value, "rowid") == 0) {
        node->field.index = FIELD_ROW_INDEX;

        // default to first table
        node->field.table_id = 0;

        return flags;
    }

    if (query[*index] == '(') {
        (*index)++;
        // We have a function

        if (strcmp(value, "EXTRACT") == 0) {
            char part[32];
            getToken(query, index, part, 32);

            // EXTRACT functions only take one parameter.
            // We'll use the self-node optimisation
            node->child_count = -1;

            if (strcmp(part, "YEAR") == 0) {
                node->function = FUNC_EXTRACT_YEAR;
            }
            else if (strcmp(part, "MONTH") == 0) {
                node->function = FUNC_EXTRACT_MONTH;
            }
            else if (strcmp(part, "DAY") == 0) {
                node->function = FUNC_EXTRACT_DAY;
            }
            else if (strcmp(part, "WEEK") == 0) {
                node->function = FUNC_EXTRACT_WEEK;
            }
            else if (strcmp(part, "WEEKYEAR") == 0) {
                node->function = FUNC_EXTRACT_WEEKYEAR;
            }
            else if (strcmp(part, "WEEKDAY") == 0) {
                node->function = FUNC_EXTRACT_WEEKDAY;
            }
            else if (strcmp(part, "HEYEAR") == 0) {
                node->function = FUNC_EXTRACT_HEYEAR;
            }
            else if (strcmp(part, "YEARDAY") == 0) {
                node->function = FUNC_EXTRACT_YEARDAY;
            }
            else if (strcmp(part, "MILLENNIUM") == 0) {
                node->function = FUNC_EXTRACT_MILLENNIUM;
            }
            else if (strcmp(part, "CENTURY") == 0) {
                node->function = FUNC_EXTRACT_CENTURY;
            }
            else if (strcmp(part, "DECADE") == 0) {
                node->function = FUNC_EXTRACT_DECADE;
            }
            else if (strcmp(part, "QUARTER") == 0) {
                node->function = FUNC_EXTRACT_QUARTER;
            }
            else if (strcmp(part, "DATE") == 0) {
                node->function = FUNC_EXTRACT_DATE;
            }
            else if (strcmp(part, "DATETIME") == 0) {
                node->function = FUNC_EXTRACT_DATETIME;
            }
            else if (strcmp(part, "JULIAN") == 0) {
                node->function = FUNC_EXTRACT_JULIAN;
            }
            else if (strcmp(part, "MONTH_STRING") == 0) {
                node->function = FUNC_EXTRACT_MONTH_STRING;
            }
            else if (strcmp(part, "WEEK_STRING") == 0) {
                node->function = FUNC_EXTRACT_WEEK_STRING;
            }
            else if (strcmp(part, "YEARDAY_STRING") == 0) {
                node->function = FUNC_EXTRACT_YEARDAY_STRING;
            }
            else {
                fprintf(stderr, "expected valid extract part - got %s\n", part);
                return -1;
            }

            skipWhitespace(query, index);

            char keyword[10];
            getToken(query, index, keyword, 10);

            if (strcmp(keyword, "FROM") != 0) {
                fprintf(stderr, "expected FROM\n");
                return -1;
            }

            skipWhitespace(query, index);

            getQuotedToken(query, index, node->field.text, MAX_FIELD_LENGTH);

            skipWhitespace(query, index);

            if (query[*index] != ')') {
                fprintf(stderr, "expected ')' got '%c'\n", query[*index]);
                return -1;
            }

            (*index)++;

            if (checkConstantField((struct Field *)node) < 0) {
                return -1;
            }

            return flags;
        }

        parseFunctionParams(query, index, node);

        if (strcmp(value, "PK") == 0) {
            node->function = FUNC_PK;
        }
        else if (strcmp(value, "UNIQUE") == 0) {
            node->function = FUNC_UNIQUE;
        }
        else if (strcmp(value, "INDEX") == 0) {
            node->function = FUNC_INDEX;
        }
        else if (strcmp(value, "CHR") == 0) {
            node->function = FUNC_CHR;
        }
        else if (strcmp(value, "RANDOM") == 0) {
            node->function = FUNC_RANDOM;
            node->field.index = FIELD_CONSTANT;
            node->field.table_id = -1;
        }
        else if (strcmp(value, "ADD") == 0) {
            node->function = FUNC_ADD;
        }
        else if (strcmp(value, "SUB") == 0) {
            node->function = FUNC_SUB;
        }
        else if (strcmp(value, "MUL") == 0) {
            node->function = FUNC_MUL;
        }
        else if (strcmp(value, "DIV") == 0) {
            node->function = FUNC_DIV;
        }
        else if (strcmp(value, "MOD") == 0) {
            node->function = FUNC_MOD;
        }
        else if (strcmp(value, "POW") == 0) {
            node->function = FUNC_POW;
        }
        else if (strcmp(value, "TO_HEX") == 0) {
            node->function = FUNC_TO_HEX;
        }
        else if (strcmp(value, "LENGTH") == 0) {
            node->function = FUNC_LENGTH;
        }
        else if (strcmp(value, "LEFT") == 0) {
            // LEFT(<field>, <count>)
            node->function = FUNC_LEFT;
        }
        else if (strcmp(value, "RIGHT") == 0) {
            // RIGHT(<field>, <count>)
            node->function = FUNC_RIGHT;
        }
        else if (strcmp(value, "DATE_ADD") == 0) {
            node->function = FUNC_DATE_ADD;
        }
        else if (strcmp(value, "DATE_SUB") == 0) {
            node->function = FUNC_DATE_SUB;
        }
        else if (strcmp(value, "DATE_DIFF") == 0) {
            node->function = FUNC_DATE_DIFF;
        }
        else if (strcmp(value, "COUNT") == 0) {
            node->function = FUNC_AGG_COUNT;
            flags |= FLAG_GROUP;

            if (strcmp(node->field.text, "*") == 0) {
                node->function = FUNC_UNITY;
                node->field.index = FIELD_COUNT_STAR;
                node->field.table_id = -1;
            }
        }
        else if (strcmp(value, "MAX") == 0) {
            node->function = FUNC_AGG_MAX;
            flags |= FLAG_GROUP;
        }
        else if (strcmp(value, "MIN") == 0) {
            node->function = FUNC_AGG_MIN;
            flags |= FLAG_GROUP;
        }
        else if (strcmp(value, "SUM") == 0) {
            node->function = FUNC_AGG_SUM;
            flags |= FLAG_GROUP;
        }
        else if (strcmp(value, "AVG") == 0) {
            node->function = FUNC_AGG_AVG;
            flags |= FLAG_GROUP;
        }
        else if (strcmp(value, "LISTAGG") == 0) {
            node->function = FUNC_AGG_LISTAGG;
            flags |= FLAG_GROUP;
        }

        return flags;
    }

    // Just a regular bare field
    strcpy(node->field.text, value);

    // The bare field could be number literal, string literal or named
    // constant.
    if (checkConstantField((struct Field *)node) < 0) {
        return -1;
    }

    checkSimpleOperators(query, index, node);

    return flags;
}

/**
 * @brief Helper function to read function params from input stream
 *
 * @param query Whole query string
 * @param index Pointer to index value
 * @param node Pointer to column struct
 * @return int
 */
static int parseFunctionParams (
    const char * query,
    size_t * index,
    struct Node *node
) {
    // getQuotedToken won't get '*' so we'll check manually
    if (query[*index] == '*') {
        node->field.text[0] = '*';
        node->field.index = FIELD_STAR;

        (*index)++;
    }
    else {
        // Default to self-child node
        node->child_count = -1;

        getQuotedToken(query, index, node->field.text, MAX_FIELD_LENGTH);

        if (checkConstantField((struct Field *)node) < 0) {
            return -1;
        }

        skipWhitespace(query, index);

        while (query[*index] == ',') {
            // We need to have multiple child nodes
            struct Node *child_node = addChildNode(node);

            // Parse second child node

            (*index)++;

            skipWhitespace(query, index);

            getQuotedToken(
                query,
                index,
                child_node->field.text,
                sizeof(child_node->field.text)
            );

            if (checkConstantField(&child_node->field) < 0) {
                return -1;
            }

            skipWhitespace(query, index);
        }
    }

    if (query[*index] != ')') {
        fprintf(stderr, "expected ')' got '%c'\n", query[*index]);
        return -1;
    }

    (*index)++;

    return 0;
}

/**
 * Detects whether the value in field->text is recognised as a constant or not.
 * If it detects a constant then it sets the field->index to FIELD_CONSTANT.
 * If it detects a 'string literal' it will remove the single quotes as well.
 * Returns:
 *   0 if ok (constant or not);
 *  -1 if an error occurred (e.g. unterminated string literal)
 */
static int checkConstantField (struct Field *field) {

    if (is_numeric(field->text)) {
        // Detected numeric constant
        field->index = FIELD_CONSTANT;
        field->table_id = -1;
    }
    else if (field->text[0] == '0' && field->text[1] == 'x') {
        // Detected hex numeric constant
        field->index = FIELD_CONSTANT;
        field->table_id = -1;
    }
    else if (field->text[0] == '\'') {
        // Detected string literal
        field->index = FIELD_CONSTANT;
        field->table_id = -1;

        int len = strlen(field->text);

        if (field->text[len - 1] != '\'') {
            fprintf(
                stderr,
                "expected apostrophe got '%c'\n",
                field->text[len - 1]
            );
            return -1;
        }

        char value[MAX_FIELD_LENGTH];
        strncpy(value, field->text + 1, len - 2);
        value[len - 2] = '\0';

        strcpy(field->text, value);
    } else if (strcmp(field->text, "CURRENT_DATE") == 0
        || strcmp(field->text, "TODAY") == 0)
    {
        field->index = FIELD_CONSTANT;
        field->table_id = -1;

        // Evaluated later
    }
    else {
        field->index = FIELD_UNKNOWN;
        field->table_id = -1;
    }

    return 0;
}

static int checkSimpleOperators(
    const char *query,
    size_t *index,
    struct Node *node
) {

    while (query[*index] != '\0' && query[*index] != ';') {
        enum Function function = FUNC_UNITY;

        skipWhitespace(query, index);

        if (query[*index] == '|' && query[*index+1] == '|') {
            function = FUNC_CONCAT;

            // Will get one more increment below
            (*index)++;
        }
        else {
            switch (query[*index])
            {
            case '+':
                function = FUNC_ADD;
                break;
            case '-':
                function = FUNC_SUB;
                break;
            case '*':
                function = FUNC_MUL;
                break;
            case '/':
                function = FUNC_DIV;
                break;
            case '%':
                function = FUNC_MOD;
                break;

            default:
                break;
            }
        }

        if (function == FUNC_UNITY) {
            // We didn't find another operator
            break;
        }

        // If this is the first operator, we set the node
        if (node->function == FUNC_UNITY) {
            node->function = function;
        }
        else if (node->function != function) {
            fprintf(stderr, "Complex expressions are not supported.\n");
            return -1;
        }

        (*index)++;

        skipWhitespace(query, index);

        struct Node *next_child = addChildNode(node);

        getQuotedToken(query, index, next_child->field.text, MAX_FIELD_LENGTH);

        checkConstantField(&next_child->field);
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

/**
 * mallocs a new node and adds it as a child of the provided node.
 * Also returns the new child for convenience.
 * If there aren't any children yet then the node is turned into a new parent.
 */
static struct Node *addChildNode (struct Node *node) {
    struct Node *child_node;

    if (node->children == NULL) {
        node->child_count = 2;

        node->children = calloc(node->child_count, sizeof(*node));

        // copy existing field to new child
        struct Node *first_child = &node->children[0];
        memcpy(&first_child->field, &node->field, sizeof(node->field));

        // Clear current field
        node->field.text[0] = '\0';

        // Clear field index
        node->field.index = FIELD_UNKNOWN;

        // Return second child
        child_node = &node->children[1];
    }
    else if (node->child_count == -1) {
        fprintf(
            stderr,
            "You found a bug. node->child_count out of sync with "
            "node->children.\n"
        );
        exit(-1);
    }
    else {
        node->child_count++;

        node->children = realloc(
            node->children,
            sizeof(*node) * node->child_count
        );

        if (node->children == NULL) {
            fprintf(stderr, "Unable to allocate memory.\n");
            exit(-1);
        }

        child_node = &node->children[node->child_count - 1];

        // NULL out children to avoid uninitialized read and set defaults
        child_node->children = NULL;
        child_node->child_count = 0;
        child_node->field.index = FIELD_UNKNOWN;
        child_node->field.table_id = -1;
        child_node->field.text[0] = '\0';
        child_node->function = FUNC_UNITY;
    }

    return child_node;
}