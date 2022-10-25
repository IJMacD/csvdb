#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "token.h"
#include "predicates.h"
#include "function.h"
#include "sort.h"
#include "date.h"
#include "util.h"

#define MAX_CTES    10

static int parseColumn (const char * query, size_t * index, struct ColumnNode *column);

static int parseFunction (const char * query, size_t * index, struct ColumnNode * column, int name_length);

static int checkConstantColumn(struct ColumnNode * column);

static struct Table *findTable (const char *table_name, struct Table *tables, int table_count);

int parseQuery (struct Query *q, const char *query) {
    /*********************
     * Begin Query parsing
     *********************/

    size_t index = 0;

    size_t query_length = strlen(query);

    // printf("Query length: %ld\n", query_length);

    // Allow SELECT to be optional and default to SELECT *
    q->columns[0].field = FIELD_STAR;
    q->columns[0].table_id = -1;
    q->column_count = 1;

    q->predicate_count = 0;

    q->order_count = 0;

    q->flags = 0;
    q->offset_value = 0;
    q->limit_value = -1;

    struct Table ctes[MAX_CTES];
    int cte_count = 0;

    skipWhitespace(query, &index);

    if (strncmp(query + index, "EXPLAIN", 7) == 0 && isspace(query[index + 7])) {
        q->flags |= FLAG_EXPLAIN;
        index += 8;
    }

    skipWhitespace(query, &index);

    // Special treatment for VALUES only (top-level) query
    // Will behave as a query: SELECT * FROM values_mem
    // Where values_mem is a simulated DB in memory
    if (strncmp(query + index, "VALUES", 6) == 0 && isspace(query[index + 6])) {
        index += 7;

        q->tables = calloc(1, sizeof(q->tables[0]));
        q->table_count = 1;

        struct Table *table = &q->tables[0];

        skipWhitespace(query, &index);

        strncpy(table->name, query + index, MAX_TABLE_LENGTH - 1);

        if (table->name[MAX_TABLE_LENGTH - 2] != '\0') {
            fprintf(stderr, "VALUES query was too long (limited to %d characters)\n", MAX_TABLE_LENGTH - 1);
            exit(-1);
        }

        table->db = DB_VALUES;

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
            fprintf(stderr, "error: expected a table name\n");
            exit(-1);
        }

        skipWhitespace(query, &index);

        if (query[index] != '\0') {
            fprintf(stderr, "error: expected end of TABLE query found '%s'\n", query + index);
            exit(-1);
        }

        char buffer[1024];
        sprintf(buffer, "FROM \"%s\"", name);

        return parseQuery(q, buffer);
    }

    char keyword[MAX_FIELD_LENGTH] = {0};

    while (index < query_length) {

        int token_length = getToken(query, &index, keyword, MAX_FIELD_LENGTH);

        if (token_length <= 0) {
            break;
        }

        // printf("Token: '%s'\n", keyword);

        if (strcmp(keyword, "WITH") == 0) {
            while (index < query_length) {
                int cte_index = cte_count++;

                if (cte_index >= MAX_CTES) {
                    fprintf(stderr, "Error: Cannot have more than %d ctes.\n", MAX_CTES);
                }

                struct Table *cte = ctes + cte_index;

                getQuotedToken(query, &index, cte->alias, MAX_TABLE_LENGTH);

                getToken(query, &index, keyword, MAX_FIELD_LENGTH);

                if (strcmp(keyword, "AS") != 0) {
                    fprintf(stderr, "Error: expected AS\n");
                    return -1;
                }

                skipWhitespace(query, &index);

                if (query[index] != '(') {
                    fprintf(stderr, "Error: Expected '('\n");
                    return -1;
                }

                int len = find_matching_parenthesis(query + index);

                if (len >= MAX_TABLE_LENGTH) {
                    fprintf(stderr, "Error: CTEs longer than %d are not supported. CTE was %d bytes.\n", MAX_TABLE_LENGTH, len);
                    exit(-1);
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
            while (index < query_length) {
                struct ColumnNode *column = &(q->columns[curr_index++]);

                if (curr_index >= MAX_FIELD_COUNT + 1) {
                    fprintf(stderr, "Too many columns\n");
                    return -1;
                }

                int result = parseColumn(query, &index, column);

                if (result < 0) {
                    return result;
                }

                q->flags |= result;

                skipWhitespace(query, &index);

                if (strncmp(query + index, "AS ", 3) == 0) {
                    index += 3;

                    getQuotedToken(query, &index, column->alias, MAX_FIELD_LENGTH);
                }

                skipWhitespace(query, &index);

                if (query[index] == '|' && query[index+1] == '|') {
                    column->concat = 1;
                    index++; // (will get another increment below)
                    skipWhitespace(query, &index);
                }
                else if (query[index] != ',') {
                    break;
                }

                index++;
            }

            q->column_count = curr_index;
        }
        else if (strcmp(keyword, "FROM") == 0) {
            int next_join_flag = 0;

            while (index < query_length) {
                q->table_count++;

                if (q->table_count == 1) {
                    q->tables = calloc(q->table_count, sizeof (struct Table));
                } else {
                    void * ptr = realloc(q->tables, sizeof (struct Table) * q->table_count);

                    if (ptr == NULL) {
                        fprintf(stderr, "Error: Can't allocate memory\n");
                        exit(-1);
                    }

                    q->tables = ptr;

                    // Zero out realloc'd space
                    memset(q->tables + (q->table_count - 1), 0, sizeof (struct Table));
                }

                struct Table *table = &q->tables[q->table_count - 1];

                table->join_type = next_join_flag;
                next_join_flag = 0;

                skipWhitespace(query, &index);

                if (query[index] == '(') {
                    // Subquery time!

                    int len = find_matching_parenthesis(query + index);

                    if (len >= MAX_TABLE_LENGTH) {
                        fprintf(stderr, "Error: Subqueries longer than %d are not supported. Subquery was %d bytes.\n", MAX_TABLE_LENGTH, len);
                        exit(-1);
                    }

                    strncpy(table->name, query + index + 1, len - 2);

                    // Indicate to query processor this is a subquery
                    table->db = DB_SUBQUERY;

                    index += len;

                    strcpy(table->alias, "subquery");

                    // TODO: subqueries can't reference CTEs
                }
                else {
                    getQuotedToken(query, &index, table->name, MAX_TABLE_LENGTH);

                    // As soon as we have a name we should search for a matching CTE
                    struct Table *cte = findTable(table->name, ctes, MAX_CTES);
                    if (cte != NULL) {
                        memcpy(table, cte, sizeof(*cte));

                        // TODO: CTEs can't reference CTEs
                    }
                }

                skipWhitespace(query, &index);

                if (strncmp(query + index, "AS ", 3) == 0) {
                    index += 3;

                    getQuotedToken(query, &index, table->alias, MAX_FIELD_LENGTH);

                    skipWhitespace(query, &index);

                    if (query[index] == '(') {
                        int start_index = index;

                        while (query[index] != '\0' && query[index] != ')') {
                            index++;
                        }

                        index++;

                        char * c = strncpy(table->alias + strlen(table->alias) + 1, query + start_index, index - start_index);

                        c[index - start_index] = '\0';
                    }
                } else if (table->alias[0] == '\0') {
                    strcpy(table->alias, table->name);
                }

                skipWhitespace(query, &index);

                // Parse JOIN predicate
                if (strncmp(query + index, "ON ", 3) == 0) {
                    index += 3;
                    skipWhitespace(query, &index);

                    struct Predicate * p = &table->join;

                    parseColumn(query, &index, &p->left);

                    char op[5];
                    getToken(query, &index, op, 5);

                    p->op = parseOperator(op);
                    if (p->op == OPERATOR_UN) {
                        fprintf(stderr, "Error: expected =|!=|<|<=|>|>=\n");
                        return -1;
                    }

                    // Check for IS NOT
                    if (strcmp(op, "IS") == 0) {
                        skipWhitespace(query, &index);
                        if (strncmp(query + index, "NOT ", 4) == 0) {
                            p->op = OPERATOR_NE;
                            index += 4;
                        }
                    }

                    parseColumn(query, &index, &p->right);

                    skipWhitespace(query, &index);
                } else if (strncmp(query + index, "USING ", 6) == 0) {
                    index += 6;
                    skipWhitespace(query, &index);

                    struct Predicate * p = &table->join;

                    // parse column (could have function and field name)
                    parseColumn(query, &index, &p->left);

                    // copy function and field name to right side of predicate
                    memcpy(&p->right, &p->left, sizeof (p->left));

                    // One side (right) needs to be on this joined table
                    // The other side needs to be from any of the previous tables
                    // we don't which yet, but it will be filled in later
                    p->right.table_id = q->table_count - 1;

                    // Set operator
                    p->op = OPERATOR_EQ;
                } else {
                    table->join.op = OPERATOR_ALWAYS;
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

            while (index < query_length) {
                void *mem;

                if (q->predicate_count == 0) {
                    mem = malloc(sizeof (*q->predicates));
                } else {
                    mem = realloc(q->predicates, sizeof (*q->predicates) * (q->predicate_count + 1));
                }

                if (mem == NULL) {
                    fprintf(stderr, "Error: Out of memory\n");
                    return -1;
                }

                q->predicates = mem;

                struct Predicate *p = &(q->predicates[q->predicate_count++]);

                int result = parseColumn(query, &index, &p->left);
                if (result < 0) {
                    return result;
                }

                // printf("Predicate field: %s\n", predicate_field);

                char op[5];
                getToken(query, &index, op, 5);

                p->op = parseOperator(op);
                if (p->op == OPERATOR_UN) {
                    fprintf(stderr, "Error: expected =|!=|<|<=|>|>=\n");
                    return -1;
                }

                // Check for IS NOT
                if (strcmp(op, "IS") == 0) {
                    skipWhitespace(query, &index);
                    if (strncmp(query + index, "NOT ", 4) == 0) {
                        p->op = OPERATOR_NE;
                        index += 4;
                    }
                }

                result = parseColumn(query, &index, &p->right);
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
                fprintf(stderr, "Error: OFFSET cannot be negative\n");
                return -1;
            }

            getToken(query, &index, keyword, MAX_FIELD_LENGTH);

            if (strcmp(keyword, "ROW") != 0 && strcmp(keyword, "ROWS") != 0) {
                fprintf(stderr, "Error: expected ROW|ROWS; Got '%s'\n", keyword);
                return -1;
            }
        }
        else if (strcmp(keyword, "FETCH") == 0) {
            getToken(query, &index, keyword, MAX_FIELD_LENGTH);

            if (strcmp(keyword, "FIRST") != 0 && strcmp(keyword, "NEXT") != 0) {
                fprintf(stderr, "Error: expected FIRST|NEXT\n");
                return -1;
            }

            skipWhitespace(query, &index);

            if (isdigit(query[index])) {

                q->limit_value = getNumericToken(query, &index);

                if (q->limit_value < 0) {
                    fprintf(stderr, "Error: FETCH FIRST cannot be negative\n");
                    return -1;
                }
            } else {
                q->limit_value = 1;
            }

            getToken(query, &index, keyword, MAX_FIELD_LENGTH);

            if (strcmp(keyword, "ROW") != 0 && strcmp(keyword, "ROWS") != 0) {
                fprintf(stderr, "Error: expected ROW|ROWS; Got '%s'\n", keyword);
                return -1;
            }

            getToken(query, &index, keyword, MAX_FIELD_LENGTH);

            if (strcmp(keyword, "ONLY") != 0) {
                fprintf(stderr, "Error: expected ONLY; Got '%s'\n", keyword);
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
                fprintf(stderr, "Error: expected BY\n");
                return -1;
            }

            q->flags |= FLAG_ORDER;

            while (index < query_length) {
                int i = q->order_count++;

                getQuotedToken(query, &index, q->order_field[i], MAX_FIELD_LENGTH);

                size_t original_index = index;

                getToken(query, &index, keyword, MAX_FIELD_LENGTH);

                // Note: current implementation only supports all ASC or all DESC
                if (strcmp(keyword, "ASC") == 0) {
                    q->order_direction[i] = ORDER_ASC;
                } else if (strcmp(keyword, "DESC") == 0) {
                    q->order_direction[i] = ORDER_DESC;
                } else {
                    q->order_direction[i] = ORDER_ASC;
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
        else {
            fprintf(stderr, "error: expected SELECT|FROM|WHERE|OFFSET|FETCH FIRST|LIMIT\n");
            fprintf(stderr, "error: Found '%s'\n", keyword);
            return -1;
        }
    }

    return 0;
}

void destroyQuery (struct Query *query) {
    if (query->predicate_count > 0) {
        free(query->predicates);
    }

    if (query->table_count > 0) {
        free(query->tables);
    }
}

static int parseColumn (const char * query, size_t * index, struct ColumnNode *column) {
    int flags = 0;

    column->field = FIELD_UNKNOWN;
    column->function = FUNC_UNITY;

    int quoted_flag = getQuotedToken(query, index, column->text, MAX_FIELD_LENGTH);

    strcpy(column->alias, column->text);

    if (quoted_flag == 2) {
        // Field is explicit
        // Nothing else to do
    }
    else if (checkConstantColumn(column) < 0) {
        return -1;
    }
    else if (strcmp(column->text, "COUNT(*)") == 0) {
        column->field = FIELD_COUNT_STAR;
        flags |= FLAG_GROUP;
    }
    else if (strcmp(column->text, "*") == 0) {
        column->field = FIELD_STAR;

        // '*' will default to ALL tables
        column->table_id = -1;
    }
    else if (strcmp(column->text, "ROW_NUMBER()") == 0) {
        column->field = FIELD_ROW_NUMBER;
    }
    else if (strcmp(column->text, "rowid") == 0) {
        column->field = FIELD_ROW_INDEX;

        // default to first table
        column->table_id = 0;
    }
    else if (strncmp(column->text, "PK(", strlen("PK(")) == 0) {
        column->function = FUNC_PK;

        parseFunction(query, index, column, strlen("PK"));
    }
    else if (strncmp(column->text, "UNIQUE(", strlen("UNIQUE(")) == 0) {
        column->function = FUNC_UNIQUE;

        // If we've been given an explicit index we're going to assume that
        // it's on the first table.
        column->table_id = 0;

        parseFunction(query, index, column, strlen("UNIQUE"));
    }
    else if (strncmp(column->text, "INDEX(", strlen("INDEX(")) == 0) {
        column->function = FUNC_INDEX;

        // If we've been given an explicit index we're going to assume that
        // it's on the first table.
        column->table_id = 0;

        parseFunction(query, index, column, strlen("INDEX"));
    }
    else if (strncmp(column->text, "CHR(", strlen("CHR(")) == 0) {
        column->function = FUNC_CHR;

        parseFunction(query, index, column, strlen("CHR"));
    }
    else if (strcmp(column->text, "RANDOM()") == 0) {
        column->function = FUNC_RANDOM;
        column->field = FIELD_CONSTANT;
        column->table_id = -1;
    }
    else if (strncmp(column->text, "TO_HEX(", strlen("TO_HEX(")) == 0) {
        column->function = FUNC_TO_HEX;

        parseFunction(query, index, column, strlen("TO_HEX"));
    }
    else if (strncmp(column->text, "LENGTH(", strlen("LENGTH(")) == 0) {
        column->function = FUNC_LENGTH;

        parseFunction(query, index, column, strlen("LENGTH"));
    }
    else if (strncmp(column->text, "LEFT(", strlen("LEFT(")) == 0) {
        // LEFT(<field>, <count>)

        column->function = FUNC_LEFT;

        char field_name[MAX_FIELD_LENGTH - 5];
        strcpy(field_name, column->text + 5);
        strcpy(column->text, field_name);

        if (checkConstantColumn(column) < 0) {
            return -1;
        }

        // Store both field name and length in same array
        // Transform to:
        // <field>\0 <count>)

        skipWhitespace(query, index);

        if (query[*index] != ',') {
            fprintf(stderr, "Expected ','; got %c\n", query[*index]);
            exit(-1);
        }

        (*index)++;

        skipWhitespace(query, index);

        int len = strlen(column->text);

        getToken(query, index, column->text + len + 1, MAX_FIELD_LENGTH - len);
    }
    else if (strncmp(column->text, "RIGHT(", strlen("RIGHT(")) == 0) {
        // RIGHT(<field>, <count>)

        column->function = FUNC_RIGHT;

        char field_name[MAX_FIELD_LENGTH - 6];
        strcpy(field_name, column->text + 6);
        strcpy(column->text, field_name);

        if (checkConstantColumn(column) < 0) {
            return -1;
        }

        // Store both field name and length in same array
        // Transform to:
        // <field>\0 <count>)

        skipWhitespace(query, index);

        if (query[*index] != ',') {
            fprintf(stderr, "Expected ','; got %c\n", query[*index]);
            exit(-1);
        }

        (*index)++;

        skipWhitespace(query, index);

        int len = strlen(column->text);

        getToken(query, index, column->text + len + 1, MAX_FIELD_LENGTH - len);

    }
    else if (strncmp(column->text, "EXTRACT(", strlen("EXTRACT(")) == 0) {
        char part[MAX_FIELD_LENGTH - 8];
        strcpy(part, column->text + 8);

        if (strcmp(part, "YEAR") == 0) {
            column->function = FUNC_EXTRACT_YEAR;
        }
        else if (strcmp(part, "MONTH") == 0) {
            column->function = FUNC_EXTRACT_MONTH;
        }
        else if (strcmp(part, "DAY") == 0) {
            column->function = FUNC_EXTRACT_DAY;
        }
        else if (strcmp(part, "WEEK") == 0) {
            column->function = FUNC_EXTRACT_WEEK;
        }
        else if (strcmp(part, "WEEKYEAR") == 0) {
            column->function = FUNC_EXTRACT_WEEKYEAR;
        }
        else if (strcmp(part, "WEEKDAY") == 0) {
            column->function = FUNC_EXTRACT_WEEKDAY;
        }
        else if (strcmp(part, "HEYEAR") == 0) {
            column->function = FUNC_EXTRACT_HEYEAR;
        }
        else if (strcmp(part, "YEARDAY") == 0) {
            column->function = FUNC_EXTRACT_YEARDAY;
        }
        else if (strcmp(part, "MILLENNIUM") == 0) {
            column->function = FUNC_EXTRACT_MILLENNIUM;
        }
        else if (strcmp(part, "CENTURY") == 0) {
            column->function = FUNC_EXTRACT_CENTURY;
        }
        else if (strcmp(part, "DECADE") == 0) {
            column->function = FUNC_EXTRACT_DECADE;
        }
        else if (strcmp(part, "QUARTER") == 0) {
            column->function = FUNC_EXTRACT_QUARTER;
        }
        else if (strcmp(part, "DATE") == 0) {
            column->function = FUNC_EXTRACT_DATE;
        }
        else if (strcmp(part, "DATETIME") == 0) {
            column->function = FUNC_EXTRACT_DATETIME;
        }
        else if (strcmp(part, "JULIAN") == 0) {
            column->function = FUNC_EXTRACT_JULIAN;
        }
        else if (strcmp(part, "MONTH_STRING") == 0) {
            column->function = FUNC_EXTRACT_MONTH_STRING;
        }
        else if (strcmp(part, "WEEK_STRING") == 0) {
            column->function = FUNC_EXTRACT_WEEK_STRING;
        }
        else if (strcmp(part, "YEARDAY_STRING") == 0) {
            column->function = FUNC_EXTRACT_YEARDAY_STRING;
        }
        else {
            fprintf(stderr, "Bad query - expected valid extract part - got %s\n", part);
            return -1;
        }

        skipWhitespace(query, index);

        char value[10];
        getToken(query, index, value, 10);

        if (strcmp(value, "FROM") != 0) {
            fprintf(stderr, "Bad query - expected FROM\n");
            return -1;
        }

        skipWhitespace(query, index);

        getQuotedToken(query, index, column->text, MAX_FIELD_LENGTH);

        size_t len = strlen(column->text);

        if (column->text[len - 1] == ')') {
            column->text[len - 1] = '\0';
        }
        else {
            skipWhitespace(query, index);

            if (query[*index] != ')') {
                fprintf(stderr, "Bad query - expected ')' got '%c'\n", query[*index]);
                return -1;
            }

            (*index)++;
        }

        if (strlen(part) + strlen(column->text) + 15 < MAX_FIELD_LENGTH)
            sprintf(column->alias, "EXTRACT(%s FROM %s)", part, column->text);

        if (checkConstantColumn(column) < 0) {
            return -1;
        }
    }
    else if (strncmp(column->text, "COUNT(", strlen("COUNT(")) == 0) {
        column->function = FUNC_AGG_COUNT;
        flags |= FLAG_GROUP;

        if (parseFunction(query, index, column, strlen("COUNT")) < 0) {
            return -1;
        }

        if (strlen(column->text) + 7 < MAX_FIELD_LENGTH)
            sprintf(column->alias, "COUNT(%s)", column->text);
    }
    else if (strncmp(column->text, "MAX(", strlen("MAX(")) == 0) {
        column->function = FUNC_AGG_MAX;
        flags |= FLAG_GROUP;

        if (parseFunction(query, index, column, strlen("MAX")) < 0) {
            return -1;
        }

        if (strlen(column->text) + 5 < MAX_FIELD_LENGTH)
            sprintf(column->alias, "MAX(%s)", column->text);
    }
    else if (strncmp(column->text, "MIN(", strlen("MIN(")) == 0) {
        column->function = FUNC_AGG_MIN;
        flags |= FLAG_GROUP;

        if (parseFunction(query, index, column, strlen("MIN")) < 0) {
            return -1;
        }

        if (strlen(column->text) + 5 < MAX_FIELD_LENGTH)
            sprintf(column->alias, "MIN(%s)", column->text);
    }
    else if (strncmp(column->text, "SUM(", strlen("SUM(")) == 0) {
        column->function = FUNC_AGG_SUM;
        flags |= FLAG_GROUP;

        if (parseFunction(query, index, column, strlen("SUM")) < 0) {
            return -1;
        }

        if (strlen(column->text) + 5 < MAX_FIELD_LENGTH)
            sprintf(column->alias, "SUM(%s)", column->text);
    }
    else if (strncmp(column->text, "AVG(", strlen("AVG(")) == 0) {
        column->function = FUNC_AGG_AVG;
        flags |= FLAG_GROUP;

        if (parseFunction(query, index, column, strlen("AVG")) < 0) {
            return -1;
        }

        if (strlen(column->text) + 5 < MAX_FIELD_LENGTH)
            sprintf(column->alias, "AVG(%s)", column->text);
    }
    else if (strncmp(column->text, "LISTAGG(", strlen("LISTAGG(")) == 0) {
        column->function = FUNC_AGG_LISTAGG;
        flags |= FLAG_GROUP;

        if (parseFunction(query, index, column, strlen("LISTAGG")) < 0) {
            return -1;
        }

        if (strlen(column->text) + 9 < MAX_FIELD_LENGTH)
            sprintf(column->alias, "LISTAGG(%s)", column->text);
    }

    return flags;
}

/**
 * @brief Helper function to skip function name, then look for and skip closing bracket
 *
 * @param query Whole query string
 * @param index Pointer to index value
 * @param column Pointer to column struct
 * @param name_length Length of function name (excluding opening bracket)
 * @return int
 */
static int parseFunction (const char * query, size_t * index, struct ColumnNode * column, int name_length) {

    char field[MAX_FIELD_LENGTH];
    strcpy(field, column->text + name_length + 1);
    strcpy(column->text, field);

    size_t len = strlen(column->text);

    if (column->text[len - 1] == ')') {
        column->text[len - 1] = '\0';
    }
    else {
        skipWhitespace(query, index);

        if (query[*index] != ')') {
            fprintf(stderr, "Bad query - expected ')' got '%c'\n", query[*index]);
            return -1;
        }

        (*index)++;
    }

    if (checkConstantColumn(column) < 0) {
        return -1;
    }

    return 0;
}

static int checkConstantColumn(struct ColumnNode * column) {

    if (is_numeric(column->text)) {
        // Detected numeric constant
        column->field = FIELD_CONSTANT;
        column->table_id = -1;
    } else if (column->text[0] == '\'') {
        // Detected string literal
        column->field = FIELD_CONSTANT;
        column->table_id = -1;

        int len = strlen(column->text);

        if (column->text[len - 1] != '\'') {
            fprintf(stderr, "Bad query - expected apostrophe got '%c'\n", column->text[len - 1]);
            return -1;
        }

        char value[MAX_FIELD_LENGTH];
        strncpy(value, column->text + 1, len - 2);
        value[len - 2] = '\0';

        strcpy(column->text, value);
    } else if (strcmp(column->text, "CURRENT_DATE") == 0
        || strcmp(column->text, "TODAY()") == 0)
    {
        column->field = FIELD_CONSTANT;
        column->table_id = -1;

        // Evaluated later
    }
    else {
        column->field = FIELD_UNKNOWN;
        column->table_id = -1;
    }

    return 0;
}

static struct Table *findTable (const char *table_name, struct Table *tables, int table_count) {
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