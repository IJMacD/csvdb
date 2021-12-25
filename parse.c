#include <string.h>
#include <ctype.h>

#include "parse.h"
#include "predicates.h"
#include "sort.h"
#include "util.h"

int parseColumn (const char * query, size_t * index, struct ResultColumn *column);

int parseFunction (const char * query, size_t * index, struct ResultColumn * column, int name_length);

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

    q->order_direction = ORDER_ASC;

    q->flags = 0;
    q->offset_value = 0;
    q->limit_value = -1;

    skipWhitespace(query, &index);

    if (strncmp(query + index, "EXPLAIN ", 8) == 0) {
        q->flags |= FLAG_EXPLAIN;
        index += 8;
    }

    char keyword[FIELD_MAX_LENGTH] = {0};

    while (index < query_length) {

        int token_length = getToken(query, &index, keyword, FIELD_MAX_LENGTH);

        if (token_length <= 0) {
            break;
        }

        // printf("Token: '%s'\n", keyword);

        if (strcmp(keyword, "SELECT") == 0) {

            int curr_index = 0;
            while (index < query_length) {
                struct ResultColumn *column = &(q->columns[curr_index++]);

                if (curr_index >= FIELD_MAX_COUNT + 1) {
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

                    getQuotedToken(query, &index, column->alias, FIELD_MAX_LENGTH);
                }

                skipWhitespace(query, &index);

                if (query[index] != ',') {
                    break;
                }

                index++;
            }

            q->column_count = curr_index;
        }
        else if (strcmp(keyword, "FROM") == 0) {
            while (index < query_length) {
                q->table_count++;

                if (q->table_count == 1) {
                    q->tables = malloc(sizeof (struct Table) * q->table_count);
                } else {
                    void * ptr = realloc(q->tables, sizeof (struct Table) * q->table_count);

                    if (ptr == NULL) {
                        fprintf(stderr, "Can't allocate memory\n");
                        exit(-1);
                    }

                    q->tables = ptr;
                }

                getQuotedToken(query, &index, q->tables[q->table_count-1].name, TABLE_MAX_LENGTH);

                skipWhitespace(query, &index);

                if (strncmp(query + index, "AS ", 3) == 0) {
                    index += 3;

                    getQuotedToken(query, &index, q->tables[q->table_count-1].alias, FIELD_MAX_LENGTH);
                }

                skipWhitespace(query, &index);

                if (query[index] != ',') {
                    break;
                }

                index++;
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
                    fprintf(stderr, "Out of memory\n");
                    return -1;
                }

                q->predicates = mem;

                struct Predicate *p = &(q->predicates[q->predicate_count++]);

                getToken(query, &index, p->field, FIELD_MAX_LENGTH);

                if (strncmp(p->field, "PK(", 3) == 0) {
                    q->flags |= FLAG_PRIMARY_KEY_SEARCH;
                    size_t len = strlen(p->field);
                    // remove trailing ')'
                    for (size_t i = 0; i < len - 4; i++) {
                        p->field[i] = p->field[i+3];
                    }
                    p->field[len - 4] = '\0';
                }

                // printf("Predicate field: %s\n", predicate_field);

                char op[5];
                getToken(query, &index, op, 5);

                p->op = parseOperator(op);
                if (p->op == OPERATOR_UN) {
                    fprintf(stderr, "Bad query - expected =|!=|<|<=|>|>=\n");
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

                getQuotedToken(query, &index, p->value, VALUE_MAX_LENGTH);

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
        }
        else if (strcmp(keyword, "FETCH") == 0) {
            getToken(query, &index, keyword, FIELD_MAX_LENGTH);

            if (strcmp(keyword, "FIRST") != 0 && strcmp(keyword, "NEXT") != 0) {
                fprintf(stderr, "Bad query - expected FIRST|NEXT\n");
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

            getToken(query, &index, keyword, FIELD_MAX_LENGTH);

            if (strcmp(keyword, "ROW") != 0 && strcmp(keyword, "ROWS") != 0) {
                fprintf(stderr, "Bad query - expected ROW|ROWS; Got '%s'\n", keyword);
                return -1;
            }

            getToken(query, &index, keyword, FIELD_MAX_LENGTH);

            if (strcmp(keyword, "ONLY") != 0) {
                fprintf(stderr, "Bad query - expected ONLY; Got '%s'\n", keyword);
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
            getToken(query, &index, keyword, FIELD_MAX_LENGTH);

            if (strcmp(keyword, "BY") != 0) {
                fprintf(stderr, "Bad query - expected BY\n");
                return -1;
            }

            q->flags |= FLAG_ORDER;

            getQuotedToken(query, &index, q->order_field, FIELD_MAX_LENGTH);

            size_t original_index = index;

            getToken(query, &index, keyword, FIELD_MAX_LENGTH);

            if (strcmp(keyword, "ASC") == 0) {
                q->order_direction = ORDER_ASC;
            } else if (strcmp(keyword, "DESC") == 0) {
                q->order_direction = ORDER_DESC;
            } else {
                index = original_index;
            }

        }
        else {
            fprintf(stderr, "Bad query - expected SELECT|FROM|WHERE|OFFSET|FETCH FIRST|LIMIT\n");
            fprintf(stderr, "Found: '%s'\n", keyword);
            return -1;
        }
    }

    return 0;
}

void destroyQuery (struct Query *query) {
    if (query->predicate_count > 0) {
        free(query->predicates);
    }

    for (int i = 0; i < query->table_count; i++) {
        closeDB(query->tables[i].db);
    }

    if (query->table_count > 0) {
        free(query->tables);
    }
}

void skipWhitespace (const char *string, size_t *index) {
    while (string[*index] != '\0') {
        while(isspace(string[*index])) { (*index)++; }

        if (strncmp(string + *index, "--", 2) == 0) {
            skipLine(string, index);
        } else {
            break;
        }
    }
}

void skipToken (const char *string, size_t *index) {
    if (string[*index] == '\'') {
        // Skip open quote
        (*index)++;

        while (string[*index] != '\0' && string[*index] != '\'') {
            (*index)++;
        }

        // Skip close quote
        if (string[*index] != '\0') {
            (*index)++;
        }
    }
    else if (string[*index] == '"') {
        // Skip open quote
        (*index)++;

        while (string[*index] != '\0' && string[*index] != '"') {
            (*index)++;
        }

        // Skip close quote
        if (string[*index] != '\0') {
            (*index)++;
        }
    }
    else {
        while (!iscntrl(string[*index]) && string[*index] != ' ' && string[*index] != ',') {
            (*index)++;
        }
    }
}

void skipLine (const char *string, size_t *index) {
    while (string[*index] != '\n' && string[*index] != '\0') {
        (*index)++;
    }
    if (string[*index] == '\n') (*index)++;
}

/**
 * @brief Get the next token in the stream
 *
 * @param string
 * @param index
 * @param token
 * @param token_max_length
 * @return int Length of token
 */
int getToken (const char *string, size_t *index, char *token, int token_max_length) {
    skipWhitespace(string, index);

    if (string[*index] == '\0') {
        return -1;
    }
    int start_index = *index;

    // printf("Token starts at %d\n", start_index);

    skipToken(string, index);

    int token_length = *index - start_index;

    if (token_length > token_max_length) {
        return -1;
    }

    // printf("Token is %d characters\n", token_length);

    memcpy(token, string + start_index, token_length);

    token[token_length] = '\0';

    return token_length;
}

/**
 * @brief Get the next token in the stream, optionally quoted
 *
 * @param string
 * @param index
 * @param token
 * @param token_max_length
 * @return int 0 means no quotes, 1 means single quotes, 2 means double quotes
 */
int getQuotedToken (const char *string, size_t *index, char *token, int token_max_length) {
    skipWhitespace(string, index);

    if (string[*index] == '\0') {
        return -1;
    }

    int quoted_flag = string[*index] == '\'' ? 1 : (string[*index] == '"' ? 2 : 0);

    int start_index = *index;

    if (quoted_flag) {
        start_index++;
    }

    // printf("Token starts at %d\n", start_index);

    skipToken(string, index);

    int token_length = *index - start_index;

    if (quoted_flag) {
        token_length--;
    }

    if (token_length > token_max_length) {
        return -1;
    }

    // printf("Token is %d characters\n", token_length);

    memcpy(token, string + start_index, token_length);

    token[token_length] = '\0';

    return quoted_flag;
}

int getNumericToken (const char *string, size_t *index) {
    char val[10];
    getToken(string, index, val, 10);
    return atol(val);
}

int parseColumn (const char * query, size_t * index, struct ResultColumn *column) {
    int flags = 0;

    column->field = FIELD_UNKNOWN;
    column->function = FUNC_UNITY;

    int quoted_flag = getQuotedToken(query, index, column->text, FIELD_MAX_LENGTH);

    strcpy(column->alias, column->text);

    if (quoted_flag == 1) {
        column->field = FIELD_CONSTANT;
    }
    else if (quoted_flag == 2) {
        // Field is explicit
        // Nothing else to do
    }
    else if (is_numeric(column->text)) {
        column->field = FIELD_CONSTANT;
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
    else if (strncmp(column->text, "EXTRACT(", 8) == 0) {
        char part[FIELD_MAX_LENGTH - 8];
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

        getQuotedToken(query, index, column->text, FIELD_MAX_LENGTH);

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

        sprintf(column->alias, "EXTRACT(%s FROM %s)", part, column->text);
    }
    else if (strncmp(column->text, "COUNT(", 6) == 0) {
        column->function = FUNC_AGG_COUNT;
        flags |= FLAG_GROUP;

        if (parseFunction(query, index, column, strlen("COUNT")) < 0) {
            return -1;
        }

        sprintf(column->alias, "COUNT(%s)", column->text);
    }
    else if (strncmp(column->text, "MAX(", 4) == 0) {
        column->function = FUNC_AGG_MAX;
        flags |= FLAG_GROUP;

        if (parseFunction(query, index, column, strlen("MAX")) < 0) {
            return -1;
        }

        sprintf(column->alias, "MAX(%s)", column->text);
    }
    else if (strncmp(column->text, "MIN(", 4) == 0) {
        column->function = FUNC_AGG_MIN;
        flags |= FLAG_GROUP;

        if (parseFunction(query, index, column, strlen("MIN")) < 0) {
            return -1;
        }

        sprintf(column->alias, "MIN(%s)", column->text);
    }
    else if (strncmp(column->text, "AVG(", 4) == 0) {
        column->function = FUNC_AGG_AVG;
        flags |= FLAG_GROUP;

        if (parseFunction(query, index, column, strlen("AVG")) < 0) {
            return -1;
        }

        sprintf(column->alias, "AVG(%s)", column->text);
    }

    return flags;
}

int parseFunction (const char * query, size_t * index, struct ResultColumn * column, int name_length) {

    char field[FIELD_MAX_LENGTH];
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

    return 0;
}