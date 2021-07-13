#include <string.h>
#include <ctype.h>

#include "parse.h"
#include "predicates.h"
#include "sort.h"

int parseQuery (struct Query *q, const char *query) {
    /*********************
     * Begin Query parsing
     *********************/

    size_t index = 0;

    size_t query_length = strlen(query);

    // printf("Query length: %ld\n", query_length);

    // Allow SELECT to be optional and default to SELECT *
    q->columns[0].field = FIELD_STAR;
    q->column_count = 1;

    q->predicate_op = OPERATOR_UN;

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
                column->field = FIELD_UNKNOWN;
                column->function = FUNC_UNITY;
                strcpy(column->alias, "");

                getToken(query, &index, column->text, FIELD_MAX_LENGTH);

                // printf("Field is %s\n", field);

                if (strcmp(column->text, "COUNT(*)") == 0) {
                    column->field = FIELD_COUNT_STAR;
                    q->flags |= FLAG_GROUP;
                }
                else if (strcmp(column->text, "*") == 0) {
                    column->field = FIELD_STAR;
                }
                else if (strcmp(column->text, "ROW_NUMBER()") == 0) {
                    column->field = FIELD_ROW_NUMBER;
                }
                else if (strcmp(column->text, "rowid") == 0) {
                    column->field = FIELD_ROW_INDEX;
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
                    else {
                        fprintf(stderr, "Bad query - expected valid extract part - got %s\n", part);
                        return -1;
                    }

                    skipWhitespace(query, &index);

                    char value[10];
                    getToken(query, &index, value, 10);

                    if (strcmp(value, "FROM") != 0) {
                        fprintf(stderr, "Bad query - expected FROM\n");
                        return -1;
                    }

                    skipWhitespace(query, &index);

                    getToken(query, &index, column->text, FIELD_MAX_LENGTH);

                    size_t len = strlen(column->text);

                    if (column->text[len - 1] == ')') {
                        column->text[len - 1] = '\0';
                    }
                    else {
                        skipWhitespace(query, &index);

                        if (query[index] != ')') {
                            fprintf(stderr, "Bad query - expected ')' got '%c'\n", query[index]);
                            return -1;
                        }

                        index++;
                    }

                    sprintf(column->alias, "EXTRACT(%s FROM %s)", part, column->text);
                }

                skipWhitespace(query, &index);

                if (strncmp(query + index, "AS ", 3) == 0) {
                    index += 3;

                    getToken(query, &index, column->alias, FIELD_MAX_LENGTH);
                }

                if (query[index] != ',') {
                    break;
                }

                index++;
            }

            q->column_count = curr_index;
        }
        else if (strcmp(keyword, "FROM") == 0) {
            getToken(query, &index, q->table, TABLE_MAX_LENGTH);
        }
        else if (strcmp(keyword, "WHERE") == 0) {
            q->flags |= FLAG_HAVE_PREDICATE;

            getToken(query, &index, q->predicate_field, FIELD_MAX_LENGTH);

            if (strncmp(q->predicate_field, "PK(", 3) == 0) {
                q->flags |= FLAG_PRIMARY_KEY_SEARCH;
                size_t len = strlen(q->predicate_field);
                // remove trailing ')'
                for (size_t i = 0; i < len - 4; i++) {
                    q->predicate_field[i] = q->predicate_field[i+3];
                }
                q->predicate_field[len - 4] = '\0';
            }

            // printf("Predicate field: %s\n", predicate_field);

            char op[5];
            getToken(query, &index, op, 5);

            q->predicate_op = parseOperator(op);
            if (q->predicate_op == OPERATOR_UN) {
                fprintf(stderr, "Bad query - expected =|!=|<|<=|>|>=\n");
                return -1;
            }

            // Check for IS NOT
            if (strcmp(op, "IS") == 0) {
                skipWhitespace(query, &index);
                if (strncmp(query + index, "NOT ", 4) == 0) {
                    q->predicate_op = OPERATOR_NE;
                    index += 4;
                }
            }

            getToken(query, &index, q->predicate_value, VALUE_MAX_LENGTH);
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

            getToken(query, &index, q->order_field, FIELD_MAX_LENGTH);

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
            fprintf(stderr, "Bad query - expected WHERE|OFFSET|FETCH FIRST|LIMIT\n");
            fprintf(stderr, "Found: '%s'\n", keyword);
            return -1;
        }
    }

    return 0;
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

int getToken (const char *string, size_t *index, char *token, int token_max_length) {
    skipWhitespace(string, index);

    if (string[*index] == '\0') {
        return -1;
    }

    int quoted_flag = (string[*index] == '\'' || string[*index] == '"');

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

    return token_length;
}

int getNumericToken (const char *string, size_t *index) {
    char val[10];
    getToken(string, index, val, 10);
    return atol(val);
}