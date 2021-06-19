#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "db.h"
#include "tree.h"
#include "limits.h"

#define FIELD_UNKNOWN       -1
#define FIELD_STAR          -2
#define FIELD_COUNT_STAR    -3
#define FIELD_ROW_NUMBER    -4
#define FIELD_ROW_INDEX     -5

#define OPERATOR_UN         0
#define OPERATOR_EQ         1
#define OPERATOR_NE         2
#define OPERATOR_LT         3
#define OPERATOR_LE         4
#define OPERATOR_GT         5
#define OPERATOR_GE         6

#define FLAG_HAVE_PREDICATE         1
#define FLAG_GROUP                  2
#define FLAG_PRIMARY_KEY_SEARCH     4
#define FLAG_ORDER                  8

#define FIELD_MAX_COUNT     10

void skipWhitespace (const char *string, size_t *index);

void skipToken (const char *string, size_t *index);

int getToken (const char *string, size_t *index, char *token, int token_max_length);

int getNumericToken (const char *string, size_t *index);

void printResultLine (struct DB *db, int *field_indices, int field_count, int record_index, int result_count);

char parseOperator (const char *input);

int evaluateExpression (char op, const char *left, const char *right);

int pk_search(struct DB *db, int pk_index, char *value);

int query (const char *query) {
    /*********************
     * Begin Query parsing
     *********************/

    if (strncmp(query, "SELECT ", 7) != 0) {
        fprintf(stderr, "Bad query - expected SELECT\n");
        return -1;
    }

    size_t query_length = strlen(query);

    // printf("Query length: %ld\n", query_length);

    size_t index = 7;

    skipWhitespace(query, &index);

    char fields[FIELD_MAX_COUNT * FIELD_MAX_LENGTH];

    int curr_index = 0;
    while (index < query_length) {
        getToken(query, &index, fields + (FIELD_MAX_LENGTH * curr_index++), FIELD_MAX_LENGTH);

        // printf("Field is %s\n", field);

        skipWhitespace(query, &index);

        if (query[index] != ',') {
            break;
        }

        index++;

        skipWhitespace(query, &index);
    }

    // printf("Asked for %d field(s)\n", curr_index);

    if (strncmp(query + index, "FROM ", 5) != 0) {
        fprintf(stderr, "Bad query - expected FROM\n");
        return -1;
    }

    index += 5;

    char table[TABLE_MAX_LENGTH];
    getToken(query, &index, table, TABLE_MAX_LENGTH);

    char predicate_field[FIELD_MAX_LENGTH];
    char predicate_value[VALUE_MAX_LENGTH];
    char predicate_op = OPERATOR_UN;

    char order_field[FIELD_MAX_LENGTH];

    int flags = 0;
    int result_count = 0;
    int group_specimen = -1;
    long offset_value = 0;
    long limit_value = -1;

    while (index < query_length) {

        skipWhitespace(query, &index);

        char keyword[FIELD_MAX_LENGTH];

        getToken(query, &index, keyword, FIELD_MAX_LENGTH);
        
        // printf("Token: '%s'\n", keyword);

        if (strcmp(keyword, "WHERE") == 0) {
            flags |= FLAG_HAVE_PREDICATE;

            skipWhitespace(query, &index);

            getToken(query, &index, predicate_field, FIELD_MAX_LENGTH);

            if (strncmp(predicate_field, "PK(", 3) == 0) {
                flags |= FLAG_PRIMARY_KEY_SEARCH;
                size_t len = strlen(predicate_field);
                // remove trailing ')'
                for (size_t i = 0; i < len - 4; i++) {
                    predicate_field[i] = predicate_field[i+3];
                }
                predicate_field[len - 4] = '\0';
            }

            // printf("Predicate field: %s\n", predicate_field);

            skipWhitespace(query, &index);

            char op[3];
            getToken(query, &index, op, 3);

            predicate_op = parseOperator(op);

            if (predicate_op == OPERATOR_UN) {
                fprintf(stderr, "Bad query - expected =|!=|<|<=|>|>=\n");
                return -1;
            }

            index++;

            skipWhitespace(query, &index);

            getToken(query, &index, predicate_value, VALUE_MAX_LENGTH);
        }
        else if (strcmp(keyword, "OFFSET") == 0) {
            skipWhitespace(query, &index);

            offset_value = getNumericToken(query, &index);

            if (offset_value < 0) {
                fprintf(stderr, "OFFSET cannot be negative\n");
                return -1;
            }
        }
        else if (strcmp(keyword, "FETCH") == 0) {
            skipWhitespace(query, &index);

            getToken(query, &index, keyword, FIELD_MAX_LENGTH);

            if (strcmp(keyword, "FIRST") != 0 && strcmp(keyword, "NEXT") != 0) {
                fprintf(stderr, "Bad query - expected FIRST|NEXT\n");
                return -1;
            }

            skipWhitespace(query, &index);

            if (isdigit(query[index])) {

                limit_value = getNumericToken(query, &index);

                if (limit_value < 0) {
                    fprintf(stderr, "FETCH FIRST cannot be negative\n");
                    return -1;
                }

                skipWhitespace(query, &index);
            } else {
                limit_value = 1;
            }

            getToken(query, &index, keyword, FIELD_MAX_LENGTH);

            if (strcmp(keyword, "ROW") != 0 && strcmp(keyword, "ROWS") != 0) {
                fprintf(stderr, "Bad query - expected ROW|ROWS; Got '%s'\n", keyword);
                return -1;
            }
        }
        else if (strcmp(keyword, "LIMIT") == 0) {
            skipWhitespace(query, &index);

            limit_value = getNumericToken(query, &index);

            if (limit_value < 0) {
                fprintf(stderr, "LIMIT cannot be negative\n");
                return -1;
            }
        }
        else if (strcmp(keyword, "ORDER") == 0) {
            skipWhitespace(query, &index);

            getToken(query, &index, keyword, FIELD_MAX_LENGTH);

            if (strcmp(keyword, "BY") != 0) {
                fprintf(stderr, "Bad query - expected BY\n");
                return -1;
            }

            flags |= FLAG_ORDER;

            skipWhitespace(query, &index);

            getToken(query, &index, order_field, FIELD_MAX_LENGTH);

        }
        else {
            fprintf(stderr, "Bad query - expected WHERE|OFFSET|FETCH FIRST|LIMIT\n");
            fprintf(stderr, "Found: '%s'\n", keyword);
            return -1;
        }
    }

    /*************************
     * Begin Query processing
     *************************/

    struct DB db;

    if (openDB(&db, table) != 0) {
        fprintf(stderr, "File not found: %s\n", table);
        return -1;
    }

    int field_indices[FIELD_MAX_COUNT];
    
    for (int i = 0; i < curr_index; i++) {
        char *field_name = fields + (i * FIELD_MAX_LENGTH);

        if (strcmp(field_name, "COUNT(*)") == 0) {
            field_indices[i] = FIELD_COUNT_STAR;
            flags |= FLAG_GROUP;
        } else if (strcmp(field_name, "*") == 0) {
            field_indices[i] = FIELD_STAR;
        } else if (strcmp(field_name, "ROW_NUMBER()") == 0) {
            field_indices[i] = FIELD_ROW_NUMBER;
        } else if (strcmp(field_name, "rowid") == 0) {
            field_indices[i] = FIELD_ROW_INDEX;
        }
        else {
            field_indices[i] = getFieldIndex(&db, field_name);

            if (field_indices[i] == -1) {
                fprintf(stderr, "Field %s not found\n", &fields[i * FIELD_MAX_LENGTH]);
                return -1;
            }
        }

    }

    /*************************
     * Special Cases
     *************************/

    // If we have COUNT(*) and there's no predicate then just early exit
    // we already know how many records there are 
    if ((flags & FLAG_GROUP) && !(flags & FLAG_HAVE_PREDICATE)) {
        // We also need to provide a specimen row
        // "0 was chosen by a fair dice roll"
        // > But now we'll use offset value
        long count = db.record_count;
        if (limit_value >= 0L && limit_value < count) {
            count = limit_value;
        }
        printResultLine(&db, field_indices, curr_index, offset_value, count);
        return 0;
    }

    int predicate_field_index = FIELD_UNKNOWN;

    if (flags & FLAG_HAVE_PREDICATE) {
        predicate_field_index = getFieldIndex(&db, predicate_field);
        // printf("Predicate index: %d\n", predicate_field_index);
    }

    // If we have a primary key search then we can binary search
    if ((flags & FLAG_PRIMARY_KEY_SEARCH) && predicate_op == OPERATOR_EQ) {
        int record_index = pk_search(&db, predicate_field_index, predicate_value);
        // If we didn't find a record we shouldn't output anything unless we're grouping
        if (record_index >= 0 || (flags & FLAG_GROUP)) {
            printResultLine(&db, field_indices, curr_index, record_index, record_index == -1 ? 0 : 1);
        }
        return 0;
    }

    /**********************
     * Start iterating rows
     **********************/
    int *result_rowids = malloc(sizeof (int) * db.record_count); 
    for (int i = 0; i < db.record_count; i++) {

        // Perform filtering if necessary
        if (flags & FLAG_HAVE_PREDICATE) {
            char value[VALUE_MAX_LENGTH];
            getRecordValue(&db, i, predicate_field_index, value, VALUE_MAX_LENGTH);
            
            if (!evaluateExpression(predicate_op, value, predicate_value)) {
                continue;
            }
        }

        // If we are grouping then don't output anything
        // but we do need to loop through the rows to check predicates
        // we'll also make note of a specimen record
        if (flags & FLAG_GROUP) {
            // SQL says specimen can be any matching record we like
            group_specimen = i;
        } else {
            // Add to result set
            result_rowids[result_count] = i;
        }

        result_count++;

        // Implement early exit FETCH FIRST/LIMIT for cases with no ORDER clause
        if (!(flags & FLAG_ORDER) && limit_value >= 0 && (result_count - offset_value) >= limit_value) {
            break;
        }
    }

    /*******************
     * Ordering
     *******************/
    if (flags & FLAG_ORDER) {
        int order_index = getFieldIndex(&db, order_field);
        struct tree *pool = malloc(sizeof (struct tree) * result_count);
        struct tree *root = pool;

        for (int i = 0; i < result_count; i++) {
            struct tree *node = pool++;
            makeNode(&db, order_index, result_rowids[i], node);

            if (i > 0) {
                insertNumericNode(root, node);
            }
        }

        // Walk tree overwriting result_rowids array
        int *result_ptr = result_rowids;
        walkTree(root, &result_ptr);
    }

    /********************
     * OFFSET/FETCH FIRST
     ********************/
    result_rowids += offset_value;
    result_count -= offset_value;

    if (limit_value >= 0 && limit_value < result_count) {
        result_count = limit_value;
    }

    /*******************
     * Output result set
     *******************/

    // COUNT(*) will print just one row
    if (flags & FLAG_GROUP) {
        // printf("Aggregate result:\n");
        printResultLine(&db, field_indices, curr_index, group_specimen, result_count);
    } else for (int i = 0; i < result_count; i++) {

        // ROW_NUMBER is offset by OFFSET from result index and is 1-index based
        printResultLine(&db, field_indices, curr_index, result_rowids[i], offset_value + i + 1);
    }

    return 0;
}

void skipWhitespace (const char * string, size_t *index) {
    while(string[(*index)] == ' ') { (*index)++; }
}

void skipToken (const char * string, size_t *index) {
    while(string[(*index)] != ' ' && string[(*index)] != ',' && string[(*index)] != '\0') { (*index)++; }
}

int getToken (const char *string, size_t *index, char *token, int token_max_length) {
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

    return 0;
}

int getNumericToken (const char *string, size_t *index) {
    char val[10];
    getToken(string, index, val, 10);
    return atol(val);
}

void printResultLine (struct DB *db, int *field_indices, int field_count, int record_index, int result_count) {
    for (int j = 0; j < field_count; j++) {

        if (field_indices[j] == FIELD_STAR) {
            for (int k = 0; k < db->field_count; k++) {
                char value[VALUE_MAX_LENGTH];
                if (getRecordValue(db, record_index, k, value, VALUE_MAX_LENGTH) > 0) {
                    printf("%s", value);
                }

                if (k < db->field_count - 1) {
                    printf("\t");
                }
            }
        } else if (field_indices[j] == FIELD_COUNT_STAR || field_indices[j] == FIELD_ROW_NUMBER) {
            // Same logic is recycled when printing result
            // FIELD_COUNT_STAR causes grouping and gets total at end
            // FIELD_ROW_NUMBER uses current matched result count at each iteration
            printf("%d", result_count);
        } else if (field_indices[j] == FIELD_ROW_INDEX) {
            // FIELD_ROW_INDEX is the input line (0 indexed)
            printf("%d", record_index);
        } else {
            char value[VALUE_MAX_LENGTH];
            if (getRecordValue(db, record_index, field_indices[j], value, VALUE_MAX_LENGTH)> 0) {
                printf("%s", value);
            }
        }

        if (j < field_count - 1) {
            printf("\t");
        }
    }
    printf("\n");
}

char parseOperator (const char *input) {
    if (strcmp(input, "=") == 0)  return OPERATOR_EQ;
    if (strcmp(input, "!=") == 0) return OPERATOR_NE;
    if (strcmp(input, "IS") == 0) return OPERATOR_EQ;
    if (strcmp(input, "<") == 0)  return OPERATOR_LT;
    if (strcmp(input, "<=") == 0) return OPERATOR_LE;
    if (strcmp(input, ">") == 0)  return OPERATOR_GT;
    if (strcmp(input, ">=") == 0) return OPERATOR_GE;
    return OPERATOR_UN;
}

int evaluateExpression (char op, const char *left, const char *right) {
    // printf("Evaluating %s OP %s\n", left, right);

    if (strcmp(right, "NULL") == 0) {
        size_t len = strlen(left);

        if (op == OPERATOR_EQ) return len == 0;
        if (op == OPERATOR_NE) return len != 0;

        return 0;
    }

    if (strcmp(left, "NULL") == 0) {
        size_t len = strlen(right);

        if (op == OPERATOR_EQ) return len == 0;
        if (op == OPERATOR_NE) return len != 0;

        return 0;
    }

    if (op == OPERATOR_EQ) return strcmp(left, right) == 0;
    if (op == OPERATOR_NE) return strcmp(left, right) != 0;

    long left_num = strtol(left, NULL, 10);
    long right_num = strtol(right, NULL, 10);

    if (op == OPERATOR_LT) return left_num < right_num;
    if (op == OPERATOR_LE) return left_num <= right_num;
    if (op == OPERATOR_GT) return left_num > right_num;
    if (op == OPERATOR_GE) return left_num >= right_num;

    return 0;
}

int pk_search(struct DB *db, int pk_index, char *value) {
    int index_a = 0;
    int index_b = db->record_count - 1;

    long search_value = atol(value);

    char val[VALUE_MAX_LENGTH];

    long curr_value;

    // Boundary cases
    getRecordValue(db, index_a, pk_index, val, VALUE_MAX_LENGTH);
    curr_value = atol(val);
    if (curr_value == search_value) {
        return index_a;
    } else if (search_value < curr_value) {
        return -1;
    }
    getRecordValue(db, index_b, pk_index, val, VALUE_MAX_LENGTH);
    curr_value = atol(val);
    if (curr_value == search_value) {
        return index_b;
    } else if (search_value > curr_value) {
        return -1;
    }

    while (index_a < index_b - 1) { 
        int index_curr = (index_a + index_b) / 2;

        getRecordValue(db, index_curr, pk_index, val, VALUE_MAX_LENGTH);

        curr_value = atol(val);

        if (curr_value == search_value) {
            // printf("pk_search [%d   <%d>   %d]: %s\n", index_a, index_curr, index_b, val);
            return index_curr;
        }

        if (curr_value < search_value) {
            // printf("pk_search [%d   (%d) x %d]: %s\n", index_a, index_curr, index_b, val);
            index_a = index_curr;

        } else {
            // printf("pk_search [%d x (%d)   %d]: %s\n", index_a, index_curr, index_b, val);
            index_b = index_curr;
        }
    }

    return -1;
}
