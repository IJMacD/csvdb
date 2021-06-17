#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "db.h"

#define FIELD_UNKNOWN       -1
#define FIELD_STAR          -2
#define FIELD_COUNT_STAR    -3

#define VALUE_MAX_LENGTH    255
#define FIELD_MAX_LENGTH    32
#define TABLE_MAX_LENGTH    255

#define FIELD_MAX_COUNT     10

void skipWhitespace (const char *string, size_t *index);

void skipToken (const char *string, size_t *index);

int getToken (const char *string, size_t *index, char *token, int token_max_length);

void printResultLine (struct DB *db, int *field_indices, int field_count, int record_index, int result_count);

int query (const char *query) {
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

    skipWhitespace(query, &index);

    char predicate_field[FIELD_MAX_LENGTH];
    char predicate_value[VALUE_MAX_LENGTH];

    int have_predicate = 0;
    int is_group = 0;
    int result_count = 0;
    int group_specimen = -1;

    if (index < query_length) {
        if (strncmp(query + index, "WHERE ", 6) != 0) {
            fprintf(stderr, "Bad query - expected WHERE\n");
            return -1;
        }

        index += 6;

        getToken(query, &index, predicate_field, FIELD_MAX_LENGTH);

        skipWhitespace(query, &index);

        if (query[index] != '=') {
            fprintf(stderr, "Bad query - expected =\n");
            return -1;
        }

        index++;

        skipWhitespace(query, &index);

        getToken(query, &index, predicate_value, VALUE_MAX_LENGTH);

        have_predicate = 1;
    }

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
            is_group = 1;
        } else if (strcmp(field_name, "*") == 0) {
            field_indices[i] = FIELD_STAR;
        }
        else {
            field_indices[i] = getFieldIndex(&db, field_name);

            if (field_indices[i] == -1) {
                fprintf(stderr, "Field %s not found\n", &fields[i * FIELD_MAX_LENGTH]);
                return -1;
            }
        }

    }

    int predicate_field_index = FIELD_UNKNOWN;

    if (have_predicate) {
        predicate_field_index = getFieldIndex(&db, predicate_field);
    }

    // If we have COUNT(*) and there's no predicate then just early exit
    // we already know how many records there are 
    if (is_group && !have_predicate) {
        printResultLine(&db, field_indices, curr_index, 0, db.record_count);
        return 0;
    }

    for (int i = 0; i < db.record_count; i++) {
        if (have_predicate) {
            char value[VALUE_MAX_LENGTH];
            getRecordValue(&db, i, predicate_field_index, value, VALUE_MAX_LENGTH);
            
            if (strcmp(value, predicate_value) != 0) {
                continue;
            }
        }

        result_count++;

        // If we are grouping then don't output anything
        // but we do need to loop through the rows to check predicates
        // we'll also make note of a specimen record
        if (is_group) {
            // SQL says specimen can be any matching record we like
            group_specimen = i;
        } else {
            printResultLine(&db, field_indices, curr_index, i, 0);
        }
    }

    // COUNT(*) will print just one row
    if (is_group) {
        printResultLine(&db, field_indices, curr_index, group_specimen, result_count);
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

    // printf("Field starts at %d\n", start_index);

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
        } else if (field_indices[j] == FIELD_COUNT_STAR) {
            printf("%d", result_count);
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