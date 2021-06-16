#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "db.h"

void skipWhitespace (const char *string, int *index);

void skipToken (const char *string, int *index);

int getToken (const char *string, int *index, char *token, int token_max_length);

int query (const char *query) {
    if (strncmp(query, "SELECT ", 7) != 0) {
        fprintf(stderr, "Bad query - expected SELECT\n");
        return -1;
    }

    size_t query_length = strlen(query);

    // printf("Query length: %ld\n", query_length);

    int index = 7;

    skipWhitespace(query, &index);

    int value_max_length = 255;
    int field_max_length = 32;
    int field_max_count = 10;
    char fields[field_max_count * field_max_length];

    int curr_index = 0;
    while (index < query_length) {
        getToken(query, &index, fields + (field_max_length * curr_index++), field_max_length);

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

    char table[255];
    getToken(query, &index, table, 255);

    skipWhitespace(query, &index);

    char predicate_field[field_max_length];
    char predicate_value[value_max_length];
    int have_predicate = 0;

    if (index < query_length) {
        if (strncmp(query + index, "WHERE ", 6) != 0) {
            fprintf(stderr, "Bad query - expected WHERE\n");
            return -1;
        }

        index += 6;

        getToken(query, &index, predicate_field, field_max_length);

        skipWhitespace(query, &index);

        if (strncmp(query + index, "=", 1) != 0) {
            fprintf(stderr, "Bad query - expected =\n");
            return -1;
        }

        index++;

        skipWhitespace(query, &index);

        getToken(query, &index, predicate_value, value_max_length);

        have_predicate = 1;
    }

    struct DB db;

    if (openDB(&db, table) != 0) {
        fprintf(stderr, "File not found: %s\n", table);
        exit(-1);
    }

    int field_indices[field_max_count];
    
    for (int i = 0; i < curr_index; i++) {
        field_indices[i] = getFieldIndex(&db, fields + (i * field_max_length));
    }

    int predicate_field_index;

    if (have_predicate) {
        predicate_field_index = getFieldIndex(&db, predicate_field);
    }

    for (int i = 0; i < db.record_count; i++) {
        if (have_predicate) {
            char value[value_max_length];
            getRecordValue(&db, i, predicate_field_index, value, value_max_length);
            
            if (strcmp(value, predicate_value) != 0) {
                continue;
            }
        }

        for (int j = 0; j < curr_index; j++) {
            char value[value_max_length];
            getRecordValue(&db, i, field_indices[j], value, value_max_length);
            printf("%s", value);

            if (j < curr_index - 1) {
                printf("\t");
            }
        }
        printf("\n");
    }
}

void skipWhitespace (const char * string, int *index) {
    while(string[(*index)] == ' ') { (*index)++; }
}

void skipToken (const char * string, int *index) {
    while(string[(*index)] != ' ' && string[(*index)] != ',' && string[(*index)] != '\0') { (*index)++; }
}

int getToken (const char *string, int *index, char *token, int token_max_length) {
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