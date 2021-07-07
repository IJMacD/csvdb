#pragma once

#include "limits.h"

#define OUTPUT_FLAG_HEADERS    1

#define FIELD_UNKNOWN       -1
#define FIELD_STAR          -2
#define FIELD_COUNT_STAR    -3
#define FIELD_ROW_NUMBER    -4
#define FIELD_ROW_INDEX     -5

#define FLAG_HAVE_PREDICATE         1
#define FLAG_GROUP                  2
#define FLAG_PRIMARY_KEY_SEARCH     4
#define FLAG_ORDER                  8
#define FLAG_EXPLAIN                4096

#define FIELD_MAX_COUNT     10

struct Query {
    char table[TABLE_MAX_LENGTH];
    char fields[FIELD_MAX_COUNT * FIELD_MAX_LENGTH];
    int field_count;
    int flags;
    int offset_value;
    int limit_value;
    char predicate_field[FIELD_MAX_LENGTH];
    int predicate_op;
    char predicate_value[VALUE_MAX_LENGTH];
    char order_field[FIELD_MAX_LENGTH];
    int order_direction;
};

int query (const char *query, int output_flags);