#pragma once

#include "db.h"
#include "limits.h"
#include "result.h"

#define FIELD_UNKNOWN                       -1
#define FIELD_STAR                          -2
#define FIELD_COUNT_STAR                    -3
#define FIELD_ROW_NUMBER                    -4
#define FIELD_ROW_INDEX                     -5
#define FIELD_CONSTANT                      -6

#define FLAG_HAVE_PREDICATE         1
#define FLAG_GROUP                  2
#define FLAG_PRIMARY_KEY_SEARCH     4
#define FLAG_ORDER                  8
#define FLAG_EXPLAIN                4096

struct Table {
    char name[TABLE_MAX_LENGTH];
    char alias[FIELD_MAX_LENGTH];
    struct DB * db;
};

struct Query {
    struct Table *tables;
    int table_count;
    struct ResultColumn columns[FIELD_MAX_COUNT];
    int column_count;
    int flags;
    int offset_value;
    int limit_value;
    struct Predicate *predicates;
    int predicate_count;
    char order_field[FIELD_MAX_LENGTH];
    int order_direction;
};

int query (const char *query, int output_flags, FILE * output);

void findColumn (struct Query *q, const char *text, int *table_id, int *column_id);