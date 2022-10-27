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

#define FLAG_HAVE_PREDICATE         (1<<0)
#define FLAG_GROUP                  (1<<1)
#define FLAG_PRIMARY_KEY_SEARCH     (1<<2)
#define FLAG_ORDER                  (1<<3)
#define FLAG_EXPLAIN                (1<<12)
#define FLAG_READ_ONLY              (1<<13)

#define JOIN_INNER  0
#define JOIN_CROSS  0
#define JOIN_LEFT   1

#define ROWID_NULL  -1

#define DB_SUBQUERY (void *)-1

struct Table {
    char name[MAX_TABLE_LENGTH];
    char alias[MAX_TABLE_LENGTH];
    struct DB * db;
    struct Predicate join;
    int join_type;
};

struct Query {
    struct Table *tables;
    int table_count;
    struct ColumnNode columns[MAX_FIELD_COUNT];
    int column_count;
    int flags;
    int offset_value;
    int limit_value;
    struct Predicate *predicates;
    int predicate_count;
    char order_field[MAX_FIELD_LENGTH][MAX_FIELD_COUNT];
    int order_direction[MAX_FIELD_COUNT];
    int order_count;
};

int query (const char *query, int output_flags, FILE * output);

int select_query (const char *query, int output_flags, FILE * output);
