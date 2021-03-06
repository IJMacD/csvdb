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

// xxxx aaa h
//          h - headers
//      aaa   - format

#define OUTPUT_OPTION_HEADERS   1

#define OUTPUT_MASK_FORMAT          0x0E

#define OUTPUT_FORMAT_TAB           (1 << 1)
#define OUTPUT_FORMAT_COMMA         (2 << 1)
#define OUTPUT_FORMAT_JSON          (3 << 1)
#define OUTPUT_FORMAT_HTML          (4 << 1)
#define OUTPUT_FORMAT_JSON_ARRAY    (5 << 1)
#define OUTPUT_FORMAT_SQL_INSERT    (6 << 1)

#define JOIN_INNER  0
#define JOIN_CROSS  0
#define JOIN_LEFT   1

#define ROWID_NULL  -1

#define DB_SUBQUERY (void *)-1
#define DB_VALUES   (void *)-2

struct Table {
    char name[MAX_TABLE_LENGTH];
    char alias[MAX_FIELD_LENGTH];
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
    char order_field[MAX_FIELD_LENGTH];
    int order_direction;
};

int query (const char *query, int output_flags, FILE * output);

int select_query (const char *query, int output_flags, FILE * output);
