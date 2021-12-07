#pragma once

#include "limits.h"

#define FIELD_UNKNOWN                       -1
#define FIELD_STAR                          -2
#define FIELD_COUNT_STAR                    -3
#define FIELD_ROW_NUMBER                    -4
#define FIELD_ROW_INDEX                     -5

#define MASK_FUNC_FAMILY            0xE0
// xxxa aaaa
// xxx          = family (mask 0xD0)
//    a aaaa    = function (mask 0x1F)

// Family 000 (0x00)
#define FUNC_UNITY                  0

#define FUNC_FORMAT                 10

// Family 001 (0x20)
#define FUNC_AGG                    0x20

#define FUNC_AGG_COUNT              0x21
#define FUNC_AGG_MAX                0x22
#define FUNC_AGG_MIN                0x23

// Family 010 (0x40)
#define FUNC_EXTRACT                0x40

#define FUNC_EXTRACT_YEAR           0x41
#define FUNC_EXTRACT_MONTH          0x42
#define FUNC_EXTRACT_DAY            0x43
#define FUNC_EXTRACT_WEEK           0x44
#define FUNC_EXTRACT_WEEKDAY        0x45
#define FUNC_EXTRACT_WEEKYEAR       0x46
#define FUNC_EXTRACT_YEARDAY        0x47
#define FUNC_EXTRACT_HEYEAR         0x48
#define FUNC_EXTRACT_MILLENNIUM     0x49
#define FUNC_EXTRACT_CENTURY        0x4A
#define FUNC_EXTRACT_DECADE         0x4B
#define FUNC_EXTRACT_QUARTER        0x4C
#define FUNC_EXTRACT_JULIAN         0x5C
#define FUNC_EXTRACT_DATE           0x5D
#define FUNC_EXTRACT_TIME           0x5E
#define FUNC_EXTRACT_DATETIME       0x5F

#define FLAG_HAVE_PREDICATE         1
#define FLAG_GROUP                  2
#define FLAG_PRIMARY_KEY_SEARCH     4
#define FLAG_ORDER                  8
#define FLAG_EXPLAIN                4096

struct ResultColumn {
    int field;
    int function;
    char text[FIELD_MAX_LENGTH];
    char alias[FIELD_MAX_LENGTH];
    int table_id;
};

struct Query {
    char table[TABLE_MAX_LENGTH];
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

int query (const char *query, int output_flags);