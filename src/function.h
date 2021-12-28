#include <stdio.h>

#include "db.h"
#include "result.h"

#define MASK_FUNC_FAMILY            0xE0
// xxxa aaaa
// xxx          = family (mask 0xD0)
//    a aaaa    = function (mask 0x1F)

// This gives 8 families with 32 functions each
// (00 function is usable in each family)

// Family 000x (0x00) Basic
#define FUNC_FAM_BASIC              0x00

#define FUNC_UNITY                  0x00
#define FUNC_CHR                    0x01
#define FUNC_TO_HEX                 0x02

#define FUNC_RANDOM                 0x10

// Family 001x (0x20) String
#define FUNC_FAM_STRING             0x20

#define FUNC_LENGTH                 0x21
#define FUNC_LEFT                   0x22
#define FUNC_RIGHT                  0x23

// Family 010x (0x40) Extract
#define FUNC_FAM_EXTRACT            0x40

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

// Family 011x (0x60) (Undefined)
#define FUNC_FAM_UNDEF_60           0x60

// Family 100x (0x80) (Undefined)
#define FUNC_FAM_UNDEF_80           0x80

// Family 101x (0xA0) Agg
#define FUNC_FAM_AGG                0xA0

#define FUNC_AGG_COUNT              0xA1
#define FUNC_AGG_MAX                0xA2
#define FUNC_AGG_MIN                0xA3
#define FUNC_AGG_AVG                0xA4

// Family 110x (0xC0) (Undefined)
#define FUNC_FAM_UNDEF_C0           0xC0

// Family 111x (0xC0) (Undefined)
#define FUNC_FAM_UNDEF_E0           0xE0


int evaluateFunction(FILE *f, struct DB *db, struct ResultColumn *column, int record_index);

int evaluateAggregateFunction (FILE *f, struct DB *db, int table_count, struct ResultColumn *column, struct RowList * row_list);
