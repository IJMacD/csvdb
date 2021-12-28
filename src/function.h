#include <stdio.h>

#include "db.h"
#include "result.h"

#define MASK_FUNC_FAMILY            0xE0
// xxxa aaaa
// xxx          = family (mask 0xD0)
//    a aaaa    = function (mask 0x1F)

// Family 000 (0x00)
#define FUNC_UNITY                  0


// Family 010 (0x10) String
#define FUNC_FAM_STRING             0x10

// Family 001 (0x20) Agg
#define FUNC_FAM_AGG                0x20

#define FUNC_AGG_COUNT              0x21
#define FUNC_AGG_MAX                0x22
#define FUNC_AGG_MIN                0x23
#define FUNC_AGG_AVG                0x24

// Family 010 (0x40) Extract
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

int evaluateFunction(FILE *f, struct DB *db, struct ResultColumn *column, int record_index);

int evaluateAggregateFunction (FILE *f, struct DB *db, int table_count, struct ResultColumn *column, struct RowList * row_list);
