#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "../structs.h"
#include "../functions/date.h"
#include "../evaluate/predicates.h"
#include "../query/result.h"
#include "../debug.h"

#define COL_JULIAN              0
#define COL_DATE                1
#define COL_YEAR                2
#define COL_MONTH               3
#define COL_DAY                 4
#define COL_WEEKYEAR            5
#define COL_WEEK                6
#define COL_WEEKDAY             7
#define COL_YEARDAY             8
#define COL_MILLENIUM           9
#define COL_CENTURY             10
#define COL_DECADE              11
#define COL_QUARTER             12
#define COL_FIRST_OF_YEAR       13
#define COL_LAST_OF_YEAR        14
#define COL_FIRST_OF_QUARTER    15
#define COL_LAST_OF_QUARTER     16
#define COL_FIRST_OF_MONTH      17
#define COL_LAST_OF_MONTH       18
#define COL_FIRST_OF_WEEK       19
#define COL_LAST_OF_WEEK        20
#define COL_IS_LEAP_YEAR        21
#define COL_WEEKDAY_IN_MONTH    22
#define COL_IS_WEEKEND          23
#define COL_MONTH_STRING        24
#define COL_YEARDAY_STRING      25
#define COL_WEEK_STRING         26
#define COL_WEEKDAY_STRING      27

static char *field_names[] = {
    "julian",
    "date",
    "year",
    "month",
    "day",
    "weekyear",
    "week",
    "weekday",
    "yearday",
    "millenium",
    "century",
    "decade",
    "quarter",
    "firstOfYear",
    "lastOfYear",
    "firstOfQuarter",
    "lastOfQuarter",
    "firstOfMonth",
    "lastOfMonth",
    "firstOfWeek",
    "lastOfWeek",
    "isLeapYear",
    "weekdayInMonth",
    "isWeekend",
    "monthString",
    "ordinalDate",
    "weekString",
    "weekDate"
};

const int month_lengths[] = {31,28,31,30,31,30,31,31,30,31,30,31};

static void getJulianRange (
    struct Node *predicates,
    int predicate_count,
    int *julian_start,
    int *julian_end
);

static void getSingleJulianRange (
    struct Node *predicate,
    int *julian_start,
    int *julian_end
);

static int printDate (
    char *value,
    int max_length,
    struct DateTime date
);

int calendar_openDB (
    struct DB *db,
    const char *filename,
    __attribute__((unused)) char **resolved
) {
    if (strcmp(filename, "CALENDAR") != 0) {
        return -1;
    }

    db->vfs = VFS_CALENDAR;
    db->_record_count = 1e7;
    db->line_indices = NULL;
    db->field_count = sizeof(field_names) / sizeof(field_names[0]);
    db->data = NULL;

    return 0;
}

void calendar_closeDB (struct DB *db) {
    if (db->data != NULL) {
        free(db->data);
    }
}

int calendar_getFieldIndex (
    __attribute__((unused)) struct DB *db,
    const char *field
) {
    if (strcmp(field, "julian") == 0 || strcmp(field, "rowid") == 0) {
        return FIELD_ROW_INDEX;
    }

    int l = sizeof(field_names) / sizeof(field_names[0]);

    for (int i = 0; i < l; i++) {
        if (strcmp(field, field_names[i]) == 0) {
            return i;
        }
    }

    return -1;
}

char *calendar_getFieldName (
    __attribute__((unused)) struct DB *db,
    int field_index
) {
    return field_names[field_index];
}

int calendar_getRecordCount (struct DB *db) {
    return db->_record_count;
}

int calendar_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
) {
    // Special case for an "index"
    if (db->field_count == 2 && field_index == 1) {
        // indexRangeScan() thinks it's dealing with an index DB, just return
        // the rowid
        return snprintf(value, value_max_length, "%d", record_index);
    }

    // julian/rowid
    if (field_index == COL_JULIAN || field_index == FIELD_ROW_INDEX) {
        return snprintf(value, value_max_length, "%d", record_index);
    }

    struct DateTime dt = {0};
    datetimeFromJulian(&dt, record_index);

    struct DateTime dt2 = {0};

    // date
    if (field_index == COL_DATE) {
        return printDate(value, value_max_length, dt);
    }

    // year
    if (field_index == COL_YEAR) {
        return snprintf(value, value_max_length, "%d", dt.year);
    }

    // month
    if (field_index == COL_MONTH) {
        return snprintf(value, value_max_length, "%d", dt.month);
    }

    // day
    if (field_index == COL_DAY) {
        return snprintf(value, value_max_length, "%d", dt.day);
    }

    // weekyear
    if (field_index == COL_WEEKYEAR) {
        return snprintf(
            value,
            value_max_length,
            "%d",
            datetimeGetWeekYear(&dt)
        );
    }

    // week
    if (field_index == COL_WEEK) {
        return snprintf(value, value_max_length, "%d", datetimeGetWeek(&dt));
    }

    // weekday
    if (field_index == COL_WEEKDAY) {
        return snprintf(value, value_max_length, "%d", datetimeGetWeekDay(&dt));
    }

    // yearday
    if (field_index == COL_YEARDAY) {
        return snprintf(value, value_max_length, "%d", datetimeGetYearDay(&dt));
    }

    // millenium
    if (field_index == COL_MILLENIUM) {
        return snprintf(value, value_max_length, "%d", dt.year / 1000);
    }

    // century
    if (field_index == COL_CENTURY) {
        return snprintf(value, value_max_length, "%d", dt.year / 100);
    }

    // decade
    if (field_index == COL_DECADE) {
        return snprintf(value, value_max_length, "%d", dt.year / 10);
    }

    // quarter
    if (field_index == COL_QUARTER) {
        return snprintf(value, value_max_length, "%d", (dt.month - 1) / 3 + 1);
    }

    // firstOfYear
    if (field_index == COL_FIRST_OF_YEAR) {
        dt2.year = dt.year;
        dt2.month = 1;
        dt2.day = 1;
        return printDate(value, value_max_length, dt2);
    }

    // lastOfYear
    if (field_index == COL_LAST_OF_YEAR) {
        dt2.year = dt.year;
        dt2.month = 12;
        dt2.day = 31;
        return printDate(value, value_max_length, dt2);
    }

    // firstOfQuarter
    if (field_index == COL_FIRST_OF_QUARTER) {
        dt2.year = dt.year;
        dt2.month = ((dt.month - 1) / 3) * 3 + 1;
        dt2.day = 1;
        return printDate(value, value_max_length, dt2);
    }

    // lastOfQuarter
    if (field_index == COL_LAST_OF_QUARTER) {
        dt2.year = dt.year;
        dt2.month = (((dt.month - 1) / 3) + 1) * 3;

        dt2.day = month_lengths[dt2.month - 1];
        if (dt2.month == 2 && isLeapYear(dt2.year)) {
            dt2.day++;
        }

        return printDate(value, value_max_length, dt2);
    }

    // firstOfMonth
    if (field_index == COL_FIRST_OF_MONTH) {
        dt2.year = dt.year;
        dt2.month = dt.month;
        dt2.day = 1;
        return printDate(value, value_max_length, dt2);
    }

    // lastOfMonth
    if (field_index == COL_LAST_OF_MONTH) {
        dt2.year = dt.year;
        dt2.month = dt.month;

        dt2.day = month_lengths[dt2.month - 1];
        if (dt2.month == 2 && isLeapYear(dt2.year)) {
            dt2.day++;
        }

        return printDate(value, value_max_length, dt2);
    }

    // firstOfWeek
    if (field_index == COL_FIRST_OF_WEEK) {
        int weekDay = datetimeGetWeekDay(&dt);
        datetimeFromJulian(&dt2, record_index - weekDay + 1);
        return printDate(value, value_max_length, dt2);
    }

    // lastOfWeek
    if (field_index == COL_LAST_OF_WEEK) {
        int weekDay = datetimeGetWeekDay(&dt);
        datetimeFromJulian(&dt2, record_index - weekDay + 7);
        return printDate(value, value_max_length, dt2);
    }

    // isLeapYear
    if (field_index == COL_IS_LEAP_YEAR) {
        return snprintf(
            value,
            value_max_length,
            "%d",
            isLeapYear(dt.year) ? 1 : 0
        );
    }

    // weekdayInMonth
    if (field_index == COL_WEEKDAY_IN_MONTH) {
        return snprintf(value, value_max_length, "%d", (dt.day - 1) / 7 + 1);
    }

    // isWeekend
    if (field_index == COL_IS_WEEKEND) {
        return snprintf(
            value,
            value_max_length,
            "%d",
            datetimeGetWeekDay(&dt) >= 6 ? 1 : 0
        );
    }

    // month string
    if (field_index == COL_MONTH_STRING) {
        return snprintf(
            value,
            value_max_length,
            "%04d-%02d",
            dt.year,
            dt.month
        );
    }

    // yearday string
    if (field_index == COL_YEARDAY_STRING) {
        return snprintf(
            value,
            value_max_length,
            "%04d-%03d",
            dt.year,
            datetimeGetYearDay(&dt)
        );
    }

    // week string
    if (field_index == COL_WEEK_STRING) {
        return snprintf(
            value,
            value_max_length,
            "%04d-W%02d",
            datetimeGetWeekYear(&dt),
            datetimeGetWeek(&dt)
        );
    }

    // month string
    if (field_index == COL_WEEKDAY_STRING) {
        return snprintf(
            value,
            value_max_length,
            "%04d-W%02d-%d",
            datetimeGetWeekYear(&dt),
            datetimeGetWeek(&dt),
            datetimeGetWeekDay(&dt)
        );
    }

    return 0;
}

// All queries go through fullTableScan but it's useful to indicate to the
// planner that julian and date are unique
enum IndexSearchType calendar_findIndex(
    struct DB *db,
    __attribute__((unused)) const char *table_name,
    struct Node *node,
    __attribute__((unused)) int index_type_flags,
    char **resolved
) {
    if (node->function != FUNC_UNITY) {
        return INDEX_NONE;
    }

    char *field_name = node->field.text;

    if (strcmp(field_name, "julian") == 0) {
        if (db != NULL) {
            calendar_openDB(db, "CALENDAR", NULL);
            db->field_count = 2;

            // Store index type in unused data field
            db->data = malloc(1);
            db->data[0] = COL_JULIAN;
        }

        // Write the resolved name if caller wants it
        if (resolved != NULL) {
            *resolved = malloc(strlen("julian")+1);
            strcpy(*resolved, "julian");
        }

        return INDEX_PRIMARY;
    }

    if (strcmp(field_name, "date") == 0) {
        if (db != NULL) {
            calendar_openDB(db, "CALENDAR", NULL);
            db->field_count = 2;

            // Store index type in unused data field
            db->data = malloc(1);
            db->data[0] = COL_DATE;
        }

        // Write the resolved name if caller wants it
        if (resolved != NULL) {
            *resolved = malloc(strlen("date")+1);
            strcpy(*resolved, "date");
        }

        return INDEX_UNIQUE;
    }

    return INDEX_NONE;
}

/**
 * Guaranteed that all predicates are on this table
 *
 * @return int number of matched rows
 */
int calendar_fullTableAccess (
    struct DB *db,
    RowListIndex list_id,
    struct Node *predicates,
    int predicate_count,
    int limit_value
) {
    int julian = -1, max_julian = -1;

    // Try to get range from predicates
    getJulianRange(predicates, predicate_count, &julian, &max_julian);

    // Default to min and max
    // There could still be a predicate which limits the output further - we
    // just can't do it as efficiently
    if (julian < 0) {
        julian = 0;
    }
    if (max_julian < 0) {
        max_julian = db->_record_count;
    }

    // No limit means we'll use the limit defined for the VFS
    // (hopefully there are enough predicates that we won't have that many
    // results though)
    if (limit_value < 0) {
        limit_value = db->_record_count;
    }

    int count = 0;

    struct Table table;
    table.db = db;

    for (; julian < max_julian; julian++) {
        int matching = 1;

        // printf("Julian: %d\n", julian);

        // Perform filtering if necessary
        if (predicate_count > 0) {
            matching = evaluateOperatorNodeListAND(
                &table,
                ROWLIST_ROWID,
                julian,
                predicates,
                predicate_count
            );
        }

        if (matching) {
            // Add to result set
            appendRowID(getRowList(list_id), julian);
            count++;
        }

        // Implement early exit FETCH FIRST/LIMIT for cases with no ORDER clause
        if (count >= limit_value) {
            break;
        }
    }

    return count;
}

static void getJulianRange (
    struct Node *predicates,
    int predicate_count,
    int *julian_start,
    int *julian_end
) {

    for (int i = 0; i < predicate_count; i++) {
        struct Node *predicate = &predicates[i];

        int j_start = -1;
        int j_end = -1;

        getSingleJulianRange(predicate, &j_start, &j_end);

        enum Function op = predicate->function;

        if (j_start != -1 && j_end != -1) {

            if (op == OPERATOR_EQ) {
                if (*julian_start == -1 || j_start > *julian_start) {
                    *julian_start = j_start;
                }

                if (*julian_end == -1 || j_end < *julian_end) {
                    *julian_end = j_end;
                }
            }

            else if (op == OPERATOR_GT) {
                if (*julian_start == -1 || j_end > *julian_start) {
                    *julian_start = j_end;
                }
            }

            else if (op == OPERATOR_GE) {
                if (*julian_start == -1 || j_start > *julian_start) {
                    *julian_start = j_start;
                }
            }

            else if (op == OPERATOR_LT) {
                if (*julian_end == -1 || j_start < *julian_end) {
                    *julian_end = j_start;
                }
            }

            else if (op == OPERATOR_LE) {
                if (*julian_end == -1 || j_end < *julian_end) {
                    *julian_end = j_end;
                }
            }
        }

        #ifdef DEBUG
        if (debug_verbosity >= 2) {
            fprintf(stderr, "[CALENDAR] Predicate %d julian start: %d end: %d\n", i, *julian_start, *julian_end);
        }
        #endif
    }
}

static void getSingleJulianRange (
    struct Node *predicate,
    int *julian_start,
    int *julian_end
) {

    struct Field *field_left = (struct Field *)&predicate->children[0];
    struct Field *field_right = (struct Field *)&predicate->children[1];

    // Prep: We need field on the left and constant on the right, swap if
    // necessary
    normalisePredicate(predicate);

    // Prep: We're only looking for constants
    if (field_right->index != FIELD_CONSTANT) {
        return;
    }

    // Prep: This should already have been taken care of
    if (strcmp(field_left->text, "julian") == 0) {
        field_left->index = FIELD_ROW_INDEX;
    }

    /**************************************
     * Check what kind of predicate we have
     **************************************/

    // Single Julian value
    if (field_left->index == FIELD_ROW_INDEX) {
        *julian_start = atoi(field_right->text);
        *julian_end = *julian_start + 1;
        return;
    }

    // Exact calendar date, ordinal date, or week date
    if (
        field_left->index == COL_DATE

        // Does not support years outside range 0000 - 9999
        || field_left->index == COL_YEARDAY_STRING

        // Does not support years outside range 0000 - 9999
        || field_left->index == COL_WEEKDAY_STRING
    ) {
        struct DateTime dt = {0};
        parseDateTime(field_right->text, &dt);
        *julian_start = datetimeGetJulian(&dt);
        *julian_end = *julian_start + 1;
        return;
    }

    // Single year
    if (field_left->index == COL_YEAR) {
        struct DateTime dt = {0};

        dt.year = atoi(field_right->text);
        dt.month = 1;
        dt.day = 1;

        *julian_start = datetimeGetJulian(&dt);

        // End is exclusive
        dt.year++;

        *julian_end = datetimeGetJulian(&dt);

        return;
    }

    // Month string
    if (field_left->index == COL_MONTH_STRING) {
        // Does not support years outside range 0000 - 9999
        if (strlen(field_right->text) == 7) {
            struct DateTime dt = {0};

            dt.year = atoi(field_right->text);
            dt.month = atoi(field_right->text + 5);
            dt.day = 1;

            *julian_start = datetimeGetJulian(&dt);

            dt.month++;

            if (dt.month > 12) {
                dt.year++;
                dt.month = 1;
            }

            *julian_end = datetimeGetJulian(&dt);
        }

        return;
    }

    // Week string
    if (field_left->index == COL_WEEK_STRING) {
        // Does not support years outside range 0000 - 9999
        if (strlen(field_right->text) == 8) {
            char buffer[12] = {0};
            sprintf(buffer, "%s-1", field_right->text);

            struct DateTime dt;
            parseDateTime(buffer, &dt);

            *julian_start = datetimeGetJulian(&dt);
            *julian_end = *julian_start + 7;
        }

        return;
    }

    if (field_left->index == COL_MILLENIUM) {
        struct DateTime dt = {0};

        dt.year = atoi(field_right->text) * 1000;
        dt.month = 1;
        dt.day = 1;

        *julian_start = datetimeGetJulian(&dt);

        dt.year += 1000;

        *julian_end = datetimeGetJulian(&dt);
    }

    if (field_left->index == COL_CENTURY) {
        struct DateTime dt = {0};

        dt.year = atoi(field_right->text) * 100;
        dt.month = 1;
        dt.day = 1;

        *julian_start = datetimeGetJulian(&dt);

        dt.year += 100;

        *julian_end = datetimeGetJulian(&dt);
    }

    if (field_left->index == COL_DECADE) {
        struct DateTime dt = {0};

        dt.year = atoi(field_right->text) * 10;
        dt.month = 1;
        dt.day = 1;

        *julian_start = datetimeGetJulian(&dt);

        dt.year += 10;

        *julian_end = datetimeGetJulian(&dt);
    }
}

static int printDate (char *value, int max_length, struct DateTime date) {
    if (date.year >= 0 && date.year < 10000) {
        return snprintf(
            value,
            max_length,
            "%04d-%02d-%02d",
            date.year,
            date.month,
            date.day
        );
    } else {
        return snprintf(
            value,
            max_length,
            "%+06d-%02d-%02d",
            date.year,
            date.month,
            date.day
        );
    }
}

/**
 * Calendar can do super efficient index searches
 */
int calendar_indexSearch(
    struct DB *db,
    const char *value,
    __attribute__((unused)) int mode,
    int * output_flag
){
    if (db->field_count == 2) {
        // Dealing with an "index"

        if (db->data[0] == COL_JULIAN) {
            *output_flag = RESULT_FOUND;
            return atol(value);
        }

        if (db->data[0] == COL_DATE) {
            struct DateTime dt;

            if (parseDateTime(value, &dt)) {
                *output_flag = RESULT_FOUND;
                return datetimeGetJulian(&dt);
            }

            // fprintf(stderr, "Couldn't parse date: '%s'\n", value);

            return RESULT_NO_ROWS;
        }

        return RESULT_NO_ROWS;
    }

    // If we're doing an index search on the "real" table then is must be
    // a "Primary key" search with a julian value
    *output_flag = RESULT_FOUND;
    return atol(value);
}