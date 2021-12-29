#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "db.h"
#include "db-calendar.h"
#include "date.h"
#include "predicates.h"
#include "result.h"

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

char *field_names[] = {
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
};

const int month_lengths[] = {31,28,31,30,31,30,31,31,30,31,30,31};

void getJulianRange (struct Predicate *predicates, int predicate_count, int *julian_start, int *julian_end);

int printDate (char *value, int max_length, struct DateTime date);

int calendar_openDB (struct DB *db, const char *filename) {
    if (strcmp(filename, "CALENDAR") != 0) {
        return -1;
    }

    db->vfs = VFS_CALENDAR;
    db->record_count = 100000;
    db->line_indices = NULL;
    db->field_count = sizeof(field_names) / sizeof(field_names[0]);

    return 0;
}

void calendar_closeDB (__attribute__((unused)) struct DB *db) {}

int calendar_getFieldIndex (__attribute__((unused)) struct DB *db, const char *field) {
    int l = sizeof(field_names) / sizeof(field_names[0]);

    for (int i = 0; i < l; i++) {
        if (strcmp(field, field_names[i]) == 0) {
            return i;
        }
    }

    return -1;
}

char *calendar_getFieldName (__attribute__((unused)) struct DB *db, int field_index) {
    return field_names[field_index];
}

int calendar_getRecordValue (__attribute__((unused)) struct DB *db, int record_index, int field_index, char *value, size_t value_max_length) {
    // julian
    if (field_index == COL_JULIAN) {
        return snprintf(value, value_max_length, "%d", record_index);
    }

    struct DateTime dt;
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
        return snprintf(value, value_max_length, "%d", datetimeGetWeekYear(&dt));
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
        return snprintf(value, value_max_length, "%d", isLeapYear(dt.year) ? 1 : 0);
    }

    // weekdayInMonth
    if (field_index == COL_WEEKDAY_IN_MONTH) {
        return snprintf(value, value_max_length, "%d", (dt.day - 1) / 7 + 1);
    }

    // isWeekend
    if (field_index == COL_IS_WEEKEND) {
        return snprintf(value, value_max_length, "%d", datetimeGetWeekDay(&dt) >= 6 ? 1 : 0);
    }

    return 0;
}

// No Indexes on CALENDAR table - all queries go through fullTableScan
int calendar_findIndex(__attribute__((unused)) struct DB *db, __attribute__((unused)) const char *table_name, __attribute__((unused)) const char *index_name, __attribute__((unused)) int index_type_flags) {
    return -1;
}

int calendar_fullTableScan (struct DB *db, struct RowList *row_list, struct Predicate *predicates, int predicate_count, int limit_value) {
    int julian = -1, max_julian = -1;

    // Try to get range from predicates
    getJulianRange(predicates, predicate_count, &julian, &max_julian);

    // Fallback to current date
    if (julian < 0 && max_julian < 0) {
        time_t t = time(NULL);
        struct tm *local = localtime(&t);

        struct DateTime dt = {0};
        dt.year = local->tm_year + 1900;
        dt.month = local->tm_mon + 1;
        dt.day = local->tm_mday;

        // printf("%04d-%02d-%02d\n", dt.year, dt.month, dt.day);

        julian = datetimeGetJulian(&dt);
    }

    // Maxiumums
    // Start: YES; End: NO
    if (julian >= 0 && max_julian < 0) {
        max_julian = julian + db->record_count;
    }
    // Start: NO; End: YES
    else if (julian < 0 && max_julian >= 0) {
        julian = max_julian - db->record_count;
    }

    // printf("%d\n", julian);

    // No limit means we'll use the limit defined for the VFS
    // (hopefully there are enough predicates that we won't have that many results though)
    if (limit_value < 0) {
        limit_value = db->record_count;
    }

    int count = 0;
    char value[VALUE_MAX_LENGTH];

    for (; julian < max_julian; julian++) {
        int matching = 1;

        // Perform filtering if necessary
        for (int j = 0; j < predicate_count && matching; j++) {
            struct Predicate *predicate = predicates + j;

            // Note: Could factor out next line if there are performance issues
            int predicate_field_index = calendar_getFieldIndex(db, predicate->left.text);
            calendar_getRecordValue(db, julian, predicate_field_index, value, VALUE_MAX_LENGTH);

            if (!evaluateExpression(predicate->op, value, predicate->right.text)) {
                matching = 0;
                break;
            }
        }

        if (matching) {
            // Add to result set
            appendRowID(row_list, julian);
        }

        // Implement early exit FETCH FIRST/LIMIT for cases with no ORDER clause
        if (count >= limit_value) {
            break;
        }
    }

    return count;
}

void getJulianRange (struct Predicate *predicates, int predicate_count, int *julian_start, int *julian_end) {

    for (int i = 0; i < predicate_count; i++) {
        struct Predicate *p = predicates + i;

        // An exact Julian
        if (strcmp(p->left.text, "julian") == 0 && p->op == OPERATOR_EQ) {
            *julian_start = atoi(p->right.text);
            *julian_end = *julian_start + 1;
            return;
        }

        // An exact date
        if (strcmp(p->left.text, "date") == 0 && p->op == OPERATOR_EQ) {
            struct DateTime dt;
            parseDateTime(p->right.text, &dt);
            *julian_start = datetimeGetJulian(&dt);
            *julian_end = *julian_start + 1;
            return;
        }

        // Dates after a specific Julian
        if (strcmp(p->left.text, "julian") == 0 && (p->op & OPERATOR_GT)) {
            *julian_start = atoi(p->right.text);

            if (!(p->op & OPERATOR_EQ)) {
                (*julian_start)++;
            }
        }

        // Dates before a specific Julian
        if (strcmp(p->left.text, "julian") == 0 && (p->op & OPERATOR_LT)) {
            *julian_end = atoi(p->right.text);

            // End is exclusive
            if (p->op & OPERATOR_EQ) {
                (*julian_end)++;
            }
        }

        // Dates after a specific date
        if (strcmp(p->left.text, "date") == 0 && (p->op & OPERATOR_GT)) {
            struct DateTime dt;
            parseDateTime(p->right.text, &dt);
            *julian_start = datetimeGetJulian(&dt);

            if (!(p->op & OPERATOR_EQ)) {
                (*julian_start)++;
            }
        }

        // Dates before a specific date
        if (strcmp(p->left.text, "date") == 0 && (p->op & OPERATOR_LT)) {
            struct DateTime dt;
            parseDateTime(p->right.text, &dt);
            *julian_end = datetimeGetJulian(&dt);

            // End is exclusive
            if (p->op & OPERATOR_EQ) {
                (*julian_end)++;
            }
        }

        // All dates in the given year
        if (strcmp(p->left.text, "year") == 0 && p->op == OPERATOR_EQ) {
            struct DateTime dt;

            dt.year = atoi(p->right.text);
            dt.month = 1;
            dt.day = 1;

            *julian_start = datetimeGetJulian(&dt);

            // End is exclusive
            dt.year++;

            *julian_end = datetimeGetJulian(&dt);
        }

        // All dates up to and including the given year
        if (strcmp(p->left.text, "year") == 0 && p->op == OPERATOR_LE) {
            struct DateTime dt;

            // End is exclusive
            dt.year = atoi(p->right.text) + 1;
            dt.month = 1;
            dt.day = 1;

            *julian_end = datetimeGetJulian(&dt);
        }

        // All dates starting from the beginning of the given year
        if (strcmp(p->left.text, "year") == 0 && p->op == OPERATOR_GE) {
            struct DateTime dt;

            dt.year = atoi(p->right.text);
            dt.month = 1;
            dt.day = 1;

            *julian_start = datetimeGetJulian(&dt);
        }
    }

}

int printDate (char *value, int max_length, struct DateTime date) {
    if (date.year >= 0 && date.year < 10000) {
        return snprintf(value, max_length, "%04d-%02d-%02d", date.year, date.month, date.day);
    } else {
        return snprintf(value, max_length, "%+06d-%02d-%02d", date.year, date.month, date.day);
    }
}