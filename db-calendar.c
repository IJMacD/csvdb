#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "db.h"
#include "db-calendar.h"
#include "date.h"

char *field_names[] = {
    "julian",
    "date",
    "year",
    "month",
    "day",
    "week",
    "weekday",
    "weekyear",
    "yearday",
    "millenium",
    "century",
    "decade",
};

void getJulianRange (struct Predicate *predicates, int predicate_count, int *julian_start, int *julian_end);

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
    if (field_index == 0) {
        return snprintf(value, value_max_length, "%d", record_index);
    }

    struct DateTime dt;
    datetimeFromJulian(&dt, record_index);

    // date
    if (field_index == 1) {
        return snprintf(value, value_max_length, "%04d-%02d-%02d", dt.year, dt.month, dt.day);
    }

    // year
    if (field_index == 2) {
        return snprintf(value, value_max_length, "%04d", dt.year);
    }

    // month
    if (field_index == 3) {
        return snprintf(value, value_max_length, "%d", dt.month);
    }

    // day
    if (field_index == 4) {
        return snprintf(value, value_max_length, "%d", dt.day);
    }

    // week
    if (field_index == 5) {
        return snprintf(value, value_max_length, "%d", datetimeGetWeek(&dt));
    }

    // weekday
    if (field_index == 6) {
        return snprintf(value, value_max_length, "%d", datetimeGetWeekDay(&dt));
    }

    // weekyear
    if (field_index == 7) {
        return snprintf(value, value_max_length, "%d", datetimeGetWeekYear(&dt));
    }

    // yearday
    if (field_index == 8) {
        return snprintf(value, value_max_length, "%d", datetimeGetYearDay(&dt));
    }

    // millenium
    if (field_index == 9) {
        return snprintf(value, value_max_length, "%d", dt.year / 1000);
    }

    // century
    if (field_index == 10) {
        return snprintf(value, value_max_length, "%d", dt.year / 100);
    }

    // decade
    if (field_index == 11) {
        return snprintf(value, value_max_length, "%d", dt.year / 10);
    }

    return 0;
}

// No Indexes on CALENDAR table - all queries go through fullTableScan
int calendar_findIndex(__attribute__((unused)) struct DB *db, __attribute__((unused)) const char *table_name, __attribute__((unused)) const char *index_name, __attribute__((unused)) int index_type_flags) {
    return -1;
}

int calendar_fullTableScan (__attribute__((unused)) struct DB *db, int *result_rowids, struct Predicate *predicates, int predicate_count, int limit_value) {
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

    // Start: YES; End: NO
    if (julian >= 0 && max_julian < 0) {
        if (limit_value < 0) {
            max_julian = julian + 1;
        }
        else {
            max_julian = julian + limit_value;
        }
    }
    // Start: NO; End: YES
    else if (julian < 0 && max_julian >= 0) {
        if (limit_value < 0) {
            julian = max_julian - 1;
        }
        else {
            julian = max_julian - limit_value;
        }
    }
    // Start: YES; End: YES
    else {
        if (limit_value >= 0 && (max_julian - julian) > limit_value) {
            max_julian = julian + limit_value;
        }
    }

    // printf("%d\n", julian);

    int count = 0;

    for (; julian < max_julian; julian++) {
        result_rowids[count++] = julian;
    }

    return count;
}

void getJulianRange (struct Predicate *predicates, int predicate_count, int *julian_start, int *julian_end) {

    for (int i = 0; i < predicate_count; i++) {
        struct Predicate *p = predicates + i;

        if (strcmp(p->field, "julian") == 0 && p->op == OPERATOR_EQ) {
            *julian_start = atoi(p->value);
            *julian_end = *julian_start + 1;
            return;
        }

        if (strcmp(p->field, "date") == 0 && p->op == OPERATOR_EQ) {
            struct DateTime dt;
            parseDateTime(p->value, &dt);
            *julian_start = datetimeGetJulian(&dt);
            *julian_end = *julian_start + 1;
            return;
        }

        if (strcmp(p->field, "julian") == 0 && (p->op & OPERATOR_GT)) {
            *julian_start = atoi(p->value);

            if (!(p->op & OPERATOR_EQ)) {
                (*julian_start)++;
            }
        }

        if (strcmp(p->field, "julian") == 0 && (p->op & OPERATOR_LT)) {
            *julian_end = atoi(p->value);

            // End is exclusive
            if (p->op & OPERATOR_EQ) {
                (*julian_end)++;
            }
        }

        if (strcmp(p->field, "date") == 0 && (p->op & OPERATOR_GT)) {
            struct DateTime dt;
            parseDateTime(p->value, &dt);
            *julian_start = datetimeGetJulian(&dt);

            if (!(p->op & OPERATOR_EQ)) {
                (*julian_start)++;
            }
        }

        if (strcmp(p->field, "date") == 0 && (p->op & OPERATOR_LT)) {
            struct DateTime dt;
            parseDateTime(p->value, &dt);
            *julian_end = datetimeGetJulian(&dt);

            // End is exclusive
            if (p->op & OPERATOR_EQ) {
                (*julian_end)++;
            }
        }
    }

}