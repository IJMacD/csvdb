#include <stdio.h>
#include <string.h>

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
    "weekday"
};


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

int calendar_getRecordValue (__attribute__((unused)) struct DB *db, int record_index, int field_index, char *value, __attribute__((unused)) size_t value_max_length) {
    // julian
    if (field_index == 0) {
        return sprintf(value, "%d", record_index);
    }

    struct DateTime dt;
    datetimeFromJulian(&dt, record_index);

    // date
    if (field_index == 1) {
        return sprintf(value, "%04d-%02d-%02d", dt.year, dt.month, dt.day);
    }

    // year
    if (field_index == 2) {
        return sprintf(value, "%04d", dt.year);
    }

    // month
    if (field_index == 3) {
        return sprintf(value, "%d", dt.month);
    }

    // day
    if (field_index == 4) {
        return sprintf(value, "%d", dt.day);
    }

    // week
    if (field_index == 5) {
        return sprintf(value, "%d", datetimeGetWeek(&dt));
    }

    // weekday
    if (field_index == 6) {
        return sprintf(value, "%d", datetimeGetWeekDay(&dt));
    }

    return 0;
}

// No Indexes on CALENDAR table - all queries go through fullTableScan
int calendar_findIndex(__attribute__((unused)) struct DB *db, __attribute__((unused)) const char *table_name, __attribute__((unused)) const char *index_name, __attribute__((unused)) int index_type_flags) {
    return -1;
}

int calendar_fullTableScan (__attribute__((unused)) struct DB *db, int *result_rowids, __attribute__((unused)) struct Predicate *predicates, __attribute__((unused)) int predicate_count, __attribute__((unused)) int limit_value, __attribute__((unused)) int offset_value) {
    *result_rowids = 2459417;
    return 1;
}