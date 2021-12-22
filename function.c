#include <stdlib.h>
#include <limits.h>

#include "function.h"

int evaluateFunction(FILE *f, struct DB *db, struct ResultColumn *column, int record_index) {
    char value[VALUE_MAX_LENGTH];

    if (getRecordValue(db, record_index, column->field, value, VALUE_MAX_LENGTH) > 0) {

        if (column->function == FUNC_UNITY) {
            fprintf(f, "%s", value);
        }
        else if ((column->function & MASK_FUNC_FAMILY) == FUNC_EXTRACT) {
            struct DateTime dt;

            if (!parseDateTime(value, &dt)) {
                return 0;
            }

            if (column->function == FUNC_EXTRACT_YEAR){
                fprintf(f, "%d", dt.year);
            }
            else if (column->function == FUNC_EXTRACT_MONTH) {
                fprintf(f, "%d", dt.month);
            }
            else if (column->function == FUNC_EXTRACT_DAY) {
                fprintf(f, "%d", dt.day);
            }
            else if (column->function == FUNC_EXTRACT_WEEK) {
                fprintf(f, "%d", datetimeGetWeek(&dt));
            }
            else if (column->function == FUNC_EXTRACT_WEEKYEAR) {
                fprintf(f, "%d", datetimeGetWeekYear(&dt));
            }
            else if (column->function == FUNC_EXTRACT_WEEKDAY) {
                fprintf(f, "%d", datetimeGetWeekDay(&dt));
            }
            else if (column->function == FUNC_EXTRACT_HEYEAR) {
                fprintf(f, "%d", dt.year + 10000);
            }
            else if (column->function == FUNC_EXTRACT_YEARDAY) {
                fprintf(f, "%d", datetimeGetYearDay(&dt));
            }
            else if (column->function == FUNC_EXTRACT_MILLENNIUM) {
                fprintf(f, "%d", dt.year / 1000);
            }
            else if (column->function == FUNC_EXTRACT_CENTURY) {
                fprintf(f, "%d", dt.year / 100);
            }
            else if (column->function == FUNC_EXTRACT_DECADE) {
                fprintf(f, "%d", dt.year / 10);
            }
            else if (column->function == FUNC_EXTRACT_QUARTER) {
                fprintf(f, "%d", (dt.month - 1) / 3 + 1);
            }
            else if (column->function == FUNC_EXTRACT_JULIAN) {
                fprintf(f, "%d", datetimeGetJulian(&dt));
            }
            else if (column->function == FUNC_EXTRACT_DATE) {
                fprintf(f, "%04d-%02d-%02d", dt.year, dt.month, dt.day);
            }
            else if (column->function == FUNC_EXTRACT_DATETIME) {
                fprintf(f, "%04d-%02d-%02dT%02d:%02d:%02d", dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second);
            }
            else {
                fprintf(f, "BADEXTRACT");
            }
        }
        else {
            return -1;
        }
    }

    return 0;
}

int evaluateAggregateFunction (FILE *f, struct DB *db, struct ResultColumn *column, struct RowList * row_list) {
    char value[VALUE_MAX_LENGTH];

    if ((column->function & MASK_FUNC_FAMILY) != FUNC_AGG) {
        return -1;
    }

    if (column->function == FUNC_AGG_COUNT) {
        int count = 0;

        for (int i = 0; i < row_list->row_count; i++) {
            int rowid = getRowID(row_list, 0, i);

            // Count up the non-NULL values
            if (getRecordValue(db, rowid, column->field, value, VALUE_MAX_LENGTH) > 0) {
                count++;
            }
        }

        fprintf(f, "%d", count);

        return 0;
    }

    if (column->function == FUNC_AGG_MIN) {
        int min = INT_MAX;

        for (int i = 0; i < row_list->row_count; i++) {
            int rowid = getRowID(row_list, 0, i);

            // Only consider the non-NULL values
            if (getRecordValue(db, rowid, column->field, value, VALUE_MAX_LENGTH) > 0) {
                int v = atoi(value);

                if (v < min) min = v;
            }
        }

        if (min < INT_MAX) {
            fprintf(f, "%d", min);
        }

        return 0;
    }

    if (column->function == FUNC_AGG_MAX) {
        int max = INT_MIN;

        for (int i = 0; i < row_list->row_count; i++) {
            int rowid = getRowID(row_list, 0, i);

            // Only consider the non-NULL values
            if (getRecordValue(db, rowid, column->field, value, VALUE_MAX_LENGTH) > 0) {
                int v = atoi(value);

                if (v > max) max = v;
            }
        }

        if (max > INT_MIN) {
            fprintf(f, "%d", max);
        }

        return 0;
    }

    if (column->function == FUNC_AGG_AVG) {
        int count = 0;
        int sum = 0;

        for (int i = 0; i < row_list->row_count; i++) {
            int rowid = getRowID(row_list, 0, i);

            // Count up the non-NULL values
            if (getRecordValue(db, rowid, column->field, value, VALUE_MAX_LENGTH) > 0) {
                count++;

                sum += atoi(value);
            }
        }

        fprintf(f, "%d", sum / count);

        return 0;
    }

    return -1;
}