#include <stdlib.h>
#include <string.h>
#include <limits.h>

#include "query.h"
#include "function.h"
#include "date.h"
#include "util.h"

int evaluateFunction(char * output, int function, char **values, __attribute__((unused)) int value_count) {

    // NULL output from VFS
    if (values[0][0] == 0) {
        output[0] = '\0';
        return 0;
    }

    if (function == FUNC_UNITY) {
        sprintf(output, "%s", values[0]);
    }
    else if (function == FUNC_CHR) {
        int codepoint = atoi(values[0]);
        writeUTF8(values[0], codepoint);

        sprintf(output, "%s", values[0]);
    }
    else if (function == FUNC_TO_HEX) {
        int val = atoi(values[0]);

        if (val < 0) {
            sprintf(output, "-0x%x", abs(val));
        } else if (val < 0x100) {
            sprintf(output, "0x%02x", val);
        } else if (val < 0x10000) {
            sprintf(output, "0x%04x", val);
        } else {
            sprintf(output, "0x%x", val);
        }
    }
    else if (function == FUNC_RANDOM) {
        sprintf(output, "%d", rand());
    }
    else if ((function & MASK_FUNC_FAMILY) == FUNC_FAM_STRING) {
        if (function == FUNC_LENGTH) {
            int len = strlen(values[0]);
            sprintf(output, "%d", len);
        }
        else if (function == FUNC_LEFT) {
            int count = atoi(values[1]);
            int len = strlen(values[0]);

            if (len > count) {
                strncpy(output, values[0], count);
                output[count] = '\0';
            } else {
                sprintf(output, "%s", values[0]);
            }
        }
        else if (function == FUNC_RIGHT) {
            int count = atoi(values[1]);
            int len = strlen(values[0]);

            if (len > count) {
                sprintf(output, "%s", values[0] + len - count);
            } else {
                sprintf(output, "%s", values[0]);
            }
        }
    }
    else if ((function & MASK_FUNC_FAMILY) == FUNC_FAM_EXTRACT) {
        struct DateTime dt;

        if (!parseDateTime(values[0], &dt)) {
            return 0;
        }

        if (function == FUNC_EXTRACT_YEAR){
            sprintf(output, "%d", dt.year);
        }
        else if (function == FUNC_EXTRACT_MONTH) {
            sprintf(output, "%d", dt.month);
        }
        else if (function == FUNC_EXTRACT_DAY) {
            sprintf(output, "%d", dt.day);
        }
        else if (function == FUNC_EXTRACT_WEEK) {
            sprintf(output, "%d", datetimeGetWeek(&dt));
        }
        else if (function == FUNC_EXTRACT_WEEKYEAR) {
            sprintf(output, "%d", datetimeGetWeekYear(&dt));
        }
        else if (function == FUNC_EXTRACT_WEEKDAY) {
            sprintf(output, "%d", datetimeGetWeekDay(&dt));
        }
        else if (function == FUNC_EXTRACT_HEYEAR) {
            sprintf(output, "%d", dt.year + 10000);
        }
        else if (function == FUNC_EXTRACT_YEARDAY) {
            sprintf(output, "%d", datetimeGetYearDay(&dt));
        }
        else if (function == FUNC_EXTRACT_MILLENNIUM) {
            sprintf(output, "%d", dt.year / 1000);
        }
        else if (function == FUNC_EXTRACT_CENTURY) {
            sprintf(output, "%d", dt.year / 100);
        }
        else if (function == FUNC_EXTRACT_DECADE) {
            sprintf(output, "%d", dt.year / 10);
        }
        else if (function == FUNC_EXTRACT_QUARTER) {
            sprintf(output, "%d", (dt.month - 1) / 3 + 1);
        }
        else if (function == FUNC_EXTRACT_JULIAN) {
            sprintf(output, "%d", datetimeGetJulian(&dt));
        }
        else if (function == FUNC_EXTRACT_DATE) {
            sprintf(output, "%04d-%02d-%02d", dt.year, dt.month, dt.day);
        }
        else if (function == FUNC_EXTRACT_DATETIME) {
            sprintf(output, "%04d-%02d-%02dT%02d:%02d:%02d", dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second);
        }
        else if (function == FUNC_EXTRACT_MONTH_STRING) {
            sprintf(output, "%04d-%02d", dt.year, dt.month);
        }
        else if (function == FUNC_EXTRACT_WEEK_STRING) {
            sprintf(output, "%04d-W%02d", datetimeGetWeekYear(&dt), datetimeGetWeek(&dt));
        }
        else if (function == FUNC_EXTRACT_YEARDAY_STRING) {
            sprintf(output, "%04d-%03d", dt.year, datetimeGetYearDay(&dt));
        }
        else {
            sprintf(output, "BADEXTRACT");
        }
    }
    else if (function == FUNC_DATE_ADD) {
        struct DateTime dt1;
        struct DateTime dt2;

        if (!parseDateTime(values[0], &dt1)) {
            return 0;
        }

        int diff = atoi(values[1]);

        int julian1 = datetimeGetJulian(&dt1);
        int julian2 = julian1 + diff;

        datetimeFromJulian(&dt2, julian2);

        return sprintf(output, "%04d-%02d-%02d", dt2.year, dt2.month, dt2.day);
    }
    else if (function == FUNC_DATE_SUB) {
        struct DateTime dt1;
        struct DateTime dt2;

        if (!parseDateTime(values[0], &dt1)) {
            return 0;
        }

        int diff = atoi(values[1]);

        int julian1 = datetimeGetJulian(&dt1);
        int julian2 = julian1 - diff;

        datetimeFromJulian(&dt2, julian2);

        return sprintf(output, "%04d-%02d-%02d", dt2.year, dt2.month, dt2.day);
    }
    else if (function == FUNC_DATE_DIFF) {
        struct DateTime dt1;
        struct DateTime dt2;

        if (!parseDateTime(values[0], &dt1)) {
            return 0;
        }

        if (!parseDateTime(values[1], &dt2)) {
            return 0;
        }

        int julian1 = datetimeGetJulian(&dt1);
        int julian2 = datetimeGetJulian(&dt2);

        return sprintf(output, "%d", julian1 - julian2);
    }
    else {
        return -1;
    }

    return 0;
}

int evaluateAggregateFunction (char * output, struct Table *tables, __attribute__((unused)) int table_count, struct ColumnNode *column, struct RowList * row_list) {
    struct Field *field = column->fields;

    char value[MAX_VALUE_LENGTH];

    if ((column->function & MASK_FUNC_FAMILY) != FUNC_FAM_AGG) {
        return -1;
    }

    if (column->function == FUNC_AGG_COUNT) {
        int count = 0;

        for (int i = 0; i < row_list->row_count; i++) {
            int rowid = getRowID(row_list, field->table_id, i);

            // Count up the non-NULL values
            if (getRecordValue(tables[field->table_id].db, rowid, field->index, value, MAX_VALUE_LENGTH) > 0) {
                count++;
            }
        }

        sprintf(output, "%d", count);

        return 0;
    }

    if (column->function == FUNC_AGG_MIN) {
        int min = INT_MAX;

        for (int i = 0; i < row_list->row_count; i++) {
            int rowid = getRowID(row_list, field->table_id, i);

            // Only consider the non-NULL values
            if (getRecordValue(tables[field->table_id].db, rowid, field->index, value, MAX_VALUE_LENGTH) > 0) {
                int v = atoi(value);

                if (v < min) min = v;
            }
        }

        if (min < INT_MAX) {
            sprintf(output, "%d", min);
        }

        return 0;
    }

    if (column->function == FUNC_AGG_MAX) {
        int max = INT_MIN;

        for (int i = 0; i < row_list->row_count; i++) {
            int rowid = getRowID(row_list, field->table_id, i);

            // Only consider the non-NULL values
            if (getRecordValue(tables[field->table_id].db, rowid, field->index, value, MAX_VALUE_LENGTH) > 0) {
                int v = atoi(value);

                if (v > max) max = v;
            }
        }

        if (max > INT_MIN) {
            sprintf(output, "%d", max);
        }

        return 0;
    }

    if (column->function == FUNC_AGG_SUM) {
        int sum = 0;
        int non_null = 0;

        for (int i = 0; i < row_list->row_count; i++) {
            int rowid = getRowID(row_list, field->table_id, i);

            // Sum the non-NULL values
            if (getRecordValue(tables[field->table_id].db, rowid, field->index, value, MAX_VALUE_LENGTH) > 0) {
                non_null = 1;
                sum += atoi(value);
            }
        }

        // If *all* rows are NULL then the result is NULL
        if (non_null) {
            sprintf(output, "%d", sum);
        }
        else {
            output[0] = '\0';
        }

        return 0;
    }

    if (column->function == FUNC_AGG_AVG) {
        int count = 0;
        int sum = 0;

        for (int i = 0; i < row_list->row_count; i++) {
            int rowid = getRowID(row_list, field->table_id, i);

            // Count up the non-NULL values
            if (getRecordValue(tables[field->table_id].db, rowid, field->index, value, MAX_VALUE_LENGTH) > 0) {
                count++;

                sum += atoi(value);
            }
        }

        if (count > 0) {
            sprintf(output, "%d", sum / count);
        }
        else {
            output[0] = '\0';
        }


        return 0;
    }

    if (column->function == FUNC_AGG_LISTAGG) {

        int have_prev = 0;

        for (int i = 0; i < row_list->row_count; i++) {
            int rowid = getRowID(row_list, field->table_id, i);

            // Count up the non-NULL values
            if (getRecordValue(tables[field->table_id].db, rowid, field->index, value, MAX_VALUE_LENGTH) > 0) {
                if (have_prev == 1) {
                    sprintf(output++, ",");
                }

                output += sprintf(output, "%s", value);

                have_prev = 1;
            }
        }

        return 0;
    }

    return -1;
}
