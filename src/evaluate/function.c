#include <stdlib.h>
#include <string.h>
#include <limits.h>

#include "../structs.h"
#include "../functions/util.h"
#include "../functions/date.h"
#include "../query/result.h"
#include "../db/db.h"

/**
 * @brief
 *
 * @param output
 * @param function
 * @param values
 * @return int number of bytes written
 */
int evaluateFunction(
    char * output,
    int function,
    char **values,
    int value_count
) {

    // Random takes 0 parameters
    if (function == FUNC_RANDOM) {
        return sprintf(output, "%d", rand());
    }

    // All other functions take at least 1 parameter. If the field is NULL then
    // the whole function evaluates to NULL.
    if (values[0][0] == 0) {
        output[0] = '\0';
        return 0;
    }

    if (function == FUNC_UNITY) {
        return sprintf(output, "%s", values[0]);
    }
    else if (function == FUNC_CHR) {
        int codepoint = atoi(values[0]);
        writeUTF8(values[0], codepoint);

        return sprintf(output, "%s", values[0]);
    }
    else if (function == FUNC_TO_HEX) {
        int val = atoi(values[0]);

        if (val < 0) {
            return sprintf(output, "-0x%x", abs(val));
        } else if (val < 0x100) {
            return sprintf(output, "0x%02x", val);
        } else if (val < 0x10000) {
            return sprintf(output, "0x%04x", val);
        } else {
            return sprintf(output, "0x%x", val);
        }
    }
    else if (function == FUNC_ADD) {
        long val1 = atol(values[0]);
        long val2 = atol(values[1]);

        return sprintf(output, "%ld", val1 + val2);
    }
    else if (function == FUNC_SUB) {
        long val1 = atol(values[0]);
        long val2 = atol(values[1]);

        return sprintf(output, "%ld", val1 - val2);
    }
    else if (function == FUNC_MUL) {
        long val1 = atol(values[0]);
        long val2 = atol(values[1]);

        return sprintf(output, "%ld", val1 * val2);
    }
    else if (function == FUNC_DIV) {
        long val1 = atol(values[0]);
        long val2 = atol(values[1]);

        if (val2 == 0) {
            output[0] = '\0';
            return 0;
        }

        return sprintf(output, "%ld", val1 / val2);
    }
    else if (function == FUNC_MOD) {
        long val1 = atol(values[0]);
        long val2 = atol(values[1]);

        if (val2 == 0) {
            output[0] = '\0';
            return 0;
        }

        return sprintf(output, "%ld", val1 % val2);
    }
    else if (function == FUNC_POW) {
        long val1 = atol(values[0]);
        long val2 = atol(values[1]);

        if (val2 < 0) {
            return sprintf(output, "0");
        }

        long prod = 1;

        for (int i = 0; i < val2; i++) {
            prod *= val1;
        }

        return sprintf(output, "%ld", prod);
    }
    else if ((function & MASK_FUNC_FAMILY) == FUNC_FAM_STRING) {
        if (function == FUNC_LENGTH) {
            int len = strlen(values[0]);
            return sprintf(output, "%d", len);
        }
        else if (function == FUNC_LEFT) {
            int count = atoi(values[1]);
            int len = strlen(values[0]);

            if (len > count) {
                strncpy(output, values[0], count);
                output[count] = '\0';
                return count;
            } else {
                return sprintf(output, "%s", values[0]);
            }
        }
        else if (function == FUNC_RIGHT) {
            int count = atoi(values[1]);
            int len = strlen(values[0]);

            if (len > count) {
                return sprintf(output, "%s", values[0] + len - count);
            } else {
                return sprintf(output, "%s", values[0]);
            }
        }
        else if (function == FUNC_CONCAT) {
            int count = 0;

            for (int i = 0; i < value_count; i++) {
                int size = sprintf(output, "%s", values[i]);
                count += size;
                output += size;
            }

            return count;
        }
    }
    else if ((function & MASK_FUNC_FAMILY) == FUNC_FAM_EXTRACT) {
        struct DateTime dt;

        if (!parseDateTime(values[0], &dt)) {
            return 0;
        }

        if (function == FUNC_EXTRACT_YEAR){
            return sprintf(output, "%d", dt.year);
        }
        else if (function == FUNC_EXTRACT_MONTH) {
            return sprintf(output, "%d", dt.month);
        }
        else if (function == FUNC_EXTRACT_DAY) {
            return sprintf(output, "%d", dt.day);
        }
        else if (function == FUNC_EXTRACT_WEEK) {
            return sprintf(output, "%d", datetimeGetWeek(&dt));
        }
        else if (function == FUNC_EXTRACT_WEEKYEAR) {
            return sprintf(output, "%d", datetimeGetWeekYear(&dt));
        }
        else if (function == FUNC_EXTRACT_WEEKDAY) {
            return sprintf(output, "%d", datetimeGetWeekDay(&dt));
        }
        else if (function == FUNC_EXTRACT_HEYEAR) {
            return sprintf(output, "%d", dt.year + 10000);
        }
        else if (function == FUNC_EXTRACT_YEARDAY) {
            return sprintf(output, "%d", datetimeGetYearDay(&dt));
        }
        else if (function == FUNC_EXTRACT_MILLENNIUM) {
            return sprintf(output, "%d", dt.year / 1000);
        }
        else if (function == FUNC_EXTRACT_CENTURY) {
            return sprintf(output, "%d", dt.year / 100);
        }
        else if (function == FUNC_EXTRACT_DECADE) {
            return sprintf(output, "%d", dt.year / 10);
        }
        else if (function == FUNC_EXTRACT_QUARTER) {
            return sprintf(output, "%d", (dt.month - 1) / 3 + 1);
        }
        else if (function == FUNC_EXTRACT_JULIAN) {
            return sprintf(output, "%d", datetimeGetJulian(&dt));
        }
        else if (function == FUNC_EXTRACT_DATE) {
            return sprintf(output, "%04d-%02d-%02d", dt.year, dt.month, dt.day);
        }
        else if (function == FUNC_EXTRACT_DATETIME) {
            return sprintf(
                output,
                "%04d-%02d-%02dT%02d:%02d:%02d",
                dt.year,
                dt.month,
                dt.day,
                dt.hour,
                dt.minute,
                dt.second
            );
        }
        else if (function == FUNC_EXTRACT_MONTH_STRING) {
            return sprintf(output, "%04d-%02d", dt.year, dt.month);
        }
        else if (function == FUNC_EXTRACT_WEEK_STRING) {
            return sprintf(
                output,
                "%04d-W%02d",
                datetimeGetWeekYear(&dt),
                datetimeGetWeek(&dt)
            );
        }
        else if (function == FUNC_EXTRACT_YEARDAY_STRING) {
            return sprintf(output, "%04d-%03d", dt.year, datetimeGetYearDay(&dt));
        }
        else {
            return sprintf(output, "BADEXTRACT");
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

int evaluateAggregateFunction (
    char * output,
    struct Table *tables,
    struct Node *node,
    struct RowList * row_list
) {

    char value[MAX_VALUE_LENGTH];

    if ((node->function & MASK_FUNC_FAMILY) != FUNC_FAM_AGG) {
        return -1;
    }

    if (node->function == FUNC_AGG_COUNT) {
        int count = 0;

        for (int i = 0; i < row_list->row_count; i++) {
            int rowid = getRowID(row_list, node->field.table_id, i);

            // Count up the non-NULL values
            if (
                getRecordValue(
                    tables[node->field.table_id].db,
                    rowid,
                    node->field.index,
                    value,
                    MAX_VALUE_LENGTH
                ) > 0
            ) {
                count++;
            }
        }

        return sprintf(output, "%d", count);
    }

    if (node->function == FUNC_AGG_MIN) {
        int min = INT_MAX;

        for (int i = 0; i < row_list->row_count; i++) {
            int rowid = getRowID(row_list, node->field.table_id, i);

            // Only consider the non-NULL values
            if (
                getRecordValue(
                    tables[node->field.table_id].db,
                    rowid,
                    node->field.index,
                    value,
                    MAX_VALUE_LENGTH
                ) > 0
            ) {
                int v = atoi(value);

                if (v < min) min = v;
            }
        }

        if (min < INT_MAX) {
            return sprintf(output, "%d", min);
        }

        return 0;
    }

    if (node->function == FUNC_AGG_MAX) {
        int max = INT_MIN;

        for (int i = 0; i < row_list->row_count; i++) {
            int rowid = getRowID(row_list, node->field.table_id, i);

            // Only consider the non-NULL values
            if (
                getRecordValue(
                    tables[node->field.table_id].db,
                    rowid,
                    node->field.index,
                    value,
                    MAX_VALUE_LENGTH
                ) > 0
            ) {
                int v = atoi(value);

                if (v > max) max = v;
            }
        }

        if (max > INT_MIN) {
            return sprintf(output, "%d", max);
        }

        return 0;
    }

    if (node->function == FUNC_AGG_SUM) {
        int sum = 0;
        int non_null = 0;

        for (int i = 0; i < row_list->row_count; i++) {
            int rowid = getRowID(row_list, node->field.table_id, i);

            // Sum the non-NULL values
            if (
                getRecordValue(
                    tables[node->field.table_id].db,
                    rowid,
                    node->field.index,
                    value,
                    MAX_VALUE_LENGTH
                ) > 0
            ) {
                non_null = 1;
                sum += atoi(value);
            }
        }

        // If *all* rows are NULL then the result is NULL
        if (non_null) {
            return sprintf(output, "%d", sum);
        }

        output[0] = '\0';

        return 0;
    }

    if (node->function == FUNC_AGG_AVG) {
        int count = 0;
        int sum = 0;

        for (int i = 0; i < row_list->row_count; i++) {
            int rowid = getRowID(row_list, node->field.table_id, i);

            // Count up the non-NULL values
            if (
                getRecordValue(
                    tables[node->field.table_id].db,
                    rowid,
                    node->field.index,
                    value,
                    MAX_VALUE_LENGTH
                ) > 0
            ) {
                count++;

                sum += atoi(value);
            }
        }

        if (count > 0) {
            return sprintf(output, "%d", sum / count);
        }

        output[0] = '\0';

        return 0;
    }

    if (node->function == FUNC_AGG_LISTAGG) {

        int have_prev = 0;

        for (int i = 0; i < row_list->row_count; i++) {
            int rowid = getRowID(row_list, node->field.table_id, i);

            // Count up the non-NULL values
            if (
                getRecordValue(
                    tables[node->field.table_id].db,
                    rowid,
                    node->field.index,
                    value,
                    MAX_VALUE_LENGTH
                ) > 0
            ) {
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