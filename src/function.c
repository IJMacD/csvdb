#include <stdlib.h>
#include <string.h>
#include <limits.h>

#include "query.h"
#include "function.h"
#include "date.h"
#include "util.h"

int evaluateFunction(char * output, struct DB *db, struct ColumnNode *column, int record_index) {
    char value[VALUE_MAX_LENGTH] = {0};
    int result;

    if (column->field == FIELD_CONSTANT) {
        strcpy(value, column->text);
        result = 1;
    }
    else {
        result = getRecordValue(db, record_index, column->field, value, VALUE_MAX_LENGTH) > 0;
    }

    if (result > 0) {

        if (column->function == FUNC_UNITY) {
            sprintf(output, "%s", value);
        }
        else if (column->function == FUNC_CHR) {
            int codepoint = atoi(value);
            writeUTF8(value, codepoint);

            sprintf(output, "%s", value);
        }
        else if (column->function == FUNC_TO_HEX) {
            int val = atoi(value);

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
        else if (column->function == FUNC_RANDOM) {
            sprintf(output, "%d", rand());
        }
        else if ((column->function & MASK_FUNC_FAMILY) == FUNC_FAM_STRING) {
            if (column->function == FUNC_LENGTH) {
                int len = strlen(value);
                sprintf(output, "%d", len);
            }
            else if (column->function == FUNC_LEFT) {
                // Both field name and length stored in same array
                // Layout:
                // <field>\0 <count>)

                int field_len = strlen(column->text);

                if (field_len > FIELD_MAX_LENGTH) {
                    fprintf(stderr, "Missing count from LEFT: %s\n", column->text);
                    exit(-1);
                }

                int count = atoi(column->text + field_len + 1);
                int len = strlen(value);

                if (len > count) {
                    strncpy(output, value, count);
                    output[count] = '\0';
                } else {
                    sprintf(output, "%s", value);
                }
            }
            else if (column->function == FUNC_RIGHT) {
                // Both field name and length stored in same array
                // Layout:
                // <field>\0 <count>)

                int field_len = strlen(column->text);

                if (field_len > FIELD_MAX_LENGTH) {
                    fprintf(stderr, "Missing count from RIGHT: %s\n", column->text);
                    exit(-1);
                }

                int count = atoi(column->text + field_len + 1);
                int len = strlen(value);

                if (len > count) {
                    sprintf(output, "%s", value + len - count);
                } else {
                    sprintf(output, "%s", value);
                }
            }
        }
        else if ((column->function & MASK_FUNC_FAMILY) == FUNC_FAM_EXTRACT) {
            struct DateTime dt;

            if (!parseDateTime(value, &dt)) {
                return 0;
            }

            if (column->function == FUNC_EXTRACT_YEAR){
                sprintf(output, "%d", dt.year);
            }
            else if (column->function == FUNC_EXTRACT_MONTH) {
                sprintf(output, "%d", dt.month);
            }
            else if (column->function == FUNC_EXTRACT_DAY) {
                sprintf(output, "%d", dt.day);
            }
            else if (column->function == FUNC_EXTRACT_WEEK) {
                sprintf(output, "%d", datetimeGetWeek(&dt));
            }
            else if (column->function == FUNC_EXTRACT_WEEKYEAR) {
                sprintf(output, "%d", datetimeGetWeekYear(&dt));
            }
            else if (column->function == FUNC_EXTRACT_WEEKDAY) {
                sprintf(output, "%d", datetimeGetWeekDay(&dt));
            }
            else if (column->function == FUNC_EXTRACT_HEYEAR) {
                sprintf(output, "%d", dt.year + 10000);
            }
            else if (column->function == FUNC_EXTRACT_YEARDAY) {
                sprintf(output, "%d", datetimeGetYearDay(&dt));
            }
            else if (column->function == FUNC_EXTRACT_MILLENNIUM) {
                sprintf(output, "%d", dt.year / 1000);
            }
            else if (column->function == FUNC_EXTRACT_CENTURY) {
                sprintf(output, "%d", dt.year / 100);
            }
            else if (column->function == FUNC_EXTRACT_DECADE) {
                sprintf(output, "%d", dt.year / 10);
            }
            else if (column->function == FUNC_EXTRACT_QUARTER) {
                sprintf(output, "%d", (dt.month - 1) / 3 + 1);
            }
            else if (column->function == FUNC_EXTRACT_JULIAN) {
                sprintf(output, "%d", datetimeGetJulian(&dt));
            }
            else if (column->function == FUNC_EXTRACT_DATE) {
                sprintf(output, "%04d-%02d-%02d", dt.year, dt.month, dt.day);
            }
            else if (column->function == FUNC_EXTRACT_DATETIME) {
                sprintf(output, "%04d-%02d-%02dT%02d:%02d:%02d", dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second);
            }
            else if (column->function == FUNC_EXTRACT_MONTH_STRING) {
                sprintf(output, "%04d-%02d", dt.year, dt.month);
            }
            else if (column->function == FUNC_EXTRACT_WEEK_STRING) {
                sprintf(output, "%04d-W%02d", datetimeGetWeekYear(&dt), datetimeGetWeek(&dt));
            }
            else if (column->function == FUNC_EXTRACT_YEARDAY_STRING) {
                sprintf(output, "%04d-%03d", dt.year, datetimeGetYearDay(&dt));
            }
            else {
                sprintf(output, "BADEXTRACT");
            }
        }
        else {
            return -1;
        }
    }

    return 0;
}

int evaluateAggregateFunction (char * output, struct DB *tables, __attribute__((unused)) int table_count, struct ColumnNode *column, struct RowList * row_list) {
    char value[VALUE_MAX_LENGTH];

    if ((column->function & MASK_FUNC_FAMILY) != FUNC_FAM_AGG) {
        return -1;
    }

    if (column->function == FUNC_AGG_COUNT) {
        int count = 0;

        for (int i = 0; i < row_list->row_count; i++) {
            int rowid = getRowID(row_list, column->table_id, i);

            // Count up the non-NULL values
            if (getRecordValue(&tables[column->table_id], rowid, column->field, value, VALUE_MAX_LENGTH) > 0) {
                count++;
            }
        }

        sprintf(output, "%d", count);

        return 0;
    }

    if (column->function == FUNC_AGG_MIN) {
        int min = INT_MAX;

        for (int i = 0; i < row_list->row_count; i++) {
            int rowid = getRowID(row_list, column->table_id, i);

            // Only consider the non-NULL values
            if (getRecordValue(&tables[column->table_id], rowid, column->field, value, VALUE_MAX_LENGTH) > 0) {
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
            int rowid = getRowID(row_list, column->table_id, i);

            // Only consider the non-NULL values
            if (getRecordValue(&tables[column->table_id], rowid, column->field, value, VALUE_MAX_LENGTH) > 0) {
                int v = atoi(value);

                if (v > max) max = v;
            }
        }

        if (max > INT_MIN) {
            sprintf(output, "%d", max);
        }

        return 0;
    }

    if (column->function == FUNC_AGG_AVG) {
        int count = 0;
        int sum = 0;

        for (int i = 0; i < row_list->row_count; i++) {
            int rowid = getRowID(row_list, column->table_id, i);

            // Count up the non-NULL values
            if (getRecordValue(&tables[column->table_id], rowid, column->field, value, VALUE_MAX_LENGTH) > 0) {
                count++;

                sum += atoi(value);
            }
        }

        sprintf(output, "%d", sum / count);

        return 0;
    }

    if (column->function == FUNC_AGG_LISTAGG) {

        int have_prev = 0;

        for (int i = 0; i < row_list->row_count; i++) {
            int rowid = getRowID(row_list, column->table_id, i);

            // Count up the non-NULL values
            if (getRecordValue(&tables[column->table_id], rowid, column->field, value, VALUE_MAX_LENGTH) > 0) {
                if (have_prev == 1) {
                    sprintf(output, ",");
                }

                sprintf(output, "%s", value);

                have_prev = 1;
            }
        }

        return 0;
    }

    return -1;
}