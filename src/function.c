#include <stdlib.h>
#include <string.h>
#include <limits.h>

#include "query.h"
#include "function.h"
#include "date.h"
#include "util.h"

int evaluateFunction(FILE *f, struct DB *db, struct ResultColumn *column, int record_index) {
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
            fprintf(f, "%s", value);
        }
        else if (column->function == FUNC_CHR) {
            int codepoint = atoi(value);
            writeUTF8(value, codepoint);

            fprintf(f, "%s", value);
        }
        else if ((column->function & MASK_FUNC_FAMILY) == FUNC_FAM_STRING) {
            if (column->function == FUNC_LENGTH) {
                int len = strlen(value);
                fprintf(f, "%d", len);
            }
            else if (column->function == FUNC_LEFT) {
                // Both field name and length stroed in same array
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
                    fwrite(value, 1, count, f);
                } else {
                    fprintf(f, "%s", value);
                }
            }
            else if (column->function == FUNC_RIGHT) {
                // Both field name and length stroed in same array
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
                    fprintf(f, "%s", value + len - count);
                } else {
                    fprintf(f, "%s", value);
                }
            }
        }
        else if ((column->function & MASK_FUNC_FAMILY) == FUNC_FAM_EXTRACT) {
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

int evaluateAggregateFunction (FILE *f, struct DB *tables, __attribute__((unused)) int table_count, struct ResultColumn *column, struct RowList * row_list) {
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

        fprintf(f, "%d", count);

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
            fprintf(f, "%d", min);
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
            fprintf(f, "%d", max);
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

        fprintf(f, "%d", sum / count);

        return 0;
    }

    return -1;
}