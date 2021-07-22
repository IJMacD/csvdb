#include <stdlib.h>

#include "function.h"

int outputFunction(FILE *f, struct DB *db, struct ResultColumn *column, int record_index) {
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
