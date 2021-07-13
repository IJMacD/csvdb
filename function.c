#include <stdlib.h>
#include <string.h>

#include "function.h"


                          //  31  28  31   30   31   30   31   31   30   31   30   31
const int month_index[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 };

int outputFunction(FILE *f, struct DB *db, struct ResultColumn *column, int record_index) {
    char value[VALUE_MAX_LENGTH];
    if (getRecordValue(db, record_index, column->field, value, VALUE_MAX_LENGTH) > 0) {

        if (column->function == FUNC_UNITY) {
            fprintf(f, "%s", value);
        }
        else if ((column->function & MASK_FUNC_FAMILY) == FUNC_EXTRACT) {
            char v[5] = {0};

            memcpy(v, value, 4);
            v[4] = '\0';
            int year = atoi(v);

            memcpy(v, value + 5, 2);
            v[2] = '\0';
            int month = atoi(v);

            memcpy(v, value + 8, 2);
            v[2] = '\0';
            int day = atoi(v);

            if (column->function == FUNC_EXTRACT_YEAR){
                fprintf(f, "%d", year);
            }
            else if (column->function == FUNC_EXTRACT_MONTH) {
                fprintf(f, "%d", month);
            }
            else if (column->function == FUNC_EXTRACT_DAY) {
                fprintf(f, "%d", day);
            }
            else if (column->function == FUNC_EXTRACT_HEYEAR) {
                fprintf(f, "%d", year + 10000);
            }
            else if (column->function == FUNC_EXTRACT_YEARDAY) {
                int leap_day = month > 2 && (year % 4 == 0 && (year % 400 == 0 || year % 100 != 0)) ? 1 : 0;
                fprintf(f, "%d", month_index[month - 1] + day + leap_day);
            }
            else if (column->function == FUNC_EXTRACT_MILLENNIUM) {
                fprintf(f, "%d", year / 1000);
            }
            else if (column->function == FUNC_EXTRACT_CENTURY) {
                fprintf(f, "%d", year / 100);
            }
            else if (column->function == FUNC_EXTRACT_DECADE) {
                fprintf(f, "%d", year / 10);
            }
            else if (column->function == FUNC_EXTRACT_QUARTER) {
                fprintf(f, "%d", (month - 1) / 3 + 1);
            }
            else if (column->function == FUNC_EXTRACT_DATE) {
                fprintf(f, "%04d-%02d-%02d", year, month, day);
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