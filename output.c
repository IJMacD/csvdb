#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "output.h"
#include "query.h"
#include "limits.h"

                          //  31  28  31   30   31   30   31   31   30   31   30   31
const int month_index[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 };

void printResultLine (FILE *f, struct DB *db, struct ResultColumn columns[], int column_count, int record_index, int result_count, int flags) {
    const char * field_sep = "\t";

    if (flags & OUTPUT_OPTION_COMMA) {
        field_sep = ",";
    }

    for (int j = 0; j < column_count; j++) {
        struct ResultColumn column = columns[j];

        if (column.field == FIELD_STAR) {
            for (int k = 0; k < db->field_count; k++) {
                char value[VALUE_MAX_LENGTH];
                if (getRecordValue(db, record_index, k, value, VALUE_MAX_LENGTH) > 0) {
                    fprintf(f, "%s", value);
                }

                if (k < db->field_count - 1) {
                    fwrite(field_sep, 1, 1, f);
                }
            }
        }
        else if (column.field == FIELD_COUNT_STAR || column.field == FIELD_ROW_NUMBER) {
            // Same logic is recycled when printing result
            // FIELD_COUNT_STAR causes grouping and gets total at end
            // FIELD_ROW_NUMBER uses current matched result count at each iteration
            fprintf(f, "%d", result_count);
        }
        else if (column.field == FIELD_ROW_INDEX) {
            // FIELD_ROW_INDEX is the input line (0 indexed)
            fprintf(f, "%d", record_index);
        }
        else if (column.field >= 0) {
            char value[VALUE_MAX_LENGTH];
            if (getRecordValue(db, record_index, column.field, value, VALUE_MAX_LENGTH) > 0) {

                if (column.function == FUNC_UNITY) {
                    fprintf(f, "%s", value);
                }
                else if ((column.function & MASK_FUNC_FAMILY) == FUNC_EXTRACT) {
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

                    if (column.function == FUNC_EXTRACT_YEAR){
                        fprintf(f, "%04d", year);
                    }
                    else if (column.function == FUNC_EXTRACT_MONTH) {
                        fprintf(f, "%02d", month);
                    }
                    else if (column.function == FUNC_EXTRACT_DAY) {
                        fprintf(f, "%02d", day);
                    }
                    else if (column.function == FUNC_EXTRACT_HEYEAR) {
                        fprintf(f, "%d", year + 10000);
                    }
                    else if (column.function == FUNC_EXTRACT_YEARDAY) {
                        int leap_day = month > 2 && (year % 4 == 0 && (year % 400 == 0 || year % 100 != 0)) ? 1 : 0;
                        fprintf(f, "%d", month_index[month - 1] + day + leap_day);
                    }
                    else {
                        fprintf(f, "BADEXTRACT");
                    }
                }
                else {
                    fprintf(f, "BADFUNC");
                }
            }
        }
        else {
            fprintf(f, "UNKNOWN");
        }

        if (j < column_count - 1) {
            fwrite(field_sep, 1, 1, f);
        }
    }
    fprintf(f, "\n");
}

void printHeaderLine (FILE *f, struct DB *db, struct ResultColumn columns[], int column_count, int flags) {
    const char * field_sep = "\t";

    if (flags & OUTPUT_OPTION_COMMA) {
        field_sep = ",";
    }

    for (int j = 0; j < column_count; j++) {
        struct ResultColumn column = columns[j];

        if (column.alias[0] != '\0') {
            fprintf(f, "%s", column.alias);
        }
        else if (column.field == FIELD_STAR) {
            for (int k = 0; k < db->field_count; k++) {
                fprintf(f, "%s", getFieldName(db, k));

                if (k < db->field_count - 1) {
                    fwrite(field_sep, 1, 1, f);
                }
            }
        }
        else if (column.field == FIELD_COUNT_STAR) {
            fprintf(f, "COUNT(*)");
        }
        else if (column.field == FIELD_ROW_NUMBER) {
            fprintf(f, "ROW_NUMBER()");
        }
        else if (column.field == FIELD_ROW_INDEX) {
            // FIELD_ROW_INDEX is the input line (0 indexed)
            fprintf(f, "rowid");
        }
        else {
            fprintf(f, "%s", column.text);
        }

        if (j < column_count - 1) {
            fwrite(field_sep, 1, 1, f);
        }
    }
    fprintf(f, "\n");
}
