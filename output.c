#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "output.h"
#include "query.h"
#include "limits.h"
#include "function.h"

void printResultLine (FILE *f, struct DB *db, struct ResultColumn columns[], int column_count, int record_index, int * result_ids, int result_count, int flags) {
    const char * field_sep = "\t";
    const char * line_end = "\n";

    if (flags & OUTPUT_FORMAT_COMMA) {
        field_sep = ",";
    }
    else if (flags & OUTPUT_FORMAT_HTML) {
        fprintf(f, "<TR><TD>");

        field_sep = "</TD><TD>";

        line_end = "</TD></TR>\n";
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
                    fprintf(f, "%s", field_sep);
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
        else if ((column.function & MASK_FUNC_FAMILY) == FUNC_AGG) {
            int result = evaluateAggregateFunction(f, db, columns + j, result_ids, result_count);
            if (result < 0) {
                fprintf(f, "BADFUNC");
            }
        }
        else if (column.field >= 0) {
            int result = evaluateFunction(f, db, columns + j, record_index);
            if (result < 0) {
                fprintf(f, "BADFUNC");
            }
        }
        else {
            fprintf(f, "UNKNOWN");
        }

        if (j < column_count - 1) {
            fprintf(f, "%s", field_sep);
        }
    }

    fprintf(f, "%s", line_end);
}

void printHeaderLine (FILE *f, struct DB *db, struct ResultColumn columns[], int column_count, int flags) {
    const char * field_sep = "\t";
    const char * line_end = "\n";

    if (flags & OUTPUT_FORMAT_COMMA) {
        field_sep = ",";
    }
    else if (flags & OUTPUT_FORMAT_HTML) {
        fprintf(f, "<TR><TH>");

        field_sep = "</TH><TH>";

        line_end = "</TH></TR>\n";
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
                    fprintf(f, "%s", field_sep);
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
            fprintf(f, "%s", field_sep);
        }
    }

    fprintf(f, "%s", line_end);
}

void printPreamble (FILE *f, __attribute__((unused)) struct DB *db, __attribute__((unused)) struct ResultColumn columns[], __attribute__((unused)) int column_count, int flags) {
    if (flags & OUTPUT_FORMAT_HTML) {
        fprintf(f, "<TABLE>\n");
    }
}

void printPostamble (FILE *f, __attribute__((unused)) struct DB *db, __attribute__((unused)) struct ResultColumn columns[], __attribute__((unused)) int column_count, __attribute__((unused)) int result_count, int flags) {
    if (flags & OUTPUT_FORMAT_HTML) {
        fprintf(f, "</TABLE>\n");
    }
}