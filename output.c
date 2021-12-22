#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "output.h"
#include "query.h"
#include "limits.h"
#include "function.h"

void printResultLine (FILE *f, struct DB *db, struct ResultColumn columns[], int column_count, int result_index, int * result_ids, int result_count, int flags) {
    const char * field_sep = "\t";
    const char * record_end = "\n";
    const char * record_sep = "";

    int format = flags & OUTPUT_MASK_FORMAT;

    if (format == OUTPUT_FORMAT_COMMA) {
        field_sep = ",";
    }
    else if (format == OUTPUT_FORMAT_HTML) {
        fprintf(f, "<TR><TD>");

        field_sep = "</TD><TD>";

        record_end = "</TD></TR>\n";
    }
    else if (format == OUTPUT_FORMAT_JSON_ARRAY) {
        fprintf(f, "[\"");

        field_sep = "\",\"";

        record_end = "\"]";

        record_sep = ",";
    }
    else if (format == OUTPUT_FORMAT_JSON) {
        fprintf(f, "{");

        field_sep = "\",";

        record_end = "\"}";

        record_sep = ",";
    }

    int record_index = result_ids[result_index];

    for (int j = 0; j < column_count; j++) {
        struct ResultColumn column = columns[j];

        if (format == OUTPUT_FORMAT_JSON && column.field != FIELD_STAR) {
            fprintf(f, "\"%s\": \"", column.alias);
        }

        if (column.field == FIELD_STAR) {
            for (int k = 0; k < db->field_count; k++) {

                if (format == OUTPUT_FORMAT_JSON) {
                    fprintf(f, "\"%s\": \"", getFieldName(db, k));
                }

                char value[VALUE_MAX_LENGTH];
                if (getRecordValue(db, record_index, k, value, VALUE_MAX_LENGTH) > 0) {
                    fprintf(f, "%s", value);
                }

                if (k < db->field_count - 1) {
                    fprintf(f, "%s", field_sep);
                }
            }
        }
        else if (column.field == FIELD_COUNT_STAR) {
            fprintf(f, "%d", result_count);
        }
        else if (column.field == FIELD_ROW_NUMBER) {
            /**
             * @todo Bug: Should add q->offset_value
             */
            fprintf(f, "%d", result_index);
        }
        else if (column.field == FIELD_ROW_INDEX) {
            // FIELD_ROW_INDEX is the input line (0 indexed)
            fprintf(f, "%d", record_index);
        }
        else if (column.field == FIELD_CONSTANT) {
            fprintf(f, "%s", column.text);
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

    fprintf(f, "%s", record_end);

    if (result_index < result_count - 1) {
        fprintf(f, "%s", record_sep);
    }
}

void printHeaderLine (FILE *f, struct DB *db, struct ResultColumn columns[], int column_count, int flags) {
    const char * field_sep = "\t";
    const char * line_end = "\n";

    int format = flags & OUTPUT_MASK_FORMAT;

    if (format == OUTPUT_FORMAT_COMMA) {
        field_sep = ",";
    }
    else if (format == OUTPUT_FORMAT_HTML) {
        fprintf(f, "<TR><TH>");

        field_sep = "</TH><TH>";

        line_end = "</TH></TR>\n";
    }
    else if (format == OUTPUT_FORMAT_JSON_ARRAY) {
        fprintf(f, "[\"");

        field_sep = "\",\"";

        line_end = "\"],";
    }
    else if (format == OUTPUT_FORMAT_JSON) {
        return;
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
    int format = flags & OUTPUT_MASK_FORMAT;

    if (format == OUTPUT_FORMAT_HTML) {
        fputs("<STYLE>.csvdb{font-family:sans-serif;width:100%;border-collapse:collapse}.csvdb th{text-transform:capitalize}.csvdb th{border-bottom:1px solid #333}.csvdb td{padding:.5em 0}.csvdb tr:hover td{background-color:#f8f8f8}</STYLE>\n<TABLE CLASS=\"csvdb\">\n", f);
    }
    else if (format == OUTPUT_FORMAT_JSON_ARRAY || format == OUTPUT_FORMAT_JSON) {
        fprintf(f, "[");
    }
}

void printPostamble (FILE *f, __attribute__((unused)) struct DB *db, __attribute__((unused)) struct ResultColumn columns[], __attribute__((unused)) int column_count, __attribute__((unused)) int result_count, int flags) {

    int format = flags & OUTPUT_MASK_FORMAT;

    if (format == OUTPUT_FORMAT_HTML) {
        fprintf(f, "</TABLE>\n");
    }
    else if (format == OUTPUT_FORMAT_JSON_ARRAY || format == OUTPUT_FORMAT_JSON) {
        fprintf(f, "]\n");
    }
}