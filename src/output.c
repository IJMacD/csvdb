#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "output.h"
#include "query.h"
#include "limits.h"
#include "function.h"
#include "util.h"

static void printAllColumns (FILE *f, struct DB *db, const char *prefix, int rowid, int format, const char * field_sep, const char * string_fmt, const char * num_fmt);
static void printAllHeaders (FILE *f, struct DB *db, const char *prefix, int format, const char * field_sep);

void printResultLine (FILE *f, struct Table *tables, int table_count, struct ColumnNode columns[], int column_count, int result_index, struct RowList * row_list, int flags) {
    const char * field_sep = "\t";
    const char * record_end = "\n";
    const char * record_sep = "";

    int format = flags & OUTPUT_MASK_FORMAT;

    int have_aggregate = 0;

    char *string_fmt = "%s";
    char *num_fmt = "%d";

    if (format == OUTPUT_FORMAT_COMMA) {
        field_sep = ",";
    }
    else if (format == OUTPUT_FORMAT_HTML) {
        fprintf(f, "<TR><TD>");

        field_sep = "</TD><TD>";

        record_end = "</TD></TR>\n";
    }
    else if (format == OUTPUT_FORMAT_JSON_ARRAY) {
        fprintf(f, "[");

        string_fmt = "\"%s\"";

        field_sep = ",";

        record_end = "]";

        record_sep = ",";
    }
    else if (format == OUTPUT_FORMAT_JSON) {
        fprintf(f, "{");

        string_fmt = "\"%s\"";

        field_sep = ",";

        record_end = "}";

        record_sep = ",";
    }
    else if (format == OUTPUT_FORMAT_SQL_INSERT) {
        fprintf(f, "(");

        string_fmt = "'%s'";

        field_sep = ",";

        record_end = ")";

        record_sep = ",\n";
    }
    else if (format == OUTPUT_FORMAT_TABLE) {
        field_sep = "";
        string_fmt = "%-20s";
        num_fmt = "%19d ";
    }

    for (int j = 0; j < column_count; j++) {
        struct ColumnNode column = columns[j];

        if ((format == OUTPUT_FORMAT_JSON
            || format == OUTPUT_FORMAT_JSON_ARRAY
            || format == OUTPUT_FORMAT_SQL_INSERT
            ) && column.concat == 1
        ) {
            fprintf(stderr, "error: Cannot output json, json_array, sql with concat columns\n");
            exit(-1);
        }

        if (format == OUTPUT_FORMAT_JSON && column.field != FIELD_STAR) {
            fprintf(f, "\"%s\": ", column.alias);
        }

        if (column.field == FIELD_STAR) {
            if (column.table_id >= 0) {
                // e.g. table.*
                struct DB *db = tables[column.table_id].db;
                int rowid = getRowID(row_list, column.table_id, result_index);
                const char *prefix = table_count > 1 ? tables[column.table_id].alias : NULL;
                printAllColumns(f, db, prefix, rowid, format, field_sep, string_fmt, num_fmt);
            } else {
                // e.g. *
                for (int m = 0; m < table_count; m++) {
                    struct DB *db = tables[m].db;
                    int rowid = getRowID(row_list, m, result_index);
                    const char *prefix = table_count > 1 ? tables[m].alias : NULL;
                    printAllColumns(f, db, prefix, rowid, format, field_sep, string_fmt, num_fmt);

                    if (m < table_count - 1) {
                        fprintf(f, "%s", field_sep);
                    }
                }
            }
        }
        else if (column.field == FIELD_COUNT_STAR) {
            fprintf(f, num_fmt, row_list->row_count);
        }
        else if (column.field == FIELD_ROW_NUMBER) {
            // ROW_NUMBER() is 1-indexed
            fprintf(f, num_fmt, result_index + 1);
        }
        else if (column.field == FIELD_ROW_INDEX) {
            // FIELD_ROW_INDEX is the input line (0 indexed)
            int rowid = getRowID(row_list, column.table_id, result_index);
            fprintf(f, num_fmt, rowid);
        }
        else if (column.field == FIELD_CONSTANT) {
            char output[MAX_VALUE_LENGTH];
            int result = evaluateFunction(output, NULL, &column, -1);

            if (result < 0) {
                fprintf(f, "BADFUNC");
            }

            if (is_numeric(output)) {
                fprintf(f, num_fmt, atoi(output));
            } else {
                fprintf(f, string_fmt, output);
            }
        }
        else if ((column.function & MASK_FUNC_FAMILY) == FUNC_FAM_AGG) {
            char output[MAX_VALUE_LENGTH];
            int result = evaluateAggregateFunction(output, tables, table_count, columns + j, row_list);

            have_aggregate = 1;

            if (result < 0) {
                fprintf(f, "BADFUNC");
            }

            if (is_numeric(output)) {
                fprintf(f, num_fmt, atoi(output));
            } else if (format == OUTPUT_FORMAT_COMMA && strchr(output, ',')) {
                // Special Case: it's important that csv be handled correctly as it
                // is used as a intermediate for VIEWs and subqueries.
                fprintf(f, "\"%s\"", output);
            } else {
                fprintf(f, string_fmt, output);
            }
        }
        else if (column.field >= 0) {
            // Evaluate plain columns as well as functions
            int rowid = getRowID(row_list, column.table_id, result_index);
            struct DB *db = tables[column.table_id].db;
            char output[MAX_VALUE_LENGTH];

            int result = evaluateFunction(output, db, columns + j, rowid);

            if (result < 0) {
                fprintf(f, "BADFUNC");
            }

            if (is_numeric(output)) {
                fprintf(f, num_fmt, atoi(output));
            } else if (format == OUTPUT_FORMAT_COMMA && strchr(output, ',')) {
                // Special Case: it's important that csv be handled correctly as it
                // is used as a intermediate for VIEWs and subqueries.
                fprintf(f, "\"%s\"", output);
            } else {
                fprintf(f, string_fmt, output);
            }
        }
        else {
            fprintf(f, "UNKNOWN");
        }

        if (j < column_count - 1 && column.concat == 0) {
            fprintf(f, "%s", field_sep);
        }
    }

    fprintf(f, "%s", record_end);

    if (result_index < row_list->row_count - 1 && have_aggregate == 0) {
        fprintf(f, "%s", record_sep);
    }
}

void printHeaderLine (FILE *f, struct Table *tables, int table_count, struct ColumnNode columns[], int column_count, int flags) {
    const char * field_sep = "\t";
    const char * line_end = "\n";

    int format = flags & OUTPUT_MASK_FORMAT;

    char *string_fmt = "%s";

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
    else if (format == OUTPUT_FORMAT_SQL_INSERT) {
        fprintf(f, "INSERT INTO \"%s\" (\"", tables[0].alias);

        field_sep = "\",\"";

        line_end = "\") VALUES\n";
    } else if (format == OUTPUT_FORMAT_TABLE) {
        field_sep = "";

        string_fmt = "%-20s";
    }


    for (int j = 0; j < column_count; j++) {
        struct ColumnNode column = columns[j];

        if ((format == OUTPUT_FORMAT_JSON
            || format == OUTPUT_FORMAT_JSON_ARRAY
            || format == OUTPUT_FORMAT_SQL_INSERT
            ) && column.concat == 1
        ) {
            fprintf(stderr, "error: Cannot output json, json_array, sql with concat columns\n");
            exit(-1);
        }

        if (column.field == FIELD_STAR) {
            if (column.table_id >= 0) {
                struct DB *db = tables[column.table_id].db;
                const char *prefix = table_count > 1 ? tables[column.table_id].alias : NULL;
                printAllHeaders(f, db, prefix, format, field_sep);
            }
            else {
                for (int m = 0; m < table_count; m++) {
                    struct DB *db = tables[m].db;
                    const char *prefix = table_count > 1 ? tables[m].alias : NULL;
                    printAllHeaders(f, db, prefix, format, field_sep);

                    if (m < table_count - 1) {
                        fprintf(f, "%s", field_sep);
                    }
                }
            }
        }
        else if (column.concat == 1) {
            // noop
        }
        else if (column.alias[0] != '\0') {
            fprintf(f, string_fmt, column.alias);
        }
        else if (column.field == FIELD_COUNT_STAR) {
            fprintf(f, string_fmt, "COUNT(*)");
        }
        else if (column.field == FIELD_ROW_NUMBER) {
            fprintf(f, string_fmt, "ROW_NUMBER()");
        }
        else if (column.field == FIELD_ROW_INDEX) {
            fprintf(f, string_fmt, "rowid");
        }
        else {
            fprintf(f, string_fmt, column.text);
        }

        if (j < column_count - 1 && column.concat == 0) {
            fprintf(f, "%s", field_sep);
        }
    }

    fprintf(f, "%s", line_end);
}

void printPreamble (FILE *f, __attribute__((unused)) struct Table *table, __attribute__((unused)) struct ColumnNode columns[], __attribute__((unused)) int column_count, int flags) {
    int format = flags & OUTPUT_MASK_FORMAT;

    if (format == OUTPUT_FORMAT_HTML) {
        fputs("<META CHARSET=\"UTF8\" /><STYLE>.csvdb{font-family:sans-serif;width:100%;border-collapse:collapse}.csvdb th{text-transform:capitalize}.csvdb th{border-bottom:1px solid #333}.csvdb td{padding:.5em 0}.csvdb tr:hover td{background-color:#f8f8f8}</STYLE>\n<TABLE CLASS=\"csvdb\">\n", f);
    }
    else if (format == OUTPUT_FORMAT_JSON_ARRAY || format == OUTPUT_FORMAT_JSON) {
        fprintf(f, "[");
    }
}

void printPostamble (FILE *f, __attribute__((unused)) struct Table *table, __attribute__((unused)) struct ColumnNode columns[], __attribute__((unused)) int column_count, __attribute__((unused)) int result_count, int flags) {

    int format = flags & OUTPUT_MASK_FORMAT;

    if (format == OUTPUT_FORMAT_HTML) {
        fprintf(f, "</TABLE>\n");
    }
    else if (format == OUTPUT_FORMAT_JSON_ARRAY || format == OUTPUT_FORMAT_JSON) {
        fprintf(f, "]\n");
    }
    else if (format == OUTPUT_FORMAT_SQL_INSERT) {
        fprintf(f, "\n");
    }
}

static void printAllColumns (FILE *f, struct DB *db, const char *prefix, int rowid, int format, const char * field_sep, const char * string_fmt, const char * num_fmt) {
    for (int k = 0; k < db->field_count; k++) {

        // Prefix
        if (format == OUTPUT_FORMAT_JSON) {
            if (prefix) {
                fprintf(f, "\"%s.%s\": ", prefix, getFieldName(db, k));
            }
            else {
                fprintf(f, "\"%s\": ", getFieldName(db, k));
            }
        }

        // Value
        char value[MAX_VALUE_LENGTH];
        int length = getRecordValue(db, rowid, k, value, MAX_VALUE_LENGTH);

        if (length == 0) {
            value[0] = '\0';
        }

        if (length >= 0) {
            if (is_numeric(value)) {
                fprintf(f, num_fmt, atoi(value));
            } else if (format == OUTPUT_FORMAT_COMMA && strchr(value, ',')) {
                // Special Case: it's important that csv be handled correctly as it
                // is used as a intermediate for VIEWs and subqueries.
                fprintf(f, "\"%s\"", value);
            } else {
                fprintf(f, string_fmt, value);
            }
        }

        // Seperator
        if (k < db->field_count - 1) {
            fprintf(f, "%s", field_sep);
        }
    }
}

static void printAllHeaders (FILE *f, struct DB *db, const char *prefix, int format, const char * field_sep) {
    char *string_fmt = (format == OUTPUT_FORMAT_TABLE) ? "%-20s" : "%s";

    for (int k = 0; k < db->field_count; k++) {
        if (prefix) {
            fprintf(f, "%s.", prefix);
        }
        fprintf(f, string_fmt, getFieldName(db, k));

        if (k < db->field_count - 1) {
            fprintf(f, "%s", field_sep);
        }
    }
}