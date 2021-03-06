#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "output.h"
#include "query.h"
#include "limits.h"
#include "function.h"

static void printAllColumns (FILE *f, struct DB *db, int rowid, int format, const char * field_sep);
static void printAllHeaders (FILE *f, struct DB *db, const char * field_sep);

void printResultLine (FILE *f, struct Table *tables, int table_count, struct ColumnNode columns[], int column_count, int result_index, struct RowList * row_list, int flags) {
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
    else if (format == OUTPUT_FORMAT_SQL_INSERT) {
        fprintf(f, "('");

        field_sep = "','";

        record_end = "')";

        record_sep = ",\n";
    }

    for (int j = 0; j < column_count; j++) {
        struct ColumnNode column = columns[j];

        if (format == OUTPUT_FORMAT_JSON && column.field != FIELD_STAR) {
            fprintf(f, "\"%s\": \"", column.alias);
        }

        if (column.field == FIELD_STAR) {
            if (column.table_id >= 0) {
                // e.g. table.*
                struct DB *db = tables[column.table_id].db;
                int rowid = getRowID(row_list, column.table_id, result_index);
                printAllColumns(f, db, rowid, format, field_sep);
            } else {
                // e.g. *
                for (int m = 0; m < table_count; m++) {
                    struct DB *db = tables[m].db;
                    int rowid = getRowID(row_list, m, result_index);
                    printAllColumns(f, db, rowid, format, field_sep);

                    if (m < table_count - 1) {
                        fprintf(f, "%s", field_sep);
                    }
                }
            }
        }
        else if (column.field == FIELD_COUNT_STAR) {
            fprintf(f, "%d", row_list->row_count);
        }
        else if (column.field == FIELD_ROW_NUMBER) {
            // ROW_NUMBER() is 1-indexed
            fprintf(f, "%d", result_index + 1);
        }
        else if (column.field == FIELD_ROW_INDEX) {
            // FIELD_ROW_INDEX is the input line (0 indexed)
            int rowid = getRowID(row_list, column.table_id, result_index);
            fprintf(f, "%d", rowid);
        }
        else if (column.field == FIELD_CONSTANT) {
            char output[MAX_VALUE_LENGTH];
            int result = evaluateFunction(output, NULL, &column, -1);

            if (result < 0) {
                fprintf(f, "BADFUNC");
            }

            fprintf(f, "%s", output);
        }
        else if ((column.function & MASK_FUNC_FAMILY) == FUNC_FAM_AGG) {
            int result = evaluateAggregateFunction(f, tables, table_count, columns + j, row_list);

            if (result < 0) {
                fprintf(f, "BADFUNC");
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

            fprintf(f, "%s", output);
        }
        else {
            fprintf(f, "UNKNOWN");
        }

        if (j < column_count - 1) {
            fprintf(f, "%s", field_sep);
        }
    }

    fprintf(f, "%s", record_end);

    if (result_index < row_list->row_count - 1) {
        fprintf(f, "%s", record_sep);
    }
}

void printHeaderLine (FILE *f, struct Table *tables, int table_count, struct ColumnNode columns[], int column_count, int flags) {
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
    else if (format == OUTPUT_FORMAT_SQL_INSERT) {
        fprintf(f, "INSERT INTO \"%s\" (\"", tables[0].alias);

        field_sep = "\",\"";

        line_end = "\") VALUES\n";
    }


    for (int j = 0; j < column_count; j++) {
        struct ColumnNode column = columns[j];

        if (column.field == FIELD_STAR) {
            if (column.table_id >= 0) {
                struct DB *db = tables[column.table_id].db;
                printAllHeaders(f, db, field_sep);
            }
            else {
                for (int m = 0; m < table_count; m++) {
                    struct DB *db = tables[m].db;
                    printAllHeaders(f, db, field_sep);

                    if (m < table_count - 1) {
                        fprintf(f, "%s", field_sep);
                    }
                }
            }
        }
        else if (column.alias[0] != '\0') {
            fprintf(f, "%s", column.alias);
        }
        else if (column.field == FIELD_COUNT_STAR) {
            fprintf(f, "COUNT(*)");
        }
        else if (column.field == FIELD_ROW_NUMBER) {
            fprintf(f, "ROW_NUMBER()");
        }
        else if (column.field == FIELD_ROW_INDEX) {
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

static void printAllColumns (FILE *f, struct DB *db, int rowid, int format, const char * field_sep) {
    for (int k = 0; k < db->field_count; k++) {

        // Prefix
        if (format == OUTPUT_FORMAT_JSON) {
            fprintf(f, "\"%s\": \"", getFieldName(db, k));
        }

        // Value
        char value[MAX_VALUE_LENGTH];
        if (getRecordValue(db, rowid, k, value, MAX_VALUE_LENGTH) > 0) {
            fprintf(f, "%s", value);
        }

        // Seperator
        if (k < db->field_count - 1) {
            fprintf(f, "%s", field_sep);
        }
    }
}

static void printAllHeaders (FILE *f, struct DB *db, const char * field_sep) {
    for (int k = 0; k < db->field_count; k++) {
        fprintf(f, "%s", getFieldName(db, k));

        if (k < db->field_count - 1) {
            fprintf(f, "%s", field_sep);
        }
    }
}