#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "../structs.h"
#include "result.h"
#include "../evaluate/evaluate.h"
#include "../evaluate/function.h"
#include "../db/db.h"
#include "../functions/util.h"

static void printAllColumnValues (
    FILE *f,
    struct DB *db,
    const char *prefix,
    int rowid,
    enum OutputOption format
);
static void printAllHeaderNames (
    FILE *f,
    struct DB *db,
    const char *prefix,
    enum OutputOption format
);

static void printHeaderName (
    FILE *f,
    enum OutputOption format,
    const char *prefix,
    const char *name
);
static void printHeaderSeparator (FILE *f, enum OutputOption format);
static void printRecordStart (
    FILE *f,
    enum OutputOption format,
    int is_first,
    int is_single_column
);
static void printRecordEnd (
    FILE *f,
    enum OutputOption format,
    int is_single_column
);
static void printRecordSeparator (FILE *f, enum OutputOption format);
static void printColumnValue (
    FILE *f,
    enum OutputOption format,
    const char *prefix,
    const char *name,
    const char *value
);
static void printColumnValueNumber (
    FILE *f,
    enum OutputOption format,
    const char *prefix,
    const char *name,
    long value
);
static void printColumnSeparator (FILE *f, enum OutputOption format);

void printResultLine (
    FILE *f,
    struct Table *tables,
    int table_count,
    struct Node columns[],
    int column_count,
    int result_index,
    RowListIndex list_id,
    enum OutputOption flags
) {
    enum OutputOption format = flags & OUTPUT_MASK_FORMAT;

    int is_single_column
        = column_count == 1 && strcmp(columns[0].alias, "_") == 0;

    int is_first = result_index == 0;

    // Do separator at start to avoid issues with aggregate queries and not
    // knowing whether or not there is going to be a record following this one.
    if (!is_first) {
        printRecordSeparator(f, format);
    }

    printRecordStart(f, format, is_first, is_single_column);

    struct RowList *row_list = getRowList(list_id);

    // Arbitrarily choose index 0 for agg rows
    int rowlist_row_index = row_list->group ? 0 : result_index;

    for (int j = 0; j < column_count; j++) {
        struct Node *node = &columns[j];

        if (node->function == FUNC_UNITY) {

            // Note: optimiser has probably already expanded FIELD_STAR away
            if (node->field.index == FIELD_STAR) {
                if (node->field.table_id >= 0) {
                    // e.g. table.*
                    struct DB *db = tables[node->field.table_id].db;
                    int rowid = getRowID(
                        row_list,
                        node->field.table_id,
                        rowlist_row_index
                    );
                    const char *prefix = table_count > 1
                        ? tables[node->field.table_id].alias : NULL;
                    printAllColumnValues(f, db, prefix, rowid, format);
                } else {
                    // e.g. *
                    for (int m = 0; m < table_count; m++) {
                        struct DB *db = tables[m].db;
                        int rowid = getRowID(row_list, m, rowlist_row_index);
                        const char *prefix = table_count > 1
                            ? tables[m].alias : NULL;
                        printAllColumnValues(f, db, prefix, rowid, format);

                        if (m < table_count - 1) {
                            printColumnSeparator(f, format);
                        }
                    }
                }
            }
            else if (node->field.index == FIELD_ROW_NUMBER) {
                // ROW_NUMBER() is 1-indexed
                printColumnValueNumber(
                    f,
                    format,
                    NULL,
                    node->alias,
                    result_index + 1
                );
            }
            else if (node->field.index == FIELD_ROW_INDEX) {
                // FIELD_ROW_INDEX is the input line (0 indexed)
                int rowid = getRowID(
                    row_list,
                    node->field.table_id,
                    rowlist_row_index
                );
                printColumnValueNumber(f, format, NULL, node->alias, rowid);
            }
            // Raw FUNC_UNITY field
            else {
                char output[MAX_VALUE_LENGTH];
                int result = evaluateNode(
                    tables,
                    list_id,
                    rowlist_row_index,
                    node,
                    output,
                    MAX_VALUE_LENGTH
                );

                if (result < 0) {
                    fprintf(
                        stderr,
                        "Cannot evaluate field '%s\n",
                        node->field.text
                    );
                    return;
                }

                printColumnValue(f, format, NULL, node->alias, output);
            }
        }
        else if ((node->function & MASK_FUNC_FAMILY) == FUNC_FAM_AGG) {
            char output[MAX_VALUE_LENGTH];
            int result = evaluateAggregateFunction(
                output,
                tables,
                node,
                list_id
            );

            printColumnValue(
                f,
                format,
                NULL,
                node->alias,
                result < 0 ? "BADFUNC" : output
            );
        }
        #ifdef JSON_BOOL
        else if (
            node->function == OPERATOR_ALWAYS &&
            (
                format == OUTPUT_FORMAT_JSON ||
                format == OUTPUT_FORMAT_JSON_ARRAY
            )
        ) {
            printColumnValue(
                f,
                format,
                NULL,
                node->alias,
                "true"
            );
        }
        else if (
            node->function == OPERATOR_NEVER &&
            (
                format == OUTPUT_FORMAT_JSON ||
                format == OUTPUT_FORMAT_JSON_ARRAY
            )
        ) {
            printColumnValue(
                f,
                format,
                NULL,
                node->alias,
                "false"
            );
        }
        #endif
        else {
            // Evaluate functions
            char output[MAX_VALUE_LENGTH];

            int result = evaluateNode(
                tables,
                list_id,
                rowlist_row_index,
                node,
                output,
                MAX_VALUE_LENGTH
            );

            printColumnValue(
                f,
                format,
                NULL,
                node->alias,
                result < 0 ? "BADFUNC" : output
            );
        }

        int is_last_column = j == column_count - 1;
        if (!is_last_column
            // && !column->concat
        ) {
            printColumnSeparator(f, format);
        }
    }

    printRecordEnd(f, format, is_single_column);
}

void printHeaderLine (
    FILE *f,
    struct Table *tables,
    int table_count,
    struct Node columns[],
    int column_count,
    enum OutputOption flags
) {
    enum OutputOption format = flags & OUTPUT_MASK_FORMAT;

    /********************
     * Header Start
     ********************/

    if (format == OUTPUT_FORMAT_HTML) {
        fprintf(f, "<THEAD><TR><TH>");
    }
    else if (format == OUTPUT_FORMAT_JSON_ARRAY) {
        // For JSON Array output a single column with an alias of "_" means
        // create a single list rather than nested arrays.
        // We don't need a header in this case
        int nested_values = column_count > 1 || strcmp(columns[0].alias, "_");
        if (!nested_values) {
            return;
        }

        fprintf(f, "[\"");
    }
    else if (format == OUTPUT_FORMAT_JSON) {
        return;
    }
    else if (format == OUTPUT_FORMAT_SQL_INSERT) {
        fprintf(f, "INSERT INTO \"%s\" (\"", tables[0].alias);
    }
    else if (format == OUTPUT_FORMAT_SQL_CREATE) {
        fprintf(f, "CREATE TABLE \"%1$s\" (\"", tables[0].alias);
    }
    else if (format == OUTPUT_FORMAT_INFO_SEP) {
        printf("\x01"); // Start of Heading
    }
    else if (format == OUTPUT_FORMAT_XML) {
        return;
    }
    else if (format == OUTPUT_FORMAT_SQL_VALUES) {
        return;
    }

    /********************
     * Header Name
     ********************/

    for (int j = 0; j < column_count; j++) {
        struct Node *node = &columns[j];

        int is_last = j == column_count - 1;

        if (node->field.index == FIELD_STAR) {
            if (node->field.table_id >= 0) {
                struct DB *db = tables[node->field.table_id].db;
                const char *prefix
                    = table_count > 1
                        ? tables[node->field.table_id].alias : NULL;
                printAllHeaderNames(f, db, prefix, format);
            }
            else {
                for (int m = 0; m < table_count; m++) {
                    struct DB *db = tables[m].db;
                    const char *prefix
                        = table_count > 1
                            ? tables[m].alias : NULL;
                    printAllHeaderNames(f, db, prefix, format);

                    if (m < table_count - 1) {
                        printHeaderSeparator(f, format);
                    }
                }
            }
        }
        else if (node->alias[0] != '\0') {
            printHeaderName(f, format, NULL, node->alias);
        }
        else if (node->field.index == FIELD_ROW_NUMBER) {
            printHeaderName(f, format, NULL, "ROW_NUMBER()");
        }
        else if (node->field.index == FIELD_ROW_INDEX) {
            printHeaderName(f, format, NULL, "rowid");
        }
        else {
            printHeaderName(f, format, NULL, node->field.text);
        }

        if (!is_last) {
            printHeaderSeparator(f, format);
        }
    }

    /********************
     * Header End
     ********************/

    if (format == OUTPUT_FORMAT_TAB) {
        fprintf(f, "\n");
    }
    else if (format == OUTPUT_FORMAT_COMMA) {
        fprintf(f, "\n");
    }
    else if (format == OUTPUT_FORMAT_HTML) {
        fprintf(f, "</TH></TR></THEAD>\n");
    }
    else if (format == OUTPUT_FORMAT_JSON_ARRAY) {
        fprintf(f, "\"],");
    }
    else if (format == OUTPUT_FORMAT_SQL_INSERT) {
        fprintf(f, "\") VALUES\n");
    }
    else if (format == OUTPUT_FORMAT_SQL_CREATE) {
        fprintf(f, "\"); INSERT INTO \"%1$s\" VALUES\n", tables[0].alias);
    }
    else if (format == OUTPUT_FORMAT_TABLE) {
        fprintf(f, "|\n");

        for (int i = 0; i < column_count; i++) {
            struct Node *node = &columns[i];

            if (node->field.index == FIELD_STAR) {
                if (node->field.table_id >= 0) {
                    struct DB *db = tables[node->field.table_id].db;
                    for (int j = 0; j < db->field_count; j++) {
                        fprintf(f, "|--------------------");
                    }
                }
                else {
                    for (int m = 0; m < table_count; m++) {
                        struct DB *db = tables[m].db;
                        for (int j = 0; j < db->field_count; j++) {
                            fprintf(f, "|--------------------");
                        }
                    }
                }
            }
            else {
                fprintf(f, "|--------------------");
            }
        }
        fprintf(f, "|\n");
    }
}

void printPreamble (
    FILE *f,
    __attribute__((unused)) struct Table *table,
    __attribute__((unused)) int table_count,
    __attribute__((unused)) struct Node columns[],
    __attribute__((unused)) int column_count,
    enum OutputOption flags
) {
    enum OutputOption format = flags & OUTPUT_MASK_FORMAT;

    if (format == OUTPUT_FORMAT_HTML) {
        fputs(
            "<STYLE>.csvdb{font-family:sans-serif;width:100%;border-collapse:"
            "collapse}.csvdb th{border-bottom:1px solid #333}.csvdb td{padding:"
            ".5em}.csvdb tr:hover td{background-color:#f8f8f8}</STYLE>\n<TABLE "
            "CLASS=\"csvdb\">\n",
            f
        );
    }
    else if (
        format == OUTPUT_FORMAT_JSON_ARRAY
        || format == OUTPUT_FORMAT_JSON
    ) {
        fprintf(f, "[");
    }
    else if (format == OUTPUT_FORMAT_XML) {
        fprintf(f, "<results>");
    }
    else if (format == OUTPUT_FORMAT_SQL_VALUES) {
        fprintf(f, "VALUES\n");
    }
}

void printPostamble (
    FILE *f,
    __attribute__((unused)) struct Table *table,
    __attribute__((unused)) int table_count,
    __attribute__((unused)) struct Node columns[],
    __attribute__((unused)) int column_count,
    __attribute__((unused)) int result_count,
    enum OutputOption flags
) {

    enum OutputOption format = flags & OUTPUT_MASK_FORMAT;

    if (format == OUTPUT_FORMAT_HTML) {
        fprintf(f, "</TBODY>\n");
        fprintf(f, "</TABLE>\n");
    }
    else if (
        format == OUTPUT_FORMAT_JSON_ARRAY
        || format == OUTPUT_FORMAT_JSON
    ) {
        fprintf(f, "]\n");
    }
    else if (
        format == OUTPUT_FORMAT_SQL_INSERT
        || format == OUTPUT_FORMAT_SQL_VALUES
    ) {
        fprintf(f, "\n");
    }
    else if (format == OUTPUT_FORMAT_XML) {
        fprintf(f, "</results>\n");
    }
}

static void printAllColumnValues (
    FILE *f,
    struct DB *db,
    const char *prefix,
    int rowid,
    enum OutputOption format
) {
    for (int k = 0; k < db->field_count; k++) {
        // Value
        char value[MAX_VALUE_LENGTH];
        int length = getRecordValue(db, rowid, k, value, MAX_VALUE_LENGTH);

        if (length <= 0) {
            value[0] = '\0';
        }

        char *field_name = NULL;
        if (format == OUTPUT_FORMAT_JSON || format == OUTPUT_FORMAT_XML) {
            field_name = getFieldName(db, k);
        }

        printColumnValue(f, format, prefix, field_name, value);

        if (k < db->field_count - 1) {
            printColumnSeparator(f, format);
        }
    }
}

static void printAllHeaderNames (
    FILE *f,
    struct DB *db,
    const char *prefix,
    enum OutputOption format
) {
    for (int k = 0; k < db->field_count; k++) {
        printHeaderName(f, format, prefix, getFieldName(db, k));

        if (k < db->field_count - 1) {
            printHeaderSeparator(f, format);
        }
    }
}

static void printHeaderName (
    FILE *f,
    enum OutputOption format,
    const char *prefix,
    const char *name
) {

    if (format == OUTPUT_FORMAT_TABLE) {
        if (prefix) {
            char s[MAX_FIELD_LENGTH];
            sprintf(s, "%s.%s", prefix, name);
            fprintf(f, "| %-19s", s);
        }
        else {
            fprintf(f, "| %-19s", name);
        }
    }
    else {
        if (prefix) {
            fprintf(f, "%s.%s", prefix, name);
        }
        else {
            fprintf(f, "%s", name);
        }
    }
}

static void printHeaderSeparator (FILE *f, enum OutputOption format) {
    if (format == OUTPUT_FORMAT_TAB) {
        fprintf(f, "\t");
    }
    else if (format == OUTPUT_FORMAT_COMMA) {
        fprintf(f, ",");
    }
    else if (format == OUTPUT_FORMAT_HTML) {
        fprintf(f, "</TH><TH>");
    }
    else if (format == OUTPUT_FORMAT_JSON_ARRAY) {
        fprintf(f, "\",\"");
    }
    else if (
        format == OUTPUT_FORMAT_SQL_INSERT ||
        format == OUTPUT_FORMAT_SQL_CREATE
    ) {
        fprintf(f, "\",\"");
    }
    else if (format == OUTPUT_FORMAT_INFO_SEP) {
        fprintf(f, "\x1f");
    }
}

static void printRecordStart (
    FILE *f,
    enum OutputOption format,
    int is_first,
    int is_single_col
) {
    if (format == OUTPUT_FORMAT_HTML) {
        if (is_first) {
            fprintf(f, "<TBODY>\n");
        }
        fprintf(f, "<TR>");
    }
    else if (format == OUTPUT_FORMAT_JSON_ARRAY) {
        // For JSON Array output a single column with an alias of "_" means
        // create a single list rather than nested arrays
        if (!is_single_col) {
            fprintf(f, "[");
        }
    }
    else if (format == OUTPUT_FORMAT_JSON) {
        fprintf(f, "{");
    }
    else if (
        format == OUTPUT_FORMAT_SQL_INSERT ||
        format == OUTPUT_FORMAT_SQL_CREATE
    ) {
        fprintf(f, "(");
    }
    else if (format == OUTPUT_FORMAT_INFO_SEP) {
        if (is_first) {
            fprintf(f, "\x02"); // Start of Text
        }
    }
    else if (format == OUTPUT_FORMAT_XML) {
        fprintf(f, "<record>");
    }
    else if (format == OUTPUT_FORMAT_SQL_VALUES) {
        fprintf(f, "(");
    }

}

static void printColumnValue (
    FILE *f,
    enum OutputOption format,
    const char *prefix,
    const char *name,
    const char *value
) {
    int value_is_numeric = is_numeric(value);

    char *escaped_value = (void *)value;

    int mallocd = 0;

    if (format == OUTPUT_FORMAT_JSON) {
        if (prefix) {
            fprintf(f, "\"%s.%s\": ", prefix, name);
        }
        else {
            fprintf(f, "\"%s\": ", name);
        }
    }
    // For XML output a column alias of "_" means create a text node rather than
    // an element. Can be used to create a flat list of elements for example.
    else if (format == OUTPUT_FORMAT_XML && strcmp(name, "_")) {
        if (prefix) {
            fprintf(f, "<%s.%s>", prefix, name);
        }
        else {
            fprintf(f, "<%s>", name);
        }
    }
    else if (format == OUTPUT_FORMAT_TABLE) {
        fprintf(f, "| ");
    }
    else if (format == OUTPUT_FORMAT_HTML) {
        if (value_is_numeric) {
            fprintf(f, "<TD ALIGN=\"RIGHT\">");
        }
        else {
            fprintf(f, "<TD>");
        }
    }

    const char * string_fmt = "%s";
    const char * num_fmt = string_fmt;

    if (format == OUTPUT_FORMAT_COMMA) {

        // Raw newlines can appear in CSV as long as they're in a quoted field
        if (strchr(value, ',') || strchr(value, '\n')) {

            string_fmt = "\"%s\"";

            // If there are any double quotes in the value, they need to be
            // double-double quoted
            if (strchr(value, '"')) {
                size_t value_length = strlen(value);
                escaped_value = malloc(value_length * 2);
                mallocd = 1;
                replace(escaped_value, value, '"', "\"\"");
            }
        }

    }
    else if (format == OUTPUT_FORMAT_JSON_ARRAY) {
        string_fmt = "\"%s\"";
        num_fmt = "%ld";
    }
    else if (format == OUTPUT_FORMAT_JSON) {

        size_t value_length = strlen(value);
        escaped_value = malloc(value_length * 2);
        char *clone = malloc(value_length * 2);
        mallocd = 1;

        replace(clone, value, '\r', "\\r");
        replace(escaped_value, clone, '\n', "\\n");

        if (strchr(value, '"')) {
            replace(clone, escaped_value, '"', "\\\"");
            free(escaped_value);
            escaped_value = clone;
        }
        else {
            free(clone);
        }

        string_fmt = "\"%s\"";
        num_fmt = "%ld";
    }
    else if (
        format == OUTPUT_FORMAT_SQL_INSERT ||
        format == OUTPUT_FORMAT_SQL_VALUES ||
        format == OUTPUT_FORMAT_SQL_CREATE
    ) {
        size_t value_length = strlen(value);
        escaped_value = malloc(value_length * 2);
        char *clone = malloc(value_length * 2);
        mallocd = 1;

        replace(clone, value, '\r', "\\r");
        replace(escaped_value, clone, '\n', "\\n");

        if (strchr(value, '\'')) {
            replace(clone, escaped_value, '\'', "''");
            free(escaped_value);
            escaped_value = clone;
        }
        else {
            free(clone);
        }

        string_fmt = "'%s'";
        num_fmt = "%ld";
    }
    else if (format == OUTPUT_FORMAT_TABLE) {
        // If there are any new lines in the value, they should be replaced.
        if (strchr(value, '\r') || strchr(value, '\n')) {
            size_t value_length = strlen(value);
            escaped_value = malloc(value_length * 3);
            char *clone = malloc(value_length * 3);
            mallocd = 1;
            replace(clone,         value, '\r', "␍");
            replace(escaped_value, clone, '\n', "␊");
        }

        string_fmt = "%-19s";
        num_fmt = "%18ld ";
    }

    #ifdef JSON_NULL
    if (
        (format == OUTPUT_FORMAT_JSON || format == OUTPUT_FORMAT_JSON_ARRAY)
        && strlen(value) == 0
    ) {
        fprintf(f, "null");
    }
    else
    #endif

    #ifdef JSON_BOOL
    if (
        (format == OUTPUT_FORMAT_JSON || format == OUTPUT_FORMAT_JSON_ARRAY)
        && strcmp(value, "true") == 0
    ) {
        fprintf(f, "true");
    }
    else if (
        (format == OUTPUT_FORMAT_JSON || format == OUTPUT_FORMAT_JSON_ARRAY)
        && strcmp(value, "false") == 0
    ) {
        fprintf(f, "false");
    }
    else
    #endif

    if (num_fmt != string_fmt && value_is_numeric) {
        fprintf(f, num_fmt, atol(value));
    } else {
        fprintf(f, string_fmt, escaped_value);
    }

    if (format == OUTPUT_FORMAT_XML && strcmp(name, "_")) {
        if (prefix) {
            fprintf(f, "</%s.%s>", prefix, name);
        }
        else {
            fprintf(f, "</%s>", name);
        }
    }
    else if (format == OUTPUT_FORMAT_HTML) {
        fprintf(f, "</TD>");
    }

    if (mallocd) {
        free(escaped_value);
    }
}

static void printColumnValueNumber (
    FILE *f,
    enum OutputOption format,
    const char *prefix,
    const char *name,
    long value
) {
    char output[16];
    sprintf(output, "%ld", value);
    printColumnValue(f, format, prefix, name, output);
}

static void printColumnSeparator (FILE *f, enum OutputOption format) {
    if (format == OUTPUT_FORMAT_TAB) {
        fprintf(f, "\t");
    }
    else if (format == OUTPUT_FORMAT_COMMA) {
        fprintf(f, ",");
    }
    else if (format == OUTPUT_FORMAT_JSON_ARRAY) {
        fprintf(f, ",");
    }
    else if (format == OUTPUT_FORMAT_JSON) {
        fprintf(f, ",");
    }
    else if (
        format == OUTPUT_FORMAT_SQL_INSERT ||
        format == OUTPUT_FORMAT_SQL_VALUES ||
        format == OUTPUT_FORMAT_SQL_CREATE
    ) {
        fprintf(f, ",");
    }
    else if (format == OUTPUT_FORMAT_INFO_SEP) {
        fprintf(f, "\x1f"); // Unit Separator
    }
}

static void printRecordEnd (
    FILE *f,
    enum OutputOption format,
    int is_single_column
) {
    if (format == OUTPUT_FORMAT_TAB) {
        fprintf(f, "\n");
    }
    else if (format == OUTPUT_FORMAT_COMMA) {
        fprintf(f, "\n");
    }
    else if (format == OUTPUT_FORMAT_HTML) {
        fprintf(f, "</TR>\n");
    }
    else if (format == OUTPUT_FORMAT_JSON_ARRAY) {
        if (!is_single_column) {
            fprintf(f, "]");
        }
    }
    else if (format == OUTPUT_FORMAT_JSON) {
        fprintf(f, "}");
    }
    else if (
        format == OUTPUT_FORMAT_SQL_INSERT ||
        format == OUTPUT_FORMAT_SQL_VALUES ||
        format == OUTPUT_FORMAT_SQL_CREATE
    ) {
        fprintf(f, ")");
    }
    else if (format == OUTPUT_FORMAT_XML) {
        fprintf(f, "</record>");
    }
    else if (format == OUTPUT_FORMAT_TABLE) {
        fprintf(f, "|\n");
    }
}

static void printRecordSeparator (FILE *f, enum OutputOption format) {
    if (format == OUTPUT_FORMAT_JSON_ARRAY) {
        fprintf(f, ",");
    }
    else if (format == OUTPUT_FORMAT_JSON) {
        fprintf(f, ",");
    }
    else if (
        format == OUTPUT_FORMAT_SQL_INSERT ||
        format == OUTPUT_FORMAT_SQL_VALUES ||
        format == OUTPUT_FORMAT_SQL_CREATE
    ) {
        fprintf(f, ",\n");
    }
    else if (format == OUTPUT_FORMAT_INFO_SEP) {
        fprintf(f, "\x1e"); // Record Separator
    }
}
