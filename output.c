#include <stdio.h>
#include "output.h"
#include "query.h"
#include "limits.h"

void printResultLine (FILE *f, struct DB *db, int *field_indices, int field_count, int record_index, int result_count, int flags) {
    const char * field_sep = "\t";

    if (flags & OUTPUT_OPTION_COMMA) {
        field_sep = ",";
    }

    for (int j = 0; j < field_count; j++) {

        if (field_indices[j] == FIELD_STAR) {
            for (int k = 0; k < db->field_count; k++) {
                char value[VALUE_MAX_LENGTH];
                if (getRecordValue(db, record_index, k, value, VALUE_MAX_LENGTH) > 0) {
                    fprintf(f, "%s", value);
                }

                if (k < db->field_count - 1) {
                    fwrite(field_sep, 1, 1, f);
                }
            }
        } else if (field_indices[j] == FIELD_COUNT_STAR || field_indices[j] == FIELD_ROW_NUMBER) {
            // Same logic is recycled when printing result
            // FIELD_COUNT_STAR causes grouping and gets total at end
            // FIELD_ROW_NUMBER uses current matched result count at each iteration
            fprintf(f, "%d", result_count);
        } else if (field_indices[j] == FIELD_ROW_INDEX) {
            // FIELD_ROW_INDEX is the input line (0 indexed)
            fprintf(f, "%d", record_index);
        } else {
            char value[VALUE_MAX_LENGTH];
            if (getRecordValue(db, record_index, field_indices[j], value, VALUE_MAX_LENGTH)> 0) {
                fprintf(f, "%s", value);
            }
        }

        if (j < field_count - 1) {
            fwrite(field_sep, 1, 1, f);
        }
    }
    fprintf(f, "\n");
}

void printHeaderLine (FILE *f, struct DB *db, int *field_indices, int field_count, int flags) {
    const char * field_sep = "\t";

    if (flags & OUTPUT_OPTION_COMMA) {
        field_sep = ",";
    }

    for (int j = 0; j < field_count; j++) {

        if (field_indices[j] == FIELD_STAR) {
            for (int k = 0; k < db->field_count; k++) {
                fprintf(f, "%s", getFieldName(db, k));

                if (k < db->field_count - 1) {
                    fwrite(field_sep, 1, 1, f);
                }
            }
        } else if (field_indices[j] == FIELD_COUNT_STAR) {
            fprintf(f, "COUNT(*)");
        } else if (field_indices[j] == FIELD_ROW_NUMBER) {
            fprintf(f, "ROW_NUMBER()");
        } else if (field_indices[j] == FIELD_ROW_INDEX) {
            // FIELD_ROW_INDEX is the input line (0 indexed)
            fprintf(f, "rowid");
        } else {
            fprintf(f, "%s", getFieldName(db, field_indices[j]));
        }

        if (j < field_count - 1) {
            fwrite(field_sep, 1, 1, f);
        }
    }
    fprintf(f, "\n");
}