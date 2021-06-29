#include <string.h>

#include "explain.h"
#include "query.h"
#include "predicates.h"
#include "indices.h"
#include "limits.h"

#define COVERING_INDEX_SUPPORT 0

int log_10 (int value);

int explain_select_query (
    const char *table,
    const char *fields,
    int field_count,
    int flags,
    int offset_value __attribute__((unused)),
    int limit_value __attribute__((unused)),
    const char *predicate_field,
    char predicate_op,
    const char *predicate_value __attribute__((unused)),
    const char *order_field __attribute__((unused)),
    int order_direction __attribute__((unused)),
    int output_flags
) {
    struct DB db;

    if (openDB(&db, table) != 0) {
        fprintf(stderr, "File not found: '%s'\n", table);
        return -1;
    }

    if (output_flags & OUTPUT_FLAG_HEADERS) {
        printf("ID\tOperation\t\tName\tRows\tCost\n");
    }

    int row_estimate = db.record_count;
    int log_rows = log_10(row_estimate);
    int needs_table_access = 1;

    // If we supported covering indices it would save a table lookup
    if (COVERING_INDEX_SUPPORT && (field_count == 1)) {
        if ((flags & FLAG_HAVE_PREDICATE) && strcmp(fields, predicate_field) == 0) {
            needs_table_access = 0;
        }
        else if ((flags & FLAG_ORDER) && strcmp(fields, order_field) == 0) {
            needs_table_access = 0;
        }
        else if (strcmp(fields, "COUNT(*)") == 0) {
            needs_table_access = 0;
        }
    }

    int op = 0;

    if (flags & FLAG_GROUP) {
        printf("%d\tSELECT STATEMENT\t\t%d\n", op++, 1);
    } else {
        printf("%d\tSELECT STATEMENT\t\n", op++);
    }

    if ((flags & FLAG_GROUP) && !(flags & FLAG_HAVE_PREDICATE)) {
        if (needs_table_access) {
            printf("%d\tTABLE ACCESS BY ROWID\t%s\t%d\t%d\n", op++, table, 1, 1);
        }
        return 0;
    }

    if (flags & FLAG_PRIMARY_KEY_SEARCH) {
        if (predicate_op == OPERATOR_EQ) {
            int cost = log_rows;
            if (needs_table_access) {
                printf("%d\tTABLE ACCESS BY ROWID\t%s\t%d\t%d\n", op++, table, 1, cost);
            }
            printf("%d\tINDEX UNIQUE SCAN\t%s\t%d\t%d\n", op++, predicate_field, 1, cost);
            return 0;
        }

        row_estimate /= 2;

        if ((flags & FLAG_ORDER) && !(flags & FLAG_GROUP) && strcmp(predicate_field, order_field) != 0) {
            printf("%d\tSORT ORDER BY\t\t\t%d\t%ld\n", op++, row_estimate, (long)row_estimate * row_estimate);
        }

        if (needs_table_access) {
            printf("%d\tTABLE ACCESS BY ROWID\t%s\t%d\t%d\n", op++, table, row_estimate, row_estimate);
        }
        printf("%d\tINDEX RANGE SCAN\t%s\t%d\t%d\n", op++, predicate_field, row_estimate, row_estimate);
        return 0;
    }

    if (flags & FLAG_HAVE_PREDICATE) {
        char index_filename[TABLE_MAX_LENGTH + 10];
        sprintf(index_filename, "%s.unique.csv", predicate_field);

        struct DB index_db;

        if (openDB(&index_db, index_filename) == 0) {
            if (predicate_op == OPERATOR_EQ) {
                int cost = log_rows;
                if (needs_table_access) {
                    printf("%d\tTABLE ACCESS BY ROWID\t%s\t%d\t%d\n", op++, table, 1, cost);
                }
                printf("%d\tINDEX UNIQUE SCAN\t%s\t%d\t%d\n", op++, predicate_field, 1, cost);

                return 0;
            }

            row_estimate /= 2;

            if ((flags & FLAG_ORDER) && !(flags & FLAG_GROUP) && strcmp(predicate_field, order_field) != 0) {
                printf("%d\tSORT ORDER BY\t\t\t%d\t%ld\n", op++, row_estimate, (long)row_estimate * row_estimate);
            }

            if (needs_table_access) {
                printf("%d\tTABLE ACCESS BY ROWID\t%s\t%d\t%d\n", op++, table, row_estimate, row_estimate);
            }
            printf("%d\tINDEX RANGE SCAN\t%s\t%d\t%d\n", op++, predicate_field, row_estimate, row_estimate);
            return 0;
        }
    }

    if (flags & FLAG_HAVE_PREDICATE) {

        if (flags & FLAG_HAVE_PREDICATE) {
            if (predicate_op == OPERATOR_EQ) {
                // 10,000 is a wild guess at index statistics
                row_estimate = row_estimate / 10000;
            } else if (predicate_op == OPERATOR_NE) {
                // 10,000 is a wild guess at index statistics
                row_estimate = row_estimate - (row_estimate / 10000);
            } else {
                row_estimate = row_estimate / 2;
            }

            if (row_estimate < 1) {
                row_estimate = 1;
            }
        }

        char index_filename[TABLE_MAX_LENGTH + 10];
        sprintf(index_filename, "%s.index.csv", predicate_field);

        struct DB index_db;

        if (openDB(&index_db, index_filename) == 0) {
            int cost = row_estimate;

            if (predicate_op == OPERATOR_EQ) {
                cost = log_rows * 2;
            }

            if ((flags & FLAG_ORDER) && !(flags & FLAG_GROUP) && strcmp(predicate_field, order_field) != 0) {
                printf("%d\tSORT ORDER BY\t\t\t%d\t%ld\n", op++, row_estimate, (long)row_estimate * row_estimate);
            }

            if (needs_table_access) {
                printf("%d\tTABLE ACCESS BY ROWID\t%s\t%d\t%d\n", op++, table, row_estimate, cost);
            }
            printf("%d\tINDEX RANGE SCAN\t%s\t%d\t%d\n", op++, predicate_field, row_estimate, cost);

            return 0;
        }
    }

    if (!(flags & FLAG_HAVE_PREDICATE) && (flags & FLAG_ORDER)) {
        // Before we do a full table scan... we have one more opportunity to use an index
        // To save a sort later, see if we can use an index for ordering now
        struct DB index_db;
        if (findIndex(&index_db, order_field, INDEX_ANY) == 0) {
            if (needs_table_access) {
                printf("%d\tTABLE ACCESS BY ROWID\t%s\t%d\t%d\n", op++, table, row_estimate, row_estimate);
            }
            printf("%d\tINDEX RANGE SCAN\t%s\t%d\t%d\n", op++, order_field, row_estimate, log_rows);

            return 0;
        }
    }

    if ((flags & FLAG_ORDER) && !(flags & FLAG_GROUP)) {
        printf("%d\tSORT ORDER BY\t\t\t%d\t%ld\n", op++, row_estimate, (long)row_estimate * row_estimate);
    }

    if (needs_table_access) {
        printf("%d\tTABLE ACCESS FULL\t%s\t%d\t%d\n", op++, table, db.record_count, db.record_count);
    }

    return 0;
}

int log_10 (int value) {
    int i = 0;
    while (value > 0) {
        value /= 10;
        i++;
    }
    return i;
}