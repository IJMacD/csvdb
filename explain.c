#include "explain.h"
#include "query.h"
#include "predicates.h"
#include "limits.h"

int log_10 (int value);

int explain_select_query (
    const char *table,
    const char *fields __attribute__((unused)),
    int field_count __attribute__((unused)),
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

    int op = 0;

    printf("%d\tSELECT STATEMENT\t\n", op++);

    if ((flags & FLAG_GROUP) && !(flags & FLAG_HAVE_PREDICATE)) {
        printf("%d\tTABLE ACCESS BY ROWID\t%s\t%d\t%d\n", op++, table, 1, 1);
        return 0;
    }

    if (flags & FLAG_PRIMARY_KEY_SEARCH) {
        if (predicate_op == OPERATOR_EQ) {
            int cost = log_rows;
            printf("%d\tTABLE ACCESS BY ROWID\t%s\t%d\t%d\n", op++, table, 1, cost);
            printf("%d\tINDEX UNIQUE SCAN\t%s\t%d\t%d\n", op++, predicate_field, 1, cost);
            return 0;
        }

        row_estimate /= 2;

        if (flags & FLAG_ORDER) {
            printf("%d\tSORT ORDER BY\t\t\t%d\t%ld\n", op++, row_estimate, (long)row_estimate * row_estimate);
        }

        printf("%d\tTABLE ACCESS BY ROWID\t%s\t%d\t%d\n", op++, table, row_estimate, row_estimate);
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
                printf("%d\tTABLE ACCESS BY ROWID\t%s\t%d\t%d\n", op++, table, 1, cost);
                printf("%d\tINDEX UNIQUE SCAN\t%s\t%d\t%d\n", op++, predicate_field, 1, cost);

                return 0;
            }

            row_estimate /= 2;

            if (flags & FLAG_ORDER) {
                printf("%d\tSORT ORDER BY\t\t\t%d\t%ld\n", op++, row_estimate, (long)row_estimate * row_estimate);
            }

            printf("%d\tTABLE ACCESS BY ROWID\t%s\t%d\t%d\n", op++, table, row_estimate, row_estimate);
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

        if (flags & FLAG_ORDER) {
            printf("%d\tSORT ORDER BY\t\t\t%d\t%ld\n", op++, row_estimate, (long)row_estimate * row_estimate);
        }

        char index_filename[TABLE_MAX_LENGTH + 10];
        sprintf(index_filename, "%s.index.csv", predicate_field);

        struct DB index_db;

        if (openDB(&index_db, index_filename) == 0) {
            int cost = row_estimate;

            if (predicate_op == OPERATOR_EQ) {
                cost = log_rows * 2;
            }

            printf("%d\tTABLE ACCESS BY ROWID\t%s\t%d\t%d\n", op++, table, row_estimate, cost);
            printf("%d\tINDEX RANGE SCAN\t%s\t%d\t%d\n", op++, predicate_field, row_estimate, cost);

            return 0;
        }
    }

    printf("%d\tTABLE ACCESS FULL\t%s\t%d\t%d\n", op++, table, db.record_count, db.record_count);

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