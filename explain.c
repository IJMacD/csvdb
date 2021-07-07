#include <string.h>

#include "explain.h"
#include "query.h"
#include "predicates.h"
#include "indices.h"
#include "limits.h"

#define COVERING_INDEX_SUPPORT 0

int log_10 (int value);

int explain_select_query (
    struct Query *q,
    int output_flags
) {
    struct DB db;

    if (openDB(&db, q->table) != 0) {
        fprintf(stderr, "File not found: '%s'\n", q->table);
        return -1;
    }

    if (output_flags & OUTPUT_FLAG_HEADERS) {
        printf("ID\tOperation\t\tName\tRows\tCost\n");
    }

    int row_estimate = db.record_count;
    int log_rows = log_10(row_estimate);
    int needs_table_access = 1;

    // If we supported covering indices it would save a table lookup
    if (COVERING_INDEX_SUPPORT && (q->field_count == 1)) {
        if ((q->flags & FLAG_HAVE_PREDICATE) && strcmp(q->fields, q->predicate_field) == 0) {
            needs_table_access = 0;
        }
        else if ((q->flags & FLAG_ORDER) && strcmp(q->fields, q->order_field) == 0) {
            needs_table_access = 0;
        }
        else if (strcmp(q->fields, "COUNT(*)") == 0) {
            needs_table_access = 0;
        }
    }

    int op = 0;

    if (q->flags & FLAG_GROUP) {
        printf("%d\tSELECT STATEMENT\t\t%d\n", op++, 1);
    } else {
        printf("%d\tSELECT STATEMENT\t\n", op++);
    }

    if ((q->flags & FLAG_GROUP) && !(q->flags & FLAG_HAVE_PREDICATE)) {
        if (needs_table_access) {
            printf("%d\tTABLE ACCESS BY ROWID\t%s\t%d\t%d\n", op++, q->table, 1, 1);
        }
        closeDB(&db);
        return 0;
    }

    if (q->flags & FLAG_PRIMARY_KEY_SEARCH) {
        if (q->predicate_op == OPERATOR_EQ) {
            int cost = log_rows;
            if (needs_table_access) {
                printf("%d\tTABLE ACCESS BY ROWID\t%s\t%d\t%d\n", op++, q->table, 1, cost);
            }
            printf("%d\tINDEX UNIQUE SCAN\t%s\t%d\t%d\n", op++, q->predicate_field, 1, cost);
            closeDB(&db);
            return 0;
        }

        row_estimate /= 2;

        if ((q->flags & FLAG_ORDER) && !(q->flags & FLAG_GROUP) && strcmp(q->predicate_field, q->order_field) != 0) {
            printf("%d\tSORT ORDER BY\t\t\t%d\t%ld\n", op++, row_estimate, (long)row_estimate * row_estimate);
        }

        if (needs_table_access) {
            printf("%d\tTABLE ACCESS BY ROWID\t%s\t%d\t%d\n", op++, q->table, row_estimate, row_estimate);
        }
        printf("%d\tINDEX RANGE SCAN\t%s\t%d\t%d\n", op++, q->predicate_field, row_estimate, row_estimate);
        closeDB(&db);
        return 0;
    }

    if (q->flags & FLAG_HAVE_PREDICATE && q->predicate_op != OPERATOR_LIKE) {

        struct DB index_db;

        if (findIndex(&index_db, q->table, q->predicate_field, INDEX_UNIQUE) == 0) {
            if (q->predicate_op == OPERATOR_EQ) {
                int cost = log_rows;
                if (needs_table_access) {
                    printf("%d\tTABLE ACCESS BY ROWID\t%s\t%d\t%d\n", op++, q->table, 1, cost);
                }
                printf("%d\tINDEX UNIQUE SCAN\t%s\t%d\t%d\n", op++, q->predicate_field, 1, cost);

                closeDB(&index_db);
                closeDB(&db);
                return 0;
            }

            row_estimate /= 2;

            if ((q->flags & FLAG_ORDER) && !(q->flags & FLAG_GROUP) && strcmp(q->predicate_field, q->order_field) != 0) {
                printf("%d\tSORT ORDER BY\t\t\t%d\t%ld\n", op++, row_estimate, (long)row_estimate * row_estimate);
            }

            if (needs_table_access) {
                printf("%d\tTABLE ACCESS BY ROWID\t%s\t%d\t%d\n", op++, q->table, row_estimate, row_estimate);
            }
            printf("%d\tINDEX RANGE SCAN\t%s\t%d\t%d\n", op++, q->predicate_field, row_estimate, row_estimate);
            closeDB(&index_db);
            closeDB(&db);
            return 0;
        }

        if (q->predicate_op == OPERATOR_EQ) {
            // 10,000 is a wild guess at index statistics
            row_estimate = row_estimate / 10000;
        } else if (q->predicate_op == OPERATOR_NE) {
            // 10,000 is a wild guess at index statistics
            row_estimate = row_estimate - (row_estimate / 10000);
        } else {
            row_estimate = row_estimate / 2;
        }

        if (row_estimate < 1) {
            row_estimate = 1;
        }

        if (findIndex(&index_db, q->table, q->predicate_field, INDEX_ANY) == 0) {
            int cost = row_estimate;

            if (q->predicate_op == OPERATOR_EQ) {
                cost = log_rows * 2;
            }

            if ((q->flags & FLAG_ORDER) && !(q->flags & FLAG_GROUP) && strcmp(q->predicate_field, q->order_field) != 0) {
                printf("%d\tSORT ORDER BY\t\t\t%d\t%ld\n", op++, row_estimate, (long)row_estimate * row_estimate);
            }

            if (needs_table_access) {
                printf("%d\tTABLE ACCESS BY ROWID\t%s\t%d\t%d\n", op++, q->table, row_estimate, cost);
            }
            printf("%d\tINDEX RANGE SCAN\t%s\t%d\t%d\n", op++, q->predicate_field, row_estimate, cost);

            closeDB(&index_db);
            closeDB(&db);
            return 0;
        }
    }

    if (!(q->flags & FLAG_HAVE_PREDICATE) && (q->flags & FLAG_ORDER)) {
        // Before we do a full table scan... we have one more opportunity to use an index
        // To save a sort later, see if we can use an index for ordering now
        struct DB index_db;
        if (findIndex(&index_db, q->table, q->order_field, INDEX_ANY) == 0) {
            if (needs_table_access) {
                printf("%d\tTABLE ACCESS BY ROWID\t%s\t%d\t%d\n", op++, q->table, row_estimate, row_estimate);
            }
            printf("%d\tINDEX RANGE SCAN\t%s\t%d\t%d\n", op++, q->order_field, row_estimate, log_rows);

            closeDB(&index_db);
            closeDB(&db);
            return 0;
        }
    }

    if ((q->flags & FLAG_ORDER) && !(q->flags & FLAG_GROUP)) {
        printf("%d\tSORT ORDER BY\t\t\t%d\t%ld\n", op++, row_estimate, (long)row_estimate * row_estimate);
    }

    if (needs_table_access) {
        printf("%d\tTABLE ACCESS FULL\t%s\t%d\t%d\n", op++, q->table, db.record_count, db.record_count);
    }

    closeDB(&db);
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