#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "../structs.h"
#include "../query/result.h"
#include "../evaluate/evaluate.h"
#include "../evaluate/predicates.h"
#include "../debug.h"
#include "../fatal.h"

int sequence_openDB (
    struct DB *db,
    const char *filename,
    __attribute__((unused)) char **resolved
) {
    if (strcmp(filename, "SEQUENCE") != 0) {
        return -1;
    }

    db->vfs = VFS_SEQUENCE;
    db->_record_count = 1e7;
    db->line_indices = NULL;
    db->field_count = 1;

    return 0;
}

void sequence_closeDB (__attribute__((unused)) struct DB *db) {}

int sequence_getFieldIndex (
    __attribute__((unused)) struct DB *db,
    const char *field
) {
    // Same as CALENDAR julian/rowid: primary key column is FIELD_ROW_INDEX so
    // optimiseRowidAlgebra and PLAN_TABLE_SCAN rowid bounds apply. SELECT *
    // still expands field 0 as "value" via getFieldName.
    if (strcmp(field, "value") == 0 || strcmp(field, "rowid") == 0) {
        return FIELD_ROW_INDEX;
    }

    return -1;
}

char *sequence_getFieldName (
    __attribute__((unused)) struct DB *db,
    int field_index
) {
    if (field_index == 0 || field_index == FIELD_ROW_INDEX)
        return "value";
    return "";
}

int sequence_getRecordCount (struct DB *db) {
    return db->_record_count;
}

int sequence_getRecordValue (
    __attribute__((unused)) struct DB *db,
    int record_index,
    __attribute__((unused)) int field_index,
    char *value,
    __attribute__((unused)) size_t value_max_length
) {
    return sprintf(value, "%d", record_index);
}

// All queries go through fullTableScan
enum IndexSearchType sequence_findIndex(
    __attribute__((unused)) struct DB *db,
    __attribute__((unused)) const char *table_name,
    __attribute__((unused)) struct Node *node,
    __attribute__((unused)) int index_type_flags,
    __attribute__((unused)) char **resolved
) {
    return INDEX_NONE;
}

static int isSequenceValueNode (struct Node *node) {
    return node->function == FUNC_UNITY && (
        node->field.index == FIELD_ROW_INDEX ||
        node->field.index == 0
    );
}

static int igcd (int a, int b) {
    if (a < 0) a = -a;
    if (b < 0) b = -b;
    while (b != 0) {
        int t = b;
        b = a % b;
        a = t;
    }
    return a;
}

/**
 * Combine x ≡ a (mod m) and x ≡ b (mod n) into x ≡ *out_rem (mod *out_mod).
 * @return 1 on success, 0 if incongruent or invalid moduli
 */
static int combineModEqual (
    int m,
    int a,
    int n,
    int b,
    int *out_mod,
    int *out_rem
) {
    if (m <= 0 || n <= 0) {
        return 0;
    }

    a %= m;
    if (a < 0) {
        a += m;
    }
    b %= n;
    if (b < 0) {
        b += n;
    }

    int g = igcd(m, n);
    if ((a - b) % g != 0) {
        return 0;
    }

    // Find k such that a + m*k ≡ b (mod n), then modulus is lcm(m,n).
    int m1 = m / g;
    int n1 = n / g;
    int target = ((b - a) / g) % n1;
    if (target < 0) {
        target += n1;
    }

    // Modular inverse of m1 modulo n1 (gcd is 1).
    int old_r = m1;
    int r = n1;
    int old_s = 1;
    int s = 0;
    while (r != 0) {
        int q = old_r / r;
        int tmp = r;
        r = old_r - q * r;
        old_r = tmp;
        tmp = s;
        s = old_s - q * s;
        old_s = tmp;
    }
    int inv = old_s % n1;
    if (inv < 0) {
        inv += n1;
    }

    int k = (int)(((long)inv * (long)target) % n1);
    long rem = (long)a + (long)m * (long)k;
    long mod = (long)m1 * (long)n;

    *out_mod = (int)mod;
    *out_rem = (int)(rem % mod);
    if (*out_rem < 0) {
        *out_rem += *out_mod;
    }
    return 1;
}

/**
 * Guaranteed that all predicates are on this table
 *
 * @return int number of matched rows
 */
int sequence_fullTableAccess (
    struct DB *db,
    RowListIndex list_id,
    struct Node *predicates,
    int predicate_count,
    int limit_value
) {
    // We might get a single AND node rather than a list
    if (predicate_count == 1 && predicates[0].function == OPERATOR_AND) {
        // Replace node list with our child list (count before reassignment)
        predicate_count = predicates[0].child_count;
        predicates = predicates[0].children;
    }

    // Inclusive
    // Rest of system can't cope with negative RowIDs
    int start = 0;
    // Exclusive
    int end = db->_record_count;

    int step = 1;
    int step_offset = 0;
    int have_mod = 0;

    int have_unprocessed_predicates = 0;

    // Establish limits from predicates
    for (int i = 0; i < predicate_count; i++) {
        struct Node *predicate = &predicates[i];

        // Must be an operator
        if ((predicate->function & MASK_FUNC_FAMILY) != FUNC_FAM_OPERATOR) {
            have_unprocessed_predicates = 1;
            continue;
        }

        enum Function op = predicate->function;

        struct Node *node_left = &predicate->children[0];
        struct Node *node_right = &predicate->children[1];

        struct Field *field_left = (struct Field *)node_left;
        struct Field *field_right = (struct Field *)node_right;

        // Prep: We need field on the left and constant on the right, swap if
        // necessary
        normalisePredicate(predicate);

        // We're only looking for constants (which are direct node constants)
        if (node_right->function != FUNC_UNITY) {
            have_unprocessed_predicates = 1;
            continue;
        }

        if (isSequenceValueNode(node_left)) {

            int value = atoi(field_right->text);

            if (op == OPERATOR_EQ) {
                start = MAX(start, value);
                end = MIN(end, value + 1);
            }
            else if (op == OPERATOR_LT) {
                end = MIN(end, value);
            }
            else if (op == OPERATOR_LE) {
                end = MIN(end, value + 1);
            }
            else if (op == OPERATOR_GT) {
                start = MAX(start, value + 1);
            }
            else if (op == OPERATOR_GE) {
                start = MAX(start, value);
            }
            else {
                // NE, LIKE, etc. — fall back to row evaluation
                have_unprocessed_predicates = 1;
            }

            (void)field_left;
        }
        else if (node_left->function == FUNC_MOD) {
            struct Node *node_left_left = &node_left->children[0];
            struct Node *node_left_right = &node_left->children[1];

            // Optimised arithmetic progression: value % k = n
            // (column on the left of %, constants elsewhere). Forms like
            // k % value = 0 must be evaluated per row — do not drop them.
            if (
                op != OPERATOR_EQ ||
                !isSequenceValueNode(node_left_left) ||
                node_left_right->function != FUNC_UNITY ||
                node_left_right->field.index != FIELD_CONSTANT
            ) {
                have_unprocessed_predicates = 1;
                continue;
            }

            int mod = atoi(node_left_right->field.text);
            int rem = atoi(field_right->text);

            if (mod <= 0) {
                have_unprocessed_predicates = 1;
                continue;
            }

            if (rem < 0) {
                rem %= mod;
                if (rem < 0) {
                    rem += mod;
                }
            }

            if (rem >= mod) {
                // Impossible
                start = 0;
                end = 0;
                continue;
            }

            if (!have_mod) {
                step = mod;
                step_offset = rem;
                have_mod = 1;
            }
            else {
                int new_step = 0;
                int new_rem = 0;
                if (combineModEqual(
                        step,
                        step_offset,
                        mod,
                        rem,
                        &new_step,
                        &new_rem))
                {
                    step = new_step;
                    step_offset = new_rem;
                }
                else if ((step_offset - rem) % igcd(step, mod) != 0) {
                    // Incongruent residues → empty result
                    start = 0;
                    end = 0;
                }
                else {
                    // Compatible but could not combine; keep step, filter rest
                    have_unprocessed_predicates = 1;
                }
            }
        }
        else {
            have_unprocessed_predicates = 1;
        }
    }

    if (step <= 0) {
        return 0;
    }

    int start_offset = (step - (start % step) + step_offset) % step;

    #ifdef DEBUG
    if (debug_verbosity >= 3) {
        fprintf(stderr, "[SEQUENCE] start: %d, end: %d, step: %d\n", start, end, step);
    }
    #endif

    if (end == db->_record_count) {
        fprintf(stderr, "Error: Unbounded SEQUENCE\n");
        csvdb_fatal(-1);
    }

    struct Table table;
    table.db = db;

    struct RowList *row_list = getRowList(list_id);
    int start_row_count = row_list->row_count;
    for (int i = start + start_offset; i < end; i += step) {
        if (limit_value >= 0 && row_list->row_count >= (unsigned)limit_value) {
            break;
        }

        if (have_unprocessed_predicates) {
            // If we have unprocessed predicates then this is necessary
            int matching = evaluateOperatorNodeListAND(
                &table,
                ROWLIST_ROWID,
                i,
                predicates,
                predicate_count
            );

            if (matching) {
                // Add to result set
                appendRowID(row_list, i);
            }
        }
        else {
            appendRowID(row_list, i);
        }
    }

    return row_list->row_count - start_row_count;
}
