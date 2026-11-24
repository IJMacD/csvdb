#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "permutations.h"

#define MAX_PERM_N 10
#define MAX_COMB_N 20
#define MAX_COMB_K 10

struct PermState {
    int is_perm; /* 1 = PERMUTATIONS, 0 = COMBINATIONS */
    int n;
    int k;
    int count;
    /* "rank" + p0..p9 or c0..c9; size 16 silences -Wformat-truncation */
    char names[1 + MAX_PERM_N][16];
};

static long factorial (int n) {
    long f = 1;
    for (int i = 2; i <= n; i++) {
        f *= i;
    }
    return f;
}

static long binom (int n, int k) {
    if (k < 0 || k > n) {
        return 0;
    }
    if (k > n - k) {
        k = n - k;
    }
    long r = 1;
    for (int i = 1; i <= k; i++) {
        r = r * (n - k + i) / i;
    }
    return r;
}

static void unrankPerm (int n, int rank, int *out) {
    int available[MAX_PERM_N];
    for (int i = 0; i < n; i++) {
        available[i] = i;
    }

    long fact = factorial(n - 1);
    long rem = rank;

    for (int pos = 0; pos < n; pos++) {
        int idx = (int)(rem / fact);
        out[pos] = available[idx];
        for (int j = idx; j < n - pos - 1; j++) {
            available[j] = available[j + 1];
        }
        rem %= fact;
        if (pos < n - 1) {
            fact /= (n - 1 - pos);
        }
    }
}

/** Lexicographic unranking of k-subsets of {0..n-1}. */
static void unrankComb (int n, int k, int rank, int *out) {
    long r = rank;
    int start = 0;
    for (int i = 0; i < k; i++) {
        for (int x = start; x <= n - (k - i); x++) {
            long c = binom(n - x - 1, k - i - 1);
            if (r < c) {
                out[i] = x;
                start = x + 1;
                break;
            }
            r -= c;
        }
    }
}

int permutations_openDB (
    struct DB *db,
    const char *filename,
    __attribute__((unused)) char **resolved
) {
    int is_perm = 0;
    int n = 0;
    int k = 0;

    if (strncmp(filename, "PERMUTATIONS(", 13) == 0) {
        is_perm = 1;
        char *end = NULL;
        n = (int)strtol(filename + 13, &end, 10);
        if (end == filename + 13 || *end != ')') {
            return -1;
        }
        if (end[1] != '\0') {
            return -1;
        }
        if (n < 0 || n > MAX_PERM_N) {
            fprintf(
                stderr,
                "Error: PERMUTATIONS(n) requires 0 <= n <= %d\n",
                MAX_PERM_N
            );
            return -1;
        }
    }
    else if (strncmp(filename, "COMBINATIONS(", 13) == 0) {
        is_perm = 0;
        char *end = NULL;
        n = (int)strtol(filename + 13, &end, 10);
        if (end == filename + 13) {
            return -1;
        }
        while (*end == ' ') {
            end++;
        }
        if (*end != ',') {
            return -1;
        }
        end++;
        while (*end == ' ') {
            end++;
        }
        char *end2 = NULL;
        k = (int)strtol(end, &end2, 10);
        if (end2 == end) {
            return -1;
        }
        while (*end2 == ' ') {
            end2++;
        }
        if (*end2 != ')' || end2[1] != '\0') {
            return -1;
        }
        if (n < 0 || n > MAX_COMB_N || k < 0 || k > MAX_COMB_K || k > n) {
            fprintf(
                stderr,
                "Error: COMBINATIONS(n, k) requires 0 <= k <= n <= %d"
                " and k <= %d\n",
                MAX_COMB_N,
                MAX_COMB_K
            );
            return -1;
        }
    }
    else {
        return -1;
    }

    long count = is_perm ? factorial(n) : binom(n, k);
    if (count > 10000000L) {
        fprintf(stderr, "Error: combination/permutation too large\n");
        return -1;
    }

    struct PermState *state = calloc(1, sizeof(*state));
    if (state == NULL) {
        return -1;
    }
    state->is_perm = is_perm;
    state->n = n;
    state->k = k;
    state->count = (int)count;

    strcpy(state->names[0], "rank");
    int width = is_perm ? n : k;
    for (int i = 0; i < width; i++) {
        snprintf(
            state->names[i + 1],
            sizeof(state->names[i + 1]),
            "%s%d",
            is_perm ? "p" : "c",
            i
        );
    }

    db->vfs = is_perm ? VFS_PERMUTATIONS : VFS_COMBINATIONS;
    db->_record_count = (int)count;
    db->field_count = 1 + width;
    db->line_indices = NULL;
    db->file = NULL;
    db->data = (char *)state;

    return 0;
}

void permutations_closeDB (struct DB *db) {
    if (db->data != NULL) {
        free(db->data);
        db->data = NULL;
    }
}

int permutations_getFieldIndex (struct DB *db, const char *field) {
    struct PermState *state = (struct PermState *)db->data;
    if (strcmp(field, "rank") == 0 || strcmp(field, "rowid") == 0) {
        return FIELD_ROW_INDEX;
    }
    for (int i = 0; i < db->field_count; i++) {
        if (strcmp(field, state->names[i]) == 0) {
            if (i == 0) {
                return FIELD_ROW_INDEX;
            }
            return i;
        }
    }
    return -1;
}

char *permutations_getFieldName (struct DB *db, int field_index) {
    struct PermState *state = (struct PermState *)db->data;
    if (field_index == FIELD_ROW_INDEX) {
        return state->names[0];
    }
    if (field_index < 0 || field_index >= db->field_count) {
        return "";
    }
    return state->names[field_index];
}

int permutations_getRecordCount (struct DB *db) {
    return db->_record_count;
}

int permutations_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
) {
    struct PermState *state = (struct PermState *)db->data;

    if (field_index == FIELD_ROW_INDEX || field_index == 0) {
        return snprintf(value, value_max_length, "%d", record_index);
    }

    int out[MAX_PERM_N];
    if (state->is_perm) {
        unrankPerm(state->n, record_index, out);
    }
    else {
        unrankComb(state->n, state->k, record_index, out);
    }

    int slot = field_index - 1;
    int width = state->is_perm ? state->n : state->k;
    if (slot < 0 || slot >= width) {
        return 0;
    }

    return snprintf(value, value_max_length, "%d", out[slot]);
}
