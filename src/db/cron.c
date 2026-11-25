#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "cron.h"
#include "../functions/date.h"
#include "../evaluate/predicates.h"
#include "../evaluate/evaluate.h"
#include "../query/result.h"
#include "../fatal.h"

#define COL_DATETIME 0
#define COL_DATE     1
#define COL_JULIAN   2
#define COL_HOUR     3
#define COL_MINUTE   4
#define COL_WEEKDAY  5
#define COL_EXPR     6

#define CRON_MIN 0
#define CRON_HOUR 1
#define CRON_DOM 2
#define CRON_MONTH 3
#define CRON_DOW 4

struct CronHit {
    int julian;
    int hour;
    int minute;
};

struct CronState {
    char expr[128];
    /* bitsets: minute 0-59, hour 0-23, dom 1-31, month 1-12, dow 0-7 */
    unsigned char minute[60];
    unsigned char hour[24];
    unsigned char dom[32];
    unsigned char month[13];
    unsigned char dow[8];
    int dom_star;
    int dow_star;
    struct CronHit *hits;
    int hit_count;
    int hit_cap;
};

static char *field_names[] = {
    "datetime",
    "date",
    "julian",
    "hour",
    "minute",
    "weekday",
    "expr",
};

static int parseCronField (
    const char *text,
    int min_v,
    int max_v,
    unsigned char *out,
    int out_len
) {
    for (int i = 0; i < out_len; i++) {
        out[i] = 0;
    }

    if (strcmp(text, "*") == 0) {
        for (int i = min_v; i <= max_v && i < out_len; i++) {
            out[i] = 1;
        }
        return 1;
    }

    const char *p = text;
    while (*p) {
        while (*p == ' ') {
            p++;
        }
        if (*p == '\0') {
            break;
        }

        int start;
        int end;
        int step = 1;

        if (*p == '*') {
            start = min_v;
            end = max_v;
            p++;
        }
        else {
            if (!isdigit((unsigned char)*p)) {
                return 0;
            }
            start = (int)strtol(p, (char **)&p, 10);
            end = start;
            if (*p == '-') {
                p++;
                if (!isdigit((unsigned char)*p)) {
                    return 0;
                }
                end = (int)strtol(p, (char **)&p, 10);
            }
        }

        if (*p == '/') {
            p++;
            if (!isdigit((unsigned char)*p)) {
                return 0;
            }
            step = (int)strtol(p, (char **)&p, 10);
            if (step <= 0) {
                return 0;
            }
            if (start == end && text[0] != '*') {
                /* N/step means N, N+step, ... max */
                end = max_v;
            }
        }

        if (start < min_v || end > max_v || start > end) {
            return 0;
        }

        for (int v = start; v <= end; v += step) {
            if (v >= 0 && v < out_len) {
                out[v] = 1;
            }
        }

        if (*p == ',') {
            p++;
            continue;
        }
        if (*p == '\0') {
            break;
        }
        return 0;
    }

    return 1;
}

static int parseCronExpr (struct CronState *state, const char *expr) {
    char buf[128];
    strncpy(buf, expr, sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';

    char *fields[5];
    int n = 0;
    char *p = buf;
    while (n < 5) {
        while (*p == ' ') {
            p++;
        }
        if (*p == '\0') {
            break;
        }
        fields[n++] = p;
        while (*p && *p != ' ') {
            p++;
        }
        if (*p == ' ') {
            *p++ = '\0';
        }
    }
    if (n != 5) {
        return 0;
    }

    if (!parseCronField(fields[0], 0, 59, state->minute, 60)) {
        return 0;
    }
    if (!parseCronField(fields[1], 0, 23, state->hour, 24)) {
        return 0;
    }
    state->dom_star = strcmp(fields[2], "*") == 0;
    if (!parseCronField(fields[2], 1, 31, state->dom, 32)) {
        return 0;
    }
    if (!parseCronField(fields[3], 1, 12, state->month, 13)) {
        return 0;
    }
    state->dow_star = strcmp(fields[4], "*") == 0;
    if (!parseCronField(fields[4], 0, 7, state->dow, 8)) {
        return 0;
    }
    /* 7 ≡ Sunday ≡ 0 */
    if (state->dow[7]) {
        state->dow[0] = 1;
    }
    if (state->dow[0]) {
        state->dow[7] = 1;
    }

    snprintf(state->expr, sizeof(state->expr), "%s", expr);
    return 1;
}

static int cronMatchesDay (struct CronState *state, struct DateTime *dt) {
    if (!state->month[dt->month]) {
        return 0;
    }

    int csvdb_wd = datetimeGetWeekDay(dt); /* Mon=1..Sun=7 */
    int cron_dow = csvdb_wd == 7 ? 0 : csvdb_wd;

    int dom_ok = state->dom_star || state->dom[dt->day];
    int dow_ok = state->dow_star || state->dow[cron_dow];

    /* Vixie: if both DOM and DOW are restricted, match either */
    if (!state->dom_star && !state->dow_star) {
        return dom_ok || dow_ok;
    }
    return dom_ok && dow_ok;
}

static int extractQuotedSpec (const char *inside, char *out, size_t out_sz) {
    while (*inside == ' ') {
        inside++;
    }
    char quote = 0;
    if (*inside == '\'' || *inside == '"') {
        quote = *inside++;
    }
    size_t n = 0;
    while (*inside && *inside != quote && !(quote == 0 && *inside == ')')) {
        if (n + 1 >= out_sz) {
            return 0;
        }
        out[n++] = *inside++;
    }
    out[n] = '\0';
    if (quote) {
        if (*inside != quote) {
            return 0;
        }
        inside++;
    }
    while (*inside == ' ') {
        inside++;
    }
    return *inside == '\0';
}

int cron_openDB (
    struct DB *db,
    const char *filename,
    __attribute__((unused)) char **resolved
) {
    if (strncmp(filename, "CRON(", 5) != 0) {
        return -1;
    }

    size_t len = strlen(filename);
    if (len < 7 || filename[len - 1] != ')') {
        return -1;
    }

    char inner[128];
    if (len - 6 >= sizeof(inner)) {
        return -1;
    }
    memcpy(inner, filename + 5, len - 6);
    inner[len - 6] = '\0';

    char spec[128];
    if (!extractQuotedSpec(inner, spec, sizeof(spec))) {
        return -1;
    }

    struct CronState *state = calloc(1, sizeof(*state));
    if (state == NULL) {
        return -1;
    }
    if (!parseCronExpr(state, spec)) {
        free(state);
        fprintf(stderr, "Error: invalid cron expression '%s'\n", spec);
        return -1;
    }

    db->vfs = VFS_CRON;
    /* Nominal capacity for createRowList before fullTableAccess expands hits */
    db->_record_count = 10000000;
    db->field_count = (int)(sizeof(field_names) / sizeof(field_names[0]));
    db->line_indices = NULL;
    db->file = NULL;
    db->data = (char *)state;

    return 0;
}

void cron_closeDB (struct DB *db) {
    if (db->data != NULL) {
        struct CronState *state = (struct CronState *)db->data;
        free(state->hits);
        free(state);
        db->data = NULL;
    }
}

int cron_getFieldIndex (
    __attribute__((unused)) struct DB *db,
    const char *field
) {
    if (strcmp(field, "julian") == 0 || strcmp(field, "rowid") == 0) {
        /* julian is not the rowid for CRON; only explicit name */
    }
    for (int i = 0; i < (int)(sizeof(field_names) / sizeof(field_names[0])); i++) {
        if (strcmp(field, field_names[i]) == 0) {
            return i;
        }
    }
    return -1;
}

char *cron_getFieldName (
    __attribute__((unused)) struct DB *db,
    int field_index
) {
    return field_names[field_index];
}

int cron_getRecordCount (struct DB *db) {
    return db->_record_count;
}

int cron_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
) {
    struct CronState *state = (struct CronState *)db->data;
    if (state == NULL || record_index < 0 || record_index >= state->hit_count) {
        return 0;
    }

    struct CronHit *hit = &state->hits[record_index];
    struct DateTime dt = {0};
    datetimeFromJulian(&dt, hit->julian);
    dt.hour = (short)hit->hour;
    dt.minute = (unsigned char)hit->minute;
    dt.second = 0;
    dt.tz_offset_minutes = TZ_LOCAL;

    if (field_index == COL_DATETIME) {
        return sprintDateTime(value, &dt);
    }
    if (field_index == COL_DATE) {
        return sprintDate(value, &dt);
    }
    if (field_index == COL_JULIAN) {
        return snprintf(value, value_max_length, "%d", hit->julian);
    }
    if (field_index == COL_HOUR) {
        return snprintf(value, value_max_length, "%d", hit->hour);
    }
    if (field_index == COL_MINUTE) {
        return snprintf(value, value_max_length, "%d", hit->minute);
    }
    if (field_index == COL_WEEKDAY) {
        return snprintf(
            value,
            value_max_length,
            "%d",
            datetimeGetWeekDay(&dt)
        );
    }
    if (field_index == COL_EXPR) {
        return snprintf(value, value_max_length, "%s", state->expr);
    }
    return 0;
}

static void getSingleDateRange (
    struct Node *predicate,
    int *julian_start,
    int *julian_end
) {
    struct Field *field_left = (struct Field *)&predicate->children[0];
    struct Field *field_right = (struct Field *)&predicate->children[1];

    normalisePredicate(predicate);

    if (field_right->index != FIELD_CONSTANT) {
        return;
    }

    if (
        field_left->index == COL_JULIAN ||
        strcmp(field_left->text, "julian") == 0
    ) {
        *julian_start = atoi(field_right->text);
        *julian_end = *julian_start + 1;
        return;
    }

    if (
        field_left->index == COL_DATE ||
        strcmp(field_left->text, "date") == 0
    ) {
        struct DateTime dt = {0};
        if (!parseDate(field_right->text, &dt)) {
            return;
        }
        *julian_start = datetimeGetJulian(&dt);
        *julian_end = *julian_start + 1;
        return;
    }

    if (
        field_left->index == COL_DATETIME ||
        strcmp(field_left->text, "datetime") == 0
    ) {
        struct DateTime dt = {0};
        if (!parseDateOrDateTime(field_right->text, &dt)) {
            return;
        }
        *julian_start = datetimeGetJulian(&dt);
        *julian_end = *julian_start + 1;
        return;
    }
}

static int getDateRange (
    struct Node *predicates,
    int predicate_count,
    int *julian_start,
    int *julian_end
) {
    int have_unprocessed = 0;

    for (int i = 0; i < predicate_count; i++) {
        struct Node *predicate = &predicates[i];
        int j_start = -1;
        int j_end = -1;
        getSingleDateRange(predicate, &j_start, &j_end);

        enum Function op = predicate->function;

        if (j_start != -1 && j_end != -1) {
            if (op == OPERATOR_EQ) {
                if (*julian_start == -1 || j_start > *julian_start) {
                    *julian_start = j_start;
                }
                if (*julian_end == -1 || j_end < *julian_end) {
                    *julian_end = j_end;
                }
            }
            else if (op == OPERATOR_GT) {
                if (*julian_start == -1 || j_end > *julian_start) {
                    *julian_start = j_end;
                }
            }
            else if (op == OPERATOR_GE) {
                if (*julian_start == -1 || j_start > *julian_start) {
                    *julian_start = j_start;
                }
            }
            else if (op == OPERATOR_LT) {
                if (*julian_end == -1 || j_start < *julian_end) {
                    *julian_end = j_start;
                }
            }
            else if (op == OPERATOR_LE) {
                if (*julian_end == -1 || j_end < *julian_end) {
                    *julian_end = j_end;
                }
            }
            else {
                have_unprocessed = 1;
            }
        }
        else {
            have_unprocessed = 1;
        }
    }

    return have_unprocessed;
}

static int pushHit (struct CronState *state, int julian, int hour, int minute) {
    if (state->hit_count >= state->hit_cap) {
        int new_cap = state->hit_cap == 0 ? 64 : state->hit_cap * 2;
        struct CronHit *n = realloc(
            state->hits,
            (size_t)new_cap * sizeof(*n)
        );
        if (n == NULL) {
            return 0;
        }
        state->hits = n;
        state->hit_cap = new_cap;
    }
    state->hits[state->hit_count].julian = julian;
    state->hits[state->hit_count].hour = hour;
    state->hits[state->hit_count].minute = minute;
    state->hit_count++;
    return 1;
}

int cron_fullTableAccess (
    struct DB *db,
    RowListIndex list_id,
    struct Node *predicates,
    int predicate_count,
    int limit_value
) {
    struct CronState *state = (struct CronState *)db->data;
    state->hit_count = 0;

    int julian = -1;
    int max_julian = -1;
    int have_unprocessed = getDateRange(
        predicates,
        predicate_count,
        &julian,
        &max_julian
    );

    if (julian < 0 || max_julian < 0 || julian >= max_julian) {
        if (limit_value > 0 && julian < 0) {
            fprintf(
                stderr,
                "Error: Unbounded CRON. Add date/julian predicates to limit "
                "output\n"
            );
            csvdb_fatal(-1);
        }
        fprintf(
            stderr,
            "Error: Unbounded CRON. Add date/julian predicates to limit "
            "output\n"
        );
        csvdb_fatal(-1);
    }

    if (limit_value < 0) {
        limit_value = 10000000;
    }

    int count = 0;
    struct Table table;
    table.db = db;

    for (int j = julian; j < max_julian; j++) {
        struct DateTime dt = {0};
        datetimeFromJulian(&dt, j);
        if (!cronMatchesDay(state, &dt)) {
            continue;
        }

        for (int h = 0; h < 24; h++) {
            if (!state->hour[h]) {
                continue;
            }
            for (int m = 0; m < 60; m++) {
                if (!state->minute[m]) {
                    continue;
                }

                if (!pushHit(state, j, h, m)) {
                    fprintf(stderr, "Error: out of memory expanding CRON\n");
                    csvdb_fatal(-1);
                }

                int rowid = state->hit_count - 1;
                int matching = 1;
                if (have_unprocessed) {
                    matching = evaluateOperatorNodeListAND(
                        &table,
                        ROWLIST_ROWID,
                        rowid,
                        predicates,
                        predicate_count
                    );
                }

                if (matching) {
                    appendRowID(getRowList(list_id), rowid);
                    count++;
                }
                else {
                    /* drop non-matching hit */
                    state->hit_count--;
                }

                if (count >= limit_value) {
                    return count;
                }
            }
        }
    }

    return count;
}
