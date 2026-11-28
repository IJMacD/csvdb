#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

#include "../structs.h"
#include "../functions/date.h"
#include "../functions/astro.h"
#include "../evaluate/predicates.h"
#include "../evaluate/evaluate.h"
#include "../query/result.h"
#include "../debug.h"
#include "../fatal.h"

#define COL_JULIAN              0
#define COL_DATE                1
#define COL_SOLAR_LONGITUDE     2
#define COL_EQUATION_OF_TIME    3
#define COL_SOLAR_DECLINATION   4
#define COL_MOON_PHASE          5
#define COL_MOON_ILLUMINATION   6
#define COL_MOON_AGE_DAYS       7
#define COL_LUNAR_LONGITUDE     8
#define COL_IS_NEW_MOON         9
#define COL_IS_FULL_MOON        10
#define COL_IS_FIRST_QUARTER    11
#define COL_IS_LAST_QUARTER     12
#define COL_SUN_RISE            13
#define COL_SUN_SET             14
#define COL_MOON_RISE           15
#define COL_MOON_SET            16
#define COL_IS_LUNAR_ECLIPSE    17
#define COL_IS_SOLAR_ECLIPSE    18

struct EphemerisState {
    int has_observer;
    double lat;
    double lon;
    char index_kind;
};

static char *field_names[] = {
    "julian",
    "date",
    "solarLongitude",
    "equationOfTime",
    "solarDeclination",
    "moonPhase",
    "moonIllumination",
    "moonAgeDays",
    "lunarLongitude",
    "isNewMoon",
    "isFullMoon",
    "isFirstQuarter",
    "isLastQuarter",
    "sunRise",
    "sunSet",
    "moonRise",
    "moonSet",
    "isLunarEclipse",
    "isSolarEclipse",
};

static struct EphemerisState *ephemerisState (struct DB *db) {
    return (struct EphemerisState *)db->data;
}

static void getSingleJulianRange (
    struct Node *predicate,
    int *julian_start,
    int *julian_end
);

static int getJulianRange (
    struct Node *predicates,
    int predicate_count,
    int *julian_start,
    int *julian_end
);

static int printDate (char *value, int max_length, struct DateTime date);

int ephemeris_openDB (
    struct DB *db,
    const char *filename,
    __attribute__((unused)) char **resolved
) {
    int has_observer = 0;
    double lat = 0;
    double lon = 0;

    if (strcmp(filename, "EPHEMERIS") == 0) {
        has_observer = 0;
    }
    else if (strncmp(filename, "EPHEMERIS(", 10) == 0) {
        const char *p = filename + 10;
        char *end = NULL;
        lat = strtod(p, &end);
        if (end == p) {
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
        p = end;
        lon = strtod(p, &end);
        if (end == p) {
            return -1;
        }
        while (*end == ' ') {
            end++;
        }
        if (*end != ')') {
            return -1;
        }
        if (end[1] != '\0') {
            return -1;
        }
        has_observer = 1;
    }
    else {
        return -1;
    }

    db->vfs = VFS_EPHEMERIS;
    db->_record_count = 1e7;
    db->line_indices = NULL;
    db->field_count = (int)(sizeof(field_names) / sizeof(field_names[0]));
    db->file = NULL;

    struct EphemerisState *state = calloc(1, sizeof(*state));
    if (state == NULL) {
        return -1;
    }
    state->has_observer = has_observer;
    state->lat = lat;
    state->lon = lon;
    state->index_kind = -1;
    db->data = (char *)state;

    return 0;
}

void ephemeris_closeDB (struct DB *db) {
    if (db->data != NULL) {
        free(db->data);
        db->data = NULL;
    }
}

int ephemeris_getFieldIndex (
    __attribute__((unused)) struct DB *db,
    const char *field
) {
    if (strcmp(field, "julian") == 0 || strcmp(field, "rowid") == 0) {
        return FIELD_ROW_INDEX;
    }

    int l = (int)(sizeof(field_names) / sizeof(field_names[0]));
    for (int i = 0; i < l; i++) {
        if (strcmp(field, field_names[i]) == 0) {
            return i;
        }
    }

    return -1;
}

char *ephemeris_getFieldName (
    __attribute__((unused)) struct DB *db,
    int field_index
) {
    return field_names[field_index];
}

int ephemeris_getRecordCount (struct DB *db) {
    return db->_record_count;
}

int ephemeris_getRecordValue (
    __attribute__((unused)) struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
) {
    if (db->field_count == 2 && field_index == 1) {
        return snprintf(value, value_max_length, "%d", record_index);
    }

    if (field_index == COL_JULIAN || field_index == FIELD_ROW_INDEX) {
        return snprintf(value, value_max_length, "%d", record_index);
    }

    struct DateTime dt = {0};
    datetimeFromJulian(&dt, record_index);

    if (field_index == COL_DATE) {
        return printDate(value, value_max_length, dt);
    }

    struct AstroDay today;
    struct AstroDay tomorrow;
    astroComputeDay(record_index, &today);
    /* Phase flags: event during civil day = crossed between midnight and next */
    astroComputeDay(record_index + 1, &tomorrow);

    if (field_index == COL_SOLAR_LONGITUDE) {
        return snprintf(value, value_max_length, "%.6f", today.solar_longitude);
    }
    if (field_index == COL_EQUATION_OF_TIME) {
        return snprintf(value, value_max_length, "%.6f", today.equation_of_time);
    }
    if (field_index == COL_SOLAR_DECLINATION) {
        return snprintf(
            value,
            value_max_length,
            "%.6f",
            today.solar_declination
        );
    }
    if (field_index == COL_MOON_PHASE) {
        return snprintf(value, value_max_length, "%.6f", today.moon_phase);
    }
    if (field_index == COL_MOON_ILLUMINATION) {
        return snprintf(
            value,
            value_max_length,
            "%.6f",
            today.moon_illumination
        );
    }
    if (field_index == COL_MOON_AGE_DAYS) {
        return snprintf(value, value_max_length, "%.6f", today.moon_age_days);
    }
    if (field_index == COL_LUNAR_LONGITUDE) {
        return snprintf(value, value_max_length, "%.6f", today.lunar_longitude);
    }
    if (field_index == COL_IS_NEW_MOON) {
        return snprintf(
            value,
            value_max_length,
            "%d",
            astroPhaseCrossed(today.moon_phase, tomorrow.moon_phase, 0.0)
        );
    }
    if (field_index == COL_IS_FULL_MOON) {
        return snprintf(
            value,
            value_max_length,
            "%d",
            astroPhaseCrossed(today.moon_phase, tomorrow.moon_phase, 0.5)
        );
    }
    if (field_index == COL_IS_FIRST_QUARTER) {
        return snprintf(
            value,
            value_max_length,
            "%d",
            astroPhaseCrossed(today.moon_phase, tomorrow.moon_phase, 0.25)
        );
    }
    if (field_index == COL_IS_LAST_QUARTER) {
        return snprintf(
            value,
            value_max_length,
            "%d",
            astroPhaseCrossed(today.moon_phase, tomorrow.moon_phase, 0.75)
        );
    }

    struct EphemerisState *state = ephemerisState(db);

    if (
        field_index == COL_SUN_RISE ||
        field_index == COL_SUN_SET ||
        field_index == COL_MOON_RISE ||
        field_index == COL_MOON_SET
    ) {
        if (state == NULL || !state->has_observer) {
            value[0] = '\0';
            return 0;
        }

        struct DateTime event = {0};
        int ok = 0;
        if (field_index == COL_SUN_RISE) {
            ok = astroRiseSet(
                record_index,
                state->lat,
                state->lon,
                today.solar_declination,
                today.sun_ra,
                -0.833,
                1,
                &event
            );
        }
        else if (field_index == COL_SUN_SET) {
            ok = astroRiseSet(
                record_index,
                state->lat,
                state->lon,
                today.solar_declination,
                today.sun_ra,
                -0.833,
                0,
                &event
            );
        }
        else if (field_index == COL_MOON_RISE) {
            ok = astroRiseSet(
                record_index,
                state->lat,
                state->lon,
                today.moon_decl,
                today.moon_ra,
                0.125,
                1,
                &event
            );
        }
        else if (field_index == COL_MOON_SET) {
            ok = astroRiseSet(
                record_index,
                state->lat,
                state->lon,
                today.moon_decl,
                today.moon_ra,
                0.125,
                0,
                &event
            );
        }

        if (!ok) {
            value[0] = '\0';
            return 0;
        }
        return sprintDateTime(value, &event);
    }

    if (
        field_index == COL_IS_LUNAR_ECLIPSE ||
        field_index == COL_IS_SOLAR_ECLIPSE
    ) {
        int lunar = 0;
        int solar = 0;
        astroEclipseFlags(record_index, &lunar, &solar);
        int flag = field_index == COL_IS_LUNAR_ECLIPSE ? lunar : solar;
        return snprintf(value, value_max_length, "%d", flag);
    }

    return 0;
}

enum IndexSearchType ephemeris_findIndex (
    struct DB *db,
    __attribute__((unused)) const char *table_name,
    struct Node *node,
    __attribute__((unused)) int index_type_flags,
    char **resolved
) {
    if (node->function != FUNC_UNITY) {
        return INDEX_NONE;
    }

    char *field_name = node->field.text;

    if (strcmp(field_name, "julian") == 0) {
        if (db != NULL) {
            const char *open_name =
                (table_name != NULL && table_name[0] != '\0')
                    ? table_name
                    : "EPHEMERIS";
            ephemeris_openDB(db, open_name, NULL);
            db->field_count = 2;
            if (db->data != NULL) {
                ephemerisState(db)->index_kind = COL_JULIAN;
            }
        }
        if (resolved != NULL) {
            *resolved = malloc(strlen("EPHEMERIS__julian") + 1);
            strcpy(*resolved, "EPHEMERIS__julian");
        }
        return INDEX_PRIMARY;
    }

    if (strcmp(field_name, "date") == 0) {
        if (db != NULL) {
            const char *open_name =
                (table_name != NULL && table_name[0] != '\0')
                    ? table_name
                    : "EPHEMERIS";
            ephemeris_openDB(db, open_name, NULL);
            db->field_count = 2;
            if (db->data != NULL) {
                ephemerisState(db)->index_kind = COL_DATE;
            }
        }
        if (resolved != NULL) {
            *resolved = malloc(strlen("EPHEMERIS__date") + 1);
            strcpy(*resolved, "EPHEMERIS__date");
        }
        return INDEX_UNIQUE;
    }

    return INDEX_NONE;
}

int ephemeris_fullTableAccess (
    struct DB *db,
    RowListIndex list_id,
    struct Node *predicates,
    int predicate_count,
    int limit_value
) {
    int julian = -1;
    int max_julian = -1;

    int have_unprocessed = getJulianRange(
        predicates,
        predicate_count,
        &julian,
        &max_julian
    );

    if (julian < 0) {
        julian = 0;
    }
    if (max_julian < 0) {
        max_julian = db->_record_count;
    }
    if (limit_value < 0) {
        limit_value = db->_record_count;
    }

    if (max_julian == db->_record_count && limit_value == db->_record_count) {
        fprintf(stderr, "Error: Unbounded EPHEMERIS\n");
        csvdb_fatal(-1);
    }

    int count = 0;
    struct Table table;
    table.db = db;

    for (; julian < max_julian; julian++) {
        int matching = 1;
        if (have_unprocessed) {
            matching = evaluateOperatorNodeListAND(
                &table,
                ROWLIST_ROWID,
                julian,
                predicates,
                predicate_count
            );
        }
        if (matching) {
            appendRowID(getRowList(list_id), julian);
            count++;
        }
        if (count >= limit_value) {
            break;
        }
    }

    return count;
}

int ephemeris_indexSearch (
    struct DB *db,
    const char *value,
    __attribute__((unused)) int mode,
    int *output_flag
) {
    if (db->field_count == 2 && db->data != NULL) {
        char kind = ephemerisState(db)->index_kind;
        if (kind == COL_JULIAN) {
            *output_flag = RESULT_FOUND;
            return (int)atol(value);
        }
        if (kind == COL_DATE) {
            struct DateTime dt = {0};
            if (parseDate(value, &dt)) {
                *output_flag = RESULT_FOUND;
                return datetimeGetJulian(&dt);
            }
            return RESULT_NO_ROWS;
        }
        return RESULT_NO_ROWS;
    }

    *output_flag = RESULT_FOUND;
    return (int)atol(value);
}

static int getJulianRange (
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

        getSingleJulianRange(predicate, &j_start, &j_end);

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
        else if (op == OPERATOR_OR) {
            // IN (...) becomes OR of EQ arms. Bound the scan to [min, max)
            // of those arms; still evaluate the OR (have_unprocessed).
            int or_start = -1;
            int or_end = -1;
            int bounded = 1;

            for (int c = 0; c < predicate->child_count; c++) {
                struct Node *arm = &predicate->children[c];
                int arm_start = -1;
                int arm_end = -1;

                if (arm->function != OPERATOR_EQ) {
                    bounded = 0;
                    break;
                }

                getSingleJulianRange(arm, &arm_start, &arm_end);
                if (arm_start < 0 || arm_end < 0) {
                    bounded = 0;
                    break;
                }

                if (or_start < 0 || arm_start < or_start) {
                    or_start = arm_start;
                }
                if (or_end < 0 || arm_end > or_end) {
                    or_end = arm_end;
                }
            }

            if (bounded && or_start >= 0 && or_end >= 0) {
                if (*julian_start == -1 || or_start > *julian_start) {
                    *julian_start = or_start;
                }
                if (*julian_end == -1 || or_end < *julian_end) {
                    *julian_end = or_end;
                }
            }

            have_unprocessed = 1;
        }
        else {
            have_unprocessed = 1;
        }
    }

    return have_unprocessed;
}

static void getSingleJulianRange (
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

    if (strcmp(field_left->text, "julian") == 0) {
        field_left->index = FIELD_ROW_INDEX;
    }

    if (field_left->index == FIELD_ROW_INDEX) {
        *julian_start = atoi(field_right->text);
        *julian_end = *julian_start + 1;
        return;
    }

    if (field_left->index == COL_DATE) {
        struct DateTime dt = {0};
        parseDate(field_right->text, &dt);
        *julian_start = datetimeGetJulian(&dt);
        *julian_end = *julian_start + 1;
        return;
    }
}

static int printDate (char *value, int max_length, struct DateTime date) {
    if (date.year >= 0 && date.year < 10000) {
        return snprintf(
            value,
            max_length,
            "%04d-%02d-%02d",
            date.year,
            date.month,
            date.day
        );
    }
    return snprintf(
        value,
        max_length,
        "%+06d-%02d-%02d",
        date.year,
        date.month,
        date.day
    );
}
