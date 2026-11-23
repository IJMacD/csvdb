#ifndef CSVDB_CONFIG_H
#define CSVDB_CONFIG_H

#include "structs.h"

#define CSVDB_MAX_INDEX_DIRS 16
#define CSVDB_MAX_PATH_LEN 512

struct CsvdbConfig {
    int loaded;

    /** Set when [output] format is present. */
    int has_format;
    /** Format bits only (OUTPUT_MASK_FORMAT). */
    enum OutputOption format;

    /** Set when [output] headers is present. */
    int has_headers;
    /** Non-zero to enable headers. */
    int headers;

    int index_dir_count;
    char index_dirs[CSVDB_MAX_INDEX_DIRS][CSVDB_MAX_PATH_LEN];
};

/**
 * Load user config once (idempotent).
 *
 * Path resolution:
 *   1. CSVDB_CONFIG — if set to a non-empty path, use that file;
 *      if set to empty, skip loading
 *   2. $XDG_CONFIG_HOME/csvdb/config
 *   3. ~/.config/csvdb/config
 *
 * Missing file is not an error.
 */
void csvdb_config_load(void);

/** Pointer to the loaded config (always non-NULL; may be empty). */
const struct CsvdbConfig *csvdb_config(void);

/**
 * Parse a format name (same spellings as -F / --format).
 * Returns format flag bits, or -1 if unrecognised.
 */
int csvdb_parse_format(const char *format_val);

#endif
