#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "helper.h"
#include "../structs.h"
#include "../functions/util.h"
#include "../query/query.h"

static int prepareHeaders (struct DB *db);

static char *get_end_of_data (struct DB *db);

static int wsv_get_record_from_line (
    const char *in_ptr,
    int field_index,
    char *out_ptr,
    size_t max_length
);

static int makeDB (struct DB *db, FILE *f);

/**
 * @brief Opens, consumes, and closes file specified by filename
 * Whitespace Separated Values
 *
 * @param db
 * @param filename can also be "stdin.wsv"
 * @param resolved if not NULL, then will write resolved path to buffer pointed
 * to by this pointer. If this pointer points to NULL then a buffer will be
 * malloc'd for it.
 */
int wsvMem_openDB (struct DB *db, const char *filename, char **resolved) {
    FILE *f = NULL;

    if (strcmp(filename, "stdin.wsv") == 0) {
        f = stdin;
    }
    else if (ends_with(filename, ".wsv")) {
        f = fopen(filename, "r");

        if (resolved != NULL) {
            *resolved = realpath(filename, *resolved);
        }
    }

    if (!f) {
        return -1;
    }

    int result = makeDB(db, f);

    fclose(f);

    return result;
}

void wsvMem_closeDB (struct DB *db) {
    if (db->line_indices != NULL) {
        // max_size of allocation is stored at start of real block
        void *ptr = db->line_indices;
        free(ptr - sizeof(int));
        db->line_indices = NULL;
    }

    if (db->fields != NULL) {
        // db->data is in the same block as db->fields
        free(db->fields);
        db->fields = NULL;
    }
}

int wsvMem_getFieldIndex (struct DB *db, const char *field) {
    char *curr_field = db->fields;

    for (int i = 0; i < db->field_count; i++) {
        if (strcmp(field, curr_field) == 0) {
            return i;
        }

        curr_field += strlen(curr_field) + 1;
    }

    return -1;
}

char *wsvMem_getFieldName (struct DB *db, int field_index) {
    char *curr_field = db->fields;

    for (int i = 0; i < db->field_count; i++) {
        if (i == field_index) {
            return curr_field;
        }

        curr_field += strlen(curr_field) + 1;
    }

    return "\0";
}

int wsvMem_getRecordCount (struct DB *db) {
    if (db->_record_count < 0) {
        indexLines(db, -1, '\0');
    }

    return db->_record_count;
}

/**
 * Returns the number of bytes read, or -1 on error
 */
int wsvMem_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
) {
    if (db->_record_count < 0) {
        // Just index as many rows as we need
        if (indexLines(db, record_index + 1, '"') < 0) {
            return -1;
        }
    }
    else {
        if (record_index >= db->_record_count) {
            return -1;
        }
    }

    if (field_index < 0 || field_index >= db->field_count) {
        return -1;
    }

    long file_offset = db->line_indices[record_index];

    return wsv_get_record_from_line(db->data + file_offset, field_index, value, value_max_length);
}

static int makeDB (struct DB *db, FILE *f) {
    db->vfs = VFS_WSV_MEM;

    // It would be nice to have a streaming solution but I don't think it's
    // realistically possible. We will just read the entire stream into memory.
    consumeStream(db, f);

    int result = prepareHeaders(db);
    if (result < 0) {
        return -1;
    }

    db->_record_count = -1;

    return 0;
}

static int prepareHeaders (struct DB *db) {
    db->field_count = 1;

    db->fields = db->data;

    if (db->data[0] == '\0') {
        fprintf(stderr, "Empty file\n");
        return -1;
    }

    char *read_ptr, *write_ptr;

    read_ptr = db->fields;
    write_ptr = db->fields;

    // Overwrite fields buffer with itself skipping quotes and inserting nulls
    while (*read_ptr && *read_ptr != '\n')
    {
        if(isspace(*read_ptr)) {
            *(write_ptr++) = '\0';
            read_ptr++;

            db->field_count++;

            // Found a space, consume all following whitespace except newline
            while (*read_ptr != '\0' && *read_ptr != '\n' && isspace(*read_ptr)) {
                read_ptr++;
            }
        }
        else {
            *(write_ptr++) = *(read_ptr++);
        }

    }

    *(write_ptr++) = '\0';

    db->data = read_ptr + 1;

    return 0;
}

// No Indexes on memory table
int wsvMem_findIndex(
    __attribute__((unused)) struct DB *db,
    __attribute__((unused)) const char *table_name,
    __attribute__((unused)) struct Node *node,
    __attribute__((unused)) int index_type_flags,
    __attribute__((unused)) char **resolved
) {
    return 0;
}

/**
 * @brief Convert from VALUES layout e.g. ('a',1),('b',2) to wsv in memory
 *
 * @param db
 * @param input
 * @param length Maximum length to read from input. -1 for no limit
 * @returns char * pointer to end of query (i.e. if it came accross a '\0' or
 * ';')
 */
const char *wsvMem_fromValues(struct DB *db, const char *input, int length) {
    const char *in_ptr = input;
    const char *end_ptr = input + length - 1;
    char *out_ptr;

    db->vfs = VFS_CSV_MEM;
    db->file = NULL;

    size_t max_data_size = MAX_VALUE_LENGTH;

    db->data = malloc(max_data_size);
    out_ptr = db->data;

    int max_line_count = 100;

    db->line_indices = malloc(max_line_count * sizeof (long));

    db->line_indices[0] = 0;

    // Headers
    // We don't know how many columns we'll need but we need to reserve space
    // for them now. Hopefully 50 columns will be enough.
    // The actual number of columns is counted later.
    const char headers[] =
        "col1\0col2\0col3\0col4\0col5\0col6\0col7\0col8\0col9\0"
        "col10\0col11\0col12\0col13\0col14\0col15\0col16\0col17\0col18\0col19\0"
        "col20\0col21\0col22\0col23\0col24\0col25\0col26\0col27\0col28\0col29\0"
        "col30\0col31\0col32\0col33\0col34\0col35\0col36\0col37\0col38\0col39\0"
        "col40\0col41\0col42\0col43\0col44\0col45\0col46\0col47\0col48\0col49\0";
    memcpy(out_ptr, headers, sizeof(headers));
    out_ptr += sizeof(headers);

    db->fields = db->data;
    db->data += sizeof(headers);

    int line_index = 0;

    while(*in_ptr != '\0' && *in_ptr != ';' && in_ptr != end_ptr) {

        if (*in_ptr != '(') {
            fprintf(
                stderr,
                "VALUES: expected row %d to start with '('. Found: %c\n",
                line_index,
                *in_ptr
            );
            exit(-1);
        }

        db->line_indices[line_index++] = out_ptr - db->data;

        // Check to see if we need to extend the line index allocation
        if (line_index == max_line_count) {
            max_line_count *= 2;
            size_t size = max_line_count * sizeof(long);
            long *ptr = realloc(db->line_indices, size);
            if (ptr == NULL) {
                fprintf(stderr, "Unable to allocate memory: %ld\n", size);
                exit(-1);
            }
            db->line_indices = ptr;
        }

        int line_length = find_matching_parenthesis(in_ptr);

        if (line_length < 0) {
            fprintf(stderr, "Can't find matching parenthesis\n");
            exit(-1);
        }

        // Check to see if we need to extend the data allocation
        // NOTE: db->fields is the start of the allocation
        size_t len = out_ptr - db->fields;
        while (len + line_length >= max_data_size) {
            max_data_size *= 2;
            // NOTE: db->fields is the start of the allocation
            void *ptr = realloc(db->fields, max_data_size);
            if (ptr == NULL) {
                fprintf(
                    stderr,
                    "Unable to allocate memory: %ld\n",
                    max_data_size
                );
                exit(-1);
            }
            db->fields = ptr;
            db->data = ptr + sizeof(headers);
            out_ptr = db->data + len;
        }

        strncpy(out_ptr, in_ptr + 1, line_length - 2);

        /**
         * Swap commas to tabs
         * will break quotes
         * TODO: Fix problems below
         * Warning: field will spaces at beginning and end!
         */
        int in_field = 0;
        for (int i = 0; i < line_length - 2; i++) {
            if (*out_ptr == '\'')  {
                *out_ptr = ' ';
                in_field = ~in_field;
            }
            else if (*out_ptr == ',' || !in_field) *out_ptr = '\t';
            out_ptr++;
        }

        in_ptr += line_length;

        // End of file
        if (*in_ptr == '\0') {
            break;
        }
        // End of line
        else if (*in_ptr == ',') {
            in_ptr++;
            *(out_ptr++) = '\n';
        }

        // Skip whitespace
        while (isspace(*in_ptr)) {
            in_ptr++;
        }
    }

    *out_ptr = '\0';

    if (*in_ptr == ';') {
        in_ptr++;
    }

    prepareHeaders(db);

    db->_record_count = line_index;

    return in_ptr;
}

/**
 * @brief Creates an empty table from a string of headers
 *
 * @param db
 * @param headers "col1,col2" etc.
 */
void wsvMem_fromHeaders (struct DB *db, const char *headers) {
    db->vfs = VFS_CSV_MEM;

    db->data = malloc(strlen(headers) + 1);

    strcpy(db->data, headers);

    // prepareHeaders() copies pointer to db->fields and adjusts db->data to be
    // after the headers.
    prepareHeaders(db);

    db->line_indices = malloc(sizeof(*db->line_indices) * 32);

    db->line_indices[0] = 0;

    db->_record_count = 0;
}

int wsvMem_fromQuery (struct DB *db, struct Query *query) {
    db->vfs = VFS_TSV_MEM;

    size_t size;

    FILE *f = open_memstream(&db->data, &size);

    int result = process_query(
        query,
        OUTPUT_OPTION_HEADERS | OUTPUT_FORMAT_TAB,
        f
    );

    fclose(f);

    if (result < 0) {
        return -1;
    }

    prepareHeaders(db);

    db->_record_count = -1;

    return 0;
}

int wsvMem_insertRow (struct DB *db, const char *row) {
    int len = strlen(row);
    char *data_end = get_end_of_data(db);
    int data_len = data_end - db->data;
    int header_len = db->data - db->fields;

    // Extra bytes for '\n' and '\0'
    int new_size = header_len + data_len + len + 2;

    db->fields = realloc(db->fields, new_size);

    if (db->fields == NULL) {
        fprintf(stderr, "Unable to allocate %d for wsv_insertRow\n", new_size);
        exit(-1);
    }

    db->data = db->fields + header_len;

    // After realloc data_end might have changed
    data_end = get_end_of_data(db);

    strcpy(data_end, row);
    if (row[len - 1] != '\n') {
        data_end[len] = '\n';
        data_end[len + 1] = '\0';
        len++;
    }

    db->_record_count++;

    // IF THERE'S A BUG CHECK HERE!
    // We're not checking if db->line_inidices is big enough
    db->line_indices[db->_record_count] = data_len + len;

    return 0;
}

static char *get_end_of_data (struct DB *db) {
    if (db->_record_count < 0) {
        indexLines(db, -1, '\0');
    }

    return db->data + db->line_indices[db->_record_count];
}

static int wsv_is_end_of_line (const char c) {
    return c == '\0' || c == '\n' || c == '\r';
}

static int wsv_is_end_of_field (const char c) {
    return isspace(c);
}

static int wsv_get_record_from_line (
    const char *in_ptr,
    int field_index,
    char *out_ptr,
    size_t max_length
) {
    int current_field_index = 0;

    char *out_start_ptr = out_ptr;
    char *out_end_ptr = out_ptr + max_length - 1;

    // Just in case we don't find anything
    *out_ptr = '\0';

    while (!wsv_is_end_of_line(*in_ptr)) {
        // Start of the field

        // Consume whole field
        while (!wsv_is_end_of_field(*in_ptr)) {
            // If we're in the correct field, copy to output
            if (current_field_index == field_index) {
                *(out_ptr++) = *in_ptr;

                // Have we run out of space?
                if (out_ptr == out_end_ptr) {
                    out_ptr--;
                    break;
                }
            }

            in_ptr++;
        }

        if (current_field_index == field_index) {
            *(out_ptr++) = '\0';

            return out_ptr - out_start_ptr;
        }

        if (isspace(*in_ptr)) {
            current_field_index++;

            // Found a space, consume all whitespace except newline
            while (*in_ptr != '\0' && *in_ptr != '\n' && isspace(*in_ptr)) {
                in_ptr++;
            }
        }
    }

    // Ran out of file
    return -1;
}