#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../structs.h"
#include "db.h"
#include "csv-mem.h"
#include "../query/query.h"
#include "../query/node.h"

static int makeDB (struct DB *db, FILE *f);

static int countLines (FILE *f);

static int measureLine (FILE *f, size_t byte_offset);

static void prepareHeaders (struct DB *db);

static int indexLines (struct DB *db);

static int makeDB (struct DB *db, FILE *f) {
    db->vfs = VFS_CSV;
    db->file = f;

    prepareHeaders(db);

    db->_record_count = -1;

    return 0;
}

/**
 * Returns 0 on success; -1 on failure
 */
int csv_openDB (struct DB *db, const char *filename, char **resolved) {
    FILE *f = fopen(filename, "r+");

    if (f && resolved != NULL) {
        *resolved = realpath(filename, *resolved);
    }

    if (!f) {
        char buffer[FILENAME_MAX];
        sprintf(buffer, "%s.csv", filename);
        f = fopen(buffer, "r+");

        if (!f) {
            return -1;
        }

        if (resolved != NULL) {
            *resolved = realpath(buffer, *resolved);
        }
    }

    return makeDB(db, f);
}

void csv_closeDB (struct DB *db) {
    if (db->line_indices != NULL) {
        free(db->line_indices);
        db->line_indices = NULL;
    }

    if (db->fields != NULL) {
        free(db->fields);
        db->fields = NULL;
    }

    if (db->file != NULL) {
        fclose(db->file);
        db->file = NULL;
    }
}

int csv_getRecordCount (struct DB *db) {
    if (db->_record_count == -1) {
        indexLines(db);
    }

    return db->_record_count;
}

/**
 * TODO: Very bad! Presumes no embedded '\n'
 */
static int countLines (FILE *f) {
    size_t buffer_size = 1024;
    int count = 0;
    char buffer[buffer_size];
    size_t read_size;

    fseek(f, 0, SEEK_SET);

    do {
        read_size = fread(buffer, 1, buffer_size, f);

        for (size_t i = 0; i < read_size; i++) {
            if (buffer[i] == '\n'){
                count++;
            }
        }
    } while (read_size > 0);

    // Check the last byte.
    // If the file ends in a new line, then fine
    // If it doesn't then we have one more line to count
    fseek(f, -1, SEEK_END);

    size_t result = fread(buffer, 1, 1, f);
    if (result == 0)
        return -1;

    if (buffer[0] != '\n') count++;

    return count;
}

/**
 * Including \n
 * TODO: Very bad! Presumes no embedded '\n'
 */
static int measureLine (FILE *f, size_t byte_offset) {
    size_t buffer_size = 1024;
    int count = 0;
    char buffer[buffer_size];
    size_t read_size;

    fseek(f, byte_offset, SEEK_SET);

    do {
        read_size = fread(buffer, 1, buffer_size, f);

        for (size_t i = 0; i < read_size; i++) {
            if (buffer[i] == '\n'){
                return count + i + 1;
            }
        }

        count += read_size;
    } while (read_size > 0);

    return count + read_size;
}

static int indexLines (struct DB *db) {
    int line_count = countLines(db->file);

    db->_record_count = line_count - 1;

    // Might be re-indexing due to insert
    if (db->line_indices != NULL) {
        free(db->line_indices);
    }

    db->line_indices = malloc((sizeof db->line_indices[0]) * (line_count + 1));

    size_t buffer_size = 1024;
    int count = 0;
    char buffer[buffer_size];
    size_t read_size;
    long pos = 0;

    fseek(db->file, 0, SEEK_SET);

    db->line_indices[count] = pos;

    int quoted = 0;

    do {
        read_size = fread(buffer, 1, buffer_size, db->file);

        for (size_t i = 0; i < read_size; i++) {
            if (buffer[i] == '\n' && !quoted){
                db->line_indices[++count] = pos + i + 1;
            }
            else if (buffer[i] == '"') {
                quoted = ~quoted;
            }
        }

        pos += read_size;
    } while (read_size > 0);

    // Check the last byte.
    // If the file ends in a new line, then fine
    // If it doesn't then we have one more line to count
    fseek(db->file, -1, SEEK_END);

    size_t result = fread(buffer, 1, 1, db->file);
    if (result == 0)
        return -1;

    if (buffer[0] != '\n') {
        db->line_indices[++count] = pos;
    }

    return count;
}

void csv_printLine (FILE *f, long position) {
    if (fseek(f, position, SEEK_SET)) {
        return;
    }

    size_t buffer_size = 1024;
    char buffer[buffer_size];
    size_t read_size;

    do {
        read_size = fread(buffer, 1, buffer_size - 1, f);

        for (size_t i = 0; i < read_size; i++) {
            if (buffer[i] == '\n'){
                buffer[i] = '\0';
                printf("%s\n", buffer);
                return;
            }
        }

        buffer[read_size] = '\0';
        printf("%s", buffer);

        if (read_size < buffer_size - 1) {
            printf("\n");
            return;
        }
    } while (read_size > 0);

    printf("Error\n");
}

int csv_getFieldIndex (struct DB *db, const char *field) {
    char *curr_field = db->fields;

    for (int i = 0; i < db->field_count; i++) {
        if (strcmp(field, curr_field) == 0) {
            return i;
        }

        curr_field += strlen(curr_field) + 1;
    }

    return -1;
}

char *csv_getFieldName (struct DB *db, int field_index) {
    char *curr_field = db->fields;

    for (int i = 0; i < db->field_count; i++) {
        if (i == field_index) {
            return curr_field;
        }

        curr_field += strlen(curr_field) + 1;
    }

    return "\0";
}

/**
 * Returns the number of bytes read, or -1 on error
 */
int csv_getRecordValue (
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length
) {
    if (db->_record_count == -1) {
        indexLines(db);
    }

    if (record_index < 0 || record_index >= db->_record_count) {
        return -1;
    }

    if (field_index < 0 || field_index >= db->field_count) {
        return -1;
    }

    long file_offset = db->line_indices[record_index + 1];

    if (fseek(db->file, file_offset, SEEK_SET)) {
        return -1;
    }

    size_t buffer_size = 1024;
    char buffer[buffer_size];
    size_t read_size;
    int current_field_index = 0;
    size_t char_index = 0;
    int quoted_flag = 0;

    do {
        read_size = fread(buffer, 1, buffer_size, db->file);

        for (size_t i = 0; i < read_size; i++) {
            if (char_index == 0 && buffer[i] == '"') {
                quoted_flag = !quoted_flag;
                continue;
            }

            // Are we currently in the correct field?
            if (current_field_index == field_index) {

                if (db->data[i] == '"') {
                    // We found a quote. It could be at the end of the field or
                    // could be escaping another quote

                    // Move on and see what we get
                    i++;

                    if (quoted_flag) {
                        if (db->data[i] == '"') {
                            // found two double quotes, copy one to output
                            value[char_index++] = '"';
                        }
                        // If we find comma or newline immediately after one double
                        // quote then it must be the end of the field.
                        else if (
                            db->data[i] == ',' ||
                            db->data[i] == '\n' ||
                            db->data[i] == '\r' ||
                            db->data[i] == '\0'
                        ) {
                            // finish off the string and return the length
                            value[char_index] = '\0';
                            return char_index;
                        }
                        else {
                            // illegal quote
                        }
                    }
                    else {
                        // illegal quote
                    }
                }
                // If we're not quoted, then comma or newline must be the end of the
                // field.
                else if (
                    !quoted_flag && (
                        db->data[i] == ',' ||
                        db->data[i] == '\n' ||
                        db->data[i] == '\r' ||
                        db->data[i] == '\0'
                    )
                ) {
                    // finish off the string and return the length
                    value[char_index] = '\0';
                    return char_index;
                }
                // Otherwise jsut a normal byte in the string
                else {
                    // Copy the current byte
                    value[char_index++] = db->data[i];
                }


                // If we've run out of storage space
                if (char_index > value_max_length) {
                    value[char_index - 1] = '\0';
                    return -1;
                }
            } else {
                // If we've found a comma we're moving on to the next field
                if (!quoted_flag && buffer[i] == ',') {
                    current_field_index++;
                }

                // If we got to a newline and we're not in the correct field
                // then the field was not found
                if (buffer[i] == '\n') {
                    value[0] = '\0';
                    return -1;
                }
            }
        }
    } while (read_size > 0);

    // Getting the very last record from the file
    if (current_field_index == field_index) {
        value[char_index] = '\0';
        return char_index;
    }

    // Ran out of file
    return -1;
}

static void prepareHeaders (struct DB *db) {
    db->field_count = 1;

    int header_length = measureLine(db->file, 0);

    if (header_length < 0) {
        fprintf(stderr, "Something went wrong measuring header\n");
        exit(-1);
    }

    db->fields = malloc(header_length);

    if (fseek(db->file, 0, SEEK_SET)) {
        fprintf(stderr, "File is not seekable\n");
        exit(-1);
    }

    int count = fread(db->fields, 1, header_length, db->file);
    if (count < header_length) {
        fprintf(stderr, "Something went wrong reading header\n");
        exit(-1);
    }

    db->fields[header_length - 1] = '\0';

    // Suppoprt Excel CSV UTF-8.
    // Check for BOM
    if (
        db->fields[0] == '\xef' &&
        db->fields[1] == '\xbb' &&
        db->fields[2] == '\xbf'
    ) {
        memmove(db->fields, db->fields + 3, header_length - 3);
    }

    char *read_ptr, *write_ptr;

    read_ptr = db->fields;
    write_ptr = db->fields;

    // Overwrite fields buffer with itself skipping quotes and inserting nulls
    while (*read_ptr && *read_ptr != '\n')
    {
        if(*read_ptr == ',') {
            *(write_ptr++) = '\0';
            db->field_count++;
        }
        else if (*read_ptr != '"' && *read_ptr != '\r') {
            *(write_ptr++) = *read_ptr;
        }

        read_ptr++;
    }

    *(write_ptr++) = '\0';
}

/**
 * @brief Searches for an index file with an explict name `x` i.e. UNIQUE(x) or
 * INDEX(x) or the autogenerated name `table__field`.
 *
 * @param db struct DB * OUT - Database to populate with index, can be NULL
 *
 * @returns INDEX_NONE on failure; INDEX_REGUALR for a regular index, or
 * INDEX_UNIQUE for unique index
 */
enum IndexSearchType csv_findIndex(
    struct DB *db,
    const char *table_name,
    struct Node *node,
    int index_type_flags,
    char **resolved
) {
    int found_unique = 0;
    const char *field_name = nodeGetFieldName(node);

    // There as no field, then we don't have an index
    if (field_name == NULL) {
        return INDEX_NONE;
    }

    size_t field_len = strlen(field_name);
    int fn = node->function;

    if (fn != FUNC_UNITY && fn != FUNC_UNIQUE && fn != FUNC_INDEX) {
        // indexes on functions are not supported yet
        return INDEX_NONE;
    }

    // Allocate working buffer
    char index_filename[MAX_TABLE_LENGTH + MAX_FIELD_LENGTH + 12];

    FILE *f = NULL;

    if (fn == FUNC_UNIQUE || fn == FUNC_INDEX) {
        // Explicit index name, with or without '.csv'

        strcpy(index_filename, field_name);
        f = fopen(index_filename, "r");

        if (!f) {
            // Add '.csv' and try again
            strcpy(index_filename + field_len, ".csv");
            f = fopen(index_filename, "r");
        }

        if (f && fn == FUNC_UNIQUE) {
            found_unique = 1;
        }
    }
    else {
        // Start trying auto names

        char table_filename[MAX_TABLE_LENGTH + MAX_FIELD_LENGTH + 12];

        // If the table name ends in '.csv' remove that before searching for a
        // matching index
        size_t t_len = strlen(table_name);
        if (strcmp(table_name + t_len - 4, ".csv") == 0) {
            t_len -= 4;
        }
        strncpy(table_filename, table_name, t_len);
        table_filename[t_len] = '\0';

        sprintf(
            index_filename,
            "%s__%s.unique.csv",
            table_filename,
            field_name
        );
        f = fopen(index_filename, "r");

        if (f) {
            found_unique = 1;
        }

        if (!f && index_type_flags == INDEX_ANY) {
            sprintf(
                index_filename,
                "%s__%s.index.csv",
                table_filename,
                field_name
            );
            f = fopen(index_filename, "r");
        }
    }

    if (!f) {
        return INDEX_NONE;
    }

    // So we do have an index

    // Write the resolved name if caller wants it
    if (resolved != NULL) {
        *resolved = malloc(strlen(index_filename) + 1);
        strcpy(*resolved, index_filename);
    }

    // If db is NULL our caller doesn't care about using the file, they just
    // want to know if the index exists.
    if (db == NULL) {
        fclose(f);

        return found_unique ? INDEX_UNIQUE : INDEX_REGULAR;
    }

    // We will open the DB for the caller
    if (openDB(db, index_filename, NULL) == 0) {
        return found_unique ? INDEX_UNIQUE : INDEX_REGULAR;
    }

    return INDEX_NONE;
}

/**
 * @brief Creates an empty table from a string of headers
 *
 * @param db
 * @param name Name of the table. Will have '.csv' appended if it doesn't have
 * already.
 * @param headers "col1,col2" etc.
 * @returns int 0 on success; -1 on failure
 */
int csv_fromHeaders (
    struct DB *db,
    const char *name,
    const char *headers
) {
    char filename[MAX_TABLE_LENGTH];

    int len = strlen(name);
    if (strcmp(name + len - 4, ".csv") == 0) {
        strcpy(filename, name);
    }
    else {
        sprintf(filename, "%s.csv", name);
    }

    FILE *f = fopen(filename, "w");

    if (f == NULL) {
        return -1;
    }

    fputs(headers, f);
    fclose(f);

    return csv_openDB(db, filename, NULL);
}

/**
 * @brief Creates a table from the results of a query
 *
 * @param db
 * @param name Name of the table. Will have '.csv' appended if it doesn't have
 * already.
 * @param query
 * @param end_ptr
 * @returns int 0 on success; -1 on failure
 */
int csv_fromQuery (
    struct DB *db,
    const char *name,
    const char *query,
    const char **end_ptr,
    const char *headers
) {
    char filename[MAX_TABLE_LENGTH];

    int len = strlen(name);
    if (strcmp(name + len - 4, ".csv") == 0) {
        strcpy(filename, name);
    }
    else {
        sprintf(filename, "%s.csv", name);
    }

    FILE *f = fopen(filename, "w");

    if (!f) {
        fprintf(stderr, "Unable to create file for table: '%s'\n", filename);
        return -1;
    }

    int flags = OUTPUT_FORMAT_COMMA;

    if (headers) {
        fputs(headers, f);
        fputc('\n', f);
    }
    else {
        flags |= OUTPUT_OPTION_HEADERS;
    }

    int result = 0;

    if (query != NULL) {
        result = select_query(query, flags, f, end_ptr);
    }

    fclose(f);

    if (result < 0) {
        return -1;
    }

    return csv_openDB(db, filename, NULL);
}

int csv_insertRow (struct DB *db, const char *row) {
    fseek(db->file, -1, SEEK_END);

    // Make sure previous record ended with \n
    if (fgetc(db->file) != '\n') {
        fseek(db->file, 0, SEEK_END);
        fputc('\n', db->file);
    }

    fseek(db->file, 0, SEEK_END);

    if (fputs(row, db->file) < 0) {
        return -1;
    }

    fputc('\n', db->file);

    indexLines(db);

    return 0;
}

int csv_insertFromQuery (
    struct DB *db,
    const char *query,
    const char **end_ptr
) {
    int flags = OUTPUT_FORMAT_COMMA;

    fseek(db->file, 0, SEEK_END);

    int result = select_query(query, flags, db->file, end_ptr);

    if (result < 0) {
        return -1;
    }

    indexLines(db);

    return 0;
}
