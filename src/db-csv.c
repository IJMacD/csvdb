#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "db.h"
#include "indices.h"
#include "limits.h"
#include "db-csv-mem.h"

static int makeDB (struct DB *db, FILE *f);

static int countFields (FILE *f);

static int countLines (FILE *f);

static int measureLine (FILE *f, size_t byte_offset);

static void prepareHeaders (struct DB *db);

extern char **global_argv;

/**
 * Indices must point to enough memory to contain all the indices
 */
static int indexLines (FILE *f, long *indices);

static int makeDB (struct DB *db, FILE *f) {
    db->vfs = VFS_CSV;
    db->file = f;

    // Try to seek to see if we have a stream
    if (fseek(db->file, 0, SEEK_SET)) {
        // File is not seekable
        // Fallback to VFS_CSV_MEM
        return csvMem_makeDB(db, f);
    }

    // OK so we don't have a stream but memory access could still be 20x faster
    // Seek to end get file size; if it's below limit then use faster memory
    // implementation.
    fseek(db->file, 0, SEEK_END);
    size_t size = ftell(db->file);
    rewind(db->file);
    if (size < MEMORY_FILE_LIMIT) {
        // Use faster VFS_CSV_MEM
        return csvMem_makeDB(db, f);
    }

    prepareHeaders(db);

    int line_count = countLines(f);

    db->line_indices = malloc((sizeof db->line_indices[0]) * (line_count + 1));

    indexLines(f, db->line_indices);

    db->record_count = line_count - 1;

    return 0;
}

/**
 * Returns 0 on success; -1 on failure
 */
int csv_openDB (struct DB *db, const char *filename) {
    FILE *f;
    char buffer[FILENAME_MAX];

    if (strcmp(filename, "stdin") == 0) {
        f = stdin;
    }
    else {
        f = fopen(filename, "r");
    }

    if (f) {
        // We found a file with the explicit name but now check if it is a SQL
        // file (or otherwise we'll assume csv)
        int len = strlen(filename);
        if (strcmp(filename + len - 4, ".sql") == 0) {
            fclose(f);
            sprintf(buffer, "%s -H -F csv -f %s", global_argv[0], filename);
            f = popen(buffer, "r");
        }
    }
    else {
        sprintf(buffer, "%s.csv", filename);
        f = fopen(buffer, "r");

        if (!f) {
            // Try sql file as a "VIEW"
            sprintf(buffer, "%s.sql", filename);
            f = fopen(buffer, "r");

            if (f) {
                fclose(f);
                sprintf(buffer, "%s -H -F csv -f %s.sql", global_argv[0], filename);
                f = popen(buffer, "r");
            }
            else {
                return -1;
            }
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

static int countFields (FILE *f) {
    size_t buffer_size = 1024;
    int count = 1;
    char buffer[buffer_size];
    size_t read_size;

    fseek(f, 0, SEEK_SET);

    do {
        read_size = fread(buffer, 1, buffer_size, f);

        for (size_t i = 0; i < read_size; i++) {
            if (buffer[i] == '\n'){
                return count;
            }

            if (buffer[i] == ','){
                count++;
            }
        }
    } while (read_size > 0);

    return count;
}

/**
 * Including \n
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

/**
 * Indices must point to enough memory to contain all the indices
 */
static int indexLines (FILE *f, long *indices) {
    size_t buffer_size = 1024;
    int count = 0;
    char buffer[buffer_size];
    size_t read_size;
    long pos = 0;

    fseek(f, 0, SEEK_SET);

    indices[count] = pos;

    do {
        read_size = fread(buffer, 1, buffer_size, f);

        for (size_t i = 0; i < read_size; i++) {
            if (buffer[i] == '\n'){
                indices[++count] = pos + i + 1;
            }
        }

        pos += read_size;
    } while (read_size > 0);

    // Check the last byte.
    // If the file ends in a new line, then fine
    // If it soesn't then we have one more line to count
    fseek(f, -1, SEEK_END);

    size_t result = fread(buffer, 1, 1, f);
    if (result == 0)
        return -1;

    if (buffer[0] != '\n') count++;

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
int csv_getRecordValue (struct DB *db, int record_index, int field_index, char *value, size_t value_max_length) {
    if (record_index < 0 || record_index >= db->record_count) {
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

                // Have we found the end of a quoted value?
                // We've found the end of a record
                if (buffer[i] == '"' || (!quoted_flag && (buffer[i] == ',' || buffer[i] == '\n' || buffer[i] == '\r'))) {

                    // There might be quotes in the middle of a values, who cares?
                    // Let's just ignore that and pretend it won't happen

                    // finish off the string and return the length
                    value[char_index] = '\0';
                    return char_index;
                }

                // Copy the current byte
                value[char_index++] = buffer[i];

                // If we've run out of storage space
                if (char_index > value_max_length) {
                    return -1;
                }
            } else {
                // If we've found a comma we're moving on to the next field
                if (!quoted_flag && buffer[i] == ',') {
                    current_field_index++;
                }

                // If we got to a newline and we're not in the correct field then the field was not found
                if (buffer[i] == '\n') {
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

    db->field_count = countFields(db->file);

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

    for (int i = 0; i < header_length; i++) {
        if(db->fields[i] == ',' || db->fields[i] == '\n' || db->fields[i] == '\r') {
            db->fields[i] = '\0';
        }
    }

}

/**
 * Searches for an index file with an explict name `x` i.e. UNIQUE(x) or INDEX(x) or
 * the autogenerated name `table__field`
 * Returns 0 on failure; 1 for a regular index, 2 for unique index
 *
 * @param db struct DB * OUT - Database to populate with index
 */
int csv_findIndex(struct DB *db, const char *table_name, const char *index_name, int index_type_flags) {
    char index_filename[MAX_TABLE_LENGTH + MAX_FIELD_LENGTH + 12];
    size_t len = strlen(index_name);

    // Try UNIQUE indexes first

    if (strncmp(index_name, "UNIQUE(", 7) == 0) {
        size_t index_name_len = len - 8;
        strncpy(index_filename, index_name + 7, index_name_len);
        strcpy(index_filename + index_name_len, ".unique.csv");
    }
    else {
        sprintf(index_filename, "%s__%s.unique.csv", table_name, index_name);
    }

    // If db is NULL our caller doesn't care about using the file, they just
    // want to know if the index exists.
    if (db == NULL) {
        FILE * f = fopen(index_filename, "r");

        if (f != NULL) {
            fclose(f);

            return INDEX_UNIQUE;
        }

    }
    else if (openDB(db, index_filename) == 0) {
        return INDEX_UNIQUE;
    }

    if (index_type_flags == INDEX_UNIQUE) {
        // We have failed - we were only looking for UNIQUE indexes
        return 0;
    }

    // Now try a regular  index

    if (strncmp(index_name, "INDEX(", 6) == 0) {
        if (index_type_flags != INDEX_ANY) return -1;

        size_t index_name_len = len - 7;
        strncpy(index_filename, index_name + 6, index_name_len);
        strcpy(index_filename + index_name_len, ".index.csv");
    }
     else {
        sprintf(index_filename, "%s__%s.index.csv", table_name, index_name);
    }

    // If db is NULL our caller doesn't care about using the file, they just
    // want to know if the index exists.
    if (db == NULL) {
        FILE * f = fopen(index_filename, "r");

        if (f == NULL) {
            return 0;
        }

        fclose(f);

        return INDEX_REGULAR;
    }

    if (openDB(db, index_filename) == 0) {
        return INDEX_REGULAR;
    }

    return 0;
}
