#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../structs.h"
#include "db.h"
#include "helper.h"
#include "csv-mem.h"
#include "../query/select.h"

static int makeDB(struct DB *db, FILE *f);

static int countFields(FILE *f);

static int countLines(FILE *f);

static int measureLine(FILE *f, size_t byte_offset);

static void prepareHeaders(struct DB *db);

static int tsv_indexLines(struct DB *db);

static int makeDB(struct DB *db, FILE *f)
{
    db->vfs = VFS_TSV;
    db->file = f;

    prepareHeaders(db);

    db->_record_count = -1;

    return 0;
}

/**
 * Returns 0 on success; -1 on failure
 */
int tsv_openDB(struct DB *db, const char *filename, char **resolved)
{
    FILE *f = NULL;

    if (strcmp(filename, "stdin.tsv") == 0)
    {
        f = stdin;
    }
    else if (ends_with(filename, ".tsv"))
    {
        f = fopen(filename, "r");

        if (resolved != NULL)
        {
            *resolved = realpath(filename, *resolved);
        }
    }

    if (!f)
    {
        return -1;
    }

    return makeDB(db, f);
}

void tsv_closeDB(struct DB *db)
{
    if (db->line_indices != NULL)
    {
        free(db->line_indices);
        db->line_indices = NULL;
    }

    if (db->fields != NULL)
    {
        free(db->fields);
        db->fields = NULL;
    }

    if (db->file != NULL)
    {
        fclose(db->file);
        db->file = NULL;
    }
}

int tsv_getRecordCount(struct DB *db)
{
    if (db->_record_count == -1)
    {
        tsv_indexLines(db);
    }

    return db->_record_count;
}

static int countLines(FILE *f)
{
    size_t buffer_size = 1024;
    int count = 0;
    char buffer[buffer_size];
    size_t read_size;

    fseek(f, 0, SEEK_SET);

    do
    {
        read_size = fread(buffer, 1, buffer_size, f);

        for (size_t i = 0; i < read_size; i++)
        {
            if (buffer[i] == '\n')
            {
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

    if (buffer[0] != '\n')
        count++;

    return count;
}

static int countFields(FILE *f)
{
    size_t buffer_size = 1024;
    int count = 1;
    char buffer[buffer_size];
    size_t read_size;

    fseek(f, 0, SEEK_SET);

    do
    {
        read_size = fread(buffer, 1, buffer_size, f);

        for (size_t i = 0; i < read_size; i++)
        {
            if (buffer[i] == '\n')
            {
                return count;
            }

            if (buffer[i] == '\t')
            {
                count++;
            }
        }
    } while (read_size > 0);

    return count;
}

/**
 * Including \n
 */
static int measureLine(FILE *f, size_t byte_offset)
{
    size_t buffer_size = 1024;
    int count = 0;
    char buffer[buffer_size];
    size_t read_size;

    fseek(f, byte_offset, SEEK_SET);

    do
    {
        read_size = fread(buffer, 1, buffer_size, f);

        for (size_t i = 0; i < read_size; i++)
        {
            if (buffer[i] == '\n')
            {
                return count + i + 1;
            }
        }

        count += read_size;
    } while (read_size > 0);

    return count + read_size;
}

static int tsv_indexLines(struct DB *db)
{
    int line_count = countLines(db->file);

    db->_record_count = line_count - 1;

    // Might be re-indexing due to insert
    if (db->line_indices != NULL)
    {
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

    do
    {
        read_size = fread(buffer, 1, buffer_size, db->file);

        for (size_t i = 0; i < read_size; i++)
        {
            if (buffer[i] == '\n')
            {
                db->line_indices[++count] = pos + i + 1;
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

    if (buffer[0] != '\n')
    {
        db->line_indices[++count] = pos;
    }

    return count;
}

void tsv_printLine(FILE *f, long position)
{
    if (fseek(f, position, SEEK_SET))
    {
        return;
    }

    size_t buffer_size = 1024;
    char buffer[buffer_size];
    size_t read_size;

    do
    {
        read_size = fread(buffer, 1, buffer_size - 1, f);

        for (size_t i = 0; i < read_size; i++)
        {
            if (buffer[i] == '\n')
            {
                buffer[i] = '\0';
                printf("%s\n", buffer);
                return;
            }
        }

        buffer[read_size] = '\0';
        printf("%s", buffer);

        if (read_size < buffer_size - 1)
        {
            printf("\n");
            return;
        }
    } while (read_size > 0);

    printf("Error\n");
}

int tsv_getFieldIndex(struct DB *db, const char *field)
{
    char *curr_field = db->fields;

    for (int i = 0; i < db->field_count; i++)
    {
        if (strcmp(field, curr_field) == 0)
        {
            return i;
        }

        curr_field += strlen(curr_field) + 1;
    }

    return -1;
}

char *tsv_getFieldName(struct DB *db, int field_index)
{
    char *curr_field = db->fields;

    for (int i = 0; i < db->field_count; i++)
    {
        if (i == field_index)
        {
            return curr_field;
        }

        curr_field += strlen(curr_field) + 1;
    }

    return "\0";
}

/**
 * Returns the number of bytes read, or -1 on error
 */
int tsv_getRecordValue(
    struct DB *db,
    int record_index,
    int field_index,
    char *value,
    size_t value_max_length)
{
    if (db->_record_count == -1)
    {
        tsv_indexLines(db);
    }

    if (record_index < 0 || record_index >= db->_record_count)
    {
        return -1;
    }

    if (field_index < 0 || field_index >= db->field_count)
    {
        return -1;
    }

    long file_offset = db->line_indices[record_index + 1];

    if (fseek(db->file, file_offset, SEEK_SET))
    {
        return -1;
    }

    size_t buffer_size = 1024;
    char buffer[buffer_size];
    size_t read_size;
    int current_field_index = 0;
    size_t char_index = 0;
    int quoted_flag = 0;

    do
    {
        read_size = fread(buffer, 1, buffer_size, db->file);

        for (size_t i = 0; i < read_size; i++)
        {
            if (char_index == 0 && buffer[i] == '"')
            {
                quoted_flag = !quoted_flag;
                continue;
            }

            // Are we currently in the correct field?
            if (current_field_index == field_index)
            {

                // Have we found the end of a quoted value?
                // We've found the end of a record
                if (
                    buffer[i] == '"' || (!quoted_flag && (buffer[i] == '\t' || buffer[i] == '\n' || buffer[i] == '\r')))
                {

                    // There might be quotes in the middle of a values, who cares?
                    // Let's just ignore that and pretend it won't happen

                    // finish off the string and return the length
                    value[char_index] = '\0';
                    return char_index;
                }

                // Copy the current byte
                value[char_index++] = buffer[i];

                // If we've run out of storage space
                if (char_index > value_max_length)
                {
                    return -1;
                }
            }
            else
            {
                // If we've found a tab we're moving on to the next field
                if (!quoted_flag && buffer[i] == '\t')
                {
                    current_field_index++;
                }

                // If we got to a newline and we're not in the correct field
                // then the field was not found
                if (buffer[i] == '\n')
                {
                    return -1;
                }
            }
        }
    } while (read_size > 0);

    // Getting the very last record from the file
    if (current_field_index == field_index)
    {
        value[char_index] = '\0';
        return char_index;
    }

    // Ran out of file
    return -1;
}

static void prepareHeaders(struct DB *db)
{

    db->field_count = countFields(db->file);

    int header_length = measureLine(db->file, 0);

    if (header_length < 0)
    {
        fprintf(stderr, "Something went wrong measuring header\n");
        exit(-1);
    }

    db->fields = malloc(header_length);

    if (fseek(db->file, 0, SEEK_SET))
    {
        fprintf(stderr, "File is not seekable\n");
        exit(-1);
    }

    int count = fread(db->fields, 1, header_length, db->file);
    if (count < header_length)
    {
        fprintf(stderr, "Something went wrong reading header\n");
        exit(-1);
    }

    for (int i = 0; i < header_length; i++)
    {
        if (
            db->fields[i] == '\t' || db->fields[i] == '\n' || db->fields[i] == '\r')
        {
            db->fields[i] = '\0';
        }
    }
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
enum IndexSearchType tsv_findIndex(
    struct DB *db,
    const char *table_name,
    struct Node *node,
    int index_type_flags,
    char **resolved)
{
    int found_unique = 0;
    const char *field_name = field_name = node->field.text;
    size_t field_len = strlen(field_name);
    int fn = node->function;

    if (fn != FUNC_UNITY && fn != FUNC_UNIQUE && fn != FUNC_INDEX)
    {
        // indexes on functions are not supported yet
        return INDEX_NONE;
    }

    char table_filename[MAX_TABLE_LENGTH + MAX_FIELD_LENGTH + 12 - 13];

    // If the table name ends in '.tsv' remove that before searching for a
    // matching index
    size_t t_len = strlen(table_name);
    if (strcmp(table_name + t_len - 4, ".tsv") == 0)
    {
        t_len -= 4;
    }
    strncpy(table_filename, table_name, t_len);
    table_filename[t_len] = '\0';

    // Allocate working buffer
    char index_filename[MAX_TABLE_LENGTH + MAX_FIELD_LENGTH + 12];

    FILE *f = NULL;

    if (fn == FUNC_UNIQUE || fn == FUNC_INDEX)
    {
        // Explicit index name, with or without '.tsv'

        strcpy(index_filename, field_name);
        f = fopen(index_filename, "r");

        if (!f)
        {
            // Add '.tsv' and try again
            strcpy(index_filename + field_len, ".tsv");
            f = fopen(index_filename, "r");
        }

        if (f && fn == FUNC_UNIQUE)
        {
            found_unique = 1;
        }
    }
    else
    {
        // Start trying auto names

        sprintf(
            index_filename,
            "%s__%s.unique.tsv",
            table_filename,
            field_name);
        f = fopen(index_filename, "r");

        if (f)
        {
            found_unique = 1;
        }

        if (!f && index_type_flags == INDEX_ANY)
        {
            sprintf(
                index_filename,
                "%s__%s.index.tsv",
                table_filename,
                field_name);
            f = fopen(index_filename, "r");
        }
    }

    if (!f)
    {
        return INDEX_NONE;
    }

    // So we do have an index

    // Write the resolved name if caller wants it
    if (resolved != NULL)
    {
        *resolved = malloc(strlen(index_filename) + 1);
        strcpy(*resolved, index_filename);
    }

    // If db is NULL our caller doesn't care about using the file, they just
    // want to know if the index exists.
    if (db == NULL)
    {
        fclose(f);

        return found_unique ? INDEX_UNIQUE : INDEX_REGULAR;
    }

    // We will open the DB for the caller
    if (openDB(db, index_filename, NULL) == 0)
    {
        return found_unique ? INDEX_UNIQUE : INDEX_REGULAR;
    }

    return INDEX_NONE;
}

/**
 * @brief Creates an empty table from a string of headers
 *
 * @param db
 * @param name Name of the table. Will have '.tsv' appended if it doesn't have
 * already.
 * @param headers "col1,col2" etc.
 * @returns int 0 on success; -1 on failure
 */
int tsv_fromHeaders(
    struct DB *db,
    const char *name,
    const char *headers)
{
    char filename[MAX_TABLE_LENGTH];

    int len = strlen(name);
    if (strcmp(name + len - 4, ".tsv") == 0)
    {
        strcpy(filename, name);
    }
    else
    {
        sprintf(filename, "%s.tsv", name);
    }

    FILE *f = fopen(filename, "w");

    if (f == NULL)
    {
        return -1;
    }

    fputs(headers, f);
    fclose(f);

    return tsv_openDB(db, filename, NULL);
}

/**
 * @brief Creates a table from the results of a query
 *
 * @param db
 * @param name Name of the table. Will have '.tsv' appended if it doesn't have
 * already.
 * @param query
 * @param end_ptr
 * @returns int 0 on success; -1 on failure
 */
int tsv_fromQuery(
    struct DB *db,
    const char *name,
    const char *query,
    const char **end_ptr)
{
    char filename[MAX_TABLE_LENGTH];

    // Force ".tsv" file extension
    int len = strlen(name);
    if (strcmp(name + len - 4, ".tsv") == 0)
    {
        strcpy(filename, name);
    }
    else
    {
        sprintf(filename, "%s.tsv", name);
    }

    FILE *f = fopen(filename, "w");

    if (!f)
    {
        fprintf(stderr, "Unable to create file for table: '%s'\n", filename);
        return -1;
    }

    int flags = OUTPUT_FORMAT_TAB | OUTPUT_OPTION_HEADERS;

    int result = select_query(query, flags, f, end_ptr);

    fclose(f);

    if (result < 0)
    {
        return -1;
    }

    return tsv_openDB(db, filename, NULL);
}

int tsv_insertRow(struct DB *db, const char *row)
{
    fseek(db->file, -1, SEEK_END);

    // Make sure previous record ended with \n
    if (fgetc(db->file) != '\n')
    {
        fseek(db->file, 0, SEEK_END);
        fputc('\n', db->file);
    }

    fseek(db->file, 0, SEEK_END);

    if (fputs(row, db->file) < 0)
    {
        return -1;
    }

    fputc('\n', db->file);

    tsv_indexLines(db);

    return 0;
}

int tsv_insertFromQuery(
    struct DB *db,
    const char *query,
    const char **end_ptr)
{
    int flags = OUTPUT_FORMAT_TAB;

    fseek(db->file, 0, SEEK_END);

    int result = select_query(query, flags, db->file, end_ptr);

    if (result < 0)
    {
        return -1;
    }

    tsv_indexLines(db);

    return 0;
}
