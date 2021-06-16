#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "db.h"

int countFields (FILE *f);

int countLines (FILE *f);

/**
 * Indices must point to enough memory to contain all the indices
 */
int indexLines (FILE *f, long *indices);

void printLine (FILE *f, long position);

void makeDB (struct DB *db, FILE *f) {
    db->file = f;

    db->field_count = countFields(f);

    int line_count = countLines(f);

    db->line_indices = malloc((sizeof db->line_indices[0]) * line_count);

    indexLines(f, db->line_indices);

    db->record_count = line_count - 1;
}

int openDB (struct DB *db, const char *filename) {
    FILE *f = fopen(filename, "rw");

    if (!f) {
        char buffer[255];
        sprintf(buffer, "%s.csv", filename);
        f = fopen(buffer, "rw");

        if (!f) {
            return -1;
        }
    }

    makeDB(db, f);

    return 0;
}

int countLines (FILE *f) {
    size_t buffer_size = 1024;
    int count = 0; 
    char buffer[buffer_size];
    size_t read_size;

    fseek(f, 0, SEEK_SET);

    do {
        read_size = fread(buffer, 1, buffer_size, f);

        for (int i = 0; i < read_size; i++) {
            if (buffer[i] == '\n'){
                count++;
            }
        }
    } while (read_size > 0);

    // Check the last byte. 
    // If the file ends in a new line, then fine
    // If it soesn't then we have one more line to count
    fseek(f, -1, SEEK_END);
    fread(buffer, 1, 1, f);
    if (buffer[0] != '\n') count++;

    return count;
}

int countFields (FILE *f) {
    size_t buffer_size = 1024;
    int count = 1; 
    char buffer[buffer_size];
    size_t read_size;

    fseek(f, 0, SEEK_SET);

    do {
        read_size = fread(buffer, 1, buffer_size, f);

        for (int i = 0; i < read_size; i++) {
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
 * Indices must point to enough memory to contain all the indices
 */
int indexLines (FILE *f, long *indices) {
    size_t buffer_size = 1024;
    int count = 0; 
    char buffer[buffer_size];
    size_t read_size;
    long pos = 0;

    fseek(f, 0, SEEK_SET);

    indices[count] = pos;

    do {
        read_size = fread(buffer, 1, buffer_size, f);

        for (int i = 0; i < read_size; i++) {
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
    fread(buffer, 1, 1, f);
    if (buffer[0] != '\n') count++;

    return count;
}

void printLine (FILE *f, long position) {
    if (fseek(f, position, SEEK_SET)) {
        return;
    }
    
    size_t buffer_size = 1024;
    char buffer[buffer_size];
    size_t read_size;

    do {
        read_size = fread(buffer, 1, buffer_size - 1, f);

        for (int i = 0; i < read_size; i++) {
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

/**
 * Max header length 1024 characters
 */
int getFieldIndex (struct DB *db, const char *field) {
    fseek(db->file, 0, SEEK_SET);
    
    size_t buffer_size = 1024;
    char buffer[buffer_size];
    size_t read_size;
    int field_index = 0;
    int char_index = 0; 
    int char_length = strlen(field);

    read_size = fread(buffer, 1, buffer_size, db->file);

    for (int i = 0; i < read_size; i++) {
        if (buffer[i] == '\n'){
            return -1;
        }

        if (buffer[i] == ','){
            char_index = 0;
            field_index++;
        }

        if (buffer[i] == field[char_index]) {
            if (char_index == char_length - 1) {
                return field_index;
            }

            char_index++;
        }
    }

    return -1;
}

int getRecordValue (struct DB *db, int record_index, int field_index, char *value, size_t value_max_length) {
    if (record_index >= db->record_count) {
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
    int char_index = 0;

    read_size = fread(buffer, 1, buffer_size, db->file);

    for (int i = 0; i < read_size; i++) {
        if (current_field_index == field_index) {
            if (buffer[i] == ',' || buffer[i] == '\n') {
                value[char_index] = '\0';
                return 0;
            }

            value[char_index++] = buffer[i];

            if (char_index > value_max_length) {
                return -1;
            }
        } else {
            if (buffer[i] == ',') {
                current_field_index++;
            }

            if (buffer[i] == '\n') {
                return -1;
            }
        }
    }

    // Getting the very last record from the file
    if (current_field_index == field_index) {
        value[char_index] = '\0';
        return 0;
    }

    return -1;
}