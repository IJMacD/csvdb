#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "../structs.h"

void consumeStream (struct DB *db, FILE *stream) {
    // 4 KB blocks
    int block_size = 4 * 1024;

    db->data = NULL;

    int read_size = -1;
    int block_count = 0;

    int offset = 0;

    do {
        block_count++;

        if (db->data == NULL) {
            db->data = malloc(block_size);
        } else {
            void * ptr = realloc(db->data, block_size * block_count);

            if (ptr == NULL) {
                fprintf(stderr, "Unable to assign memory");
                exit(-1);
            }

            db->data = ptr;
        }

        read_size = fread(db->data + offset, 1, block_size, stream);

        offset += read_size;
    } while (read_size > 0);

    // Add null terminator to end of stream.
    // Necessary when more than one query is executed in same process.
    db->data[offset] = '\0';
}

int indexLines (struct DB *db, int max_lines, char quote_char) {
    int count = 0;
    size_t i = 0;

    // max_size is current size of allocation
    // max_size is stored at start of allocation
    int *max_size = NULL;

    if (db->line_indices == NULL) {
        max_size = malloc(sizeof(*db->line_indices) * (32 + 1));
        // Save current size at start of allocation
        *max_size = 32;
        // False start
        db->line_indices = (void *)max_size + sizeof(*max_size);
    }
    else {
        void *ptr = db->line_indices;
        max_size = ptr - sizeof(*max_size);
    }

    // Check if we've already done a partial index
    if (db->_record_count < -1) {
        count = -db->_record_count - 1;

        if (max_lines <= count) {
            // We've already indexed as much as we need to
            return count;
        }

        // Jump ahead in file to avoid unnecessary repetition
        i = db->line_indices[count];
    }

    db->line_indices[count++] = i;

    int quoted = 0;

    while (db->data[i] != '\0') {
        if (db->data[i] == '\n' && !quoted){
            if (count == *max_size) {
                *max_size *= 2;
                // max_size is the real location of the allocation

                void *ptr = realloc(
                    max_size,
                    sizeof(*db->line_indices) * *max_size + sizeof(*max_size)
                );

                if (ptr == NULL) {
                    fprintf(
                        stderr,
                        "Unable to allocate memory for %d line_indices\n",
                        *max_size
                    );
                    exit(-1);
                }
                // save new memory addresses
                max_size = ptr;
                db->line_indices = ptr + sizeof(*max_size);
            }

            db->line_indices[count++] = i + 1;

            if (max_lines > -1 && count >= max_lines) {
                count--;

                // Save count as negative number to indicate full scan has not
                // yet taken place.
                db->_record_count = -count - 1;

                return count;
            }
        }
        else if (db->data[i] == quote_char) {
            quoted = ~quoted;
        }

        i++;
    }

    if (db->data[i-1] == '\n') {
        count--;
    }

    // Add final count to track file size
    db->line_indices[count] = i;

    db->_record_count = count;

    return count;
}

int ends_with (const char *string, const char *search) {
    int string_len = strlen(string);
    int search_len = strlen(search);

    return strcmp(string + string_len - search_len, search) == 0;
}