#include <stdlib.h>

int is_end_of_line (const char c) {
    return c == '\0' || c == '\n' || c == '\r';
}

int is_end_of_field (const char c) {
    return c == ',' || is_end_of_line(c);
}

int csv_get_record_from_line (const char *in_ptr, int field_index, char *out_ptr, size_t max_length) {
    int current_field_index = 0;

    char *out_start_ptr = out_ptr;
    char *out_end_ptr = out_ptr + max_length;

    // Just in case we don't find anything
    *out_ptr = '\0';

    while (!is_end_of_line(*in_ptr)) {
        int quoted_flag = 0;

        // Start of the field
        // Check if quoted or not

        if (*in_ptr == '"') {
            quoted_flag = 1;
            in_ptr++;
        }

        // Consume whole field
        while (quoted_flag || !is_end_of_field(*in_ptr)) {
            if (*in_ptr == '"') {
                in_ptr++;

                if (is_end_of_field(*in_ptr)) {
                    break;
                }
            }

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

        if (*in_ptr == ',') {
            current_field_index++;
            in_ptr++;
        }
    }

    // Ran out of file
    return -1;
}