#include <ctype.h>

int is_numeric (const char *string) {
    const char *ptr = string;
    int decimal = 0;
    while (*ptr != '\0') {
        if (*ptr == '.') {
            if (decimal) return 0;
            decimal = 1;
        }
        else if (!isdigit(*ptr)) return 0;
        ptr++;
    }
    return 1;
}

void reverse_array (int * array, int size) {
    for (int i = 0; i < size / 2; i++) {
        int i1 = size - i - 1;
        int temp = array[i1];
        array[i1] = array[i];
        array[i] = temp;
    }
}

int str_find_index (const char * string, char chr) {
    int i = 0;
    while (string[i] != '\0') {
        if (string[i] == chr) return i;
        i++;
    }
    return -1;
}