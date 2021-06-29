#include <ctype.h>

int is_numeric (const char *string) {
    const char *ptr = string;
    while (*ptr != '\0') {
        if (!isdigit(*ptr)) return 0;
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