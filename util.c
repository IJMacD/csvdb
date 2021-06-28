#include <ctype.h>

int is_numeric (const char *string) {
    const char *ptr = string;
    while (*ptr != '\0') {
        if (!isdigit(*ptr)) return 0;
        ptr++;
    }
    return 1;
}