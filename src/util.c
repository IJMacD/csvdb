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

int writeUTF8(char * output, int codepoint) {
    if (codepoint < 0x80) {
      output[0] = (codepoint >>0  & 0x7F) | 0x00;
      output[1] = '\0';
      return 1;
    }

    if (codepoint < 0x0800) {
      output[0] = (codepoint >>6  & 0x1F) | 0xC0;
      output[1] = (codepoint >>0  & 0x3F) | 0x80;
      output[2] = '\0';

      return 2;
    }

    if (codepoint < 0x010000) {
      output[0] = (codepoint >>12 & 0x0F) | 0xE0;
      output[1] = (codepoint >>6  & 0x3F) | 0x80;
      output[2] = (codepoint >>0  & 0x3F) | 0x80;
      output[3] = '\0';

      return 3;
    }

    if (codepoint < 0x110000) {
      output[0] = (codepoint >>18 & 0x07) | 0xF0;
      output[1] = (codepoint >>12 & 0x3F) | 0x80;
      output[2] = (codepoint >>6  & 0x3F) | 0x80;
      output[3] = (codepoint >>0  & 0x3F) | 0x80;
      output[4] = '\0';

      return 4;
    }

    return 0;
}