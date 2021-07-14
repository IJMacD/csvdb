#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "function.h"

struct DateTime {
    unsigned short year;
    unsigned char month;
    unsigned char day;
    unsigned char hour;
    unsigned char minute;
    unsigned char second;
};

                          //  31  28  31   30   31   30   31   31   30   31   30   31
const int month_index[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 };

int parseDateTime(const char *input, struct DateTime *output);

int checkFormat(const char *input, const char *format);

int isLeapYear(int year);

int datetimeGetYearDay(struct DateTime *dt);

int datetimeGetDayDiff(struct DateTime *dt1, struct DateTime *dt2);

int datetimeGetWeek (struct DateTime *dt);

int datetimeGetWeekYear (struct DateTime *dt);

int datetimeGetWeekDay (struct DateTime *dt);

int outputFunction(FILE *f, struct DB *db, struct ResultColumn *column, int record_index) {
    char value[VALUE_MAX_LENGTH];
    if (getRecordValue(db, record_index, column->field, value, VALUE_MAX_LENGTH) > 0) {

        if (column->function == FUNC_UNITY) {
            fprintf(f, "%s", value);
        }
        else if ((column->function & MASK_FUNC_FAMILY) == FUNC_EXTRACT) {
            struct DateTime dt;

            if (!parseDateTime(value, &dt)) {
                return 0;
            }

            if (column->function == FUNC_EXTRACT_YEAR){
                fprintf(f, "%d", dt.year);
            }
            else if (column->function == FUNC_EXTRACT_MONTH) {
                fprintf(f, "%d", dt.month);
            }
            else if (column->function == FUNC_EXTRACT_DAY) {
                fprintf(f, "%d", dt.day);
            }
            else if (column->function == FUNC_EXTRACT_WEEK) {
                fprintf(f, "%d", datetimeGetWeek(&dt));
            }
            else if (column->function == FUNC_EXTRACT_WEEKYEAR) {
                fprintf(f, "%d", datetimeGetWeekYear(&dt));
            }
            else if (column->function == FUNC_EXTRACT_WEEKDAY) {
                fprintf(f, "%d", datetimeGetWeekDay(&dt));
            }
            else if (column->function == FUNC_EXTRACT_HEYEAR) {
                fprintf(f, "%d", dt.year + 10000);
            }
            else if (column->function == FUNC_EXTRACT_YEARDAY) {
                fprintf(f, "%d", datetimeGetYearDay(&dt));
            }
            else if (column->function == FUNC_EXTRACT_MILLENNIUM) {
                fprintf(f, "%d", dt.year / 1000);
            }
            else if (column->function == FUNC_EXTRACT_CENTURY) {
                fprintf(f, "%d", dt.year / 100);
            }
            else if (column->function == FUNC_EXTRACT_DECADE) {
                fprintf(f, "%d", dt.year / 10);
            }
            else if (column->function == FUNC_EXTRACT_QUARTER) {
                fprintf(f, "%d", (dt.month - 1) / 3 + 1);
            }
            else if (column->function == FUNC_EXTRACT_DATE) {
                fprintf(f, "%04d-%02d-%02d", dt.year, dt.month, dt.day);
            }
            else {
                fprintf(f, "BADEXTRACT");
            }
        }
        else {
            return -1;
        }
    }

    return 0;
}


/**
 * Returns 1 on success; 0 on failure
 */
int parseDateTime(const char *input, struct DateTime *output) {
    if (checkFormat(input, "nnnn-nn-nn")) {
        char v[5] = {0};

        memcpy(v, input, 4);
        v[4] = '\0';
        output->year = atoi(v);

        memcpy(v, input + 5, 2);
        v[2] = '\0';
        output->month = atoi(v);

        memcpy(v, input + 8, 2);
        v[2] = '\0';
        output->day = atoi(v);

        return 1;
    }

    if (checkFormat(input, "nn-aaa-nnnn")) {
        char v[5] = {0};

        memcpy(v, input, 2);
        v[2] = '\0';
        output->day = atoi(v);

        memcpy(v, input + 3, 3);
        v[3] = '\0';
             if (strcmp(v, "Jan") == 0 || strcmp(v, "JAN") == 0) output->month = 1;
        else if (strcmp(v, "Feb") == 0 || strcmp(v, "FEB") == 0) output->month = 2;
        else if (strcmp(v, "Mar") == 0 || strcmp(v, "MAR") == 0) output->month = 3;
        else if (strcmp(v, "Apr") == 0 || strcmp(v, "APR") == 0) output->month = 4;
        else if (strcmp(v, "May") == 0 || strcmp(v, "MAY") == 0) output->month = 5;
        else if (strcmp(v, "Jun") == 0 || strcmp(v, "JUN") == 0) output->month = 6;
        else if (strcmp(v, "Jul") == 0 || strcmp(v, "JUL") == 0) output->month = 7;
        else if (strcmp(v, "Aug") == 0 || strcmp(v, "AUG") == 0) output->month = 8;
        else if (strcmp(v, "Sep") == 0 || strcmp(v, "SEP") == 0) output->month = 9;
        else if (strcmp(v, "Oct") == 0 || strcmp(v, "OCT") == 0) output->month = 10;
        else if (strcmp(v, "Nov") == 0 || strcmp(v, "NOV") == 0) output->month = 11;
        else if (strcmp(v, "Dec") == 0 || strcmp(v, "DEC") == 0) output->month = 12;

        memcpy(v, input + 7, 4);
        v[4] = '\0';
        output->year = atoi(v);

        return 1;
    }

    return 0;
}

/**
 * Verifies input matches format.
 * 'n' matches a digit
 * 'a' matches a character
 * Other characters match exactly
 * Returns 0 if the input does not match the format
 * Returns 1 if the input does match the format
 */
int checkFormat(const char *input, const char *format) {
    const char *ptr_i = input;
    const char *ptr_f = format;
    while(*ptr_f != '\0') {
        if (*ptr_i == '\0') return 0;
        if (*ptr_f == 'n') {
            if (!isdigit(*ptr_i)) return 0;
        }
        else if (*ptr_f == 'a') {
            if (!isalpha(*ptr_i)) return 0;
        }
        else if (*ptr_f != *ptr_i) return 0;

        ptr_i++;
        ptr_f++;
    }

    return 1;
}

int isLeapYear(int year) {
    return (year % 4 == 0 && (year % 400 == 0 || year % 100 != 0));
}

int datetimeGetYearDay(struct DateTime *dt) {
    int leap_day = dt->month > 2 && isLeapYear(dt->year) ? 1 : 0;
    return month_index[dt->month - 1] + dt->day + leap_day;
}

/**
 * dt1 must be less than dt2;
 */
int datetimeGetDayDiff(struct DateTime *dt1, struct DateTime *dt2) {
    int yd1 = datetimeGetYearDay(dt1);
    int yd2 = datetimeGetYearDay(dt2);
    int delta = -yd1;

    int year = dt1->year;
    while (year++ < dt2->year) {
        delta += isLeapYear(year) ? 366 : 365;
    }

    delta += yd2;

    return delta;
}

/**
 * ISO Week Number
 * Algorithm from https://www.tondering.dk/claus/cal/week.php
 */
int datetimeGetWeek (struct DateTime *dt) {
    int n, g, s;

    if (dt->month < 3) {
        int a = dt->year - 1;
        int b = a / 4 - a / 100 + a / 400;
        int c = (a - 1) / 4 - (a - 1) / 100 + (a - 1) / 400;
        s = b - c;
        int e = 0;
        int f = dt->day - 1 + 31 * (dt->month - 1);
        g = (a + b) % 7;
        int d = (f + g - e) % 7;
        n = f + 3 - d;
    }
    else {
        int a = dt->year;
        int b = a / 4 - a / 100 + a / 400;
        int c = (a - 1) / 4 - (a - 1) / 100 + (a - 1) / 400;
        s = b - c;
        int e = s + 1;
        int f = dt->day + (153 * (dt->month - 3) + 2) / 5 + 58 + s;
        g = (a + b) % 7;
        int d = (f + g - e) % 7;
        n = f + 3 - d;
    }

    if (n < 0) {
        return 53 - (g - s) / 5;
    }
    else if (n > 364 + s) {
        return 1;
    }
    else {
        return n / 7 + 1;
    }
}


/**
 * ISO Week Year
 * Algorithm from https://www.tondering.dk/claus/cal/week.php
 */
int datetimeGetWeekYear (struct DateTime *dt) {
    int n, g, s;

    if (dt->month < 3) {
        int a = dt->year - 1;
        int b = a / 4 - a / 100 + a / 400;
        int c = (a - 1) / 4 - (a - 1) / 100 + (a - 1) / 400;
        s = b - c;
        int e = 0;
        int f = dt->day - 1 + 31 * (dt->month - 1);
        g = (a + b) % 7;
        int d = (f + g - e) % 7;
        n = f + 3 - d;
    }
    else {
        int a = dt->year;
        int b = a / 4 - a / 100 + a / 400;
        int c = (a - 1) / 4 - (a - 1) / 100 + (a - 1) / 400;
        s = b - c;
        int e = s + 1;
        int f = dt->day + (153 * (dt->month - 3) + 2) / 5 + 58 + s;
        g = (a + b) % 7;
        int d = (f + g - e) % 7;
        n = f + 3 - d;
    }

    if (n < 0) {
        return dt->year - 1;
    }
    else if (n > 364 + s) {
        return dt->year + 1;
    }
    else {
        return dt->year;
    }
}

/**
 * ISO Week Number
 * Algorithm from https://www.tondering.dk/claus/cal/week.php
 */
int datetimeGetWeekDay (struct DateTime *dt) {
    int d;

    if (dt->month < 3) {
        int a = dt->year - 1;
        int b = a / 4 - a / 100 + a / 400;
        int e = 0;
        int f = dt->day - 1 + 31 * (dt->month - 1);
        int g = (a + b) % 7;
        d = (f + g - e) % 7;
    }
    else {
        int a = dt->year;
        int b = a / 4 - a / 100 + a / 400;
        int c = (a - 1) / 4 - (a - 1) / 100 + (a - 1) / 400;
        int s = b - c;
        int e = s + 1;
        int f = dt->day + (153 * (dt->month - 3) + 2) / 5 + 58 + s;
        int g = (a + b) % 7;
        d = (f + g - e) % 7;
    }

    return d + 1;
}