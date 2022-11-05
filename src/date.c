#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <time.h>

#include "structs.h"
#include "date.h"

int checkFormat(const char *input, const char *format);

const int month_index[] = {
//   31   28   31
      0,  31,  59,
//   30   31   30
     90, 120, 151,
//   31   31   30
    181, 212, 243,
//   31   30   31
    273, 304, 334
};

/**
 * Returns 1 on success; 0 on failure
 */
int parseDateTime(const char *input, struct DateTime *output) {
    if (strcmp(input, "TODAY()") == 0 || strcmp(input, "CURRENT_DATE") == 0) {
        time_t t = time(NULL);
        struct tm *local = localtime(&t);

        output->year = local->tm_year + 1900;
        output->month = local->tm_mon + 1;
        output->day = local->tm_mday;

        output->hour = 0;
        output->minute = 0;
        output->second = 0;

        return 1;
    }

    if (checkFormat(input, "+nnnnn-nn-nn")) {
        char v[6] = {0};

        memcpy(v, input + 1, 5);
        v[5] = '\0';
        output->year = atoi(v);

        memcpy(v, input + 7, 2);
        v[2] = '\0';
        output->month = atoi(v);

        memcpy(v, input + 10, 2);
        v[2] = '\0';
        output->day = atoi(v);

        output->hour = 0;
        output->minute = 0;
        output->second = 0;

        return 1;
    }

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

        output->hour = 0;
        output->minute = 0;
        output->second = 0;

        return 1;
    }

    if (
        checkFormat(input, "nn-aaa-nnnn")
        || checkFormat(input, "nn aaa nnnn")
    ) {
        char v[5] = {0};

        memcpy(v, input, 2);
        v[2] = '\0';
        output->day = atoi(v);

        char m = 0;
        memcpy(v, input + 3, 3);
        v[3] = '\0';
             if (strcmp(v, "Jan") == 0 || strcmp(v, "JAN") == 0) m = 1;
        else if (strcmp(v, "Feb") == 0 || strcmp(v, "FEB") == 0) m = 2;
        else if (strcmp(v, "Mar") == 0 || strcmp(v, "MAR") == 0) m = 3;
        else if (strcmp(v, "Apr") == 0 || strcmp(v, "APR") == 0) m = 4;
        else if (strcmp(v, "May") == 0 || strcmp(v, "MAY") == 0) m = 5;
        else if (strcmp(v, "Jun") == 0 || strcmp(v, "JUN") == 0) m = 6;
        else if (strcmp(v, "Jul") == 0 || strcmp(v, "JUL") == 0) m = 7;
        else if (strcmp(v, "Aug") == 0 || strcmp(v, "AUG") == 0) m = 8;
        else if (strcmp(v, "Sep") == 0 || strcmp(v, "SEP") == 0) m = 9;
        else if (strcmp(v, "Oct") == 0 || strcmp(v, "OCT") == 0) m = 10;
        else if (strcmp(v, "Nov") == 0 || strcmp(v, "NOV") == 0) m = 11;
        else if (strcmp(v, "Dec") == 0 || strcmp(v, "DEC") == 0) m = 12;
        output->month = m;

        memcpy(v, input + 7, 4);
        v[4] = '\0';
        output->year = atoi(v);

        output->hour = 0;
        output->minute = 0;
        output->second = 0;

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
 * Result is negative if dt1 is less than dt2
 */
int datetimeGetDayDiff(struct DateTime *dt1, struct DateTime *dt2) {
    return datetimeGetJulian(dt1) - datetimeGetJulian(dt2);
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

/**
 * Algorithm from https://quasar.as.utexas.edu/BillInfo/JulianDatesG.html
 */
int datetimeGetJulian (struct DateTime *dt) {
    int y = dt->year;
    int m = dt->month;
    int d = dt->day;

    if (m < 3) {
        y--;
        m += 12;
    }

    int a = y / 100;
    int b = a / 4;
    int c = 2 - a + b;
    int e = 365.25 * (y + 4716);
    int f = 30.6001 * (m + 1);

    int h = dt->hour;

    int g = h < 12 ? 1 : 0;

    return c + d + e + f - 1524 - g;
}

/**
 * Algorithm from https://quasar.as.utexas.edu/BillInfo/JulianDatesG.html
 */
void datetimeFromJulian (struct DateTime *dt, int julian) {
    int Z = julian + 1;
    int W = (Z - 1867216.25)/36524.25;
    int X = W/4;
    int A = Z+1+W-X;
    int B = A+1524;
    int C = (B-122.1)/365.25;
    int D = 365.25 * C;
    int E = (B-D)/30.6001;
    int F = 30.6001 * E;

    dt->day = B-D-F;
    dt->month = E-1;
    if (dt->month > 12) {
        dt->month -= 12;
    }
    dt->year = dt->month <= 2 ? C-4715 : C-4716;
}