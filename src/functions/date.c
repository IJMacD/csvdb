#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <time.h>

#include "../structs.h"
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
 * Parses a single date into a DateTime struct
 *
 * Supports the following formats:
 *  - TODAY()
 *  - CURRENT_DATE
 *  - 2023-01-01
 *  - 2023‐01‐01 (U+2010)
 *  - -2023-01-01
 *  - −2023‐01‐01 (U+2212, U+2010)
 *  - +02023-01-01
 *  - +02023‐01‐01 (U+2010)
 *  - -02023-01-01
 *  - −02023‐01‐01 (U+2212, U+2010)
 *  - 01-JAN-2023
 *  - 01 JAN 2023
 *  - 2023-001
 *  - 2023‐001 (U+2010)
 *  - 2023-W08-6
 *  - 2023‐W08‐6 (U+2010)
 *  - 2023W086
 *
 * Returns 1 on success; 0 on failure
 */
int parseDateTime(const char *input, struct DateTime *output) {
    if (strcmp(input, "CURRENT_DATE") == 0) {
        time_t t = time(NULL);
        struct tm *local = localtime(&t);

        output->year = local->tm_year + 1900;
        output->month = local->tm_mon + 1;
        output->day = local->tm_mday;

        return 1;
    }
    if (strcmp(input, "CURRENT_TIME") == 0) {
        time_t t = time(NULL);
        struct tm *local = localtime(&t);

        output->hour = local->tm_hour;
        output->minute = local->tm_min;
        output->second = local->tm_sec;

        return 1;
    }

    if (checkFormat(input, "nnnn-nn-nnTnn:nn:nn")) {
        output->year = atoi(input);
        output->month = atoi(input + 5);
        output->day = atoi(input + 8);

        output->hour = atoi(input + 11);
        output->minute = atoi(input + 14);
        output->second = atoi(input + 17);

        return 1;
    }

    if (checkFormat(input, "nnnn-nn-nn")) {
        output->year = atoi(input);
        output->month = atoi(input + 5);
        output->day = atoi(input + 8);

        output->hour = 0;
        output->minute = 0;
        output->second = 0;

        return 1;
    }

    if (checkFormat(input, "nnnn\xe2\x80\x90nn\xe2\x80\x90nn")) {
        output->year = atoi(input);
        output->month = atoi(input + 7);
        output->day = atoi(input + 12);

        output->hour = 0;
        output->minute = 0;
        output->second = 0;

        return 1;
    }

    // We should only parse formats that are clearly and explicitly dates.
    // e.g. FUNC_ADD always tests if its operands are dates.
    // This breaks when trying to add 7 or 8 digit numbers
    // if (checkFormat(input, "nnnnnnnn")) {
    //     int val = atoi(input);
    //     output->year = val / 10000;
    //     output->month = (val / 100) % 100;
    //     output->day = val % 100;

    //     output->hour = 0;
    //     output->minute = 0;
    //     output->second = 0;

    //     return 1;
    // }

    if (checkFormat(input, "nnnn-nnn")
        || checkFormat(input, "nnnn\xe2\x80\x90nnn")
        // || checkFormat(input, "nnnnnnn")
    ) {
        int val = atoi(input);
        int yearday;

        if (input[4] == '-') {
            output->year = val;
            yearday = atoi(input + 5);
        }
        else if (input[4] == '\xe2') {
            output->year = val;
            yearday = atoi(input + 7);
        }
        else {
            yearday = val % 1000;
            output->year = val / 1000;
        }

        int i = 0;
        for (; i < 12; i++) {
            if (month_index[i] > yearday) {
                break;
            }
        }

        output->month = i;

        output->day = yearday - month_index[i - 1];

        output->hour = 0;
        output->minute = 0;
        output->second = 0;

        return 1;
    }

    if (checkFormat(input, "-nnnn-nn-nn")) {
        output->year = atoi(input);
        output->month = atoi(input + 6);
        output->day = atoi(input + 9);

        output->hour = 0;
        output->minute = 0;
        output->second = 0;

        return 1;
    }

    if (checkFormat(input, "\xe2\x88\x92nnnn\xe2\x80\x90nn\xe2\x80\x90nn")) {
        output->year = -atoi(input + 3);
        output->month = atoi(input + 10);
        output->day = atoi(input + 15);

        output->hour = 0;
        output->minute = 0;
        output->second = 0;

        return 1;
    }

    if (checkFormat(input, "+nnnnn-nn-nn")
        || checkFormat(input, "-nnnnn-nn-nn")
    ) {
        output->year = atoi(input);
        output->month = atoi(input + 7);
        output->day = atoi(input + 10);

        output->hour = 0;
        output->minute = 0;
        output->second = 0;

        return 1;
    }

    if (checkFormat(input, "+nnnnn\xe2\x80\x90nn\xe2\x80\x90nn")) {
        output->year = atoi(input);
        output->month = atoi(input + 9);
        output->day = atoi(input + 12);

        output->hour = 0;
        output->minute = 0;
        output->second = 0;

        return 1;
    }

    if (checkFormat(input, "\xe2\x88\x92nnnnn\xe2\x80\x90nn\xe2\x80\x90nn")) {
        output->year = -atoi(input + 3);
        output->month = atoi(input + 11);
        output->day = atoi(input + 16);

        output->hour = 0;
        output->minute = 0;
        output->second = 0;

        return 1;
    }

    if (
        checkFormat(input, "nn-aaa-nnnn")
        || checkFormat(input, "nn aaa nnnn")
    ) {
        output->day = atoi(input);

        const char *c = input + 3;

        int m = 0;
             if (strncmp(c, "Jan", 3) == 0 || strncmp(c, "JAN", 3) == 0) m = 1;
        else if (strncmp(c, "Feb", 3) == 0 || strncmp(c, "FEB", 3) == 0) m = 2;
        else if (strncmp(c, "Mar", 3) == 0 || strncmp(c, "MAR", 3) == 0) m = 3;
        else if (strncmp(c, "Apr", 3) == 0 || strncmp(c, "APR", 3) == 0) m = 4;
        else if (strncmp(c, "May", 3) == 0 || strncmp(c, "MAY", 3) == 0) m = 5;
        else if (strncmp(c, "Jun", 3) == 0 || strncmp(c, "JUN", 3) == 0) m = 6;
        else if (strncmp(c, "Jul", 3) == 0 || strncmp(c, "JUL", 3) == 0) m = 7;
        else if (strncmp(c, "Aug", 3) == 0 || strncmp(c, "AUG", 3) == 0) m = 8;
        else if (strncmp(c, "Sep", 3) == 0 || strncmp(c, "SEP", 3) == 0) m = 9;
        else if (strncmp(c, "Oct", 3) == 0 || strncmp(c, "OCT", 3) == 0) m = 10;
        else if (strncmp(c, "Nov", 3) == 0 || strncmp(c, "NOV", 3) == 0) m = 11;
        else if (strncmp(c, "Dec", 3) == 0 || strncmp(c, "DEC", 3) == 0) m = 12;

        if (m == 0) {
            return 0;
        }

        output->month = m;

        output->year = atoi(input + 7);

        output->hour = 0;
        output->minute = 0;
        output->second = 0;

        return 1;
    }

    if (checkFormat(input, "nnnn-Wnn-n")
        || checkFormat(input, "nnnn\xe2\x80\x90Wnn\xe2\x80\x90n")
        || checkFormat(input, "nnnnWnnn")
    ) {
        int weekyear = atoi(input);
        int week, weekday;

        if (input[4] == '-') {
            week = atoi(input + 6);
            weekday = input[9] - '0';
        }
        else if (input[4] == '\xe2') {
            week = atoi(input + 8);
            weekday = input[13] - '0';
        }
        else {
            int val = atoi(input + 5);
            week = val / 10;
            weekday = val % 10;
        }

        output->year = weekyear;
        output->month = 1;
        output->day = 1;

        output->hour = 0;
        output->minute = 0;
        output->second = 0;

        int julian = datetimeGetJulian(output);

        int weekdayFirst = datetimeGetWeekDay(output);

        int days = (week - 1) * 7 + weekday;

        julian += days - weekdayFirst;

        if (weekdayFirst >= 4) {
            julian += 7;
        }

        datetimeFromJulian(output, julian);

        return 1;
    }

    return 0;
}

/**
 * Parses a time 01:23:45 into a DateTime object
 * @returns 1 for success; 0 for failure
 */
int parseTime(const char *input, struct DateTime *output)
{
    if (checkFormat(input, "nn:nn:nn"))
    {
        output->year = 0;
        output->month = 0;
        output->day = 0;

        output->hour = atoi(input);
        output->minute = atoi(input + 3);
        output->second = atoi(input + 6);

        return 1;
    }

    // Hours are allowed to roll over
    if (checkFormat(input, "nnn:nn:nn"))
    {
        output->year = 0;
        output->month = 0;
        output->day = 0;

        output->hour = atoi(input);
        output->minute = atoi(input + 4);
        output->second = atoi(input + 7);

        return 1;
    }

    // Hours are allowed to roll over
    if (checkFormat(input, "nnnn:nn:nn"))
    {
        output->year = 0;
        output->month = 0;
        output->day = 0;

        output->hour = atoi(input);
        output->minute = atoi(input + 5);
        output->second = atoi(input + 8);

        return 1;
    }

    return 0;
}

/**
 * `output` must be at least 11 chars long
 */
int sprintDate (char *output, struct DateTime *dt) {
    return sprintf(output, "%04d-%02d-%02d", dt->year, dt->month, dt->day);
}

int sprintTime(char *output, struct DateTime *dt)
{
    return sprintf(output, "%02d:%02d:%02d", dt->hour, dt->minute, dt->second);
}

/**
 * Verifies input matches format.
 * 'n' matches a digit
 * 'a' matches a letter
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

    // Special case for 0000-01-01
    if (dt->year == 0 && dt->month == 1 && dt->day == 1) {
        return 52;
    }

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

    // Special case for 0000-01-01
    if (dt->year == 0 && dt->month == 1 && dt->day == 1) {
        return -1;
    }

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
 * ISO Week Day
 * 1 = Mon, 7 = Sun
 */
int datetimeGetWeekDay (struct DateTime *dt) {
    struct DateTime dt2 = *dt;
    dt2.hour = 12;
    int julian = datetimeGetJulian(&dt2);
    return (julian % 7) + 1;
}

/**
 * Respects hour field in DateTime struct
 * i.e. if dt->hour == 0 then gives JD at midnight
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

int timeInSeconds(struct DateTime *dt)
{
    return dt->hour * 3600 + dt->minute * 60 + dt->second;
}

void timeFromSeconds(struct DateTime *dt, int seconds)
{
    dt->year = 0;
    dt->month = 0;
    dt->day = 0;

    dt->hour = seconds / 3600;
    dt->minute = (seconds / 60) % 60;
    dt->second = seconds % 60;
}