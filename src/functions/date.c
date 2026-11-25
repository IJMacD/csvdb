#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdio.h>
#include <time.h>
#include <limits.h>

#include "../structs.h"
#include "date.h"

int checkFormat(const char *input, const char *format);

static time_t timegmCompat(struct tm *tm);

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

static const int month_day_counts[] = {
    31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
};

/**
 * Parses a single dateTime into a DateTime struct
 *
 * Supports the following formats:
 *  - 2013-01-01T10:10:00          (local / unknown zone)
 *  - 2013-01-01T10:10:00Z         (UTC)
 *  - 2013-01-01T10:10:00+09:00    (converted to UTC)
 *  - 2013-01-01T10:10:00-05:30    (converted to UTC)
 *
 * Returns 1 on success; 0 on failure
 */
int parseDateTime(const char *input, struct DateTime *output)
{
    if (!checkFormat(input, "nnnn-nn-nnTnn:nn:nn"))
    {
        return 0;
    }

    output->year = atoi(input);
    output->month = atoi(input + 5);
    output->day = atoi(input + 8);
    output->hour = atoi(input + 11);
    output->minute = atoi(input + 14);
    output->second = atoi(input + 17);

    const char *rest = input + 19;

    if (*rest == '\0')
    {
        output->tz_offset_minutes = TZ_LOCAL;
        return 1;
    }

    short offset = 0;
    if (!parseTzOffset(rest, &offset))
    {
        return 0;
    }

    if (offset == 0)
    {
        output->tz_offset_minutes = 0;
        return 1;
    }

    // Convert wall clock in `offset` to UTC for storage
    output->tz_offset_minutes = offset;
    if (!datetimeAtTimeZone(output, 0))
    {
        return 0;
    }
    return 1;
}

int parseDateOrDateTime(const char *input, struct DateTime *output)
{
    if (parseDateTime(input, output))
    {
        return 1;
    }
    return parseDate(input, output);
}

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
int parseDate(const char *input, struct DateTime *output)
{
    // Only set TZ_LOCAL after a successful match — callers may try parseDate
    // after parseDateTime on the same struct (e.g. FUNC_ADD).

    if (strcmp(input, "CURRENT_DATE") == 0) {
        time_t t = time(NULL);
        struct tm *local = localtime(&t);

        output->year = local->tm_year + 1900;
        output->month = local->tm_mon + 1;
        output->day = local->tm_mday;
        output->tz_offset_minutes = TZ_LOCAL;

        return 1;
    }

    // Require end of string so datetimes (…T…) are not accepted as dates
    if (checkFormat(input, "nnnn-nn-nn") && input[10] == '\0') {
        output->year = atoi(input);
        output->month = atoi(input + 5);
        output->day = atoi(input + 8);

        output->hour = 0;
        output->minute = 0;
        output->second = 0;

        output->tz_offset_minutes = TZ_LOCAL;
        return 1;
    }

    if (checkFormat(input, "nnnn\xe2\x80\x90nn\xe2\x80\x90nn")) {
        output->year = atoi(input);
        output->month = atoi(input + 7);
        output->day = atoi(input + 12);

        output->hour = 0;
        output->minute = 0;
        output->second = 0;

        output->tz_offset_minutes = TZ_LOCAL;
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

        output->tz_offset_minutes = TZ_LOCAL;
        return 1;
    }

    if (checkFormat(input, "-nnnn-nn-nn")) {
        output->year = atoi(input);
        output->month = atoi(input + 6);
        output->day = atoi(input + 9);

        output->hour = 0;
        output->minute = 0;
        output->second = 0;

        output->tz_offset_minutes = TZ_LOCAL;
        return 1;
    }

    if (checkFormat(input, "\xe2\x88\x92nnnn\xe2\x80\x90nn\xe2\x80\x90nn")) {
        output->year = -atoi(input + 3);
        output->month = atoi(input + 10);
        output->day = atoi(input + 15);

        output->hour = 0;
        output->minute = 0;
        output->second = 0;

        output->tz_offset_minutes = TZ_LOCAL;
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

        output->tz_offset_minutes = TZ_LOCAL;
        return 1;
    }

    if (checkFormat(input, "+nnnnn\xe2\x80\x90nn\xe2\x80\x90nn")) {
        output->year = atoi(input);
        output->month = atoi(input + 9);
        output->day = atoi(input + 12);

        output->hour = 0;
        output->minute = 0;
        output->second = 0;

        output->tz_offset_minutes = TZ_LOCAL;
        return 1;
    }

    if (checkFormat(input, "\xe2\x88\x92nnnnn\xe2\x80\x90nn\xe2\x80\x90nn")) {
        output->year = -atoi(input + 3);
        output->month = atoi(input + 11);
        output->day = atoi(input + 16);

        output->hour = 0;
        output->minute = 0;
        output->second = 0;

        output->tz_offset_minutes = TZ_LOCAL;
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

        output->tz_offset_minutes = TZ_LOCAL;
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

        // ISO week 1 contains the year's first Thursday. If Jan 1 is
        // Friday–Sunday (weekday 5–7), that week belongs to the previous
        // ISO year and week 1 starts the following Monday — shift by 7.
        // Thursday (4) is still week 1 of the new year; do not shift.
        if (weekdayFirst >= 5) {
            julian += 7;
        }

        datetimeFromJulian(output, julian);

        output->tz_offset_minutes = TZ_LOCAL;
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
    // Only set TZ_LOCAL after a successful match (see parseDate).

    if (strcmp(input, "CURRENT_TIME") == 0)
    {
        time_t t = time(NULL);
        struct tm *local = localtime(&t);

        output->hour = local->tm_hour;
        output->minute = local->tm_min;
        output->second = local->tm_sec;
        output->tz_offset_minutes = TZ_LOCAL;

        return 1;
    }

    if (checkFormat(input, "nn:nn:nn"))
    {
        output->year = 0;
        output->month = 0;
        output->day = 0;

        output->hour = atoi(input);
        output->minute = atoi(input + 3);
        output->second = atoi(input + 6);

        output->tz_offset_minutes = TZ_LOCAL;
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

        output->tz_offset_minutes = TZ_LOCAL;
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

        output->tz_offset_minutes = TZ_LOCAL;
        return 1;
    }

    return 0;
}

/**
 * `output` must be at least 25 chars long (YYYY-MM-DDTHH:MM:SS+HH:MM)
 */
int sprintDateTime(char *output, struct DateTime *dt)
{
    int n = sprintf(
        output,
        "%04d-%02d-%02dT%02d:%02d:%02d",
        dt->year, dt->month, dt->day,
        dt->hour, dt->minute, dt->second);

    if (dt->tz_offset_minutes == TZ_LOCAL)
    {
        return n;
    }

    if (dt->tz_offset_minutes == 0 && dt->tz_format != TZ_FMT_OFFSET)
    {
        output[n++] = 'Z';
        output[n] = '\0';
        return n;
    }

    int off = dt->tz_offset_minutes;
    char sign = '+';
    if (off < 0)
    {
        sign = '-';
        off = -off;
    }
    return n + sprintf(output + n, "%c%02d:%02d", sign, off / 60, off % 60);
}

/**
 * `output` must be at least 11 chars long
 */
int sprintDate(char *output, struct DateTime *dt)
{
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

int daysInMonth(int year, int month) {
    if (month < 1 || month > 12) {
        return 0;
    }
    if (month == 2 && isLeapYear(year)) {
        return 29;
    }
    return month_day_counts[month - 1];
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
    dt->tz_offset_minutes = TZ_LOCAL;
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
    dt->tz_offset_minutes = TZ_LOCAL;
}

static time_t timegmCompat(struct tm *tm)
{
#if defined(__USE_MISC) || defined(_DEFAULT_SOURCE) || defined(_BSD_SOURCE) || defined(__APPLE__)
    return timegm(tm);
#else
    /* Fallback: force UTC via environment-free algorithm using timegm if
     * declared, otherwise approximate with mktime after setting tm_isdst=0.
     * Prefer the GNU/BSD timegm when available at link time. */
    extern time_t timegm(struct tm *);
    return timegm(tm);
#endif
}

long datetimeGetUnix(struct DateTime *dt)
{
    struct tm tp = {0};
    tp.tm_year = dt->year - 1900;
    tp.tm_mon = dt->month - 1;
    tp.tm_mday = dt->day;
    tp.tm_hour = dt->hour;
    tp.tm_min = dt->minute;
    tp.tm_sec = dt->second;
    tp.tm_isdst = -1;

    if (dt->tz_offset_minutes == TZ_LOCAL)
    {
        return mktime(&tp);
    }

    tp.tm_isdst = 0;
    return timegmCompat(&tp) - (long)dt->tz_offset_minutes * 60L;
}

void datetimeFromUnix(struct DateTime *dt, time_t time)
{
    if (dt->tz_offset_minutes == TZ_LOCAL)
    {
        struct tm *tp = localtime(&time);
        dt->year = tp->tm_year + 1900;
        dt->month = tp->tm_mon + 1;
        dt->day = tp->tm_mday;
        dt->hour = tp->tm_hour;
        dt->minute = tp->tm_min;
        dt->second = tp->tm_sec;
        return;
    }

    time_t wall = time + (long)dt->tz_offset_minutes * 60L;
    struct tm *tp = gmtime(&wall);
    dt->year = tp->tm_year + 1900;
    dt->month = tp->tm_mon + 1;
    dt->day = tp->tm_mday;
    dt->hour = tp->tm_hour;
    dt->minute = tp->tm_min;
    dt->second = tp->tm_sec;
}

enum DateUnit parseDateUnit(const char *unit)
{
    if (unit == NULL || unit[0] == '\0')
    {
        return DATE_UNIT_INVALID;
    }

    if (strcmp(unit, "MILLENNIUM") == 0 || strcmp(unit, "MILLENNIUMS") == 0 ||
        strcmp(unit, "MILLENNIA") == 0)
        return DATE_UNIT_MILLENNIUM;
    if (strcmp(unit, "CENTURY") == 0 || strcmp(unit, "CENTURIES") == 0)
        return DATE_UNIT_CENTURY;
    if (strcmp(unit, "DECADE") == 0 || strcmp(unit, "DECADES") == 0)
        return DATE_UNIT_DECADE;
    if (strcmp(unit, "YEAR") == 0 || strcmp(unit, "YEARS") == 0)
        return DATE_UNIT_YEAR;
    if (strcmp(unit, "QUARTER") == 0 || strcmp(unit, "QUARTERS") == 0)
        return DATE_UNIT_QUARTER;
    if (strcmp(unit, "MONTH") == 0 || strcmp(unit, "MONTHS") == 0)
        return DATE_UNIT_MONTH;
    if (strcmp(unit, "WEEK") == 0 || strcmp(unit, "WEEKS") == 0)
        return DATE_UNIT_WEEK;
    if (strcmp(unit, "DAY") == 0 || strcmp(unit, "DAYS") == 0)
        return DATE_UNIT_DAY;
    if (strcmp(unit, "HOUR") == 0 || strcmp(unit, "HOURS") == 0)
        return DATE_UNIT_HOUR;
    if (strcmp(unit, "MINUTE") == 0 || strcmp(unit, "MINUTES") == 0)
        return DATE_UNIT_MINUTE;
    if (strcmp(unit, "SECOND") == 0 || strcmp(unit, "SECONDS") == 0)
        return DATE_UNIT_SECOND;

    return DATE_UNIT_INVALID;
}

int intervalToInt(int count, enum DateUnit unit, long *out)
{
    switch (unit)
    {
    case DATE_UNIT_SECOND:
        *out = (long)count;
        return 0;
    case DATE_UNIT_MINUTE:
        *out = (long)count * 60L;
        return 0;
    case DATE_UNIT_HOUR:
        *out = (long)count * 3600L;
        return 0;
    case DATE_UNIT_DAY:
        *out = (long)count;
        return 0;
    case DATE_UNIT_WEEK:
        *out = (long)count * 7L;
        return 0;
    default:
        return -1;
    }
}

int isCalendarIntervalConstant(const char *text)
{
    if (text == NULL || text[0] == '\0')
    {
        return 0;
    }

    const char *p = text;
    while (*p == ' ')
    {
        p++;
    }

    char *end = NULL;
    long amount = strtol(p, &end, 10);
    if (end == p)
    {
        return 0;
    }

    while (*end == ' ')
    {
        end++;
    }

    // Plain integer (day-/second-scale INTERVAL already folded at parse time)
    if (*end == '\0')
    {
        return 0;
    }

    enum DateUnit unit = parseDateUnit(end);
    if (unit == DATE_UNIT_INVALID)
    {
        return 0;
    }

    if (amount < INT_MIN || amount > INT_MAX)
    {
        return 0;
    }

    long unused = 0;
    return intervalToInt((int)amount, unit, &unused) != 0;
}

int parseIso8601Period(const char *input, struct IsoPeriod *out)
{
    if (input == NULL || out == NULL)
    {
        return 0;
    }

    memset(out, 0, sizeof(*out));

    const char *p = input;
    while (*p == ' ')
    {
        p++;
    }

    int sign = 1;
    if (*p == '-')
    {
        sign = -1;
        p++;
    }

    if (*p != 'P' && *p != 'p')
    {
        return 0;
    }
    p++;

    if (*p == '\0')
    {
        return 0;
    }

    int in_time = 0;
    int saw_any = 0;

    while (*p != '\0')
    {
        if (*p == 'T' || *p == 't')
        {
            if (in_time)
            {
                return 0;
            }
            in_time = 1;
            p++;
            continue;
        }

        char *end = NULL;
        long n = strtol(p, &end, 10);
        if (end == p)
        {
            return 0;
        }
        p = end;

        if (*p == '\0')
        {
            return 0;
        }

        char u = (char)toupper((unsigned char)*p);
        p++;

        if (n < INT_MIN || n > INT_MAX)
        {
            return 0;
        }

        int v = (int)(sign * n);

        if (!in_time)
        {
            if (u == 'Y')
            {
                out->years = v;
            }
            else if (u == 'M')
            {
                out->months = v;
            }
            else if (u == 'W')
            {
                out->weeks = v;
            }
            else if (u == 'D')
            {
                out->days = v;
            }
            else
            {
                return 0;
            }
        }
        else
        {
            out->has_time = 1;
            if (u == 'H')
            {
                out->hours = v;
            }
            else if (u == 'M')
            {
                out->minutes = v;
            }
            else if (u == 'S')
            {
                out->seconds = v;
            }
            else
            {
                return 0;
            }
        }

        saw_any = 1;
    }

    return saw_any;
}

void dateAddPeriod(struct DateTime *dt, const struct IsoPeriod *period, int sign)
{
    if (dt == NULL || period == NULL)
    {
        return;
    }

    if (period->years)
    {
        dateAddUnit(dt, sign * period->years, DATE_UNIT_YEAR);
    }
    if (period->months)
    {
        dateAddUnit(dt, sign * period->months, DATE_UNIT_MONTH);
    }
    if (period->weeks)
    {
        dateAddUnit(dt, sign * period->weeks, DATE_UNIT_WEEK);
    }
    if (period->days)
    {
        dateAddUnit(dt, sign * period->days, DATE_UNIT_DAY);
    }
    if (period->hours)
    {
        dateAddUnit(dt, sign * period->hours, DATE_UNIT_HOUR);
    }
    if (period->minutes)
    {
        dateAddUnit(dt, sign * period->minutes, DATE_UNIT_MINUTE);
    }
    if (period->seconds)
    {
        dateAddUnit(dt, sign * period->seconds, DATE_UNIT_SECOND);
    }
}

int parseTzOffset(const char *input, short *offset_minutes)
{
    if (input == NULL || offset_minutes == NULL)
    {
        return 0;
    }

    if (strcmp(input, "Z") == 0 || strcmp(input, "UTC") == 0)
    {
        *offset_minutes = 0;
        return 1;
    }

    if ((input[0] != '+' && input[0] != '-') || !isdigit((unsigned char)input[1]))
    {
        return 0;
    }

    int sign = (input[0] == '-') ? -1 : 1;
    int hours = 0;
    int mins = 0;

    if (checkFormat(input + 1, "nn:nn") && input[6] == '\0')
    {
        hours = atoi(input + 1);
        mins = atoi(input + 4);
    }
    else if (checkFormat(input + 1, "nnnn") && input[5] == '\0')
    {
        hours = atoi(input + 1) / 100;
        mins = atoi(input + 1) % 100;
    }
    else if (checkFormat(input + 1, "nn") && input[3] == '\0')
    {
        hours = atoi(input + 1);
        mins = 0;
    }
    else
    {
        return 0;
    }

    if (hours > 23 || mins > 59)
    {
        return 0;
    }

    int total = sign * (hours * 60 + mins);
    if (total < SHRT_MIN + 1 || total > SHRT_MAX)
    {
        return 0;
    }

    *offset_minutes = (short)total;
    return 1;
}

int getLocalTzOffsetMinutes(short *offset_minutes)
{
    time_t t = time(NULL);
    struct tm local;

#if defined(_POSIX_VERSION) || defined(__APPLE__) || defined(__linux__)
    if (localtime_r(&t, &local) == NULL)
    {
        return 0;
    }
#else
    struct tm *lp = localtime(&t);
    if (lp == NULL)
    {
        return 0;
    }
    local = *lp;
#endif

#if defined(__tm_gmtoff) || defined(__USE_MISC) || defined(_DEFAULT_SOURCE) || defined(__APPLE__) || defined(__FreeBSD__) || defined(__linux__)
    /* tm_gmtoff is seconds east of UTC (glibc / BSD). */
    long off = local.tm_gmtoff;
    if (off / 60 < SHRT_MIN + 1 || off / 60 > SHRT_MAX)
    {
        return 0;
    }
    *offset_minutes = (short)(off / 60);
    return 1;
#else
    struct tm utc;
#if defined(_POSIX_VERSION) || defined(__APPLE__) || defined(__linux__)
    if (gmtime_r(&t, &utc) == NULL)
    {
        return 0;
    }
#else
    struct tm *gp = gmtime(&t);
    if (gp == NULL)
    {
        return 0;
    }
    utc = *gp;
#endif
    local.tm_isdst = -1;
    utc.tm_isdst = 0;
    time_t local_as = mktime(&local);
    time_t utc_as = timegmCompat(&utc);
    if (local_as == (time_t)-1 || utc_as == (time_t)-1)
    {
        return 0;
    }
    long off = (long)(local_as - utc_as);
    if (off / 60 < SHRT_MIN + 1 || off / 60 > SHRT_MAX)
    {
        return 0;
    }
    *offset_minutes = (short)(off / 60);
    return 1;
#endif
}

void dateAddUnit(struct DateTime *dt, int count, enum DateUnit unit)
{
    if (count == 0 || unit == DATE_UNIT_INVALID)
    {
        return;
    }

    switch (unit)
    {
    case DATE_UNIT_MILLENNIUM:
        dateAddUnit(dt, count * 1000, DATE_UNIT_YEAR);
        break;

    case DATE_UNIT_CENTURY:
        dateAddUnit(dt, count * 100, DATE_UNIT_YEAR);
        break;

    case DATE_UNIT_DECADE:
        dateAddUnit(dt, count * 10, DATE_UNIT_YEAR);
        break;

    case DATE_UNIT_YEAR:
        dt->year += count;
        {
            int max_day = daysInMonth(dt->year, dt->month);
            if (dt->day > max_day)
            {
                dt->day = max_day;
            }
        }
        break;

    case DATE_UNIT_QUARTER:
        dateAddUnit(dt, count * 3, DATE_UNIT_MONTH);
        break;

    case DATE_UNIT_MONTH:
    {
        int month0 = (int)dt->month - 1 + count;
        int year_delta = month0 / 12;
        month0 %= 12;
        if (month0 < 0)
        {
            month0 += 12;
            year_delta--;
        }
        dt->year += year_delta;
        dt->month = month0 + 1;
        {
            int max_day = daysInMonth(dt->year, dt->month);
            if (dt->day > max_day)
            {
                dt->day = max_day;
            }
        }
        break;
    }

    case DATE_UNIT_WEEK:
        dateAddUnit(dt, count * 7, DATE_UNIT_DAY);
        break;

    case DATE_UNIT_DAY:
    {
        int julian = datetimeGetJulian(dt);
        short tz = dt->tz_offset_minutes;
        unsigned char hour = dt->hour;
        unsigned char minute = dt->minute;
        unsigned char second = dt->second;
        datetimeFromJulian(dt, julian + count);
        dt->hour = hour;
        dt->minute = minute;
        dt->second = second;
        dt->tz_offset_minutes = tz;
        break;
    }

    case DATE_UNIT_HOUR:
    case DATE_UNIT_MINUTE:
    case DATE_UNIT_SECOND:
    {
        long seconds = 0;
        if (unit == DATE_UNIT_HOUR)
            seconds = (long)count * 3600L;
        else if (unit == DATE_UNIT_MINUTE)
            seconds = (long)count * 60L;
        else
            seconds = (long)count;

        long epoch = datetimeGetUnix(dt);
        datetimeFromUnix(dt, (time_t)(epoch + seconds));
        break;
    }

    default:
        break;
    }
}

void dateTrunc(struct DateTime *dt, enum DateUnit unit)
{
    switch (unit)
    {
    case DATE_UNIT_MILLENNIUM:
        // Same floor division as EXTRACT(MILLENNIUM …): year / 1000
        dt->year = (dt->year / 1000) * 1000;
        dt->month = 1;
        dt->day = 1;
        dt->hour = 0;
        dt->minute = 0;
        dt->second = 0;
        break;
    case DATE_UNIT_CENTURY:
        dt->year = (dt->year / 100) * 100;
        dt->month = 1;
        dt->day = 1;
        dt->hour = 0;
        dt->minute = 0;
        dt->second = 0;
        break;
    case DATE_UNIT_DECADE:
        dt->year = (dt->year / 10) * 10;
        dt->month = 1;
        dt->day = 1;
        dt->hour = 0;
        dt->minute = 0;
        dt->second = 0;
        break;
    case DATE_UNIT_YEAR:
        dt->month = 1;
        dt->day = 1;
        dt->hour = 0;
        dt->minute = 0;
        dt->second = 0;
        break;
    case DATE_UNIT_QUARTER:
        dt->month = ((dt->month - 1) / 3) * 3 + 1;
        dt->day = 1;
        dt->hour = 0;
        dt->minute = 0;
        dt->second = 0;
        break;
    case DATE_UNIT_MONTH:
        dt->day = 1;
        dt->hour = 0;
        dt->minute = 0;
        dt->second = 0;
        break;
    case DATE_UNIT_WEEK:
    {
        int weekday = datetimeGetWeekDay(dt); /* Mon=1 … Sun=7 */
        dateAddUnit(dt, -(weekday - 1), DATE_UNIT_DAY);
        dt->hour = 0;
        dt->minute = 0;
        dt->second = 0;
        break;
    }
    case DATE_UNIT_DAY:
        dt->hour = 0;
        dt->minute = 0;
        dt->second = 0;
        break;
    case DATE_UNIT_HOUR:
        dt->minute = 0;
        dt->second = 0;
        break;
    case DATE_UNIT_MINUTE:
        dt->second = 0;
        break;
    case DATE_UNIT_SECOND:
    default:
        break;
    }
}

int datetimeAtTimeZone(struct DateTime *dt, short target_offset_minutes)
{
    if (dt->tz_offset_minutes == TZ_LOCAL)
    {
        return 0;
    }

    long epoch = datetimeGetUnix(dt);
    dt->tz_offset_minutes = target_offset_minutes;
    datetimeFromUnix(dt, (time_t)epoch);
    return 1;
}