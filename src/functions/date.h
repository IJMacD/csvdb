#include "time.h"
#include "../structs.h"

enum DateUnit {
    DATE_UNIT_INVALID = -1,
    DATE_UNIT_MILLENNIUM = 0,
    DATE_UNIT_CENTURY,
    DATE_UNIT_DECADE,
    DATE_UNIT_YEAR,
    DATE_UNIT_QUARTER,
    DATE_UNIT_MONTH,
    DATE_UNIT_WEEK,
    DATE_UNIT_DAY,
    DATE_UNIT_HOUR,
    DATE_UNIT_MINUTE,
    DATE_UNIT_SECOND
};

int parseDateTime(const char *input, struct DateTime *output);

int parseDate(const char *input, struct DateTime *output);

int parseTime(const char *input, struct DateTime *output);

/**
 * Parse date or datetime (datetime first). Returns 1 on success.
 */
int parseDateOrDateTime(const char *input, struct DateTime *output);

int sprintDateTime(char *output, struct DateTime *dt);

int sprintDate(char *output, struct DateTime *dt);

int sprintTime(char *output, struct DateTime *dt);

int isLeapYear(int year);

int daysInMonth(int year, int month);

int datetimeGetYearDay(struct DateTime *dt);

int datetimeGetDayDiff(struct DateTime *dt1, struct DateTime *dt2);

int datetimeGetWeek(struct DateTime *dt);

int datetimeGetWeekYear(struct DateTime *dt);

int datetimeGetWeekDay(struct DateTime *dt);

/**
 * Gives integer Julian day number at midnight at the beginning of the provided
 * date.
 *
 * Since Julian days start at the previous noon, the returned value is actually
 * 0.5 days lower than the true value at midnight.
 * i.e. it is evaluted at midnight and truncated.
 *
 * e.g.
 *          |  2021-12-25  |  2021-12-26  |
 *  |    2459573   |    2459574   |    2459575   |
 *
 * As the Julian day 2459574 starts at noon on Christmas day (2021-12-25T12) the
 * Julian value at midnight at the start of Boxing Day (2021-12-26T00) should be
 * 2459574.5 However, the value is truncated and 2459574 is returned for an
 * input date of 2021-12-26.
 */
int datetimeGetJulian(struct DateTime *dt);

void datetimeFromJulian(struct DateTime *dt, int julian);

long datetimeGetUnix(struct DateTime *dt);

/**
 * Fill calendar fields from unix time using dt->tz_offset_minutes as the
 * target zone (TZ_LOCAL → localtime; otherwise wall clock in that offset).
 */
void datetimeFromUnix(struct DateTime *dt, time_t time);

int timeInSeconds(struct DateTime *dt);

void timeFromSeconds(struct DateTime *dt, int seconds);

/**
 * Parse a date unit name (YEAR/YEARS, MONTH, HOUR, …).
 * Returns DATE_UNIT_INVALID if not recognised.
 */
enum DateUnit parseDateUnit(const char *unit);

/**
 * Convert INTERVAL n UNIT into an integer for use with +/−.
 * DAY/WEEK → day count; HOUR/MINUTE/SECOND → seconds.
 * Returns 0 on success; -1 if unit cannot be a fixed integer (MONTH/YEAR/…).
 */
int intervalToInt(int count, enum DateUnit unit, long *out);

/**
 * True if text is an INTERVAL calendar form ("N UNIT") that cannot fold to a
 * fixed day/second integer (MONTH, YEAR, MILLENNIUM, …).
 */
int isCalendarIntervalConstant(const char *text);

/**
 * ISO-8601 duration / period components (PnYnMnWnDTnHnMnS).
 * Designators after T set has_time.
 */
struct IsoPeriod {
    int years;
    int months;
    int weeks;
    int days;
    int hours;
    int minutes;
    int seconds;
    int has_time;
};

/**
 * Parse an ISO-8601 period string (e.g. P1Y2M, P2W, PT15M, -P1Y).
 * Returns 1 on success; 0 on failure.
 */
int parseIso8601Period(const char *input, struct IsoPeriod *out);

/**
 * Apply period × sign to dt (years, months, weeks, days, then time units).
 */
void dateAddPeriod(struct DateTime *dt, const struct IsoPeriod *period, int sign);

/**
 * Parse timezone target: Z, UTC, +HH:MM, -HH:MM.
 * Writes minutes east of UTC to *offset_minutes.
 * Returns 1 on success; 0 on failure.
 */
int parseTzOffset(const char *input, short *offset_minutes);

/**
 * True if local UTC offset can be determined; writes minutes east of UTC.
 */
int getLocalTzOffsetMinutes(short *offset_minutes);

void dateAddUnit(struct DateTime *dt, int count, enum DateUnit unit);

void dateTrunc(struct DateTime *dt, enum DateUnit unit);

/**
 * Convert dt (not TZ_LOCAL) to wall clock in target_offset_minutes.
 * Returns 1 on success; 0 if dt is TZ_LOCAL.
 */
int datetimeAtTimeZone(struct DateTime *dt, short target_offset_minutes);
