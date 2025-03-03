#include "time.h"
#include "../structs.h"

int parseDateTime(const char *input, struct DateTime *output);

int parseDate(const char *input, struct DateTime *output);

int parseTime(const char *input, struct DateTime *output);

int sprintDateTime(char *output, struct DateTime *dt);

int sprintDate(char *output, struct DateTime *dt);

int sprintTime(char *output, struct DateTime *dt);

int isLeapYear(int year);

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

void datetimeFromUnix(struct DateTime *dt, time_t time);

int timeInSeconds(struct DateTime *dt);

void timeFromSeconds(struct DateTime *dt, int seconds);