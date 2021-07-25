
struct DateTime {
    short year;
    unsigned char month;
    unsigned char day;
    unsigned char hour;
    unsigned char minute;
    unsigned char second;
};

int parseDateTime(const char *input, struct DateTime *output);

int isLeapYear(int year);

int datetimeGetYearDay(struct DateTime *dt);

int datetimeGetDayDiff(struct DateTime *dt1, struct DateTime *dt2);

int datetimeGetWeek (struct DateTime *dt);

int datetimeGetWeekYear (struct DateTime *dt);

int datetimeGetWeekDay (struct DateTime *dt);

int datetimeGetJulian (struct DateTime *dt);

void datetimeFromJulian (struct DateTime *dt, int julian);