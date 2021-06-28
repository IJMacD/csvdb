#define OUTPUT_FLAG_HEADERS    1

#define FIELD_UNKNOWN       -1
#define FIELD_STAR          -2
#define FIELD_COUNT_STAR    -3
#define FIELD_ROW_NUMBER    -4
#define FIELD_ROW_INDEX     -5

int query (const char *query, int output_flags);