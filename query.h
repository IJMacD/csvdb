#define OUTPUT_FLAG_HEADERS    1

#define FIELD_UNKNOWN       -1
#define FIELD_STAR          -2
#define FIELD_COUNT_STAR    -3
#define FIELD_ROW_NUMBER    -4
#define FIELD_ROW_INDEX     -5

#define FLAG_HAVE_PREDICATE         1
#define FLAG_GROUP                  2
#define FLAG_PRIMARY_KEY_SEARCH     4
#define FLAG_ORDER                  8

int query (const char *query, int output_flags);