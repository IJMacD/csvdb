#pragma once

#include <stdio.h>

#include "limits.h"

#define ROWID_NULL  -1

#define DB_SUBQUERY (void *)-1

#define dt(stop,start)  (stop.tv_sec - start.tv_sec) * 1000000 + stop.tv_usec - start.tv_usec

#define MIN(a,b)    ((a<b)?(a):(b))

#define VFS_COUNT   10

enum VFSType {
    VFS_NULL        = 0,
    VFS_CSV         = 1,
    VFS_CSV_MEM     = 2,
    VFS_VIEW        = 3,
    VFS_CALENDAR    = 4,
    VFS_SEQUENCE    = 5,
    VFS_SAMPLE      = 6,
    VFS_DIR         = 7,
    VFS_CSV_MMAP    = 8,
    VFS_TEMP        = 9,
};

struct DB {
    enum VFSType vfs;
    FILE *file;
    char *fields;
    int field_count;
    long *line_indices;
    char * data;
    int _record_count;
};

enum Order {
    ORDER_UN =      0,
    ORDER_ASC =     '\x1e',
    ORDER_DESC =    '\x1f',
};

struct TreeNode {
    int key;
    char value[MAX_TABLE_LENGTH];
    struct TreeNode *left;
    struct TreeNode *right;
};

enum Function {
    MASK_FUNC_FAMILY =              0xE0,
    // xxxa aaaa
    // xxx          = family (mask 0xD0)
    //    a aaaa    = function (mask 0x1F)

    // This gives 8 families with 32 functions each
    // (00 function is usable in each family)

    // Family 000x (0x00) Basic
    // FUNC_FAM_BASIC =             0x00,

    FUNC_UNITY =                    0x00,
    FUNC_CHR =                      0x01,
    FUNC_TO_HEX =                   0x02,

    FUNC_RANDOM =                   0x0A,

    // Maths functions

    FUNC_ADD =                      0x11,
    FUNC_SUB =                      0x12,
    FUNC_MUL =                      0x13,
    FUNC_DIV =                      0x14,
    FUNC_MOD =                      0x15,
    FUNC_POW =                      0x16,

    FUNC_PARENS =                   0x1F,

    // Family 001x (0x20) String
    FUNC_FAM_STRING =               0x20,

    FUNC_LENGTH =                   0x21,
    FUNC_LEFT =                     0x22,
    FUNC_RIGHT =                    0x23,
    FUNC_CONCAT =                   0x24,

    // Family 010x (0x40) Extract
    FUNC_FAM_EXTRACT =              0x40,

    FUNC_EXTRACT_YEAR =             0x41,
    FUNC_EXTRACT_MONTH =            0x42,
    FUNC_EXTRACT_DAY =              0x43,
    FUNC_EXTRACT_WEEK =             0x44,
    FUNC_EXTRACT_WEEKDAY =          0x45,
    FUNC_EXTRACT_WEEKYEAR =         0x46,
    FUNC_EXTRACT_YEARDAY =          0x47,
    FUNC_EXTRACT_HEYEAR =           0x48,
    FUNC_EXTRACT_MILLENNIUM =       0x49,
    FUNC_EXTRACT_CENTURY =          0x4A,
    FUNC_EXTRACT_DECADE =           0x4B,
    FUNC_EXTRACT_QUARTER =          0x4C,

    FUNC_EXTRACT_HOUR =             0x4D,
    FUNC_EXTRACT_MINUTE =           0x4E,
    FUNC_EXTRACT_SECOND =           0x4F,

    FUNC_EXTRACT_MONTH_STRING =     0x50,
    FUNC_EXTRACT_WEEK_STRING =      0x51,
    FUNC_EXTRACT_YEARDAY_STRING =   0x52,

    FUNC_EXTRACT_JULIAN =           0x5C,
    FUNC_EXTRACT_DATE =             0x5D,
    FUNC_EXTRACT_TIME =             0x5E,
    FUNC_EXTRACT_DATETIME =         0x5F,

    // Family 011x (0x60) (Date)
    FUNC_FAM_DATE_60 =              0x60,

    FUNC_DATE_ADD =                 0x61,
    FUNC_DATE_SUB =                 0x62,
    FUNC_DATE_DIFF =                0x63,

    FUNC_DATE_TODAY =               0x70,

    // Family 100x (0x80) (Undefined)
    FUNC_FAM_UNDEF_80 =             0x80,

    // Family 101x (0xA0) Agg
    FUNC_FAM_AGG =                  0xA0,

    FUNC_AGG_COUNT =                0xA1,
    FUNC_AGG_MIN =                  0xA2,
    FUNC_AGG_MAX =                  0xA3,
    FUNC_AGG_SUM =                  0xA4,
    FUNC_AGG_AVG =                  0xA5,
    FUNC_AGG_LISTAGG =              0xA6,

    // Family 110x (0xC0) (Comparison Operators)
    // FUNC_FAM_OPERATOR =          0xC0, // (Aliased below)

    // Operator bitmap
    //
    //          GT      LT      EQ
    // NV                           |   0   Never
    // EQ                       1   |   1
    // LT               1       0   |   2
    // LE               1       1   |   3
    // GT       1       0       0   |   4
    // GE       1       0       1   |   5
    // NE       1       1       0   |   6
    // AL       1       1       1   |   7   Always

    OPERATOR_NEVER =                0xC0,
    OPERATOR_EQ =                   0xC1,
    OPERATOR_LT =                   0xC2,
    OPERATOR_LE =                   0xC3,
    OPERATOR_GT =                   0xC4,
    OPERATOR_GE =                   0xC5,
    OPERATOR_NE =                   0xC6,
    OPERATOR_ALWAYS =               0xC7,
    OPERATOR_LIKE =                 0xC8,
    OPERATOR_OR =                   0xC9,
    OPERATOR_AND =                  0xCA,

    // Family 111x (0xE0) (Dummy)
    FUNC_FAM_DUMMY =                0xE0,

    FUNC_PK =                       0xE1,
    FUNC_UNIQUE =                   0xE2,
    FUNC_INDEX =                    0xE3,

    FUNC_UNKNOWN =                  0xFF
};

#define FUNC_FAM_OPERATOR        OPERATOR_NEVER

enum FieldIndex {
    FIELD_UNKNOWN =                     -1,
    FIELD_STAR =                        -2,
    FIELD_COUNT_STAR =                  -3,
    FIELD_ROW_NUMBER =                  -4,
    FIELD_ROW_INDEX =                   -5,
    FIELD_CONSTANT =                    -6,
};

struct Field {
    char text[MAX_FIELD_LENGTH];
    int table_id;
    enum FieldIndex index;
};

// Since Field is first field of Node, it means that Node can be cast to Field
struct Node {
    struct Field field;
    enum Function function;
    char alias[MAX_FIELD_LENGTH];
    /**
     * @brief When function != FUNC_UNITY, child_count = -1 means this node is
     * it's own child.
     * i.e. this node's table_id and field are the single parameter to the
     * function.
     */
    int child_count;
    /* Can only be used when function is not FUNC_UNITY */
    struct Node *children;
};

enum IndexSearchResult {
    RESULT_FOUND =          0,
    RESULT_NO_ROWS =       -1,
    RESULT_BETWEEN =       -2,
    RESULT_BELOW_MIN =     -3,
    RESULT_ABOVE_MAX =     -4,

    RESULT_NO_INDEX =     -10,
};

enum IndexSearchType {
    INDEX_NONE =        0,
    INDEX_ANY =         0,
    INDEX_REGULAR =     1,
    INDEX_UNIQUE =      2,
    INDEX_PRIMARY =     3,
};

enum IndexScanMode {
    MODE_UNIQUE =        0,
    MODE_LOWER_BOUND =   1,
    MODE_UPPER_BOUND =   2,
};

enum PlanStepType {
    NO_PLAN =                   0,

    // POP = 0, PUSH = 1
    PLAN_TABLE_ACCESS_FULL =    0x01,
    PLAN_TABLE_SCAN =           0x02,
    PLAN_PK =                   0x04,
    PLAN_PK_RANGE =             0x05,
    PLAN_UNIQUE =               0x06,
    PLAN_UNIQUE_RANGE =         0x07,
    PLAN_INDEX_RANGE =          0x08,
    PLAN_INDEX_SCAN =           0x09,
    PLAN_COVERING_INDEX_SEEK =  0x0A,
    PLAN_DUMMY_ROW =            0x0F,

    // POP = 1(n), PUSH = 1(n'); n' < n
    PLAN_TABLE_ACCESS_ROWID =   0x10,

    // POP = 1(n x m), PUSH = 1(n' x m+1)
    PLAN_CROSS_JOIN =           0x21,
    PLAN_CONSTANT_JOIN =        0x22,
    PLAN_LOOP_JOIN =            0x23,
    PLAN_UNIQUE_JOIN =          0x24,
    PLAN_INDEX_JOIN =           0x25,

    // POP = 1, PUSH = 1
    PLAN_SORT =                 0x30,
    PLAN_REVERSE =              0x31,
    PLAN_SLICE =                0x32,
    PLAN_OFFSET =               0x33,

    // POP = 1, PUSH = N
    PLAN_GROUP_SORTED =         0x40,
    PLAN_GROUP =                0x41,

    // POP = 2, PUSH = 1
    PLAN_UNION =                0x50,
    PLAN_INTERSECT =            0x51,
    PLAN_EXCEPT =               0x52,

    // POP = N, PUSH = 0
    PLAN_SELECT =               0x60,
};

struct PlanStep {
    enum PlanStepType type;
    int limit;
    int node_count;
    struct Node *nodes;
};

struct Plan {
    int step_count;
    struct PlanStep steps[MAX_PLAN_STEPS];
};

// hsxx aaaa
// h         - headers
//  s        - stats
//   v       - verbose
//    a      - ast
//      aaaa - format

enum OutputOption {
    OUTPUT_OPTION_HEADERS =     1 << 7,
    OUTPUT_OPTION_STATS =       1 << 6,
    OUTPUT_OPTION_VERBOSE =     1 << 5,
    OUTPUT_OPTION_AST =         1 << 4,

    OUTPUT_MASK_FORMAT =        0x0F,

    OUTPUT_FORMAT_TAB =            1,
    OUTPUT_FORMAT_COMMA =          2,
    OUTPUT_FORMAT_JSON =           3,
    OUTPUT_FORMAT_HTML =           4,
    OUTPUT_FORMAT_JSON_ARRAY =     5,
    OUTPUT_FORMAT_SQL_INSERT =     6,
    OUTPUT_FORMAT_TABLE =          7,
    OUTPUT_FORMAT_INFO_SEP =       8,
    OUTPUT_FORMAT_XML =            9,
    OUTPUT_FORMAT_SQL_VALUES =    10,
};

enum QueryFlag {
    FLAG_HAVE_PREDICATE =       (1<<0),
    FLAG_GROUP =                (1<<1),
    FLAG_EXPLAIN =              (1<<12),
    FLAG_READ_ONLY =            (1<<13),
};

enum JoinType {
    JOIN_INNER =    0,
    JOIN_CROSS =    0,
    JOIN_LEFT =     1,
};

struct Table {
    char name[MAX_TABLE_LENGTH];
    char alias[MAX_TABLE_LENGTH];
    struct DB * db;
    struct Node join;
    enum JoinType join_type;
};

struct Query {
    #ifdef DEBUG
    int id;
    #endif
    struct Table *tables;
    int table_count;
    struct Node columns[MAX_FIELD_COUNT];
    int column_count;
    enum QueryFlag flags;
    int offset_value;
    int limit_value;
    struct Node *predicate_nodes;
    int predicate_count;
    struct Node order_nodes[MAX_FIELD_COUNT];
    int order_count;
    struct Node group_nodes[MAX_FIELD_COUNT];
    int group_count;
};

typedef int RowListIndex;

#define ROWLIST_ROWID -1

struct RowList {
    int row_count;
    int join_count;
    int group;
    int * row_ids;
};

struct ResultSet {
    int size;
    int count;
    int *row_list_indices;
};

struct DateTime {
    /** Year, range approx -32768 to 32767 */
    short year;
    /** Month, 1-based indexing */
    unsigned char month;
    /** Day of month */
    unsigned char day;
    /** Hour */
    unsigned char hour;
    /** Minute */
    unsigned char minute;
    /** Second */
    unsigned char second;
};

struct VFS {
    int (* openDB)(struct DB *db, const char *filename);
    void (* closeDB)(struct DB *db);
    int (* getFieldIndex)(struct DB *db, const char *field);
    char *(* getFieldName)(struct DB *db, int field_index);
    int (* getRecordCount)(struct DB *db);
    int (* getRecordValue)(
        struct DB *db,
        int record_index,
        int field_index,
        char *value,
        size_t value_max_length
    );
    enum IndexSearchType (* findIndex)(
        struct DB *db,
        const char *table_name,
        const char *index_name,
        int index_type_flags
    );
    int (* fullTableAccess)(
        struct DB *db,
        int row_list,
        struct Node *nodes,
        int node_count,
        int limit_value
    );
    int (* fullTableScan)(
        struct DB *db,
        int row_list,
        int start_rowid,
        int limit_value
    );
    int (* indexSearch)(
        struct DB *db,
        const char *value,
        int mode,
        int * output_flag
    );
    int (* insertRow)(
        struct DB *db,
        const char *row
    );
    int (* insertFromQuery)(
        struct DB *db,
        const char *query,
        const char **end_ptr
    );
};
