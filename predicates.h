#include "db.h"

#define OPERATOR_UN         0
#define OPERATOR_EQ         1
#define OPERATOR_NE         2
#define OPERATOR_LT         3
#define OPERATOR_LE         4
#define OPERATOR_GT         5
#define OPERATOR_GE         6

char parseOperator (const char *input);

int evaluateExpression (char op, const char *left, const char *right);

int pk_search(struct DB *db, int pk_index, const char *value, int result_index);