#include <stdio.h>

#include "db.h"
#include "query.h"
#include "date.h"
#include "result.h"

int evaluateFunction(FILE *f, struct DB *db, struct ResultColumn *column, int record_index);

int evaluateAggregateFunction (FILE *f, struct DB *db[], int table_count, struct ResultColumn *column, struct RowList * row_list);
