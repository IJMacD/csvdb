#include <stdio.h>

#include "db.h"
#include "query.h"
#include "date.h"

int evaluateFunction(FILE *f, struct DB *db, struct ResultColumn *column, int record_index);

int evaluateAggregateFunction (FILE *f, struct DB *db, struct ResultColumn *column, int *result_ids, int result_count);
