#include <stdio.h>

#include "db.h"
#include "query.h"
#include "date.h"

int outputFunction(FILE *f, struct DB *db, struct ResultColumn *column, int record_index);