#include "db.h"
#include "predicates.h"

int filterRows (struct DB *db, int *source_rowids, int source_count, struct Predicate *p, int *result_rowids);
