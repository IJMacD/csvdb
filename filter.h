#include "db.h"
#include "predicates.h"
#include "result.h"

int filterRows (struct DB *db, struct RowList * source_list, struct Predicate *p, struct RowList * target_list);
