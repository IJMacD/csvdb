#include "db.h"
#include "predicates.h"
#include "result.h"

int filterRows (struct Query *q, struct RowList * source_list, struct Predicate *p, struct RowList * target_list);
