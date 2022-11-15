#include "../structs.h"

int executeSourceDummyRow (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
);

int executeSourcePK (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
);

int executeSourceUnique (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
);

int executeSourceIndexSeek (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
);

int executeSourceIndexScan (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
);

int executeSourceTableFull (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
);

int executeSourceTableScan (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
);

int executeSourceCoveringIndexSeek (
    struct Table *tables,
    struct PlanStep *step,
    struct ResultSet *result_set
);
