#pragma once

#include "query.h"
#include "predicates.h"
#include "limits.h"

#define PLAN_TABLE_ACCESS_FULL      0
#define PLAN_TABLE_ACCESS_ROWID     1
#define PLAN_PK_UNIQUE              2
#define PLAN_PK_RANGE               3
#define PLAN_INDEX_UNIQUE           4
#define PLAN_INDEX_RANGE            5
#define PLAN_SORT                   10
#define PLAN_REVERSE                11
#define PLAN_SLICE                  12
#define PLAN_GROUP                  15
#define PLAN_UNION                  20
#define PLAN_INTERSECT              21
#define PLAN_SELECT                 50

struct PlanStep {
    int type;
    int param1;
    int param2;
    int predicate_count;
    struct Predicate *predicates;
};

struct Plan {
    int step_count;
    struct PlanStep steps[PLAN_MAX_STEPS];
};

int makePlan (struct Query *q, struct Plan *plan);

void destroyPlan (struct Plan *plan);