#include "structs.h"

void insertNode (struct DB *db, int field_index, struct tree *root, struct tree *node);

void insertNumericNode (struct tree *root, struct tree *node);

void insertTextNode (struct DB *db, int field_index, struct tree *root, struct tree *node);

void walkTree (struct tree *node, struct RowList * row_list);

void walkTreeBackwards (struct tree *node, struct RowList * row_list);