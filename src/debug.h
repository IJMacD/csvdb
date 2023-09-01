#include "structs.h"

extern int debug_verbosity;

void debugRowList (struct RowList * list, int verbosity);

void debugTree (struct TreeNode * node);

void debugResultSet (struct ResultSet *results);

void debugNode (struct Node * node);

void debugNodes (struct Node nodes[], int node_count);

void debugLog (struct Query *query, const char *msg);

void debugFrom (struct Query *query);

void debugAST (FILE *output, struct Query *query);

void debugASTNode (FILE *output, struct Node *node);

void debugASTField (FILE *output, struct Field *field);
