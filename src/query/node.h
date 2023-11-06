struct Node * allocateNodeChildren (struct Node *node, int new_children);

struct Node * addChildNode (struct Node *node);

void copyNodeTree (struct Node *dest, struct Node *src);

void clearNode (struct Node *node);

void freeNode (struct Node *node);

struct Node * cloneNodeIntoChild (struct Node *node);

int getTableBitMap (struct Node *node);

void swapNodes (struct Node *nodeA, struct Node *nodeB);

const char *nodeGetFieldName (struct Node *node);
