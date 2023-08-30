struct Node * allocateNodeChildren (struct Node *node, int new_children);

struct Node * addChildNode (struct Node *node);

void copyNodeTree (struct Node *dest, struct Node *src);

void freeNode (struct Node *node);

void cloneNodeIntoChild (struct Node *node);
