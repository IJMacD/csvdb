struct Node * allocateNodeChildren (struct Node *node, int new_children);

struct Node * addChildNode (struct Node *node);

void copyNodeTree (struct Node *dest, struct Node *src);

void clearNode (struct Node *node);

void freeNode (struct Node *node);

struct Node * cloneNodeIntoChild (struct Node *node);

int getTableBitMap (struct Node *node);

enum IndexSearchType findNodeIndex (
    struct DB *db,
    const char *table_name,
    struct Node *node,
    enum IndexSearchType index_type_flags
);