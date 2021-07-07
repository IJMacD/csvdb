struct tree {
    int rowid;
    unsigned long value;
    struct tree *left;
    struct tree *right;
};

void insertNode (struct DB *db, int field_index, struct tree *root, struct tree *node);

void insertNumericNode (struct tree *root, struct tree *node);

void insertTextNode (struct DB *db, int field_index, struct tree *root, struct tree *node);

void walkTree (struct tree *node, int **rowids);

void walkTreeBackwards (struct tree *node, int **rowids);