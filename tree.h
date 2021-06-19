struct tree {
    int rowid;
    long value;
    struct tree *left;
    struct tree *right;
};

void makeNode (struct DB *db, int field_index, int rowid, struct tree *node);

void insertNumericNode (struct tree *root, struct tree *node);

void walkTree (struct tree *node, int **rowids);